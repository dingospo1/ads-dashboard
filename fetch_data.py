"""
Fetches campaign-level data from all Google Ads accounts across both MCCs.
Returns a structured dict ready for the dashboard template.
"""

import os
import json
import logging
from datetime import datetime, timedelta

import requests
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials

logger = logging.getLogger(__name__)

DEVELOPER_TOKEN = os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN", "yZwRitD8t90ZfDP_dc7IlQ")
API_VERSION = "v20"
BASE_URL = f"https://googleads.googleapis.com/{API_VERSION}"

MCCS = {
    "happy": {
        "label": "Happy Mondays",
        "login_customer_id": "9418382054",
        "auth": "oauth",
        "client_id": os.environ.get("HAPPY_CLIENT_ID", ""),
        "client_secret": os.environ.get("HAPPY_CLIENT_SECRET", ""),
        "refresh_token": os.environ.get("HAPPY_REFRESH_TOKEN", ""),
    },
    "upscale": {
        "label": "Upscale",
        "login_customer_id": "1722529448",
        "auth": "service_account",
        "service_account_json": os.environ.get("UPSCALE_SERVICE_ACCOUNT_JSON", ""),
    },
}

ACCOUNT_NAMES = {
    "8804096601": "Anavrin",
    "3456762782": "Aromely",
    "5525863856": "Dons Liquors",
    "2614199584": "Lucky Honey",
    "1850620188": "Plum Play UK",
    "9222479033": "Vintage Muscle",
    "6248452745": "Warrior Willpower",
    "6078059992": "Home Teeth Whitening",
    "4999947870": "MCM Ecom",
    "8045249572": "SilverAnt",
    "6812459700": "HGV Direct",
}


def get_token(mcc_key: str) -> str:
    mcc = MCCS[mcc_key]
    if mcc["auth"] == "service_account":
        sa_json = mcc["service_account_json"]
        if not sa_json:
            raise ValueError("UPSCALE_SERVICE_ACCOUNT_JSON env var not set")
        info = json.loads(sa_json)
        creds = service_account.Credentials.from_service_account_info(
            info, scopes=["https://www.googleapis.com/auth/adwords"]
        )
        creds.refresh(Request())
        return creds.token
    else:
        if not mcc["refresh_token"]:
            raise ValueError(f"Refresh token not set for {mcc_key}")
        creds = Credentials(
            token=None,
            refresh_token=mcc["refresh_token"],
            token_uri="https://oauth2.googleapis.com/token",
            client_id=mcc["client_id"],
            client_secret=mcc["client_secret"],
        )
        creds.refresh(Request())
        return creds.token


def gaql(token: str, customer_id: str, login_customer_id: str, query: str) -> list:
    url = f"{BASE_URL}/customers/{customer_id}/googleAds:searchStream"
    resp = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "developer-token": DEVELOPER_TOKEN,
            "login-customer-id": login_customer_id,
            "Content-Type": "application/json",
        },
        json={"query": query},
        timeout=30,
    )
    if not resp.ok:
        logger.error("API error %s for account %s: %s", resp.status_code, customer_id, resp.text[:300])
        return []
    chunks = resp.json()
    return [row for chunk in chunks for row in chunk.get("results", [])]


def list_child_accounts(token: str, login_customer_id: str) -> list:
    rows = gaql(
        token, login_customer_id, login_customer_id,
        "SELECT customer_client.id, customer_client.descriptive_name, "
        "customer_client.manager, customer_client.status "
        "FROM customer_client WHERE customer_client.level = 1"
    )
    return [
        {
            "id": r["customerClient"]["id"],
            "name": r["customerClient"].get("descriptiveName", ""),
        }
        for r in rows
        if not r["customerClient"].get("manager", False)
        and r["customerClient"].get("status") == "ENABLED"
    ]


def fetch_campaigns(token: str, account_id: str, login_customer_id: str, days: int = 7) -> list:
    end = datetime.now() - timedelta(days=1)
    start = end - timedelta(days=days - 1)
    start_str = start.strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")

    rows = gaql(
        token, account_id, login_customer_id,
        f"SELECT campaign.name, campaign.advertising_channel_type, "
        f"campaign.primary_status, "
        f"metrics.clicks, metrics.impressions, metrics.cost_micros, "
        f"metrics.conversions_by_conversion_date, "
        f"metrics.conversions_value_by_conversion_date "
        f"FROM campaign "
        f"WHERE campaign.status = 'ENABLED' "
        f"AND segments.date BETWEEN '{start_str}' AND '{end_str}'"
    )

    camps = {}
    for r in rows:
        name = r["campaign"]["name"]
        cost = int(r["metrics"].get("costMicros", 0)) / 1e6
        value = float(r["metrics"].get("conversionsValueByConversionDate", 0))
        if name not in camps:
            camps[name] = {
                "name": name,
                "type": r["campaign"].get("advertisingChannelType", ""),
                "status": r["campaign"].get("primaryStatus", "UNKNOWN"),
                "cost": 0, "revenue": 0, "clicks": 0, "conversions": 0,
            }
        camps[name]["cost"] += cost
        camps[name]["revenue"] += value
        camps[name]["clicks"] += int(r["metrics"].get("clicks", 0))
        camps[name]["conversions"] += float(r["metrics"].get("conversionsByConversionDate", 0))

    result = []
    for c in camps.values():
        c["cost"] = round(c["cost"], 2)
        c["revenue"] = round(c["revenue"], 2)
        c["conversions"] = round(c["conversions"], 1)
        c["roas"] = round(c["revenue"] / c["cost"], 2) if c["cost"] > 0 else 0
        result.append(c)
    result.sort(key=lambda x: x["cost"], reverse=True)
    return result


def fetch_all(days: int = 7) -> dict:
    """Fetch campaign data for all accounts across both MCCs. Returns dashboard-ready dict."""
    logger.info("Starting full data fetch for %d days...", days)
    data = {"happy": [], "upscale": [], "fetched_at": None}

    for mcc_key in ["happy", "upscale"]:
        mcc = MCCS[mcc_key]
        try:
            token = get_token(mcc_key)
        except Exception as e:
            logger.error("Auth failed for %s: %s", mcc_key, e)
            continue

        accounts = list_child_accounts(token, mcc["login_customer_id"])
        logger.info("Found %d accounts in %s", len(accounts), mcc_key)

        for acc in accounts:
            acc_id = str(acc["id"])
            acc_name = ACCOUNT_NAMES.get(acc_id, acc.get("name", acc_id))
            try:
                campaigns = fetch_campaigns(token, acc_id, mcc["login_customer_id"], days=days)
                total_cost = sum(c["cost"] for c in campaigns)
                total_revenue = sum(c["revenue"] for c in campaigns)

                data[mcc_key].append({
                    "name": acc_name,
                    "totalCost": round(total_cost, 2),
                    "totalRevenue": round(total_revenue, 2),
                    "totalRoas": round(total_revenue / total_cost, 2) if total_cost > 0 else 0,
                    "campaigns": campaigns,
                })
                logger.info("  %s: $%.0f cost, $%.0f rev, %.2fx", acc_name, total_cost, total_revenue,
                            total_revenue / total_cost if total_cost > 0 else 0)
            except Exception as e:
                logger.error("  Failed to fetch %s (%s): %s", acc_name, acc_id, e)

    # Sort by cost desc
    data["happy"].sort(key=lambda x: x["totalCost"], reverse=True)
    data["upscale"].sort(key=lambda x: x["totalCost"], reverse=True)
    data["fetched_at"] = datetime.now().strftime("%Y-%m-%d %H:%M UTC+7")

    all_accs = data["happy"] + data["upscale"]
    total_cost = sum(a["totalCost"] for a in all_accs)
    total_rev = sum(a["totalRevenue"] for a in all_accs)
    logger.info("Fetch complete. %d accounts, $%.0f cost, $%.0f rev",
                len(all_accs), total_cost, total_rev)

    return data


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = fetch_all()
    print(json.dumps(result, indent=2))
