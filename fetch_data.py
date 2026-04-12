"""
Fetches campaign-level data from all Google Ads accounts across both MCCs.
Returns a structured dict ready for the dashboard template.
"""

import os
import json
import logging
from datetime import datetime, timedelta, date

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


def compute_date_range(days=7, range_type=None, custom_start=None, custom_end=None):
    """Returns (start_str, end_str) for the given range specification."""
    today = date.today()
    yesterday = today - timedelta(days=1)

    if range_type == "mtd":
        s = today.replace(day=1)
        e = yesterday
        if s > e:  # today is the 1st
            s = (today - timedelta(days=1)).replace(day=1)
    elif range_type == "ytd":
        s = today.replace(month=1, day=1)
        e = yesterday
    elif range_type == "lastmonth":
        first_this = today.replace(day=1)
        e = first_this - timedelta(days=1)
        s = e.replace(day=1)
    elif range_type == "custom" and custom_start and custom_end:
        s = datetime.strptime(custom_start, "%Y-%m-%d").date()
        e = datetime.strptime(custom_end, "%Y-%m-%d").date()
    else:
        e = yesterday
        s = e - timedelta(days=days - 1)

    return s.strftime("%Y-%m-%d"), e.strftime("%Y-%m-%d")


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


def fetch_campaigns(token: str, account_id: str, login_customer_id: str,
                    start_str: str, end_str: str):
    """Returns (campaigns_list, merchant_id) where merchant_id is the linked MC account (or 0)."""
    rows = gaql(
        token, account_id, login_customer_id,
        f"SELECT campaign.id, campaign.name, campaign.status, campaign.advertising_channel_type, "
        f"campaign.primary_status, campaign.shopping_setting.merchant_id, "
        f"metrics.clicks, metrics.impressions, metrics.cost_micros, "
        f"metrics.conversions_by_conversion_date, "
        f"metrics.conversions_value_by_conversion_date "
        f"FROM campaign "
        f"WHERE campaign.status IN ('ENABLED', 'PAUSED') "
        f"AND segments.date BETWEEN '{start_str}' AND '{end_str}'"
    )

    camps = {}
    merchant_ids = set()
    for r in rows:
        name = r["campaign"]["name"]
        campaign_id = str(r["campaign"].get("id", ""))
        campaign_status = r["campaign"].get("status", "ENABLED")   # ENABLED or PAUSED
        primary_status = r["campaign"].get("primaryStatus", "UNKNOWN")  # ELIGIBLE, LIMITED, etc.
        cost = int(r["metrics"].get("costMicros", 0)) / 1e6
        value = float(r["metrics"].get("conversionsValueByConversionDate", 0))
        # Collect merchant center IDs from shopping campaigns
        mid = r["campaign"].get("shoppingSetting", {}).get("merchantId")
        if mid:
            merchant_ids.add(int(mid))
        if name not in camps:
            camps[name] = {
                "id": campaign_id,
                "name": name,
                "type": r["campaign"].get("advertisingChannelType", ""),
                "status": primary_status,
                "campaignStatus": campaign_status,
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
    merchant_id = next(iter(merchant_ids), 0)
    return result, merchant_id


def fetch_all(days: int = 7, range_type=None, custom_start=None, custom_end=None) -> dict:
    """Fetch campaign data for all accounts across both MCCs."""
    start_str, end_str = compute_date_range(days, range_type, custom_start, custom_end)
    logger.info("Starting full data fetch: %s to %s...", start_str, end_str)
    data = {"happy": [], "upscale": [], "fetched_at": None,
            "start_date": start_str, "end_date": end_str}

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
                campaigns, merchant_id = fetch_campaigns(token, acc_id, mcc["login_customer_id"], start_str, end_str)
                # Totals from active (ENABLED) campaigns only
                active = [c for c in campaigns if c["campaignStatus"] == "ENABLED"]
                total_cost = sum(c["cost"] for c in active)
                total_revenue = sum(c["revenue"] for c in active)

                data[mcc_key].append({
                    "name": acc_name,
                    "accountId": acc_id,
                    "merchantId": merchant_id,
                    "totalCost": round(total_cost, 2),
                    "totalRevenue": round(total_revenue, 2),
                    "totalRoas": round(total_revenue / total_cost, 2) if total_cost > 0 else 0,
                    "campaigns": campaigns,
                })
                logger.info("  %s: $%.0f cost, $%.0f rev", acc_name, total_cost, total_revenue)
            except Exception as e:
                logger.error("  Failed to fetch %s (%s): %s", acc_name, acc_id, e)

    data["happy"].sort(key=lambda x: x["totalCost"], reverse=True)
    data["upscale"].sort(key=lambda x: x["totalCost"], reverse=True)
    data["fetched_at"] = datetime.now().strftime("%Y-%m-%d %H:%M UTC+7")
    return data


def fetch_deeper(account_name: str, mcc_key: str, start_str: str, end_str: str) -> dict:
    """Fetch detailed metrics for a single account (Deeper Dive modal)."""
    mcc = MCCS[mcc_key]
    try:
        token = get_token(mcc_key)
    except Exception as e:
        return {"error": f"Auth failed: {e}"}

    accounts = list_child_accounts(token, mcc["login_customer_id"])
    target_id = None
    for acc in accounts:
        acc_id = str(acc["id"])
        name = ACCOUNT_NAMES.get(acc_id, acc.get("name", acc_id))
        if name == account_name:
            target_id = acc_id
            break

    if not target_id:
        return {"error": "Account not found"}

    login_id = mcc["login_customer_id"]

    # Daily breakdown
    daily_rows = gaql(token, target_id, login_id, f"""
        SELECT segments.date,
        metrics.cost_micros,
        metrics.conversions_value_by_conversion_date,
        metrics.conversions_by_conversion_date,
        metrics.clicks, metrics.impressions
        FROM campaign
        WHERE campaign.status IN ('ENABLED', 'PAUSED')
        AND segments.date BETWEEN '{start_str}' AND '{end_str}'
    """)

    daily = {}
    for r in daily_rows:
        d = r["segments"]["date"]
        if d not in daily:
            daily[d] = {"date": d, "cost": 0, "revenue": 0, "conversions": 0, "clicks": 0, "impressions": 0}
        daily[d]["cost"] += int(r["metrics"].get("costMicros", 0)) / 1e6
        daily[d]["revenue"] += float(r["metrics"].get("conversionsValueByConversionDate", 0))
        daily[d]["conversions"] += float(r["metrics"].get("conversionsByConversionDate", 0))
        daily[d]["clicks"] += int(r["metrics"].get("clicks", 0))
        daily[d]["impressions"] += int(r["metrics"].get("impressions", 0))

    for d in daily.values():
        d["cost"] = round(d["cost"], 2)
        d["revenue"] = round(d["revenue"], 2)
        d["conversions"] = round(d["conversions"], 1)
        d["roas"] = round(d["revenue"] / d["cost"], 2) if d["cost"] > 0 else 0

    daily_list = sorted(daily.values(), key=lambda x: x["date"])

    # Campaign metrics with impression share
    camp_rows = gaql(token, target_id, login_id, f"""
        SELECT campaign.id, campaign.name, campaign.status, campaign.primary_status,
        metrics.cost_micros,
        metrics.conversions_value_by_conversion_date,
        metrics.conversions_by_conversion_date,
        metrics.clicks, metrics.impressions,
        metrics.search_impression_share
        FROM campaign
        WHERE campaign.status IN ('ENABLED', 'PAUSED')
        AND segments.date BETWEEN '{start_str}' AND '{end_str}'
    """)

    camps = {}
    for r in camp_rows:
        name = r["campaign"]["name"]
        if name not in camps:
            camps[name] = {
                "id": str(r["campaign"].get("id", "")),
                "status": r["campaign"].get("primaryStatus", "UNKNOWN"),
                "campaignStatus": r["campaign"].get("status", "ENABLED"),
                "cost": 0, "revenue": 0, "conversions": 0,
                "clicks": 0, "impressions": 0, "is_sum": 0.0, "is_count": 0
            }
        camps[name]["cost"] += int(r["metrics"].get("costMicros", 0)) / 1e6
        camps[name]["revenue"] += float(r["metrics"].get("conversionsValueByConversionDate", 0))
        camps[name]["conversions"] += float(r["metrics"].get("conversionsByConversionDate", 0))
        camps[name]["clicks"] += int(r["metrics"].get("clicks", 0))
        camps[name]["impressions"] += int(r["metrics"].get("impressions", 0))
        is_val = r["metrics"].get("searchImpressionShare")
        if is_val and str(is_val) not in ("", "--"):
            try:
                camps[name]["is_sum"] += float(is_val)
                camps[name]["is_count"] += 1
            except Exception:
                pass

    # Build per-campaign metrics list
    campaign_list = []
    for name, c in camps.items():
        cost = c["cost"]
        revenue = c["revenue"]
        conversions = c["conversions"]
        clicks = c["clicks"]
        impressions = c["impressions"]
        is_pct = (c["is_sum"] / c["is_count"] * 100) if c["is_count"] > 0 else 0
        campaign_list.append({
            "id": c["id"],
            "name": name,
            "status": c["status"],
            "campaignStatus": c["campaignStatus"],
            "cost": round(cost, 2),
            "revenue": round(revenue, 2),
            "roas": round(revenue / cost, 2) if cost > 0 else 0,
            "conversions": round(conversions, 1),
            "clicks": clicks,
            "impressions": impressions,
            "cpc": round(cost / clicks, 2) if clicks > 0 else 0,
            "ctr": round(clicks / impressions * 100, 2) if impressions > 0 else 0,
            "convRate": round(conversions / clicks * 100, 2) if clicks > 0 else 0,
            "aov": round(revenue / conversions, 2) if conversions > 0 else 0,
            "impressionShare": round(is_pct, 1),
        })
    campaign_list.sort(key=lambda x: x["cost"], reverse=True)

    total_cost = sum(c["cost"] for c in camps.values())
    total_rev = sum(c["revenue"] for c in camps.values())
    total_conv = sum(c["conversions"] for c in camps.values())
    total_clicks = sum(c["clicks"] for c in camps.values())
    total_impr = sum(c["impressions"] for c in camps.values())
    is_vals = [c["is_sum"] / c["is_count"] for c in camps.values() if c["is_count"] > 0]
    avg_is = (sum(is_vals) / len(is_vals) * 100) if is_vals else 0

    # Product level (Shopping only, graceful fallback)
    prod_list = []
    try:
        prod_rows = gaql(token, target_id, login_id, f"""
            SELECT segments.product_title,
            metrics.cost_micros,
            metrics.conversions_value_by_conversion_date,
            metrics.conversions_by_conversion_date,
            metrics.clicks, metrics.impressions
            FROM shopping_performance_view
            WHERE segments.date BETWEEN '{start_str}' AND '{end_str}'
        """)
        products = {}
        for r in prod_rows:
            title = r["segments"].get("productTitle") or "(no title)"
            if title not in products:
                products[title] = {"name": title, "cost": 0, "revenue": 0,
                                   "conversions": 0, "clicks": 0, "impressions": 0}
            products[title]["cost"] += int(r["metrics"].get("costMicros", 0)) / 1e6
            products[title]["revenue"] += float(r["metrics"].get("conversionsValueByConversionDate", 0))
            products[title]["conversions"] += float(r["metrics"].get("conversionsByConversionDate", 0))
            products[title]["clicks"] += int(r["metrics"].get("clicks", 0))
            products[title]["impressions"] += int(r["metrics"].get("impressions", 0))

        prod_list = sorted(products.values(), key=lambda x: x["cost"], reverse=True)[:50]
        for p in prod_list:
            p["cost"] = round(p["cost"], 2)
            p["revenue"] = round(p["revenue"], 2)
            p["conversions"] = round(p["conversions"], 1)
            p["roas"] = round(p["revenue"] / p["cost"], 2) if p["cost"] > 0 else 0
            p["cpc"] = round(p["cost"] / p["clicks"], 2) if p["clicks"] > 0 else 0
            p["ctr"] = round(p["clicks"] / p["impressions"] * 100, 2) if p["impressions"] > 0 else 0
            p["aov"] = round(p["revenue"] / p["conversions"], 2) if p["conversions"] > 0 else 0
    except Exception as e:
        logger.warning("Product fetch failed for %s: %s", account_name, e)

    return {
        "accountName": account_name,
        "startDate": start_str,
        "endDate": end_str,
        "totalCost": round(total_cost, 2),
        "totalRevenue": round(total_rev, 2),
        "totalRoas": round(total_rev / total_cost, 2) if total_cost > 0 else 0,
        "totalConversions": round(total_conv, 1),
        "totalClicks": total_clicks,
        "totalImpressions": total_impr,
        "cpc": round(total_cost / total_clicks, 2) if total_clicks > 0 else 0,
        "ctr": round(total_clicks / total_impr * 100, 2) if total_impr > 0 else 0,
        "convRate": round(total_conv / total_clicks * 100, 2) if total_clicks > 0 else 0,
        "aov": round(total_rev / total_conv, 2) if total_conv > 0 else 0,
        "impressionShare": round(avg_is, 1),
        "daily": daily_list,
        "campaigns": campaign_list,
        "products": prod_list,
    }


def fetch_all_for_range(days: int = 7, offset: int = 0,
                        custom_start: str = None, custom_end: str = None) -> dict:
    """Fetch account and campaign-level data for comparison period."""
    if custom_start and custom_end:
        start_str, end_str = custom_start, custom_end
    else:
        e = date.today() - timedelta(days=1 + offset)
        s = e - timedelta(days=days - 1)
        start_str, end_str = s.strftime("%Y-%m-%d"), e.strftime("%Y-%m-%d")

    logger.info("Comparison fetch: %s to %s...", start_str, end_str)
    data = {"happy": [], "upscale": []}

    for mcc_key in ["happy", "upscale"]:
        mcc = MCCS[mcc_key]
        try:
            token = get_token(mcc_key)
        except Exception as e:
            logger.error("Auth failed for %s: %s", mcc_key, e)
            continue

        accounts = list_child_accounts(token, mcc["login_customer_id"])

        for acc in accounts:
            acc_id = str(acc["id"])
            acc_name = ACCOUNT_NAMES.get(acc_id, acc.get("name", acc_id))
            try:
                rows = gaql(token, acc_id, mcc["login_customer_id"],
                    f"SELECT campaign.name, metrics.cost_micros, "
                    f"metrics.conversions_value_by_conversion_date "
                    f"FROM campaign WHERE campaign.status = 'ENABLED' "
                    f"AND segments.date BETWEEN '{start_str}' AND '{end_str}'"
                )
                camps = {}
                for r in rows:
                    name = r["campaign"]["name"]
                    cost = int(r["metrics"].get("costMicros", 0)) / 1e6
                    value = float(r["metrics"].get("conversionsValueByConversionDate", 0))
                    if name not in camps:
                        camps[name] = {"name": name, "cost": 0, "revenue": 0}
                    camps[name]["cost"] += cost
                    camps[name]["revenue"] += value

                camp_list = []
                for c in camps.values():
                    c["cost"] = round(c["cost"], 2)
                    c["revenue"] = round(c["revenue"], 2)
                    c["roas"] = round(c["revenue"] / c["cost"], 2) if c["cost"] > 0 else 0
                    camp_list.append(c)

                total_cost = sum(c["cost"] for c in camp_list)
                total_revenue = sum(c["revenue"] for c in camp_list)
                data[mcc_key].append({
                    "name": acc_name,
                    "totalCost": round(total_cost, 2),
                    "totalRevenue": round(total_revenue, 2),
                    "totalRoas": round(total_revenue / total_cost, 2) if total_cost > 0 else 0,
                    "campaigns": camp_list,
                })
            except Exception as e:
                logger.error("  [cmp] Failed %s: %s", acc_name, e)

    return data


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = fetch_all()
    print(json.dumps(result, indent=2))
