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

# Manual Merchant Center ID overrides.
# Set MERCHANT_ID_MAP in Render as a JSON string, e.g.:
#   {"1234567890": 987654321, "0987654321": 111222333}
# Keys are Google Ads account IDs (strings), values are MC account IDs (integers).
# These are used as fallback when auto-discovery from Shopping campaign settings gives 0.
def _load_merchant_id_map():
    raw = os.environ.get("MERCHANT_ID_MAP", "")
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return {str(k): int(v) for k, v in data.items()}
    except Exception as e:
        logger.warning("Failed to parse MERCHANT_ID_MAP: %s", e)
        return {}

MERCHANT_ID_MAP = _load_merchant_id_map()

# Static account list — fallback when Google Ads API cache is empty (e.g. quota exhausted).
# Merchant IDs are resolved via MERCHANT_ID_MAP env var.
STATIC_ACCOUNTS = {
    "happy": [
        {"name": "ByAnavrin",            "accountId": "8804096601"},
        {"name": "Aromely",              "accountId": "3456762782"},
        {"name": "Don's Liquors",        "accountId": "5525863856"},
        {"name": "Home Teeth Whitening", "accountId": "2614199584"},
        {"name": "Lucky Honey",          "accountId": "1850620188"},
        {"name": "Plum Play UK",         "accountId": "9222479033"},
        {"name": "Vintage Muscle",       "accountId": "6248452745"},
        {"name": "Warrior Willpower",    "accountId": "6078059992"},
    ],
    "upscale": [
        {"name": "All Cars Fix", "accountId": "4999947870"},
        {"name": "SilverAnt",    "accountId": "8045249572"},
        {"name": "HGV Direct",   "accountId": "6812459700"},
    ],
}

# Optional display-name overrides — only used when the API returns an empty descriptiveName.
# The API's real descriptiveName always takes priority.
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


def gaql(token: str, customer_id: str, login_customer_id: str, query: str,
         raise_on_error: bool = False) -> list:
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
        msg = f"API error {resp.status_code} for account {customer_id}: {resp.text[:300]}"
        logger.error(msg)
        if raise_on_error:
            raise RuntimeError(msg)
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
    merchant_spend = {}   # mid → total cost, so we pick the highest-spend MC if there are multiple
    for r in rows:
        name = r["campaign"]["name"]
        campaign_id = str(r["campaign"].get("id", ""))
        campaign_status = r["campaign"].get("status", "ENABLED")   # ENABLED or PAUSED
        primary_status = r["campaign"].get("primaryStatus", "UNKNOWN")  # ELIGIBLE, LIMITED, etc.
        cost = int(r["metrics"].get("costMicros", 0)) / 1e6
        value = float(r["metrics"].get("conversionsValueByConversionDate", 0))
        # Track merchant center IDs and their spend
        mid = r["campaign"].get("shoppingSetting", {}).get("merchantId")
        if mid:
            merchant_spend[int(mid)] = merchant_spend.get(int(mid), 0) + cost
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
    # Pick the MC with the most spend (deterministic, handles multi-MC accounts)
    merchant_id = max(merchant_spend, key=merchant_spend.get) if merchant_spend else 0
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
            acc_name = acc.get("name") or ACCOUNT_NAMES.get(acc_id, acc_id)
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
        name = acc.get("name") or ACCOUNT_NAMES.get(acc_id, acc_id)
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

    # Product level (Shopping/PMax only, graceful fallback)
    # Note: _by_conversion_date metrics are not available on shopping_performance_view —
    # use standard conversions_value / conversions here (product breakdown, not period comparison).
    prod_list = []
    try:
        prod_rows = gaql(token, target_id, login_id, f"""
            SELECT segments.product_title,
            metrics.cost_micros,
            metrics.conversions_value,
            metrics.conversions,
            metrics.clicks, metrics.impressions
            FROM shopping_performance_view
            WHERE segments.date BETWEEN '{start_str}' AND '{end_str}'
        """, raise_on_error=True)
        products = {}
        for r in prod_rows:
            title = r["segments"].get("productTitle") or "(no title)"
            if title not in products:
                products[title] = {"name": title, "cost": 0, "revenue": 0,
                                   "conversions": 0, "clicks": 0, "impressions": 0}
            products[title]["cost"]        += int(r["metrics"].get("costMicros", 0)) / 1e6
            products[title]["revenue"]     += float(r["metrics"].get("conversionsValue", 0))
            products[title]["conversions"] += float(r["metrics"].get("conversions", 0))
            products[title]["clicks"]      += int(r["metrics"].get("clicks", 0))
            products[title]["impressions"] += int(r["metrics"].get("impressions", 0))

        prod_list = sorted(products.values(), key=lambda x: x["cost"], reverse=True)[:100]
        for p in prod_list:
            p["cost"]        = round(p["cost"], 2)
            p["revenue"]     = round(p["revenue"], 2)
            p["conversions"] = round(p["conversions"], 1)
            p["roas"]        = round(p["revenue"] / p["cost"], 2) if p["cost"] > 0 else 0
            p["cpc"]         = round(p["cost"] / p["clicks"], 2) if p["clicks"] > 0 else 0
            p["ctr"]         = round(p["clicks"] / p["impressions"] * 100, 2) if p["impressions"] > 0 else 0
            p["aov"]         = round(p["revenue"] / p["conversions"], 2) if p["conversions"] > 0 else 0
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
            acc_name = acc.get("name") or ACCOUNT_NAMES.get(acc_id, acc_id)
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


def get_mc_token(mcc_key: str) -> str:
    """Get a Content API access token for a given MCC.

    For both Happy Mondays (OAuth) and Upscale (service account or OAuth override):
    - If UPSCALE_CONTENT_REFRESH_TOKEN is set, Upscale uses OAuth just like Happy Mondays.
    - Otherwise Upscale falls back to the service account.
    - Happy Mondays always uses HAPPY_CONTENT_REFRESH_TOKEN.
    Generate refresh tokens by running generate_content_token.py locally.
    """
    # OAuth override for Upscale (uses same OAuth client as Happy Mondays)
    if mcc_key == "upscale":
        upscale_rt = os.environ.get("UPSCALE_CONTENT_REFRESH_TOKEN", "")
        if upscale_rt:
            client_id     = os.environ.get("HAPPY_CLIENT_ID", "")
            client_secret = os.environ.get("HAPPY_CLIENT_SECRET", "")
            creds = Credentials(
                token=None,
                refresh_token=upscale_rt,
                token_uri="https://oauth2.googleapis.com/token",
                client_id=client_id,
                client_secret=client_secret,
            )
            creds.refresh(Request())
            return creds.token

    mcc = MCCS[mcc_key]
    if mcc["auth"] == "service_account":
        sa_json = mcc["service_account_json"]
        if not sa_json:
            raise ValueError("UPSCALE_SERVICE_ACCOUNT_JSON env var not set")
        info = json.loads(sa_json)
        creds = service_account.Credentials.from_service_account_info(
            info,
            scopes=[
                "https://www.googleapis.com/auth/adwords",
                "https://www.googleapis.com/auth/content",
            ]
        )
        creds.refresh(Request())
        return creds.token
    else:
        content_rt = os.environ.get("HAPPY_CONTENT_REFRESH_TOKEN", "")
        if not content_rt:
            raise ValueError(
                "HAPPY_CONTENT_REFRESH_TOKEN is not set. "
                "Run generate_content_token.py to create a refresh token with "
                "the Content API scope, then add it to Render environment variables."
            )
        creds = Credentials(
            token=None,
            refresh_token=content_rt,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=mcc["client_id"],
            client_secret=mcc["client_secret"],
        )
        creds.refresh(Request())
        return creds.token


def fetch_mc_status(merchant_id: int, token: str) -> dict:
    """Fetch product approval status for a Merchant Center account via Content API.
    Returns a dict with total, approved, disapproved, pending counts and top disapproval reasons."""
    base = f"https://shoppingcontent.googleapis.com/content/v2.1/{merchant_id}/productstatuses"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"maxResults": 250}

    all_statuses = []
    page_token = None
    pages = 0
    max_pages = 40  # cap at 10k products

    while pages < max_pages:
        p = dict(params)
        if page_token:
            p["pageToken"] = page_token
        try:
            resp = requests.get(base, headers=headers, params=p, timeout=20)
        except Exception as e:
            logger.warning("MC fetch network error for %s: %s", merchant_id, e)
            break

        if resp.status_code in (401, 403):
            logger.warning("MC auth error %s for merchant %s", resp.status_code, merchant_id)
            return {"error": f"Access denied (HTTP {resp.status_code}). Token may lack Content API scope."}
        if not resp.ok:
            try:
                err_msg = resp.json().get("error", {}).get("message", resp.text[:200])
            except Exception:
                err_msg = resp.text[:200]
            logger.warning("MC fetch error %s for merchant %s: %s", resp.status_code, merchant_id, err_msg)
            return {"error": f"API {resp.status_code}: {err_msg}"}

        data = resp.json()
        resources = data.get("resources", [])
        all_statuses.extend(resources)
        page_token = data.get("nextPageToken")
        pages += 1
        if not page_token:
            break

    if not all_statuses:
        return {"total": 0, "approved": 0, "disapproved": 0, "pending": 0,
                "approvalRate": 0, "topReasons": []}

    total = len(all_statuses)
    approved = 0
    disapproved = 0
    pending = 0
    issue_counts = {}  # all merchant-actionable issues across all products

    for ps in all_statuses:
        dests = ps.get("destinationStatuses", [])
        statuses_for_product = set()
        for dest in dests:
            approval = dest.get("approvalStatus") or dest.get("status", "")
            statuses_for_product.add(approval.upper())

        if "APPROVED" in statuses_for_product:
            approved += 1
        elif "DISAPPROVED" in statuses_for_product:
            disapproved += 1
        else:
            pending += 1

        # Count ALL merchant-actionable issues (disapproved + limited)
        for issue in ps.get("itemLevelIssues", []):
            if issue.get("resolution") == "merchant_action":
                reason = issue.get("description") or issue.get("code") or "Unknown"
                issue_counts[reason] = issue_counts.get(reason, 0) + 1

    top_issues = sorted(issue_counts.items(), key=lambda x: x[1], reverse=True)[:8]
    approval_rate = round(approved / total * 100, 1) if total > 0 else 0

    return {
        "total": total,
        "approved": approved,
        "disapproved": disapproved,
        "pending": pending,
        "approvalRate": approval_rate,
        "topReasons": [{"reason": r, "count": c} for r, c in top_issues],
    }


def fetch_all_mc_status(cached_data: dict) -> dict:
    """Build MC status for all accounts using merchant IDs already in cached_data.
    Returns { "happy": [...], "upscale": [...] } with status per account."""
    result = {"happy": [], "upscale": []}

    for mcc_key in ["happy", "upscale"]:
        accounts = cached_data.get(mcc_key, []) or STATIC_ACCOUNTS.get(mcc_key, [])
        if not accounts:
            continue
        try:
            token = get_mc_token(mcc_key)
        except ValueError as e:
            # Not configured yet (e.g. missing HAPPY_CONTENT_REFRESH_TOKEN)
            logger.warning("MC token not configured for %s: %s", mcc_key, e)
            for acc in accounts:
                result[mcc_key].append({
                    "name": acc["name"],
                    "merchantId": acc.get("merchantId", 0),
                    "error": "Content API not configured. Run generate_content_token.py and set HAPPY_CONTENT_REFRESH_TOKEN in Render.",
                })
            continue
        except Exception as e:
            logger.error("MC token failed for %s: %s", mcc_key, e)
            for acc in accounts:
                result[mcc_key].append({
                    "name": acc["name"],
                    "merchantId": acc.get("merchantId", 0),
                    "error": f"Auth failed: {e}",
                })
            continue

        for acc in accounts:
            acc_id = str(acc.get("accountId", ""))
            mid = acc.get("merchantId", 0) or MERCHANT_ID_MAP.get(acc_id, 0)
            entry = {
                "name": acc["name"],
                "merchantId": mid,
                "accountId": acc_id,
            }
            if not mid:
                entry["error"] = "No Merchant Center linked. Add this account's MC ID to MERCHANT_ID_MAP in Render env vars."
            else:
                status = fetch_mc_status(mid, token)
                entry.update(status)
            result[mcc_key].append(entry)

    return result


# Segment fields: key → (GAQL field, JSON camelCase key in response)
SEGMENT_FIELDS = {
    "product_type_l1": ("segments.product_type_l1", "productTypeL1"),
    "product_type_l2": ("segments.product_type_l2", "productTypeL2"),
    "product_type_l3": ("segments.product_type_l3", "productTypeL3"),
    "product_type_l4": ("segments.product_type_l4", "productTypeL4"),
    "custom_label_0":  ("segments.product_custom_attribute0", "productCustomAttribute0"),
    "custom_label_1":  ("segments.product_custom_attribute1", "productCustomAttribute1"),
    "custom_label_2":  ("segments.product_custom_attribute2", "productCustomAttribute2"),
    "custom_label_3":  ("segments.product_custom_attribute3", "productCustomAttribute3"),
    "custom_label_4":  ("segments.product_custom_attribute4", "productCustomAttribute4"),
}


def fetch_segment(account_id: str, mcc_key: str, start_str: str, end_str: str,
                  segment_key: str) -> dict:
    """Fetch shopping_performance_view grouped by a single segment dimension.
    Uses account_id directly so no list_child_accounts call is needed."""
    if segment_key not in SEGMENT_FIELDS:
        return {"error": f"Unknown segment: {segment_key}"}

    gaql_field, json_key = SEGMENT_FIELDS[segment_key]
    mcc = MCCS.get(mcc_key)
    if not mcc:
        return {"error": f"Unknown MCC: {mcc_key}"}

    try:
        token = get_token(mcc_key)
    except Exception as e:
        return {"error": f"Auth failed: {e}"}

    login_id = mcc["login_customer_id"]
    rows = gaql(token, account_id, login_id, f"""
        SELECT {gaql_field},
        metrics.cost_micros,
        metrics.conversions_value,
        metrics.conversions,
        metrics.clicks, metrics.impressions
        FROM shopping_performance_view
        WHERE segments.date BETWEEN '{start_str}' AND '{end_str}'
    """)

    # Log first row to diagnose field name issues
    if rows:
        logger.info("fetch_segment sample segments keys for %s (%s): %s",
                    segment_key, account_id, list(rows[0].get("segments", {}).keys()))

    groups: dict = {}
    for r in rows:
        seg = r.get("segments", {})
        # Try camelCase key first, then lowercase variant, then snake_case
        val = (seg.get(json_key)
               or seg.get(json_key[0].lower() + json_key[1:])
               or seg.get(gaql_field.split(".")[-1])
               or "")
        val = val.strip() if val else "(not set)"
        if not val:
            val = "(not set)"
        if val not in groups:
            groups[val] = {"name": val, "cost": 0.0, "revenue": 0.0,
                           "conversions": 0.0, "clicks": 0, "impressions": 0}
        g = groups[val]
        g["cost"]        += int(r["metrics"].get("costMicros", 0)) / 1e6
        g["revenue"]     += float(r["metrics"].get("conversionsValue", 0))
        g["conversions"] += float(r["metrics"].get("conversions", 0))
        g["clicks"]      += int(r["metrics"].get("clicks", 0))
        g["impressions"] += int(r["metrics"].get("impressions", 0))

    items = sorted(groups.values(), key=lambda x: x["cost"], reverse=True)
    for item in items:
        item["cost"]        = round(item["cost"], 2)
        item["revenue"]     = round(item["revenue"], 2)
        item["conversions"] = round(item["conversions"], 1)
        item["roas"]        = round(item["revenue"] / item["cost"], 2) if item["cost"] > 0 else 0
        item["cpc"]         = round(item["cost"] / item["clicks"], 2)  if item["clicks"] > 0 else 0
        item["ctr"]         = round(item["clicks"] / item["impressions"] * 100, 2) if item["impressions"] > 0 else 0
        item["aov"]         = round(item["revenue"] / item["conversions"], 2) if item["conversions"] > 0 else 0

    return {"segment": segment_key, "items": items}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = fetch_all()
    print(json.dumps(result, indent=2))
