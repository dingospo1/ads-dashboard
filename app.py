"""
Google Ads Performance Dashboard — Flask app with hourly auto-refresh.
"""

import os
import json
import logging
import threading
import urllib.parse
from datetime import datetime, date, timedelta

import requests as req_lib
from flask import Flask, render_template, jsonify, request, redirect

from fetch_data import (
    fetch_all, fetch_all_for_range, fetch_deeper, compute_date_range,
    fetch_all_mc_status, fetch_segment, get_token, list_child_accounts,
    MCCS, SEGMENT_FIELDS, MERCHANT_ID_MAP
)
import opportunities as opps_mod

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)

VALID_DAYS = [7, 14, 30, 90, 180, 365]
VALID_RANGES = ["mtd", "ytd", "lastmonth"]

# Cache: key = ("rolling", days) or ("special", range_type)
_data = {}
_compare = {}
_lock = threading.Lock()


def _cache_key(days, range_type, custom_start=None, custom_end=None):
    if custom_start and custom_end:
        return ("custom", custom_start, custom_end)
    if range_type in VALID_RANGES:
        return ("special", range_type)
    return ("rolling", days if days in VALID_DAYS else 7)


def refresh_data():
    """Fetch fresh data for rolling ranges and special ranges."""
    global _compare
    # Rolling
    for days in VALID_DAYS:
        try:
            new_data = fetch_all(days=days)
            with _lock:
                _data[("rolling", days)] = new_data
            logger.info("Refreshed rolling %dd", days)
        except Exception as e:
            logger.error("Refresh failed rolling %dd: %s", days, e)
    # Special ranges
    for rt in VALID_RANGES:
        try:
            new_data = fetch_all(range_type=rt)
            with _lock:
                _data[("special", rt)] = new_data
            logger.info("Refreshed %s", rt)
        except Exception as e:
            logger.error("Refresh failed %s: %s", rt, e)
    with _lock:
        _compare.clear()


def refresh_opportunities():
    """Daily: regenerate Opportunities audits for every account."""
    with _lock:
        cached = _data.get(("rolling", 7))
    if not cached:
        logger.info("Skipping opportunities refresh — no cached data yet")
        return
    try:
        opps_mod.regenerate_all(cached)
    except Exception as e:
        logger.error("Opportunities refresh failed: %s", e)


def start_scheduler():
    from apscheduler.schedulers.background import BackgroundScheduler
    scheduler = BackgroundScheduler()
    interval = int(os.environ.get("REFRESH_INTERVAL_MINUTES", 60))
    scheduler.add_job(refresh_data, "interval", minutes=interval, id="refresh")
    # Opportunities are on-demand only (no daily pre-compute) to keep API costs low
    scheduler.start()
    logger.info("Scheduler started: refresh every %d min", interval)


@app.route("/")
def dashboard():
    days = request.args.get("days", 7, type=int)
    range_type = request.args.get("range", None)
    custom_start = request.args.get("start", None)
    custom_end = request.args.get("end", None)

    if days not in VALID_DAYS:
        days = 7
    if range_type not in VALID_RANGES:
        range_type = None

    key = _cache_key(days, range_type, custom_start, custom_end)
    with _lock:
        data = _data.get(key, {"happy": [], "upscale": [], "fetched_at": "Loading..."})

    all_accs = data.get("happy", []) + data.get("upscale", [])
    total_cost = sum(a["totalCost"] for a in all_accs)
    total_revenue = sum(a["totalRevenue"] for a in all_accs)
    total_roas = round(total_revenue / total_cost, 2) if total_cost > 0 else 0
    num_campaigns = sum(len(a.get("campaigns", [])) for a in all_accs)

    # Compute display date range for subtitle
    start_str, end_str = compute_date_range(days, range_type, custom_start, custom_end)

    # Detect Happy Mondays auth failure — no accounts loaded but Upscale is fine
    happy_auth_failed = (
        len(data.get("happy", [])) == 0
        and len(data.get("upscale", [])) > 0
        and data.get("fetched_at", "Loading...") != "Loading..."
    )

    return render_template(
        "dashboard.html",
        data_json=json.dumps(data),
        total_cost=total_cost,
        total_revenue=total_revenue,
        total_roas=total_roas,
        num_accounts=len(all_accs),
        num_campaigns=num_campaigns,
        fetched_at=data.get("fetched_at", "Never"),
        current_days=days,
        valid_days=VALID_DAYS,
        range_type=range_type or "",
        custom_start=custom_start or "",
        custom_end=custom_end or "",
        start_date=start_str,
        end_date=end_str,
        happy_auth_failed=happy_auth_failed,
    )


@app.route("/api/data")
def api_data():
    days = request.args.get("days", 7, type=int)
    range_type = request.args.get("range", None)
    custom_start = request.args.get("start", None)
    custom_end = request.args.get("end", None)

    if days not in VALID_DAYS:
        days = 7
    if range_type not in VALID_RANGES:
        range_type = None

    key = _cache_key(days, range_type, custom_start, custom_end)
    with _lock:
        cached = _data.get(key)

    if cached:
        return jsonify(cached)

    try:
        result = fetch_all(days=days, range_type=range_type,
                           custom_start=custom_start, custom_end=custom_end)
        with _lock:
            _data[key] = result
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/compare")
def api_compare():
    days = request.args.get("days", 7, type=int)
    mode = request.args.get("mode", "period")
    range_type = request.args.get("range", None)
    custom_start = request.args.get("start", None)
    custom_end = request.args.get("end", None)

    if mode not in ("period", "year"):
        return jsonify({"error": "invalid mode"}), 400

    start_str, end_str = compute_date_range(days, range_type, custom_start, custom_end)
    cache_key = (start_str, end_str, mode)

    with _lock:
        if cache_key in _compare:
            return jsonify(_compare[cache_key])

    s = datetime.strptime(start_str, "%Y-%m-%d").date()
    e = datetime.strptime(end_str, "%Y-%m-%d").date()
    period_days = (e - s).days + 1

    if mode == "period":
        cmp_end = s - timedelta(days=1)
        cmp_start = cmp_end - timedelta(days=period_days - 1)
    else:
        try:
            cmp_start = s.replace(year=s.year - 1)
            cmp_end = e.replace(year=e.year - 1)
        except ValueError:
            cmp_start = s - timedelta(days=365)
            cmp_end = e - timedelta(days=365)

    try:
        compare_data = fetch_all_for_range(
            custom_start=cmp_start.strftime("%Y-%m-%d"),
            custom_end=cmp_end.strftime("%Y-%m-%d")
        )
        with _lock:
            _compare[cache_key] = compare_data
        return jsonify(compare_data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/deeper")
def api_deeper():
    account_name = request.args.get("account", "")
    mcc_key = request.args.get("mcc", "happy")
    days = request.args.get("days", 7, type=int)
    range_type = request.args.get("range", None)
    custom_start = request.args.get("start", None)
    custom_end = request.args.get("end", None)

    if not account_name or mcc_key not in ("happy", "upscale"):
        return jsonify({"error": "invalid params"}), 400

    if days not in VALID_DAYS:
        days = 7
    if range_type not in VALID_RANGES:
        range_type = None

    start_str, end_str = compute_date_range(days, range_type, custom_start, custom_end)

    try:
        result = fetch_deeper(account_name, mcc_key, start_str, end_str)
        return jsonify(result)
    except Exception as e:
        logger.error("Deeper dive failed for %s: %s", account_name, e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/mc-status")
def api_mc_status():
    """Fetch Merchant Center product approval status for all accounts."""
    # Use any cached data set to get merchant IDs (prefer the 7-day cache)
    with _lock:
        cached = _data.get(("rolling", 7)) or next(iter(_data.values()), None)

    if not cached:
        return jsonify({"error": "No cached data yet. Wait for initial refresh."}), 503

    try:
        result = fetch_all_mc_status(cached)
        return jsonify(result)
    except Exception as e:
        logger.error("MC status fetch failed: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/mc-debug")
def api_mc_debug():
    """Debug: show auto-discovered and manually configured merchant IDs per account."""
    with _lock:
        cached = _data.get(("rolling", 7)) or next(iter(_data.values()), None)
    if not cached:
        return jsonify({"error": "No cached data yet."}), 503

    rows = []
    for mcc_key in ["happy", "upscale"]:
        for acc in cached.get(mcc_key, []):
            acc_id = str(acc.get("accountId", ""))
            auto_mid = acc.get("merchantId", 0)
            map_mid = MERCHANT_ID_MAP.get(acc_id, 0)
            effective_mid = auto_mid or map_mid
            rows.append({
                "mcc": mcc_key,
                "name": acc["name"],
                "accountId": acc_id,
                "autoDiscoveredMerchantId": auto_mid,
                "mappedMerchantId": map_mid,
                "effectiveMerchantId": effective_mid,
                "source": "auto" if auto_mid else ("map" if map_mid else "none"),
            })
    return jsonify(rows)


@app.route("/api/deeper-segment")
def api_deeper_segment():
    """Fetch shopping_performance_view grouped by product type or custom label."""
    account_id  = request.args.get("account_id", "").strip()
    mcc         = request.args.get("mcc", "").strip()
    segment_key = request.args.get("segment", "product_type_l1").strip()
    days        = int(request.args.get("days", 7))
    range_type  = request.args.get("range", "").strip()
    custom_start = request.args.get("start", "")
    custom_end   = request.args.get("end", "")

    if segment_key not in SEGMENT_FIELDS:
        return jsonify({"error": f"Unknown segment: {segment_key}"}), 400
    if mcc not in ("happy", "upscale"):
        return jsonify({"error": "Invalid mcc"}), 400

    start_str, end_str = compute_date_range(
        days=days,
        range_type=range_type or None,
        custom_start=custom_start or None,
        custom_end=custom_end or None,
    )
    result = fetch_segment(account_id, mcc, start_str, end_str, segment_key)
    return jsonify(result)


@app.route("/api/segment-debug")
def api_segment_debug():
    """Debug: returns first 3 raw rows from shopping_performance_view for a given account."""
    account_id = request.args.get("account_id", "").strip()
    mcc_key    = request.args.get("mcc", "happy").strip()
    from fetch_data import gaql, get_token, MCCS, SEGMENT_FIELDS
    if not account_id:
        return jsonify({"error": "pass ?account_id=XXXXXXXXX&mcc=happy"}), 400
    mcc = MCCS.get(mcc_key)
    if not mcc:
        return jsonify({"error": "bad mcc"}), 400
    try:
        token = get_token(mcc_key)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    from datetime import date, timedelta
    end = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    start = (date.today() - timedelta(days=8)).strftime("%Y-%m-%d")
    fields = ", ".join(f for f, _ in SEGMENT_FIELDS.values())
    rows = gaql(token, account_id, mcc["login_customer_id"], f"""
        SELECT {fields}, metrics.cost_micros
        FROM shopping_performance_view
        WHERE segments.date BETWEEN '{start}' AND '{end}'
        LIMIT 3
    """)
    return jsonify([r.get("segments", {}) for r in rows])


@app.route("/api/accounts")
def api_accounts():
    """Debug endpoint — returns raw account IDs and descriptiveNames from the Google Ads API."""
    import requests as req
    result = {}
    for mcc_key, mcc in MCCS.items():
        try:
            token = get_token(mcc_key)
            login_id = mcc["login_customer_id"]
            # Call API directly to capture any error response
            url = f"https://googleads.googleapis.com/v20/customers/{login_id}/googleAds:searchStream"
            resp = req.post(url, headers={
                "Authorization": f"Bearer {token}",
                "developer-token": "yZwRitD8t90ZfDP_dc7IlQ",
                "login-customer-id": login_id,
                "Content-Type": "application/json",
            }, json={"query": "SELECT customer_client.id, customer_client.descriptive_name FROM customer_client WHERE customer_client.level = 1"}, timeout=15)
            if resp.ok:
                chunks = resp.json()
                rows = [r for c in chunks for r in c.get("results", [])]
                result[mcc_key] = [{"id": str(r["customerClient"]["id"]), "name": r["customerClient"].get("descriptiveName", "")} for r in rows]
            else:
                result[mcc_key] = {"error": f"HTTP {resp.status_code}", "body": resp.text[:500]}
        except Exception as e:
            result[mcc_key] = {"error": str(e)}
    return jsonify(result)


@app.route("/api/opportunities")
def api_opportunities():
    """Return cached opportunities for an account, or generate live if ?force=1."""
    account_id = request.args.get("account_id", "").strip()
    mcc_key = request.args.get("mcc", "").strip()
    force = request.args.get("force", "0") == "1"
    account_name = request.args.get("name", "")

    if not account_id or mcc_key not in ("happy", "upscale"):
        return jsonify({"error": "account_id and mcc (happy|upscale) required"}), 400

    # Parse skip list (findings the user has marked as not relevant)
    skip_raw = request.args.get("skip", "")
    skip_list = []
    if skip_raw:
        try:
            skip_list = json.loads(skip_raw)
        except Exception:
            pass

    if not force:
        cached = opps_mod.get_cached(account_id, mcc_key)
        if cached:
            return jsonify(cached)

    # No cache (or forced) — generate synchronously
    result = opps_mod.generate_opportunities(account_id, mcc_key, account_name, skip_list=skip_list)
    return jsonify(result)


@app.route("/api/chat", methods=["POST"])
def api_chat():
    """Multi-turn chat about a specific account."""
    body = request.json or {}
    account_id  = body.get("account_id", "").strip()
    mcc_key     = body.get("mcc", "").strip()
    account_name = body.get("name", "")
    messages    = body.get("messages", [])

    if not account_id or mcc_key not in ("happy", "upscale"):
        return jsonify({"error": "account_id and mcc required"}), 400
    if not messages:
        return jsonify({"error": "messages required"}), 400

    response = opps_mod.chat_with_account(account_id, mcc_key, account_name, messages)
    return jsonify({"response": response})


@app.route("/api/opportunities/refresh-all", methods=["POST"])
def api_opps_refresh_all():
    """Trigger a full regenerate of opportunities for all accounts (async)."""
    threading.Thread(target=refresh_opportunities, daemon=True).start()
    return jsonify({"status": "opportunities refresh started"})


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    threading.Thread(target=refresh_data, daemon=True).start()
    return jsonify({"status": "refresh started"})


@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "ranges_loaded": len(_data),
        "fetched_at": datetime.now().isoformat()
    })


def update_render_env(key: str, value: str):
    """Persist a new token to Render env vars so it survives restarts."""
    api_key = os.environ.get("RENDER_API_KEY")
    service_id = os.environ.get("RENDER_SERVICE_ID", "srv-d7de14n7f7vs739p6dkg")
    if not api_key:
        logger.warning("RENDER_API_KEY not set — token updated in memory only")
        return
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    resp = req_lib.get(f"https://api.render.com/v1/services/{service_id}/env-vars", headers=headers, timeout=10)
    if not resp.ok:
        logger.error("Render API get env vars failed: %s", resp.text[:200])
        return
    updated = []
    found = False
    for item in resp.json():
        ev = item.get("envVar", item)
        if ev.get("key") == key:
            updated.append({"key": key, "value": value})
            found = True
        else:
            updated.append({"key": ev["key"], "value": ev.get("value", "")})
    if not found:
        updated.append({"key": key, "value": value})
    put = req_lib.put(f"https://api.render.com/v1/services/{service_id}/env-vars", headers=headers, json=updated, timeout=10)
    if put.ok:
        logger.info("Render API: updated %s", key)
    else:
        logger.error("Render API: failed to update %s: %s", key, put.text[:200])


@app.route("/auth/start")
def auth_start():
    """Start Google OAuth flow to re-authenticate Happy Mondays MCC."""
    client_id = MCCS["happy"]["client_id"]
    redirect_uri = request.host_url.rstrip("/") + "/auth/callback"
    params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": "https://www.googleapis.com/auth/adwords",
        "access_type": "offline",
        "prompt": "consent",
    }
    return redirect("https://accounts.google.com/o/oauth2/auth?" + urllib.parse.urlencode(params))


@app.route("/auth/callback")
def auth_callback():
    """Handle OAuth callback — exchange code, update token in memory and Render."""
    code = request.args.get("code")
    error = request.args.get("error")
    if error or not code:
        return f"<h2>Auth failed: {error or 'no code'}</h2><a href='/'>Back</a>", 400

    resp = req_lib.post("https://oauth2.googleapis.com/token", data={
        "client_id": MCCS["happy"]["client_id"],
        "client_secret": MCCS["happy"]["client_secret"],
        "code": code,
        "redirect_uri": request.host_url.rstrip("/") + "/auth/callback",
        "grant_type": "authorization_code",
    }, timeout=10)

    if not resp.ok:
        return f"<h2>Token exchange failed</h2><a href='/'>Back</a>", 400

    new_token = resp.json().get("refresh_token")
    if not new_token:
        return "<h2>No refresh token returned — try again</h2><a href='/auth/start'>Retry</a>", 400

    MCCS["happy"]["refresh_token"] = new_token
    logger.info("Happy Mondays token refreshed via OAuth flow")
    update_render_env("HAPPY_REFRESH_TOKEN", new_token)
    threading.Thread(target=refresh_data, daemon=True).start()

    return """<html><body style="font-family:sans-serif;background:#111;color:#fff;padding:40px;text-align:center">
        <h2>✅ Re-authenticated successfully!</h2>
        <p>Dashboard is refreshing data now...</p>
        <script>setTimeout(() => window.location.href = '/', 3000)</script>
        </body></html>"""


_initialized = False


def init_app():
    global _initialized
    if _initialized:
        return
    _initialized = True
    logger.info("Fetching initial data...")
    refresh_data()
    start_scheduler()


init_app()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
