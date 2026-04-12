"""
Google Ads Performance Dashboard — Flask app with hourly auto-refresh.
"""

import os
import json
import logging
import threading
from datetime import datetime, date, timedelta

from flask import Flask, render_template, jsonify, request

from fetch_data import (
    fetch_all, fetch_all_for_range, fetch_deeper, compute_date_range,
    fetch_all_mc_status
)

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


def start_scheduler():
    from apscheduler.schedulers.background import BackgroundScheduler
    scheduler = BackgroundScheduler()
    interval = int(os.environ.get("REFRESH_INTERVAL_MINUTES", 60))
    scheduler.add_job(refresh_data, "interval", minutes=interval, id="refresh")
    scheduler.start()
    logger.info("Scheduler started: every %d minutes", interval)


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
