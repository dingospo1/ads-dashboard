"""
Google Ads Performance Dashboard — Flask app with hourly auto-refresh.
Supports multiple time ranges and comparison periods.
"""

import os
import json
import logging
import threading
from datetime import datetime

from flask import Flask, render_template, jsonify, request

from fetch_data import fetch_all, fetch_all_for_range

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)

VALID_DAYS = [7, 14, 30, 90, 180, 365]

# In-memory data store: _data[days] = current data, _compare[(days, mode)] = comparison data
_data = {}
_compare = {}
_lock = threading.Lock()


def refresh_data():
    """Fetch fresh data for all time ranges."""
    global _data, _compare
    for days in VALID_DAYS:
        try:
            new_data = fetch_all(days=days)
            with _lock:
                _data[days] = new_data
            logger.info("Data refreshed for %dd at %s", days, new_data.get("fetched_at", "unknown"))
        except Exception as e:
            logger.error("Data refresh failed for %dd: %s", days, e)
    # Clear comparison cache on refresh so it gets re-fetched with fresh tokens
    with _lock:
        _compare.clear()


def start_scheduler():
    """Background scheduler that refreshes data every hour."""
    from apscheduler.schedulers.background import BackgroundScheduler
    scheduler = BackgroundScheduler()
    interval = int(os.environ.get("REFRESH_INTERVAL_MINUTES", 60))
    scheduler.add_job(refresh_data, "interval", minutes=interval, id="refresh")
    scheduler.start()
    logger.info("Scheduler started: refreshing every %d minutes", interval)


@app.route("/")
def dashboard():
    days = request.args.get("days", 7, type=int)
    if days not in VALID_DAYS:
        days = 7

    with _lock:
        data = _data.get(days, {"happy": [], "upscale": [], "fetched_at": "Loading..."})

    all_accs = data.get("happy", []) + data.get("upscale", [])
    total_cost = sum(a["totalCost"] for a in all_accs)
    total_revenue = sum(a["totalRevenue"] for a in all_accs)
    total_roas = round(total_revenue / total_cost, 2) if total_cost > 0 else 0
    num_campaigns = sum(len(a["campaigns"]) for a in all_accs)

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
    )


@app.route("/api/data")
def api_data():
    days = request.args.get("days", 7, type=int)
    if days not in VALID_DAYS:
        days = 7
    with _lock:
        return jsonify(_data.get(days, {}))


@app.route("/api/compare")
def api_compare():
    """Return comparison data for a given time range and mode.
    mode=period: previous N days (e.g. 7d current = day -8 to -14)
    mode=year: same N days one year ago
    """
    days = request.args.get("days", 7, type=int)
    mode = request.args.get("mode", "period")
    if days not in VALID_DAYS or mode not in ("period", "year"):
        return jsonify({"error": "invalid params"}), 400

    cache_key = (days, mode)
    with _lock:
        if cache_key in _compare:
            return jsonify(_compare[cache_key])

    # Compute offset
    if mode == "period":
        offset_days = days  # shift back by N days
    else:
        offset_days = 365  # shift back by 1 year

    try:
        compare_data = fetch_all_for_range(days=days, offset=offset_days)
        with _lock:
            _compare[cache_key] = compare_data
        return jsonify(compare_data)
    except Exception as e:
        logger.error("Compare fetch failed for %dd/%s: %s", days, mode, e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    threading.Thread(target=refresh_data, daemon=True).start()
    return jsonify({"status": "refresh started"})


@app.route("/health")
def health():
    d7 = _data.get(7, {})
    return jsonify({"status": "ok", "fetched_at": d7.get("fetched_at"), "ranges_loaded": list(_data.keys())})


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
