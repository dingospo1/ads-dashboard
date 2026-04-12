"""
Google Ads Performance Dashboard — Flask app with hourly auto-refresh.
Supports multiple time ranges: 7, 14, 30, 90, 180, 365 days.
"""

import os
import json
import logging
import threading
from datetime import datetime

from flask import Flask, render_template, jsonify, request

from fetch_data import fetch_all

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)

VALID_DAYS = [7, 14, 30, 90, 180, 365]

# In-memory data store keyed by days
_data = {}
_lock = threading.Lock()


def refresh_data():
    """Fetch fresh data for all time ranges."""
    global _data
    for days in VALID_DAYS:
        try:
            new_data = fetch_all(days=days)
            with _lock:
                _data[days] = new_data
            logger.info("Data refreshed for %dd at %s", days, new_data.get("fetched_at", "unknown"))
        except Exception as e:
            logger.error("Data refresh failed for %dd: %s", days, e)


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


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    """Manual refresh endpoint."""
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
