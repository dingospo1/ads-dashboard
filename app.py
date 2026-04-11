"""
Google Ads Performance Dashboard — Flask app with hourly auto-refresh.
"""

import os
import json
import logging
import threading
from datetime import datetime

from flask import Flask, render_template, jsonify

from fetch_data import fetch_all

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)

# In-memory data store
_data = {"happy": [], "upscale": [], "fetched_at": "Not yet fetched"}
_lock = threading.Lock()


def refresh_data():
    """Fetch fresh data from Google Ads API."""
    global _data
    try:
        new_data = fetch_all()
        with _lock:
            _data = new_data
        logger.info("Data refreshed at %s", _data.get("fetched_at", "unknown"))
    except Exception as e:
        logger.error("Data refresh failed: %s", e)


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
    with _lock:
        data = _data.copy()

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
    )


@app.route("/api/data")
def api_data():
    with _lock:
        return jsonify(_data)


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    """Manual refresh endpoint."""
    threading.Thread(target=refresh_data, daemon=True).start()
    return jsonify({"status": "refresh started"})


@app.route("/health")
def health():
    return jsonify({"status": "ok", "fetched_at": _data.get("fetched_at")})


# Gunicorn preload hook: runs once when using --preload
# For direct `python app.py`, handled in __main__
_initialized = False

def init_app():
    global _initialized
    if _initialized:
        return
    _initialized = True
    logger.info("Fetching initial data...")
    refresh_data()
    start_scheduler()


# Auto-init when imported by gunicorn with --preload
init_app()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
