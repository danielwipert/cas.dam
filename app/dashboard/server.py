"""
server.py
Chorus AI Systems — Data Analytics Manager (DAM)
Flask dashboard server. Run from the app/ directory:
    python dashboard/server.py
Then open http://127.0.0.1:5000
"""

import glob
import json
import os
import sys

from flask import Flask, abort, jsonify, render_template

# Allow importing from app/core/
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.factlist_store import get_kpi_trend
from core.historical_kpis import HISTORICAL_BENCHMARKS

app = Flask(__name__, template_folder="templates")

BASE_DIR        = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPORT_DATA_DIR = os.path.join(BASE_DIR, "output", "report_data")

DOMAINS = ["fulfillment", "carrier_performance", "cost", "operational_integrity"]
DOMAIN_LABELS = {
    "fulfillment":           "Fulfillment",
    "carrier_performance":   "Carrier Performance",
    "cost":                  "Cost & Efficiency",
    "operational_integrity": "Operational Integrity",
}
STATUS_ORDER = {"red": 0, "yellow": 1, "informational": 2, "green": 3}
STATUS_SCORES = {"green": 9.0, "yellow": 6.0, "red": 3.0, "informational": 5.5}


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------

def _report_files():
    return sorted(glob.glob(os.path.join(REPORT_DATA_DIR, "*.json")))


def _load(run_id=None):
    files = _report_files()
    if not files:
        return None
    path = os.path.join(REPORT_DATA_DIR, f"{run_id}.json") if run_id else files[-1]
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _recent_runs(n=12):
    files = sorted(_report_files(), reverse=True)
    runs = []
    for fp in files[:n]:
        try:
            with open(fp, encoding="utf-8") as f:
                d = json.load(f)
            runs.append({
                "run_id":       d["run_id"],
                "report_week":  d["report_week"],
                "final_status": d["final_status"],
            })
        except Exception:
            pass
    return runs


def _domain_status(factlist):
    result = {}
    for f in factlist:
        d, s = f["domain"], f["threshold_status"]
        if d not in result or STATUS_ORDER.get(s, 99) < STATUS_ORDER.get(result[d], 99):
            result[d] = s
    return result


def _domain_scores(factlist):
    from collections import defaultdict
    buckets = defaultdict(list)
    for f in factlist:
        buckets[f["domain"]].append(STATUS_SCORES.get(f["threshold_status"], 5.5))
    return {d: round(sum(v) / len(v), 1) for d, v in buckets.items()}


def _kpi_summary(factlist):
    counts = {"n_green": 0, "n_yellow": 0, "n_red": 0, "n_info": 0}
    for f in factlist:
        s = f["threshold_status"]
        if s == "green":         counts["n_green"] += 1
        elif s == "yellow":      counts["n_yellow"] += 1
        elif s == "red":         counts["n_red"] += 1
        else:                    counts["n_info"] += 1
    return counts


def _kpi_trends(factlist):
    orig = os.getcwd()
    os.chdir(BASE_DIR)
    trends = {}
    try:
        for f in factlist:
            name = f["kpi_name"]
            if name not in trends:
                trends[name] = get_kpi_trend(name, n_weeks=8)
    finally:
        os.chdir(orig)
    return trends


def _template_ctx(run):
    factlist = run["factlist"]
    return dict(
        run=run,
        recent_runs=_recent_runs(),
        domain_status=_domain_status(factlist),
        domain_scores=_domain_scores(factlist),
        kpi_summary=_kpi_summary(factlist),
        kpi_trends=_kpi_trends(factlist),
        historical_benchmarks=HISTORICAL_BENCHMARKS,
        domains=DOMAINS,
        domain_labels=DOMAIN_LABELS,
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    run = _load()
    if not run:
        return (
            "<h2 style='font-family:system-ui;padding:40px'>"
            "No reports found. Run the pipeline first:<br>"
            "<code>python pipeline.py --test</code></h2>",
            404,
        )
    return render_template("dashboard.html", **_template_ctx(run))


@app.route("/report/<run_id>")
def report(run_id):
    run = _load(run_id)
    if not run:
        abort(404)
    return render_template("dashboard.html", **_template_ctx(run))


@app.route("/api/runs")
def api_runs():
    return jsonify(_recent_runs())


@app.route("/api/data/<run_id>")
def api_data(run_id):
    run = _load(run_id)
    if not run:
        abort(404)
    return jsonify(run)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    print("\n  DAM Dashboard")
    print(f"  http://0.0.0.0:{port}")
    print(f"  Report data dir: {REPORT_DATA_DIR}\n")
    app.run(debug=debug, host="0.0.0.0", port=port)
