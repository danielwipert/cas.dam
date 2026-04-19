"""
build_history.py
Chorus AI Systems — Data Analytics Manager (DAM)

Generates 9 weeks of synthetic data and runs the full pipeline for each,
building up the factlist + report_data history that powers the dashboard
sparklines. Wipes any prior factlists for the target weeks first.

Run from app/:
    python scripts/build_history.py

Estimated time: ~10 minutes (9 pipeline runs × ~65s each).
"""

import os
import subprocess
import sys
import time
from datetime import datetime

# Must run from app/
APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, APP_DIR)

from scripts.generate_test_data import generate_week

# ---------------------------------------------------------------------------
# Week schedule
# progress = 0.0 → year-ago performance (worse), 1.0 → current (best)
# Seeds chosen for variety; all deterministic and reproducible.
# ---------------------------------------------------------------------------
WEEKS = [
    # (week_end_date,  seed,  progress)
    ("2026-02-07",  1001,  0.000),   # oldest — year-ago performance
    ("2026-02-14",  1002,  0.125),
    ("2026-02-21",  1003,  0.250),
    ("2026-02-28",  1004,  0.375),
    ("2026-03-07",  1005,  0.500),   # midpoint — month-avg performance
    ("2026-03-14",  1006,  0.625),
    ("2026-03-21",  1007,  0.750),
    ("2026-03-28",  1008,  0.875),
    ("2026-04-04",    42,  1.000),   # current — last_week performance
]

N_ORDERS  = 500
DATA_DIR  = os.path.join(APP_DIR, "data", "test")
FACTLIST_DIR = os.path.join(APP_DIR, "data", "factlists")


def load_api_key() -> str:
    env_path = os.path.join(APP_DIR, ".env")
    if not os.path.exists(env_path):
        print("  ERROR: app/.env not found. Add TOGETHER_API_KEY=... to it.")
        sys.exit(1)
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line.startswith("TOGETHER_API_KEY="):
                return line.split("=", 1)[1].strip()
    print("  ERROR: TOGETHER_API_KEY not found in app/.env")
    sys.exit(1)


def run_pipeline(week_date: str, api_key: str) -> bool:
    cmd = [
        sys.executable, "pipeline.py",
        "--shopify", "data/test/shopify_orders.csv",
        "--tpl",     "data/test/tpl_shipments.csv",
        "--fedex",   "data/test/fedex_tracking.csv",
        "--dhl",     "data/test/dhl_tracking.csv",
        "--week",    week_date,
    ]
    env = {**os.environ, "TOGETHER_API_KEY": api_key}
    result = subprocess.run(cmd, cwd=APP_DIR, env=env)
    return result.returncode == 0


def main():
    print("\n" + "=" * 62)
    print("  DAM History Builder")
    print(f"  Building {len(WEEKS)} weeks of synthetic data")
    print(f"  {N_ORDERS} orders per week | Est. ~{len(WEEKS) * 70 // 60} min")
    print("=" * 62)

    api_key = load_api_key()

    total_start = time.time()
    results = []

    for i, (week_date, seed, progress) in enumerate(WEEKS, 1):
        print(f"\n{'-' * 62}")
        print(f"  Week {i}/{len(WEEKS)}: {week_date}  (progress={progress:.3f})")
        print("-" * 62)

        # 1. Generate CSVs
        print("  [1/2] Generating synthetic data...")
        generate_week(week_date, n_orders=N_ORDERS, seed=seed,
                      out_dir=DATA_DIR, progress=progress)

        # 2. Run pipeline
        print(f"\n  [2/2] Running pipeline for {week_date}...")
        t0      = time.time()
        success = run_pipeline(week_date, api_key)
        elapsed = time.time() - t0

        results.append((week_date, success, elapsed))
        status  = "DONE" if success else "FAILED"
        print(f"\n  {status} in {elapsed:.0f}s")

    # Summary
    total_elapsed = time.time() - total_start
    print(f"\n{'=' * 62}")
    print("  History Build Complete")
    print(f"  Total time: {total_elapsed / 60:.1f} min")
    print("-" * 62)
    for week, ok, t in results:
        icon = "OK" if ok else "XX"
        print(f"  {icon}  {week}  ({t:.0f}s)")

    failed = [w for w, ok, _ in results if not ok]
    if failed:
        print(f"\n  WARNING: {len(failed)} run(s) failed: {failed}")
        print("  Re-run manually: python pipeline.py --shopify ... --week <date>")
    else:
        print(f"\n  All {len(WEEKS)} weeks complete. Dashboard sparklines are now live.")
        print("  Restart the Flask server to see the full history.")
        print("=" * 62 + "\n")


if __name__ == "__main__":
    main()
