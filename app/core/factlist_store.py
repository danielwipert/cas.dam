"""
factlist_store.py
Chorus AI Systems — Data Analytics Manager (DAM)

FactList persistence: save and load weekly FactLists as dated JSON files.
Also manages the rolling 4-week cost baseline for cost KPI thresholds.

Design:
  - One JSON file per week: factlists/YYYY-MM-DD.json
  - No database required — plain file-based storage
  - First run: no prior week available; cost KPIs marked informational
  - Weeks 1-3: partial baseline; cost KPIs disclosed as building
  - Week 4+: full 4-week rolling baseline available

Public API:
    save_factlist(factlist, week_date)
    load_prior_factlist(current_week_date) -> list[FactListEntry] | None
    load_cost_baseline(current_week_date) -> dict | None
    get_baseline_status(current_week_date) -> str
"""

import json
import os
from datetime import datetime, timedelta
from typing import Optional

from core.schemas import FactListEntry, KPIDomain, ThresholdStatus

FACTLIST_DIR = "data/factlists"
COST_KPI_NAMES = {"Shipping Cost per Order", "Cost by Carrier"}


# ---------------------------------------------------------------------------
# SAVE
# ---------------------------------------------------------------------------

def save_factlist(factlist: list[FactListEntry], week_date: str) -> str:
    """
    Persist the FactList to disk as a dated JSON file.
    Returns the file path written.
    Called by the orchestrator after Stage 3 completes successfully.
    """
    os.makedirs(FACTLIST_DIR, exist_ok=True)
    path = os.path.join(FACTLIST_DIR, f"{week_date}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(
            [entry.model_dump(mode="json") for entry in factlist],
            f,
            indent=2,
            default=str,
        )
    return path


# ---------------------------------------------------------------------------
# LOAD PRIOR WEEK
# ---------------------------------------------------------------------------

def load_prior_factlist(current_week_date: str) -> Optional[list[FactListEntry]]:
    """
    Load the most recent FactList that predates current_week_date.
    Returns None if no prior week exists (first run).

    Args:
        current_week_date: ISO date string, e.g. "2026-04-04"
    """
    if not os.path.exists(FACTLIST_DIR):
        return None

    available = sorted(
        f for f in os.listdir(FACTLIST_DIR)
        if f.endswith(".json") and f < f"{current_week_date}.json"
    )
    if not available:
        return None

    path = os.path.join(FACTLIST_DIR, available[-1])
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return [FactListEntry(**entry) for entry in data]
    except Exception as e:
        # Corrupt file — surface as None, not a crash
        print(f"  ⚠  Could not load prior FactList from {path}: {e}")
        return None


# ---------------------------------------------------------------------------
# COST BASELINE
# ---------------------------------------------------------------------------

def load_cost_baseline(current_week_date: str) -> Optional[dict]:
    """
    Compute the rolling 4-week average cost per carrier from stored FactLists.
    Returns None if fewer than 4 prior weeks are available.

    Returns dict with keys:
        {
          "fedex_avg": float,
          "dhl_avg": float,
          "overall_avg": float,
          "weeks_included": int,
          "is_full_baseline": bool   # True only when weeks_included >= 4
        }
    """
    if not os.path.exists(FACTLIST_DIR):
        return None

    # Collect up to 4 prior weeks (not including current)
    available = sorted(
        f for f in os.listdir(FACTLIST_DIR)
        if f.endswith(".json") and f < f"{current_week_date}.json"
    )
    prior_weeks = available[-4:]  # most recent 4

    if not prior_weeks:
        return None

    fedex_vals: list[float] = []
    dhl_vals:   list[float] = []
    overall_vals: list[float] = []

    for fname in prior_weeks:
        path = os.path.join(FACTLIST_DIR, fname)
        try:
            with open(path, encoding="utf-8") as f:
                entries = json.load(f)
            for entry in entries:
                name = entry.get("kpi_name", "")
                val  = entry.get("final_value")
                if val is None:
                    continue
                if name == "Cost by Carrier":
                    fedex_vals.append(float(val))
                    aux = entry.get("auxiliary_value")
                    if aux is not None:
                        dhl_vals.append(float(aux))
                if name == "Shipping Cost per Order":
                    overall_vals.append(float(val))
        except Exception:
            continue

    if not overall_vals:
        return None

    return {
        "fedex_avg":        round(sum(fedex_vals)   / len(fedex_vals),   2) if fedex_vals   else None,
        "dhl_avg":          round(sum(dhl_vals)     / len(dhl_vals),     2) if dhl_vals     else None,
        "overall_avg":      round(sum(overall_vals) / len(overall_vals), 2),
        "weeks_included":   len(prior_weeks),
        "is_full_baseline": len(prior_weeks) >= 4,
    }


# ---------------------------------------------------------------------------
# BASELINE STATUS (for report disclosure)
# ---------------------------------------------------------------------------

def get_baseline_status(current_week_date: str) -> str:
    """
    Return a human-readable string describing the cost baseline state.
    Used in the report's verification footer.
    """
    if not os.path.exists(FACTLIST_DIR):
        return "first run — no baseline available; cost KPIs are informational"

    available = sorted(
        f for f in os.listdir(FACTLIST_DIR)
        if f.endswith(".json") and f < f"{current_week_date}.json"
    )
    n = len(available)
    if n == 0:
        return "first run — no baseline available; cost KPIs are informational"
    if n < 4:
        return f"baseline building ({n}/4 weeks); cost KPIs are informational"
    return f"full 4-week rolling baseline ({n} weeks of history)"


# ---------------------------------------------------------------------------
# LIST ALL STORED WEEKS  (for diagnostics / Layer 5 review)
# ---------------------------------------------------------------------------

def list_stored_weeks() -> list[str]:
    """Return sorted list of week dates with stored FactLists."""
    if not os.path.exists(FACTLIST_DIR):
        return []
    return sorted(
        f.replace(".json", "")
        for f in os.listdir(FACTLIST_DIR)
        if f.endswith(".json")
    )


def get_kpi_trend(kpi_name: str, n_weeks: int = 4) -> list[dict]:
    """
    Return the last n_weeks of values for a given KPI name.
    Useful for Layer 5 drift detection.

    Returns list of {week_date, value, threshold_status} dicts.
    """
    weeks = list_stored_weeks()[-n_weeks:]
    trend = []
    for week in weeks:
        path = os.path.join(FACTLIST_DIR, f"{week}.json")
        try:
            with open(path, encoding="utf-8") as f:
                entries = json.load(f)
            for entry in entries:
                if entry.get("kpi_name") == kpi_name:
                    trend.append({
                        "week_date":        week,
                        "value":            entry.get("final_value"),
                        "threshold_status": entry.get("threshold_status"),
                    })
                    break
        except Exception:
            continue
    return trend
