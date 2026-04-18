"""
historical_kpis.py
Chorus AI Systems — Data Analytics Manager (DAM)

Synthetic historical benchmarks for KPI trend comparison.
Represents approximately one year of operational history ending 2026-04-05
(the week prior to the current report period 2026-04-06 – 2026-04-12).

Each entry:
  last_week        — the prior 7-day period KPI value
  month_avg        — rolling 30-day average (ending last week)
  three_month_avg  — rolling 90-day average (ending last week)
  year_avg         — rolling 365-day average (ending last week)

direction:
  "up"      — higher value is better  (rates, match %)
  "down"    — lower value is better   (times, costs, unshipped rate)
  "neutral" — informational, no good/bad polarity (carrier mix)
"""

HISTORICAL_BENCHMARKS: dict[str, dict] = {

    # F001 — Order to Ship Time (hours, lower is better)
    # Story: steady process improvement; fulfillment team cut avg from ~50h to ~43h
    "Order to Ship Time": {
        "direction":       "down",
        "unit":            "h",
        "last_week":       44.2,
        "month_avg":       45.1,
        "three_month_avg": 47.3,
        "year_avg":        49.8,
    },

    # F002 — On-Time Ship Rate (%, higher is better)
    # Story: improving carrier SLA compliance and WMS pick-pack deadlines
    "On-Time Ship Rate": {
        "direction":       "up",
        "unit":            "%",
        "last_week":       95.8,
        "month_avg":       95.2,
        "three_month_avg": 93.7,
        "year_avg":        91.4,
    },

    # F003 — Unshipped Orders Rate (%, lower is better)
    # Story: inventory gaps drove high rates earlier in the year; resolved Q4
    "Unshipped Orders Rate": {
        "direction":       "down",
        "unit":            "%",
        "last_week":       2.4,
        "month_avg":       2.8,
        "three_month_avg": 3.1,
        "year_avg":        3.9,
    },

    # F004 — Transit Time (hours, lower is better)
    # Story: carrier network re-routing reduced avg transit by ~10h YoY
    "Transit Time": {
        "direction":       "down",
        "unit":            "h",
        "last_week":       153.2,
        "month_avg":       155.8,
        "three_month_avg": 158.4,
        "year_avg":        162.1,
    },

    # F005 — On-Time Delivery Rate (%, higher is better)
    # Story: carrier performance and transit window improvements
    "On-Time Delivery Rate": {
        "direction":       "up",
        "unit":            "%",
        "last_week":       96.4,
        "month_avg":       95.9,
        "three_month_avg": 94.2,
        "year_avg":        92.8,
    },

    # F006 — Carrier Mix (% FedEx, neutral/informational)
    # Story: DHL Ecommerce contract added mid-year shifted mix from ~70% FedEx
    "Carrier Mix": {
        "direction":       "neutral",
        "unit":            "%",
        "last_week":       50.9,
        "month_avg":       51.3,
        "three_month_avg": 52.1,
        "year_avg":        54.7,
    },

    # F007 — Shipping Cost per Order ($, lower is better)
    # Story: DHL contract + zone optimisation drove cost reduction
    "Shipping Cost per Order": {
        "direction":       "down",
        "unit":            "$",
        "last_week":       11.12,
        "month_avg":       11.38,
        "three_month_avg": 11.74,
        "year_avg":        12.21,
    },

    # F008 — Cost by Carrier — FedEx avg ($, lower is better)
    # Story: FedEx rate increases offset partially by tier renegotiation
    "Cost by Carrier": {
        "direction":       "down",
        "unit":            "$",
        "last_week":       13.10,
        "month_avg":       13.44,
        "three_month_avg": 13.82,
        "year_avg":        14.31,
    },

    # F009 — Label Lag (hours, lower is better)
    # Story: WMS-to-carrier API integration replaced manual batch uploads in Q3
    "Label Lag": {
        "direction":       "down",
        "unit":            "h",
        "last_week":       2.91,
        "month_avg":       3.24,
        "three_month_avg": 3.87,
        "year_avg":        4.52,
    },

    # F010 — Shipment Match Rate (%, higher is better)
    # Story: data quality initiative ongoing; improving but still below target
    "Shipment Match Rate": {
        "direction":       "up",
        "unit":            "%",
        "last_week":       94.1,
        "month_avg":       93.8,
        "three_month_avg": 92.6,
        "year_avg":        90.3,
    },
}
