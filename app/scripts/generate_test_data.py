"""
generate_test_data.py
Chorus AI Systems — Data Analytics Manager (DAM)

Parameterized synthetic data generator. KPI targets are sampled from
the historical benchmark distributions so each generated week tells a
coherent story that aligns with the benchmark trajectory.

Usage (from app/):
    python scripts/generate_test_data.py                         # current-week defaults
    python scripts/generate_test_data.py --week 2026-03-28 \\
        --n-orders 500 --seed 1001 --progress 0.875

Arguments:
    --week      Week end date (YYYY-MM-DD). Default: 2026-04-04
    --n-orders  Number of Shopify orders.  Default: 500
    --seed      Random seed.               Default: 42
    --progress  0.0 = year-ago performance, 1.0 = current. Default: 1.0
    --out-dir   Output CSV directory.      Default: data/test
"""

import argparse
import csv
import os
import random
from datetime import datetime, timedelta


# ---------------------------------------------------------------------------
# Performance trajectory (year_avg → last_week per historical_kpis.py)
# Each entry: {"year": year_avg, "current": last_week_value, "sigma": weekly_sigma}
# ---------------------------------------------------------------------------
KPI_RANGES = {
    # Control handle          year     current   sigma
    "on_time_ship_rate":   {"year": 0.914,  "current": 0.958,  "sigma": 0.015},
    "unshipped_rate":      {"year": 0.039,  "current": 0.024,  "sigma": 0.003},
    "transit_h":           {"year": 162.1,  "current": 153.2,  "sigma": 3.0},
    "fedex_mix":           {"year": 0.547,  "current": 0.509,  "sigma": 0.015},
    "fedex_cost":          {"year": 14.31,  "current": 13.10,  "sigma": 0.30},
    "dhl_cost":            {"year": 11.20,  "current": 9.94,   "sigma": 0.25},
    "label_lag_h":         {"year": 4.52,   "current": 2.91,   "sigma": 0.30},
    "match_rate":          {"year": 0.903,  "current": 0.941,  "sigma": 0.010},
    # Mean OTS for on-time orders (older = slower internal fulfillment)
    "ots_on_time_mean_h":  {"year": 46.0,   "current": 42.0,   "sigma": 1.5},
}

CLAMPS = {
    "on_time_ship_rate":  (0.82,  0.99),
    "unshipped_rate":     (0.012, 0.08),
    "transit_h":          (130.0, 182.0),
    "fedex_mix":          (0.42,  0.65),
    "fedex_cost":         (10.0,  19.0),
    "dhl_cost":           (7.0,   14.5),
    "label_lag_h":        (0.4,   9.0),
    "match_rate":         (0.85,  0.995),
    "ots_on_time_mean_h": (30.0,  47.5),
}

TRANSIT_WINDOWS = {
    ("FedEx", "FedEx Ground"):                8,
    ("FedEx", "FedEx Home Delivery"):         8,
    ("FedEx", "FedEx Express Saver"):         6,
    ("FedEx", "FedEx 2Day"):                  5,
    ("FedEx", "FedEx Overnight"):             4,
    ("DHL Ecommerce", "DHL Ecommerce Ground"):    8,
    ("DHL Ecommerce", "DHL Ecommerce Expedited"): 6,
}

FEDEX_SERVICES = {
    "FedEx Ground":        {"cost_lo": 8.00,  "cost_hi": 16.00, "window": 8},
    "FedEx Home Delivery": {"cost_lo": 8.00,  "cost_hi": 15.00, "window": 8},
    "FedEx Express Saver": {"cost_lo": 13.00, "cost_hi": 20.00, "window": 6},
    "FedEx 2Day":          {"cost_lo": 19.00, "cost_hi": 27.00, "window": 5},
    "FedEx Overnight":     {"cost_lo": 33.00, "cost_hi": 47.00, "window": 4},
}
DHL_SERVICES = {
    "DHL Ecommerce Ground":    {"cost_lo": 7.00, "cost_hi": 13.00, "window": 8},
    "DHL Ecommerce Expedited": {"cost_lo": 11.00,"cost_hi": 17.00, "window": 6},
}

FEDEX_SVC_WEIGHTS = [50, 20, 15, 10, 5]
DHL_SVC_WEIGHTS   = [70, 30]

US_STATES = ["IL", "CA", "TX", "NY", "FL", "WA", "OH", "GA", "CO", "MI"]
ZIPS      = ["60010", "90210", "75201", "10001", "33101",
             "98101", "44101", "30301", "80201", "48201"]

CANCEL_RATE = 0.018   # ~1.8% of orders cancelled (fixed)


# ---------------------------------------------------------------------------
# Target sampler
# ---------------------------------------------------------------------------

def sample_targets(rng: random.Random, progress: float) -> dict:
    """
    Sample KPI control parameters for the given progress (0.0=year_ago, 1.0=current).
    Returns dict of control handle → float value.
    """
    p = max(0.0, min(1.0, progress))
    targets = {}
    for key, r in KPI_RANGES.items():
        center = r["year"] + p * (r["current"] - r["year"])
        val    = rng.gauss(center, r["sigma"])
        lo, hi = CLAMPS[key]
        targets[key] = max(lo, min(hi, val))
    return targets


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def rand_dt(rng: random.Random, start: datetime, end: datetime) -> datetime:
    delta = int((end - start).total_seconds())
    return start + timedelta(seconds=rng.randint(0, delta))


def fedex_tracking(rng: random.Random) -> str:
    return f"7489{rng.randint(10000000000000, 99999999999999)}"


def dhl_tracking(rng: random.Random) -> str:
    return f"GM{rng.randint(100000000, 999999999)}DE"


# ---------------------------------------------------------------------------
# Main generator
# ---------------------------------------------------------------------------

def generate_week(
    week_date: str,
    n_orders:  int   = 500,
    seed:      int   = 42,
    out_dir:   str   = "data/test",
    progress:  float = 1.0,
) -> dict:
    """
    Generate one week of synthetic CSVs. Returns the sampled target dict.

    Outputs (all written to out_dir/):
        shopify_orders.csv
        tpl_shipments.csv
        fedex_tracking.csv
        dhl_tracking.csv
    """
    rng = random.Random(seed)
    os.makedirs(out_dir, exist_ok=True)

    week_end   = datetime.strptime(week_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
    week_start = (week_end - timedelta(days=6)).replace(hour=0, minute=0, second=0)

    t = sample_targets(rng, progress)

    # ── SHOPIFY ORDERS ───────────────────────────────────────────────────────
    orders = []
    for i in range(1, n_orders + 1):
        created_at   = rand_dt(rng, week_start, week_end)
        state_idx    = rng.randint(0, len(US_STATES) - 1)
        subtotal     = round(rng.uniform(15.0, 295.0), 2)
        ship_fee     = round(rng.uniform(0.0, 18.0), 2)
        is_cancelled = rng.random() < CANCEL_RATE
        cancelled_at = (
            (created_at + timedelta(hours=rng.randint(1, 8))).isoformat()
            if is_cancelled else ""
        )
        promised_ship = (created_at + timedelta(days=2)).date().isoformat()

        orders.append({
            "Name":               f"ORD-{i:04d}",
            "Created At":         created_at.isoformat(),
            "Shipping Country":   "United States",
            "Province":           US_STATES[state_idx],
            "Zip":                ZIPS[state_idx],
            "Subtotal":           subtotal,
            "Total Price":        round(subtotal + ship_fee, 2),
            "Payment Status":     "voided" if is_cancelled else "paid",
            "Fulfillment Status": "unfulfilled" if is_cancelled else "fulfilled",
            "Ship By Date":       promised_ship,
            "Cancelled At":       cancelled_at,
            "Is Cancelled":       "true" if is_cancelled else "false",
        })

    _write_csv(f"{out_dir}/shopify_orders.csv", orders)

    # ── 3PL SHIPMENTS ────────────────────────────────────────────────────────
    cancelled_ids   = {o["Name"] for o in orders if o["Is Cancelled"] == "true"}
    active_orders   = [o for o in orders if o["Name"] not in cancelled_ids]

    # Extra unshipped beyond cancellations (drives F003)
    total_unshipped = max(len(cancelled_ids), int(round(t["unshipped_rate"] * n_orders)))
    n_extra         = total_unshipped - len(cancelled_ids)
    extra_unship    = set(
        o["Name"] for o in rng.sample(active_orders, min(n_extra, len(active_orders)))
    ) if n_extra > 0 else set()

    shipments       = []
    tracking_index  = {}   # tracking_number → (carrier, svc_name, window, cost_mean)

    for order in active_orders:
        if order["Name"] in extra_unship:
            continue

        created_dt   = datetime.fromisoformat(order["Created At"])
        promised_end = datetime.fromisoformat(order["Ship By Date"]) + timedelta(
            hours=23, minutes=59, seconds=59
        )

        # Carrier + service
        is_fedex = rng.random() < t["fedex_mix"]
        if is_fedex:
            carrier  = "FedEx"
            svc_name = rng.choices(list(FEDEX_SERVICES), weights=FEDEX_SVC_WEIGHTS)[0]
            svc      = FEDEX_SERVICES[svc_name]
            cost_mu  = t["fedex_cost"]
            tracking = fedex_tracking(rng)
        else:
            carrier  = "DHL Ecommerce"
            svc_name = rng.choices(list(DHL_SERVICES), weights=DHL_SVC_WEIGHTS)[0]
            svc      = DHL_SERVICES[svc_name]
            cost_mu  = t["dhl_cost"]
            tracking = dhl_tracking(rng)

        tracking_index[tracking] = (carrier, svc_name, svc["window"], cost_mu)

        # shipped_at — controls F001 (ots) and F002 (on-time ship rate)
        # On-time: shipped_at.date() <= promised_date.date()
        # Late:    shipped_at.date() > promised_date.date()
        if rng.random() < t["on_time_ship_rate"]:
            ots_h      = max(3.0, min(47.9, rng.gauss(t["ots_on_time_mean_h"], 5.0)))
            shipped_at = created_dt + timedelta(hours=ots_h)
            # Hard-enforce on-time: ship on or before promised date
            if shipped_at.date() > promised_end.date():
                hours_to_end = (promised_end - created_dt).total_seconds() / 3600
                shipped_at   = created_dt + timedelta(
                    hours=rng.uniform(max(3.0, hours_to_end - 18), hours_to_end)
                )
        else:
            # Hard-enforce late: ship strictly after the promised date (next day+)
            next_day   = promised_end + timedelta(hours=rng.gauss(20, 8))
            shipped_at = max(promised_end + timedelta(hours=1), next_day)

        # first_scan_at — controls F009 (label lag = first_scan - shipped)
        lag_h      = max(0.2, rng.gauss(t["label_lag_h"], 0.5))
        first_scan = shipped_at + timedelta(hours=lag_h)

        # delivered_at — controls F004 (transit = delivered - first_scan) and F005 (on-time delivery)
        # Scale transit target proportionally to service window (Ground=8d baseline)
        window   = svc["window"]
        scale    = window / 8.0
        tr_mean  = t["transit_h"] * scale
        tr_sigma = 20.0 * scale
        tr_hours = max(window * 3.0, rng.gauss(tr_mean, tr_sigma))
        delivered_at = first_scan + timedelta(hours=tr_hours)

        label_created = created_dt + timedelta(hours=rng.uniform(1.0, 8.0))

        shipments.append({
            "Shipment ID":   f"SHIP-{len(shipments) + 1:04d}",
            "Order Ref":     order["Name"],
            "Tracking #":    tracking,
            "Carrier Name":  carrier,
            "Service":       svc_name,
            "Label Created": label_created.isoformat(),
            "Ship Date":     shipped_at.isoformat(),
            "First Scan":    first_scan.isoformat(),
            "Delivery Date": delivered_at.isoformat(),
            "Freight Cost":  "",
        })

    _write_csv(f"{out_dir}/tpl_shipments.csv", shipments)

    # ── CARRIER EXPORTS (FedEx + DHL) ────────────────────────────────────────
    n_drop   = int(round((1.0 - t["match_rate"]) * len(shipments)))
    drop_set = set(rng.sample(range(len(shipments)), min(n_drop, len(shipments))))

    fedex_rows, dhl_rows = [], []

    for idx, s in enumerate(shipments):
        if idx in drop_set:
            continue

        carrier, svc_name, _, cost_mu = tracking_index[s["Tracking #"]]
        if carrier == "FedEx":
            svc_info = FEDEX_SERVICES[svc_name]
        else:
            svc_info = DHL_SERVICES[svc_name]

        cost = round(
            max(svc_info["cost_lo"],
                min(svc_info["cost_hi"], rng.gauss(cost_mu, 1.0))),
            2
        )

        if carrier == "FedEx":
            fedex_rows.append({
                "Tracking Number":      s["Tracking #"],
                "First Scan Date":      s["First Scan"],
                "Delivered Date":       s["Delivery Date"],
                "Billed Weight Charge": cost,
                "Shipment Status":      "DELIVERED",
                "Exception Code":       "",
            })
        else:
            dhl_rows.append({
                "Waybill":         s["Tracking #"],
                "Picked Up":       s["First Scan"],
                "POD Date":        s["Delivery Date"],
                "Charged Amount":  cost,
                "Status":          "Delivered",
                "Exception Notes": "",
            })

    if fedex_rows:
        _write_csv(f"{out_dir}/fedex_tracking.csv", fedex_rows)
    if dhl_rows:
        _write_csv(f"{out_dir}/dhl_tracking.csv", dhl_rows)

    # ── Summary ───────────────────────────────────────────────────────────────
    n_shipped    = len(shipments)
    carrier_recs = len(fedex_rows) + len(dhl_rows)
    fedex_count  = sum(1 for s in shipments if s["Carrier Name"] == "FedEx")
    on_time_count = 0
    for s in shipments:
        ord_idx = int(s["Shipment ID"].split("-")[1]) - 1
        # Find the matching order by Order Ref
        order_ref = s["Order Ref"]
        order = next((o for o in orders if o["Name"] == order_ref), None)
        if order:
            promised_d = datetime.fromisoformat(order["Ship By Date"]).date()
            shipped_d  = datetime.fromisoformat(s["Ship Date"]).date()
            if shipped_d <= promised_d:
                on_time_count += 1

    print(f"\n  {week_date} | seed={seed} | progress={progress:.3f}")
    print(f"  Orders:       {n_orders}  |  Cancelled: {len(cancelled_ids)}  |  Extra unshipped: {n_extra}")
    print(f"  Shipped:      {n_shipped}  |  Carrier records: {carrier_recs}  (match {carrier_recs/n_shipped*100:.1f}%)")
    print(f"  FedEx:        {fedex_count} ({fedex_count/n_shipped*100:.1f}%)  |  DHL: {n_shipped-fedex_count} ({(n_shipped-fedex_count)/n_shipped*100:.1f}%)")
    print(f"  On-time ship: {on_time_count/n_shipped*100:.1f}%  (target {t['on_time_ship_rate']*100:.1f}%)")
    print(f"  Transit tgt:  {t['transit_h']:.1f}h  |  Label lag tgt: {t['label_lag_h']:.2f}h")
    print(f"  FedEx cost:   ${t['fedex_cost']:.2f}  |  DHL cost: ${t['dhl_cost']:.2f}")

    return t


def _write_csv(path: str, rows: list) -> None:
    if not rows:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DAM synthetic data generator")
    parser.add_argument("--week",      default="2026-04-04", help="Week end date YYYY-MM-DD")
    parser.add_argument("--n-orders",  type=int, default=500, help="Number of orders")
    parser.add_argument("--seed",      type=int, default=42,  help="Random seed")
    parser.add_argument("--progress",  type=float, default=1.0,
                        help="0.0 = year-ago performance, 1.0 = current")
    parser.add_argument("--out-dir",   default="data/test", help="Output directory")
    args = parser.parse_args()

    generate_week(
        week_date=args.week,
        n_orders=args.n_orders,
        seed=args.seed,
        out_dir=args.out_dir,
        progress=args.progress,
    )
    print(f"\n  CSVs written to {args.out_dir}/")
