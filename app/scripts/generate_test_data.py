"""
generate_test_data.py
Chorus AI Systems — Data Analytics Manager (DAM)

Generates synthetic CSV test data for Shopify, 3PL, FedEx, and DHL.

Design goals:
- Column names intentionally differ from canonical schema fields
  (Stage 1's LLM must figure out the mapping — that's the point)
- ~150 orders, ~145 shipments, ~140 carrier records
- Realistic KPI spread: mostly green, a few yellow signals
- A handful of edge cases: late ships, label lag outlier, unshipped orders,
  unmatched carrier records, one cancelled order
- All dates within a realistic 7-day order window (carrier data covers 14 days)

Run:  python generate_test_data.py
Output: test_data/shopify_orders.csv
        test_data/tpl_shipments.csv
        test_data/fedex_tracking.csv
        test_data/dhl_tracking.csv
"""

import csv
import os
import random
from datetime import datetime, timedelta

random.seed(42)   # reproducible

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

OUTPUT_DIR    = "data/test"
WEEK_START    = datetime(2026, 3, 28, 0, 0, 0)   # Saturday — start of order window
WEEK_END      = datetime(2026, 4, 4, 23, 59, 59)  # Friday — end of order window
N_ORDERS      = 150
FEDEX_SHARE   = 0.62   # ~62% of shipments via FedEx, rest DHL

US_STATES = ["IL", "CA", "TX", "NY", "FL", "WA", "OH", "GA", "CO", "MI"]
ZIPS      = ["60010", "90210", "75201", "10001", "33101",
             "98101", "44101", "30301", "80201", "48201"]

FEDEX_SERVICES = {
    "FedEx Ground":       {"days": 5, "cost_range": (7.50, 11.00)},
    "FedEx Home Delivery":{"days": 5, "cost_range": (7.50, 11.00)},
    "FedEx Express Saver":{"days": 3, "cost_range": (12.00, 17.00)},
    "FedEx 2Day":         {"days": 2, "cost_range": (18.00, 24.00)},
    "FedEx Overnight":    {"days": 1, "cost_range": (32.00, 45.00)},
}
DHL_SERVICES = {
    "DHL Ecommerce Ground":     {"days": 5, "cost_range": (6.50, 10.00)},
    "DHL Ecommerce Expedited":  {"days": 3, "cost_range": (11.00, 16.00)},
}

def rand_dt(start: datetime, end: datetime) -> datetime:
    delta = int((end - start).total_seconds())
    return start + timedelta(seconds=random.randint(0, delta))

def business_days_later(dt: datetime, days: int) -> datetime:
    """Add business days (skip Sat/Sun)."""
    added = 0
    current = dt
    while added < days:
        current += timedelta(days=1)
        if current.weekday() < 5:
            added += 1
    # Add a few hours of variability
    current += timedelta(hours=random.randint(8, 18))
    return current

def fedex_tracking() -> str:
    return f"7489{random.randint(10000000000000, 99999999999999)}"

def dhl_tracking() -> str:
    return f"GM{random.randint(100000000, 999999999)}DE"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# GENERATE ORDERS
# ---------------------------------------------------------------------------

orders = []
for i in range(1, N_ORDERS + 1):
    order_id      = f"ORD-{i:04d}"
    created_at    = rand_dt(WEEK_START, WEEK_END)
    state_idx     = random.randint(0, len(US_STATES) - 1)
    subtotal      = round(random.uniform(15.0, 250.0), 2)
    shipping_fee  = round(random.uniform(0.0, 15.0), 2)
    total         = round(subtotal + shipping_fee, 2)

    # ~2% cancelled
    is_cancelled  = (i <= 3)
    cancelled_at  = (created_at + timedelta(hours=random.randint(1, 6))).isoformat() if is_cancelled else ""

    # promised ship date: next business day for most, same day for expedited
    promised_ship = business_days_later(created_at, 1).date().isoformat()

    fin_status    = "paid" if not is_cancelled else "voided"
    ful_status    = "unfulfilled" if is_cancelled else "fulfilled"

    orders.append({
        # --- deliberately non-canonical column names ---
        "Name":             order_id,
        "Created At":       created_at.isoformat(),
        "Shipping Country": "United States",
        "Province":         US_STATES[state_idx],
        "Zip":              ZIPS[state_idx],
        "Subtotal":         subtotal,
        "Total Price":      total,
        "Payment Status":   fin_status,
        "Fulfillment Status": ful_status,
        "Ship By Date":     promised_ship,
        "Cancelled At":     cancelled_at,
        "Is Cancelled":     "true" if is_cancelled else "false",
    })

with open(f"{OUTPUT_DIR}/shopify_orders.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=list(orders[0].keys()))
    writer.writeheader()
    writer.writerows(orders)

print(f"shopify_orders.csv — {len(orders)} rows")

# ---------------------------------------------------------------------------
# GENERATE 3PL SHIPMENTS
# ---------------------------------------------------------------------------
# 147 of 150 orders get shipped (3 cancelled = no shipment)
# Of those, 5 are "late ship" (shipped 2 days after promised)
# 3 have a label lag outlier (>12 hours between label and first scan)

shipped_order_ids = [o["Name"] for o in orders if o["Is Cancelled"] == "false"]

shipments = []
tracking_to_service = {}   # tracking_number → (carrier, service_level, days)

for idx, order_id in enumerate(shipped_order_ids):
    shipment_id = f"SHIP-{idx+1:04d}"

    # Find the order to get created_at and promised_ship_date
    order = next(o for o in orders if o["Name"] == order_id)
    order_created = datetime.fromisoformat(order["Created At"])
    promised_ship_str = order["Ship By Date"]
    promised_ship = datetime.fromisoformat(promised_ship_str + "T12:00:00")

    # Assign carrier + service level
    is_fedex = (random.random() < FEDEX_SHARE)
    if is_fedex:
        service_level = random.choices(
            list(FEDEX_SERVICES.keys()),
            weights=[50, 20, 15, 10, 5]
        )[0]
        carrier = "FedEx"
        tracking = fedex_tracking()
        svc_info = FEDEX_SERVICES[service_level]
    else:
        service_level = random.choices(
            list(DHL_SERVICES.keys()),
            weights=[70, 30]
        )[0]
        carrier = "DHL Ecommerce"
        tracking = dhl_tracking()
        svc_info = DHL_SERVICES[service_level]

    tracking_to_service[tracking] = (carrier, service_level, svc_info["days"], svc_info["cost_range"])

    # Label created: same day or next morning after order
    label_created = order_created + timedelta(hours=random.randint(2, 10))

    # Shipped at: on time for most, late for idx 10-14 (test late ship KPI)
    is_late_ship = (10 <= idx <= 14)
    if is_late_ship:
        shipped_at = promised_ship + timedelta(hours=random.randint(25, 50))
    else:
        shipped_at = promised_ship - timedelta(hours=random.randint(1, 18))

    # First scan: label lag outlier for idx 20-22 (>12 hours)
    is_label_lag = (20 <= idx <= 22)
    if is_label_lag:
        first_scan = shipped_at + timedelta(hours=random.randint(14, 20))
    else:
        first_scan = shipped_at + timedelta(hours=random.randint(1, 4))

    # Delivered at: on time for most, 1 day late for idx 30-34
    is_late_delivery = (30 <= idx <= 34)
    transit_days = svc_info["days"]
    if is_late_delivery:
        delivered_at = business_days_later(first_scan, transit_days + 1)
    else:
        delivered_at = business_days_later(first_scan, transit_days)

    # Shipping cost: populated by Stage 2 from carrier data — leave blank in 3PL export
    cost = ""

    shipments.append({
        # --- deliberately non-canonical column names ---
        "Shipment ID":    shipment_id,
        "Order Ref":      order_id,
        "Tracking #":     tracking,
        "Carrier Name":   carrier,
        "Service":        service_level,
        "Label Created":  label_created.isoformat(),
        "Ship Date":      shipped_at.isoformat(),
        "First Scan":     first_scan.isoformat(),
        "Delivery Date":  delivered_at.isoformat(),
        "Freight Cost":   cost,   # blank — Stage 2 fills from carrier
    })

with open(f"{OUTPUT_DIR}/tpl_shipments.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=list(shipments[0].keys()))
    writer.writeheader()
    writer.writerows(shipments)

print(f"tpl_shipments.csv   — {len(shipments)} rows")

# ---------------------------------------------------------------------------
# GENERATE CARRIER EXPORTS (FedEx + DHL)
# ---------------------------------------------------------------------------
# ~140 of 147 shipments get a carrier record (7 missing = unmatched)
# This tests the Shipment Match Rate KPI

fedex_rows = []
dhl_rows   = []

unmatched_indices = set(random.sample(range(len(shipments)), 7))

for idx, s in enumerate(shipments):
    if idx in unmatched_indices:
        continue   # no carrier record for these — tests match rate

    carrier = s["Carrier Name"]
    tracking = s["Tracking #"]
    _, service_level, _, cost_range = tracking_to_service[tracking]
    cost = round(random.uniform(*cost_range), 2)

    first_scan = datetime.fromisoformat(s["First Scan"])
    delivered_at = datetime.fromisoformat(s["Delivery Date"])

    if carrier == "FedEx":
        fedex_rows.append({
            # --- FedEx export column names ---
            "Tracking Number":    tracking,
            "First Scan Date":    first_scan.isoformat(),
            "Delivered Date":     delivered_at.isoformat(),
            "Billed Weight Charge": cost,
            "Shipment Status":    "DELIVERED",
            "Exception Code":     "",
        })
    else:
        dhl_rows.append({
            # --- DHL export column names ---
            "Waybill":            tracking,
            "Picked Up":          first_scan.isoformat(),
            "POD Date":           delivered_at.isoformat(),
            "Charged Amount":     cost,
            "Status":             "Delivered",
            "Exception Notes":    "",
        })

with open(f"{OUTPUT_DIR}/fedex_tracking.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=list(fedex_rows[0].keys()))
    writer.writeheader()
    writer.writerows(fedex_rows)

with open(f"{OUTPUT_DIR}/dhl_tracking.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=list(dhl_rows[0].keys()))
    writer.writeheader()
    writer.writerows(dhl_rows)

print(f"fedex_tracking.csv  — {len(fedex_rows)} rows")
print(f"dhl_tracking.csv    — {len(dhl_rows)} rows")

# ---------------------------------------------------------------------------
# PRINT EXPECTED KPI SUMMARY  (so we can verify Stage 3 output later)
# ---------------------------------------------------------------------------

total_shipped = len(shipments)
cancelled_count = sum(1 for o in orders if o["Is Cancelled"] == "true")
unshipped_count = N_ORDERS - total_shipped   # only cancelled orders are unshipped here
carrier_records = len(fedex_rows) + len(dhl_rows)
unmatched_count = total_shipped - carrier_records

# On-time ship rate
late_ships = sum(1 for idx, s in enumerate(shipments) if 10 <= idx <= 14)
on_time_ship_rate = (total_shipped - late_ships) / total_shipped

# On-time delivery
late_deliveries = sum(1 for idx, s in enumerate(shipments) if 30 <= idx <= 34)
on_time_delivery_rate = (total_shipped - late_deliveries) / total_shipped

# Shipment match rate
match_rate = carrier_records / total_shipped

# Carrier split
fedex_count = len(fedex_rows)
dhl_count   = len(dhl_rows)

print()
print("=" * 50)
print("EXPECTED KPI BASELINE  (verify against Stage 3)")
print("=" * 50)
print(f"Total orders:             {N_ORDERS}")
print(f"Cancelled orders:         {cancelled_count}")
print(f"Unshipped rate:           {unshipped_count/N_ORDERS*100:.1f}%  (target < 1%)")
print(f"Total shipments:          {total_shipped}")
print(f"On-time ship rate:        {on_time_ship_rate*100:.1f}%  (target >= 98%)")
print(f"On-time delivery rate:    {on_time_delivery_rate*100:.1f}%  (target >= 98%)")
print(f"Carrier records:          {carrier_records}")
print(f"Unmatched shipments:      {unmatched_count}")
print(f"Shipment match rate:      {match_rate*100:.1f}%  (target >= 99.8%)")
print(f"FedEx shipments:          {fedex_count}  ({fedex_count/total_shipped*100:.0f}%)")
print(f"DHL shipments:            {dhl_count}  ({dhl_count/total_shipped*100:.0f}%)")
print()
print("Edge cases embedded:")
print("  Late ships (idx 10-14):       5  → On-Time Ship Rate ~96.6%  [YELLOW]")
print("  Late deliveries (idx 30-34):  5  → On-Time Delivery Rate ~96.6%  [YELLOW]")
print("  Label lag outliers (idx 20-22): 3  → Label Lag KPI affected")
print("  No carrier records:           7  → Match Rate ~95.2%  [YELLOW]")
print("  Cancelled / unshipped:        3  → Unshipped Rate 2.0%  [RED]")
