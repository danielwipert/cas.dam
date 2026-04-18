"""
generate_adversarial_data.py
Chorus AI Systems — Data Analytics Manager (DAM)

Generates four categories of adversarial test CSVs with planted errors.
Each category tests that a specific gate catches its assigned failure.

Categories (spec §11.2):
  1. duplicate_order_ids     → Gate 1 hard fail (schema validation)
  2. missing_required_fields → Gate 1 hard fail (required fields absent)
  3. mismatched_timestamps   → Gate 1 + Stage 3 (impossible sequences)
  4. corrupted_tracking      → Stage 2 (match rate drops / fuzzy needed)

Each category gets its own subdirectory under test_data/adversarial/.
Clean base data is copied in, then specific rows are corrupted.

Run: python generate_adversarial_data.py
"""

import csv
import os
import shutil
import random

random.seed(99)

BASE_DIR = "data/test"
ADV_DIR  = os.path.join("data", "test", "adversarial")

SOURCE_FILES = {
    "shopify": os.path.join(BASE_DIR, "shopify_orders.csv"),
    "tpl":     os.path.join(BASE_DIR, "tpl_shipments.csv"),
    "fedex":   os.path.join(BASE_DIR, "fedex_tracking.csv"),
    "dhl":     os.path.join(BASE_DIR, "dhl_tracking.csv"),
}


def read_csv(path: str) -> tuple[list[str], list[dict]]:
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        headers = list(rows[0].keys()) if rows else []
    return headers, rows


def write_csv(path: str, headers: list[str], rows: list[dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def copy_clean(category: str) -> dict[str, tuple[list[str], list[dict]]]:
    """Copy clean test data into the adversarial category dir and return dicts."""
    out_dir = os.path.join(ADV_DIR, category)
    os.makedirs(out_dir, exist_ok=True)
    tables = {}
    for key, src in SOURCE_FILES.items():
        headers, rows = read_csv(src)
        tables[key] = (headers, [row.copy() for row in rows])
    return tables


def save_category(category: str, tables: dict) -> None:
    out_dir = os.path.join(ADV_DIR, category)
    file_map = {
        "shopify": "shopify_orders.csv",
        "tpl":     "tpl_shipments.csv",
        "fedex":   "fedex_tracking.csv",
        "dhl":     "dhl_tracking.csv",
    }
    for key, (headers, rows) in tables.items():
        write_csv(os.path.join(out_dir, file_map[key]), headers, rows)


# ===========================================================================
# CATEGORY 1: DUPLICATE ORDER IDs
# Expected catch: Gate 1 (schema validation — duplicate order_id hard fail)
# ===========================================================================

def generate_duplicate_order_ids():
    """
    Plant 5 duplicate order IDs in the Shopify CSV.
    The same order_id appears in two different rows with different data.
    Gate 1 should detect this and halt with a duplicate_order_id error.
    """
    tables = copy_clean("duplicate_order_ids")
    s_headers, s_rows = tables["shopify"]

    # Duplicate rows 10-14: copy them to the end with same order ID
    duplicated = []
    for i in range(10, 15):
        dupe = s_rows[i].copy()
        # Change subtotal slightly so it's clearly a different row, not identical
        dupe["Subtotal"] = str(round(float(dupe["Subtotal"]) + 5.00, 2))
        duplicated.append(dupe)

    s_rows_with_dupes = s_rows + duplicated
    tables["shopify"] = (s_headers, s_rows_with_dupes)
    save_category("duplicate_order_ids", tables)

    print(f"duplicate_order_ids: {len(s_rows_with_dupes)} rows "
          f"(5 duplicated order IDs planted at rows 10-14)")


# ===========================================================================
# CATEGORY 2: MISSING REQUIRED FIELDS
# Expected catch: Gate 1 (required field absent — hard fail)
# ===========================================================================

def generate_missing_required_fields():
    """
    Plant three types of missing-field errors:
      - 3 Shopify rows with blank order_id (Name column)
      - 3 3PL rows with blank tracking_number
      - 3 FedEx rows with blank tracking number
    Gate 1 schema validation must catch these as hard failures.
    """
    tables = copy_clean("missing_required_fields")
    s_headers, s_rows = tables["shopify"]
    t_headers, t_rows = tables["tpl"]
    f_headers, f_rows = tables["fedex"]

    # Blank order IDs in Shopify rows 20-22
    for i in [20, 21, 22]:
        s_rows[i]["Name"] = ""

    # Blank tracking numbers in 3PL rows 5-7
    for i in [5, 6, 7]:
        t_rows[i]["Tracking #"] = ""

    # Blank tracking numbers in FedEx rows 2-4
    for i in [2, 3, 4]:
        f_rows[i]["Tracking Number"] = ""

    tables["shopify"] = (s_headers, s_rows)
    tables["tpl"]     = (t_headers, t_rows)
    tables["fedex"]   = (f_headers, f_rows)
    save_category("missing_required_fields", tables)

    print("missing_required_fields: 9 blank required fields planted "
          "(3 order IDs, 3 3PL tracking#, 3 FedEx tracking#)")


# ===========================================================================
# CATEGORY 3: MISMATCHED TIMESTAMPS
# Expected catch: Gate 1 (impossible timestamp sequences)
# ===========================================================================

def generate_mismatched_timestamps():
    """
    Plant three types of timestamp anomalies:
      - delivered_at BEFORE first_scan_at (physically impossible)
      - shipped_at BEFORE label_created_at (label created after ship?)
      - A far-future date (year 2099) suggesting a data entry error
    Gate 1 or Stage 3 KPI computation should detect these.
    """
    tables = copy_clean("mismatched_timestamps")
    t_headers, t_rows = tables["tpl"]

    # Rows 0-2: delivered before first scan (swap the two timestamps)
    for i in [0, 1, 2]:
        first_scan  = t_rows[i].get("First Scan", "")
        delivery    = t_rows[i].get("Delivery Date", "")
        if first_scan and delivery:
            t_rows[i]["First Scan"]    = delivery    # delivered timestamp in scan field
            t_rows[i]["Delivery Date"] = first_scan  # scan timestamp in delivery field

    # Rows 3-5: shipped before label created (swap label_created and ship_date)
    for i in [3, 4, 5]:
        label   = t_rows[i].get("Label Created", "")
        shipped = t_rows[i].get("Ship Date", "")
        if label and shipped:
            t_rows[i]["Label Created"] = shipped
            t_rows[i]["Ship Date"]     = label

    # Row 6: far-future delivery date (data entry error)
    if len(t_rows) > 6:
        t_rows[6]["Delivery Date"] = "2099-12-31T00:00:00"

    tables["tpl"] = (t_headers, t_rows)
    save_category("mismatched_timestamps", tables)

    print("mismatched_timestamps: 7 timestamp anomalies planted "
          "(3 delivered-before-scanned, 3 shipped-before-labelled, 1 far-future date)")


# ===========================================================================
# CATEGORY 4: CORRUPTED TRACKING NUMBERS
# Expected catch: Stage 2 match rate drops; fuzzy matching required
# ===========================================================================

def generate_corrupted_tracking():
    """
    Plant three types of tracking number corruptions in the 3PL file.
    The carrier files have the correct tracking numbers — so the 3PL
    tracking numbers won't match exactly, forcing Stage 2 fuzzy matching.

    Corruption types:
      - Transposed digits (e.g. ...1234... → ...2134...)
      - Stripped prefix  (e.g. GM123... → 123...)
      - Extra character  (e.g. 1Z999... → 1Z9990...)

    10 corrupted entries planted — enough to significantly drop match rate.
    Stage 2 should attempt fuzzy matching; if confidence < 0.90 they stay
    unmatched, causing match rate to drop and triggering a warning/halt.
    """
    tables = copy_clean("corrupted_tracking")
    t_headers, t_rows = tables["tpl"]

    corrupted_count = 0

    # Transposed digits — rows 0-3 (FedEx-style long numerics)
    fedex_rows = [r for r in t_rows if "FedEx" in r.get("Carrier Name", "")][:4]
    for row in fedex_rows:
        orig = row["Tracking #"]
        if len(orig) > 8:
            # Swap two adjacent digits in the middle
            mid = len(orig) // 2
            lst = list(orig)
            lst[mid], lst[mid+1] = lst[mid+1], lst[mid]
            row["Tracking #"] = "".join(lst)
            corrupted_count += 1

    # Stripped prefix — rows 0-3 of DHL (strip leading "GM")
    dhl_rows = [r for r in t_rows if "DHL" in r.get("Carrier Name", "")][:3]
    for row in dhl_rows:
        orig = row["Tracking #"]
        if orig.startswith("GM"):
            row["Tracking #"] = orig[2:]   # strip "GM" prefix
            corrupted_count += 1

    # Extra character injected — rows 4-6 of FedEx
    fedex_rows2 = [r for r in t_rows if "FedEx" in r.get("Carrier Name", "")][4:7]
    for row in fedex_rows2:
        orig = row["Tracking #"]
        if len(orig) > 4:
            # Insert a "0" after the 4th character
            row["Tracking #"] = orig[:4] + "0" + orig[4:]
            corrupted_count += 1

    tables["tpl"] = (t_headers, t_rows)
    save_category("corrupted_tracking", tables)

    print(f"corrupted_tracking: {corrupted_count} tracking numbers corrupted "
          f"(transposed digits, stripped prefix, extra character)")


# ===========================================================================
# MAIN
# ===========================================================================

if __name__ == "__main__":
    # Verify base test data exists first
    for key, path in SOURCE_FILES.items():
        if not os.path.exists(path):
            print(f"ERROR: Base test data not found at {path}")
            print("Run generate_test_data.py first.")
            raise SystemExit(1)

    print(f"Generating adversarial test data in {ADV_DIR}/\n")

    generate_duplicate_order_ids()
    generate_missing_required_fields()
    generate_mismatched_timestamps()
    generate_corrupted_tracking()

    print()
    print("Adversarial test data generated:")
    for category in ["duplicate_order_ids", "missing_required_fields",
                     "mismatched_timestamps", "corrupted_tracking"]:
        cat_dir = os.path.join(ADV_DIR, category)
        files   = os.listdir(cat_dir) if os.path.exists(cat_dir) else []
        print(f"  {category}/  ({len(files)} files)")

    print()
    print("Run the adversarial test suite:")
    print("  from meta_governance import AdversarialRunner")
    print("  runner = AdversarialRunner()")
    print("  results = runner.run_all()")
    print("  runner.print_report(results)")
