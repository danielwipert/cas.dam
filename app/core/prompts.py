"""
prompts.py
Chorus AI Systems — Data Analytics Manager (DAM)

Prompt templates for all LLM stages.

Each prompt is a Python function that accepts the stage's input data
and returns a fully-formed string ready to send to the API.

Stages covered:
  build_stage1_prompt()  — field mapping (Llama 3.3 70B)
  build_stage2_exact_prompt()   — exact join proposal (Llama 3.3 70B)
  build_stage2_fuzzy_prompt()   — fuzzy reconciliation (Llama 3.3 70B)
  build_stage3_prompt()  — KPI computation (Llama 3.3 70B)
  build_stage4_generation_prompt()   — insight generation (DeepSeek V3)
  build_stage4_verification_prompt() — citation verification (Qwen2.5-7B)

Design rules applied to every prompt:
  - Output must be JSON only — no preamble, no markdown fences
  - Schema is defined explicitly in the prompt so the model knows
    exactly what fields to emit
  - Constitutional constraints stated plainly (what the model must NOT do)
  - Retry variant included for each stage (adds failure context)
"""

import json
from typing import Optional


# ---------------------------------------------------------------------------
# STAGE 1: FIELD MAPPING
# ---------------------------------------------------------------------------

STAGE1_SYSTEM = """You are a data engineer performing CSV field mapping.
You will be given the column headers and sample rows from one or more CSV files,
along with the canonical schema each file must map to.

Your job is to propose a mapping from every source column to the correct
canonical field. You must output valid JSON and nothing else.
No explanations. No markdown. No preamble. Only the JSON object."""


def build_stage1_prompt(
    source_file: str,
    source_columns: list[str],
    sample_rows: list[dict],
    canonical_fields: list[str],
    canonical_descriptions: dict[str, str],
    retry_context: Optional[str] = None
) -> str:
    """
    Prompt for Stage 1: map source CSV columns to canonical schema fields.

    Args:
        source_file: filename (e.g. "shopify_orders.csv")
        source_columns: list of column headers from the CSV
        sample_rows: first 3 rows as list of dicts (column → value)
        canonical_fields: list of canonical field names the LLM must map to
        canonical_descriptions: dict of field_name → plain-English description
        retry_context: if this is a retry, include the previous failure reason
    """
    sample_json = json.dumps(sample_rows[:3], indent=2, default=str)
    canonical_block = "\n".join(
        f'  "{f}": "{canonical_descriptions.get(f, "")}"'
        for f in canonical_fields
    )

    retry_block = ""
    if retry_context:
        retry_block = f"""
RETRY CONTEXT — your previous mapping attempt failed for this reason:
{retry_context}

Fix only the fields mentioned above. Do not change mappings that were accepted.
"""

    return f"""You are mapping source CSV columns to a canonical data schema.

SOURCE FILE: {source_file}

SOURCE COLUMNS:
{json.dumps(source_columns)}

SAMPLE DATA (first 3 rows):
{sample_json}

CANONICAL SCHEMA — map each source column to one of these fields:
{{
{canonical_block}
}}
{retry_block}
RULES:
- Every source column must appear in your output, even if you cannot map it
- Set canonical_field to null if a source column has no canonical equivalent
- Set ambiguous to true if you are uncertain about the mapping
- mapping_confidence must be between 0.0 and 1.0
- DO NOT invent canonical fields that are not in the list above
- DO NOT skip source columns

OUTPUT FORMAT — return a JSON object with this exact structure:
{{
  "source_file": "{source_file}",
  "mappings": [
    {{
      "source_column": "<exact source column name>",
      "canonical_field": "<canonical field name or null>",
      "mapping_confidence": <float 0.0-1.0>,
      "ambiguous": <true or false>,
      "ambiguity_note": "<explanation if ambiguous, else null>"
    }}
  ]
}}

Return only the JSON object. No other text."""


# Canonical field descriptions passed to Stage 1 per source file

SHOPIFY_CANONICAL_DESCRIPTIONS = {
    "order_id":                   "Unique order identifier",
    "order_created_at":           "ISO datetime when order was placed",
    "destination_country":        "Country the order ships to",
    "destination_state":          "State or province",
    "destination_zip":            "Postal or ZIP code",
    "order_subtotal":             "Pre-shipping order value",
    "order_total":                "Total including shipping",
    "financial_status":           "Payment status: paid, pending, voided, refunded, etc.",
    "fulfillment_status_shopify": "Fulfillment status: fulfilled, unfulfilled, partial",
    "promised_ship_date":         "Date by which the order must ship",
    "cancelled_at":               "ISO datetime of cancellation if applicable",
    "is_cancelled":               "Boolean — true if order was cancelled",
}

TPL_CANONICAL_DESCRIPTIONS = {
    "shipment_id":          "Unique shipment identifier",
    "order_id":             "Order this shipment fulfils (foreign key)",
    "tracking_number":      "Carrier tracking number",
    "carrier":              "Carrier name (FedEx, DHL Ecommerce, etc.)",
    "service_level":        "Specific service (FedEx Ground, DHL Ecommerce Expedited, etc.)",
    "label_created_at":     "ISO datetime when shipping label was generated",
    "shipped_at":           "ISO datetime when package left the warehouse",
    "first_scan_at":        "ISO datetime of first carrier scan",
    "delivered_at":         "ISO datetime of confirmed delivery",
    "shipping_cost_actual": "Actual freight cost — may be blank (filled from carrier data)",
}

FEDEX_CANONICAL_DESCRIPTIONS = {
    "carrier":                   "Always 'FedEx' for this file",
    "tracking_number":           "FedEx tracking number",
    "first_scan_at":             "ISO datetime of first carrier scan",
    "delivered_at":              "ISO datetime of delivery",
    "shipping_cost_actual":      "Billed freight charge",
    "carrier_status_normalized": "Delivery status (e.g. DELIVERED)",
    "exception_metadata":        "Exception code or notes if any",
}

DHL_CANONICAL_DESCRIPTIONS = {
    "carrier":                   "Always 'DHL Ecommerce' for this file",
    "tracking_number":           "DHL waybill / tracking number",
    "first_scan_at":             "ISO datetime of pickup scan",
    "delivered_at":              "ISO datetime of proof-of-delivery",
    "shipping_cost_actual":      "Charged freight amount",
    "carrier_status_normalized": "Delivery status (e.g. Delivered)",
    "exception_metadata":        "Exception notes if any",
}


# ---------------------------------------------------------------------------
# STAGE 2: EXACT JOIN PROPOSAL
# ---------------------------------------------------------------------------

STAGE2_SYSTEM = """You are a data reconciliation analyst.
You will be given canonical order, shipment, and carrier tables.
Your job is to propose joins between records and output valid JSON only.
No explanations. No markdown. No preamble. Only the JSON object."""


def build_stage2_exact_prompt(
    unmatched_shipments: list[dict],
    canonical_orders_sample: list[dict],
    retry_context: Optional[str] = None
) -> str:
    """
    Prompt for Stage 2 Phase 1: propose exact joins.
    Python will perform the actual joins — this prompt asks the LLM
    to confirm the join keys and flag any obvious anomalies it sees
    before Python executes.

    Args:
        unmatched_shipments: shipments not yet joined to an order
        canonical_orders_sample: sample of order records for context
        retry_context: failure reason if retrying
    """
    retry_block = f"\nRETRY CONTEXT: {retry_context}\n" if retry_context else ""

    return f"""You are reviewing shipment and order records before a join operation.

TASK: Confirm the correct join keys and flag any anomalies you observe
in the data before the join is executed.
{retry_block}
SHIPMENTS AWAITING JOIN (sample — up to 10 shown):
{json.dumps(unmatched_shipments[:10], indent=2, default=str)}

ORDERS SAMPLE (for context):
{json.dumps(canonical_orders_sample[:10], indent=2, default=str)}

RULES:
- The join key between shipments and orders is: order_id
- The join key between shipments and carrier records is: tracking_number
- Flag any shipment where order_id does not appear to be a valid order ID format
- Flag any tracking number that looks malformed
- DO NOT invent matches. DO NOT guess. Only flag genuine anomalies.

OUTPUT FORMAT:
{{
  "join_key_order": "order_id",
  "join_key_carrier": "tracking_number",
  "anomalies": [
    {{
      "shipment_id": "<id>",
      "anomaly_type": "<description>",
      "field_affected": "<field name>"
    }}
  ],
  "anomaly_count": <integer>
}}

Return only the JSON object. No other text."""


def build_stage2_fuzzy_prompt(
    unmatched_shipments: list[dict],
    unmatched_carrier_records: list[dict],
    retry_context: Optional[str] = None
) -> str:
    """
    Prompt for Stage 2 Phase 2: fuzzy reconciliation of unmatched records.
    LLM proposes matches; Python validates each one before accepting.

    Args:
        unmatched_shipments: shipments with no carrier match after exact join
        unmatched_carrier_records: carrier records with no matching shipment
        retry_context: failure reason if retrying
    """
    retry_block = f"\nRETRY CONTEXT: {retry_context}\n" if retry_context else ""

    return f"""You are a data reconciliation analyst performing fuzzy matching.

Exact joins have already been attempted and failed for these records.
Your job is to find likely matches by analyzing the data carefully.
{retry_block}
UNMATCHED SHIPMENTS (from 3PL — these need a carrier record):
{json.dumps(unmatched_shipments, indent=2, default=str)}

UNMATCHED CARRIER RECORDS (no matching shipment found):
{json.dumps(unmatched_carrier_records, indent=2, default=str)}

TASK: For each unmatched shipment, propose the best matching carrier record
if one exists. Common reasons exact matching fails:
  - Tracking number prefix stripped (e.g. "1Z" removed)
  - Transposed digits in the middle of a tracking number
  - Leading zeros added or removed
  - Carrier system added/removed suffix characters

RULES:
- Only propose a match if you are genuinely confident (>= 0.90)
- If no good match exists for a shipment, set matched_carrier_tracking to null
- Explain your reasoning concisely in fuzzy_match_rationale
- DO NOT fabricate tracking numbers
- DO NOT propose the same carrier record for two different shipments
- confidence must be between 0.0 and 1.0

OUTPUT FORMAT:
{{
  "fuzzy_proposals": [
    {{
      "shipment_id": "<3PL shipment ID>",
      "shipment_tracking": "<tracking number from 3PL>",
      "matched_carrier_tracking": "<carrier tracking number or null>",
      "fuzzy_match_confidence": <float 0.0-1.0>,
      "fuzzy_match_rationale": "<brief explanation of why these match>"
    }}
  ]
}}

Return only the JSON object. No other text."""


# ---------------------------------------------------------------------------
# STAGE 3: KPI COMPUTATION
# ---------------------------------------------------------------------------

STAGE3_SYSTEM = """You are a data analyst computing operational KPIs from
reconciled ecommerce data. You will receive a dataset and compute exactly
10 KPIs. Output valid JSON only. No explanations. No markdown. No preamble."""


def build_stage3_prompt(
    reconciliation_summary: dict,
    dataset_stats: dict,
    kpi_definitions: list[dict],
    thresholds: dict,
    week_date: str,
    prior_week_summary: Optional[dict] = None,
    retry_context: Optional[str] = None
) -> str:
    """
    Prompt for Stage 3: compute all 10 KPIs from reconciled data.
    NOTE: Python will independently recompute every value — this is
    the LLM demonstrating analytical judgment, not the source of truth.

    Args:
        reconciliation_summary: aggregated stats from the reconciled dataset
        dataset_stats: row counts, date ranges, carrier breakdown
        kpi_definitions: list of {fact_id, kpi_name, domain, formula_description}
        thresholds: dict of kpi_name → {green, yellow, red} threshold rules
        week_date: ISO date string for this report week
        prior_week_summary: prior week KPI values for WoW delta (optional)
        retry_context: failure reason if retrying
    """
    prior_block = ""
    if prior_week_summary:
        prior_block = f"""
PRIOR WEEK KPI VALUES (for week-over-week delta):
{json.dumps(prior_week_summary, indent=2, default=str)}
"""
    retry_block = f"\nRETRY CONTEXT: {retry_context}\n" if retry_context else ""

    kpi_block = json.dumps(kpi_definitions, indent=2)
    threshold_block = json.dumps(thresholds, indent=2)

    return f"""You are computing 10 operational KPIs for a weekly ecommerce report.

WEEK DATE: {week_date}

DATASET STATISTICS:
{json.dumps(dataset_stats, indent=2, default=str)}

RECONCILIATION SUMMARY:
{json.dumps(reconciliation_summary, indent=2, default=str)}
{prior_block}{retry_block}
KPI DEFINITIONS — compute each one:
{kpi_block}

THRESHOLD RULES — classify each KPI as green, yellow, red, or informational:
{threshold_block}

RULES:
- Compute every KPI listed. Do not skip any.
- Use only the data provided above. Do not invent numbers.
- For rates, express as a decimal between 0.0 and 1.0 (e.g. 0.966 not 96.6)
- For durations (Order to Ship Time, Transit Time, Label Lag), express in hours
- For cost KPIs, express in USD
- If a KPI cannot be computed due to missing data, set llm_value to null
  and explain in data_provenance.exclusions
- python_value and final_value must both be set to null —
  Python will fill these in. DO NOT guess what Python will compute.
- llm_python_match must be set to null — Python sets this after recomputation
- python_verified must always be false — Python sets this to true after verification

OUTPUT FORMAT — return a JSON object with this exact structure:
{{
  "factlist": [
    {{
      "fact_id": "F001",
      "domain": "<domain string>",
      "kpi_name": "<exact KPI name from definitions>",
      "llm_value": <computed float or null>,
      "python_value": null,
      "final_value": null,
      "threshold_status": "<green|yellow|red|informational>",
      "llm_python_match": null,
      "prior_week_value": <float or null>,
      "wow_delta": <float or null — compute if prior week available>,
      "data_provenance": {{
        "source_tables": ["<table names used>"],
        "row_count": <integer>,
        "formula_used": "<plain English formula description>",
        "date_range": "<ISO date range string>",
        "exclusions": "<any excluded records or null>"
      }},
      "week_date": "{week_date}",
      "python_verified": false
    }}
  ]
}}

The factlist must contain exactly 10 entries, one per KPI definition.
Return only the JSON object. No other text."""


# KPI definitions passed to Stage 3

KPI_DEFINITIONS = [
    {
        "fact_id": "F001",
        "domain": "fulfillment",
        "kpi_name": "Order to Ship Time",
        "formula_description": "Average hours from order_created_at to shipped_at, across all shipped (non-cancelled) orders",
    },
    {
        "fact_id": "F002",
        "domain": "fulfillment",
        "kpi_name": "On-Time Ship Rate",
        "formula_description": "Percentage of orders where shipped_at <= promised_ship_date, expressed as decimal (e.g. 0.966). Only include orders that have both shipped_at and promised_ship_date.",
    },
    {
        "fact_id": "F003",
        "domain": "fulfillment",
        "kpi_name": "Unshipped Orders Rate",
        "formula_description": "Percentage of total orders (including cancelled) with no shipment record, expressed as decimal. Cancelled orders count as unshipped.",
    },
    {
        "fact_id": "F004",
        "domain": "carrier_performance",
        "kpi_name": "Transit Time",
        "formula_description": "Average hours from first_scan_at to delivered_at, across all delivered shipments",
    },
    {
        "fact_id": "F005",
        "domain": "carrier_performance",
        "kpi_name": "On-Time Delivery Rate",
        "formula_description": "Percentage of delivered shipments where actual transit time <= expected transit window for that carrier + service level. Express as decimal. Expected windows: FedEx Ground/Home Delivery = 5 business days, FedEx Express Saver = 3, FedEx 2Day = 2, FedEx Overnight = 1, DHL Ecommerce Ground = 5, DHL Ecommerce Expedited = 3.",
    },
    {
        "fact_id": "F006",
        "domain": "carrier_performance",
        "kpi_name": "Carrier Mix",
        "formula_description": "Percentage split of total shipments between FedEx and DHL Ecommerce. Express as two decimals that sum to 1.0. Report as informational — no threshold applied.",
    },
    {
        "fact_id": "F007",
        "domain": "cost",
        "kpi_name": "Shipping Cost per Order",
        "formula_description": "Total shipping cost (from carrier records) divided by total shipped orders. If fewer than 4 weeks of history exist, mark as informational.",
    },
    {
        "fact_id": "F008",
        "domain": "cost",
        "kpi_name": "Cost by Carrier",
        "formula_description": "Average shipping cost broken down by carrier (FedEx average, DHL average). Express as a dict: {FedEx: float, DHL Ecommerce: float}. If fewer than 4 weeks of history exist, mark as informational.",
    },
    {
        "fact_id": "F009",
        "domain": "operational_integrity",
        "kpi_name": "Label Lag",
        "formula_description": "Average hours from label_created_at to first_scan_at, across all shipments. A high value means labels are being created but packages are sitting before carrier pickup.",
    },
    {
        "fact_id": "F010",
        "domain": "operational_integrity",
        "kpi_name": "Shipment Match Rate",
        "formula_description": "Percentage of shipments successfully matched to a carrier record (exact or fuzzy), expressed as decimal. Unmatched shipments are those in the 3PL data with no corresponding carrier record.",
    },
]

KPI_THRESHOLDS = {
    "On-Time Ship Rate":      {"green": ">= 0.98", "yellow": "0.95-0.97",  "red": "< 0.95"},
    "On-Time Delivery Rate":  {"green": ">= 0.98", "yellow": "0.95-0.97",  "red": "< 0.95"},
    "Shipment Match Rate":    {"green": ">= 0.998","yellow": "0.990-0.997","red": "< 0.99"},
    "Unshipped Orders Rate":  {"green": "< 0.01",  "yellow": "0.01-0.03",  "red": "> 0.03"},
    "Order to Ship Time":     {"green": "<= 24 hours", "yellow": "24-48 hours", "red": "> 48 hours"},
    "Transit Time":           {"green": "<= expected window in hours", "yellow": "up to 24 hours over", "red": "> 24 hours over"},
    "Shipping Cost per Order":{"green": "within 10% of 4-week baseline", "yellow": "10-25% deviation", "red": "> 25% deviation", "first_run": "informational"},
    "Cost by Carrier":        {"green": "within 10% of 4-week baseline", "yellow": "10-25% deviation", "red": "> 25% deviation", "first_run": "informational"},
    "Carrier Mix":            {"status": "informational — no threshold"},
    "Label Lag":              {"green": "<= 4 hours", "yellow": "4-12 hours", "red": "> 12 hours"},
}


# ---------------------------------------------------------------------------
# STAGE 4: INSIGHT GENERATION  (DeepSeek V3)
# ---------------------------------------------------------------------------

STAGE4_GENERATION_SYSTEM = """You are an operations analyst writing a weekly
management report for a senior leadership team. You will be given a verified
FactList of KPI values and you will generate analytical insights, anomaly
flags, root cause hypotheses, and recommended actions.

Every claim you make must cite specific FACT_IDs from the FactList.
A claim without a FACT_ID citation will be stripped from the report.
Output valid JSON only. No explanations. No markdown. No preamble."""


def build_stage4_generation_prompt(
    factlist: list[dict],
    week_date: str,
    retry_context: Optional[str] = None
) -> str:
    """
    Prompt for Stage 4 generation: DeepSeek V3 generates insights.

    Args:
        factlist: the verified FactList from Stage 3 (python_verified=True entries only)
        week_date: ISO date string for the report week
        retry_context: failure reason if retrying
    """
    retry_block = f"\nRETRY CONTEXT: {retry_context}\n" if retry_context else ""

    return f"""You are writing operational insights for a weekly ecommerce management report.

REPORT WEEK: {week_date}
{retry_block}
VERIFIED FACTLIST — these are the only facts you may cite:
{json.dumps(factlist, indent=2, default=str)}

TASK: Generate analytical insights across all four domains:
  - fulfillment
  - carrier_performance
  - cost
  - operational_integrity

For each domain, generate 2-4 insights covering:
  1. An observation (what the numbers show)
  2. A hypothesis (why it might be happening — only if data supports it)
  3. A recommended action (what leadership should do about it)

RULES:
- Every claim_text must be directly supported by one or more FACT_IDs
- cited_fact_ids must contain the actual FACT_ID (e.g. "F002") — not a description
- claim_type must be one of: observation, hypothesis, recommended_action
- DO NOT make projections about future performance
- DO NOT make claims about data not in the FactList
- DO NOT recommend business strategy beyond operational actions
- If a KPI is green and unremarkable, a brief observation is sufficient
- Red and yellow KPIs deserve deeper analysis and a recommended_action
- recommended_action field is only populated for claim_type = recommended_action

OUTPUT FORMAT:
{{
  "insights": [
    {{
      "claim_text": "<the insight in plain English, suitable for a CEO>",
      "claim_type": "<observation|hypothesis|recommended_action>",
      "cited_fact_ids": ["F001", "F002"],
      "verification_verdict": "aligned",
      "strip_reason": null,
      "recommended_action": "<specific action or null>",
      "domain": "<fulfillment|carrier_performance|cost|operational_integrity>"
    }}
  ]
}}

Write at a level appropriate for a CEO or COO. Be direct. Be specific.
Cite the numbers. Do not pad with generic commentary.
Return only the JSON object. No other text."""


# ---------------------------------------------------------------------------
# STAGE 4: CITATION VERIFICATION  (Qwen2.5 72B)
# ---------------------------------------------------------------------------

STAGE4_VERIFICATION_SYSTEM = """You are an adversarial verification analyst.
Your job is to find flaws in insight claims before they reach a CEO.
You are the last line of defense — err on the side of stripping.
Output valid JSON only. No explanations. No markdown. No preamble."""


def build_stage4_verification_prompt(
    insights: list[dict],
    factlist: list[dict],
    retry_context: Optional[str] = None
) -> str:
    """
    Prompt for Stage 4 verification: Qwen2.5 72B checks every citation.

    Args:
        insights: the claims generated by DeepSeek V3
        factlist: the verified FactList to check citations against
        retry_context: failure reason if retrying
    """
    retry_block = f"\nRETRY CONTEXT: {retry_context}\n" if retry_context else ""

    return f"""You are adversarially reviewing insight claims before they reach a CEO.
Your role is to strip any claim that cannot be fully defended from the FactList alone.
{retry_block}
FACTLIST — the only permitted source of truth:
{json.dumps(factlist, indent=2, default=str)}

CLAIMS TO VERIFY:
{json.dumps(insights, indent=2, default=str)}

TASK: For each claim, check all of the following:
  1. Every cited_fact_id exists in the FactList
  2. The cited fact's value directly supports the specific number or direction stated
     (e.g. "declined to 96.6%" requires a fact with final_value ≈ 0.966)
  3. The claim does not assert causation, trends, or comparisons not present in the FactList
  4. The claim_type is correctly applied:
     - observation: must be a direct reading of cited fact values — no inference
     - hypothesis: must be explicitly grounded in cited facts — flag vague "may be due to" language
     - recommended_action: action must be directly motivated by cited facts — not general best practice

STRIP if any of the following are true:
  - A cited FACT_ID does not exist in the FactList
  - The cited fact value does not support the specific claim (wrong number, wrong direction)
  - The claim makes a projection or forecast about future performance
  - The claim introduces context or causes not derivable from the cited facts
  - The claim_type does not match what the claim actually does
  - A hypothesis asserts causation rather than possibility
  - A recommended_action is generic advice unrelated to the cited fact values

ALIGN only when every element of the claim is fully and precisely supported.
Directionally correct is not sufficient — citation accuracy is the standard.
When in doubt, strip.

OUTPUT FORMAT:
{{
  "verdicts": [
    {{
      "claim_text": "<exact claim_text from input>",
      "verification_verdict": "<aligned|stripped>",
      "strip_reason": "<specific reason if stripped, else null>",
      "cited_fact_ids_valid": <true|false>
    }}
  ],
  "total_claims": <integer>,
  "aligned_count": <integer>,
  "stripped_count": <integer>
}}

Return only the JSON object. No other text."""


# ---------------------------------------------------------------------------
# PROMPT REGISTRY  (convenience lookup by stage name)
# ---------------------------------------------------------------------------

SYSTEM_PROMPTS = {
    "stage_1":               STAGE1_SYSTEM,
    "stage_2_exact":         STAGE2_SYSTEM,
    "stage_2_fuzzy":         STAGE2_SYSTEM,
    "stage_3":               STAGE3_SYSTEM,
    "stage_4_generation":    STAGE4_GENERATION_SYSTEM,
    "stage_4_verification":  STAGE4_VERIFICATION_SYSTEM,
}
