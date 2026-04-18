"""
schemas.py
Chorus AI Systems — Data Analytics Manager (DAM)
Pydantic data contracts for all pipeline stages, MVS interface contract,
canonical tables, FactList, and governance objects.

Every stage's input and output is typed here. Nothing flows between
stages without conforming to these schemas.
"""

from __future__ import annotations
from datetime import datetime
from enum import Enum
from typing import Any, Literal, Optional
from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# ENUMS
# ---------------------------------------------------------------------------

class MatchMethod(str, Enum):
    """How a shipment was matched to a carrier record in Stage 2."""
    exact    = "exact"
    fuzzy_llm = "fuzzy_llm"
    unmatched = "unmatched"


class JoinStatus(str, Enum):
    """Final join outcome for a shipment record."""
    matched   = "matched"
    unmatched = "unmatched"
    partial   = "partial"


class ThresholdStatus(str, Enum):
    """RAG status for a KPI value against its locked threshold."""
    green  = "green"
    yellow = "yellow"
    red    = "red"
    informational = "informational"   # e.g. Carrier Mix, first-run cost KPIs


class DegradationLevel(int, Enum):
    """
    System degradation levels (Principle 6).
    0 = Normal, 1 = Partial output, 2 = Full halt.
    """
    normal  = 0
    partial = 1
    halt    = 2


class KPIDomain(str, Enum):
    """The four KPI domains from the spec."""
    fulfillment          = "fulfillment"
    carrier_performance  = "carrier_performance"
    cost                 = "cost"
    operational_integrity = "operational_integrity"


class ClaimType(str, Enum):
    """How a Stage 4 insight claim is classified."""
    observation        = "observation"
    hypothesis         = "hypothesis"
    recommended_action = "recommended_action"


class VerificationVerdict(str, Enum):
    """Qwen verifier's verdict on each Stage 4 claim."""
    aligned  = "aligned"   # FACT_ID exists and supports the claim
    stripped = "stripped"  # Failed citation check; removed from output


class FinancialStatus(str, Enum):
    """Shopify financial status values."""
    paid             = "paid"
    pending          = "pending"
    refunded         = "refunded"
    partially_refunded = "partially_refunded"
    voided           = "voided"
    authorized       = "authorized"


class FulfillmentStatusShopify(str, Enum):
    """Shopify fulfillment status values."""
    fulfilled         = "fulfilled"
    partial           = "partial"
    unfulfilled       = "unfulfilled"
    restocked         = "restocked"


# ---------------------------------------------------------------------------
# CANONICAL TABLES  (Stage 1 output / Stage 2 input)
# ---------------------------------------------------------------------------

class CanonicalOrder(BaseModel):
    """
    One row from the Shopify canonical table.
    Primary key: order_id.
    """
    order_id:                  str
    order_created_at:          datetime
    destination_country:       str
    destination_state:         Optional[str]  = None
    destination_zip:           Optional[str]  = None
    order_subtotal:            float
    order_total:               float
    financial_status:          FinancialStatus
    fulfillment_status_shopify: FulfillmentStatusShopify
    promised_ship_date:        Optional[datetime] = None
    cancelled_at:              Optional[datetime] = None
    is_cancelled:              bool = False


class CanonicalShipment(BaseModel):
    """
    One row from the 3PL canonical table.
    Primary key: shipment_id.  Foreign key: order_id → CanonicalOrder.
    shipping_cost_actual is populated during Stage 2 from carrier data.
    """
    shipment_id:           str
    order_id:              str
    tracking_number:       str
    carrier:               str
    service_level:         str
    label_created_at:      datetime
    shipped_at:            Optional[datetime] = None
    first_scan_at:         Optional[datetime] = None
    delivered_at:          Optional[datetime] = None
    shipping_cost_actual:  Optional[float]    = None   # filled by Stage 2


class CanonicalCarrierShipment(BaseModel):
    """
    One row from a carrier export (FedEx or DHL).
    Primary key: (carrier, tracking_number) composite.
    """
    carrier:                    str
    tracking_number:            str
    first_scan_at:              Optional[datetime] = None
    delivered_at:               Optional[datetime] = None
    shipping_cost_actual:       Optional[float]    = None
    carrier_status_normalized:  Optional[str]      = None
    exception_metadata:         Optional[str]      = None   # free-text flag


class ReconciliationShipment(BaseModel):
    """
    One row in the Stage 2 output table.
    Primary key: shipment_id.
    Every record is here — unmatched records are flagged, not dropped.
    """
    shipment_id:             str
    order_id:                str
    tracking_number:         str
    match_method:            MatchMethod
    join_status:             JoinStatus
    carrier_record_exists:   bool
    data_quality_flag:       Optional[str]  = None   # human-readable flag
    exception_type:          Optional[str]  = None
    fuzzy_match_confidence:  Optional[float] = Field(default=None, ge=0.0, le=1.0)
    fuzzy_match_rationale:   Optional[str]  = None   # LLM explanation

    @field_validator("fuzzy_match_confidence")
    @classmethod
    def confidence_only_on_fuzzy(cls, v, info):
        """Clear confidence score for non-fuzzy matches."""
        if v is not None and info.data.get("match_method") != MatchMethod.fuzzy_llm:
            return None
        return v


# ---------------------------------------------------------------------------
# FIELD MAPPING LOG  (Stage 1 output, governance artifact)
# ---------------------------------------------------------------------------

class FieldMappingEntry(BaseModel):
    """One LLM-proposed column mapping for a single source field."""
    source_file:       str              # e.g. "shopify_orders.csv"
    source_column:     str              # original header name
    canonical_field:   Optional[str]    # target field in canonical schema (None = unmapped)
    mapping_confidence: float = Field(ge=0.0, le=1.0)
    ambiguous:         bool   = False   # True → surfaced as data quality warning
    ambiguity_note:    Optional[str] = None


class FieldMappingLog(BaseModel):
    """Complete record of all LLM field mapping decisions for one run."""
    run_id:    str
    mappings:  list[FieldMappingEntry]
    ambiguous_field_count: int = 0


# ---------------------------------------------------------------------------
# FACTLIST  (Stage 3 output, immutable after emission)
# ---------------------------------------------------------------------------

class DataProvenance(BaseModel):
    """
    Traceability metadata attached to every FactList entry.
    Records exactly how the KPI value was derived.
    """
    source_tables:   list[str]         # canonical table(s) used
    row_count:       int               # number of rows included in computation
    formula_used:    str               # human-readable formula description
    date_range:      str               # e.g. "2026-03-28 to 2026-04-04"
    exclusions:      Optional[str] = None  # any records excluded and why


class FactListEntry(BaseModel):
    """
    One verified KPI fact.
    FACT_ID is the citation anchor for Stage 4 claims.
    final_value is always the Python-computed number — LLM value logged for drift only.
    """
    fact_id:           str              # e.g. "F001" through "F010"
    domain:            KPIDomain
    kpi_name:          str
    llm_value:         Optional[float]  = None  # LLM's computed result (logged, never published)
    python_value:      float            # deterministic recomputation (authoritative)
    final_value:       float            # always equals python_value
    threshold_status:  ThresholdStatus
    llm_python_match:  bool             # False → discrepancy logged
    prior_week_value:  Optional[float] = None
    wow_delta:         Optional[float] = None   # week-over-week change
    auxiliary_value:   Optional[float] = None   # per-KPI secondary value (e.g. DHL avg for F008)
    data_provenance:   DataProvenance
    week_date:         str              # ISO date string for the report week
    python_verified:   bool = True      # gate flag — Stage 4 checks this


# ---------------------------------------------------------------------------
# STAGE 4: INSIGHTS
# ---------------------------------------------------------------------------

class InsightClaim(BaseModel):
    """
    One claim generated by DeepSeek V3 and verified by Qwen2.5-7B.
    Only claims with verdict=aligned surface in the report.
    """
    claim_text:         str
    claim_type:         ClaimType
    cited_fact_ids:     list[str]       # must reference valid FactList FACT_IDs
    verification_verdict: VerificationVerdict
    strip_reason:       Optional[str] = None   # populated when verdict=stripped
    recommended_action: Optional[str] = None   # populated for recommended_action type
    domain:             KPIDomain


# ---------------------------------------------------------------------------
# HEALTH TELEMETRY  (embedded in every stage response)
# ---------------------------------------------------------------------------

class HealthTelemetry(BaseModel):
    """
    Structured metadata emitted with every stage response.
    Aggregated by the orchestrator's Layer 5 across runs.
    """
    stage:             str              # e.g. "stage_1", "stage_4"
    retry_count:       int    = 0
    api_cost_usd:      float  = 0.0
    latency_seconds:   float  = 0.0
    model_used:        Optional[str]  = None
    fallback_activated: bool  = False

    # Stage-specific signals (optional — only populated where relevant)
    mapping_confidence_avg:      Optional[float] = None  # Stage 1
    ambiguous_field_count:       Optional[int]   = None  # Stage 1
    exact_match_rate:            Optional[float] = None  # Stage 2
    fuzzy_match_volume:          Optional[int]   = None  # Stage 2
    fuzzy_match_avg_confidence:  Optional[float] = None  # Stage 2
    kpi_mismatch_count:          Optional[int]   = None  # Stage 3
    claim_count_generated:       Optional[int]   = None  # Stage 4
    claim_acceptance_rate:       Optional[float] = None  # Stage 4
    cross_verifier_agreement:    Optional[float] = None  # Stage 4
    render_time_seconds:         Optional[float] = None  # Stage 5
    pdf_page_count:              Optional[int]   = None  # Stage 5


# ---------------------------------------------------------------------------
# MVS INTERFACE CONTRACT  (VerifiedOutput / DegradationSignal)
# ---------------------------------------------------------------------------

class VerifiedOutput(BaseModel):
    """
    Returned by a stage MVS when it completes successfully.
    The orchestrator proceeds to the next stage on receipt.
    payload contains the stage's typed output (varies by stage).
    """
    stage:             str
    payload:           Any             # typed per-stage (see StageXOutput below)
    health_telemetry:  HealthTelemetry


class DegradationSignal(BaseModel):
    """
    Returned by a stage MVS when it fails after internal retry.
    The orchestrator decides whether to continue (Level 1) or halt (Level 2).
    The stage recommends a level — the orchestrator decides.
    """
    stage:                  str
    failure_reason:         str
    degradation_level_recommendation: DegradationLevel
    health_telemetry:       HealthTelemetry
    detail:                 Optional[dict[str, Any]] = None  # structured failure info


# ---------------------------------------------------------------------------
# STAGE-LEVEL INPUT / OUTPUT CONTRACTS
# ---------------------------------------------------------------------------

class Stage1Input(BaseModel):
    """Four raw CSV file paths passed to Stage 1 by the orchestrator."""
    shopify_csv_path:  str
    tpl_csv_path:      str
    fedex_csv_path:    str
    dhl_csv_path:      str
    run_id:            str


class Stage1Output(BaseModel):
    """Canonical tables + field mapping log emitted by Stage 1."""
    canonical_orders:            list[CanonicalOrder]
    canonical_shipments:         list[CanonicalShipment]
    canonical_carrier_shipments: list[CanonicalCarrierShipment]
    field_mapping_log:           FieldMappingLog


class Stage2Input(BaseModel):
    """Stage 1 canonical tables passed to Stage 2."""
    canonical_orders:            list[CanonicalOrder]
    canonical_shipments:         list[CanonicalShipment]
    canonical_carrier_shipments: list[CanonicalCarrierShipment]
    run_id:                      str


class Stage2Output(BaseModel):
    """Reconciled shipments table + summary stats emitted by Stage 2."""
    reconciliation_shipments: list[ReconciliationShipment]
    exact_match_rate:         float
    fuzzy_match_volume:       int
    unmatched_count:          int


class Stage3Input(BaseModel):
    """Stage 2 output + canonical tables passed to Stage 3."""
    reconciliation_shipments:    list[ReconciliationShipment]
    canonical_orders:            list[CanonicalOrder]
    canonical_shipments:         list[CanonicalShipment]
    canonical_carrier_shipments: list[CanonicalCarrierShipment]
    prior_week_factlist:         Optional[list[FactListEntry]] = None
    run_id:                      str
    week_date:                   str


class Stage3Output(BaseModel):
    """Immutable FactList emitted by Stage 3. Python-verified flag must be True."""
    factlist:        list[FactListEntry]
    kpi_mismatch_count: int
    python_verified: bool = True   # constitutional flag — Stage 4 checks this


class Stage4Input(BaseModel):
    """Verified FactList passed to Stage 4. Rejected if python_verified is False."""
    factlist:        list[FactListEntry]
    python_verified: bool
    run_id:          str
    week_date:       str


class Stage4Output(BaseModel):
    """Verified insights array emitted by Stage 4. Stripped claims are excluded."""
    verified_insights:          list[InsightClaim]
    claim_count_generated:      int
    claim_acceptance_rate:      float
    cross_verifier_agreement:   float
    stripped_claim_log:         list[InsightClaim]   # stripped claims logged, not surfaced


class Stage5Input(BaseModel):
    """Complete verified payload assembled by the orchestrator for Stage 5."""
    stage1_output:       Stage1Output
    stage2_output:       Stage2Output
    stage3_output:       Stage3Output
    stage4_output:       Stage4Output
    degradation_signals: list[DegradationSignal]  # disclosures from any failed stage
    run_id:              str
    report_week:         str   # human-readable date range for report header


class Stage5Output(BaseModel):
    """PDF file path + render metadata emitted by Stage 5."""
    pdf_path:       str
    render_time_s:  float
    page_count:     int
    sections_rendered: list[str]


# ---------------------------------------------------------------------------
# RUN LOG  (written to disk after every execution)
# ---------------------------------------------------------------------------

class RunLog(BaseModel):
    """
    Complete structured log for one pipeline execution.
    Written to disk regardless of outcome (Layer 5 input).
    """
    run_id:              str
    started_at:          datetime
    completed_at:        Optional[datetime]       = None
    final_status:        Literal["pending", "full", "partial", "halted"] = "pending"
    degradation_level:   DegradationLevel         = DegradationLevel.normal
    degradation_signals: list[DegradationSignal]  = Field(default_factory=list)
    stage_telemetry:     list[HealthTelemetry]     = Field(default_factory=list)
    total_api_cost_usd:  float                    = 0.0
    total_latency_s:     float                    = 0.0
    models_used:         list[str]                = Field(default_factory=list)
    fallback_activated:  bool                     = False
    kpi_mismatch_count:  int                      = 0
    claim_acceptance_rate: Optional[float]        = None
    cross_verifier_agreement: Optional[float]     = None
    claim_audit_log:     list["InsightClaim"]     = Field(default_factory=list)
    pdf_path:            Optional[str]            = None
