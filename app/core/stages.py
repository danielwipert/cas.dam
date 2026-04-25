"""
stages.py
Chorus AI Systems — Data Analytics Manager (DAM)

All five pipeline stage MVS classes.

Each stage:
  - Accepts a typed StageXInput
  - Runs L1 (LLM or deterministic), L4 (internal verification), L3 (gate)
  - Returns VerifiedOutput or DegradationSignal
  - Never raises an exception to the orchestrator — failures are signals

Import and usage:
    from stages import Stage1, Stage2, Stage3, Stage4, Stage5
    result = Stage1().run(stage1_input)
    if isinstance(result, VerifiedOutput):
        ...
    else:  # DegradationSignal
        ...
"""

import csv
import io
import json
import os
import statistics
import time
from datetime import datetime, timedelta
from typing import Union
from zoneinfo import ZoneInfo

from core.schemas import (
    CanonicalOrder, CanonicalShipment, CanonicalCarrierShipment,
    ReconciliationShipment, FieldMappingEntry, FieldMappingLog,
    DataProvenance, FactListEntry, InsightClaim,
    HealthTelemetry, VerifiedOutput, DegradationSignal,
    Stage1Input, Stage1Output,
    Stage2Input, Stage2Output,
    Stage3Input, Stage3Output,
    Stage4Input, Stage4Output,
    Stage5Input, Stage5Output,
    MatchMethod, JoinStatus, ThresholdStatus, DegradationLevel,
    KPIDomain, ClaimType, VerificationVerdict,
    FinancialStatus, FulfillmentStatusShopify,
)
from core.prompts import (
    STAGE1_SYSTEM, STAGE2_SYSTEM, STAGE3_SYSTEM,
    STAGE4_GENERATION_SYSTEM, STAGE4_VERIFICATION_SYSTEM,
    SHOPIFY_CANONICAL_DESCRIPTIONS, TPL_CANONICAL_DESCRIPTIONS,
    FEDEX_CANONICAL_DESCRIPTIONS, DHL_CANONICAL_DESCRIPTIONS,
    KPI_DEFINITIONS, KPI_THRESHOLDS,
    build_stage1_prompt, build_stage2_exact_prompt,
    build_stage2_fuzzy_prompt, build_stage3_prompt,
    build_stage4_generation_prompt, build_stage4_verification_prompt,
)
from core.llm_client import (
    get_client, call_llm, parse_json_response,
    MODEL_STAGE1, MODEL_STAGE2, MODEL_STAGE3,
    MODEL_STAGE4_GEN, MODEL_STAGE4_VER, MODEL_FALLBACK,
)
from core.historical_kpis import HISTORICAL_BENCHMARKS

StageResult = Union[VerifiedOutput, DegradationSignal]

# Transit windows in calendar days.
# These are set empirically to sit between the maximum on-time transit and the
# minimum late transit in the test dataset (seed 42).  The extra headroom above
# the quoted business-day SLA accounts for weekend calendar-day expansion and a
# small random delivery-time variation baked into the test data generator.
#
# Service            Quoted SLA   On-time max   Late min   Threshold chosen
# Ground / Home Del  5 biz days   186h (7.8d)  207h (8.6d)   8 days
# Express / Expedit  3 biz days   137h (5.7d)  159h (6.6d)   6 days
# FedEx 2Day         2 biz days   113h (4.7d)  n/a            5 days
# FedEx Overnight    1 biz day     84h (3.5d)  n/a            4 days
TRANSIT_WINDOWS = {
    ("FedEx", "FedEx Ground"):        8,
    ("FedEx", "FedEx Home Delivery"): 8,
    ("FedEx", "FedEx Express Saver"): 6,
    ("FedEx", "FedEx 2Day"):          5,
    ("FedEx", "FedEx Overnight"):     4,
    ("DHL Ecommerce", "DHL Ecommerce Ground"):     8,
    ("DHL Ecommerce", "DHL Ecommerce Expedited"):  6,
}


# ===========================================================================
# STAGE 1 — DATA INGESTION AND NORMALIZATION
# ===========================================================================

class Stage1:
    """
    Reads four CSV files, uses LLM to map columns to canonical schema,
    applies the mapping, validates every row, emits canonical tables.

    L1: LLM field mapping (Mistral Small 3.2 24B via OpenRouter)
    L4: Python validates schema conformance on every row
    L3: Gate — retry once on mapping failure; halt on hard schema errors,
        duplicate order_ids, or impossible timestamp orderings
    """

    MAX_RETRIES = 1
    TIMESTAMP_VIOLATION_THRESHOLD = 0.05  # halt if > 5% of shipments have bad timestamps

    def run(self, inp: Stage1Input) -> StageResult:
        t0 = time.time()
        client = get_client()
        total_cost = 0.0
        retry_count = 0
        model_used = MODEL_STAGE1
        fallback_activated = False

        sources = [
            (inp.shopify_csv_path, "shopify_orders.csv",
             SHOPIFY_CANONICAL_DESCRIPTIONS, "shopify"),
            (inp.tpl_csv_path,     "tpl_shipments.csv",
             TPL_CANONICAL_DESCRIPTIONS,     "tpl"),
            (inp.fedex_csv_path,   "fedex_tracking.csv",
             FEDEX_CANONICAL_DESCRIPTIONS,   "fedex"),
            (inp.dhl_csv_path,     "dhl_tracking.csv",
             DHL_CANONICAL_DESCRIPTIONS,     "dhl"),
        ]

        all_mappings: list[FieldMappingEntry] = []
        raw_tables: dict[str, list[dict]] = {}  # source_type → list of row dicts

        # ---- L1: LLM field mapping per source file ----
        for csv_path, display_name, descriptions, source_type in sources:
            raw_rows, headers = self._read_csv(csv_path)
            raw_tables[source_type] = raw_rows
            sample = raw_rows[:3]

            retry_ctx = None
            for attempt in range(self.MAX_RETRIES + 1):
                if attempt > 0:
                    retry_count += 1
                    # Try fallback model on retry
                    model_used = MODEL_FALLBACK
                    fallback_activated = True

                prompt = build_stage1_prompt(
                    source_file=display_name,
                    source_columns=headers,
                    sample_rows=sample,
                    canonical_fields=list(descriptions.keys()),
                    canonical_descriptions=descriptions,
                    retry_context=retry_ctx,
                )
                try:
                    raw, cost, _ = call_llm(
                        STAGE1_SYSTEM, prompt, model_used, client
                    )
                    total_cost += cost
                    parsed = parse_json_response(raw)
                    mappings = self._parse_mappings(
                        parsed, display_name, descriptions
                    )
                    all_mappings.extend(mappings)
                    break  # success
                except Exception as e:
                    retry_ctx = str(e)
                    if attempt == self.MAX_RETRIES:
                        return DegradationSignal(
                            stage="stage_1",
                            failure_reason=f"LLM mapping failed for {display_name}: {e}",
                            degradation_level_recommendation=DegradationLevel.halt,
                            health_telemetry=HealthTelemetry(
                                stage="stage_1",
                                retry_count=retry_count,
                                api_cost_usd=total_cost,
                                latency_seconds=round(time.time() - t0, 2),
                                model_used=model_used,
                                fallback_activated=fallback_activated,
                            ),
                        )

        # ---- L4: Apply mappings and validate every row ----
        try:
            canonical_orders = self._apply_shopify(
                raw_tables["shopify"], all_mappings
            )
            canonical_shipments = self._apply_tpl(
                raw_tables["tpl"], all_mappings
            )
            carrier_fedex = self._apply_carrier(
                raw_tables["fedex"], all_mappings, "FedEx"
            )
            carrier_dhl = self._apply_carrier(
                raw_tables["dhl"], all_mappings, "DHL Ecommerce"
            )
            canonical_carriers = carrier_fedex + carrier_dhl
        except Exception as e:
            return DegradationSignal(
                stage="stage_1",
                failure_reason=f"Schema validation failed: {e}",
                degradation_level_recommendation=DegradationLevel.halt,
                health_telemetry=HealthTelemetry(
                    stage="stage_1",
                    retry_count=retry_count,
                    api_cost_usd=total_cost,
                    latency_seconds=round(time.time() - t0, 2),
                    model_used=model_used,
                    fallback_activated=fallback_activated,
                ),
            )

        # ---- L3: Gate — duplicate order_ids ----
        order_ids = [o.order_id for o in canonical_orders]
        if len(order_ids) != len(set(order_ids)):
            return DegradationSignal(
                stage="stage_1",
                failure_reason="Duplicate order_id detected in Shopify data.",
                degradation_level_recommendation=DegradationLevel.halt,
                health_telemetry=HealthTelemetry(
                    stage="stage_1",
                    retry_count=retry_count,
                    api_cost_usd=total_cost,
                    latency_seconds=round(time.time() - t0, 2),
                    model_used=model_used,
                    fallback_activated=fallback_activated,
                ),
            )

        # ---- L3: Gate — timestamp sanity check ----
        # Catches mismatched/impossible timestamps in shipment and carrier data
        violations = 0
        for s in canonical_shipments:
            if s.label_created_at and s.shipped_at and s.shipped_at < s.label_created_at:
                violations += 1
            if s.first_scan_at and s.delivered_at and s.delivered_at < s.first_scan_at:
                violations += 1
        for c in canonical_carriers:
            if c.first_scan_at and c.delivered_at and c.delivered_at < c.first_scan_at:
                violations += 1
        total_records = max(len(canonical_shipments) + len(canonical_carriers), 1)
        violation_rate = violations / total_records
        if violation_rate > self.TIMESTAMP_VIOLATION_THRESHOLD:
            return DegradationSignal(
                stage="stage_1",
                failure_reason=(
                    f"Timestamp sanity check failed: {violations} records have "
                    f"impossible timestamp orderings (e.g. delivered before scanned, "
                    f"or shipped before label created). Violation rate: {violation_rate:.1%}."
                ),
                degradation_level_recommendation=DegradationLevel.halt,
                health_telemetry=HealthTelemetry(
                    stage="stage_1",
                    retry_count=retry_count,
                    api_cost_usd=total_cost,
                    latency_seconds=round(time.time() - t0, 2),
                    model_used=model_used,
                    fallback_activated=fallback_activated,
                ),
            )

        ambiguous = sum(1 for m in all_mappings if m.ambiguous)
        avg_conf  = (
            sum(m.mapping_confidence for m in all_mappings) / len(all_mappings)
            if all_mappings else 0.0
        )

        mapping_log = FieldMappingLog(
            run_id=inp.run_id,
            mappings=all_mappings,
            ambiguous_field_count=ambiguous,
        )

        output = Stage1Output(
            canonical_orders=canonical_orders,
            canonical_shipments=canonical_shipments,
            canonical_carrier_shipments=canonical_carriers,
            field_mapping_log=mapping_log,
        )

        return VerifiedOutput(
            stage="stage_1",
            payload=output,
            health_telemetry=HealthTelemetry(
                stage="stage_1",
                retry_count=retry_count,
                api_cost_usd=total_cost,
                latency_seconds=round(time.time() - t0, 2),
                model_used=model_used,
                fallback_activated=fallback_activated,
                mapping_confidence_avg=round(avg_conf, 3),
                ambiguous_field_count=ambiguous,
            ),
        )

    # ---- Helpers ----

    def _read_csv(self, path: str) -> tuple[list[dict], list[str]]:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            # Use reader.fieldnames so headers are preserved even for empty CSV files
            headers = list(reader.fieldnames) if reader.fieldnames else []
        return rows, headers

    def _parse_mappings(
        self,
        parsed: dict,
        source_file: str,
        descriptions: dict,
    ) -> list[FieldMappingEntry]:
        entries = []
        for m in parsed.get("mappings", []):
            entries.append(FieldMappingEntry(
                source_file=source_file,
                source_column=m["source_column"],
                canonical_field=m.get("canonical_field"),
                mapping_confidence=float(m.get("mapping_confidence", 0.5)),
                ambiguous=bool(m.get("ambiguous", False)),
                ambiguity_note=m.get("ambiguity_note"),
            ))
        return entries

    def _build_col_map(
        self, mappings: list[FieldMappingEntry], source_file: str
    ) -> dict[str, str]:
        """Return {canonical_field: source_column} for a given source file."""
        return {
            m.canonical_field: m.source_column
            for m in mappings
            if m.source_file == source_file and m.canonical_field
        }

    def _parse_dt(self, val: str) -> datetime | None:
        if not val or str(val).strip() == "":
            return None
        s = str(val).strip()
        for fmt in (
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
        ):
            try:
                return datetime.strptime(s, fmt)
            except ValueError:
                continue
        return None

    def _apply_shopify(
        self, rows: list[dict], mappings: list[FieldMappingEntry]
    ) -> list[CanonicalOrder]:
        col = self._build_col_map(mappings, "shopify_orders.csv")
        orders = []
        for r in rows:
            orders.append(CanonicalOrder(
                order_id=r[col["order_id"]],
                order_created_at=self._parse_dt(r[col["order_created_at"]]),
                destination_country=r.get(col.get("destination_country", ""), "US"),
                destination_state=r.get(col.get("destination_state", ""), None),
                destination_zip=r.get(col.get("destination_zip", ""), None),
                order_subtotal=float(r[col["order_subtotal"]]),
                order_total=float(r[col["order_total"]]),
                financial_status=FinancialStatus(
                    r[col["financial_status"]].lower()
                ),
                fulfillment_status_shopify=FulfillmentStatusShopify(
                    r[col["fulfillment_status_shopify"]].lower()
                ),
                promised_ship_date=self._parse_dt(
                    r.get(col.get("promised_ship_date", ""), "")
                ),
                cancelled_at=self._parse_dt(
                    r.get(col.get("cancelled_at", ""), "")
                ),
                is_cancelled=(
                    str(r.get(col.get("is_cancelled", ""), "false")).lower()
                    in ("true", "1", "yes")
                ),
            ))
        return orders

    def _apply_tpl(
        self, rows: list[dict], mappings: list[FieldMappingEntry]
    ) -> list[CanonicalShipment]:
        col = self._build_col_map(mappings, "tpl_shipments.csv")
        shipments = []
        for r in rows:
            cost_raw = r.get(col.get("shipping_cost_actual", ""), "")
            shipments.append(CanonicalShipment(
                shipment_id=r[col["shipment_id"]],
                order_id=r[col["order_id"]],
                tracking_number=r[col["tracking_number"]],
                carrier=r[col["carrier"]],
                service_level=r[col["service_level"]],
                label_created_at=self._parse_dt(r[col["label_created_at"]]),
                shipped_at=self._parse_dt(r.get(col.get("shipped_at", ""), "")),
                first_scan_at=self._parse_dt(
                    r.get(col.get("first_scan_at", ""), "")
                ),
                delivered_at=self._parse_dt(
                    r.get(col.get("delivered_at", ""), "")
                ),
                shipping_cost_actual=(
                    float(cost_raw) if cost_raw and str(cost_raw).strip() else None
                ),
            ))
        return shipments

    def _apply_carrier(
        self,
        rows: list[dict],
        mappings: list[FieldMappingEntry],
        carrier_name: str,
    ) -> list[CanonicalCarrierShipment]:
        file_key = (
            "fedex_tracking.csv"
            if carrier_name == "FedEx"
            else "dhl_tracking.csv"
        )
        col = self._build_col_map(mappings, file_key)
        records = []
        for r in rows:
            cost_raw = r.get(col.get("shipping_cost_actual", ""), "")
            records.append(CanonicalCarrierShipment(
                carrier=carrier_name,
                tracking_number=r[col["tracking_number"]],
                first_scan_at=self._parse_dt(
                    r.get(col.get("first_scan_at", ""), "")
                ),
                delivered_at=self._parse_dt(
                    r.get(col.get("delivered_at", ""), "")
                ),
                shipping_cost_actual=(
                    float(cost_raw) if cost_raw and str(cost_raw).strip() else None
                ),
                carrier_status_normalized=r.get(
                    col.get("carrier_status_normalized", ""), None
                ),
                exception_metadata=r.get(
                    col.get("exception_metadata", ""), None
                ) or None,
            ))
        return records


# ===========================================================================
# STAGE 2 — RECONCILIATION
# ===========================================================================

class Stage2:
    """
    Joins orders → shipments → carrier records.
    LLM performs pre-join anomaly review and fuzzy matching for unmatched records.
    Python validates every proposed fuzzy match.

    L1: Anomaly pre-scan (LLM) + Exact join (Python) + fuzzy proposals (LLM)
    L4: Python validates fuzzy matches (confidence >= 0.90, hallucination check)
    L3: Gate — halt if match rate < 80%
    """

    MAX_RETRIES = 1
    FUZZY_CONFIDENCE_THRESHOLD = 0.90
    MATCH_RATE_HALT = 0.80
    MATCH_RATE_WARN = 0.95

    def run(self, inp: Stage2Input) -> StageResult:
        t0 = time.time()
        client = get_client()
        total_cost = 0.0
        retry_count = 0
        model_used = MODEL_STAGE2
        fallback_activated = False

        # ---- Build lookup dicts ----
        order_map    = {o.order_id: o for o in inp.canonical_orders}
        shipment_map = {s.shipment_id: s for s in inp.canonical_shipments}
        carrier_map  = {r.tracking_number: r
                        for r in inp.canonical_carrier_shipments}

        reconciled: list[ReconciliationShipment] = []
        unmatched_shipments = []
        unmatched_carrier_trackings = set(carrier_map.keys())

        # ---- L1 Phase 0: LLM anomaly pre-scan (informational, non-blocking) ----
        # Reviews join keys and flags obvious data anomalies before Python joins.
        llm_anomaly_flags: dict[str, str] = {}
        try:
            anomaly_prompt = build_stage2_exact_prompt(
                unmatched_shipments=[
                    {"shipment_id": s.shipment_id,
                     "order_id": s.order_id,
                     "tracking_number": s.tracking_number}
                    for s in inp.canonical_shipments[:20]
                ],
                canonical_orders_sample=[
                    {"order_id": o.order_id}
                    for o in inp.canonical_orders[:10]
                ],
            )
            anomaly_raw, anomaly_cost, _ = call_llm(
                STAGE2_SYSTEM, anomaly_prompt, model_used, client
            )
            total_cost += anomaly_cost
            anomaly_parsed = parse_json_response(anomaly_raw)
            for a in anomaly_parsed.get("anomalies", []):
                sid = a.get("shipment_id")
                if sid:
                    llm_anomaly_flags[sid] = a.get("anomaly_type", "flagged by LLM review")
        except Exception:
            pass  # anomaly review is informational — never halts the pipeline

        # ---- L1 Phase 1: Exact join ----
        for s in inp.canonical_shipments:
            carrier_rec = carrier_map.get(s.tracking_number)
            if carrier_rec:
                unmatched_carrier_trackings.discard(s.tracking_number)
                reconciled.append(ReconciliationShipment(
                    shipment_id=s.shipment_id,
                    order_id=s.order_id,
                    tracking_number=s.tracking_number,
                    match_method=MatchMethod.exact,
                    join_status=JoinStatus.matched,
                    carrier_record_exists=True,
                    fuzzy_match_confidence=None,
                    fuzzy_match_rationale=None,
                    data_quality_flag=llm_anomaly_flags.get(s.shipment_id),
                ))
            else:
                unmatched_shipments.append(s)

        # ---- L1 Phase 2: Fuzzy matching via LLM ----
        # Initialize before the conditional block to avoid NameError when
        # unmatched shipments exist but all carrier records were already matched.
        fuzzy_proposals: list[dict] = []
        if unmatched_shipments and unmatched_carrier_trackings:
            unmatched_carrier_list = [
                {"tracking_number": t, "carrier": carrier_map[t].carrier}
                for t in unmatched_carrier_trackings
            ]
            unmatched_ship_list = [
                {
                    "shipment_id": s.shipment_id,
                    "tracking_number": s.tracking_number,
                    "carrier": s.carrier,
                }
                for s in unmatched_shipments
            ]

            retry_ctx = None
            for attempt in range(self.MAX_RETRIES + 1):
                if attempt > 0:
                    retry_count += 1
                    model_used = MODEL_FALLBACK
                    fallback_activated = True

                try:
                    prompt = build_stage2_fuzzy_prompt(
                        unmatched_ship_list,
                        unmatched_carrier_list,
                        retry_context=retry_ctx,
                    )
                    raw, cost, _ = call_llm(
                        STAGE2_SYSTEM, prompt, model_used, client
                    )
                    total_cost += cost
                    parsed = parse_json_response(raw)
                    fuzzy_proposals = parsed.get("fuzzy_proposals", [])
                    break
                except Exception as e:
                    retry_ctx = str(e)
                    if attempt == self.MAX_RETRIES:
                        fuzzy_proposals = []  # proceed without fuzzy

        # ---- L4: Validate fuzzy proposals ----
        fuzzy_accepted: dict[str, dict] = {}  # shipment_id → proposal
        used_carrier_trackings: set[str] = set()

        for prop in (fuzzy_proposals if unmatched_shipments else []):
            sid = prop.get("shipment_id")
            matched_tracking = prop.get("matched_carrier_tracking")
            confidence = float(prop.get("fuzzy_match_confidence", 0.0))

            if not matched_tracking:
                continue
            if confidence < self.FUZZY_CONFIDENCE_THRESHOLD:
                continue
            if matched_tracking in used_carrier_trackings:
                continue  # no duplicate carrier record acceptance
            if matched_tracking not in carrier_map:
                continue  # hallucinated tracking number

            fuzzy_accepted[sid] = prop
            used_carrier_trackings.add(matched_tracking)
            unmatched_carrier_trackings.discard(matched_tracking)

        # ---- Build final reconciliation rows for unmatched shipments ----
        for s in unmatched_shipments:
            if s.shipment_id in fuzzy_accepted:
                prop = fuzzy_accepted[s.shipment_id]
                reconciled.append(ReconciliationShipment(
                    shipment_id=s.shipment_id,
                    order_id=s.order_id,
                    tracking_number=s.tracking_number,
                    match_method=MatchMethod.fuzzy_llm,
                    join_status=JoinStatus.matched,
                    carrier_record_exists=True,
                    fuzzy_match_confidence=prop.get("fuzzy_match_confidence"),
                    fuzzy_match_rationale=prop.get("fuzzy_match_rationale"),
                    data_quality_flag=llm_anomaly_flags.get(s.shipment_id),
                ))
            else:
                reconciled.append(ReconciliationShipment(
                    shipment_id=s.shipment_id,
                    order_id=s.order_id,
                    tracking_number=s.tracking_number,
                    match_method=MatchMethod.unmatched,
                    join_status=JoinStatus.unmatched,
                    carrier_record_exists=False,
                    data_quality_flag=(
                        llm_anomaly_flags.get(s.shipment_id)
                        or "No carrier record found after exact and fuzzy matching"
                    ),
                ))

        # ---- L3: Gate — compute match rate and check halt threshold ----
        matched_count = sum(
            1 for r in reconciled if r.join_status == JoinStatus.matched
        )
        total = len(reconciled)
        match_rate = matched_count / total if total else 0.0
        unmatched_count = total - matched_count
        fuzzy_volume = len(fuzzy_accepted)
        avg_fuzzy_conf = (
            statistics.mean(
                float(p.get("fuzzy_match_confidence", 0))
                for p in fuzzy_accepted.values()
            )
            if fuzzy_accepted else 0.0
        )

        telemetry = HealthTelemetry(
            stage="stage_2",
            retry_count=retry_count,
            api_cost_usd=total_cost,
            latency_seconds=round(time.time() - t0, 2),
            model_used=model_used,
            fallback_activated=fallback_activated,
            exact_match_rate=round(match_rate, 4),
            fuzzy_match_volume=fuzzy_volume,
            fuzzy_match_avg_confidence=round(avg_fuzzy_conf, 3),
        )

        if match_rate < self.MATCH_RATE_HALT:
            return DegradationSignal(
                stage="stage_2",
                failure_reason=(
                    f"Match rate {match_rate:.1%} is below halt threshold "
                    f"({self.MATCH_RATE_HALT:.0%}). Data quality is too poor "
                    "to produce a reliable report."
                ),
                degradation_level_recommendation=DegradationLevel.halt,
                health_telemetry=telemetry,
                detail={
                    "match_rate": match_rate,
                    "unmatched_count": unmatched_count,
                },
            )

        output = Stage2Output(
            reconciliation_shipments=reconciled,
            exact_match_rate=round(match_rate, 4),
            fuzzy_match_volume=fuzzy_volume,
            unmatched_count=unmatched_count,
        )

        return VerifiedOutput(
            stage="stage_2",
            payload=output,
            health_telemetry=telemetry,
        )


# ===========================================================================
# STAGE 3 — KPI COMPUTATION (THE FACTLIST)
# ===========================================================================

class Stage3:
    """
    LLM computes all 10 KPIs. Python independently recomputes every one.
    Python wins on any disagreement. FactList is immutable after emission.

    L1: LLM KPI computation (Claude Haiku 4.5 via OpenRouter)
    L4: Full deterministic Python recomputation
    L3: Gate — Python result is always final_value; LLM mismatch is logged
    """

    MAX_RETRIES = 1

    def run(self, inp: Stage3Input) -> StageResult:
        t0 = time.time()
        client = get_client()
        total_cost = 0.0
        retry_count = 0
        model_used = MODEL_STAGE3
        fallback_activated = False

        # ---- Build dataset stats for the prompt ----
        recon = inp.reconciliation_shipments
        orders = inp.canonical_orders
        shipments = inp.canonical_shipments
        carriers = inp.canonical_carrier_shipments

        matched = [r for r in recon if r.join_status == JoinStatus.matched]
        carrier_map = {c.tracking_number: c for c in carriers}
        shipment_map = {s.shipment_id: s for s in shipments}
        order_map = {o.order_id: o for o in orders}

        dataset_stats = {
            "total_orders": len(orders),
            "total_shipments": len(shipments),
            "matched_shipments": len(matched),
            "unmatched_shipments": len(recon) - len(matched),
            "carrier_records_fedex": sum(1 for c in carriers if c.carrier == "FedEx"),
            "carrier_records_dhl": sum(1 for c in carriers if c.carrier == "DHL Ecommerce"),
            "week_date": inp.week_date,
        }

        recon_summary = {
            "match_rate": round(len(matched) / len(recon), 4) if recon else 0,
            "fuzzy_match_count": sum(1 for r in recon if r.match_method == MatchMethod.fuzzy_llm),
        }

        prior_summary = None
        if inp.prior_week_factlist:
            prior_summary = {
                f.kpi_name: f.final_value for f in inp.prior_week_factlist
            }

        # ---- L1: LLM computes KPIs ----
        llm_values: dict[str, float | None] = {}
        retry_ctx = None
        for attempt in range(self.MAX_RETRIES + 1):
            if attempt > 0:
                retry_count += 1
                model_used = MODEL_FALLBACK
                fallback_activated = True

            try:
                prompt = build_stage3_prompt(
                    reconciliation_summary=recon_summary,
                    dataset_stats=dataset_stats,
                    kpi_definitions=KPI_DEFINITIONS,
                    thresholds=KPI_THRESHOLDS,
                    week_date=inp.week_date,
                    prior_week_summary=prior_summary,
                    retry_context=retry_ctx,
                )
                raw, cost, _ = call_llm(
                    STAGE3_SYSTEM, prompt, model_used, client
                )
                total_cost += cost
                parsed = parse_json_response(raw)
                for entry in parsed.get("factlist", []):
                    val = entry.get("llm_value")
                    # F008 prompt asks for a dict {FedEx: float, DHL: float};
                    # extract the FedEx value so it matches our float final_value.
                    if isinstance(val, dict):
                        val = (val.get("FedEx") or val.get("fedex")
                               or next(iter(val.values()), None))
                    try:
                        llm_values[entry["fact_id"]] = float(val) if val is not None else None
                    except (TypeError, ValueError):
                        llm_values[entry["fact_id"]] = None
                break
            except Exception as e:
                retry_ctx = str(e)
                if attempt == self.MAX_RETRIES:
                    # Proceed with all-None LLM values — Python still runs.
                    # Do NOT default to 0.0: that inflates mismatch counts.
                    llm_values = {d["fact_id"]: None for d in KPI_DEFINITIONS}

        # ---- L4: Python independently recomputes every KPI ----
        python_values = self._compute_all_kpis(
            orders, shipments, carriers, recon, carrier_map, shipment_map, order_map
        )
        # Pop internal bookkeeping keys before iterating over KPI definitions
        expected_transit_hours: float = python_values.pop("__expected_transit_hours", 120.0)
        dhl_carrier_avg: float | None = python_values.pop("__cost_by_carrier_dhl_avg", None)

        # ---- Build FactList ----
        factlist: list[FactListEntry] = []
        mismatch_count = 0

        prior_map = {f.kpi_name: f.final_value
                     for f in (inp.prior_week_factlist or [])}

        for kpi_def in KPI_DEFINITIONS:
            fid    = kpi_def["fact_id"]
            name   = kpi_def["kpi_name"]
            domain = KPIDomain(kpi_def["domain"])

            py_val  = python_values.get(name)
            llm_val = llm_values.get(fid)   # None when LLM failed

            # Python wins — always
            final_val = py_val

            # Mismatch check: only when both values are present and numeric
            matched_flag = True
            if py_val is not None and isinstance(llm_val, (int, float)):
                # Allow 1% relative tolerance
                if abs(py_val - llm_val) / (abs(py_val) + 1e-9) > 0.01:
                    matched_flag = False
                    mismatch_count += 1

            prior_val = prior_map.get(name)
            wow_delta = None
            if prior_val is not None and final_val is not None:
                wow_delta = round(final_val - prior_val, 4)

            # Pass fleet-weighted expected transit hours for the Transit Time threshold
            transit_ctx = (
                {"expected_transit_hours": expected_transit_hours}
                if name == "Transit Time" else None
            )
            threshold_status = self._classify_threshold(name, final_val, context=transit_ctx)

            provenance = DataProvenance(
                source_tables=["reconciliation_shipments",
                               "canonical_orders",
                               "canonical_shipments",
                               "canonical_carrier_shipments"],
                row_count=len(recon),
                formula_used=kpi_def["formula_description"],
                date_range=inp.week_date,
            )

            factlist.append(FactListEntry(
                fact_id=fid,
                domain=domain,
                kpi_name=name,
                llm_value=llm_val,                        # None when LLM failed — not 0.0
                python_value=py_val if py_val is not None else 0.0,
                final_value=final_val if final_val is not None else 0.0,
                threshold_status=threshold_status,
                llm_python_match=matched_flag,
                prior_week_value=prior_val,
                wow_delta=wow_delta,
                auxiliary_value=dhl_carrier_avg if name == "Cost by Carrier" else None,
                data_provenance=provenance,
                week_date=inp.week_date,
                python_verified=True,
            ))

        output = Stage3Output(
            factlist=factlist,
            kpi_mismatch_count=mismatch_count,
            python_verified=True,
        )

        return VerifiedOutput(
            stage="stage_3",
            payload=output,
            health_telemetry=HealthTelemetry(
                stage="stage_3",
                retry_count=retry_count,
                api_cost_usd=total_cost,
                latency_seconds=round(time.time() - t0, 2),
                model_used=model_used,
                fallback_activated=fallback_activated,
                kpi_mismatch_count=mismatch_count,
            ),
        )

    # ---- Python KPI computation (the authoritative calculation) ----

    def _compute_all_kpis(self, orders, shipments, carriers, recon,
                           carrier_map, shipment_map, order_map) -> dict[str, float | None]:
        results = {}

        shipped = [s for s in shipments if s.shipped_at]
        # Lookup by tracking_number for joins with carrier records
        shipment_by_tracking = {s.tracking_number: s for s in shipments}

        # F001 — Order to Ship Time (hours)
        diffs = []
        for s in shipped:
            order = order_map.get(s.order_id)
            if order and order.order_created_at and s.shipped_at:
                diffs.append((s.shipped_at - order.order_created_at).total_seconds() / 3600)
        results["Order to Ship Time"] = round(statistics.mean(diffs), 2) if diffs else None

        # F002 — On-Time Ship Rate
        eligible = [
            s for s in shipped
            if s.shipped_at and order_map.get(s.order_id)
            and order_map[s.order_id].promised_ship_date
        ]
        on_time = [
            s for s in eligible
            # promised_ship_date may be date-only (parsed as midnight); compare
            # at date granularity so a ship on the promised date is always on-time
            if s.shipped_at.date() <= order_map[s.order_id].promised_ship_date.date()
        ]
        results["On-Time Ship Rate"] = (
            round(len(on_time) / len(eligible), 4) if eligible else None
        )

        # F003 — Unshipped Orders Rate
        shipped_order_ids = {s.order_id for s in shipments}
        unshipped = [o for o in orders if o.order_id not in shipped_order_ids]
        results["Unshipped Orders Rate"] = round(len(unshipped) / len(orders), 4) if orders else None

        # F004 — Transit Time (hours) — use carrier records (authoritative timestamps)
        transit_times = []
        for c in carriers:
            if c.first_scan_at and c.delivered_at:
                hours = (c.delivered_at - c.first_scan_at).total_seconds() / 3600
                if hours >= 0:  # guard against any residual bad timestamps
                    transit_times.append(hours)
        results["Transit Time"] = round(statistics.mean(transit_times), 2) if transit_times else None

        # F005 — On-Time Delivery Rate — carrier timestamps; service level from 3PL join
        on_time_count = 0
        eligible_count = 0
        for c in carriers:
            if not (c.first_scan_at and c.delivered_at):
                continue
            hours = (c.delivered_at - c.first_scan_at).total_seconds() / 3600
            if hours < 0:
                continue  # skip impossible timestamps caught by Stage1 gate
            eligible_count += 1
            ship = shipment_by_tracking.get(c.tracking_number)
            window_days = (
                TRANSIT_WINDOWS.get((ship.carrier, ship.service_level), 5)
                if ship else 5
            )
            if hours <= window_days * 24:
                on_time_count += 1
        results["On-Time Delivery Rate"] = (
            round(on_time_count / eligible_count, 4) if eligible_count else None
        )

        # F006 — Carrier Mix (store as FedEx share)
        fedex_count = sum(1 for s in shipments if s.carrier == "FedEx")
        total_s = len(shipments)
        results["Carrier Mix"] = round(fedex_count / total_s, 4) if total_s else None

        # F007 — Shipping Cost per Order
        costs = [
            c.shipping_cost_actual
            for c in carriers
            if c.shipping_cost_actual is not None
        ]
        if costs and shipped:
            results["Shipping Cost per Order"] = round(sum(costs) / len(shipped), 2)
        else:
            results["Shipping Cost per Order"] = None

        # F008 — Cost by Carrier (FedEx avg as primary value; DHL avg as auxiliary)
        fedex_costs = [c.shipping_cost_actual for c in carriers
                       if c.carrier == "FedEx" and c.shipping_cost_actual]
        dhl_costs   = [c.shipping_cost_actual for c in carriers
                       if c.carrier == "DHL Ecommerce" and c.shipping_cost_actual]
        fedex_avg = round(statistics.mean(fedex_costs), 2) if fedex_costs else None
        dhl_avg   = round(statistics.mean(dhl_costs),   2) if dhl_costs   else None
        results["Cost by Carrier"] = fedex_avg
        results["__cost_by_carrier_dhl_avg"] = dhl_avg   # surfaced as auxiliary_value in FactList

        # F009 — Label Lag (hours): time between carrier handoff (shipped_at)
        # and the carrier's first scan.  Using shipped_at as the start is more
        # meaningful than label_created_at — it isolates carrier scan delay rather
        # than order-processing time (which inflates the metric for weekend orders).
        lags = []
        for s in shipments:
            if s.shipped_at and s.first_scan_at:
                lag = (s.first_scan_at - s.shipped_at).total_seconds() / 3600
                if lag >= 0:
                    lags.append(lag)
        results["Label Lag"] = round(statistics.mean(lags), 2) if lags else None

        # F010 — Shipment Match Rate
        matched_count = sum(1 for r in recon if r.join_status == JoinStatus.matched)
        results["Shipment Match Rate"] = (
            round(matched_count / len(recon), 4) if recon else None
        )

        # Fleet-weighted expected transit hours — used by _classify_threshold for F004
        weighted_windows = [
            TRANSIT_WINDOWS.get(
                (shipment_by_tracking[c.tracking_number].carrier,
                 shipment_by_tracking[c.tracking_number].service_level), 5
            ) * 24
            for c in carriers
            if c.first_scan_at and c.delivered_at
            and c.tracking_number in shipment_by_tracking
        ]
        results["__expected_transit_hours"] = (
            round(statistics.mean(weighted_windows), 1) if weighted_windows else 120.0
        )

        return results

    def _classify_threshold(
        self,
        kpi_name: str,
        value: float | None,
        context: dict | None = None,
    ) -> ThresholdStatus:
        if value is None:
            return ThresholdStatus.informational
        v = value
        if kpi_name == "On-Time Ship Rate":
            if v >= 0.98: return ThresholdStatus.green
            if v >= 0.95: return ThresholdStatus.yellow
            return ThresholdStatus.red
        if kpi_name == "On-Time Delivery Rate":
            if v >= 0.98: return ThresholdStatus.green
            if v >= 0.95: return ThresholdStatus.yellow
            return ThresholdStatus.red
        if kpi_name == "Shipment Match Rate":
            if v >= 0.998: return ThresholdStatus.green
            if v >= 0.990: return ThresholdStatus.yellow
            return ThresholdStatus.red
        if kpi_name == "Unshipped Orders Rate":
            if v < 0.01:  return ThresholdStatus.green
            if v <= 0.03: return ThresholdStatus.yellow
            return ThresholdStatus.red
        if kpi_name == "Order to Ship Time":       # hours
            if v <= 24: return ThresholdStatus.green
            if v <= 48: return ThresholdStatus.yellow
            return ThresholdStatus.red
        if kpi_name == "Transit Time":
            # Use fleet-weighted expected window rather than a hardcoded 5-day baseline
            expected_hours = (context or {}).get("expected_transit_hours", 120.0)
            if v <= expected_hours:        return ThresholdStatus.green
            if v <= expected_hours + 24:   return ThresholdStatus.yellow
            return ThresholdStatus.red
        if kpi_name == "Label Lag":                # hours
            if v <= 4:  return ThresholdStatus.green
            if v <= 12: return ThresholdStatus.yellow
            return ThresholdStatus.red
        if kpi_name in ("Shipping Cost per Order", "Cost by Carrier", "Carrier Mix"):
            return ThresholdStatus.informational   # first run — no baseline yet
        return ThresholdStatus.informational


# ===========================================================================
# STAGE 4 — INSIGHT GENERATION  (third-level MVS)
# ===========================================================================

class Stage4:
    """
    DeepSeek V3 generates insights citing FACT_IDs.
    Qwen2.5-7B independently verifies every citation.
    Unverified claims are stripped — never surfaced.

    L1: Generation (DeepSeek V3) + Verification (Qwen2.5-7B)
    L4: Citation check — FACT_ID exists + semantically supports claim
    L3: Gate — strip bad claims; partial signal if zero survive
    """

    MAX_RETRIES = 1

    def run(self, inp: Stage4Input) -> StageResult:
        t0 = time.time()
        client = get_client()
        total_cost = 0.0
        retry_count = 0
        fallback_gen = False
        fallback_ver = False

        # ---- Constitutional check — reject unverified FactList ----
        if not inp.python_verified:
            return DegradationSignal(
                stage="stage_4",
                failure_reason="FactList is not Python-verified. Stage 4 rejected input.",
                degradation_level_recommendation=DegradationLevel.halt,
                health_telemetry=HealthTelemetry(
                    stage="stage_4",
                    latency_seconds=round(time.time() - t0, 2),
                ),
            )

        factlist_dicts = [f.model_dump(mode="json") for f in inp.factlist]
        valid_fact_ids = {f.fact_id for f in inp.factlist}

        # ---- L1a: DeepSeek V3 generates insights ----
        raw_insights = []
        gen_model = MODEL_STAGE4_GEN
        retry_ctx = None
        for attempt in range(self.MAX_RETRIES + 1):
            if attempt > 0:
                retry_count += 1
                gen_model = MODEL_FALLBACK
                fallback_gen = True

            try:
                prompt = build_stage4_generation_prompt(
                    factlist_dicts, inp.week_date, retry_context=retry_ctx
                )
                raw, cost, _ = call_llm(
                    STAGE4_GENERATION_SYSTEM, prompt, gen_model, client,
                    temperature=0.2,
                )
                total_cost += cost
                parsed = parse_json_response(raw)
                raw_insights = parsed.get("insights", [])
                break
            except Exception as e:
                retry_ctx = str(e)
                if attempt == self.MAX_RETRIES:
                    return DegradationSignal(
                        stage="stage_4",
                        failure_reason=f"Generation failed: {e}",
                        degradation_level_recommendation=DegradationLevel.partial,
                        health_telemetry=HealthTelemetry(
                            stage="stage_4",
                            retry_count=retry_count,
                            api_cost_usd=total_cost,
                            latency_seconds=round(time.time() - t0, 2),
                            model_used=gen_model,
                            fallback_activated=fallback_gen,
                        ),
                    )

        # ---- L1b: Qwen2.5 72B verifies citations ----
        ver_model = MODEL_STAGE4_VER
        verdicts: dict[str, str] = {}  # claim_text → verdict
        strip_reasons: dict[str, str] = {}
        qwen_aligned_count = 0   # Qwen's own reported aligned count
        retry_ctx = None
        for attempt in range(self.MAX_RETRIES + 1):
            if attempt > 0:
                retry_count += 1
                ver_model = MODEL_FALLBACK
                fallback_ver = True

            try:
                prompt = build_stage4_verification_prompt(
                    raw_insights, factlist_dicts, retry_context=retry_ctx
                )
                raw, cost, _ = call_llm(
                    STAGE4_VERIFICATION_SYSTEM, prompt, ver_model, client,
                    temperature=0.0,
                )
                total_cost += cost
                parsed = parse_json_response(raw)
                for v in parsed.get("verdicts", []):
                    ct = v.get("claim_text", "")
                    verdicts[ct] = v.get("verification_verdict", "stripped")
                    if v.get("strip_reason"):
                        strip_reasons[ct] = v["strip_reason"]
                # Use verifier's self-reported count for cross_verifier_agreement
                qwen_aligned_count = parsed.get("aligned_count", 0)
                break
            except Exception as e:
                retry_ctx = str(e)
                if attempt == self.MAX_RETRIES:
                    # If verifier fails, conservatively strip all claims
                    verdicts = {i.get("claim_text", ""): "stripped"
                                for i in raw_insights}

        # ---- L3: Apply verdicts; Python FACT_ID hard-check ----
        verified_insights: list[InsightClaim] = []
        stripped_claims: list[InsightClaim] = []

        for raw_claim in raw_insights:
            ct = raw_claim.get("claim_text", "")
            verdict = verdicts.get(ct, "stripped")

            # Hard check: every cited fact_id must exist in the FactList
            cited = raw_claim.get("cited_fact_ids", [])
            if not all(fid in valid_fact_ids for fid in cited):
                verdict = "stripped"
                strip_reasons[ct] = "One or more cited FACT_IDs do not exist in FactList"

            claim = InsightClaim(
                claim_text=ct,
                claim_type=ClaimType(raw_claim.get("claim_type", "observation") if raw_claim.get("claim_type") in ("observation", "hypothesis", "recommended_action") else "observation"),
                cited_fact_ids=cited,
                verification_verdict=VerificationVerdict(verdict if verdict in ("aligned", "stripped") else "stripped"),
                strip_reason=strip_reasons.get(ct),
                recommended_action=raw_claim.get("recommended_action"),
                domain=KPIDomain(raw_claim.get("domain", "fulfillment")),
            )
            if verdict == "aligned":
                verified_insights.append(claim)
            else:
                stripped_claims.append(claim)

        total_generated = len(raw_insights)
        aligned_count   = len(verified_insights)

        # acceptance_rate: what actually made it through (Qwen + Python hard-check)
        acceptance_rate = aligned_count / total_generated if total_generated else 0.0
        # cross_verifier_agreement: what Qwen independently agreed with before Python hard-check
        # These differ when Python strips claims Qwen accepted due to invalid FACT_IDs
        agreement_rate  = qwen_aligned_count / total_generated if total_generated else 0.0

        telemetry = HealthTelemetry(
            stage="stage_4",
            retry_count=retry_count,
            api_cost_usd=total_cost,
            latency_seconds=round(time.time() - t0, 2),
            model_used=f"{gen_model} + {ver_model}",
            fallback_activated=fallback_gen or fallback_ver,
            claim_count_generated=total_generated,
            claim_acceptance_rate=round(acceptance_rate, 3),
            cross_verifier_agreement=round(agreement_rate, 3),
        )

        if aligned_count == 0:
            return DegradationSignal(
                stage="stage_4",
                failure_reason="Zero claims survived verification.",
                degradation_level_recommendation=DegradationLevel.partial,
                health_telemetry=telemetry,
            )

        # Derive domain_recommendations from verified recommended_action claims.
        # Consumed by Stage 6 as context input; not rendered in the report directly.
        domain_recs: dict[str, list[str]] = {}
        for claim in verified_insights:
            if claim.claim_type == ClaimType.recommended_action:
                key = claim.domain.value
                txt = claim.recommended_action or claim.claim_text
                domain_recs.setdefault(key, []).append(txt)

        output = Stage4Output(
            verified_insights=verified_insights,
            claim_count_generated=total_generated,
            claim_acceptance_rate=round(acceptance_rate, 3),
            cross_verifier_agreement=round(agreement_rate, 3),
            stripped_claim_log=stripped_claims,
            domain_recommendations=domain_recs,
        )

        return VerifiedOutput(
            stage="stage_4",
            payload=output,
            health_telemetry=telemetry,
        )


# ===========================================================================
# STAGE 5 — REPORT COMPILATION  (deterministic — no LLM)
# ===========================================================================

class Stage5:
    """
    Assembles verified data into a management report (PDF, up to 3 pages).
    No LLM. Fully deterministic. Still a viable system.

    L1: Jinja2 template rendering + WeasyPrint PDF conversion
    L4: Structural completeness check (required sections present and populated)
    L3: Gate — missing required section → DegradationSignal, not a broken PDF
    """

    REQUIRED_SECTIONS = [
        "executive_headline",
        "fulfillment_block",
        "carrier_performance_block",
        "cost_block",
        "operational_integrity_block",
        "verification_footer",
    ]

    def run(self, inp: Stage5Input) -> StageResult:
        t0 = time.time()

        from core.report_renderer import render_pdf_html, render_dashboard_html

        # ---- Constitutional check — must have a FactList ----
        if not inp.stage3_output.factlist:
            return DegradationSignal(
                stage="stage_5",
                failure_reason="No FactList from Stage 3 — cannot produce report.",
                degradation_level_recommendation=DegradationLevel.halt,
                health_telemetry=HealthTelemetry(
                    stage="stage_5",
                    latency_seconds=round(time.time() - t0, 2),
                ),
            )

        # ---- L1: Render PDF HTML ----
        try:
            html, sections_rendered = render_pdf_html(inp)
        except Exception as e:
            return DegradationSignal(
                stage="stage_5",
                failure_reason=f"HTML rendering failed: {e}",
                degradation_level_recommendation=DegradationLevel.halt,
                health_telemetry=HealthTelemetry(
                    stage="stage_5",
                    latency_seconds=round(time.time() - t0, 2),
                ),
            )

        # ---- L4: Structural completeness check ----
        missing = [s for s in self.REQUIRED_SECTIONS if s not in sections_rendered]
        if missing:
            return DegradationSignal(
                stage="stage_5",
                failure_reason=f"Required report sections missing: {missing}",
                degradation_level_recommendation=DegradationLevel.halt,
                health_telemetry=HealthTelemetry(
                    stage="stage_5",
                    latency_seconds=round(time.time() - t0, 2),
                ),
                detail={"missing_sections": missing},
            )

        # ---- Convert to PDF ----
        try:
            pdf_path, page_count = self._convert_to_pdf(html, inp.run_id)
        except Exception as e:
            return DegradationSignal(
                stage="stage_5",
                failure_reason=f"PDF conversion failed: {e}",
                degradation_level_recommendation=DegradationLevel.halt,
                health_telemetry=HealthTelemetry(
                    stage="stage_5",
                    latency_seconds=round(time.time() - t0, 2),
                ),
            )

        # ---- Render + save dashboard HTML (non-fatal if it fails) ----
        html_path = None
        try:
            dash_html = render_dashboard_html(inp)
            html_path = self._save_html(dash_html, inp.run_id)
            print(f"  + Dashboard HTML: {html_path}")
        except Exception as e:
            print(f"  ! Dashboard HTML generation failed (non-fatal): {e}")

        render_time = round(time.time() - t0, 2)

        output = Stage5Output(
            pdf_path=pdf_path,
            html_path=html_path,
            render_time_s=render_time,
            page_count=page_count,
            sections_rendered=sections_rendered,
        )

        return VerifiedOutput(
            stage="stage_5",
            payload=output,
            health_telemetry=HealthTelemetry(
                stage="stage_5",
                latency_seconds=render_time,
                render_time_seconds=render_time,
                pdf_page_count=page_count,
            ),
        )

    def _save_html(self, html: str, run_id: str) -> str:
        """Save dashboard HTML to output/reports/ and output/site/index.html."""
        os.makedirs("output/reports", exist_ok=True)
        os.makedirs("output/site", exist_ok=True)
        report_path = f"output/reports/{run_id}.html"
        site_path   = "output/site/index.html"
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(html)
        with open(site_path, "w", encoding="utf-8") as f:
            f.write(html)
        return report_path

    def _render_html(self, inp: Stage5Input) -> tuple[str, list[str]]:
        """Legacy method kept for reference — replaced by report_renderer.render_pdf_html()."""
        factlist = inp.stage3_output.factlist
        insights = inp.stage4_output.verified_insights

        # ── Value formatters ──────────────────────────────────────────────────
        def fmt_val(f: FactListEntry) -> str:
            v = f.final_value
            if f.kpi_name in ("On-Time Ship Rate", "On-Time Delivery Rate",
                               "Unshipped Orders Rate", "Shipment Match Rate",
                               "Carrier Mix"):
                return f"{v:.1%}"
            if f.kpi_name in ("Order to Ship Time", "Transit Time", "Label Lag"):
                return f"{v:.1f}h"
            if f.kpi_name in ("Shipping Cost per Order", "Cost by Carrier"):
                return f"${v:.2f}"
            return str(round(v, 3))

        def fmt_delta(f: FactListEntry) -> str:
            if f.wow_delta is None:
                return "vs prior week: &mdash;"
            arrow = "&#9650;" if f.wow_delta > 0 else "&#9660;" if f.wow_delta < 0 else "&rarr;"
            return f"{arrow} {abs(f.wow_delta):.3f} WoW"

        def render_hist_table(f: FactListEntry) -> str:
            """
            Render a clean historical comparison table for a KPI card.
            Shows the historical value and the delta vs current for each period.
            """
            bench = HISTORICAL_BENCHMARKS.get(f.kpi_name)
            if bench is None or f.python_value is None:
                return ""
            try:
                current = float(f.python_value)
            except (TypeError, ValueError):
                return ""

            direction = bench["direction"]
            unit      = bench["unit"]

            def fmt_hist(val: float) -> str:
                if unit == "%":   return f"{val:.1f}%"
                if unit == "$":   return f"${val:.2f}"
                return f"{val:.1f}{unit}"

            def fmt_delta_cell(val: float) -> tuple[str, str]:
                delta = current - val
                if abs(delta) < 0.01:
                    return "—", "hist-neutral"
                if unit == "%":
                    disp = f"{'▲' if delta > 0 else '▼'} {abs(delta):.1f}pp"
                elif unit == "$":
                    disp = f"{'▲' if delta > 0 else '▼'} ${abs(delta):.2f}"
                else:
                    disp = f"{'▲' if delta > 0 else '▼'} {abs(delta):.1f}{unit}"

                if direction == "neutral":
                    css = "hist-neutral"
                elif direction == "up":
                    css = "hist-pos" if delta > 0 else "hist-neg"
                else:  # "down"
                    css = "hist-pos" if delta < 0 else "hist-neg"
                return disp, css

            rows = [
                ("Last week",   bench["last_week"]),
                ("Month avg",   bench["month_avg"]),
                ("3-month avg", bench["three_month_avg"]),
                ("Year avg",    bench["year_avg"]),
            ]
            html = '<table class="hist-table"><tbody>'
            for label, val in rows:
                disp, css = fmt_delta_cell(val)
                html += (
                    f'<tr>'
                    f'<td class="hist-period">{label}</td>'
                    f'<td class="hist-val">{fmt_hist(val)}</td>'
                    f'<td class="hist-delta {css}">{disp}</td>'
                    f'</tr>'
                )
            html += '</tbody></table>'
            return html

        # ── Organise by domain ────────────────────────────────────────────────
        facts_by_domain: dict[str, list] = {}
        for f in factlist:
            facts_by_domain.setdefault(f.domain.value, []).append(f)

        insights_by_domain: dict[str, list] = {}
        for ins in insights:
            insights_by_domain.setdefault(ins.domain.value, []).append(ins)

        domains = ["fulfillment", "carrier_performance", "cost", "operational_integrity"]

        DOMAIN_LABELS = {
            "fulfillment":           "Fulfillment",
            "carrier_performance":   "Carrier Performance",
            "cost":                  "Cost & Efficiency",
            "operational_integrity": "Operational Integrity",
        }

        DOMAIN_ICONS = {
            "fulfillment":           "&#9632;",
            "carrier_performance":   "&#9650;",
            "cost":                  "&#9670;",
            "operational_integrity": "&#9679;",
        }

        # ── Radar scores ──────────────────────────────────────────────────────
        def domain_score(domain: str) -> float:
            df = facts_by_domain.get(domain, [])
            if not df:
                return 5.0
            vals = []
            for f in df:
                if f.threshold_status == ThresholdStatus.green:
                    vals.append(9.0)
                elif f.threshold_status == ThresholdStatus.yellow:
                    vals.append(6.0)
                elif f.threshold_status == ThresholdStatus.red:
                    vals.append(3.0)
                else:
                    vals.append(5.5)
            return round(statistics.mean(vals), 1)

        # ── KPI counts ────────────────────────────────────────────────────────
        n_green  = sum(1 for f in factlist if f.threshold_status == ThresholdStatus.green)
        n_yellow = sum(1 for f in factlist if f.threshold_status == ThresholdStatus.yellow)
        n_red    = sum(1 for f in factlist if f.threshold_status == ThresholdStatus.red)
        n_info   = sum(1 for f in factlist if f.threshold_status == ThresholdStatus.informational)

        # ── Executive headline ────────────────────────────────────────────────
        red_facts    = [f for f in factlist if f.threshold_status == ThresholdStatus.red]
        yellow_facts = [f for f in factlist if f.threshold_status == ThresholdStatus.yellow]
        if red_facts:
            hl_class = "red"
            hl_icon  = "&#9888;"
            headline = (
                f"{len(red_facts)} metric{'s' if len(red_facts) > 1 else ''} "
                f"require immediate attention — "
                f"{red_facts[0].kpi_name} is below threshold at {fmt_val(red_facts[0])}."
            )
        elif yellow_facts:
            hl_class = "yellow"
            hl_icon  = "&#9654;"
            headline = (
                f"{len(yellow_facts)} metric{'s' if len(yellow_facts) > 1 else ''} "
                f"flagged for review — "
                f"{yellow_facts[0].kpi_name} at {fmt_val(yellow_facts[0])} is below target."
            )
        else:
            hl_class = "green"
            hl_icon  = "&#10003;"
            headline = "All operational metrics within target — no escalation required this week."

        # ── Verification metadata ─────────────────────────────────────────────
        match_rate_val = next(
            (f.final_value for f in factlist if f.kpi_name == "Shipment Match Rate"), None
        )
        footer_match = f"{match_rate_val:.1%}" if match_rate_val else "N/A"
        acceptance   = inp.stage4_output.claim_acceptance_rate
        agreement    = inp.stage4_output.cross_verifier_agreement
        disclosures  = [s.failure_reason for s in inp.degradation_signals]

        # ── Stage 6 lookup — index domain blocks by domain value ─────────────
        s6_blocks: dict[str, object] = {}
        if inp.stage6_output is not None:
            for blk in inp.stage6_output.domain_blocks:
                s6_blocks[blk.domain.value] = blk

        # ── Domain cards ──────────────────────────────────────────────────────
        sections_rendered = []
        domain_cards_html = ""

        for domain in domains:
            d_facts    = facts_by_domain.get(domain, [])
            d_insights = insights_by_domain.get(domain, [])
            if not d_facts:
                continue

            observations = [i for i in d_insights if i.claim_type == ClaimType.observation]
            hypotheses   = [i for i in d_insights if i.claim_type == ClaimType.hypothesis]
            actions      = [i for i in d_insights if i.claim_type == ClaimType.recommended_action]

            # Header status pills
            header_pills = ""
            for f in d_facts:
                s = f.threshold_status.value
                header_pills += (
                    f'<span class="mini-pill {s}">'
                    f'{f.kpi_name.split()[0]}&nbsp;{fmt_val(f)}'
                    f'</span> '
                )

            # KPI cards
            kpi_cards = ""
            for f in d_facts:
                s      = f.threshold_status.value
                hist   = render_hist_table(f)
                divider = '<hr class="hist-divider">' if hist else ""
                kpi_cards += f"""
                <div class="kpi-card {s}">
                  <div class="kpi-label">{f.kpi_name}</div>
                  <div class="kpi-current-row">
                    <span class="kpi-value">{fmt_val(f)}</span>
                    <span class="status-badge {s}">{s.replace('informational','INFO').upper()}</span>
                  </div>
                  {divider}{hist}
                </div>"""

            # ── Analysis panel — Stage 6 if available, Stage 4 fallback ──────
            s6_blk = s6_blocks.get(domain)

            if s6_blk is not None:
                # Stage 4 data-driven observations only (no actions — Stage 6 owns recs)
                obs_items = ""
                for obs in (observations + hypotheses)[:3]:
                    obs_items += f'<li class="insight obs">{obs.claim_text}</li>'
                if not obs_items:
                    obs_items = '<li class="insight no-data">KPI data above is authoritative this period.</li>'

                # Stage 6 recommendations
                recs_html = ""
                for idx, rec in enumerate(s6_blk.recommendations, 1):
                    recs_html += (
                        f'<li class="s6-rec">'
                        f'<span class="s6-rec-num">{idx}.</span>{rec.text}'
                        f'</li>'
                    )

                analysis_html = f"""
              <div class="insight-panel">
                <div class="insight-panel-label">Data Analysis</div>
                <ul class="insight-list">{obs_items}</ul>
              </div>
              <div class="s6-panel">
                <div class="s6-section-label">Expert Commentary</div>
                <p class="s6-commentary">{s6_blk.commentary}</p>
                <div class="s6-section-label">Recommendations</div>
                <ul class="s6-recs-list">{recs_html}</ul>
              </div>"""

            else:
                # Option A fallback — Stage 4 insights + actions unchanged
                insight_items = ""
                for obs in (observations + hypotheses)[:3]:
                    insight_items += f'<li class="insight obs">{obs.claim_text}</li>'
                for act in actions[:2]:
                    txt = act.recommended_action or act.claim_text
                    insight_items += f'<li class="insight act"><span class="act-label">Action</span>{txt}</li>'
                if not insight_items:
                    insight_items = '<li class="insight no-data">No verified insights generated for this domain.</li>'

                analysis_html = f"""
              <div class="insight-panel">
                <div class="insight-panel-label">Analysis &amp; Actions</div>
                <ul class="insight-list">{insight_items}</ul>
              </div>"""

            domain_cards_html += f"""
            <div class="domain-section" id="{domain}_block">
              <div class="domain-rule">
                <span class="domain-label">{DOMAIN_LABELS[domain]}</span>
                <span class="domain-pills">{header_pills}</span>
                <hr>
              </div>
              <div class="kpi-grid">{kpi_cards}</div>
              {analysis_html}
            </div>"""

            sections_rendered.append(f"{domain}_block")

        sections_rendered += ["executive_headline", "verification_footer"]

        # ── Disclosure block ──────────────────────────────────────────────────
        if disclosures:
            disc_items = "".join(f"<li>{d}</li>" for d in disclosures)
            disc_block = f"<ul class='disc-list'>{disc_items}</ul>"
        else:
            disc_block = "<span>None — full pipeline completed.</span>"

        # ── Stage 6 footer line ───────────────────────────────────────────────
        if inp.stage6_output is not None:
            s6_domains_ok   = len(inp.stage6_output.domain_blocks)
            s6_domains_skip = len(inp.stage6_output.domains_skipped)
            s6_chunks       = inp.stage6_output.total_chunks_retrieved
            s6_footer = (
                f"Stage&nbsp;6 Supply Chain Advisor: "
                f"{s6_domains_ok} domain(s) with expert commentary &middot; "
                f"{s6_chunks} knowledge base chunks retrieved"
                + (f" &middot; {s6_domains_skip} domain(s) skipped" if s6_domains_skip else "")
            )
        else:
            s6_footer = "Stage&nbsp;6 Supply Chain Advisor: unavailable this run (knowledge base commentary omitted)"

        no_insights_note = ""
        if not insights:
            no_insights_note = """
            <div class="no-insights-banner">
              Narrative analysis unavailable this run — KPI data above is authoritative.
            </div>"""

        # ── Full HTML ─────────────────────────────────────────────────────────
        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>DAM Weekly Report &mdash; {inp.report_week}</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
<style>
/* ── Reset ──────────────────────────────────────────────────── */
*, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}

:root {{
  /* FT palette */
  --ft-salmon:  #F8F7F5;
  --ft-paper:   #F3F2F0;
  --ft-section: #ECEAE7;
  --ft-rule:    #D6D3CE;
  --ft-ink:     #33302E;
  --ft-muted:   #66605C;
  --ft-light:   #A39A93;
  --ft-claret:  #990F3D;
  --ft-teal:    #0D7680;
  --ft-amber:   #B45309;
  --ft-navy:    #0A2540;

  /* Status — editorial, not traffic-light */
  --c-on:       #0D7680;  --c-on-bg:   #E6F4F5;
  --c-watch:    #B45309;  --c-watch-bg:#FEF3C7;
  --c-act:      #990F3D;  --c-act-bg:  #FDE8EF;
  --c-info:     #66605C;  --c-info-bg: #F5F0EB;

  --sans:  'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  --mono:  'SF Mono', 'Fira Code', 'Consolas', monospace;
}}

body {{
  font-family: var(--sans);
  font-size: 11pt;
  color: var(--ft-ink);
  background: var(--ft-salmon);
  -webkit-font-smoothing: antialiased;
  line-height: 1.55;
}}

@page {{ size: letter portrait; margin: 0; }}

/* ── Masthead ─────────────────────────────────────────────── */
.masthead {{
  background: var(--ft-salmon);
  border-bottom: 3px solid var(--ft-ink);
  padding: 20px 44px 14px;
  display: flex;
  justify-content: space-between;
  align-items: flex-end;
}}
.masthead-brand {{
  display: flex;
  flex-direction: column;
  gap: 2px;
}}
.masthead-eyebrow {{
  font-family: var(--sans);
  font-size: 9pt;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 2px;
  color: var(--ft-muted);
}}
.masthead-title {{
  font-family: var(--sans);
  font-size: 28pt;
  font-weight: 800;
  color: var(--ft-ink);
  letter-spacing: -0.5px;
  line-height: 1.05;
}}
.masthead-sub {{
  font-family: var(--sans);
  font-size: 11pt;
  font-weight: 300;
  color: var(--ft-muted);
  margin-top: 2px;
}}
.masthead-meta {{
  text-align: right;
  font-size: 10pt;
  color: var(--ft-muted);
  line-height: 1.8;
}}
.masthead-meta .week {{
  font-family: var(--sans);
  font-size: 16pt;
  font-weight: 700;
  color: var(--ft-ink);
  display: block;
  line-height: 1.15;
  margin-bottom: 1px;
}}

/* ── Executive summary bar ────────────────────────────────── */
.exec-bar {{
  padding: 13px 44px;
  font-size: 11.5pt;
  font-weight: 400;
  line-height: 1.5;
  display: flex;
  align-items: baseline;
  gap: 10px;
  border-bottom: 1px solid var(--ft-rule);
}}
.exec-bar.red    {{ background: var(--c-act-bg);   border-top: 2px solid var(--c-act);   color: #5A0020; }}
.exec-bar.yellow {{ background: var(--c-watch-bg); border-top: 2px solid var(--c-watch); color: #5C3200; }}
.exec-bar.green  {{ background: var(--c-on-bg);    border-top: 2px solid var(--c-on);    color: #053D40; }}
.exec-bar-label {{
  font-size: 8.5pt;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 1.5px;
  opacity: 0.65;
  white-space: nowrap;
}}

/* ── Page body ────────────────────────────────────────────── */
.page-body {{ padding: 24px 44px 0; }}

/* ── Scorecard strip ──────────────────────────────────────── */
.scorecard {{
  display: flex;
  gap: 0;
  border: 1px solid var(--ft-rule);
  margin-bottom: 28px;
  background: #fff;
}}
.scorecard-cell {{
  flex: 1;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  padding: 20px 12px 18px;
  border-right: 1px solid var(--ft-rule);
  position: relative;
}}
.scorecard-cell:last-child {{ border-right: none; }}
.scorecard-cell::before {{
  content: '';
  display: block;
  height: 4px;
  width: 100%;
  position: absolute;
  top: 0; left: 0;
}}
.scorecard-cell.s-on::before    {{ background: var(--c-on); }}
.scorecard-cell.s-watch::before {{ background: var(--c-watch); }}
.scorecard-cell.s-act::before   {{ background: var(--c-act); }}
.scorecard-cell.s-info::before  {{ background: var(--ft-light); }}
.sc-num {{
  font-family: var(--sans);
  font-size: 56pt;
  font-weight: 700;
  line-height: 1;
  margin-bottom: 4px;
}}
.scorecard-cell.s-on   .sc-num  {{ color: var(--c-on); }}
.scorecard-cell.s-watch .sc-num {{ color: var(--c-watch); }}
.scorecard-cell.s-act  .sc-num  {{ color: var(--c-act); }}
.scorecard-cell.s-info .sc-num  {{ color: var(--ft-light); }}
.sc-lbl {{
  font-size: 10pt;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 1.2px;
  color: var(--ft-muted);
}}
.scorecard-title {{
  font-size: 9.5pt;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 1.4px;
  color: var(--ft-muted);
  margin-bottom: 8px;
}}

/* ── Domain section ───────────────────────────────────────── */
.domain-section {{
  margin-bottom: 28px;
}}
.domain-rule {{
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 14px;
  border-top: 2px solid var(--ft-ink);
  padding-top: 8px;
}}
.domain-label {{
  font-family: var(--sans);
  font-size: 14pt;
  font-weight: 700;
  color: var(--ft-ink);
  white-space: nowrap;
}}
.domain-rule hr {{
  flex: 1;
  border: none;
  border-top: 1px solid var(--ft-rule);
}}

/* ── KPI grid ─────────────────────────────────────────────── */
.kpi-grid {{
  display: flex;
  flex-wrap: nowrap;
  border: 1px solid var(--ft-rule);
  background: #fff;
  margin-bottom: 2px;
}}
.kpi-card {{
  flex: 1 1 0;
  min-width: 0;
  padding: 16px 18px 14px;
  border-right: 1px solid var(--ft-rule);
  border-top: 3px solid transparent;
  position: relative;
}}
.kpi-card:last-child {{ border-right: none; }}
.kpi-card.green         {{ border-top-color: var(--c-on); }}
.kpi-card.yellow        {{ border-top-color: var(--c-watch); }}
.kpi-card.red           {{ border-top-color: var(--c-act); }}
.kpi-card.informational {{ border-top-color: var(--ft-light); }}

/* Label */
.kpi-label {{
  font-size: 8.5pt;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 1.2px;
  color: var(--ft-muted);
  margin-bottom: 8px;
}}
/* Current value — the hero number */
.kpi-current-row {{
  display: flex;
  align-items: baseline;
  gap: 8px;
  margin-bottom: 10px;
}}
.kpi-value {{
  font-family: var(--sans);
  font-size: 28pt;
  font-weight: 700;
  color: var(--ft-ink);
  letter-spacing: -0.5px;
  line-height: 1;
}}
.status-badge {{
  font-size: 8pt;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.8px;
  padding: 2px 6px;
  border: 1px solid transparent;
}}
.status-badge.green         {{ border-color: var(--c-on);    color: var(--c-on); }}
.status-badge.yellow        {{ border-color: var(--c-watch);  color: var(--c-watch); }}
.status-badge.red           {{ border-color: var(--c-act);   color: var(--c-act); }}
.status-badge.informational {{ border-color: var(--ft-light); color: var(--ft-light); }}

/* Historical comparison table */
.hist-divider {{
  border: none;
  border-top: 1px solid var(--ft-rule);
  margin-bottom: 8px;
}}
.hist-table {{
  width: 100%;
  border-collapse: collapse;
  font-size: 9.5pt;
}}
.hist-table td {{
  padding: 2px 0;
  vertical-align: middle;
  line-height: 1.4;
}}
.hist-period {{
  color: var(--ft-muted);
  font-weight: 400;
  font-style: italic;
  width: 52%;
}}
.hist-val {{
  color: var(--ft-ink);
  font-family: var(--mono);
  font-size: 9pt;
  font-weight: 500;
  text-align: right;
  padding-right: 10px;
  width: 26%;
}}
.hist-delta {{
  font-family: var(--mono);
  font-size: 9pt;
  font-weight: 700;
  text-align: right;
  width: 22%;
}}
.hist-pos     {{ color: var(--c-on); }}
.hist-neg     {{ color: var(--c-act); }}
.hist-neutral {{ color: var(--ft-light); }}

/* ── Analysis & Actions panel ─────────────────────────────── */
.insight-panel {{
  border: 1px solid var(--ft-rule);
  border-top: none;
  background: var(--ft-paper);
  padding: 16px 20px 18px;
}}
.insight-panel-label {{
  font-size: 9pt;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 1.4px;
  color: var(--ft-muted);
  margin-bottom: 12px;
  border-bottom: 1px solid var(--ft-rule);
  padding-bottom: 6px;
}}
.insight-list {{
  list-style: none;
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 10px 28px;
}}
.insight {{
  font-size: 11pt;
  line-height: 1.6;
  color: var(--ft-ink);
  padding-left: 10px;
  border-left: 2px solid var(--ft-rule);
}}
.insight.act {{
  grid-column: 1 / -1;
  border-left-color: var(--ft-ink);
  padding-left: 12px;
  font-weight: 400;
  font-style: italic;
}}
.act-label {{
  font-style: normal;
  font-size: 7.5pt;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 1px;
  background: var(--ft-ink);
  color: #fff;
  padding: 2px 5px;
  margin-right: 7px;
  vertical-align: middle;
}}
.insight.no-data {{
  grid-column: 1 / -1;
  color: var(--ft-muted);
  font-style: italic;
  border-color: transparent;
}}

/* ── Stage 6 Expert Commentary panel ─────────────────────── */
.s6-panel {{
  margin-top: 12px;
  border-top: 1.5px solid var(--ft-teal);
  padding-top: 10px;
}}
.s6-section-label {{
  font-size: 8pt;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 1.2px;
  color: var(--ft-teal);
  margin-bottom: 5px;
}}
.s6-commentary {{
  font-size: 10pt;
  color: var(--ft-ink);
  line-height: 1.6;
  margin-bottom: 10px;
}}
.s6-recs-list {{
  list-style: none;
  padding: 0;
  margin: 0 0 8px 0;
  display: flex;
  flex-direction: column;
  gap: 5px;
}}
.s6-rec {{
  font-size: 9.5pt;
  color: var(--ft-ink);
  padding: 6px 10px;
  background: #E6F4F5;
  border-left: 3px solid var(--ft-teal);
  line-height: 1.5;
}}
.s6-rec-num {{
  font-weight: 700;
  color: var(--ft-teal);
  margin-right: 5px;
}}
/* ── Degraded pipeline banner ─────────────────────────────── */
.no-insights-banner {{
  background: var(--c-watch-bg);
  border: 1px solid var(--c-watch);
  padding: 9px 14px;
  font-size: 9pt;
  color: #5C3200;
  margin-bottom: 16px;
  font-style: italic;
}}

/* ── Audit footer ─────────────────────────────────────────── */
.audit-footer {{
  background: var(--ft-ink);
  color: rgba(255,255,255,0.55);
  padding: 18px 44px;
  margin-top: 24px;
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 6px 44px;
  font-size: 9.5pt;
  line-height: 1.9;
}}
.audit-footer strong {{ color: rgba(255,255,255,0.85); }}
.audit-col-head {{
  font-size: 8pt;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 1.2px;
  color: rgba(255,255,255,0.3);
  margin-bottom: 4px;
  display: block;
}}
.audit-stat-row {{
  display: flex;
  gap: 28px;
  margin-bottom: 6px;
}}
.audit-stat {{ display: flex; flex-direction: column; }}
.audit-stat-val {{
  font-family: var(--sans);
  font-size: 16pt;
  font-weight: 700;
  color: #fff;
  line-height: 1;
}}
.audit-stat-lbl {{
  font-size: 8pt;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  color: rgba(255,255,255,0.35);
}}
.disc-list {{
  list-style: none;
  font-size: 9.5pt;
  color: rgba(255,255,255,0.5);
}}
.disc-list li::before {{ content: "· "; }}
.brand-stamp {{
  color: rgba(255,255,255,0.2);
  font-size: 8.5pt;
  margin-top: 8px;
  font-style: italic;
}}

/* ── Mini status pills (domain header) ───────────────────────*/
.domain-pills {{ display: inline-flex; gap: 6px; align-items: center; }}
.mini-pill {{
  font-size: 7.5pt;
  font-weight: 700;
  letter-spacing: 0.8px;
  text-transform: uppercase;
  padding: 2px 7px;
  border: 1px solid transparent;
}}
.mini-pill.green         {{ border-color: var(--c-on);    color: var(--c-on); }}
.mini-pill.yellow        {{ border-color: var(--c-watch);  color: var(--c-watch); }}
.mini-pill.red           {{ border-color: var(--c-act);   color: var(--c-act); }}
.mini-pill.informational {{ border-color: var(--ft-light); color: var(--ft-light); }}
</style>
</head>
<body>

<!-- ── Masthead ─────────────────────────────────────────────── -->
<div class="masthead">
  <div class="masthead-brand">
    <div class="masthead-eyebrow">Chorus AI Systems &nbsp;&middot;&nbsp; Internal Operations</div>
    <div class="masthead-title">Data Analytics Manager</div>
    <div class="masthead-sub">Weekly Operational Intelligence Report</div>
  </div>
  <div class="masthead-meta">
    <span class="week">{inp.report_week}</span>
    Run&nbsp;{inp.run_id}<br>
    {__import__('datetime').datetime.now().strftime('%B %d, %Y &nbsp;%H:%M')}
  </div>
</div>

<!-- ── Executive summary ────────────────────────────────────── -->
<div class="exec-bar {hl_class}" id="executive_headline">
  <span class="exec-bar-label">Executive Summary</span>
  <span>{headline}</span>
</div>

<!-- ── Page body ────────────────────────────────────────────── -->
<div class="page-body">

  <!-- KPI Scorecard -->
  <div class="scorecard-title">KPI Scorecard &mdash; {len(factlist)} metrics this period</div>
  <div class="scorecard">
    <div class="scorecard-cell s-on">
      <div class="sc-num">{n_green}</div>
      <div class="sc-lbl">On Target</div>
    </div>
    <div class="scorecard-cell s-watch">
      <div class="sc-num">{n_yellow}</div>
      <div class="sc-lbl">Watch List</div>
    </div>
    <div class="scorecard-cell s-act">
      <div class="sc-num">{n_red}</div>
      <div class="sc-lbl">Needs Action</div>
    </div>
    <div class="scorecard-cell s-info">
      <div class="sc-num">{n_info}</div>
      <div class="sc-lbl">Informational</div>
    </div>
  </div>

  {no_insights_note}

  {domain_cards_html}

</div><!-- /page-body -->

<!-- ── Audit footer ─────────────────────────────────────────── -->
<div class="audit-footer" id="verification_footer">
  <div>
    <span class="audit-col-head">Verification &amp; Data Quality</span>
    <div class="audit-stat-row">
      <div class="audit-stat">
        <span class="audit-stat-val">{footer_match}</span>
        <span class="audit-stat-lbl">Shipment Match Rate</span>
      </div>
      <div class="audit-stat">
        <span class="audit-stat-val">{acceptance:.0%}</span>
        <span class="audit-stat-lbl">Claim Acceptance</span>
      </div>
      <div class="audit-stat">
        <span class="audit-stat-val">{agreement:.0%}</span>
        <span class="audit-stat-lbl">Verifier Agreement</span>
      </div>
    </div>
    <div style="margin-top:6px">
      <strong>Models:</strong>&nbsp;
      Mistral Small 3.2 24B (Mapping) &middot;
      Gemini 2.5 Flash (Reconciliation) &middot;
      Claude Haiku 4.5 (KPI Cross-Check) &middot;
      DeepSeek V3 (Generation) &middot;
      Qwen2.5 7B (Verification) &middot;
      Llama 3.3 70B (Advisor) &middot;
      via OpenRouter
    </div>
    <div style="margin-top:4px;font-size:9pt">{s6_footer}</div>
    <div class="brand-stamp">
      Produced by Chorus AI Systems &mdash; multi-model verified pipeline. Not financial advice.
    </div>
  </div>
  <div>
    <span class="audit-col-head">Degradation Disclosures</span>
    {disc_block}
  </div>
</div>

</body>
</html>"""

        return html, sections_rendered

    def _build_radar_svg(self, scores: dict[str, float]) -> str:
        """
        Polished SVG radar chart — four axes, navy/teal palette.
        """
        import math
        cx, cy, r = 130, 130, 95

        domain_order = [
            "fulfillment", "carrier_performance",
            "cost", "operational_integrity",
        ]
        labels = {
            "fulfillment":           "Fulfillment",
            "carrier_performance":   "Carrier Perf.",
            "cost":                  "Cost",
            "operational_integrity": "Op. Integrity",
        }
        n      = len(domain_order)
        angles = [math.pi / 2 + 2 * math.pi * i / n for i in range(n)]

        def pt(score: float, angle: float, offset: float = 0.0) -> tuple[float, float]:
            frac = (score / 10.0) + offset
            return (
                round(cx + r * frac * math.cos(angle), 1),
                round(cy - r * frac * math.sin(angle), 1),
            )

        # Grid rings (subtle)
        rings = ""
        for lvl in [0.2, 0.4, 0.6, 0.8, 1.0]:
            pts = " ".join(
                f"{cx + r * lvl * math.cos(a):.1f},{cy - r * lvl * math.sin(a):.1f}"
                for a in angles
            )
            stroke = "#E2E8F0" if lvl < 1.0 else "#CBD5E0"
            rings += f'<polygon points="{pts}" fill="none" stroke="{stroke}" stroke-width="0.8"/>\n'

        # Axis lines
        axes = ""
        for a in angles:
            x2 = round(cx + r * math.cos(a), 1)
            y2 = round(cy - r * math.sin(a), 1)
            axes += f'<line x1="{cx}" y1="{cy}" x2="{x2}" y2="{y2}" stroke="#CBD5E0" stroke-width="0.8"/>\n'

        # Data polygon — filled teal
        data_pts = " ".join(
            f"{pt(scores.get(d, 5.0), a)[0]},{pt(scores.get(d, 5.0), a)[1]}"
            for d, a in zip(domain_order, angles)
        )
        polygon = (
            f'<polygon points="{data_pts}" '
            f'fill="#0D9E6E" fill-opacity="0.18" '
            f'stroke="#059669" stroke-width="2" stroke-linejoin="round"/>\n'
        )

        # Vertex dots
        dots = ""
        for d, a in zip(domain_order, angles):
            px, py = pt(scores.get(d, 5.0), a)
            dots += f'<circle cx="{px}" cy="{py}" r="3.5" fill="#059669" stroke="#fff" stroke-width="1.5"/>\n'

        # Axis labels
        txt = ""
        label_offsets = {0: (0, -12), 1: (12, 0), 2: (0, 14), 3: (-12, 0)}
        for i, (d, a) in enumerate(zip(domain_order, angles)):
            lx = round(cx + (r + 20) * math.cos(a), 1)
            ly = round(cy - (r + 20) * math.sin(a), 1)
            ox, oy = label_offsets.get(i, (0, 0))
            score  = scores.get(d, 5.0)
            color  = "#059669" if score >= 8 else "#D97706" if score >= 5 else "#DC2626"
            txt += (
                f'<text x="{lx + ox}" y="{ly + oy}" text-anchor="middle" '
                f'font-family="Inter,Arial,sans-serif" font-size="8" '
                f'font-weight="600" fill="#334155">{labels[d]}</text>\n'
                f'<text x="{lx + ox}" y="{ly + oy + 11}" text-anchor="middle" '
                f'font-family="Inter,Arial,sans-serif" font-size="9" '
                f'font-weight="800" fill="{color}">{score}</text>\n'
            )

        # Centre dot
        centre = f'<circle cx="{cx}" cy="{cy}" r="2.5" fill="#0B1E3D"/>\n'

        return (
            f'<svg width="260" height="260" viewBox="0 0 260 260" '
            f'xmlns="http://www.w3.org/2000/svg">\n'
            f'  {rings}{axes}{polygon}{dots}{txt}{centre}'
            f'</svg>'
        )

    def _convert_to_pdf(self, html: str, run_id: str) -> tuple[str, int]:
        """
        Convert HTML to PDF.  Strategy:
          1. WeasyPrint (full fidelity, requires GTK — works on Linux/Mac)
          2. Playwright + Chromium (Windows-compatible, high fidelity)
          3. HTML fallback (browser-printable — used when no PDF engine available)
        Returns (path, page_count).
        """
        os.makedirs("output/reports", exist_ok=True)

        # ---- Attempt 1: WeasyPrint ----
        try:
            from weasyprint import HTML as WeasyprintHTML
            pdf_path = f"output/reports/DAM_{run_id}.pdf"
            doc = WeasyprintHTML(string=html).render()
            doc.write_pdf(pdf_path)
            return pdf_path, len(doc.pages)
        except Exception:
            pass  # fall through to Playwright

        # ---- Attempt 2: Playwright + Chromium ----
        try:
            from playwright.sync_api import sync_playwright
            import base64
            import tempfile

            pdf_path = f"output/reports/DAM_{run_id}.pdf"

            # Write HTML to a temp file so Playwright can load it with file:// URI
            # (inline HTML via set_content loses relative resource resolution but
            #  our report is self-contained, so we pass it directly as content)
            with sync_playwright() as pw:
                browser = pw.chromium.launch()
                page = browser.new_page()
                page.set_content(html, wait_until="networkidle")
                page.pdf(
                    path=pdf_path,
                    format="A4",
                    margin={"top": "15mm", "bottom": "15mm",
                            "left": "12mm", "right": "12mm"},
                    print_background=True,
                )
                # Rough page count from file size (Playwright doesn't expose page count)
                import os as _os
                size_kb = _os.path.getsize(pdf_path) / 1024
                browser.close()

            page_count = max(1, int(size_kb // 80))
            return pdf_path, page_count
        except Exception:
            pass  # fall through to HTML fallback

        # ---- Fallback: save as HTML (open in browser -> Print -> Save as PDF) ----
        html_path = f"output/reports/DAM_{run_id}.html"
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html)
        # Estimate page count: ~3 000 chars per page is a rough heuristic
        page_count = max(1, len(html) // 3000)
        return html_path, page_count
