"""
pipeline.py
Chorus AI Systems — Data Analytics Manager (DAM)

The outer MVS orchestrator. Sequences all five stages, enforces the MVS
interface contract, handles system-level degradation, runs cross-stage
consistency checks, and writes the run log to disk.

Usage:
    python pipeline.py \\
        --shopify  data/shopify_orders.csv \\
        --tpl      data/tpl_shipments.csv \\
        --fedex    data/fedex_tracking.csv \\
        --dhl      data/dhl_tracking.csv \\
        --week     2026-04-04

    # Use synthetic test data:
    python pipeline.py --test

    # Layer 5 health summary (no pipeline run):
    python pipeline.py --meta

    # Adversarial test suite:
    python pipeline.py --adversarial
"""

import argparse
import json
import os
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Union

from dotenv import load_dotenv

# Load OPENROUTER_API_KEY (and any other secrets) from app/.env before any
# module that reads env vars is imported.
load_dotenv(Path(__file__).resolve().parent / ".env")

from core.schemas import (
    DegradationLevel, DegradationSignal, HealthTelemetry,
    RunLog, Stage1Input, Stage2Input, Stage3Input, Stage4Input,
    Stage5Input, Stage6Input, Stage1Output, Stage2Output, Stage3Output,
    Stage4Output, Stage6Output, VerifiedOutput,
)
from core.stages import Stage1, Stage2, Stage3, Stage4, Stage5
from core.stage6_supply_chain_advisor import Stage6SupplyChainAdvisor
from core.factlist_store import save_factlist, load_prior_factlist, get_baseline_status
from core.llm_client import preflight_models, ALL_PIPELINE_MODELS

StageResult = Union[VerifiedOutput, DegradationSignal]

LOG_DIR           = "output/run_logs"
FACTLIST_DIR      = "data/factlists"
REPORT_DATA_DIR   = "output/report_data"


# ===========================================================================
# ORCHESTRATOR
# ===========================================================================

class DAMOrchestrator:
    """
    Outer MVS. Calls each stage in sequence, receives VerifiedOutput or
    DegradationSignal, makes system-level degradation decisions.

    Does NOT reach inside stage internals.
    Coordinates; does not govern stage execution.
    """

    def run(
        self,
        shopify_path: str,
        tpl_path:     str,
        fedex_path:   str,
        dhl_path:     str,
        week_date:    str,
        skip_preflight: bool = False,
    ) -> RunLog:

        run_id   = f"DAM-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"
        started  = datetime.now()
        log      = RunLog(run_id=run_id, started_at=started)
        signals: list[DegradationSignal] = []

        print(f"\n{'='*60}")
        print(f"  Chorus AI — Data Analytics Manager")
        print(f"  Run ID : {run_id}")
        print(f"  Week   : {week_date}")
        print(f"{'='*60}\n")

        # Preflight: verify every configured model is reachable on OpenRouter
        # before burning time on Stage 1. Halts (does not degrade) on failure —
        # model selection is part of the pipeline's value proposition.
        if not skip_preflight:
            print("[ Preflight ] Checking model availability on OpenRouter...")
            checks = preflight_models(ALL_PIPELINE_MODELS)
            failed = {m: e for m, e in checks.items() if e}
            if failed:
                print(f"  x {len(failed)} model(s) unreachable:")
                for m, e in failed.items():
                    print(f"    - {m}\n        {e}")
                raise SystemExit(
                    "Aborting: one or more configured models are unavailable. "
                    "Fix model IDs in app/core/llm_client.py or check "
                    "OPENROUTER_API_KEY before retrying."
                )
            print(f"  + {len(checks)} models reachable\n")

        # ----------------------------------------------------------------
        # STAGE 1
        # ----------------------------------------------------------------
        print("[ Stage 1 ] Ingestion & Normalization...")
        s1_input = Stage1Input(
            shopify_csv_path=shopify_path,
            tpl_csv_path=tpl_path,
            fedex_csv_path=fedex_path,
            dhl_csv_path=dhl_path,
            run_id=run_id,
        )
        s1_result = Stage1().run(s1_input)
        self._record_telemetry(log, s1_result)

        if isinstance(s1_result, DegradationSignal):
            signals.append(s1_result)
            print(f"  x Stage 1 failed -- HALT: {s1_result.failure_reason}")
            return self._finalise(log, "halted", DegradationLevel.halt,
                                  signals, started)

        s1_out: Stage1Output = s1_result.payload
        print(f"  + {len(s1_out.canonical_orders)} orders  "
              f"| {len(s1_out.canonical_shipments)} shipments  "
              f"| {len(s1_out.canonical_carrier_shipments)} carrier records")
        if s1_out.field_mapping_log.ambiguous_field_count:
            print(f"  ! {s1_out.field_mapping_log.ambiguous_field_count} "
                  f"ambiguous field mapping(s) -- see field_mapping_log")

        # ----------------------------------------------------------------
        # STAGE 2
        # ----------------------------------------------------------------
        print("\n[ Stage 2 ] Reconciliation...")
        s2_input = Stage2Input(
            canonical_orders=s1_out.canonical_orders,
            canonical_shipments=s1_out.canonical_shipments,
            canonical_carrier_shipments=s1_out.canonical_carrier_shipments,
            run_id=run_id,
        )
        s2_result = Stage2().run(s2_input)
        self._record_telemetry(log, s2_result)

        if isinstance(s2_result, DegradationSignal):
            signals.append(s2_result)
            print(f"  x Stage 2 failed -- HALT: {s2_result.failure_reason}")
            return self._finalise(log, "halted", DegradationLevel.halt,
                                  signals, started)

        s2_out: Stage2Output = s2_result.payload
        print(f"  + Match rate: {s2_out.exact_match_rate:.1%}  "
              f"| Fuzzy matches: {s2_out.fuzzy_match_volume}  "
              f"| Unmatched: {s2_out.unmatched_count}")
        if s2_out.exact_match_rate < 0.95:
            print(f"  ! Match rate below 95% -- disclosed in report")

        # ----------------------------------------------------------------
        # STAGE 3
        # ----------------------------------------------------------------
        print("\n[ Stage 3 ] KPI Computation...")
        prior_factlist = load_prior_factlist(week_date)
        s3_input = Stage3Input(
            reconciliation_shipments=s2_out.reconciliation_shipments,
            canonical_orders=s1_out.canonical_orders,
            canonical_shipments=s1_out.canonical_shipments,
            canonical_carrier_shipments=s1_out.canonical_carrier_shipments,
            prior_week_factlist=prior_factlist,
            run_id=run_id,
            week_date=week_date,
        )
        s3_result = Stage3().run(s3_input)
        self._record_telemetry(log, s3_result)

        if isinstance(s3_result, DegradationSignal):
            signals.append(s3_result)
            print(f"  x Stage 3 failed -- partial output: {s3_result.failure_reason}")
            return self._finalise(log, "partial", DegradationLevel.partial,
                                  signals, started)

        s3_out: Stage3Output = s3_result.payload
        print(f"  + {len(s3_out.factlist)} KPIs computed  "
              f"| LLM/Python mismatches: {s3_out.kpi_mismatch_count}")
        for f in s3_out.factlist:
            status_icon = {"green": "[G]", "yellow": "[Y]",
                           "red": "[R]", "informational": "[ ]"}.get(
                f.threshold_status.value, "[ ]"
            )
            print(f"    {status_icon}  {f.kpi_name:30s}  {f.final_value:.4f}")

        # Persist FactList via factlist_store (cost baseline tracking)
        saved_path = save_factlist(s3_out.factlist, week_date)
        print(f"  FactList saved : {saved_path}")
        baseline_note = get_baseline_status(week_date)
        print(f"  Baseline status: {baseline_note}")
        log.kpi_mismatch_count = s3_out.kpi_mismatch_count

        # ----------------------------------------------------------------
        # STAGE 4
        # ----------------------------------------------------------------
        print("\n[ Stage 4 ] Insight Generation + Verification...")
        s4_input = Stage4Input(
            factlist=s3_out.factlist,
            python_verified=s3_out.python_verified,
            run_id=run_id,
            week_date=week_date,
        )
        s4_result = Stage4().run(s4_input)
        self._record_telemetry(log, s4_result)

        if isinstance(s4_result, DegradationSignal):
            signals.append(s4_result)
            print(f"  ! Stage 4 failed -- partial: {s4_result.failure_reason}")
            # Build report without insights (Level 1 degradation).
            # Stage 5 handles empty verified_insights gracefully.
            s4_out = Stage4Output(
                verified_insights=[],
                claim_count_generated=0,
                claim_acceptance_rate=0.0,
                cross_verifier_agreement=0.0,
                stripped_claim_log=[],
                domain_recommendations={},
            )
        else:
            s4_out: Stage4Output = s4_result.payload
            print(f"  + {len(s4_out.verified_insights)} verified insights  "
                  f"| Acceptance rate: {s4_out.claim_acceptance_rate:.0%}  "
                  f"| Verifier agreement: {s4_out.cross_verifier_agreement:.0%}")
            log.claim_acceptance_rate = s4_out.claim_acceptance_rate
            log.cross_verifier_agreement = s4_out.cross_verifier_agreement
            log.claim_audit_log = s4_out.verified_insights + s4_out.stripped_claim_log

            # Meta-governance: flag 100% agreement for 3+ consecutive runs
            self._check_verifier_agreement(s4_out.cross_verifier_agreement, run_id)

        # ---- Orchestrator L4: cross-stage FACT_ID consistency check ----
        valid_fact_ids = {f.fact_id for f in s3_out.factlist}
        orphaned = [
            ins for ins in s4_out.verified_insights
            if not all(fid in valid_fact_ids for fid in ins.cited_fact_ids)
        ]
        if orphaned:
            print(f"  ! {len(orphaned)} insight(s) cite FACT_IDs not in FactList -- stripped")
            s4_out.verified_insights = [
                i for i in s4_out.verified_insights if i not in orphaned
            ]

        # ----------------------------------------------------------------
        # STAGE 6
        # ----------------------------------------------------------------
        print("\n[ Stage 6 ] Supply Chain Advisor (RAG)...")
        s6_input = Stage6Input(
            stage4_output=s4_out,
            factlist=s3_out.factlist,
        )
        s6_result = Stage6SupplyChainAdvisor().run(s6_input)
        self._record_telemetry(log, s6_result)

        if isinstance(s6_result, DegradationSignal):
            signals.append(s6_result)
            s6_out: Stage6Output = None
            print(f"  ! Stage 6 degraded -- report continues without expert commentary")
            print(f"    Reason: {s6_result.failure_reason}")
        else:
            s6_out: Stage6Output = s6_result.payload
            print(f"  + {len(s6_out.domain_blocks)} domain blocks  "
                  f"| {s6_out.total_chunks_retrieved} chunks retrieved")
            if s6_out.domains_skipped:
                print(f"  ! Domains skipped: {[d.value for d in s6_out.domains_skipped]}")

        # ----------------------------------------------------------------
        # STAGE 5
        # ----------------------------------------------------------------
        print("\n[ Stage 5 ] Report Compilation...")
        report_week = self._format_week_label(week_date)
        s5_input = Stage5Input(
            stage1_output=s1_out,
            stage2_output=s2_out,
            stage3_output=s3_out,
            stage4_output=s4_out,
            stage6_output=s6_out,
            degradation_signals=signals,
            run_id=run_id,
            report_week=report_week,
        )
        s5_result = Stage5().run(s5_input)
        self._record_telemetry(log, s5_result)

        if isinstance(s5_result, DegradationSignal):
            signals.append(s5_result)
            print(f"  x Stage 5 failed: {s5_result.failure_reason}")
            return self._finalise(log, "partial", DegradationLevel.partial,
                                  signals, started)

        s5_out: Stage5Output = s5_result.payload
        print(f"  + PDF: {s5_out.pdf_path}  "
              f"| Pages: {s5_out.page_count}  "
              f"| Render: {s5_out.render_time_s:.1f}s")
        if s5_out.html_path:
            print(f"  + Site: output/site/index.html")

        log.pdf_path = s5_out.pdf_path
        final_status = "partial" if signals else "full"

        self._save_report_data(log, s3_out.factlist, s4_out.verified_insights,
                               week_date, report_week, final_status, signals, s6_out,
                               s5_out.html_path)

        print(f"\n{'='*60}")
        print(f"  Run complete -- status: {final_status.upper()}")
        print(f"  Report: {s5_out.pdf_path}")
        print(f"{'='*60}\n")

        return self._finalise(log, final_status, DegradationLevel.normal,
                              signals, started)

    # ----------------------------------------------------------------
    # Helpers
    # ----------------------------------------------------------------

    def _record_telemetry(self, log: RunLog, result: StageResult) -> None:
        t = result.health_telemetry
        log.stage_telemetry.append(t)
        log.total_api_cost_usd += t.api_cost_usd
        log.total_latency_s    += t.latency_seconds
        if t.model_used and t.model_used not in log.models_used:
            log.models_used.append(t.model_used)
        if t.fallback_activated:
            log.fallback_activated = True

    def _finalise(
        self,
        log: RunLog,
        status: str,
        level: DegradationLevel,
        signals: list[DegradationSignal],
        started: datetime,
    ) -> RunLog:
        log.completed_at        = datetime.now()
        log.final_status        = status
        log.degradation_level   = level
        log.degradation_signals = signals
        log.total_latency_s     = round(log.total_latency_s, 2)
        log.total_api_cost_usd  = round(log.total_api_cost_usd, 6)
        self._save_run_log(log)
        print(f"\n  Total API cost : ${log.total_api_cost_usd:.6f}")
        print(f"  Total latency  : {log.total_latency_s:.1f}s")
        return log

    def _save_report_data(
        self,
        log: RunLog,
        factlist,
        insights,
        week_date: str,
        report_week: str,
        final_status: str,
        signals: list,
        s6_out=None,
        html_path: str = None,
    ) -> None:
        os.makedirs(REPORT_DATA_DIR, exist_ok=True)
        data = {
            "run_id":                 log.run_id,
            "week_date":              week_date,
            "report_week":            report_week,
            "generated_at":           datetime.now().isoformat(),
            "final_status":           final_status,
            "factlist":               [f.model_dump(mode="json") for f in factlist],
            "insights":               [i.model_dump(mode="json") for i in insights],
            "stage_telemetry":        [t.model_dump(mode="json") for t in log.stage_telemetry],
            "total_api_cost_usd":     log.total_api_cost_usd,
            "total_latency_s":        log.total_latency_s,
            "models_used":            log.models_used,
            "claim_acceptance_rate":  log.claim_acceptance_rate,
            "cross_verifier_agreement": log.cross_verifier_agreement,
            "kpi_mismatch_count":     log.kpi_mismatch_count,
            "html_path":              html_path or log.pdf_path,
            "stage6_domains_processed": len(s6_out.domain_blocks) if s6_out else 0,
            "stage6_chunks_retrieved":  s6_out.total_chunks_retrieved if s6_out else 0,
            "stage6_domains_skipped":   [d.value for d in s6_out.domains_skipped] if s6_out else [],
        }
        path = f"{REPORT_DATA_DIR}/{log.run_id}.json"
        with open(path, "w") as f:
            json.dump(data, f, indent=2, default=str)
        print(f"  Dashboard data : {path}")

    def _save_run_log(self, log: RunLog) -> None:
        os.makedirs(LOG_DIR, exist_ok=True)
        path = f"{LOG_DIR}/{log.run_id}.json"
        with open(path, "w") as f:
            json.dump(log.model_dump(mode="json"), f, indent=2, default=str)
        print(f"  Run log        : {path}")

    def _format_week_label(self, week_date: str) -> str:
        try:
            end   = datetime.strptime(week_date, "%Y-%m-%d")
            start = end - timedelta(days=6)
            return f"{start.strftime('%b %d')} - {end.strftime('%b %d, %Y')}"
        except Exception:
            return week_date

    def _check_verifier_agreement(self, agreement: float, run_id: str) -> None:
        """
        Meta-governance: flag if verifier agrees 100% for 3 consecutive runs.
        Writes a flag file to run_logs/verifier_agreement_flag.json.
        """
        flag_path = f"{LOG_DIR}/verifier_agreement_flag.json"
        history = []
        if os.path.exists(flag_path):
            try:
                with open(flag_path) as f:
                    history = json.load(f)
            except Exception:
                history = []

        history.append({"run_id": run_id, "agreement": agreement})
        history = history[-10:]  # keep last 10

        with open(flag_path, "w") as f:
            json.dump(history, f, indent=2)

        recent = history[-3:]
        if len(recent) == 3 and all(r["agreement"] == 1.0 for r in recent):
            print("  ! META-GOVERNANCE ALERT: Verifier agreed 100% for 3 "
                  "consecutive runs. Review Stage 4 verification prompt.")


# ===========================================================================
# CLI ENTRY POINT
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Chorus AI -- Data Analytics Manager"
    )
    parser.add_argument("--shopify", help="Path to Shopify orders CSV")
    parser.add_argument("--tpl",     help="Path to 3PL shipments CSV")
    parser.add_argument("--fedex",   help="Path to FedEx tracking CSV")
    parser.add_argument("--dhl",     help="Path to DHL tracking CSV")
    parser.add_argument("--week",    help="Week end date (YYYY-MM-DD). "
                        "Defaults to today, or 2026-04-04 in --test mode.",
                        default=None)
    parser.add_argument("--test",    action="store_true",
                        help="Use synthetic test data from test_data/")
    parser.add_argument("--meta",    action="store_true",
                        help="Show Layer 5 health summary (no pipeline run)")
    parser.add_argument("--adversarial", action="store_true",
                        help="Run adversarial test suite against planted-error data")
    parser.add_argument("--skip-preflight", action="store_true",
                        help="Skip OpenRouter model availability check (faster dev iteration)")
    args = parser.parse_args()

    # ---- --meta: Layer 5 health summary ----
    if args.meta:
        from core.meta_governance import print_layer5_summary
        print_layer5_summary()
        return

    # ---- --adversarial: gate-verification test suite ----
    if args.adversarial:
        from core.meta_governance import AdversarialRunner
        runner = AdversarialRunner()
        results = runner.run_all()
        runner.print_report(results)
        return

    # ---- Normal pipeline run ----
    if args.test:
        shopify = "data/test/shopify_orders.csv"
        tpl     = "data/test/tpl_shipments.csv"
        fedex   = "data/test/fedex_tracking.csv"
        dhl     = "data/test/dhl_tracking.csv"
        week    = args.week or "2026-04-04"
    else:
        if not all([args.shopify, args.tpl, args.fedex, args.dhl]):
            print("Error: provide --shopify, --tpl, --fedex, --dhl "
                  "or use --test for synthetic data.")
            sys.exit(1)
        shopify = args.shopify
        tpl     = args.tpl
        fedex   = args.fedex
        dhl     = args.dhl
        week    = args.week or datetime.now().strftime("%Y-%m-%d")

    orchestrator = DAMOrchestrator()
    orchestrator.run(shopify, tpl, fedex, dhl, week, skip_preflight=args.skip_preflight)


if __name__ == "__main__":
    main()
