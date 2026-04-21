"""
meta_governance.py
Chorus AI Systems — Data Analytics Manager (DAM)

Layer 5: Adaptive Intelligence — meta-governance implementation.

This module observes the pipeline's own observation processes (Principle 13).
It does not govern individual runs; it monitors patterns across runs and
raises structured alerts when those patterns signal configuration drift,
model degradation, or verification blind spots.

Three responsibilities:
  1. Layer5Monitor   — reads run logs, computes rolling metrics, emits alerts
  2. CalibrationLog  — records human-vs-system verdict comparisons
  3. AdversarialRunner — runs the pipeline on planted-error test data and
                         verifies each gate catches its assigned error

Bounded authority (spec §3.4):
  Layer 5 detects and recommends. It cannot modify thresholds, swap models,
  or release output. All alerts are written to disk for operator review.
"""

from __future__ import annotations

import json
import os
import statistics
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from typing import Optional

LOG_DIR            = "output/run_logs"
META_DIR           = "output/meta_governance"
CALIBRATION_FILE   = os.path.join(META_DIR, "calibration_log.json")
ALERT_FILE         = os.path.join(META_DIR, "alerts.json")
AGREEMENT_FILE     = os.path.join(LOG_DIR,  "verifier_agreement_flag.json")


# ---------------------------------------------------------------------------
# ALERT DATACLASS
# ---------------------------------------------------------------------------

@dataclass
class Alert:
    """A structured Layer 5 alert. Written to disk; never acted on autonomously."""
    alert_id:      str
    raised_at:     str          # ISO datetime
    alert_type:    str          # e.g. "rising_retry_rate", "agreement_lock"
    severity:      str          # "warning" | "critical"
    description:   str
    metric_value:  float
    threshold:     float
    recommendation: str
    resolved:      bool = False


# ---------------------------------------------------------------------------
# LAYER 5 MONITOR
# ---------------------------------------------------------------------------

class Layer5Monitor:
    """
    Reads structured run logs and computes rolling health metrics across runs.
    Emits alerts when metrics cross thresholds defined in the spec.

    Thresholds (from spec §3.3):
      - retry_rate_7d > 20%          → warning
      - halt_rate_7d > 5%            → warning
      - avg_grounding_score drop > 3% → warning (via claim_acceptance_rate proxy)
      - kpi_mismatch_rate rising     → warning
      - verifier_agreement == 1.0 for 3 consecutive runs → critical
      - any single stage failing > 30% of runs → warning
    """

    THRESHOLDS = {
        "retry_rate":          0.20,   # > 20% of stages needed a retry
        "halt_rate":           0.05,   # > 5% of runs halted
        "claim_acceptance_low": 0.70,  # < 70% claims passing verification
        "kpi_mismatch_high":   3,      # > 3 mismatches per run average
        "agreement_lock_runs": 3,      # 3 consecutive runs at 100% agreement
        "stage_failure_rate":  0.30,   # any stage failing > 30% of runs
    }

    def analyze(self, n_recent: int = 10) -> list[Alert]:
        """
        Load the n most recent run logs and compute rolling health metrics.
        Returns a list of Alerts (may be empty if all metrics are healthy).
        Persists alerts to META_DIR/alerts.json.
        """
        logs = self._load_recent_logs(n_recent)
        if len(logs) < 2:
            return []   # need at least 2 runs to detect trends

        alerts: list[Alert] = []
        now = datetime.now().isoformat()

        # ---- Halt rate ----
        halt_count = sum(1 for l in logs if l.get("final_status") == "halted")
        halt_rate  = halt_count / len(logs)
        if halt_rate > self.THRESHOLDS["halt_rate"]:
            alerts.append(Alert(
                alert_id=f"HALT-{now[:10]}",
                raised_at=now,
                alert_type="high_halt_rate",
                severity="critical",
                description=(
                    f"{halt_count}/{len(logs)} recent runs halted "
                    f"({halt_rate:.0%}). Systemic data quality or "
                    f"configuration issue likely."
                ),
                metric_value=halt_rate,
                threshold=self.THRESHOLDS["halt_rate"],
                recommendation=(
                    "Review run logs for Stage 1/2 failure reasons. "
                    "Check upstream CSV format for changes. "
                    "Run adversarial test suite to isolate failure category."
                ),
            ))

        # ---- Retry rate (across all stages) ----
        all_retries = []
        all_stages  = []
        for l in logs:
            for t in l.get("stage_telemetry", []):
                all_stages.append(t)
                all_retries.append(t.get("retry_count", 0))

        if all_stages:
            stages_with_retry = sum(1 for r in all_retries if r > 0)
            retry_rate = stages_with_retry / len(all_stages)
            if retry_rate > self.THRESHOLDS["retry_rate"]:
                alerts.append(Alert(
                    alert_id=f"RETRY-{now[:10]}",
                    raised_at=now,
                    alert_type="rising_retry_rate",
                    severity="warning",
                    description=(
                        f"{retry_rate:.0%} of stage calls required a retry "
                        f"across the last {len(logs)} runs. "
                        f"Prompt quality or model stability may have degraded."
                    ),
                    metric_value=retry_rate,
                    threshold=self.THRESHOLDS["retry_rate"],
                    recommendation=(
                        "Identify which stage has the highest retry rate from "
                        "stage_telemetry. Review that stage's prompt template. "
                        "Check if the fallback model is activating more than baseline."
                    ),
                ))

        # ---- Claim acceptance rate ----
        acceptance_rates = [
            l["claim_acceptance_rate"]
            for l in logs
            if l.get("claim_acceptance_rate") is not None
        ]
        if acceptance_rates:
            avg_acceptance = statistics.mean(acceptance_rates)
            if avg_acceptance < self.THRESHOLDS["claim_acceptance_low"]:
                alerts.append(Alert(
                    alert_id=f"ACCEPT-{now[:10]}",
                    raised_at=now,
                    alert_type="low_claim_acceptance",
                    severity="warning",
                    description=(
                        f"Average Stage 4 claim acceptance rate is "
                        f"{avg_acceptance:.0%} across the last {len(logs)} runs. "
                        f"Generation model may be producing poorly-cited insights."
                    ),
                    metric_value=avg_acceptance,
                    threshold=self.THRESHOLDS["claim_acceptance_low"],
                    recommendation=(
                        "Review Stage 4 generation prompt. Check if FactList "
                        "structure has changed. Consider prompt revision flag."
                    ),
                ))

        # ---- KPI mismatch rate ----
        mismatch_counts = [
            l.get("kpi_mismatch_count", 0)
            for l in logs
            if l.get("final_status") != "halted"
        ]
        if mismatch_counts:
            avg_mismatches = statistics.mean(mismatch_counts)
            if avg_mismatches > self.THRESHOLDS["kpi_mismatch_high"]:
                alerts.append(Alert(
                    alert_id=f"MISMATCH-{now[:10]}",
                    raised_at=now,
                    alert_type="high_kpi_mismatch_rate",
                    severity="warning",
                    description=(
                        f"Average LLM/Python KPI mismatch is {avg_mismatches:.1f} "
                        f"per run across the last {len(logs)} runs. "
                        f"Stage 3 generation model may be drifting on formula logic."
                    ),
                    metric_value=avg_mismatches,
                    threshold=self.THRESHOLDS["kpi_mismatch_high"],
                    recommendation=(
                        "Identify which KPIs mismatch most often from stage_telemetry. "
                        "Revise Stage 3 prompt to clarify those formulas. "
                        "Python always wins on mismatch — report accuracy is unaffected, "
                        "but rising mismatch signals model drift."
                    ),
                ))

        # ---- Verifier agreement lock ----
        agreement_alert = self._check_agreement_lock(logs)
        if agreement_alert:
            alerts.append(agreement_alert)

        # ---- Per-stage failure distribution ----
        stage_failures: dict[str, int] = {}
        stage_appearances: dict[str, int] = {}
        for l in logs:
            for sig in l.get("degradation_signals", []):
                stage = sig.get("stage", "unknown")
                stage_failures[stage] = stage_failures.get(stage, 0) + 1
            for t in l.get("stage_telemetry", []):
                stage = t.get("stage", "unknown")
                stage_appearances[stage] = stage_appearances.get(stage, 0) + 1

        for stage, fail_count in stage_failures.items():
            appearances = stage_appearances.get(stage, len(logs))
            if appearances > 0:
                fail_rate = fail_count / appearances
                if fail_rate > self.THRESHOLDS["stage_failure_rate"]:
                    alerts.append(Alert(
                        alert_id=f"STAGE-{stage.upper()}-{now[:10]}",
                        raised_at=now,
                        alert_type="high_stage_failure_rate",
                        severity="warning",
                        description=(
                            f"{stage} is failing in {fail_rate:.0%} of runs "
                            f"({fail_count}/{appearances}). "
                            f"Prompt or data format issue specific to this stage."
                        ),
                        metric_value=fail_rate,
                        threshold=self.THRESHOLDS["stage_failure_rate"],
                        recommendation=(
                            f"Review {stage} prompt template and recent "
                            f"DegradationSignal failure_reason messages. "
                            f"Check if upstream data format has changed."
                        ),
                    ))

        self._save_alerts(alerts)
        return alerts

    def summary(self, n_recent: int = 10) -> dict:
        """
        Return a human-readable summary dict of rolling health metrics.
        Used for console output and operator dashboards.
        """
        logs = self._load_recent_logs(n_recent)
        if not logs:
            return {"status": "no_data", "runs_analysed": 0}

        statuses = [l.get("final_status", "unknown") for l in logs]
        costs    = [l.get("total_api_cost_usd", 0) for l in logs]
        latencies = [l.get("total_latency_s", 0) for l in logs]
        acceptance = [
            l["claim_acceptance_rate"] for l in logs
            if l.get("claim_acceptance_rate") is not None
        ]
        mismatches = [l.get("kpi_mismatch_count", 0) for l in logs]

        return {
            "runs_analysed":           len(logs),
            "full_rate":               f"{statuses.count('full') / len(logs):.0%}",
            "partial_rate":            f"{statuses.count('partial') / len(logs):.0%}",
            "halt_rate":               f"{statuses.count('halted') / len(logs):.0%}",
            "avg_api_cost_usd":        f"${statistics.mean(costs):.4f}" if costs else "N/A",
            "avg_latency_s":           f"{statistics.mean(latencies):.1f}s" if latencies else "N/A",
            "avg_claim_acceptance":    f"{statistics.mean(acceptance):.0%}" if acceptance else "N/A",
            "avg_kpi_mismatches":      f"{statistics.mean(mismatches):.1f}" if mismatches else "N/A",
            "fallback_activation_rate": (
                f"{sum(1 for l in logs if l.get('fallback_activated')) / len(logs):.0%}"
            ),
        }

    # ---- Helpers ----

    def _load_recent_logs(self, n: int) -> list[dict]:
        if not os.path.exists(LOG_DIR):
            return []
        files = sorted(
            f for f in os.listdir(LOG_DIR)
            if f.endswith(".json") and f != "verifier_agreement_flag.json"
        )
        recent = files[-n:]
        logs = []
        for fname in recent:
            try:
                with open(os.path.join(LOG_DIR, fname)) as f:
                    logs.append(json.load(f))
            except Exception:
                continue
        return logs

    def _check_agreement_lock(self, logs: list[dict]) -> Optional[Alert]:
        """
        Flag if verifier agreed 100% for 3 or more consecutive runs.
        Persistent total agreement means the verifier is not catching anything.
        """
        recent_agreements = [
            l.get("cross_verifier_agreement")
            for l in logs[-3:]
            if l.get("cross_verifier_agreement") is not None
        ]
        if (len(recent_agreements) == 3
                and all(a == 1.0 for a in recent_agreements)):
            return Alert(
                alert_id=f"AGREE-LOCK-{datetime.now().strftime('%Y%m%d')}",
                raised_at=datetime.now().isoformat(),
                alert_type="verifier_agreement_lock",
                severity="critical",
                description=(
                    "Stage 4 verifier agreed with 100% of generated claims "
                    "for 3 consecutive runs. The verifier may have stopped "
                    "catching errors — this is a governance blind spot signal."
                ),
                metric_value=1.0,
                threshold=1.0,
                recommendation=(
                    "Run the adversarial test suite immediately. "
                    "Review Stage 4 verification prompt for over-permissiveness. "
                    "Consider injecting known-bad claims into Stage 4 to test "
                    "whether the verifier actually rejects them."
                ),
            )
        return None

    def _save_alerts(self, alerts: list[Alert]) -> None:
        if not alerts:
            return
        os.makedirs(META_DIR, exist_ok=True)
        alert_file = os.path.join(META_DIR, "alerts.json")
        existing = []
        if os.path.exists(alert_file):
            try:
                with open(alert_file) as f:
                    existing = json.load(f)
            except Exception:
                existing = []
        all_alerts = existing + [asdict(a) for a in alerts]
        with open(alert_file, "w") as f:
            json.dump(all_alerts, f, indent=2)


# ---------------------------------------------------------------------------
# CALIBRATION LOG
# ---------------------------------------------------------------------------

class CalibrationLog:
    """
    Records human reviewer verdicts alongside system verdicts for Stage 4 claims.
    Used to track false positive rate (system approved bad claim) and
    false negative rate (system rejected good claim).

    Spec §11.3: alert if false positive rate exceeds 5%.
    """

    def __init__(self):
        os.makedirs(META_DIR, exist_ok=True)
        self.path = CALIBRATION_FILE

    def record(
        self,
        run_id:           str,
        claim_text:       str,
        system_verdict:   str,   # "aligned" or "stripped"
        human_verdict:    str,   # "aligned" or "stripped"
        reviewer_note:    str = "",
    ) -> None:
        """Add one human-vs-system verdict comparison to the log."""
        entry = {
            "recorded_at":    datetime.now().isoformat(),
            "run_id":         run_id,
            "claim_text":     claim_text[:200],   # truncate for storage
            "system_verdict": system_verdict,
            "human_verdict":  human_verdict,
            "agreement":      system_verdict == human_verdict,
            "false_positive": system_verdict == "aligned" and human_verdict == "stripped",
            "false_negative": system_verdict == "stripped" and human_verdict == "aligned",
            "reviewer_note":  reviewer_note,
        }
        records = self._load()
        records.append(entry)
        with open(self.path, "w") as f:
            json.dump(records, f, indent=2)

    def metrics(self) -> dict:
        """
        Compute calibration metrics across all recorded verdicts.
        Returns false_positive_rate, false_negative_rate, agreement_rate,
        total_reviewed, and an alert flag if FP rate > 5%.
        """
        records = self._load()
        if not records:
            return {"status": "no_calibration_data", "total_reviewed": 0}

        total  = len(records)
        fp     = sum(1 for r in records if r.get("false_positive"))
        fn     = sum(1 for r in records if r.get("false_negative"))
        agree  = sum(1 for r in records if r.get("agreement"))
        fp_rate = fp / total
        fn_rate = fn / total

        return {
            "total_reviewed":     total,
            "false_positive_rate": round(fp_rate, 4),
            "false_negative_rate": round(fn_rate, 4),
            "agreement_rate":      round(agree / total, 4),
            "fp_alert":            fp_rate > 0.05,
            "fn_alert":            fn_rate > 0.10,
            "alert_message": (
                f"FALSE POSITIVE RATE {fp_rate:.0%} exceeds 5% threshold. "
                f"Verifier is approving bad claims. Review Stage 4 verification prompt."
                if fp_rate > 0.05 else None
            ),
        }

    def _load(self) -> list[dict]:
        if not os.path.exists(self.path):
            return []
        try:
            with open(self.path) as f:
                return json.load(f)
        except Exception:
            return []


# ---------------------------------------------------------------------------
# ADVERSARIAL TEST RUNNER
# ---------------------------------------------------------------------------

class AdversarialRunner:
    """
    Runs the DAM pipeline on planted-error test data and verifies that
    each gate catches its assigned error category.

    Four test categories (spec §11.2):
      1. corrupted_tracking   — Gate 2 (match rate drops) + Stage 2 fuzzy
      2. duplicate_order_ids  — Gate 1 (schema hard fail)
      3. mismatched_timestamps — Gate 1 + Stage 3 KPI (negative transit times)
      4. missing_required_fields — Gate 1 hard fail

    Usage:
        runner = AdversarialRunner()
        results = runner.run_all()
        runner.print_report(results)
    """

    ADVERSARIAL_DIR = "data/test/adversarial"
    RESULTS_FILE    = os.path.join(META_DIR, "adversarial_results.json")

    EXPECTED_CATCHES = {
        "duplicate_order_ids":      {"stage": "stage_1", "signal_type": "DegradationSignal"},
        "missing_required_fields":  {"stage": "stage_1", "signal_type": "DegradationSignal"},
        "mismatched_timestamps":    {"stage": "stage_1", "signal_type": "DegradationSignal"},
        "corrupted_tracking":       {"stage": "stage_2", "min_unmatched": 3},
    }

    def run_all(self) -> list[dict]:
        """
        Run each adversarial test case through the pipeline and check
        whether the appropriate gate caught the planted error.
        Returns a list of result dicts.
        """
        from core.stages import Stage1, Stage2
        from core.schemas import Stage1Input, Stage2Input

        results = []

        for category, expectation in self.EXPECTED_CATCHES.items():
            test_dir = os.path.join(self.ADVERSARIAL_DIR, category)
            if not os.path.exists(test_dir):
                results.append({
                    "category": category,
                    "status":   "SKIP",
                    "reason":   f"Test data not found at {test_dir}",
                })
                continue

            shopify = os.path.join(test_dir, "shopify_orders.csv")
            tpl     = os.path.join(test_dir, "tpl_shipments.csv")
            fedex   = os.path.join(test_dir, "fedex_tracking.csv")
            dhl     = os.path.join(test_dir, "dhl_tracking.csv")

            for path in [shopify, tpl, fedex, dhl]:
                if not os.path.exists(path):
                    results.append({
                        "category": category,
                        "status":   "SKIP",
                        "reason":   f"Missing file: {path}",
                    })
                    break
            else:
                result = self._run_category(
                    category, expectation, shopify, tpl, fedex, dhl
                )
                results.append(result)

        self._save_results(results)
        return results

    def _run_category(
        self,
        category:    str,
        expectation: dict,
        shopify:     str,
        tpl:         str,
        fedex:       str,
        dhl:         str,
    ) -> dict:
        from core.stages import Stage1, Stage2
        from core.schemas import Stage1Input, Stage2Input, DegradationSignal, VerifiedOutput

        run_id = f"ADV-{category}-{datetime.now().strftime('%H%M%S')}"

        # Stage 1
        s1_result = Stage1().run(Stage1Input(
            shopify_csv_path=shopify, tpl_csv_path=tpl,
            fedex_csv_path=fedex, dhl_csv_path=dhl,
            run_id=run_id,
        ))

        expected_stage = expectation.get("stage")
        expected_signal = expectation.get("signal_type")

        # Category expected to be caught at Stage 1
        if expected_stage == "stage_1":
            caught = isinstance(s1_result, DegradationSignal)
            return {
                "category":      category,
                "status":        "PASS" if caught else "FAIL",
                "expected":      f"DegradationSignal from stage_1",
                "actual":        (
                    f"DegradationSignal: {s1_result.failure_reason[:100]}"
                    if caught else
                    f"VerifiedOutput — error not caught"
                ),
                "run_id":        run_id,
                "tested_at":     datetime.now().isoformat(),
            }

        # Category expected to be caught at Stage 2
        if expected_stage == "stage_2":
            if isinstance(s1_result, DegradationSignal):
                return {
                    "category": category,
                    "status":   "FAIL",
                    "expected": "Stage 1 should pass; Stage 2 should flag",
                    "actual":   f"Stage 1 halted unexpectedly: {s1_result.failure_reason[:100]}",
                    "run_id":   run_id,
                    "tested_at": datetime.now().isoformat(),
                }

            s1_out = s1_result.payload
            s2_result = Stage2().run(Stage2Input(
                canonical_orders=s1_out.canonical_orders,
                canonical_shipments=s1_out.canonical_shipments,
                canonical_carrier_shipments=s1_out.canonical_carrier_shipments,
                run_id=run_id,
            ))

            min_unmatched = expectation.get("min_unmatched", 1)
            if isinstance(s2_result, DegradationSignal):
                # Halt from low match rate — counts as caught
                caught = True
                actual = f"DegradationSignal: {s2_result.failure_reason[:100]}"
            elif isinstance(s2_result, VerifiedOutput):
                unmatched = s2_result.payload.unmatched_count
                caught = unmatched >= min_unmatched
                actual = (
                    f"VerifiedOutput with {unmatched} unmatched records "
                    f"(expected >= {min_unmatched})"
                )
            else:
                caught = False
                actual = "Unexpected result type"

            return {
                "category":  category,
                "status":    "PASS" if caught else "FAIL",
                "expected":  f"Stage 2 flags >= {min_unmatched} unmatched records",
                "actual":    actual,
                "run_id":    run_id,
                "tested_at": datetime.now().isoformat(),
            }

        return {
            "category": category,
            "status":   "ERROR",
            "reason":   f"Unknown expected_stage: {expected_stage}",
        }

    def print_report(self, results: list[dict]) -> None:
        print("\n" + "=" * 55)
        print("  ADVERSARIAL TEST SUITE RESULTS")
        print("=" * 55)
        passed = sum(1 for r in results if r.get("status") == "PASS")
        failed = sum(1 for r in results if r.get("status") == "FAIL")
        skipped = sum(1 for r in results if r.get("status") == "SKIP")
        print(f"  PASSED: {passed}  FAILED: {failed}  SKIPPED: {skipped}")
        print()
        for r in results:
            icon = {"PASS": "✓", "FAIL": "✗", "SKIP": "–", "ERROR": "!"}.get(
                r.get("status", "?"), "?"
            )
            print(f"  {icon}  {r['category']}")
            if r.get("status") != "PASS":
                print(f"       Expected : {r.get('expected', r.get('reason', ''))}")
                print(f"       Actual   : {r.get('actual', '')}")
        print("=" * 55 + "\n")

    def _save_results(self, results: list[dict]) -> None:
        os.makedirs(META_DIR, exist_ok=True)
        existing = []
        if os.path.exists(self.RESULTS_FILE):
            try:
                with open(self.RESULTS_FILE) as f:
                    existing = json.load(f)
            except Exception:
                existing = []
        batch = {
            "run_at":  datetime.now().isoformat(),
            "results": results,
        }
        existing.append(batch)
        with open(self.RESULTS_FILE, "w") as f:
            json.dump(existing, f, indent=2)


# ---------------------------------------------------------------------------
# CONVENIENCE: print Layer 5 summary to console
# ---------------------------------------------------------------------------

def print_layer5_summary(n_recent: int = 10) -> None:
    """Print a formatted Layer 5 health summary to the console."""
    monitor = Layer5Monitor()
    summary = monitor.summary(n_recent)
    alerts  = monitor.analyze(n_recent)

    print("\n" + "=" * 55)
    print("  LAYER 5 — PIPELINE HEALTH SUMMARY")
    print("=" * 55)
    for k, v in summary.items():
        if k != "status":
            print(f"  {k:30s}  {v}")
    if alerts:
        print(f"\n  ALERTS ({len(alerts)}):")
        for a in alerts:
            icon = "🔴" if a.severity == "critical" else "🟡"
            print(f"  {icon}  [{a.alert_type}] {a.description[:80]}")
            print(f"       → {a.recommendation[:80]}")
    else:
        print("\n  No alerts. All metrics within thresholds.")
    print("=" * 55 + "\n")
