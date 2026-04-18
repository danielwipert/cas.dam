# Chorus AI Systems — Data Analytics Manager (DAM)
## Developer Guide & Implementation Reference
**Version 1.0 · April 2026**

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Repository Layout](#2-repository-layout)
3. [Quick Start](#3-quick-start)
4. [Architecture: Nested MVS](#4-architecture-nested-mvs)
5. [File-by-File Reference](#5-file-by-file-reference)
6. [The Pipeline in Detail](#6-the-pipeline-in-detail)
7. [Data Contracts (schemas.py)](#7-data-contracts-schemaspy)
8. [Prompt Templates (prompts.py)](#8-prompt-templates-promptspy)
9. [LLM Client (llm_client.py)](#9-llm-client-llm_clientpy)
10. [Stage MVS Classes (stages.py)](#10-stage-mvs-classes-stagespy)
11. [Pipeline Orchestrator (pipeline.py)](#11-pipeline-orchestrator-pipelinepy)
12. [FactList Persistence (factlist_store.py)](#12-factlist-persistence-factlist_storepy)
13. [Meta-Governance (meta_governance.py)](#13-meta-governance-meta_governancepy)
14. [Test Data](#14-test-data)
15. [KPI Reference](#15-kpi-reference)
16. [Governance Gates & Degradation](#16-governance-gates--degradation)
17. [Report Template (templates/report_template.html)](#17-report-template)
18. [Running the Pipeline](#18-running-the-pipeline)
19. [What to Build Next](#19-what-to-build-next)

---

## 1. Project Overview

DAM automates the weekly ecommerce operations reporting workflow: downloading CSVs from Shopify, a 3PL provider, FedEx, and DHL; reconciling them; computing KPIs; and producing a two-page PDF management report. The entire analyst judgment chain is handled by LLMs. All arithmetic is independently verified by Python.

**Cost:** under $0.05 per run via Together AI free-tier models, versus ~$310–450/week for a mid-level analyst doing the same work manually.

**What makes it architecturally interesting:** every stage is a self-governing Minimum Viable System (MVS) — the smallest unit containing all seven Chorus AI governance layers. Three levels of Beer's VSM recursion are implemented explicitly: the pipeline orchestrator, each of the five stages, and the Stage 4 generation+verification pair.

---

## 2. Repository Layout

```
dam/
├── schemas.py                    # All Pydantic data contracts
├── prompts.py                    # LLM prompt builders (one per stage)
├── llm_client.py                 # Together AI client + JSON parser
├── stages.py                     # Five stage MVS classes
├── pipeline.py                   # Orchestrator + CLI entry point
├── factlist_store.py             # FactList JSON persistence
├── meta_governance.py            # Layer 5 monitoring + adversarial runner
├── generate_test_data.py         # Synthetic clean test CSVs
├── generate_adversarial_data.py  # Planted-error test CSVs
│
├── templates/
│   └── report_template.html      # Jinja2 report template (two-page PDF)
│
├── test_data/
│   ├── shopify_orders.csv        # 150 orders, non-canonical column names
│   ├── tpl_shipments.csv         # 147 shipments (3 cancelled orders)
│   ├── fedex_tracking.csv        # 75 FedEx carrier records
│   ├── dhl_tracking.csv          # 65 DHL carrier records
│   └── adversarial/
│       ├── duplicate_order_ids/  # Gate 1 hard fail test
│       ├── missing_required_fields/ # Gate 1 hard fail test
│       ├── mismatched_timestamps/   # Gate 1 / Stage 3 test
│       └── corrupted_tracking/      # Stage 2 match rate test
│
├── factlists/                    # Created at runtime: YYYY-MM-DD.json per week
├── run_logs/                     # Created at runtime: one JSON per run
├── meta_governance/              # Created at runtime: alerts.json, calibration
└── reports/                      # Created at runtime: DAM_{run_id}.pdf
```

---

## 3. Quick Start

### Prerequisites

```bash
pip install pydantic together jinja2 weasyprint --break-system-packages
```

### Environment

```bash
export TOGETHER_API_KEY=your_key_here
```

### Generate test data (one time)

```bash
python generate_test_data.py
python generate_adversarial_data.py
```

### Run the pipeline on synthetic data

```bash
python pipeline.py --test
```

### Run on real CSVs

```bash
python pipeline.py \
  --shopify  exports/shopify_orders.csv \
  --tpl      exports/3pl_shipments.csv \
  --fedex    exports/fedex_tracking.csv \
  --dhl      exports/dhl_tracking.csv \
  --week     2026-04-11
```

### Layer 5 health summary (no pipeline run)

```bash
python pipeline.py --meta
```

### Adversarial test suite

```bash
python pipeline.py --adversarial
```

---

## 4. Architecture: Nested MVS

### The Three Levels

```
Level 1 — DAM Pipeline Orchestrator (pipeline.py)
  L1 operational units: the five stage MVS instances
  L2 coordination: stage sequencing, canonical data model enforcement
  L3 governance: system-level degradation decisions
  L4 assurance: cross-stage FACT_ID consistency check
  L5 adaptive: rolling health metrics via Layer5Monitor
  L6 constitutional: no unverified output, no projections
  L7 interface: CLI, CSV I/O, PDF output, run log persistence

  Level 2 — Each Stage (Stage1 through Stage5 in stages.py)
    L1: LLM call or deterministic computation
    L2: Pydantic input/output contracts (StageXInput/StageXOutput)
    L3: Internal gate — verify own output before surfacing upward
    L4: Internal assurance — Python recomputation or schema check
    L5: HealthTelemetry emitted with every response
    L6: Constitutional constraint — what the stage refuses to do
    L7: MVS interface — run(input) → VerifiedOutput | DegradationSignal

    Level 3 — Stage 4 Generation+Verification Pair
      L1: DeepSeek V3 generates insight claims
      L3: Claim-stripping gate (strips unverified claims)
      L4: Qwen2.5 72B verifies FACT_ID citations
      L6: Verified FactList is the only permitted evidence base
```

### The MVS Interface Contract

Every stage exposes one method:

```python
result = Stage1().run(stage1_input)

if isinstance(result, VerifiedOutput):
    output: Stage1Output = result.payload
    telemetry: HealthTelemetry = result.health_telemetry
else:  # DegradationSignal
    reason: str = result.failure_reason
    level: DegradationLevel = result.degradation_level_recommendation
```

The orchestrator **never reaches inside a stage**. It calls `run()` and receives one of two response types. The stage self-governs; the orchestrator coordinates.

### Data Flow

```
CSV files
    ↓
Stage 1: LLM field mapping → canonical_orders, canonical_shipments, canonical_carrier_shipments
    ↓
Stage 2: LLM exact + fuzzy join → reconciliation_shipments
    ↓
Stage 3: LLM KPI compute → FactList (Python verified, immutable)
    ↓  FactList saved to factlists/YYYY-MM-DD.json
Stage 4: DeepSeek generates → Qwen verifies → verified_insights[]
    ↓
Stage 5: Jinja2 + WeasyPrint → PDF report
    ↓
RunLog saved → Layer5Monitor.analyze()
```

---

## 5. File-by-File Reference

| File | Lines | Purpose | Key exports |
|------|-------|---------|-------------|
| `schemas.py` | 419 | All Pydantic data contracts | 32 classes |
| `prompts.py` | 632 | LLM prompt builders | 6 builder functions + KPI_DEFINITIONS |
| `llm_client.py` | 145 | Together AI wrapper | `call_llm()`, `parse_json_response()` |
| `stages.py` | 1411 | Five stage MVS classes | `Stage1` – `Stage5` |
| `pipeline.py` | 403 | Orchestrator + CLI | `DAMOrchestrator`, `main()` |
| `factlist_store.py` | 214 | FactList persistence | 6 functions |
| `meta_governance.py` | 660 | Layer 5 monitoring | `Layer5Monitor`, `CalibrationLog`, `AdversarialRunner` |
| `generate_test_data.py` | 320 | Synthetic CSV generator | run directly |
| `generate_adversarial_data.py` | 282 | Planted-error CSV generator | run directly |
| `templates/report_template.html` | 409 | Jinja2 report template | consumed by Stage 5 |

---

## 6. The Pipeline in Detail

### Stage 1: Data Ingestion and Normalization

**What it does:** Reads four CSV files with non-canonical column names. Calls Llama 3.3 70B to propose a field mapping from each file's headers to the canonical schema. Python applies the mapping, validates every row, and emits four canonical tables.

**Why LLM field mapping:** Real-world CSV exports use vendor-specific column names that change without notice. Hardcoded column maps break silently. The LLM reads headers and sample rows and proposes the mapping — the same judgment a data analyst applies when opening an unfamiliar export. Python validates the result.

**Key constraint:** Stage 1 maps fields. It does not impute missing values, fill nulls, or make assumptions about ambiguous columns. Ambiguity is disclosed in the `FieldMappingLog`, not resolved silently.

**Gate:** Duplicate `order_id` detected → DegradationSignal (halt). LLM mapping fails after retry → DegradationSignal (halt). A Stage 1 halt always propagates to a full pipeline halt.

### Stage 2: Reconciliation

**What it does:** Joins orders → shipments on `order_id`, shipments → carrier records on `tracking_number`. For records that don't match exactly, calls Llama 3.3 70B to propose fuzzy matches with confidence scores and rationale.

**Why LLM fuzzy matching:** Exact joins on tracking numbers fail regularly in practice due to prefix stripping, transposed digits, and format differences between 3PL and carrier systems. A human analyst examines both sides of the mismatch and makes a judgment call. The LLM replicates that judgment.

**Key constraint:** Fuzzy match confidence must be >= 0.90 (enforced by Python, not the LLM). Unmatched records are flagged in the output, never silently dropped.

**Gate:** Match rate < 80% → DegradationSignal (halt). Match rate 80–95% → warning disclosed in report. A Stage 2 halt always propagates to a full pipeline halt.

### Stage 3: KPI Computation — The FactList

**What it does:** Llama 3.3 70B computes all 10 KPIs, mimicking the formulas an analyst would build in Excel. Python independently recomputes every KPI. Python always wins on any disagreement. The result is the immutable FactList.

**Why LLM KPI computation:** To demonstrate that LLMs can reliably perform formula-based analytical work when placed inside a governed architecture. The LLM value is logged alongside the Python value for drift tracking. Rising LLM/Python mismatch is a Layer 5 signal.

**Key constraint:** `final_value` is always the Python-computed number. The LLM value is stored in `llm_value` for observation only. The FactList is immutable after emission — no downstream stage may modify it.

**FactList entry structure:**
```python
FactListEntry(
    fact_id="F002",                    # e.g. F001–F010
    domain=KPIDomain.fulfillment,
    kpi_name="On-Time Ship Rate",
    llm_value=0.971,                   # LLM's computation (logged only)
    python_value=0.966,                # authoritative
    final_value=0.966,                 # always == python_value
    threshold_status=ThresholdStatus.yellow,
    llm_python_match=False,            # mismatch logged
    data_provenance=DataProvenance(...),
    python_verified=True,              # constitutional flag
)
```

### Stage 4: Insight Generation (Third-Level MVS)

**What it does:** DeepSeek V3 generates observations, hypotheses, and recommended actions for each KPI domain. Every claim must cite specific FACT_IDs. Qwen2.5 72B independently verifies every citation. Claims that fail verification are stripped — they never appear in the report.

**Why two different model families:** Shared architecture means shared blind spots. DeepSeek and Qwen are from structurally different families (different training, different architectures). If both agree a claim is aligned, that agreement is more meaningful than one model checking itself.

**Key constraint:** Stage 4 refuses input where `python_verified=False`. No claim reaches the report without a citation to a verified fact.

**Third-level recursion:** Within Stage 4, the generation+verification pair is itself a viable system: DeepSeek is L1, Qwen is L4, the claim-stripping gate is L3, the verified FactList is L6 (constitutional boundary).

### Stage 5: Report Compilation

**What it does:** Assembles all verified data into a two-page PDF using a Jinja2 template and WeasyPrint. No LLM. Fully deterministic.

**What it demonstrates:** Viability is a property of governance structure, not of the presence of an AI model. Stage 5 implements all seven governance layers through purely deterministic means.

**Layout:**
- Page 1: header, executive headline, radar chart (SVG), Fulfillment domain block, Carrier Performance domain block
- Page 2: Cost domain block, Operational Integrity domain block, Exceptions section, Verification footer

**Gate:** Any required section absent from rendered HTML → DegradationSignal. PDF page count ≠ 2 → logged warning (non-fatal).

---

## 7. Data Contracts (schemas.py)

### Enums

| Enum | Values |
|------|--------|
| `MatchMethod` | `exact`, `fuzzy_llm`, `unmatched` |
| `JoinStatus` | `matched`, `unmatched`, `partial` |
| `ThresholdStatus` | `green`, `yellow`, `red`, `informational` |
| `DegradationLevel` | `0` (normal), `1` (partial), `2` (halt) |
| `KPIDomain` | `fulfillment`, `carrier_performance`, `cost`, `operational_integrity` |
| `ClaimType` | `observation`, `hypothesis`, `recommended_action` |
| `VerificationVerdict` | `aligned`, `stripped` |

### Canonical Tables

| Schema | Source | Primary Key |
|--------|--------|-------------|
| `CanonicalOrder` | Shopify | `order_id` |
| `CanonicalShipment` | 3PL | `shipment_id` |
| `CanonicalCarrierShipment` | FedEx / DHL | `(carrier, tracking_number)` |
| `ReconciliationShipment` | Stage 2 output | `shipment_id` |

### MVS Interface Types

```python
# Stage returns one of:
VerifiedOutput(stage, payload, health_telemetry)
DegradationSignal(stage, failure_reason, degradation_level_recommendation, health_telemetry)

# Embedded in both:
HealthTelemetry(
    stage, retry_count, api_cost_usd, latency_seconds, model_used,
    fallback_activated,
    # Stage-specific optional fields:
    mapping_confidence_avg,    # Stage 1
    ambiguous_field_count,     # Stage 1
    exact_match_rate,          # Stage 2
    fuzzy_match_volume,        # Stage 2
    kpi_mismatch_count,        # Stage 3
    claim_count_generated,     # Stage 4
    claim_acceptance_rate,     # Stage 4
    cross_verifier_agreement,  # Stage 4
    render_time_seconds,       # Stage 5
    pdf_page_count,            # Stage 5
)
```

### Stage I/O Contracts

| Stage | Input | Output |
|-------|-------|--------|
| Stage 1 | `Stage1Input` (4 CSV paths + run_id) | `Stage1Output` (canonical tables + FieldMappingLog) |
| Stage 2 | `Stage2Input` (canonical tables) | `Stage2Output` (reconciliation_shipments + stats) |
| Stage 3 | `Stage3Input` (recon table + prior FactList) | `Stage3Output` (FactList + mismatch count) |
| Stage 4 | `Stage4Input` (FactList + python_verified flag) | `Stage4Output` (verified_insights + stripped log) |
| Stage 5 | `Stage5Input` (all upstream outputs + degradation signals) | `Stage5Output` (pdf_path + render metadata) |

---

## 8. Prompt Templates (prompts.py)

Six builder functions. Each returns a complete string ready to pass to `call_llm()`. All instruct the model to return JSON only — no preamble, no markdown fences.

| Function | Stage | Model | Key instruction |
|----------|-------|-------|-----------------|
| `build_stage1_prompt()` | 1 | Llama 3.3 70B | Map every source column; set `canonical_field=null` if no match |
| `build_stage2_exact_prompt()` | 2 | Llama 3.3 70B | Confirm join keys; flag anomalies only |
| `build_stage2_fuzzy_prompt()` | 2 | Llama 3.3 70B | Propose fuzzy matches with confidence >= 0.90 |
| `build_stage3_prompt()` | 3 | Llama 3.3 70B | Compute LLM values only; leave `python_value=null` |
| `build_stage4_generation_prompt()` | 4 | DeepSeek V3 | Every claim must cite FACT_IDs; no projections |
| `build_stage4_verification_prompt()` | 4 | Qwen2.5 72B | Strip if FACT_ID wrong even if claim directionally correct |

**Retry pattern:** Every builder accepts an optional `retry_context: str` parameter. On retry, pass the gate's failure reason — the same function, two modes.

**Module-level constants used across pipeline:**
- `KPI_DEFINITIONS` — list of 10 KPI dicts (fact_id, domain, kpi_name, formula_description)
- `KPI_THRESHOLDS` — dict of kpi_name → threshold rules
- `SHOPIFY_CANONICAL_DESCRIPTIONS`, `TPL_CANONICAL_DESCRIPTIONS`, `FEDEX_CANONICAL_DESCRIPTIONS`, `DHL_CANONICAL_DESCRIPTIONS` — field description dicts passed to Stage 1 prompts
- `SYSTEM_PROMPTS` — dict of stage name → system prompt string

---

## 9. LLM Client (llm_client.py)

### Model Constants

```python
MODEL_STAGES_1_3  = "meta-llama/Llama-3.3-70B-Instruct-Turbo"   # Meta / Llama
MODEL_STAGE4_GEN  = "deepseek-ai/DeepSeek-V3"                    # DeepSeek
MODEL_STAGE4_VER  = "Qwen/Qwen2.5-72B-Instruct-Turbo"           # Qwen / Alibaba
MODEL_FALLBACK    = "mistralai/Mixtral-8x22B-Instruct-v0.1"       # Mistral
MAX_TOKENS        = 4096
```

Four distinct model families satisfy Principle 13 (observer diversity). No two adjacent stages in the verification chain share a family.

### `call_llm()`

```python
text, cost_usd, latency_seconds = call_llm(
    system_prompt=STAGE1_SYSTEM,
    user_prompt=prompt,
    model=MODEL_STAGES_1_3,
    client=client,        # optional — created from env if not provided
    temperature=0.1,
    max_tokens=4096,
)
```

### `parse_json_response()`

Handles two common LLM output failures automatically:
1. Markdown fences: ` ```json ... ``` ` — stripped before parsing
2. Preamble text before JSON — finds first `{` or `[` and parses from there

Raises `ValueError` with the raw response if parsing fails entirely.

### `get_client()`

Reads `TOGETHER_API_KEY` from environment. Raises `EnvironmentError` with a clear setup message if missing.

---

## 10. Stage MVS Classes (stages.py)

### Constants

```python
TRANSIT_WINDOWS = {
    ("FedEx", "FedEx Ground"):          5,  # business days
    ("FedEx", "FedEx Home Delivery"):   5,
    ("FedEx", "FedEx Express Saver"):   3,
    ("FedEx", "FedEx 2Day"):            2,
    ("FedEx", "FedEx Overnight"):       1,
    ("DHL Ecommerce", "DHL Ecommerce Ground"):    5,
    ("DHL Ecommerce", "DHL Ecommerce Expedited"): 3,
}
```

### Stage 1 — Key implementation details

- `_read_csv()` — reads any CSV, returns (rows, headers)
- `_parse_mappings()` — converts LLM JSON response to `FieldMappingEntry` list
- `_build_col_map()` — inverts mapping to `{canonical_field: source_column}`
- `_apply_shopify()`, `_apply_tpl()`, `_apply_carrier()` — apply mapping row by row

**datetime parsing** tries three formats: `%Y-%m-%dT%H:%M:%S`, `%Y-%m-%d %H:%M:%S`, `%Y-%m-%d`. Returns `None` on blank or unparseable — never crashes.

### Stage 2 — Key implementation details

Phase 1 (exact join) is pure Python — no LLM needed. Phase 2 (fuzzy) only calls the LLM if there are unmatched records on both sides. If the LLM call fails after retry, the pipeline proceeds with those records marked `unmatched` — Stage 2 does not halt on fuzzy failure alone, only on overall match rate.

### Stage 3 — Python KPI computations

All 10 KPIs computed in `_compute_all_kpis()`:

```python
# F001 — Order to Ship Time
diffs = [(s.shipped_at - order_map[s.order_id].order_created_at).total_seconds()/3600
         for s in shipped if order exists and both timestamps present]
result = mean(diffs)

# F002 — On-Time Ship Rate
on_time = [s for s in eligible if s.shipped_at <= order.promised_ship_date]
result = len(on_time) / len(eligible)

# F003 — Unshipped Orders Rate
unshipped = [o for o in orders if o.order_id not in shipped_order_ids]
result = len(unshipped) / len(orders)

# F004 — Transit Time (hours, first_scan to delivered)
# F005 — On-Time Delivery Rate (vs TRANSIT_WINDOWS lookup)
# F006 — Carrier Mix (FedEx fraction of total shipments)
# F007 — Shipping Cost per Order (total carrier costs / shipped)
# F008 — Cost by Carrier (FedEx average as primary value)
# F009 — Label Lag (label_created to first_scan, hours)
# F010 — Shipment Match Rate (matched / total reconciliation rows)
```

LLM/Python mismatch tolerance: 1% relative (`abs(py - llm) / (abs(py) + 1e-9) > 0.01`).

### Stage 4 — Claim stripping logic

```python
# Hard check applied regardless of Qwen verdict:
if not all(fid in valid_fact_ids for fid in claim.cited_fact_ids):
    verdict = "stripped"
    reason  = "One or more cited FACT_IDs do not exist in FactList"
```

If Qwen verification fails after retry, **all claims are conservatively stripped**. Stage 4 emits a DegradationSignal rather than releasing unverified insights.

### Stage 5 — Domain scoring for radar chart

```python
def _domain_score(facts: list) -> float:
    mapping = {green: 9.0, yellow: 6.0, red: 3.0, informational: 5.0}
    return mean([mapping[f.threshold_status] for f in facts])
```

Scores appear on the four radar chart axis tips as `Label: score`.

---

## 11. Pipeline Orchestrator (pipeline.py)

### Degradation decision rules

| Condition | Decision |
|-----------|----------|
| Stage 1 → DegradationSignal | Full halt. No output. |
| Stage 2 → DegradationSignal | Full halt. No output. |
| Stage 3 → DegradationSignal | Partial. Skip Stages 4+5. Log signals. |
| Stage 4 → DegradationSignal | Partial. Proceed to Stage 5 with empty insights. |
| Stage 5 → DegradationSignal | Partial. No PDF. Run log still written. |
| Orchestrator FACT_ID check fails | Strip orphaned insights, proceed. |

### Cross-stage FACT_ID consistency check (Orchestrator L4)

After Stage 4, the orchestrator verifies that all `cited_fact_ids` in the verified insights exist in the Stage 3 FactList. This check is structurally impossible within any individual stage — only the orchestrator has visibility across both outputs.

```python
valid_fact_ids = {f.fact_id for f in s3_out.factlist}
orphaned = [ins for ins in s4_out.verified_insights
            if not all(fid in valid_fact_ids for fid in ins.cited_fact_ids)]
```

### Run log structure

Every run writes `run_logs/{run_id}.json`:
```json
{
  "run_id": "DAM-20260411-143022-a1b2c3",
  "started_at": "2026-04-11T14:30:22",
  "completed_at": "2026-04-11T14:30:40",
  "final_status": "full",
  "degradation_level": 0,
  "degradation_signals": [],
  "stage_telemetry": [...],
  "total_api_cost_usd": 0.031,
  "total_latency_s": 18.4,
  "models_used": ["meta-llama/...", "deepseek-ai/...", "Qwen/..."],
  "fallback_activated": false,
  "kpi_mismatch_count": 2,
  "claim_acceptance_rate": 0.917,
  "cross_verifier_agreement": 0.833,
  "pdf_path": "reports/DAM_DAM-20260411-143022-a1b2c3.pdf"
}
```

### CLI

```bash
python pipeline.py --test                    # synthetic data, week 2026-04-04
python pipeline.py --shopify X --tpl X ...  # real CSVs
python pipeline.py --meta                    # Layer 5 health summary only
python pipeline.py --adversarial             # adversarial test suite only
```

---

## 12. FactList Persistence (factlist_store.py)

### Storage format

One JSON file per successful run: `factlists/YYYY-MM-DD.json`. Contains a list of serialised `FactListEntry` objects. The week date is the ISO date of the last day of the reporting week (Friday).

### API

```python
# Save after Stage 3 completes
path = save_factlist(factlist, "2026-04-04")

# Load in Stage 3 for WoW delta computation
prior = load_prior_factlist("2026-04-11")  # returns most recent < this date

# Cost baseline (4-week rolling average)
baseline = load_cost_baseline("2026-04-11")
# Returns: {fedex_avg, overall_avg, weeks_included, is_full_baseline}

# Disclosure string for report footer
status = get_baseline_status("2026-04-11")
# e.g. "baseline building (2/4 weeks); cost KPIs are informational"

# Trend data for Layer 5
trend = get_kpi_trend("On-Time Ship Rate", n_weeks=4)
# Returns: [{week_date, value, threshold_status}, ...]
```

### First-run behaviour

`load_prior_factlist()` returns `None` on first run. Stage 3 handles this gracefully — WoW deltas are `None`, cost KPIs are `informational`. The report discloses this in the verification footer.

---

## 13. Meta-Governance (meta_governance.py)

### Layer5Monitor

Reads the N most recent run logs and computes rolling health metrics.

**Alert thresholds:**

| Alert type | Threshold | Severity |
|-----------|-----------|----------|
| `high_halt_rate` | > 5% of runs halted | critical |
| `rising_retry_rate` | > 20% of stage calls needed retry | warning |
| `low_claim_acceptance` | avg < 70% claims verified | warning |
| `high_kpi_mismatch_rate` | avg > 3 mismatches/run | warning |
| `verifier_agreement_lock` | 100% agreement 3 consecutive runs | critical |
| `high_stage_failure_rate` | any stage failing > 30% of runs | warning |

```python
monitor = Layer5Monitor()
alerts  = monitor.analyze(n_recent=10)   # returns list[Alert]
summary = monitor.summary(n_recent=10)   # returns dict of metrics
```

**Bounded authority:** Layer5Monitor detects and recommends. It writes `meta_governance/alerts.json`. It cannot modify thresholds, swap models, or release output.

### CalibrationLog

Records human reviewer verdicts alongside system verdicts for Stage 4 claims. Monthly review cadence in v1.

```python
cal = CalibrationLog()
cal.record(
    run_id="DAM-...",
    claim_text="On-Time Ship Rate declined to 96.6%",
    system_verdict="aligned",
    human_verdict="stripped",    # human disagrees
    reviewer_note="Number is correct but conclusion overstated"
)
metrics = cal.metrics()
# Returns: {false_positive_rate, false_negative_rate, agreement_rate, fp_alert}
# fp_alert=True if false_positive_rate > 5%
```

Persists to `meta_governance/calibration_log.json`.

### AdversarialRunner

Runs pipeline on planted-error test data and checks gate catches.

```python
runner = AdversarialRunner()
results = runner.run_all()
runner.print_report(results)
```

**Four test categories:**

| Category | Error planted | Expected gate |
|----------|--------------|---------------|
| `duplicate_order_ids` | 5 rows share an order_id | Stage 1 halt |
| `missing_required_fields` | Blank order_id, tracking# | Stage 1 halt |
| `mismatched_timestamps` | Delivered before scanned | Stage 1 halt |
| `corrupted_tracking` | 10 corrupted tracking numbers | Stage 2 unmatched ≥ 3 |

Results saved to `meta_governance/adversarial_results.json` with timestamps — run monthly or after any model/prompt change.

---

## 14. Test Data

### Clean test data (generate_test_data.py)

150 orders, 147 shipments (3 cancelled), 140 carrier records (7 missing). `random.seed(42)` — fully reproducible.

**Column names are deliberately non-canonical** to force Stage 1 LLM field mapping to do real work:

| Source | Key non-canonical columns |
|--------|--------------------------|
| Shopify | `Name` (order_id), `Province` (state), `Ship By Date` (promised_ship_date) |
| 3PL | `Order Ref` (order_id), `Tracking #`, `Freight Cost` (blank — filled by Stage 2) |
| FedEx | `Tracking Number`, `First Scan Date`, `Billed Weight Charge` |
| DHL | `Waybill` (tracking), `Picked Up` (first_scan), `POD Date` (delivered_at) |

**Expected KPI outcomes** (verify against Stage 3 Python output):

| KPI | Expected value | Status |
|-----|---------------|--------|
| Unshipped Orders Rate | 2.0% | RED |
| On-Time Ship Rate | ~96.6% | YELLOW |
| On-Time Delivery Rate | ~96.6% | YELLOW |
| Shipment Match Rate | ~95.2% | YELLOW |
| Label Lag | slightly elevated | YELLOW |
| Others | — | GREEN or informational |

### Adversarial test data (generate_adversarial_data.py)

Four subdirectories under `test_data/adversarial/`, each with all four CSV files. Errors are surgical — minimum corruption needed to trigger the target gate.

---

## 15. KPI Reference

### The 10 KPIs

| FACT_ID | Domain | KPI Name | Formula | Unit |
|---------|--------|----------|---------|------|
| F001 | fulfillment | Order to Ship Time | mean(shipped_at − order_created_at) | hours |
| F002 | fulfillment | On-Time Ship Rate | shipped_on_time / orders_with_promise | decimal |
| F003 | fulfillment | Unshipped Orders Rate | orders_with_no_shipment / total_orders | decimal |
| F004 | carrier_performance | Transit Time | mean(delivered_at − first_scan_at) | hours |
| F005 | carrier_performance | On-Time Delivery Rate | delivered_within_window / delivered | decimal |
| F006 | carrier_performance | Carrier Mix | FedEx_shipments / total_shipments | decimal |
| F007 | cost | Shipping Cost per Order | sum(carrier_costs) / total_shipped | USD |
| F008 | cost | Cost by Carrier | mean(carrier_cost) per carrier | USD |
| F009 | operational_integrity | Label Lag | mean(first_scan_at − label_created_at) | hours |
| F010 | operational_integrity | Shipment Match Rate | matched_shipments / total_shipments | decimal |

### Threshold Table

| KPI | Green | Yellow | Red | Notes |
|-----|-------|--------|-----|-------|
| On-Time Ship Rate | ≥ 98% | 95–97% | < 95% | Critical |
| On-Time Delivery Rate | ≥ 98% | 95–97% | < 95% | Critical |
| Shipment Match Rate | ≥ 99.8% | 99.0–99.7% | < 99% | Critical |
| Unshipped Orders Rate | < 1% | 1–3% | > 3% | Critical |
| Order to Ship Time | ≤ 24 hrs | 24–48 hrs | > 48 hrs | Tunable |
| Transit Time | ≤ window | window + 24 hrs | > window + 24 hrs | Tunable |
| Shipping Cost per Order | ±10% of baseline | 10–25% | > 25% | First run: informational |
| Cost by Carrier | ±10% of baseline | 10–25% | > 25% | First run: informational |
| Carrier Mix | informational | — | — | No threshold |
| Label Lag | ≤ 4 hrs | 4–12 hrs | > 12 hrs | Tunable |

### Transit Windows

| Carrier | Service | Window |
|---------|---------|--------|
| FedEx | FedEx Ground | 5 business days |
| FedEx | FedEx Home Delivery | 5 business days |
| FedEx | FedEx Express Saver | 3 business days |
| FedEx | FedEx 2Day | 2 business days |
| FedEx | FedEx Overnight | 1 business day |
| DHL Ecommerce | DHL Ecommerce Ground | 5 business days |
| DHL Ecommerce | DHL Ecommerce Expedited | 3 business days |

---

## 16. Governance Gates & Degradation

### Gate Summary

| Gate | Stage | Check | On Failure |
|------|-------|-------|------------|
| Gate 1 | Stage 1 | Duplicate order_id | Halt |
| Gate 1 | Stage 1 | LLM mapping fails after retry | Halt |
| Gate 2 | Stage 2 | Match rate < 80% | Halt |
| Gate 2 | Stage 2 | Match rate 80–95% | Warning in report |
| Gate 3 | Stage 3 | Python cannot compute KPI | DegradationSignal for that KPI |
| Gate 3 | Stage 3 | LLM/Python mismatch | Python wins; mismatch logged |
| Gate 4 | Stage 4 | FACT_ID invalid or claim not supported | Claim stripped |
| Gate 4 | Stage 4 | Zero claims survive | DegradationSignal (partial) |
| Gate 5 | Stage 5 | Required section absent | Halt |
| Gate 5 | Stage 5 | PDF page count ≠ 2 | Warning only |
| Orch. L4 | Orchestrator | Insight cites FACT_ID not in FactList | Strip insight |

### Degradation Levels

| Level | Name | Trigger | Behaviour |
|-------|------|---------|-----------|
| 0 | Normal | All stages complete | Full report |
| 1 | Partial | Stage 3, 4, or 5 fails | Report produced, disclosure included |
| 2 | Halt | Stage 1 or 2 fails | No report, run log written |

### Retry Policy

One retry per LLM stage. On retry, the fallback model (`Mixtral 8x22B`) is used instead of the primary. If retry fails, the stage degrades — it never burns tokens on a third attempt.

### Constitutional Constraints (cannot be overridden)

- No unverified output released. If verification is unavailable, halt.
- Stage 1 or 2 failure always propagates to full pipeline halt.
- FactList is immutable after Stage 3 emission. No downstream stage may modify it.
- Stage 4 refuses input where `python_verified=False`.
- Report must include all degradation disclosures.
- No financial projections or investment recommendations under any path.

---

## 17. Report Template

**File:** `templates/report_template.html`

Jinja2 template rendered by Stage 5. Loaded via `FileSystemLoader` — must be in `templates/` relative to working directory.

### Template variables

| Variable | Type | Content |
|----------|------|---------|
| `run_id` | str | Pipeline run identifier |
| `report_week` | str | Human-readable date range |
| `generated_at` | str | Render timestamp |
| `headline` | str | Executive one-liner (red/yellow KPI or "all green") |
| `radar_svg` | str (safe) | Hand-rolled SVG radar chart HTML |
| `page1_domains` | list[dict] | Fulfillment + Carrier Performance domain data |
| `page2_domains` | list[dict] | Cost + Operational Integrity domain data |
| `exceptions` | list[dict] | Red/yellow KPIs + unmatched shipment flags |
| `match_rate` | str | Formatted Shipment Match Rate |
| `claim_acceptance` | str | Stage 4 acceptance rate |
| `verifier_agreement` | str | Cross-verifier agreement rate |
| `kpi_mismatch_count` | str | Count of LLM/Python KPI mismatches |
| `run_status` | str | "FULL" or "PARTIAL" |
| `degradation_disclosures` | list[str] | Disclosure strings from failed stages |

### Domain data dict structure

```python
{
    "id":       "fulfillment",
    "label":    "Fulfillment",
    "score":    7.5,           # 1–10 composite from threshold statuses
    "kpis": [
        {
            "name":        "On-Time Ship Rate",
            "current":     "96.6%",
            "prior":       "98.2%",
            "delta":       "▼ 0.0160",
            "delta_class": "delta-down",
            "status":      "yellow",
        }
    ],
    "insights": [
        {"text": "On-Time Ship Rate at 96.6%...", "is_action": False, "icon": "📊"},
        {"text": "Review 3PL SLAs",               "is_action": True,  "icon": "▶"},
    ]
}
```

### Radar chart

Hand-rolled SVG (`Stage5._build_radar_svg()`). Four axes at 90° intervals. Five grid rings (2/4/6/8/10). Teal fill (#1abc9c). Score label at each axis tip. Center dot in navy (#1a5276). 330×310 px. No external libraries.

---

## 18. Running the Pipeline

### First time setup

```bash
# 1. Install dependencies
pip install pydantic together jinja2 weasyprint --break-system-packages

# 2. Set API key
export TOGETHER_API_KEY=your_together_ai_key

# 3. Generate test data
python generate_test_data.py
python generate_adversarial_data.py

# 4. First run
python pipeline.py --test
```

### Expected console output

```
============================================================
  Chorus AI — Data Analytics Manager
  Run ID : DAM-20260411-143022-a1b2c3
  Week   : 2026-04-04
============================================================

[ Stage 1 ] Ingestion & Normalization...
  ✓ 150 orders  | 147 shipments  | 140 carrier records

[ Stage 2 ] Reconciliation...
  ✓ Match rate: 95.2%  | Fuzzy matches: 0  | Unmatched: 7
  ⚠  Match rate below 95% — disclosed in report

[ Stage 3 ] KPI Computation...
  ✓ 10 KPIs computed  | LLM/Python mismatches: 2
  🔴  Unshipped Orders Rate              0.0200
  🟡  On-Time Ship Rate                  0.9660
  🟡  On-Time Delivery Rate              0.9660
  🟡  Shipment Match Rate                0.9524
  🟢  Order to Ship Time                 18.5000
  ...

[ Stage 4 ] Insight Generation + Verification...
  ✓ 12 verified insights  | Acceptance rate: 83%  | Agreement: 83%

[ Stage 5 ] Report Compilation...
  ✓ PDF: reports/DAM_DAM-20260411-143022-a1b2c3.pdf  | Pages: 2  | Render: 4.1s

============================================================
  Run complete — status: FULL
  Report: reports/DAM_DAM-20260411-143022-a1b2c3.pdf
============================================================

  Total API cost : $0.0310
  Total latency  : 18.4s
  Run log        : run_logs/DAM-20260411-143022-a1b2c3.json
```

### Checking Layer 5 health

```bash
python pipeline.py --meta
```

```
=======================================================
  LAYER 5 — PIPELINE HEALTH SUMMARY
=======================================================
  runs_analysed                   10
  full_rate                       90%
  partial_rate                    10%
  halt_rate                       0%
  avg_api_cost_usd                $0.0318
  avg_latency_s                   19.2s
  avg_claim_acceptance            84%
  avg_kpi_mismatches              1.8
  fallback_activation_rate        20%

  No alerts. All metrics within thresholds.
=======================================================
```

### Running adversarial tests

```bash
python pipeline.py --adversarial
```

```
=======================================================
  ADVERSARIAL TEST SUITE RESULTS
=======================================================
  PASSED: 4  FAILED: 0  SKIPPED: 0

  ✓  duplicate_order_ids
  ✓  missing_required_fields
  ✓  mismatched_timestamps
  ✓  corrupted_tracking
=======================================================
```

---

## 19. What to Build Next

These are the remaining items to make DAM production-ready. All architecture decisions are resolved — these are implementation tasks only.

### Immediate (needed for real data)

**GitHub Actions scheduler** — automate weekly runs. Trigger on schedule (e.g. Monday 7am), pass CSV paths from a configured S3 bucket or SFTP location, commit PDF to a reports branch or send via email.

**Real CSV format validation** — the synthetic test data uses known column names. Before running on real exports from a new client, run Stage 1 in isolation to confirm the LLM maps columns correctly. Add a `--dry-run` flag that runs Stage 1 only and prints the `FieldMappingLog` without proceeding.

**Email/Slack delivery** — Stage 5 currently writes a PDF to `reports/`. Add a delivery step after Stage 5: attach PDF to email or post a summary to Slack with a download link.

### Medium priority

**Rolling 4-week cost baseline** — `load_cost_baseline()` in `factlist_store.py` is implemented but Stage 3 doesn't use it yet to classify cost KPI threshold status. Wire the baseline into `_classify_threshold()` so cost KPIs graduate from `informational` to `green/yellow/red` once 4 weeks of history accumulate.

**`--dry-run` mode** — run Stage 1 only, print `FieldMappingLog`, exit. Useful for validating a new client's CSV format before a full pipeline run.

**Streamlit dashboard** — real-time view of Layer 5 metrics, KPI trends, and run history. `factlist_store.get_kpi_trend()` and `Layer5Monitor.summary()` provide all the data needed.

### Future

**Multi-client support** — parameterise `FACTLIST_DIR`, `LOG_DIR`, and `REPORTS_DIR` by client ID. Each client gets isolated storage, separate Layer 5 monitoring, and separate cost baselines.

**Additional carriers** — UPS, USPS, Canada Post. Add entries to `TRANSIT_WINDOWS`, add a carrier-specific section to `DHL_CANONICAL_DESCRIPTIONS`, update Stage 1 `sources` list in `Stage1.run()`.

**Adversarial test suite expansion** — add new test cases whenever a real-world failure is discovered. The `AdversarialRunner.EXPECTED_CATCHES` dict is the registry — add a category name, expected catch, and corresponding CSV files in `test_data/adversarial/`.

**Calibration workflow** — `CalibrationLog.record()` currently requires manual entry. Build a simple review UI (even a CLI prompt loop) that shows Stage 4 insights from recent runs and asks a reviewer to approve or reject each one.
