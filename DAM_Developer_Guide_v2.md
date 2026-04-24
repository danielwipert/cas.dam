# Chorus AI Systems — Data Analytics Manager (DAM)
## Developer Guide & Implementation Reference
**Version 2.0 · April 2026**

Supersedes v1.0. Changes from v1:
- Added Stage 6 (Supply Chain Advisor — RAG)
- Replaced two-page WeasyPrint report with editorial PDF + dark-navy HTML dashboard
- Updated renderer to a three-tier PDF fallback (WeasyPrint → Playwright/Chromium → HTML)
- Corrected Stage 4 verifier: Qwen2.5 **7B** Turbo (not 72B — unavailable serverless on this tier)
- Added Flask dashboard, Render deployment, CASDAM case-study site
- Added `historical_kpis.py` synthetic benchmarks and `build_history.py` 9-week runner
- Corrected `TRANSIT_WINDOWS` to calendar days, based on empirical test-data distribution
- Corrected repo layout (flat `dam/` → `app/core/`, `app/scripts/`, `app/dashboard/`, `app/knowledge_base/`, `docs/`)
- Documented KPI fixes (F002, F005, F009) and the Stage 4 `"monitor"`→`"observation"` fallback

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Repository Layout](#2-repository-layout)
3. [Quick Start](#3-quick-start)
4. [Architecture: Nested MVS](#4-architecture-nested-mvs)
5. [File-by-File Reference](#5-file-by-file-reference)
6. [Pipeline Detail (Stages 1–6)](#6-pipeline-detail-stages-16)
7. [Data Contracts (schemas.py)](#7-data-contracts-schemaspy)
8. [Prompt Templates (prompts.py)](#8-prompt-templates-promptspy)
9. [LLM Client (llm_client.py)](#9-llm-client-llm_clientpy)
10. [Stage MVS Classes (stages.py)](#10-stage-mvs-classes-stagespy)
11. [Stage 6 RAG Layer](#11-stage-6-rag-layer)
12. [Report Renderer](#12-report-renderer)
13. [Historical KPIs and Trend Data](#13-historical-kpis-and-trend-data)
14. [Flask Dashboard](#14-flask-dashboard)
15. [Pipeline Orchestrator](#15-pipeline-orchestrator)
16. [FactList Persistence](#16-factlist-persistence)
17. [Meta-Governance](#17-meta-governance)
18. [Test Data](#18-test-data)
19. [KPI Reference](#19-kpi-reference)
20. [Governance Gates & Degradation](#20-governance-gates--degradation)
21. [Deployment (Render) & Case-Study Site](#21-deployment-render--case-study-site)
22. [Running the Pipeline](#22-running-the-pipeline)
23. [What to Build Next](#23-what-to-build-next)

---

## 1. Project Overview

DAM automates the weekly ecommerce operations reporting workflow: ingesting CSVs from Shopify, a 3PL, FedEx, and DHL; reconciling them; computing 10 KPIs; grounding commentary in a supply-chain knowledge base; and publishing an executive PDF and an HTML dashboard.

**Cost:** ~$0.02–0.05 per run on Together AI. **Latency:** ~60 seconds end-to-end.

**What makes it architecturally interesting:** every stage is a self-governing Minimum Viable System (MVS) — the smallest unit containing all seven Chorus AI governance layers. Three levels of Beer's VSM recursion are implemented explicitly (orchestrator, stages, Stage 4 generation+verification pair); Stage 6 adds a fourth governance loop (retrieval → generation → deterministic citation check).

---

## 2. Repository Layout

```
cas.dam/
├── app/                                 # Pipeline + dashboard
│   ├── pipeline.py                      # Orchestrator + CLI
│   ├── .env                             # TOGETHER_API_KEY (gitignored)
│   │
│   ├── core/
│   │   ├── schemas.py                   # All Pydantic contracts
│   │   ├── prompts.py                   # LLM prompt builders + KPI definitions
│   │   ├── llm_client.py                # Together AI client + pricing
│   │   ├── stages.py                    # Stage1..Stage5 MVS classes
│   │   ├── stage6_supply_chain_advisor.py
│   │   ├── rag_engine.py                # FAISS retrieval, no LLM
│   │   ├── knowledge_base.py            # Run-once FAISS index builder
│   │   ├── report_renderer.py           # render_pdf_html + render_dashboard_html
│   │   ├── historical_kpis.py           # Year-long benchmarks (synthetic)
│   │   ├── factlist_store.py            # FactList JSON persistence, trends, baseline
│   │   └── meta_governance.py           # Layer5Monitor + CalibrationLog + AdversarialRunner
│   │
│   ├── scripts/
│   │   ├── generate_test_data.py
│   │   ├── generate_adversarial_data.py
│   │   ├── build_history.py             # 9-week history runner (sparklines)
│   │   └── preview_render.py            # Preview HTML/PDF without a full run
│   │
│   ├── dashboard/
│   │   ├── server.py                    # Flask app, reads output/report_data/*.json
│   │   └── templates/dashboard.html
│   │
│   ├── knowledge_base/
│   │   ├── source_registry.json         # 11 source PDFs
│   │   ├── sources/                     # PDFs (gitignored)
│   │   ├── faiss_index/                 # Built output (gitignored)
│   │   ├── chunk_store.json             # Built output (gitignored)
│   │   └── hf_cache/                    # MiniLM cache (gitignored)
│   │
│   ├── data/
│   │   ├── test/                        # Synthetic CSVs + adversarial/
│   │   └── factlists/                   # YYYY-MM-DD.json per successful run
│   │
│   └── output/                          # All gitignored
│       ├── reports/                     # DAM_<run_id>.pdf + <run_id>.html
│       ├── site/index.html              # Latest dashboard snapshot
│       ├── report_data/                 # Flask reads these
│       ├── run_logs/                    # One JSON per run
│       └── meta_governance/             # alerts, calibration, adversarial results
│
├── docs/                                # CASDAM case-study site (GitHub Pages)
│   ├── index.html
│   ├── dashboard/index.html             # Embedded latest dashboard
│   ├── assets/{chorus-logo.svg, screenshots/, sample-report.pdf}
│   ├── css/style.css
│   ├── js/main.js
│   └── sitemap.xml
│
├── planning/docs/                       # Design docs (gitignored)
│   └── DAM_Developer_Guide_v2.md
│
├── README.md                            # Repo root README
├── Procfile                             # Render: web: python app/dashboard/server.py
├── requirements.txt                     # Flask + pydantic + requests + python-dotenv
├── package.json                         # Puppeteer only (for screenshot.mjs)
├── screenshot.mjs                       # Captures report sections for the case-study site
└── .gitignore
```

Note: `requirements.txt` intentionally contains only dashboard runtime dependencies. The full pipeline (together, playwright, faiss-cpu, sentence-transformers, pymupdf, weasyprint) is a local-only install. The Render deployment serves already-generated `report_data/*.json`; it does not run the pipeline.

---

## 3. Quick Start

All pipeline commands run from the `app/` directory.

### Prerequisites

```bash
pip install pydantic together flask python-dotenv playwright pymupdf \
            faiss-cpu sentence-transformers
playwright install chromium
```

WeasyPrint is optional. On Windows it requires GTK and is usually skipped — Playwright is the working PDF engine.

### Environment

```bash
echo "TOGETHER_API_KEY=your_key" > app/.env
```

`build_history.py` loads the key from `app/.env`; `llm_client.get_client()` loads it from the process environment.

### One-time setup

```bash
cd app

# Build the FAISS knowledge base index (needs PDFs in knowledge_base/sources/)
python -m core.knowledge_base

# Generate synthetic test CSVs
python scripts/generate_test_data.py
python scripts/generate_adversarial_data.py
```

### Run

```bash
# Synthetic data
python pipeline.py --test

# Real CSVs
python pipeline.py \
  --shopify data/shopify_orders.csv \
  --tpl     data/tpl_shipments.csv \
  --fedex   data/fedex_tracking.csv \
  --dhl     data/dhl_tracking.csv \
  --week    2026-04-11

# No-run utilities
python pipeline.py --meta           # Layer 5 health summary
python pipeline.py --adversarial    # Gate verification suite

# Flask dashboard
python dashboard/server.py          # http://127.0.0.1:5000

# 9-week history (for dashboard sparklines, ~10 min, 9 × full pipeline)
python scripts/build_history.py

# Preview PDF/HTML from a saved report_data JSON (no LLM)
python scripts/preview_render.py
```

---

## 4. Architecture: Nested MVS

### The Four Levels of Recursion

```
Level 1 — DAM Pipeline Orchestrator (app/pipeline.py)
  L1 operational: six stage MVS instances
  L2 coordination: stage sequencing, Stage6→Stage5 ordering, canonical model enforcement
  L3 governance: system-level degradation decisions
  L4 assurance: cross-stage FACT_ID consistency check after Stage 4
  L5 adaptive: verifier-agreement flag + Layer5Monitor rolling metrics
  L6 constitutional: no unverified output, no financial projections
  L7 interface: CLI, CSV I/O, PDF + HTML + JSON artefacts, run log persistence

  Level 2 — Each Stage (Stage1..Stage5 in stages.py; Stage6 in stage6_supply_chain_advisor.py)
    L1: LLM call or deterministic computation
    L2: Pydantic input/output contracts
    L3: internal gate — verify own output before surfacing upward
    L4: internal assurance — Python recomputation or schema/citation check
    L5: HealthTelemetry emitted with every response
    L6: constitutional constraint (what this stage refuses to do)
    L7: MVS interface — run(input) → VerifiedOutput | DegradationSignal

    Level 3 — Stage 4 generation+verification pair
      L1: DeepSeek V3 generates insights
      L3: claim-stripping gate (FACT_ID must exist)
      L4: Qwen2.5-7B verifies claim/citation alignment
      L6: FactList is the only permitted evidence base

    Level 3 — Stage 6 retrieval+generation+citation pair
      L1: FAISS retrieves chunks; Llama 3.3 70B generates commentary + recs
      L4: deterministic Python citation validator (chunk_id and FACT_ID exact match + inline-citation stripping)
      L3: recommendations with no valid citation are stripped; domains with <3 chunks are skipped
      L6: no financial projections, no knowledge beyond retrieved chunks + FactList
```

### The MVS Interface Contract

```python
result = Stage1().run(stage1_input)

if isinstance(result, VerifiedOutput):
    output: Stage1Output    = result.payload
    telemetry: HealthTelemetry = result.health_telemetry
else:  # DegradationSignal
    reason: str = result.failure_reason
    level:  DegradationLevel = result.degradation_level_recommendation
```

The orchestrator never reaches inside a stage. Stages self-govern; the orchestrator coordinates.

### Data Flow

```
CSV files
    ↓
Stage 1  (Llama 3.3 70B)     Field mapping → canonical tables
    ↓
Stage 2  (Llama 3.3 70B)     Exact + fuzzy join → reconciliation table
    ↓
Stage 3  (Llama 3.3 70B)     KPI compute (LLM) + Python recompute → FactList
    ↓    FactList saved → data/factlists/YYYY-MM-DD.json
Stage 4  (DeepSeek V3 → Qwen2.5 7B)   Generate → verify → strip → verified_insights[]
    ↓
Stage 6  (Llama 3.3 70B + FAISS)      Per-domain commentary + recommendations with citations
    ↓
Stage 5  (deterministic)     Render PDF + dashboard HTML + report_data JSON
    ↓
run_logs/<run_id>.json  →  Layer5Monitor.analyze()
```

Stage ordering note: **Stage 6 runs after Stage 4 and before Stage 5** so that Stage 5 can embed Expert Commentary panels from Stage 6 output.

---

## 5. File-by-File Reference

| File | Purpose |
|------|---------|
| `app/pipeline.py` | Orchestrator + CLI (`--test`, `--meta`, `--adversarial`, CSV flags) |
| `app/core/schemas.py` | All Pydantic contracts; enums; Stage1..Stage6 I/O types |
| `app/core/prompts.py` | Six prompt builders, `KPI_DEFINITIONS`, `KPI_THRESHOLDS`, canonical field descriptions, `SYSTEM_PROMPTS` |
| `app/core/llm_client.py` | Together AI wrapper; `get_client()`, `call_llm()`, `parse_json_response()`; model constants + pricing |
| `app/core/stages.py` | `Stage1`..`Stage5` MVS classes, `TRANSIT_WINDOWS`, Stage 5 PDF fallback chain |
| `app/core/stage6_supply_chain_advisor.py` | `Stage6SupplyChainAdvisor` MVS; citation validator; inline-citation stripper |
| `app/core/rag_engine.py` | `build_domain_context()`; FAISS query + deduplication per domain |
| `app/core/knowledge_base.py` | Run-once index builder (`python -m core.knowledge_base`) |
| `app/core/report_renderer.py` | `render_pdf_html()` + `render_dashboard_html()`; shared design system |
| `app/core/historical_kpis.py` | `HISTORICAL_BENCHMARKS` dict: year-long synthetic comparison values |
| `app/core/factlist_store.py` | `save_factlist`, `load_prior_factlist`, `load_cost_baseline`, `get_baseline_status`, `get_kpi_trend`, `list_stored_weeks` |
| `app/core/meta_governance.py` | `Layer5Monitor`, `CalibrationLog`, `AdversarialRunner`, `print_layer5_summary()` |
| `app/dashboard/server.py` | Flask app; routes `/`, `/report/<run_id>`, `/api/runs`, `/api/data/<run_id>` |
| `app/scripts/generate_test_data.py` | Synthetic CSVs; reproducible via `random.seed(42)` |
| `app/scripts/generate_adversarial_data.py` | Planted-error CSVs, four categories |
| `app/scripts/build_history.py` | Runs the full pipeline for 9 weeks to seed sparkline data |
| `app/scripts/preview_render.py` | Re-renders PDF + HTML from a saved `report_data/*.json` with mock Stage 6 content |
| `screenshot.mjs` | Puppeteer: captures report sections for the CASDAM case-study site |
| `Procfile` | `web: python app/dashboard/server.py` (Render deploy target) |
| `requirements.txt` | Dashboard-only runtime deps |

---

## 6. Pipeline Detail (Stages 1–6)

### Stage 1 — Ingestion & Normalisation
- **Model:** Llama 3.3 70B (`MODEL_STAGES_1_3`)
- **What it does:** Reads four CSVs with non-canonical column names. Asks the LLM for a mapping. Python applies the mapping and validates every row.
- **Constraint:** Maps fields; never imputes. Ambiguity is disclosed in `FieldMappingLog`, not silently resolved.
- **Gates:** Duplicate `order_id` → halt. LLM mapping fails after retry → halt. Any Stage 1 halt propagates to full pipeline halt.
- **Datetime parsing** tolerates three formats (`%Y-%m-%dT%H:%M:%S[.%f]`, `%Y-%m-%d %H:%M:%S[.%f]`, `%Y-%m-%d`); microsecond suffixes parse correctly (fixed in commit `8733033`).

### Stage 2 — Reconciliation
- **Model:** Llama 3.3 70B for fuzzy matches only; phase 1 (exact) is pure Python.
- **Gates:** Match rate < 80% → halt. 80–95% → warning disclosed in report. Fuzzy confidence floor: 0.90 (Python-enforced, not trusted from the LLM).

### Stage 3 — KPI Computation → FactList
- **Model:** Llama 3.3 70B for LLM-side compute; Python recomputes every KPI. Python always wins on mismatch.
- **Mismatch tolerance:** 1 % relative (`abs(py - llm) / (abs(py) + 1e-9) > 0.01`).
- **Constraint:** FactList is immutable after emission. `final_value == python_value`; `llm_value` is logged only.
- **KPI fixes applied since v1:**
  - F002 On-Time Ship Rate: compare `shipped_at.date() <= promised_ship_date.date()`
  - F005 On-Time Delivery Rate: `TRANSIT_WINDOWS` updated to calendar days
  - F009 Label Lag: computed from `shipped_at` → `first_scan_at`

### Stage 4 — Insight Generation + Verification (third-level MVS)
- **Models:** DeepSeek V3 generator, Qwen2.5-**7B** Turbo verifier (not 72B — 72B is not available serverless on this Together tier).
- **Why two families:** Structurally different training + architecture = lower correlated blind spots. If both agree, agreement means more than self-consistency.
- **Third-level recursion:** generation = L1, Qwen verification = L4, claim-stripping gate = L3, FactList = L6.
- **Claim stripping:** unconditional Python check —
  ```python
  if not all(fid in valid_fact_ids for fid in claim.cited_fact_ids):
      verdict = "stripped"
  ```
  If Qwen fails after retry, **all claims are stripped** and Stage 4 emits a DegradationSignal rather than releasing unverified insights.
- **Claim-type fallback:** LLMs occasionally return `"monitor"` as `claim_type` (not in the enum). Stage 4 coerces any out-of-enum value to `"observation"` (`stages.py:1175`).

### Stage 6 — Supply Chain Advisor (RAG)
See [§11 Stage 6 RAG Layer](#11-stage-6-rag-layer).

### Stage 5 — Report Compilation
- **Model:** none; fully deterministic.
- **What it produces:**
  - PDF at `output/reports/DAM_<run_id>.pdf` (three-tier fallback, see §12)
  - Dashboard HTML at `output/reports/<run_id>.html`
  - Latest-snapshot HTML at `output/site/index.html`
- **L4 structural completeness:** `REQUIRED_SECTIONS` = `executive_headline`, `fulfillment_block`, `carrier_performance_block`, `cost_block`, `operational_integrity_block`, `verification_footer`. Missing any → halt.
- **PDF page count** is approximated from file size when Playwright is used; WeasyPrint provides an exact count.

---

## 7. Data Contracts (schemas.py)

### Key Enums

| Enum | Values |
|------|--------|
| `MatchMethod` | `exact`, `fuzzy_llm`, `unmatched` |
| `JoinStatus` | `matched`, `unmatched`, `partial` |
| `ThresholdStatus` | `green`, `yellow`, `red`, `informational` |
| `DegradationLevel` | `normal` (0), `partial` (1), `halt` (2) |
| `KPIDomain` | `fulfillment`, `carrier_performance`, `cost`, `operational_integrity` |
| `ClaimType` | `observation`, `hypothesis`, `recommended_action` |
| `VerificationVerdict` | `aligned`, `stripped` |
| `FinancialStatus` | `paid`, `pending`, `refunded`, `partially_refunded`, `voided`, `authorized` |
| `FulfillmentStatusShopify` | `fulfilled`, `partial`, `unfulfilled`, `restocked` |

### Stage I/O Contracts

| Stage | Input | Output |
|-------|-------|--------|
| Stage 1 | `Stage1Input` (4 CSV paths + run_id) | `Stage1Output` (canonical tables + FieldMappingLog) |
| Stage 2 | `Stage2Input` (canonical tables) | `Stage2Output` (reconciliation + rates) |
| Stage 3 | `Stage3Input` (recon + prior FactList + week_date) | `Stage3Output` (FactList, mismatches, `python_verified`) |
| Stage 4 | `Stage4Input` (FactList + `python_verified`) | `Stage4Output` (verified_insights, stripped log, domain_recommendations) |
| Stage 6 | `Stage6Input` (Stage4Output + FactList) | `Stage6Output` (domain_blocks, domains_skipped, total_chunks_retrieved) |
| Stage 5 | `Stage5Input` (all upstream outputs + signals) | `Stage5Output` (pdf_path, html_path, render_time, page_count, sections_rendered) |

### Stage 6 types

```python
Stage6Recommendation(
    text: str,
    source_chunk_ids: list[str],
    source_fact_ids:  list[str],
)

Stage6DomainBlock(
    domain: KPIDomain,
    commentary: str,
    recommendations: list[Stage6Recommendation],
    chunk_citations: list[str],
    citation_sources: list[str],
)

Stage6Output(
    domain_blocks:           list[Stage6DomainBlock],
    domains_skipped:         list[KPIDomain],
    total_chunks_retrieved:  int,
)
```

### HealthTelemetry (shared)

Embedded in both `VerifiedOutput` and `DegradationSignal`. Stage-specific fields include:
`mapping_confidence_avg`, `ambiguous_field_count` (S1), `exact_match_rate`, `fuzzy_match_volume` (S2), `kpi_mismatch_count` (S3), `claim_count_generated`, `claim_acceptance_rate`, `cross_verifier_agreement` (S4), `render_time_seconds`, `pdf_page_count` (S5), `domains_processed`, `chunks_retrieved_per_domain`, `green_kpi_domains`, `yellow_red_kpi_domains`, `recommendations_stripped` (S6).

---

## 8. Prompt Templates (prompts.py)

Six builder functions; each returns a complete string expecting JSON-only output.

| Function | Stage | Model | Key instruction |
|----------|-------|-------|-----------------|
| `build_stage1_prompt()` | 1 | Llama 3.3 70B | Map every source column; `canonical_field=null` if no match |
| `build_stage2_exact_prompt()` | 2 | Llama 3.3 70B | Confirm join keys; flag anomalies only |
| `build_stage2_fuzzy_prompt()` | 2 | Llama 3.3 70B | Propose fuzzy matches with confidence ≥ 0.90 |
| `build_stage3_prompt()` | 3 | Llama 3.3 70B | Compute LLM values only; leave `python_value=null` |
| `build_stage4_generation_prompt()` | 4 | DeepSeek V3 | Every claim must cite FACT_IDs; no projections |
| `build_stage4_verification_prompt()` | 4 | Qwen2.5 7B | Strip if FACT_ID wrong, even if directionally correct |

Stage 6's prompt is built in `stage6_supply_chain_advisor._build_prompt()` — not a module-level builder, because it depends on the `DomainContext` object assembled by `rag_engine`.

**Retry pattern:** each builder accepts `retry_context: str`. On retry, the gate's failure reason is passed back — same function, two modes.

**Module constants:** `KPI_DEFINITIONS`, `KPI_THRESHOLDS`, `SHOPIFY_/TPL_/FEDEX_/DHL_CANONICAL_DESCRIPTIONS`, `SYSTEM_PROMPTS`.

---

## 9. LLM Client (llm_client.py)

### Model Constants

```python
MODEL_STAGES_1_3 = "meta-llama/Llama-3.3-70B-Instruct-Turbo"
MODEL_STAGE4_GEN = "deepseek-ai/DeepSeek-V3"
MODEL_STAGE4_VER = "Qwen/Qwen2.5-7B-Instruct-Turbo"
MODEL_FALLBACK   = "meta-llama/Llama-3.3-70B-Instruct-Turbo"   # same family — last resort
MAX_TOKENS       = 4096
API_TIMEOUT      = 120
```

Three distinct model families. Together AI serverless availability on this account tier excludes Qwen2.5-72B and Mixtral-8x22B; the v1 guide listed both by mistake.

### Pricing

```python
MODEL_PRICING = {
    "meta-llama/Llama-3.3-70B-Instruct-Turbo": {"input": 0.88, "output": 0.88},  # per 1M tokens
    "deepseek-ai/DeepSeek-V3":                 {"input": 1.25, "output": 1.25},
    "Qwen/Qwen2.5-7B-Instruct-Turbo":          {"input": 0.30, "output": 0.30},
}
```

### API

```python
client = get_client()                       # reads TOGETHER_API_KEY; EnvironmentError if missing
text, cost, latency = call_llm(system_prompt, user_prompt, model, client=client,
                                temperature=0.1, max_tokens=4096)
parsed = parse_json_response(raw)           # handles ```json fences + leading preamble
```

---

## 10. Stage MVS Classes (stages.py)

### TRANSIT_WINDOWS (calendar days)

```python
TRANSIT_WINDOWS = {
    ("FedEx", "FedEx Ground"):                    8,   # 5 biz-day SLA
    ("FedEx", "FedEx Home Delivery"):             8,
    ("FedEx", "FedEx Express Saver"):             6,
    ("FedEx", "FedEx 2Day"):                      5,
    ("FedEx", "FedEx Overnight"):                 4,
    ("DHL Ecommerce", "DHL Ecommerce Ground"):    8,
    ("DHL Ecommerce", "DHL Ecommerce Expedited"): 6,
}
```

Windows are set empirically to sit between the maximum on-time transit and minimum late transit in the seed-42 test data. Calendar days account for weekend expansion plus small random delivery-time variation.

### Stage 3 Python KPI computations

All 10 KPIs live in `_compute_all_kpis()`:

```
F001 Order to Ship Time        mean((shipped_at − order_created_at).hours)
F002 On-Time Ship Rate         on_time / eligible     # date-only comparison
F003 Unshipped Orders Rate     unshipped / total
F004 Transit Time              mean((delivered_at − first_scan_at).hours)
F005 On-Time Delivery Rate     delivered_within_TRANSIT_WINDOWS / delivered
F006 Carrier Mix               FedEx fraction
F007 Shipping Cost per Order   sum(carrier_costs) / total_shipped
F008 Cost by Carrier           mean(cost per carrier)
F009 Label Lag                 mean((first_scan_at − shipped_at).hours)
F010 Shipment Match Rate       matched / total
```

### Stage 5 PDF fallback chain

```python
def _convert_to_pdf(html, run_id):
    try WeasyPrint     → (pdf_path, page_count)     # Linux/Mac, requires GTK
    try Playwright+Chromium → (pdf_path, ~size/80)  # Windows path; used in practice
    fallback: write HTML only → (html_path, len/3000)
```

Playwright is the working engine on Windows; WeasyPrint needs GTK and is typically skipped. The HTML fallback lets the user print-to-PDF from a browser.

---

## 11. Stage 6 RAG Layer

Stage 6 = retrieval → generation → deterministic citation check. Runs after Stage 4, before Stage 5.

### Sources (`app/knowledge_base/source_registry.json`)

11 supply-chain textbooks and benchmarks, domain-tagged:

| Source | Domain tags | Type |
|--------|-------------|------|
| The Goal (Goldratt) | fulfillment | framework |
| FedEx Service Guide 2026 | carrier_performance, cost | benchmark |
| DHL Ecommerce Trend Report | carrier_performance, cost | benchmark |
| CSCMP Definitions and Glossary | all four | definition |
| SCOR Model (ASCM) | all four | framework |
| Logistics & Supply Chain Management (Christopher) | all four | framework |
| Fundamentals of Supply Chain | all four | framework |
| Operations Management (Slack et al.) | fulfillment, operational_integrity | framework |
| Operations and Supply Chain Management | all four | framework |
| Supply Chain Management: Strategy, Planning, and Operation (Chopra) | all four | framework |
| Supply Chain Management and Advanced Planning (Stadtler) | fulfillment, cost, op_int | framework |

### Index build

```bash
cd app && python -m core.knowledge_base
# python -m core.knowledge_base --smoke-test    # tiny corpus check
```

- Reads PDFs via `pymupdf` (`fitz`).
- Chunks to ~450-word blocks with 50-word overlap.
- Embeds with `sentence-transformers/all-MiniLM-L6-v2` (HF cache redirected into `knowledge_base/hf_cache/` to avoid Windows `.cache` conflicts).
- Writes `knowledge_base/faiss_index/index.faiss`, `id_map.json`, and `chunk_store.json`.

### Retrieval (rag_engine.py)

- `build_domain_context(domain, factlist, stage4_output) → DomainContext`
- Lazy singletons for the FAISS index, id map, chunk store, embedding model.
- Filters chunks by `domain_tag` overlap; dedupes; returns `RetrievedChunk` list scored by FAISS.
- `MIN_CHUNKS_FOR_DOMAIN = 3` — fewer than 3 unique chunks → `sufficient=False` → Stage 6 skips the domain.

### Generation + validation (stage6_supply_chain_advisor.py)

- **Model:** Llama 3.3 70B; `MAX_RETRIES=1`, fallback to `MODEL_FALLBACK`.
- **Prompt contract:** clean-prose commentary + 2–3 recommendations; chunk/FACT IDs belong ONLY in the `*_chunk_ids` / `source_fact_ids` arrays, never inside prose.
- **Citation validator (`_validate_and_build_block`)**
  - Commentary must cite ≥ 1 valid chunk_id; if none, fallback is the top-scoring retrieved chunk's id.
  - Each recommendation must cite ≥ 1 valid chunk_id OR ≥ 1 valid FACT_ID. Otherwise stripped.
  - `_strip_inline_citations()` scrubs any leaked IDs from prose regardless of LLM output — chunk_id patterns (`word_digits_hexhash`), parenthetical ID groups, and `F\d{3}` markers are removed and spacing normalised.
- **Constitutional L7 check:** `is_index_available()` false → immediate DegradationSignal Level 1.
- **Gate L3:** zero valid domain blocks produced → DegradationSignal Level 1.

### Stage 6 degradation behaviour

Failure in Stage 6 is always a Level 1 (partial) degradation, never a halt. Stage 5 still ships the report; the verification footer discloses Stage 6 absence.

---

## 12. Report Renderer

`app/core/report_renderer.py` exports two self-contained functions:

- `render_pdf_html(stage5_input) → (html, sections_rendered)` — white-background editorial PDF
- `render_dashboard_html(stage5_input) → html` — dark-navy HTML dashboard

Both share one design system:

- **Typefaces:** Playfair Display 700/900 (display serif) · Inter 300–800 (body) · JetBrains Mono 400–600 (data values)
- **Palette:** gold `#c9a84c`, ink `#0d1117`, navy `#070d1a`, surface `#0d1627`
- **Status colours:** green `#059669`, amber `#d97706`, red `#dc2626`
- **Grid:** 40 px body margin, consistent Elam-style gutters

### Editorial PDF

- Gold masthead bar, Playfair wordmark, Inter metadata row
- Domain sections as bordered cards with 3 px gold top border
- Per-domain Data Analysis panel (white background + black left border)
- Per-domain Expert Commentary panel (warm-cream `#fdf8ed` + gold left border) — populated from Stage 6
- `@page { margin: 18px 0 }`; `page-break-inside: avoid` on individual cards/panels ONLY, never on whole domain sections (causes whitespace gaps)
- Minimum 8 pt font, minimum weight 400 (lighter weights render smudgy in Chromium PDF)

### Dark-navy dashboard

- Sticky navbar, hero status banner, full historical comparison tables (last week / month / 3-month / year)
- Collapsible data-provenance section per domain
- Self-contained — data baked in at render time; works as a static file
- Latest successful run always overwrites `output/site/index.html`

### Stage 5 output paths

```
output/reports/DAM_<run_id>.pdf        # editorial PDF
output/reports/<run_id>.html           # dashboard HTML (run-specific)
output/site/index.html                 # dashboard HTML (latest snapshot)
```

### Preview renderer (`scripts/preview_render.py`)

Loads a saved `report_data/*.json`, fabricates `Stage6Output` with mock commentary (Stage 6 outputs are not persisted in report_data), and re-renders both HTML files without calling any LLM. Used for design iteration.

---

## 13. Historical KPIs and Trend Data

Two sources feed the dashboard's comparison tables and sparklines.

### `historical_kpis.py` — long-run synthetic benchmarks

`HISTORICAL_BENCHMARKS` is a dict keyed by KPI name containing:
```python
{
    "direction": "up" | "down" | "neutral",
    "unit":      "%" | "h" | "$",
    "last_week":      float,
    "month_avg":      float,
    "three_month_avg":float,
    "year_avg":       float,
}
```
Represents ~1 year of operational history ending 2026-04-05. Used by `report_renderer.py` and `dashboard/server.py` to render the year-long comparison tables.

### `factlist_store.get_kpi_trend()` — short-run actuals

```python
get_kpi_trend(kpi_name, n_weeks=4) -> list[{week_date, value, threshold_status}]
```
Reads `data/factlists/YYYY-MM-DD.json` files. Powers dashboard sparklines. To populate the full 9-week sparkline data, run `scripts/build_history.py`.

### `build_history.py`

Runs 9 pipeline invocations across 9 synthetic weeks, each with its own seed and a `progress ∈ [0, 1]` parameter that interpolates performance from year-ago to current. Wipes prior factlists for the target weeks, regenerates CSVs via `generate_test_data.generate_week()`, and runs `pipeline.py`. ~10 minutes end-to-end.

---

## 14. Flask Dashboard

`app/dashboard/server.py`. Reads `output/report_data/*.json` files (written by `pipeline.py::_save_report_data()`).

### Routes

| Route | Purpose |
|-------|---------|
| `/` | Latest run dashboard |
| `/report/<run_id>` | Specific run |
| `/api/runs` | JSON list of the 12 most recent runs |
| `/api/data/<run_id>` | Raw report_data JSON for a run |

### Template context

`_template_ctx()` passes:
- `run` — full report_data dict
- `recent_runs` — last 12 `{run_id, report_week, final_status}`
- `domain_status` — worst threshold status per domain
- `domain_scores` — composite score per domain (green=9 / yellow=6 / info=5.5 / red=3, averaged)
- `kpi_summary` — `{n_green, n_yellow, n_red, n_info}`
- `kpi_trends` — 8-week history per KPI name (via `get_kpi_trend`)
- `historical_benchmarks` — the full `HISTORICAL_BENCHMARKS` dict
- `domains` / `domain_labels`

### Run

```bash
cd app && python dashboard/server.py
# PORT env var (default 5000); FLASK_DEBUG=1 for debug mode
```

---

## 15. Pipeline Orchestrator

### Stage sequencing

```
S1 → S2 → S3 → (save FactList) → S4 → orch L4 FACT_ID check → S6 → S5 → save run_log
```

### Degradation decision rules

| Condition | Decision |
|-----------|----------|
| Stage 1 → DegradationSignal | Full halt, no output |
| Stage 2 → DegradationSignal | Full halt, no output |
| Stage 3 → DegradationSignal | Partial, skip 4+6+5 |
| Stage 4 → DegradationSignal | Partial, continue with empty `verified_insights` |
| Stage 6 → DegradationSignal | Partial, continue; Stage 5 renders without Expert Commentary |
| Stage 5 → DegradationSignal | Partial, no PDF; run log still written |
| Orch. L4 FACT_ID check fails | Strip orphaned insights, continue |

### Orchestrator L4 cross-stage check

After Stage 4, the orchestrator verifies every `cited_fact_ids` entry exists in the Stage 3 FactList. Structurally impossible within any single stage — only the orchestrator has visibility across both outputs.

```python
valid_fact_ids = {f.fact_id for f in s3_out.factlist}
orphaned = [ins for ins in s4_out.verified_insights
            if not all(fid in valid_fact_ids for fid in ins.cited_fact_ids)]
```

### Run log

Every run writes `output/run_logs/<run_id>.json` and `output/report_data/<run_id>.json`. The report_data file is the Flask dashboard's input.

### Verifier-agreement flag

The orchestrator appends per-run Stage 4 `cross_verifier_agreement` to `output/run_logs/verifier_agreement_flag.json` (last 10). If the last three runs all had 1.0 agreement, it prints a meta-governance alert: "Review Stage 4 verification prompt."

### CLI

```bash
python pipeline.py --test                               # synthetic data, week 2026-04-04
python pipeline.py --shopify X --tpl X --fedex X --dhl X --week YYYY-MM-DD
python pipeline.py --meta                               # Layer 5 health summary
python pipeline.py --adversarial                        # gate verification suite
```

---

## 16. FactList Persistence

One JSON file per successful run: `data/factlists/YYYY-MM-DD.json`. The week date is the ISO date of the last day of the reporting week.

### API

```python
save_factlist(factlist, week_date) -> path
load_prior_factlist(current_week_date) -> list[FactListEntry] | None
load_cost_baseline(current_week_date) -> {fedex_avg, dhl_avg, overall_avg,
                                           weeks_included, is_full_baseline} | None
get_baseline_status(current_week_date) -> human-readable string for report footer
list_stored_weeks() -> list[str]
get_kpi_trend(kpi_name, n_weeks=4) -> list[{week_date, value, threshold_status}]
```

### Baseline behaviour

- First run: `load_prior_factlist` returns `None`; WoW deltas are `None`; cost KPIs informational.
- Weeks 1–3: partial baseline; `get_baseline_status` returns `"baseline building (n/4 weeks)"`.
- Week 4+: full baseline available; `is_full_baseline=True`.

Note: the 4-week cost baseline is computed by `load_cost_baseline()` but is **not yet wired into Stage 3's `_classify_threshold()`**. Cost KPIs remain `informational`. See §23 for the follow-up.

---

## 17. Meta-Governance

### Layer5Monitor (`core/meta_governance.py`)

```python
monitor = Layer5Monitor()
alerts  = monitor.analyze(n_recent=10)   # list[Alert]
summary = monitor.summary(n_recent=10)   # dict of rolling metrics
print_layer5_summary(n_recent=10)        # CLI printer (pipeline.py --meta uses this)
```

**Alert thresholds:**

| Alert | Condition | Severity |
|-------|-----------|----------|
| `high_halt_rate` | > 5 % runs halted | critical |
| `rising_retry_rate` | > 20 % of stage calls retried | warning |
| `low_claim_acceptance` | avg < 70 % | warning |
| `high_kpi_mismatch_rate` | avg > 3 mismatches/run | warning |
| `verifier_agreement_lock` | 100 % agreement 3 consecutive runs | critical |
| `high_stage_failure_rate` | any stage failing > 30 % | warning |

Bounded authority: Layer5Monitor writes `output/meta_governance/alerts.json`. It cannot modify thresholds, swap models, or release output.

### CalibrationLog

Records human verdicts alongside system verdicts for Stage 4 claims. `false_positive_rate > 5 %` raises `fp_alert`. Persists to `output/meta_governance/calibration_log.json`.

### AdversarialRunner

```python
runner = AdversarialRunner()
results = runner.run_all()     # also used by `pipeline.py --adversarial`
```

**Expected catches:**

| Category | Stage | Expected |
|----------|-------|----------|
| `duplicate_order_ids` | stage_1 | DegradationSignal |
| `missing_required_fields` | stage_1 | DegradationSignal |
| `mismatched_timestamps` | stage_1 | DegradationSignal |
| `corrupted_tracking` | stage_2 | `min_unmatched: 3` |

Results written to `output/meta_governance/adversarial_results.json` with timestamps. Run monthly or after any model/prompt change.

---

## 18. Test Data

### Clean test data (`scripts/generate_test_data.py`)

Reproducible via `random.seed(42)`. Non-canonical column names force real Stage 1 work.

| Source | Key non-canonical columns |
|--------|--------------------------|
| Shopify | `Name` (order_id), `Province` (state), `Ship By Date` (promised_ship_date) |
| 3PL | `Order Ref` (order_id), `Tracking #`, `Freight Cost` |
| FedEx | `Tracking Number`, `First Scan Date`, `Billed Weight Charge` |
| DHL | `Waybill` (tracking), `Picked Up` (first_scan), `POD Date` (delivered_at) |

`generate_week(week_date, n_orders, seed, out_dir, progress)` is also callable directly (used by `build_history.py`). The `progress` parameter interpolates KPI values from "year-ago performance" (0.0) to "current performance" (1.0).

### Adversarial test data (`scripts/generate_adversarial_data.py`)

Four subdirectories under `data/test/adversarial/`, each a full four-CSV set. Errors are surgical — minimum corruption to trigger a single gate.

---

## 19. KPI Reference

### The 10 KPIs

| FACT_ID | Domain | KPI | Formula | Unit |
|---------|--------|-----|---------|------|
| F001 | fulfillment | Order to Ship Time | mean(shipped_at − order_created_at) | hours |
| F002 | fulfillment | On-Time Ship Rate | shipped_on_time / eligible_orders | decimal |
| F003 | fulfillment | Unshipped Orders Rate | unshipped / total_orders | decimal |
| F004 | carrier_performance | Transit Time | mean(delivered_at − first_scan_at) | hours |
| F005 | carrier_performance | On-Time Delivery Rate | delivered_within_window / delivered | decimal |
| F006 | carrier_performance | Carrier Mix | FedEx_shipments / total_shipments | decimal |
| F007 | cost | Shipping Cost per Order | sum(carrier_costs) / total_shipped | USD |
| F008 | cost | Cost by Carrier | mean(carrier_cost) per carrier | USD |
| F009 | operational_integrity | Label Lag | mean(first_scan_at − shipped_at) | hours |
| F010 | operational_integrity | Shipment Match Rate | matched / total_shipments | decimal |

### Thresholds

| KPI | Green | Yellow | Red | Notes |
|-----|-------|--------|-----|-------|
| On-Time Ship Rate | ≥ 98 % | 95–97 % | < 95 % | critical |
| On-Time Delivery Rate | ≥ 98 % | 95–97 % | < 95 % | critical |
| Shipment Match Rate | ≥ 99.8 % | 99.0–99.7 % | < 99 % | critical |
| Unshipped Orders Rate | < 1 % | 1–3 % | > 3 % | critical |
| Order to Ship Time | ≤ 24 h | 24–48 h | > 48 h | tunable |
| Transit Time | ≤ window | +24 h | > +24 h | tunable |
| Shipping Cost per Order | ±10 % baseline | 10–25 % | > 25 % | informational pre-baseline |
| Cost by Carrier | ±10 % baseline | 10–25 % | > 25 % | informational pre-baseline |
| Carrier Mix | informational | — | — | no threshold |
| Label Lag | ≤ 4 h | 4–12 h | > 12 h | tunable |

---

## 20. Governance Gates & Degradation

### Gate Summary

| Gate | Stage | Check | On Failure |
|------|-------|-------|------------|
| 1 | 1 | Duplicate order_id | halt |
| 1 | 1 | LLM mapping fails after retry | halt |
| 2 | 2 | Match rate < 80 % | halt |
| 2 | 2 | Match rate 80–95 % | warning in report |
| 3 | 3 | Python cannot compute KPI | DegradationSignal for that KPI |
| 3 | 3 | LLM/Python mismatch | Python wins, logged |
| 4 | 4 | FACT_ID invalid or claim unsupported | claim stripped |
| 4 | 4 | Zero claims survive | DegradationSignal (partial) |
| 6 | 6 | FAISS index unavailable | DegradationSignal (partial) |
| 6 | 6 | < 3 chunks for a domain | domain skipped |
| 6 | 6 | No valid block after validation | DegradationSignal (partial) |
| 5 | 5 | Required section absent | halt |
| 5 | 5 | PDF page count unusual | warning only |
| Orch. L4 | orch. | Insight cites FACT_ID not in FactList | strip insight |

### Degradation Levels

| Level | Name | Trigger | Behaviour |
|-------|------|---------|-----------|
| 0 | Normal | All stages complete | Full report |
| 1 | Partial | S3/S4/S5/S6 fails | Report produced, disclosure included |
| 2 | Halt | S1 or S2 fails | No report, run log written |

### Retry policy

One retry per LLM stage. On retry, `MODEL_FALLBACK` is used. If retry fails, the stage degrades — it never burns tokens on a third attempt.

### Constitutional constraints (non-overridable)

- No unverified output released. If verification is unavailable, halt.
- Stage 1 or 2 failure always propagates to full pipeline halt.
- FactList is immutable after Stage 3 emission.
- Stage 4 refuses input where `python_verified=False`.
- Stage 6 refuses input if FAISS index is unavailable.
- Report must include all degradation disclosures.
- No financial projections or investment recommendations under any path.

---

## 21. Deployment (Render) & Case-Study Site

### Render (Flask dashboard)

- `Procfile`: `web: python app/dashboard/server.py`
- `requirements.txt` is minimal on purpose — the deployed service serves pre-generated `report_data/*.json`, not the full pipeline.
- Server reads `PORT` from env; binds `0.0.0.0`.

To update production data: run the pipeline locally, commit new `output/report_data/*.json` files, push. (Note: `output/` is gitignored by default — include `report_data/` explicitly when updating production.)

### CASDAM case-study site (`docs/`)

Static single-page site hosted on GitHub Pages from `/docs`. Live at `danielwipert.github.io/cas.dam` once repo Settings → Pages → main → /docs is enabled.

Assets:
- `docs/index.html` — marketing site
- `docs/dashboard/index.html` — embedded latest dashboard
- `docs/assets/chorus-logo.svg` — teal signal arcs + CHORUS AI SYSTEMS wordmark
- `docs/assets/screenshots/report-p1.png`, `report-p2.png`, `report-p3.png` — produced by `screenshot.mjs`
- `docs/assets/sample-report.pdf` — copy of a representative pipeline output
- `docs/sitemap.xml`
- `docs/css/style.css`, `docs/js/main.js`

### `screenshot.mjs`

Puppeteer script at repo root. Loads a specific rendered report HTML, captures three sections (masthead + first domain, verification footer, richest Stage 6 panel), writes PNGs into `docs/assets/screenshots/`. Run with `node screenshot.mjs`.

---

## 22. Running the Pipeline

### Expected console output (abridged)

```
============================================================
  Chorus AI — Data Analytics Manager
  Run ID : DAM-20260411-143022-a1b2c3
  Week   : 2026-04-11
============================================================

[ Stage 1 ] Ingestion & Normalization...
  + 150 orders  | 147 shipments  | 140 carrier records

[ Stage 2 ] Reconciliation...
  + Match rate: 95.2%  | Fuzzy matches: 0  | Unmatched: 7
  ! Match rate below 95% -- disclosed in report

[ Stage 3 ] KPI Computation...
  + 10 KPIs computed  | LLM/Python mismatches: 2
  [R]  Unshipped Orders Rate              0.0200
  [Y]  On-Time Ship Rate                  0.9660
  [Y]  On-Time Delivery Rate              0.9660
  [Y]  Shipment Match Rate                0.9524
  [G]  Order to Ship Time                18.5000
  ...
  FactList saved : data/factlists/2026-04-11.json
  Baseline status: full 4-week rolling baseline (8 weeks of history)

[ Stage 4 ] Insight Generation + Verification...
  + 12 verified insights | Acceptance: 83% | Verifier agreement: 83%

[ Stage 6 ] Supply Chain Advisor (RAG)...
  [OK]   fulfillment: 2 recommendations, 4 citations
  [OK]   carrier_performance: 2 recommendations, 3 citations
  [OK]   cost: 3 recommendations, 5 citations
  [OK]   operational_integrity: 2 recommendations, 3 citations
  + 4 domain blocks | 40 chunks retrieved

[ Stage 5 ] Report Compilation...
  + Dashboard HTML: output/reports/DAM-...html
  + PDF: output/reports/DAM_DAM-...pdf | Pages: 3 | Render: 4.1s

============================================================
  Run complete -- status: FULL
============================================================
  Total API cost : $0.0310
  Total latency  : 18.4s
```

---

## 23. What to Build Next

### Immediate

- **Wire cost baseline into Stage 3 thresholds.** `factlist_store.load_cost_baseline()` returns the rolling 4-week averages; Stage 3 currently still marks F007/F008 as `informational`. Hook the baseline into `_classify_threshold()` so cost KPIs graduate to green/yellow/red once `is_full_baseline=True`.
- **`--dry-run` flag.** Runs Stage 1 only, prints `FieldMappingLog`, exits. Validates a new client's CSV format before a full run.
- **GitHub Actions scheduler.** Weekly trigger, CSVs from S3/SFTP, commits PDF to a reports branch or emails it.
- **Deployed-data sync script.** A small helper that copies fresh `output/report_data/*.json` into the tracked set for the Render deployment.

### Medium

- **Email/Slack delivery.** Post-Stage-5 step: attach the PDF and a summary to an email or Slack webhook.
- **Stage 6 calibration loop.** Extend `CalibrationLog` to cover Stage 6 recommendations, not just Stage 4 claims.
- **Real-data knowledge base.** Replace synthetic `HISTORICAL_BENCHMARKS` with values derived from accumulated factlists once 52+ weeks exist.

### Future

- **Multi-client support.** Parameterise `FACTLIST_DIR`, `LOG_DIR`, `REPORTS_DIR`, `KB_DIR` by client ID. Isolated storage, separate Layer 5 monitoring and cost baselines per client.
- **Additional carriers (UPS, USPS, Canada Post).** Add to `TRANSIT_WINDOWS`, add canonical descriptions, update Stage 1 `sources` list.
- **Adversarial test expansion.** Add categories whenever a real-world failure is discovered. `AdversarialRunner.EXPECTED_CATCHES` is the registry.
- **Calibration UI.** A small CLI loop (or Streamlit page) that shows recent Stage 4/6 outputs and records reviewer verdicts into `CalibrationLog`.
