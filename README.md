# Chorus AI — Data Analytics Manager (DAM)

**Weekly ecommerce operations report, end-to-end, under $0.05 per run.**

DAM is a six-stage governed pipeline that ingests raw Shopify, 3PL, and carrier CSVs, reconciles them, computes 10 operational KPIs, generates executive commentary grounded in a supply-chain knowledge base (RAG), and publishes two outputs: an editorial PDF and a dark-theme HTML dashboard.

Every stage is a self-governing Minimum Viable System (MVS) implementing all seven Chorus AI governance layers. Every LLM claim cites a verified FACT_ID or a knowledge-base chunk_id; Python recomputes every KPI; unverified output is stripped before the report is released.

---

## Repository layout

```
cas.dam/
├── app/                              # The pipeline
│   ├── pipeline.py                   # Orchestrator + CLI
│   ├── core/                         # All stage and governance code
│   │   ├── schemas.py                # Pydantic data contracts
│   │   ├── prompts.py                # LLM prompt builders
│   │   ├── llm_client.py             # OpenRouter client + preflight + pricing
│   │   ├── stages.py                 # Stages 1–5 MVS classes
│   │   ├── stage6_supply_chain_advisor.py
│   │   ├── rag_engine.py             # FAISS query/retrieval
│   │   ├── knowledge_base.py         # Run-once FAISS index builder
│   │   ├── historical_kpis.py        # Synthetic year-long benchmarks
│   │   ├── factlist_store.py         # FactList JSON persistence + trends
│   │   ├── report_renderer.py        # PDF + dashboard HTML renderers
│   │   └── meta_governance.py        # Layer 5 monitor + adversarial runner
│   ├── scripts/
│   │   ├── generate_test_data.py     # Synthetic clean CSVs
│   │   ├── generate_adversarial_data.py
│   │   ├── build_history.py          # 9-week history run (sparkline data)
│   │   └── preview_render.py         # Preview reports without a full run
│   ├── dashboard/                    # Flask dashboard
│   │   ├── server.py
│   │   └── templates/
│   ├── knowledge_base/               # Stage 6 RAG
│   │   ├── source_registry.json
│   │   ├── sources/                  # PDFs (gitignored)
│   │   ├── faiss_index/              # Generated (gitignored)
│   │   └── hf_cache/                 # Embedding model cache (gitignored)
│   ├── data/
│   │   ├── test/                     # Synthetic CSVs
│   │   └── factlists/                # One JSON per successful run
│   └── output/                       # Generated (gitignored)
│       ├── reports/                  # PDFs + HTML per run
│       ├── site/index.html           # Latest dashboard snapshot
│       ├── report_data/              # Structured data Flask reads
│       ├── run_logs/
│       └── meta_governance/
│
├── docs/                             # CASDAM case-study site (GitHub Pages)
│   ├── index.html
│   ├── dashboard/index.html          # Embedded latest dashboard
│   ├── assets/
│   └── sitemap.xml
│
├── planning/docs/                    # Design docs (gitignored)
│   └── DAM_Developer_Guide_v2.md
│
├── screenshot.mjs                    # Puppeteer — case-study screenshots
├── Procfile                          # Render deploy: runs the Flask dashboard
├── requirements.txt                  # Dashboard deps only
└── package.json                      # Puppeteer only
```

---

## Quick start

From the `app/` directory.

### Pipeline

```bash
# One-time: Python deps for the full pipeline
pip install pydantic openai flask python-dotenv playwright pymupdf \
            faiss-cpu sentence-transformers
playwright install chromium

# OpenRouter key (single account routes to all six model families)
echo "OPENROUTER_API_KEY=your_key" > app/.env

# One-time: build the Stage 6 FAISS index (needs PDFs in app/knowledge_base/sources/)
cd app && python -m core.knowledge_base

# One-time: generate synthetic test CSVs
python scripts/generate_test_data.py
python scripts/generate_adversarial_data.py

# Run the pipeline on synthetic data
python pipeline.py --test

# Run on real CSVs
python pipeline.py \
  --shopify data/shopify_orders.csv \
  --tpl     data/tpl_shipments.csv \
  --fedex   data/fedex_tracking.csv \
  --dhl     data/dhl_tracking.csv \
  --week    2026-04-11

# Other CLI modes
python pipeline.py --meta          # Layer 5 health summary, no run
python pipeline.py --adversarial   # Planted-error gate tests
```

A successful run writes:

- `output/reports/DAM_<run_id>.pdf` — editorial PDF
- `output/reports/<run_id>.html` + `output/site/index.html` — dashboard HTML
- `output/report_data/<run_id>.json` — structured data for the Flask dashboard
- `output/run_logs/<run_id>.json` — full telemetry
- `data/factlists/<week>.json` — immutable FactList (cost baseline history)

Typical cost: ~$0.04 per run. Typical latency: ~150 seconds (multi-provider routing through OpenRouter — slower than single-provider but well within budget for a weekly pipeline).

### Flask dashboard

```bash
cd app && python dashboard/server.py
# http://127.0.0.1:5000
```

The dashboard reads `output/report_data/*.json` and renders KPI trends, sparklines, domain status, and Stage 6 commentary for the most recent run (or any historical run via `/report/<run_id>`).

### 9-week history (for sparkline data)

```bash
cd app && python scripts/build_history.py
```

Generates 9 weeks of synthetic data with a progress curve from "year-ago" to "current," runs the full pipeline for each, and populates `data/factlists/` so the dashboard shows real trend lines. ~10 minutes total.

---

## What the pipeline produces

### Editorial PDF (`output/reports/DAM_<run_id>.pdf`)

Executive summary on a white page. Playfair Display masthead, Inter body, JetBrains Mono for KPI values. Gold accent (#c9a84c), status colours (green #059669 / amber #d97706 / red #dc2626). Each KPI domain is a bordered card with its own Data Analysis panel and, when Stage 6 succeeded, an Expert Commentary panel on warm-cream background.

Rendered by `app/core/report_renderer.py::render_pdf_html()`. Converted to PDF by Stage 5 via a three-tier fallback: WeasyPrint → Playwright/Chromium → HTML-only (print-to-PDF in a browser). Playwright is the working path on Windows.

### Dark-navy HTML dashboard (`output/site/index.html`)

Same typefaces and gold accent on a navy background (#070d1a). Sticky navbar, hero status banner, full historical comparison tables (last week / month / 3-month / year), collapsible data provenance per domain. Self-contained — the data is baked in at render time, so it works as a static file.

### Flask dashboard (`app/dashboard/server.py`)

Live view of the most recent run with KPI sparklines driven by `factlist_store.get_kpi_trend()`. Deployed to Render via `Procfile`. Serves at port 5000 locally, `$PORT` in production.

### CASDAM case-study site (`docs/`)

Plain HTML/CSS/JS marketing site for GitHub Pages, hosted at `danielwipert.github.io/cas.dam`. `docs/dashboard/` embeds the latest dashboard snapshot; `screenshot.mjs` uses Puppeteer to capture report sections into `docs/assets/screenshots/`.

---

## Pipeline at a glance

```
CSV files
    ↓
Preflight                  Ping every configured model on OpenRouter (~2s).
                           Halt if any is unreachable — model selection is part
                           of the pipeline's value proposition, not optional.
    ↓
Stage 1 (Mistral Small 3.2 24B)   Field mapping → canonical orders / shipments / carrier records
    ↓
Stage 2 (Gemini 2.5 Flash)        Exact + fuzzy join → reconciliation table
    ↓
Stage 3 (Claude Haiku 4.5)        LLM + Python KPI compute → FactList (Python wins)
    ↓
Stage 4 (DeepSeek V3  →    Generate insights → Qwen2.5 7B verifies every FACT_ID
         Qwen2.5 7B)       Claims that fail citation check are stripped
    ↓
Stage 6 (Llama 3.3 70B +   RAG over 11 supply-chain textbooks (FAISS + MiniLM-L6-v2)
         FAISS RAG)        Per-domain executive commentary + recommendations
                           Deterministic citation check (Python, not LLM)
    ↓
Stage 5 (no LLM)           Render PDF + dashboard HTML + report_data JSON
```

All six model families are routed through a single OpenAI-compatible client pointed at OpenRouter (`app/core/llm_client.py`). Fallback for any stage is Llama 3.3 70B.

Stages 1–2 halt the pipeline on failure. Stages 3, 4, 5, 6 degrade gracefully — the report still ships with a disclosure in the verification footer. Preflight halts unconditionally.

---

## Deployment (Render)

The `Procfile` runs the Flask dashboard: `web: python app/dashboard/server.py`. `requirements.txt` is intentionally minimal (`flask`, `pydantic`, `requests`, `python-dotenv`, `openai`) because the deployed service serves already-generated `report_data/*.json` — it does not run the pipeline. (`openai` is included because `llm_client.py` is imported transitively, but no API calls are made on the deployed service.)

To update the deployed dashboard, run the pipeline locally, commit the resulting `output/report_data/*.json` files, and push. (The `output/` directory is otherwise gitignored; include report_data explicitly when updating production data.)

---

## Further reading

- **[planning/docs/DAM_Developer_Guide_v2.md](planning/docs/DAM_Developer_Guide_v2.md)** — the full implementation reference: schemas, prompts, gates, degradation logic, KPI formulas, thresholds, Stage 6 RAG design, report template variables, adversarial test categories.
- **[CASDAM case study](https://danielwipert.github.io/cas.dam)** — the narrative-style site in `docs/`.
- **Contact:** [LinkedIn — Daniel Wipert](https://www.linkedin.com/in/daniel-wipert/) · [GitHub](https://github.com/danielwipert)
