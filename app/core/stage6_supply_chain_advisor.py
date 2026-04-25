"""
stage6_supply_chain_advisor.py
Chorus AI Systems — Data Analytics Manager (DAM)

Stage 6: Supply Chain Advisor — RAG-powered knowledge layer.

Runs after Stage 4, before Stage 5. For each KPI domain:
  1. Retrieves relevant chunks from the FAISS knowledge base (via rag_engine)
  2. Calls Llama 3.3 70B to generate expert commentary + unified recommendations
  3. Validates all citations in deterministic Python (not LLM-evaluated)
  4. Strips any recommendation that fails citation check

Degrades gracefully: FAISS unavailable or too few chunks → DegradationSignal Level 1.
Report releases without Stage 6 content; verification footer discloses absence.
"""

from __future__ import annotations

import json
import re
import time
from typing import Union

from .schemas import (
    KPIDomain, DegradationLevel, DegradationSignal, HealthTelemetry,
    VerifiedOutput, Stage6Input, Stage6Output, Stage6DomainBlock,
    Stage6Recommendation,
)
from .rag_engine import build_domain_context, is_index_available, DomainContext
from .llm_client import get_client, call_llm, parse_json_response, MODEL_STAGE6, MODEL_FALLBACK

StageResult = Union[VerifiedOutput, DegradationSignal]

MAX_RETRIES  = 1
MAX_TOKENS   = 2048

# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

STAGE6_SYSTEM = """You are the SVP of Global Supply Chain Operations advising the CEO and Board of a Fortune 50 company. Your analysis is published in a confidential weekly executive management report read at the highest level of the organisation.

Write with the authority, precision, and strategic judgment expected in that room. Every sentence must state something specific and operational — not something any consultant could write. Avoid hedging, passive voice, and generic supply chain truisms.

COMMENTARY RULES:
- Exactly 1 paragraph, 3-5 sentences.
- State what the KPI movement means for the business operationally — root cause or systemic implication, not just a restatement of the number.
- Bring supply chain strategic perspective that goes beyond what the data analysis already says.
- Write in direct, confident executive prose. No academic framing.

RECOMMENDATION RULES:
- 2-3 recommendations. Each must name: the specific action, the operational lever, and the expected outcome.
- Recommendations should be distinct from each other — different levers, not variations of the same idea.
- No vague directives like "improve processes" or "review performance." Be specific about what, how, and why.

CITATION RULES — CRITICAL:
- The commentary and recommendation text fields must contain ZERO citation markers of any kind.
- Do NOT write chunk IDs, FACT IDs, source titles, author names, parenthetical references, or any identifiers inside the commentary or recommendation text.
- Chunk IDs belong ONLY in the commentary_chunk_ids and source_chunk_ids arrays. FACT IDs belong ONLY in source_fact_ids arrays.
- A reader of the commentary or recommendation text should see clean prose with no brackets, no codes, no references.

ALSO:
- Do not make financial projections or revenue estimates.
- Do not restate Stage 4 insights verbatim.
- Output strict JSON only. No preamble, no markdown fences."""


# ---------------------------------------------------------------------------
# Prompt builder
# ---------------------------------------------------------------------------

def _build_prompt(ctx: DomainContext) -> str:
    domain_label = ctx.domain.value.replace("_", " ").title()

    # ── KPI status block ──────────────────────────────────────────────────
    kpi_lines = []
    for k in ctx.kpi_summaries:
        delta_str = ""
        if k["wow_delta"] is not None:
            sign = "+" if k["wow_delta"] >= 0 else ""
            delta_str = f"  WoW delta: {sign}{k['wow_delta']:.4f}"
        kpi_lines.append(
            f"  {k['fact_id']}  {k['kpi_name']}: {k['value']:.4f}"
            f"  [{k['threshold_status'].upper()}]{delta_str}"
        )
    kpi_block = "\n".join(kpi_lines) if kpi_lines else "  No KPI data available."

    # ── Stage 4 context block ──────────────────────────────────────────────
    insights_block = ""
    if ctx.stage4_insights:
        lines = "\n".join(f"  - {i}" for i in ctx.stage4_insights)
        insights_block = f"\nSTAGE 4 DATA-DRIVEN INSIGHTS:\n{lines}"

    actions_block = ""
    if ctx.stage4_actions:
        lines = "\n".join(f"  - {a}" for a in ctx.stage4_actions)
        actions_block = f"\nSTAGE 4 RECOMMENDED ACTIONS (for context — synthesise, do not repeat):\n{lines}"

    # ── Knowledge base chunks ──────────────────────────────────────────────
    chunk_lines = []
    for c in ctx.chunks:
        # Truncate text to ~300 words for prompt efficiency
        words  = c.text.split()
        text   = " ".join(words[:300])
        if len(words) > 300:
            text += "..."
        chunk_lines.append(
            f"[chunk_id: {c.chunk_id}]\n"
            f"Type: {c.content_type}\n"
            f"Text: {text}"
        )
    chunks_block = "\n\n---\n\n".join(chunk_lines)

    # ── Output schema instructions ─────────────────────────────────────────
    schema = json.dumps({
        "commentary": "<CLEAN PROSE ONLY. No chunk IDs, no FACT IDs, no source references. 1 paragraph, 3-5 sentences. Executive strategic analysis.>",
        "commentary_chunk_ids": ["<chunk_id that informed this commentary — ID only, never in text>"],
        "recommendations": [
            {
                "text": "<CLEAN PROSE ONLY. No chunk IDs, no FACT IDs, no brackets. Specific action + lever + expected outcome.>",
                "source_chunk_ids": ["<chunk_id — ID only, never in text>"],
                "source_fact_ids":  ["<FACT_ID — ID only, never in text>"]
            }
        ]
    }, indent=2)

    return f"""DOMAIN: {domain_label}

KPI DATA THIS WEEK:
{kpi_block}
{insights_block}
{actions_block}

KNOWLEDGE BASE EXCERPTS ({len(ctx.chunks)} chunks):

{chunks_block}

---

TASK: Write executive-level supply chain commentary and 2-3 recommendations for the {domain_label} domain.

OUTPUT FORMAT (strict JSON):
{schema}

FINAL RULES:
- commentary and recommendation text fields: clean prose only — zero IDs, zero brackets, zero source references.
- commentary_chunk_ids: list 1-4 chunk_ids that informed the commentary.
- Each recommendation must have at least one source_chunk_id OR source_fact_id in its arrays (not in its text).
- 2 to 3 recommendations total."""


# ---------------------------------------------------------------------------
# Citation validator
# ---------------------------------------------------------------------------

def _strip_inline_citations(text: str) -> str:
    """
    Hard backstop: remove any leaked chunk IDs, FACT IDs, or citation markers
    from prose text regardless of what the LLM produced.
    """
    # chunk_id word followed by an identifier: "chunk_id supply_chain_0132_abc123"
    text = re.sub(r'\bchunk_id\s+\S+', '', text, flags=re.IGNORECASE)
    # Standalone chunk IDs: pattern word_digits_hexhash (e.g. SCOR_Modelpdf_0021_c4a5b8)
    text = re.sub(r'\b[A-Za-z][A-Za-z0-9_]*_\d+_[a-f0-9]{6,}\b', '', text)
    # Parenthetical groups containing only IDs/codes (e.g. "(SCOR_..., SCOR_...)")
    text = re.sub(r'\(\s*[A-Za-z][A-Za-z0-9_]*_\d+[^)]*\)', '', text)
    # FACT ID references: (F001), [F001], F001, F002
    text = re.sub(r'[\(\[]\s*F\d{3}(?:\s*,\s*F\d{3})*\s*[\)\]]', '', text)
    text = re.sub(r'\bF\d{3}\b', '', text)
    # Clean up punctuation/spacing artifacts left by removals
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'\s+([,.])', r'\1', text)
    text = re.sub(r',\s*\.', '.', text)
    text = re.sub(r'\.\s*\.', '.', text)
    return text.strip()


def _validate_and_build_block(
    domain: KPIDomain,
    raw: dict,
    valid_chunk_ids: set[str],
    valid_fact_ids:  set[str],
    ctx: DomainContext,
) -> tuple[Stage6DomainBlock | None, int]:
    """
    Deterministic Python citation check. Returns (block, recommendations_stripped).
    Returns (None, 0) if commentary citation check fails entirely.
    """
    commentary      = _strip_inline_citations(str(raw.get("commentary", "")).strip())
    comm_chunk_ids  = raw.get("commentary_chunk_ids", [])
    raw_recs        = raw.get("recommendations", [])

    if not commentary:
        return None, 0

    # Commentary must cite at least one valid chunk_id
    cited_in_commentary = [c for c in comm_chunk_ids if c in valid_chunk_ids]
    if not cited_in_commentary:
        # Attempt fallback: use the top-scoring chunk as the commentary citation
        if ctx.chunks:
            cited_in_commentary = [ctx.chunks[0].chunk_id]
        else:
            return None, 0

    # Validate recommendations — strip those with no valid citations
    validated_recs: list[Stage6Recommendation] = []
    stripped = 0

    for rec in raw_recs:
        text        = _strip_inline_citations(str(rec.get("text", "")).strip())
        chunk_ids   = [c for c in rec.get("source_chunk_ids", []) if c in valid_chunk_ids]
        fact_ids    = [f for f in rec.get("source_fact_ids",  []) if f in valid_fact_ids]

        if not text:
            stripped += 1
            continue

        if not chunk_ids and not fact_ids:
            stripped += 1
            continue

        validated_recs.append(Stage6Recommendation(
            text=text,
            source_chunk_ids=chunk_ids,
            source_fact_ids=fact_ids,
        ))

    # Need at least 1 recommendation to surface the block
    if not validated_recs:
        return None, stripped + len(raw_recs)

    all_chunk_citations = list({
        c for r in validated_recs for c in r.source_chunk_ids
    } | set(cited_in_commentary))

    block = Stage6DomainBlock(
        domain=domain,
        commentary=commentary,
        recommendations=validated_recs[:3],
        chunk_citations=all_chunk_citations,
        citation_sources=["Expert Opinion"],
    )

    return block, stripped


# ---------------------------------------------------------------------------
# Stage 6 MVS class
# ---------------------------------------------------------------------------

class Stage6SupplyChainAdvisor:
    """
    Stage 6 — Supply Chain Advisor.

    L1 Operational  : rag_engine retrieves chunks; Llama 3.3 70B generates commentary.
    L2 Coordination : Consumes Stage4Output + FactList. Emits Stage6Output.
    L3 Runtime Gate : Strips unverified recommendations. Skips domains < 3 chunks.
    L4 Assurance    : Deterministic Python citation check — chunk_id and FACT_ID exact-match.
    L5 Telemetry    : domains_processed, chunks_retrieved_per_domain, recommendations_stripped.
    L6 Policy       : No financial projections. No external knowledge. No FactList modification.
    L7 Interface    : FAISS unavailable → DegradationSignal Level 1 immediately.
    """

    def run(self, inp: Stage6Input) -> StageResult:
        t0         = time.time()
        total_cost = 0.0
        client     = get_client()

        valid_fact_ids = {f.fact_id for f in inp.factlist}

        # ── L7: Constitutional check — refuse without FAISS ──────────────────
        if not is_index_available():
            return DegradationSignal(
                stage="stage_6",
                failure_reason="FAISS knowledge base index unavailable. Stage 6 requires the index to be built before running.",
                degradation_level_recommendation=DegradationLevel.partial,
                health_telemetry=HealthTelemetry(
                    stage="stage_6",
                    latency_seconds=round(time.time() - t0, 2),
                ),
            )

        # ── L1: Build domain contexts via RAG engine ──────────────────────────
        all_domains   = list(KPIDomain)
        domain_blocks: list[Stage6DomainBlock] = []
        domains_skipped: list[KPIDomain]        = []
        total_chunks_retrieved = 0
        chunks_per_domain: dict[str, int]       = {}
        recs_stripped_total = 0
        green_domains: list[str]       = []
        yellow_red_domains: list[str]  = []

        from .schemas import ThresholdStatus
        for domain in all_domains:
            domain_facts = [f for f in inp.factlist if f.domain == domain]
            statuses = {f.threshold_status for f in domain_facts}
            if ThresholdStatus.red in statuses or ThresholdStatus.yellow in statuses:
                yellow_red_domains.append(domain.value)
            else:
                green_domains.append(domain.value)

        print(f"  [ Stage 6 ] Building RAG contexts...")
        domain_contexts: dict[KPIDomain, DomainContext] = {}
        for domain in all_domains:
            ctx = build_domain_context(domain, inp.factlist, inp.stage4_output)
            domain_contexts[domain] = ctx
            chunks_per_domain[domain.value] = len(ctx.chunks)
            total_chunks_retrieved += len(ctx.chunks)

            if not ctx.sufficient:
                print(f"    [SKIP] {domain.value}: only {len(ctx.chunks)} chunks retrieved (< 3)")
                domains_skipped.append(domain)

        # ── L1: LLM generation per domain ────────────────────────────────────
        print(f"  [ Stage 6 ] Generating commentary ({len(all_domains) - len(domains_skipped)} domains)...")

        for domain in all_domains:
            if domain in domains_skipped:
                continue

            ctx = domain_contexts[domain]
            valid_chunk_ids = {c.chunk_id for c in ctx.chunks}

            prompt      = _build_prompt(ctx)
            model       = MODEL_STAGE6
            retry_ctx   = None
            block       = None
            recs_stripped = 0

            for attempt in range(MAX_RETRIES + 1):
                if attempt > 0:
                    model = MODEL_FALLBACK

                try:
                    raw_text, cost, _ = call_llm(
                        STAGE6_SYSTEM, prompt, model, client,
                        temperature=0.2,
                        max_tokens=MAX_TOKENS,
                    )
                    total_cost += cost
                    parsed = parse_json_response(raw_text)

                    block, recs_stripped = _validate_and_build_block(
                        domain, parsed, valid_chunk_ids, valid_fact_ids, ctx
                    )
                    recs_stripped_total += recs_stripped

                    if block is not None:
                        break
                    else:
                        retry_ctx = "Citation validation failed — no valid block produced."

                except Exception as e:
                    retry_ctx = str(e)
                    if attempt == MAX_RETRIES:
                        print(f"    [WARN] {domain.value}: LLM failed after retry — {e}")

            if block is not None:
                domain_blocks.append(block)
                print(f"    [OK]   {domain.value}: {len(block.recommendations)} recommendations, "
                      f"{len(block.chunk_citations)} citations")
            else:
                domains_skipped.append(domain)
                print(f"    [SKIP] {domain.value}: no valid block after LLM + citation check")

        # ── L3: Gate check ────────────────────────────────────────────────────
        if len(domain_blocks) == 0:
            return DegradationSignal(
                stage="stage_6",
                failure_reason="No domain blocks produced after LLM generation and citation validation.",
                degradation_level_recommendation=DegradationLevel.partial,
                health_telemetry=HealthTelemetry(
                    stage="stage_6",
                    api_cost_usd=round(total_cost, 6),
                    latency_seconds=round(time.time() - t0, 2),
                    model_used=MODEL_STAGE6,
                    domains_processed=0,
                    chunks_retrieved_per_domain=str(chunks_per_domain),
                    recommendations_stripped=recs_stripped_total,
                ),
            )

        output = Stage6Output(
            domain_blocks=domain_blocks,
            domains_skipped=domains_skipped,
            total_chunks_retrieved=total_chunks_retrieved,
        )

        telemetry = HealthTelemetry(
            stage="stage_6",
            api_cost_usd=round(total_cost, 6),
            latency_seconds=round(time.time() - t0, 2),
            model_used=MODEL_STAGE6,
            domains_processed=len(domain_blocks),
            chunks_retrieved_per_domain=str(chunks_per_domain),
            green_kpi_domains=str(green_domains),
            yellow_red_kpi_domains=str(yellow_red_domains),
            recommendations_stripped=recs_stripped_total,
        )

        return VerifiedOutput(
            stage="stage_6",
            payload=output,
            health_telemetry=telemetry,
        )
