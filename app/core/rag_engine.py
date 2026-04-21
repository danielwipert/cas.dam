"""
rag_engine.py
Chorus AI Systems — Data Analytics Manager (DAM)

Stage 6 query and retrieval layer. No LLM calls.

Builds domain-specific queries from KPI state, retrieves relevant chunks
from the FAISS index filtered by domain_tag, deduplicates, and assembles
a context object per domain for Stage 6's LLM generation step.

Public API:
    build_domain_context(domain, factlist, stage4_output) -> DomainContext | None
"""

from __future__ import annotations

import json
import os
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import faiss
import numpy as np

# Redirect HF cache (same as knowledge_base.py)
os.environ.setdefault(
    "HF_HOME",
    str(Path(__file__).resolve().parent.parent / "knowledge_base" / "hf_cache"),
)
from sentence_transformers import SentenceTransformer

from .schemas import KPIDomain, ThresholdStatus, FactListEntry, Stage4Output

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BASE_DIR    = Path(__file__).resolve().parent.parent
KB_DIR      = BASE_DIR / "knowledge_base"
INDEX_PATH  = KB_DIR / "faiss_index" / "index.faiss"
ID_MAP_PATH = KB_DIR / "faiss_index" / "id_map.json"
CHUNK_STORE = KB_DIR / "chunk_store.json"

EMBEDDING_MODEL = "all-MiniLM-L6-v2"

# Minimum unique chunks required to produce commentary for a domain
MIN_CHUNKS_FOR_DOMAIN = 3

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class RetrievedChunk:
    chunk_id:     str
    source_title: str
    author:       str
    domain_tags:  list[str]
    content_type: str
    text:         str
    score:        float


@dataclass
class DomainContext:
    """Everything Stage 6 needs to generate commentary for one domain."""
    domain:           KPIDomain
    chunks:           list[RetrievedChunk]
    kpi_summaries:    list[dict]        # {kpi_name, value, status, wow_delta}
    stage4_insights:  list[str]         # verified insight texts for this domain
    stage4_actions:   list[str]         # recommended action texts for this domain
    sufficient:       bool              # False if < MIN_CHUNKS_FOR_DOMAIN


# ---------------------------------------------------------------------------
# Resource loader — lazy singleton
# ---------------------------------------------------------------------------

_index:       Optional[faiss.IndexFlat] = None
_chunk_store: Optional[dict]            = None
_id_map:      Optional[dict]            = None          # str(faiss_int) -> chunk_id
_reverse_map: Optional[dict]            = None          # chunk_id -> faiss_int
_model:       Optional[SentenceTransformer] = None
_domain_ids:  Optional[dict[str, list[int]]] = None     # domain_tag -> [faiss_ints]


def _is_index_available() -> bool:
    return INDEX_PATH.exists() and CHUNK_STORE.exists() and ID_MAP_PATH.exists()


def _load_resources() -> bool:
    """Load FAISS index, chunk store, id map, and embedding model once."""
    global _index, _chunk_store, _id_map, _reverse_map, _model, _domain_ids

    if _index is not None:
        return True

    if not _is_index_available():
        return False

    try:
        _index = faiss.read_index(str(INDEX_PATH))

        with open(CHUNK_STORE, encoding="utf-8") as f:
            _chunk_store = json.load(f)

        with open(ID_MAP_PATH) as f:
            _id_map = json.load(f)   # keys are strings

        _reverse_map = {v: int(k) for k, v in _id_map.items()}

        # Pre-build domain -> faiss integer ID list for fast filtered retrieval
        _domain_ids = defaultdict(list)
        for faiss_id_str, chunk_id in _id_map.items():
            chunk = _chunk_store.get(chunk_id)
            if chunk:
                for tag in chunk.get("domain_tags", []):
                    _domain_ids[tag].append(int(faiss_id_str))

        _model = SentenceTransformer(EMBEDDING_MODEL)
        return True

    except Exception as e:
        print(f"[rag_engine] Failed to load resources: {e}")
        return False


# ---------------------------------------------------------------------------
# Query construction
# ---------------------------------------------------------------------------

# Human-readable KPI names per domain for query building
_DOMAIN_KPI_LABELS = {
    KPIDomain.fulfillment:           "order fulfillment on-time shipment rate",
    KPIDomain.carrier_performance:   "carrier delivery performance on-time rate",
    KPIDomain.cost:                  "shipping cost per order cost variance",
    KPIDomain.operational_integrity: "shipment match rate label lag data quality",
}

_DOMAIN_CONTEXT = {
    KPIDomain.fulfillment:           "ecommerce warehouse fulfillment operations",
    KPIDomain.carrier_performance:   "3PL carrier delivery performance management",
    KPIDomain.cost:                  "ecommerce shipping cost management",
    KPIDomain.operational_integrity: "shipment tracking data quality operations",
}


def _build_queries(
    domain: KPIDomain,
    factlist: list[FactListEntry],
    stage4_output: Stage4Output,
) -> list[tuple[str, int]]:
    """
    Build 4 queries for a domain. Returns list of (query_text, top_k).
    top_k is 6 for Red/Yellow domains, 3 for Green.
    """
    domain_facts = [f for f in factlist if f.domain == domain]
    statuses = {f.threshold_status for f in domain_facts}
    has_alert = ThresholdStatus.red in statuses or ThresholdStatus.yellow in statuses

    top_k_primary  = 6 if has_alert else 3
    top_k_overview = 3

    kpi_label   = _DOMAIN_KPI_LABELS[domain]
    domain_ctx  = _DOMAIN_CONTEXT[domain]
    domain_name = domain.value.replace("_", " ")

    # Build a brief KPI status phrase for richer diagnostic queries
    alert_kpis = [
        f.kpi_name for f in domain_facts
        if f.threshold_status in (ThresholdStatus.red, ThresholdStatus.yellow)
    ]
    alert_phrase = (
        " ".join(alert_kpis[:2]) if alert_kpis
        else kpi_label
    )

    queries = [
        (
            f"why does {alert_phrase} degrade in {domain_ctx}",
            top_k_primary,
        ),
        (
            f"how to improve {alert_phrase} {domain_ctx}",
            top_k_primary,
        ),
        (
            f"industry benchmark {kpi_label} ecommerce standard best practice",
            top_k_overview,
        ),
        (
            f"{domain_name} performance management {domain_ctx}",
            top_k_overview,
        ),
    ]

    return queries


# ---------------------------------------------------------------------------
# Retrieval
# ---------------------------------------------------------------------------

def _retrieve_chunks(
    query: str,
    domain_tag: str,
    top_k: int,
) -> list[RetrievedChunk]:
    """
    Retrieve top_k chunks from the FAISS index, filtered to domain_tag
    before search using FAISS IDSelectorBatch.
    """
    if not _load_resources():
        return []

    valid_ids = _domain_ids.get(domain_tag, [])
    if not valid_ids:
        return []

    vec = _model.encode([query], normalize_embeddings=True).astype(np.float32)

    id_array = np.array(valid_ids, dtype=np.int64)
    selector = faiss.IDSelectorBatch(id_array)
    params   = faiss.SearchParameters()
    params.sel = selector

    actual_k = min(top_k, len(valid_ids))
    scores, indices = _index.search(vec, actual_k, params=params)

    results = []
    for score, idx in zip(scores[0], indices[0]):
        if idx < 0:
            continue
        chunk_id = _id_map.get(str(idx))
        if not chunk_id:
            continue
        chunk = _chunk_store.get(chunk_id)
        if not chunk:
            continue
        results.append(RetrievedChunk(
            chunk_id=chunk_id,
            source_title=chunk.get("source_title", ""),
            author=chunk.get("author", ""),
            domain_tags=chunk.get("domain_tags", []),
            content_type=chunk.get("content_type", ""),
            text=chunk.get("text", ""),
            score=float(score),
        ))

    return results


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def _deduplicate_chunks(chunks: list[RetrievedChunk]) -> list[RetrievedChunk]:
    """Remove duplicate chunk_ids, keeping highest-scoring occurrence."""
    seen:   dict[str, RetrievedChunk] = {}
    for c in chunks:
        if c.chunk_id not in seen or c.score > seen[c.chunk_id].score:
            seen[c.chunk_id] = c
    # Sort by score descending
    return sorted(seen.values(), key=lambda x: x.score, reverse=True)


# ---------------------------------------------------------------------------
# Domain context builder — public API
# ---------------------------------------------------------------------------

def build_domain_context(
    domain: KPIDomain,
    factlist: list[FactListEntry],
    stage4_output: Stage4Output,
) -> DomainContext:
    """
    Orchestrate query building, retrieval, and deduplication for one domain.
    Returns DomainContext. If sufficient=False, Stage 6 skips this domain.
    """
    if not _load_resources():
        # Index unavailable — return empty context; Stage 6 handles degradation
        return DomainContext(
            domain=domain,
            chunks=[],
            kpi_summaries=[],
            stage4_insights=[],
            stage4_actions=[],
            sufficient=False,
        )

    # ── Build queries ──────────────────────────────────────────────────────
    queries = _build_queries(domain, factlist, stage4_output)

    # ── Retrieve + pool ────────────────────────────────────────────────────
    all_chunks: list[RetrievedChunk] = []
    for query_text, top_k in queries:
        retrieved = _retrieve_chunks(query_text, domain.value, top_k)
        all_chunks.extend(retrieved)

    # ── Deduplicate ────────────────────────────────────────────────────────
    unique_chunks = _deduplicate_chunks(all_chunks)

    # Cap at 12 unique chunks per spec (8-12 range)
    unique_chunks = unique_chunks[:12]

    sufficient = len(unique_chunks) >= MIN_CHUNKS_FOR_DOMAIN

    # ── KPI summaries for this domain ──────────────────────────────────────
    domain_facts = [f for f in factlist if f.domain == domain]
    kpi_summaries = [
        {
            "fact_id":         f.fact_id,
            "kpi_name":        f.kpi_name,
            "value":           f.final_value,
            "threshold_status": f.threshold_status.value,
            "wow_delta":       f.wow_delta,
            "prior_week":      f.prior_week_value,
        }
        for f in domain_facts
    ]

    # ── Stage 4 insights and actions for this domain ───────────────────────
    domain_insights = [
        i for i in stage4_output.verified_insights
        if i.domain == domain
    ]
    from .schemas import ClaimType
    stage4_insights = [
        i.claim_text for i in domain_insights
        if i.claim_type != ClaimType.recommended_action
    ]
    stage4_actions = [
        i.recommended_action or i.claim_text
        for i in domain_insights
        if i.claim_type == ClaimType.recommended_action
    ]

    return DomainContext(
        domain=domain,
        chunks=unique_chunks,
        kpi_summaries=kpi_summaries,
        stage4_insights=stage4_insights,
        stage4_actions=stage4_actions,
        sufficient=sufficient,
    )


def is_index_available() -> bool:
    """Public check used by Stage 6 to decide whether to attempt retrieval."""
    return _is_index_available()
