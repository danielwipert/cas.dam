"""
knowledge_base.py
Chorus AI Systems — Data Analytics Manager (DAM)

Run-once builder for the Stage 6 RAG knowledge base.
Reads source PDFs via source_registry.json, chunks text, embeds with
all-MiniLM-L6-v2, and writes a FAISS index + companion chunk_store.json.

Re-run only when adding new sources.

Usage (from app/ directory):
    python -m core.knowledge_base
    python -m core.knowledge_base --smoke-test
"""

from __future__ import annotations

import argparse
import json
import os
import time
import uuid
from pathlib import Path
from typing import Any

import fitz                          # pymupdf
import faiss
import numpy as np

# Redirect HuggingFace cache into project to avoid Windows .cache file conflict
os.environ.setdefault(
    "HF_HOME",
    str(Path(__file__).resolve().parent.parent / "knowledge_base" / "hf_cache"),
)

from sentence_transformers import SentenceTransformer

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BASE_DIR      = Path(__file__).resolve().parent.parent   # app/
KB_DIR        = BASE_DIR / "knowledge_base"
SOURCES_DIR   = KB_DIR / "sources"
INDEX_DIR     = KB_DIR / "faiss_index"
REGISTRY_PATH = KB_DIR / "source_registry.json"
CHUNK_STORE   = KB_DIR / "chunk_store.json"
INDEX_PATH    = INDEX_DIR / "index.faiss"
ID_MAP_PATH   = INDEX_DIR / "id_map.json"   # maps FAISS int id → chunk_id

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

CHUNK_TARGET_WORDS = 450
CHUNK_OVERLAP_WORDS = 50
EMBEDDING_MODEL = "all-MiniLM-L6-v2"


# ---------------------------------------------------------------------------
# PDF extraction
# ---------------------------------------------------------------------------

def extract_text_from_pdf(pdf_path: Path) -> str:
    """Extract all plain text from a PDF. Returns empty string on failure."""
    try:
        doc = fitz.open(str(pdf_path))
        pages = []
        for page in doc:
            text = page.get_text()
            if text.strip():
                pages.append(text)
        doc.close()
        return "\n".join(pages)
    except Exception as e:
        print(f"    [WARN] Could not extract text from {pdf_path.name}: {e}")
        return ""


# ---------------------------------------------------------------------------
# Chunker
# ---------------------------------------------------------------------------

def chunk_text(
    text: str,
    target_words: int = CHUNK_TARGET_WORDS,
    overlap_words: int = CHUNK_OVERLAP_WORDS,
) -> list[str]:
    """
    Split text into overlapping word-based chunks.
    Returns list of chunk strings.
    """
    words = text.split()
    if not words:
        return []

    chunks = []
    start = 0
    step = target_words - overlap_words

    while start < len(words):
        end = min(start + target_words, len(words))
        chunk = " ".join(words[start:end])
        if len(chunk.strip()) > 50:   # skip trivially short chunks
            chunks.append(chunk)
        if end == len(words):
            break
        start += step

    return chunks


# ---------------------------------------------------------------------------
# Builder
# ---------------------------------------------------------------------------

def build_knowledge_base(verbose: bool = True) -> dict[str, Any]:
    """
    Full build pipeline:
      1. Load source registry
      2. Extract + chunk each PDF
      3. Embed all chunks
      4. Write FAISS index + chunk_store.json

    Returns a summary dict.
    """
    t0 = time.time()
    INDEX_DIR.mkdir(parents=True, exist_ok=True)

    # ── Load registry ────────────────────────────────────────────────────────
    with open(REGISTRY_PATH) as f:
        registry = json.load(f)

    if verbose:
        print(f"\n{'='*60}")
        print(f"  Stage 6 Knowledge Base Builder")
        print(f"  Sources: {len(registry)}  |  Model: {EMBEDDING_MODEL}")
        print(f"{'='*60}\n")

    # ── Extract and chunk ────────────────────────────────────────────────────
    all_chunks: list[dict[str, Any]] = []
    sources_processed = 0
    sources_skipped = 0

    for entry in registry:
        filename     = entry["filename"]
        source_title = entry["source_title"]
        author       = entry["author"]
        domain_tags  = entry["domain_tags"]
        content_type = entry["content_type"]
        pdf_path     = SOURCES_DIR / filename

        if not pdf_path.exists():
            if verbose:
                print(f"  [SKIP] File not found: {filename}")
            sources_skipped += 1
            continue

        if verbose:
            print(f"  Extracting: {filename[:55]}")

        text = extract_text_from_pdf(pdf_path)
        if not text.strip():
            if verbose:
                print(f"    [SKIP] No extractable text — scanned PDF?")
            sources_skipped += 1
            continue

        chunks = chunk_text(text)
        if verbose:
            word_count = len(text.split())
            print(f"    {word_count:,} words -> {len(chunks)} chunks")

        for i, chunk_text_str in enumerate(chunks):
            chunk_id = f"{filename[:20].replace(' ', '_').replace('.', '')[:15]}_{i:04d}_{uuid.uuid4().hex[:6]}"
            all_chunks.append({
                "chunk_id":     chunk_id,
                "source_title": source_title,
                "author":       author,
                "domain_tags":  domain_tags,
                "content_type": content_type,
                "filename":     filename,
                "chunk_index":  i,
                "text":         chunk_text_str,
            })

        sources_processed += 1

    if not all_chunks:
        print("\n[ERROR] No chunks produced. Check source PDFs and registry.")
        return {"success": False}

    total_chunks = len(all_chunks)
    if verbose:
        print(f"\n  Total chunks: {total_chunks:,} across {sources_processed} sources")

    # ── Embed ────────────────────────────────────────────────────────────────
    if verbose:
        print(f"\n  Loading embedding model: {EMBEDDING_MODEL} ...")

    model = SentenceTransformer(EMBEDDING_MODEL)
    texts = [c["text"] for c in all_chunks]

    if verbose:
        print(f"  Embedding {total_chunks:,} chunks (this may take a few minutes)...")

    embeddings = model.encode(
        texts,
        batch_size=64,
        show_progress_bar=verbose,
        convert_to_numpy=True,
        normalize_embeddings=True,
    )

    dim = embeddings.shape[1]

    # ── Build FAISS index ────────────────────────────────────────────────────
    # IndexFlatIP with normalized embeddings == cosine similarity
    index = faiss.IndexFlatIP(dim)
    index.add(embeddings.astype(np.float32))

    faiss.write_index(index, str(INDEX_PATH))

    # id_map: FAISS integer position → chunk_id (needed for lookup after search)
    id_map = {i: c["chunk_id"] for i, c in enumerate(all_chunks)}
    with open(ID_MAP_PATH, "w") as f:
        json.dump(id_map, f)

    # ── Write chunk store ────────────────────────────────────────────────────
    chunk_store = {c["chunk_id"]: c for c in all_chunks}
    with open(CHUNK_STORE, "w", encoding="utf-8") as f:
        json.dump(chunk_store, f, ensure_ascii=False, indent=2)

    elapsed = round(time.time() - t0, 1)

    if verbose:
        index_size_mb = round(INDEX_PATH.stat().st_size / 1_048_576, 1)
        store_size_mb = round(CHUNK_STORE.stat().st_size / 1_048_576, 1)
        print(f"\n  FAISS index : {INDEX_PATH}  ({index_size_mb} MB)")
        print(f"  Chunk store : {CHUNK_STORE}  ({store_size_mb} MB)")
        print(f"  Build time  : {elapsed}s")
        print(f"\n  Sources processed : {sources_processed}")
        print(f"  Sources skipped   : {sources_skipped}")
        print(f"  Total chunks      : {total_chunks:,}")
        print(f"\n  Knowledge base ready.\n")

    return {
        "success":           True,
        "sources_processed": sources_processed,
        "sources_skipped":   sources_skipped,
        "total_chunks":      total_chunks,
        "embedding_dim":     dim,
        "elapsed_seconds":   elapsed,
    }


# ---------------------------------------------------------------------------
# Smoke test
# ---------------------------------------------------------------------------

def smoke_test() -> None:
    """Query the built index with 4 representative phrases and print top results."""
    if not INDEX_PATH.exists() or not CHUNK_STORE.exists():
        print("[ERROR] Index not found. Run build first.")
        return

    print(f"\n{'='*60}")
    print("  Smoke Test — Stage 6 RAG Knowledge Base")
    print(f"{'='*60}\n")

    model = SentenceTransformer(EMBEDDING_MODEL)
    index = faiss.read_index(str(INDEX_PATH))

    with open(CHUNK_STORE, encoding="utf-8") as f:
        chunk_store: dict = json.load(f)
    with open(ID_MAP_PATH) as f:
        id_map: dict = json.load(f)

    queries = [
        ("fulfillment",           "why does on-time delivery rate drop in peak season"),
        ("cost",                  "shipping cost increase dimensional weight pricing"),
        ("operational_integrity", "tracking number mismatch 3PL carrier data quality"),
        ("fulfillment",           "order to ship time bottleneck warehouse operations"),
    ]

    for domain_hint, query in queries:
        print(f"  Query [{domain_hint}]: \"{query}\"")
        vec = model.encode([query], normalize_embeddings=True).astype(np.float32)
        scores, indices = index.search(vec, 5)

        for rank, (score, idx) in enumerate(zip(scores[0], indices[0]), 1):
            chunk_id = id_map.get(str(idx))
            if not chunk_id:
                continue
            chunk = chunk_store.get(chunk_id, {})
            title  = chunk.get("source_title", "?")[:40]
            tags   = chunk.get("domain_tags", [])
            snippet = chunk.get("text", "")[:120].replace("\n", " ")
            snippet = snippet.encode("ascii", errors="replace").decode("ascii")
            title_safe = title.encode("ascii", errors="replace").decode("ascii")
            print(f"    [{rank}] score={score:.3f}  [{','.join(tags)}]  {title_safe}")
            print(f"        {snippet}...")
        print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stage 6 Knowledge Base Builder")
    parser.add_argument("--smoke-test", action="store_true",
                        help="Run smoke test against existing index (no rebuild)")
    parser.add_argument("--rebuild", action="store_true",
                        help="Force rebuild even if index already exists")
    args = parser.parse_args()

    if args.smoke_test:
        smoke_test()
    else:
        if INDEX_PATH.exists() and not args.rebuild:
            print(f"Index already exists at {INDEX_PATH}")
            print("Use --rebuild to force a fresh build, or --smoke-test to query it.")
        else:
            result = build_knowledge_base()
            if result.get("success"):
                smoke_test()
