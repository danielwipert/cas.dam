"""
llm_client.py
Chorus AI Systems - Data Analytics Manager (DAM)

Shared OpenRouter client, model constants, and JSON parsing utility.
All stages import from here - no API setup scattered across files.

Architecture: single OpenAI-compatible client pointed at OpenRouter, which
proxies to five distinct model families (Mistral / Google / Anthropic /
DeepSeek / Alibaba). See planning/docs/DAM_Multi_Model_Strategy_v1.md.
"""

import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from openai import OpenAI

# ---------------------------------------------------------------------------
# MODEL CONSTANTS  (five distinct families - all routed via OpenRouter)
# ---------------------------------------------------------------------------
#   Stage 1 mapping     : Mistral Small 3.2 24B   (Mistral)
#   Stage 2 reconcile   : Gemini 2.5 Flash        (Google)
#   Stage 3 KPI check   : Claude Haiku 4.5        (Anthropic)
#   Stage 4 generation  : DeepSeek V3             (DeepSeek)
#   Stage 4 verification: Qwen2.5-7B Turbo        (Alibaba/Qwen)
#   Fallback            : Llama 3.3 70B Instruct  (Meta)

MODEL_STAGE1       = "mistralai/mistral-small-3.2-24b-instruct"
MODEL_STAGE2       = "google/gemini-2.5-flash"
MODEL_STAGE3       = "anthropic/claude-haiku-4.5"
MODEL_STAGE4_GEN   = "deepseek/deepseek-chat-v3"
MODEL_STAGE4_VER   = "qwen/qwen-2.5-7b-instruct"
MODEL_STAGE6       = "meta-llama/llama-3.3-70b-instruct"
MODEL_FALLBACK     = "meta-llama/llama-3.3-70b-instruct"

# Per-model pricing in USD per 1M tokens (OpenRouter pass-through rates,
# verify against https://openrouter.ai/models before assuming exact figures).
MODEL_PRICING: dict[str, dict[str, float]] = {
    MODEL_STAGE1:     {"input": 0.20, "output": 0.60},
    MODEL_STAGE2:     {"input": 0.30, "output": 2.50},
    MODEL_STAGE3:     {"input": 1.00, "output": 5.00},
    MODEL_STAGE4_GEN: {"input": 0.14, "output": 0.28},
    MODEL_STAGE4_VER: {"input": 0.30, "output": 0.30},
    MODEL_STAGE6:     {"input": 0.88, "output": 0.88},
    MODEL_FALLBACK:   {"input": 0.88, "output": 0.88},
}

# All models intentionally configured on the pipeline. Used by preflight().
# Deduplicated since MODEL_STAGE6 and MODEL_FALLBACK currently point at the
# same Llama 3.3 70B route - one ping covers both.
ALL_PIPELINE_MODELS: list[str] = list(dict.fromkeys([
    MODEL_STAGE1,
    MODEL_STAGE2,
    MODEL_STAGE3,
    MODEL_STAGE4_GEN,
    MODEL_STAGE4_VER,
    MODEL_STAGE6,
    MODEL_FALLBACK,
]))

# OpenRouter API endpoint
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"

# Optional headers for OpenRouter analytics (visible on their dashboard).
OPENROUTER_REFERER = os.environ.get("OPENROUTER_REFERER", "https://cas.dam")
OPENROUTER_TITLE   = os.environ.get("OPENROUTER_TITLE",   "Chorus AI - DAM")

# Max tokens per call - enough for all stage outputs
MAX_TOKENS = 4096

# Default API call timeout in seconds
API_TIMEOUT = 120

# ---------------------------------------------------------------------------
# CLIENT FACTORY
# ---------------------------------------------------------------------------

def get_client(timeout: int = API_TIMEOUT) -> OpenAI:
    """
    Return an OpenAI-compatible client configured for OpenRouter.
    Reads OPENROUTER_API_KEY from environment.
    Raises clearly if the key is missing.
    """
    api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "OPENROUTER_API_KEY environment variable is not set. "
            "Export it before running the pipeline:\n"
            "  export OPENROUTER_API_KEY=your_key_here"
        )
    return OpenAI(
        api_key=api_key,
        base_url=OPENROUTER_BASE_URL,
        timeout=timeout,
        default_headers={
            "HTTP-Referer": OPENROUTER_REFERER,
            "X-Title":      OPENROUTER_TITLE,
        },
    )


# ---------------------------------------------------------------------------
# LLM CALL WRAPPER
# ---------------------------------------------------------------------------

def call_llm(
    system_prompt: str,
    user_prompt: str,
    model: str,
    client: Optional[OpenAI] = None,
    temperature: float = 0.1,
    max_tokens: int = MAX_TOKENS,
) -> tuple[str, float, float]:
    """
    Call the OpenRouter chat completions endpoint.

    Returns:
        (response_text, cost_usd, latency_seconds)
    """
    if client is None:
        client = get_client()

    t0 = time.time()
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_prompt},
        ],
        temperature=temperature,
        max_tokens=max_tokens,
    )
    latency = round(time.time() - t0, 2)

    text = response.choices[0].message.content or ""

    usage = getattr(response, "usage", None)
    cost = 0.0
    if usage:
        pricing      = MODEL_PRICING.get(model, {"input": 1.0, "output": 1.0})
        input_tokens  = getattr(usage, "prompt_tokens",     0) or 0
        output_tokens = getattr(usage, "completion_tokens", 0) or 0
        cost = (
            input_tokens  * pricing["input"]  / 1_000_000
            + output_tokens * pricing["output"] / 1_000_000
        )

    return text, cost, latency


# ---------------------------------------------------------------------------
# PREFLIGHT AVAILABILITY CHECK
# ---------------------------------------------------------------------------

def preflight_models(
    model_ids: list[str],
    timeout: int = 10,
) -> dict[str, Optional[str]]:
    """
    Fire a 1-token ping at each model in parallel to verify availability
    before the pipeline commits to a real run.

    Catches the common failure modes:
      - Model ID typo / renamed / removed from OpenRouter
      - No account access for the model
      - API key missing or expired
      - Auth or routing broken on OpenRouter side

    Returns:
        Dict mapping model_id -> None (ok) or error string (failed).
    """
    client = get_client(timeout=timeout)
    results: dict[str, Optional[str]] = {}

    def ping(model: str) -> tuple[str, Optional[str]]:
        try:
            client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": "ping"}],
                max_tokens=1,
                temperature=0,
            )
            return model, None
        except Exception as e:
            return model, f"{type(e).__name__}: {e}"

    with ThreadPoolExecutor(max_workers=max(1, len(model_ids))) as pool:
        futures = [pool.submit(ping, m) for m in model_ids]
        for fut in as_completed(futures):
            model, err = fut.result()
            results[model] = err

    return results


# ---------------------------------------------------------------------------
# JSON PARSING UTILITY
# ---------------------------------------------------------------------------

def parse_json_response(raw: str) -> dict:
    """
    Parse a JSON response from an LLM.

    Handles the two most common failure modes:
      1. Model wraps output in ```json ... ``` fences
      2. Model adds a preamble sentence before the JSON

    Raises ValueError with a clear message if parsing fails.
    """
    text = re.sub(r"```(?:json)?\s*", "", raw).strip()
    text = text.rstrip("`").strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    for start_char, end_char in [('{', '}'), ('[', ']')]:
        start = text.find(start_char)
        if start != -1:
            depth = 0
            end = -1
            for i, ch in enumerate(text[start:], start):
                if ch == start_char:
                    depth += 1
                elif ch == end_char:
                    depth -= 1
                    if depth == 0:
                        end = i + 1
                        break
            if end != -1:
                try:
                    return json.loads(text[start:end])
                except json.JSONDecodeError:
                    pass

    raise ValueError(
        f"Could not parse JSON from LLM response.\n"
        f"Raw response (first 500 chars):\n{raw[:500]}"
    )
