"""
llm_client.py
Chorus AI Systems — Data Analytics Manager (DAM)

Shared Together AI client, model constants, and JSON parsing utility.
All stages import from here — no API setup scattered across files.
"""

import json
import os
import re
import time
from typing import Optional

from together import Together

# ---------------------------------------------------------------------------
# MODEL CONSTANTS  (three distinct families — Together AI serverless availability)
# ---------------------------------------------------------------------------
# Qwen2.5-72B-Instruct-Turbo and Mixtral-8x22B require dedicated endpoints on
# this account tier and are not available serverless.  Updated to available models:
#   - Stages 1-3: Meta Llama 3.3 70B  (Meta)
#   - Stage 4 gen: DeepSeek V3        (DeepSeek)
#   - Stage 4 ver: Qwen2.5-7B Turbo   (Alibaba/Qwen — cross-family independence)
#   - Fallback:    Llama 3.3 70B      (same family; last resort)

MODEL_STAGES_1_3   = "meta-llama/Llama-3.3-70B-Instruct-Turbo"
MODEL_STAGE4_GEN   = "deepseek-ai/DeepSeek-V3"
MODEL_STAGE4_VER   = "Qwen/Qwen2.5-7B-Instruct-Turbo"
MODEL_FALLBACK     = "meta-llama/Llama-3.3-70B-Instruct-Turbo"

# Per-model pricing in USD per 1M tokens (Together AI serverless rates)
MODEL_PRICING: dict[str, dict[str, float]] = {
    "meta-llama/Llama-3.3-70B-Instruct-Turbo": {"input": 0.88, "output": 0.88},
    "deepseek-ai/DeepSeek-V3":                 {"input": 1.25, "output": 1.25},
    "Qwen/Qwen2.5-7B-Instruct-Turbo":          {"input": 0.30, "output": 0.30},
}

# Max tokens per call — enough for all stage outputs
MAX_TOKENS = 4096

# Default API call timeout in seconds
API_TIMEOUT = 120

# ---------------------------------------------------------------------------
# CLIENT FACTORY
# ---------------------------------------------------------------------------

def get_client(timeout: int = API_TIMEOUT) -> Together:
    """
    Return a Together AI client.
    Reads TOGETHER_API_KEY from environment.
    Raises clearly if the key is missing.
    """
    api_key = os.environ.get("TOGETHER_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "TOGETHER_API_KEY environment variable is not set. "
            "Export it before running the pipeline:\n"
            "  export TOGETHER_API_KEY=your_key_here"
        )
    return Together(api_key=api_key, timeout=timeout)


# ---------------------------------------------------------------------------
# LLM CALL WRAPPER
# ---------------------------------------------------------------------------

def call_llm(
    system_prompt: str,
    user_prompt: str,
    model: str,
    client: Optional[Together] = None,
    temperature: float = 0.1,
    max_tokens: int = MAX_TOKENS,
) -> tuple[str, float, float]:
    """
    Call the Together AI chat completions endpoint.

    Returns:
        (response_text, cost_usd, latency_seconds)

    cost_usd is estimated from token counts — Together free-tier
    models are $0 but we track for when the paid tier is used.
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
    # Strip markdown fences if present
    text = re.sub(r"```(?:json)?\s*", "", raw).strip()
    text = text.rstrip("`").strip()

    # Try direct parse first
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Find the first { or [ and try from there
    for start_char, end_char in [('{', '}'), ('[', ']')]:
        start = text.find(start_char)
        if start != -1:
            # Find the matching closing bracket
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
