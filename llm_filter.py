"""
Uses gpt-4.1-mini to filter and rank the best prospects
from the free Apollo search results before spending enrichment credits.
"""

import json
import logging
from typing import Optional

from openai import OpenAI

import config

logger = logging.getLogger(__name__)

client = OpenAI()  # uses OPENAI_API_KEY env var; base_url pre-configured


def filter_and_rank(
    candidates: list[dict],
    already_contacted: set,
    max_picks: int = 25,
    model: Optional[str] = None,
    extra_directions: str = "",
) -> list[str]:
    """
    Takes raw Apollo search results (free, no emails), filters out
    already-contacted people, then asks the LLM to pick the best
    prospects for Ahead of Market's video production services.

    Returns a list of Apollo person IDs (up to max_picks).
    """

    # Step 1: Remove already contacted
    fresh = [p for p in candidates if p.get("id") not in already_contacted]
    logger.info(f"Candidates after removing duplicates: {len(fresh)} (removed {len(candidates) - len(fresh)})")

    if not fresh:
        logger.warning("No fresh candidates to filter.")
        return []

    # Step 2: Build a compact summary for the LLM (save tokens)
    summaries = []
    for p in fresh:
        org = p.get("organization", {})
        org_name = org.get("name", "Unknown") if isinstance(org, dict) else "Unknown"
        summary = {
            "id": p["id"],
            "name": f"{p.get('first_name', '')} {p.get('last_name_obfuscated', '')}".strip(),
            "title": p.get("title", ""),
            "company": org_name,
            "has_email": p.get("has_email", False),
        }
        summaries.append(summary)

    # Only send people who have emails available
    with_email = [s for s in summaries if s.get("has_email")]
    logger.info(f"Candidates with email available: {len(with_email)}")

    if not with_email:
        logger.warning("No candidates with available emails.")
        return []

    # Cap at 150 to keep token usage low
    to_evaluate = with_email[:150]

    # Step 3: Ask the LLM to rank
    prompt = _build_ranking_prompt(to_evaluate, max_picks, extra_directions=extra_directions)

    selected_model = (model or config.OPENAI_MODEL).strip()
    logger.info(f"Sending {len(to_evaluate)} candidates to LLM for ranking with model={selected_model}...")
    response = client.chat.completions.create(
        model=selected_model,
        messages=[
            {"role": "system", "content": "You are a sales prospecting assistant. Return only valid JSON."},
            {"role": "user", "content": prompt},
        ],
        max_completion_tokens=2000,
    )

    raw = response.choices[0].message.content.strip()
    # Strip markdown code fences if present
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
        if raw.endswith("```"):
            raw = raw[:-3]
        raw = raw.strip()

    try:
        result = json.loads(raw)
        picked_ids = result if isinstance(result, list) else result.get("ids", [])
    except json.JSONDecodeError:
        logger.error(f"LLM returned invalid JSON: {raw[:200]}")
        # Fallback: just take the first max_picks with emails
        picked_ids = [s["id"] for s in with_email[:max_picks]]

    logger.info(f"LLM selected {len(picked_ids)} prospects")
    return picked_ids[:max_picks]


def _build_ranking_prompt(candidates: list[dict], max_picks: int, extra_directions: str = "") -> str:
    candidates_json = json.dumps(candidates, indent=None)
    extra = (extra_directions or "").strip()
    extra_block = f"\n\nAdditional run-specific directions:\n{extra}" if extra else ""

    return f"""You are selecting cold outreach prospects for Ahead of Market (AOM), a video production studio in Phoenix, AZ.

AOM creates story-driven video content for web and social media. Their ideal clients are:
- Local Arizona businesses (Phoenix metro area)
- Industries: Hospitality, Restaurants, Health/Wellness/Fitness, Real Estate, Events, Software/SaaS, Construction, Nonprofit
- Company size: 11-200 employees
- Decision makers: CEOs, Founders, Owners, CMOs, Marketing Directors/Managers
- Businesses that would benefit from professional video content (brand videos, social content, event recaps, founder stories)

From the following list of {len(candidates)} prospects, select the BEST {max_picks} for cold outreach. Prioritize:
1. Title relevance (founders/owners/marketing leaders are best)
2. Company name suggests a good industry fit
3. Variety across different companies (do not pick multiple people from the same company){extra_block}

Return ONLY a JSON array of the selected person IDs, like: ["id1", "id2", ...]

Candidates:
{candidates_json}"""
