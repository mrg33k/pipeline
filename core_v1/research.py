import json
import logging
import re
from typing import Dict, List

import requests
from bs4 import BeautifulSoup
from openai import OpenAI

import config
from models import Profile, ResearchCard

logger = logging.getLogger(__name__)
_client = OpenAI(api_key=config.OPENAI_API_KEY)


def _clean(text: str) -> str:
    text = re.sub(r"\s+", " ", (text or "")).strip()
    return text


def _best_opener_anchor(profile: Profile, proof_phrase: str, meaning_line: str) -> str:
    candidates = [
        _clean(proof_phrase),
        _clean(meaning_line),
        _clean(profile.review_signals[0] if profile.review_signals else ""),
        _clean(profile.company_industry),
    ]
    for item in candidates:
        if not item:
            continue
        # Keep opener anchor short and concrete.
        words = item.split()
        short = " ".join(words[:10]).strip()
        if short:
            return short
    return "what you are building"


def _short_phrase(text: str, max_words: int = 10) -> str:
    clean = _clean(text)
    if not clean:
        return ""
    words = clean.split()
    return " ".join(words[:max_words]).strip()


def _pick_opener_source_hint(source_truth: List[str]) -> str:
    if "linkedin" in source_truth:
        return "linkedin"
    if "reviews" in source_truth:
        return "reviews"
    if "website" in source_truth:
        return "website"
    return ""


def _parse_confidence(value) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        num = float(value)
        if num > 1.0 and num <= 100.0:
            return max(0.0, min(1.0, num / 100.0))
        return max(0.0, min(1.0, num))

    text = _clean(str(value)).lower()
    if not text:
        return 0.0

    bucket_map = {
        "high": 0.85,
        "medium": 0.65,
        "med": 0.65,
        "low": 0.35,
        "strong": 0.85,
        "weak": 0.35,
    }
    if text in bucket_map:
        return bucket_map[text]

    if text.endswith("%"):
        try:
            pct = float(text[:-1].strip())
            return max(0.0, min(1.0, pct / 100.0))
        except Exception:
            return 0.0

    try:
        num = float(text)
        if num > 1.0 and num <= 100.0:
            return max(0.0, min(1.0, num / 100.0))
        return max(0.0, min(1.0, num))
    except Exception:
        return 0.0


def _bucket(industry: str, desc: str) -> str:
    text = f"{industry} {desc}".lower()
    if any(t in text for t in ["restaurant", "hospitality", "dining", "food"]):
        return "restaurant"
    if any(t in text for t in ["software", "saas", "tech", "platform"]):
        return "saas"
    if any(t in text for t in ["real estate", "broker", "property"]):
        return "real_estate"
    if any(t in text for t in ["construction", "contractor", "builder"]):
        return "construction"
    if any(t in text for t in ["nonprofit", "charity", "foundation"]):
        return "nonprofit"
    if any(t in text for t in ["event", "wedding", "venue"]):
        return "events"
    return "general"


def _snippet_from_url(url: str) -> str:
    if not url:
        return ""
    try:
        if not url.startswith("http"):
            url = f"https://{url}"
        resp = requests.get(url, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        for bad in soup(["script", "style", "noscript"]):
            bad.extract()
        text = _clean(soup.get_text(" "))
        return text[:1200]
    except Exception:
        return ""


def gather_profile_context(profile: Profile) -> Profile:
    # website snippet
    site = _snippet_from_url(profile.company_domain)
    if site:
        profile.homepage_snippet = site

    # linkedin snippet (best effort)
    if profile.linkedin_url:
        ln = _snippet_from_url(profile.linkedin_url)
        if ln:
            profile.linkedin_snippet = ln

    # review signals (lightweight heuristic from existing text)
    base = " ".join([profile.company_description, profile.homepage_snippet])
    signals = []
    patterns = [
        r"\baward[- ]winning\b",
        r"\btop rated\b",
        r"\bfamily owned\b",
        r"\bcommunity\b",
        r"\blive music\b",
        r"\bfarm[- ]to[- ]table\b",
    ]
    for p in patterns:
        m = re.search(p, base, flags=re.IGNORECASE)
        if m:
            signals.append(m.group(0))
    profile.review_signals = signals[:3]
    return profile


def build_research_card(profile: Profile) -> ResearchCard:
    source_truth: List[str] = []
    if profile.homepage_snippet or profile.company_description:
        source_truth.append("website")
    if profile.linkedin_snippet:
        source_truth.append("linkedin")
    if profile.review_signals:
        source_truth.append("reviews")

    industry_bucket = _bucket(profile.company_industry, profile.company_description + " " + profile.homepage_snippet)

    prompt = {
        "company": profile.company_name,
        "industry": profile.company_industry,
        "description": profile.company_description[:800],
        "homepage_snippet": profile.homepage_snippet[:1000],
        "linkedin_snippet": profile.linkedin_snippet[:600],
        "review_signals": profile.review_signals[:3],
        "industry_bucket": industry_bucket,
    }

    try:
        resp = _client.chat.completions.create(
            model=config.RESEARCH_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You extract concise outreach research. Return JSON only with keys: "
                        "impact_core, impact_subject, proof_phrase, meaning_line, confidence. "
                        "meaning_line must be conversational and factual."
                    ),
                },
                {"role": "user", "content": json.dumps(prompt, ensure_ascii=True)},
            ],
            max_completion_tokens=280,
            response_format={"type": "json_object"},
        )
        raw = (resp.choices[0].message.content or "").strip()
        data = json.loads(raw)
    except Exception as exc:
        logger.info("research_llm_fallback:%s:%s", profile.email, exc)
        data = {}

    impact_core = _clean(str(data.get("impact_core") or ""))
    impact_subject = _clean(str(data.get("impact_subject") or ""))
    proof_phrase = _short_phrase(str(data.get("proof_phrase") or ""), max_words=10)
    meaning_line = _clean(str(data.get("meaning_line") or ""))
    confidence = _parse_confidence(data.get("confidence"))

    if not impact_core:
        impact_core = "you help customers get consistent outcomes"
    if not impact_subject:
        impact_subject = "local customers"
    if not proof_phrase:
        fallback = profile.review_signals[0] if profile.review_signals else profile.company_industry or "your work"
        proof_phrase = _short_phrase(str(fallback), max_words=10)
    if not meaning_line:
        meaning_line = _clean(f"You help {impact_subject} through {impact_core}.")
    if len(meaning_line.split()) > 20:
        meaning_line = " ".join(meaning_line.split()[:20]).rstrip(".,") + "."

    confidence = max(confidence, 0.55 if source_truth else 0.2)
    quality = "strong" if (confidence >= config.EVIDENCE_MIN_CONFIDENCE and proof_phrase and meaning_line and source_truth) else "weak"
    opener_fact = _best_opener_anchor(profile, proof_phrase, meaning_line)
    opener_source_hint = _pick_opener_source_hint(source_truth)

    return ResearchCard(
        industry_bucket=industry_bucket,
        source_truth=source_truth,
        impact_core=impact_core,
        impact_subject=impact_subject,
        proof_phrase=proof_phrase,
        meaning_line=meaning_line,
        confidence=confidence,
        quality=quality,
        opener_fact=opener_fact,
        opener_source_hint=opener_source_hint,
    )
