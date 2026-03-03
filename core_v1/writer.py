import json
import logging
import re
from dataclasses import asdict
from typing import Dict, List, Tuple

from openai import OpenAI

import config
from models import Profile, ResearchCard, WriteResult

logger = logging.getLogger(__name__)
_client = OpenAI(api_key=config.OPENAI_API_KEY)

FIXED_ASK_LINE = "Are you already working with someone on web/social stuff?"
FIXED_P3_LINE = (
    "I had a couple ideas for you guys, but I didn't want to assume anything, so I thought I'd introduce "
    "myself first. I'm in the area most of this week if you'd prefer to meet briefly, otherwise I'm happy "
    "to hop on Zoom as well."
)
SOFT_LENGTH_WARN_WORDS = 120

_GENERIC_PATTERNS = [
    r"quick idea based on your business",
    r"wanted to introduce myself",
    r"based on what I saw",
    r"your business caught my eye",
    r"i wanted to reach out",
]

_STOPWORDS = {
    "with", "that", "your", "team", "company", "local", "area", "people", "their", "this", "from", "into",
    "and", "the", "for", "you", "around", "over", "while", "have", "has",
}


def _company_initials(name: str) -> str:
    parts = [p for p in re.split(r"[^A-Za-z0-9]+", (name or "").strip()) if p]
    if not parts:
        return "CO"
    return "".join(p[0].upper() for p in parts[:5])


def _word_count(text: str) -> int:
    return len(re.findall(r"\b\w+\b", text or ""))


def _tokenize(text: str) -> List[str]:
    return [
        t.lower()
        for t in re.findall(r"[A-Za-z][A-Za-z0-9]{3,}", (text or ""))
        if t.lower() not in _STOPWORDS
    ]


def _clean_line(text: str) -> str:
    line = (text or "").replace("\r", " ").replace("\n", " ").strip()
    line = re.sub(r"(?i)^subject\s*:.*$", "", line).strip()
    line = re.sub(r"(?i)^hi\s+[^,]+,\s*", "", line).strip()
    line = re.sub(r"(?i)^best,\s*", "", line).strip()
    line = re.sub(r"\s+", " ", line).strip()
    if line and not re.search(r"[.!?]$", line):
        line += "."
    return line


def _trim_phrase(text: str, max_words: int = 12) -> str:
    cleaned = _clean_line(text).rstrip(".")
    words = cleaned.split()
    if not words:
        return ""
    return " ".join(words[:max_words]).strip(" ,")


def _normalize_fact_for_opener(text: str, max_words: int = 10) -> str:
    fact = _trim_phrase(text, max_words=max_words)
    if not fact:
        return ""

    fact = re.sub(r"^[,;:\-]+", "", fact).strip()
    fact = re.sub(
        r"(?i)^(with over|over|our|we provide|we help|we|provides|provide|offering|offers|located in)\s+",
        "",
        fact,
    ).strip()
    fact = re.sub(r"[,\-:;]+$", "", fact).strip()
    fact = re.sub(r"\s+", " ", fact).strip()
    return fact


def _deterministic_opener_from_card(profile: Profile, card: ResearchCard) -> str:
    company = (profile.company_name or "your company").strip()
    city = (profile.company_city or "the area").strip()
    fact = _normalize_fact_for_opener(card.opener_fact or card.proof_phrase or card.meaning_line, max_words=10)
    if not fact:
        fact = _normalize_fact_for_opener(card.impact_core or card.impact_subject or "what you are building", max_words=10)
    if not fact:
        fact = "what you're building"

    bucket = (card.industry_bucket or "").strip().lower()
    truth = set(card.source_truth or [])

    if bucket == "restaurant":
        if "reviews" in truth:
            return _clean_line(
                f"I was looking at places around {city} and noticed people mention {fact} when talking about {company}, which stood out to me."
            )
        return _clean_line(
            f"I was looking at places around {city} and noticed {company} is known for {fact}, which stood out to me."
        )
    if bucket == "saas":
        return _clean_line(
            f"I was looking into software teams around {city} and noticed {company} focuses on {fact}, which stood out to me."
        )
    if bucket == "real_estate":
        return _clean_line(
            f"I was looking into real estate groups around {city} and noticed {company} is known for {fact}, which stood out to me."
        )
    if bucket == "construction":
        return _clean_line(
            f"I was looking into builders around {city} and noticed {company} is known for {fact}, which stood out to me."
        )
    return _clean_line(
        f"I was looking into businesses around {city} and noticed {company} focuses on {fact}, which stood out to me."
    )


def _build_opener_prompt(profile: Profile, card: ResearchCard, tone_template: str, retry_reason: str = "", previous: str = "") -> str:
    source_truth = ", ".join(card.source_truth or []) or "none"
    source_instruction = (
        "Mention source naturally only if true: use LinkedIn only when source_truth includes linkedin, "
        "reviews only when it includes reviews, website only when it includes website."
    )
    retry_instruction = ""
    if retry_reason:
        retry_instruction = (
            f"Previous opener failed for reason: {retry_reason}.\n"
            f"Previous opener: {previous}\n"
            "Rewrite a better opener with one concrete detail and no generic language.\n"
        )

    opener_fact = _normalize_fact_for_opener(card.opener_fact or card.proof_phrase or card.meaning_line, max_words=10)
    if not opener_fact:
        opener_fact = _normalize_fact_for_opener(card.impact_core or card.impact_subject, max_words=10)

    return (
        f"Template intent:\n{tone_template}\n\n"
        "Write ONLY paragraph 1 for a cold outreach email.\n"
        "Rules:\n"
        "- One sentence only.\n"
        "- Plain language, calm and human.\n"
        "- Personalize with one concrete detail from research.\n"
        "- No greeting, no signoff, no question.\n"
        "- Do not use placeholders.\n"
        f"- {source_instruction}\n\n"
        f"Profile: first_name={profile.first_name}, company={profile.company_name}, title={profile.title}, city={profile.company_city}, state={profile.company_state}\n"
        f"industry_bucket={card.industry_bucket}\n"
        f"source_truth={source_truth}\n"
        f"impact_subject={card.impact_subject}\n"
        f"opener_fact={opener_fact}\n"
        f"meaning_line={card.meaning_line}\n"
        f"{retry_instruction}"
    )


def _generate_opener(
    profile: Profile,
    card: ResearchCard,
    tone_template: str,
    model: str,
    retry_reason: str = "",
    previous: str = "",
) -> Tuple[str, str, str]:
    prompt = _build_opener_prompt(profile, card, tone_template, retry_reason=retry_reason, previous=previous)
    response = _client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "You write short personalized opener lines."},
            {"role": "user", "content": prompt},
        ],
        max_completion_tokens=260,
    )
    raw = (response.choices[0].message.content or "").strip()
    finish = str(response.choices[0].finish_reason or "")
    return _clean_line(raw), raw, finish


def _is_generic_opener(opener: str) -> bool:
    text = (opener or "").strip().lower()
    if not text:
        return True
    if _word_count(text) < 8:
        return True
    for pattern in _GENERIC_PATTERNS:
        if re.search(pattern, text):
            return True
    return False


def _has_evidence_link(opener: str, profile: Profile, card: ResearchCard) -> bool:
    opener_tokens = set(_tokenize(opener))
    if not opener_tokens:
        return False

    anchors = [card.proof_phrase, card.impact_core, card.impact_subject, card.opener_fact, profile.company_name]
    anchor_tokens = set()
    for anchor in anchors:
        anchor_tokens.update(_tokenize(anchor))

    if not anchor_tokens:
        return False
    return len(opener_tokens.intersection(anchor_tokens)) >= 1


def _source_truth_ok(opener: str, card: ResearchCard) -> bool:
    low = (opener or "").lower()
    if "linkedin" in low and "linkedin" not in card.source_truth:
        return False
    if "review" in low and "reviews" not in card.source_truth:
        return False
    if "website" in low and "website" not in card.source_truth:
        return False
    return True


def _render_fixed_email(first_name: str, opener: str) -> str:
    first = (first_name or "there").strip() or "there"
    p1 = _clean_line(opener)
    if not p1:
        p1 = "I came across your business and wanted to reach out."
    return f"Hi {first},\n\n{p1}\n\n{FIXED_ASK_LINE}\n\n{FIXED_P3_LINE}\n\nBest,"


def _soft_length_warning(body: str) -> None:
    count = _word_count(body)
    if count > SOFT_LENGTH_WARN_WORDS:
        logger.warning("length_warning:%d_words", count)


def _hard_checks(body: str, opener: str, profile: Profile, card: ResearchCard) -> List[str]:
    issues: List[str] = []

    expected_name = (profile.first_name or "there").strip() or "there"
    if not re.match(rf"(?is)^\s*Hi\s+{re.escape(expected_name)},", body):
        issues.append("bad_greeting")

    if len(re.findall(r"(?im)^Best,\s*$", body)) != 1:
        issues.append("bad_signoff")

    if re.search(r"(?im)^subject\s*:", body):
        issues.append("subject_leak")

    if FIXED_ASK_LINE not in body:
        issues.append("missing_fixed_ask_line")

    if FIXED_P3_LINE not in body:
        issues.append("missing_fixed_p3_line")

    if _is_generic_opener(opener):
        issues.append("generic_opener")

    if not _has_evidence_link(opener, profile, card):
        issues.append("missing_evidence_anchor")

    if not _source_truth_ok(opener, card):
        issues.append("source_claim_untrue")

    return sorted(set(issues))


def write_email(profile: Profile, card: ResearchCard, tone_template: str, model: str, polish: bool = True) -> Tuple[WriteResult, Dict]:
    trace: Dict = {
        "writer_stage": [],
        "opener_attempt_1": "",
        "opener_attempt_2": "",
        "opener_raw_1": "",
        "opener_raw_2": "",
        "opener_finish_reason_1": "",
        "opener_finish_reason_2": "",
        "opener_selected": "",
        "hard_issues": [],
        "final_body": "",
    }

    subject = f"{config.OUTREACH_SUBJECT_PREFIX} {_company_initials(profile.company_name)}"

    if card.quality != "strong":
        fallback_opener = _clean_line(
            f"I came across {profile.company_name or 'your business'} and wanted to reach out with a quick introduction."
        )
        body = _render_fixed_email(profile.first_name, fallback_opener)
        trace["writer_stage"].append("weak_evidence")
        trace["opener_selected"] = fallback_opener
        trace["final_body"] = body
        return WriteResult(subject=subject, body=body, status="skipped", skip_reason="weak_evidence"), trace

    try:
        trace["writer_stage"].append("opener_generated")
        opener_1, raw_1, finish_1 = _generate_opener(profile, card, tone_template, model)
        trace["opener_attempt_1"] = opener_1
        trace["opener_raw_1"] = raw_1
        trace["opener_finish_reason_1"] = finish_1

        opener_selected = opener_1
        if _is_generic_opener(opener_1):
            trace["writer_stage"].append("opener_retry")
            opener_2, raw_2, finish_2 = _generate_opener(
                profile,
                card,
                tone_template,
                model,
                retry_reason="generic_or_empty_opener",
                previous=opener_1,
            )
            trace["opener_attempt_2"] = opener_2
            trace["opener_raw_2"] = raw_2
            trace["opener_finish_reason_2"] = finish_2
            opener_selected = opener_2

        if _is_generic_opener(opener_selected):
            trace["writer_stage"].append("opener_fallback")
            opener_selected = _deterministic_opener_from_card(profile, card)

        trace["opener_selected"] = opener_selected
        trace["writer_stage"].append("rendered")
        body = _render_fixed_email(profile.first_name, opener_selected)
        trace["final_body"] = body
    except Exception as exc:
        logger.info("writer_generation_error:%s:%s", profile.email, exc)
        fallback_opener = _clean_line(
            f"I came across {profile.company_name or 'your business'} and wanted to reach out with a quick introduction."
        )
        body = _render_fixed_email(profile.first_name, fallback_opener)
        trace["final_body"] = body
        trace["writer_stage"].append("generation_error")
        return WriteResult(subject=subject, body=body, status="skipped", skip_reason="writer_generation_error"), trace

    trace["writer_stage"].append("checked")
    issues = _hard_checks(body, trace["opener_selected"], profile, card)
    trace["hard_issues"] = issues
    _soft_length_warning(body)

    if issues:
        return WriteResult(subject=subject, body=body, status="skipped", skip_reason=issues[0]), trace

    return WriteResult(subject=subject, body=body, status="drafted"), trace
