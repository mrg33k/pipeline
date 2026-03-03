"""
Lightweight research on each enriched contact.
Uses Apollo data + a quick web scrape of the company homepage.
"""

import logging
import json
import os
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from typing import Optional, Any
from urllib.parse import quote_plus

from openai import OpenAI

import config

logger = logging.getLogger(__name__)
_evidence_client = OpenAI()
_EVIDENCE_FAILURE_REPORT_PATH = None


class EvidenceExtractionError(RuntimeError):
    """Raised when evidence extraction fails and run should stop."""

    def __init__(
        self,
        message: str,
        stage: str,
        email: str = "",
        company: str = "",
        raw: str = "",
        finish_reason: str = "",
        request_id: str = "",
        attempts: int = 0,
    ):
        super().__init__(message)
        self.stage = stage
        self.email = email
        self.company = company
        self.raw = raw
        self.finish_reason = finish_reason
        self.request_id = request_id
        self.attempts = attempts


def get_evidence_failure_report_path() -> str:
    global _EVIDENCE_FAILURE_REPORT_PATH
    if _EVIDENCE_FAILURE_REPORT_PATH is None:
        os.makedirs(config.LOG_DIR, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        _EVIDENCE_FAILURE_REPORT_PATH = os.path.join(config.LOG_DIR, f"evidence_failures_{stamp}.jsonl")
    return _EVIDENCE_FAILURE_REPORT_PATH


def _record_evidence_failure(payload: dict) -> None:
    path = get_evidence_failure_report_path()
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=True) + "\n")


_BUSINESS_TYPE_RULES = [
    ("restaurant", ["restaurant", "dining", "food", "hospitality", "bar", "cafe"]),
    ("saas", ["software", "saas", "technology", "platform", "app"]),
    ("real_estate", ["real estate", "property", "brokerage", "leasing", "mortgage"]),
    ("nonprofit", ["nonprofit", "foundation", "charity", "community", "education"]),
    ("construction", ["construction", "contractor", "builder", "remodel", "development"]),
    ("events", ["event", "wedding", "venue", "planning"]),
]

_LANGUAGE_CUES = {
    "restaurant": ["service", "kitchen", "guests", "menu", "dining room"],
    "saas": ["users", "workflow", "teams", "platform", "operators"],
    "real_estate": ["listings", "properties", "agents", "buyers", "leases"],
    "nonprofit": ["programs", "families", "students", "community", "support"],
    "construction": ["projects", "builds", "job sites", "clients", "timelines"],
    "events": ["events", "venues", "guests", "planning", "coordination"],
    "general": ["team", "customers", "day-to-day work"],
}


def build_evidence_pack(profile: dict) -> dict:
    """
    Build normalized evidence object used by email writer.
    """
    industry = (profile.get("company_industry") or "").strip()
    description = (profile.get("company_description") or "").strip()
    snippet = (profile.get("homepage_snippet") or "").strip()
    linkedin_snippet = (profile.get("linkedin_snippet") or "").strip()
    review_signals = [str(x).strip() for x in (profile.get("review_signals") or []) if str(x).strip()]
    merged = " ".join(part for part in [description, snippet, linkedin_snippet, " ".join(review_signals)] if part).strip()

    business_type = _derive_business_type(industry, merged)
    industry_bucket = _canonicalize_industry(industry, merged, profile)
    if industry_bucket != "general":
        business_type = industry_bucket
    proof_points = _extract_proof_points(" ".join(part for part in [description, linkedin_snippet] if part), snippet, limit=3)
    impact_signals = _extract_impact_signals(merged, limit=3)
    social_proof_signals = _extract_social_proof_signals(review_signals, merged, limit=3)
    discovery_signals = _derive_discovery_signals(profile)
    source_truth = _derive_source_truth(discovery_signals)
    who_they_serve = _derive_who_they_serve(merged, business_type)
    why_business_matters_local = _derive_why_business_matters_local(profile, proof_points, impact_signals)
    persona_lens = _derive_persona_lens(business_type)
    impact_subject = _build_impact_subject(who_they_serve, business_type)
    impact_core = _build_impact_core(
        business_type=business_type,
        merged_text=merged,
        proof_points=proof_points,
        impact_signals=impact_signals,
        social_proof_signals=social_proof_signals,
    )
    impact_core = _normalize_impact_core(impact_core, business_type=business_type, impact_subject=impact_subject)
    proof_phrase = _pick_proof_phrase(
        proof_points=proof_points,
        social_proof_signals=social_proof_signals,
        fallback=why_business_matters_local,
    )
    meaning_line = _build_meaning_line(
        industry_bucket=industry_bucket,
        impact_core=impact_core,
        impact_subject=impact_subject,
        proof_phrase=proof_phrase,
    )
    human_thanks_line = _build_human_thanks_line(
        business_type=business_type,
        proof_points=proof_points or social_proof_signals,
        impact_signals=impact_signals or social_proof_signals,
        who_they_serve=who_they_serve,
    )
    opener_angles = _build_opener_angles(
        business_type=business_type,
        proof_points=proof_points,
        social_proof_signals=social_proof_signals,
        why_business_matters_local=why_business_matters_local,
    )
    opener_confidence = _score_opener_confidence(discovery_signals, opener_angles)
    evidence_quality = "strong" if (opener_angles and (proof_points or social_proof_signals)) else "weak"
    source_tags = []
    confidence = 0.0
    if description:
        source_tags.append("apollo_description")
        confidence += 0.4
    if snippet:
        source_tags.append("homepage_snippet")
        confidence += 0.4
    if linkedin_snippet:
        source_tags.append("linkedin_snippet")
        confidence += 0.2
    if social_proof_signals:
        source_tags.append("reviews")
        confidence += 0.2
    if industry:
        source_tags.append("industry_label")
        confidence += 0.2

    return {
        "industry_label": industry or "Unknown",
        "industry_bucket": industry_bucket,
        "business_type": business_type,
        "proof_points": proof_points,
        "proof_phrase": proof_phrase,
        "community_impact_signals": impact_signals,
        "social_proof_signals": social_proof_signals,
        "discovery_signals": discovery_signals,
        "source_truth": source_truth,
        "opener_angles": opener_angles,
        "persona_lens": persona_lens,
        "opener_confidence": opener_confidence,
        "evidence_quality": evidence_quality,
        "human_thanks": human_thanks_line,
        "human_thanks_line": human_thanks_line,
        "why_business_matters_local": why_business_matters_local,
        "who_they_serve": who_they_serve,
        "impact_subject": impact_subject,
        "impact_core": impact_core,
        "meaning_line": meaning_line,
        "language_cues": _LANGUAGE_CUES.get(business_type, _LANGUAGE_CUES["general"]),
        "confidence": round(min(confidence, 1.0), 2),
        "source_tags": source_tags,
    }


def build_research_card(profile: dict, evidence_pack: Optional[dict] = None) -> dict:
    """
    Return the normalized, minimal Research Card used by the writer.
    """
    evidence = dict(evidence_pack or build_evidence_pack(profile) or {})
    industry_bucket = _canonicalize_industry(
        evidence.get("industry_label", ""),
        " ".join(
            [
                " ".join(evidence.get("proof_points") or []),
                " ".join(evidence.get("community_impact_signals") or []),
                " ".join(evidence.get("social_proof_signals") or []),
                str(evidence.get("why_business_matters_local") or ""),
                str(evidence.get("who_they_serve") or ""),
            ]
        ),
        profile,
    )
    if industry_bucket != "general":
        evidence["business_type"] = industry_bucket

    source_truth = _derive_source_truth(evidence.get("source_truth") or evidence.get("discovery_signals") or [])
    impact_subject = _build_impact_subject(str(evidence.get("impact_subject") or evidence.get("who_they_serve") or ""), industry_bucket)
    impact_core = _normalize_impact_core(
        str(evidence.get("impact_core") or ""),
        business_type=industry_bucket,
        impact_subject=impact_subject,
    )
    if not impact_core:
        impact_core = _normalize_impact_core(
            _build_impact_core(
                business_type=industry_bucket,
                merged_text=" ".join(
                    [
                        " ".join(evidence.get("proof_points") or []),
                        " ".join(evidence.get("community_impact_signals") or []),
                        " ".join(evidence.get("social_proof_signals") or []),
                        str(evidence.get("why_business_matters_local") or ""),
                    ]
                ),
                proof_points=evidence.get("proof_points") or [],
                impact_signals=evidence.get("community_impact_signals") or [],
                social_proof_signals=evidence.get("social_proof_signals") or [],
            ),
            business_type=industry_bucket,
            impact_subject=impact_subject,
        )

    proof_phrase = _pick_proof_phrase(
        proof_points=evidence.get("proof_points") or [],
        social_proof_signals=evidence.get("social_proof_signals") or [],
        fallback=str(evidence.get("proof_phrase") or evidence.get("why_business_matters_local") or ""),
    )
    meaning_line = _build_meaning_line(
        industry_bucket=industry_bucket,
        impact_core=impact_core,
        impact_subject=impact_subject,
        proof_phrase=proof_phrase,
    )

    confidence = _safe_float(evidence.get("confidence"), default=0.0)
    evidence_quality = str(evidence.get("evidence_quality") or "").strip().lower()
    if not meaning_line.strip():
        evidence_quality = "weak"

    return {
        "industry_bucket": industry_bucket or "general",
        "source_truth": source_truth,
        "impact_core": impact_core,
        "impact_subject": impact_subject,
        "proof_phrase": proof_phrase,
        "meaning_line": meaning_line,
        "confidence": confidence,
        "evidence_quality": evidence_quality if evidence_quality in {"strong", "weak"} else ("strong" if confidence >= float(config.EVIDENCE_MIN_CONFIDENCE) else "weak"),
    }


def collect_raw_context(profile: dict) -> dict:
    """Collect raw context fields used by evidence extraction."""
    return {
        "company_name": profile.get("company_name", ""),
        "company_industry": profile.get("company_industry", ""),
        "company_description": profile.get("company_description", ""),
        "homepage_snippet": profile.get("homepage_snippet", ""),
        "linkedin_snippet": profile.get("linkedin_snippet", ""),
        "review_signals": profile.get("review_signals", []),
        "discovery_signals": profile.get("discovery_signals", []),
        "company_city": profile.get("company_city", ""),
        "company_state": profile.get("company_state", ""),
        "title": profile.get("title", ""),
    }


def extract_evidence_with_llm(context: dict, model: Optional[str] = None) -> tuple[dict, dict]:
    """
    Perform one LLM extraction attempt.
    Returns (parsed_json, meta) or raises EvidenceExtractionError with category.
    """
    selected_model = (model or config.EVIDENCE_MODEL).strip()
    system = (
        "You extract evidence for local outreach writing. "
        "Return strict JSON only. Never invent facts."
    )
    user = (
        "Given this business context, return JSON with keys:\n"
        "industry_label (string),\n"
        "business_type (string),\n"
        "proof_points (array of 1-3 short factual lines),\n"
        "community_impact_signals (array of 1-3 factual lines),\n"
        "human_thanks (one sentence: what a local person could sincerely thank them for),\n"
        "human_thanks_line (same idea as human_thanks but concise and direct),\n"
        "why_business_matters_local (one sentence about why this business matters in local context),\n"
        "who_they_serve (one short phrase, who benefits from their work),\n"
        "discovery_signals (array of sources used: website|linkedin|reviews),\n"
        "social_proof_signals (array of 1-3 review/reputation lines when available),\n"
        "opener_angles (array of 1-3 opener hook lines tied to evidence),\n"
        "persona_lens (one of: business_owner, food_lover, builder_to_builder, local_neighbor),\n"
        "opener_confidence (number 0-1),\n"
        "evidence_quality (strong|weak),\n"
        "language_cues (array of 3-6 industry words),\n"
        "industry_bucket (restaurant|saas|real_estate|construction|nonprofit|events|general),\n"
        "impact_subject (short phrase: who they help),\n"
        "impact_core (short sentence fragment: what they help happen),\n"
        "proof_phrase (max 10 words factual anchor),\n"
        "meaning_line (one conversational sentence for why they matter, evidence-backed),\n"
        "source_truth (array subset of website|linkedin|reviews).\n\n"
        f"Context: {context}"
    )

    try:
        resp = _evidence_client.chat.completions.create(
            model=selected_model,
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
            max_completion_tokens=config.EVIDENCE_MAX_COMPLETION_TOKENS,
            response_format={"type": "json_object"},
            timeout=config.EVIDENCE_TIMEOUT_SEC,
        )
    except Exception as exc:
        raise EvidenceExtractionError(f"Evidence API call failed: {exc}", stage="api_call") from exc

    choice = resp.choices[0] if resp.choices else None
    raw = ((choice.message.content if choice and choice.message else "") or "").strip()
    finish_reason = (choice.finish_reason if choice else "") or ""
    request_id = getattr(resp, "_request_id", "") or ""
    meta = {"raw": raw, "finish_reason": finish_reason, "request_id": request_id}

    if not raw:
        raise EvidenceExtractionError(
            "Evidence model returned empty content.",
            stage="empty_output",
            raw="",
            finish_reason=finish_reason,
            request_id=request_id,
        )

    parsed = _safe_parse_json_object(raw)
    if parsed is None:
        raise EvidenceExtractionError(
            "Evidence parse failed (invalid JSON).",
            stage="invalid_json",
            raw=raw[:400],
            finish_reason=finish_reason,
            request_id=request_id,
        )

    return parsed, meta


def normalize_evidence(parsed: dict, base: dict) -> dict:
    """Normalize parsed evidence onto base schema."""
    merged = dict(base)
    merged["industry_label"] = str(parsed.get("industry_label") or merged["industry_label"]).strip() or "Unknown"
    merged["business_type"] = str(parsed.get("business_type") or merged["business_type"]).strip() or "general"

    proof = [str(x).strip() for x in (parsed.get("proof_points") or []) if str(x).strip()]
    impact = [str(x).strip() for x in (parsed.get("community_impact_signals") or []) if str(x).strip()]
    social_proof = [str(x).strip() for x in (parsed.get("social_proof_signals") or []) if str(x).strip()]
    discovery_signals = [str(x).strip().lower() for x in (parsed.get("discovery_signals") or []) if str(x).strip()]
    opener_angles = [str(x).strip() for x in (parsed.get("opener_angles") or []) if str(x).strip()]
    cues = [str(x).strip() for x in (parsed.get("language_cues") or []) if str(x).strip()]
    human_thanks = str(parsed.get("human_thanks") or "").strip()
    human_thanks_line = str(parsed.get("human_thanks_line") or human_thanks).strip()
    why_business_matters_local = str(parsed.get("why_business_matters_local") or base.get("why_business_matters_local") or "").strip()
    who_they_serve = str(parsed.get("who_they_serve") or base.get("who_they_serve") or "").strip()
    persona_lens = str(parsed.get("persona_lens") or base.get("persona_lens") or "").strip()
    opener_confidence_raw = parsed.get("opener_confidence", base.get("opener_confidence", 0.0))
    evidence_quality = str(parsed.get("evidence_quality") or base.get("evidence_quality") or "").strip().lower()
    industry_bucket = str(parsed.get("industry_bucket") or base.get("industry_bucket") or "").strip().lower()
    source_truth = [str(x).strip().lower() for x in (parsed.get("source_truth") or []) if str(x).strip()]
    impact_subject = str(parsed.get("impact_subject") or base.get("impact_subject") or "").strip()
    impact_core = str(parsed.get("impact_core") or base.get("impact_core") or "").strip()
    proof_phrase = str(parsed.get("proof_phrase") or base.get("proof_phrase") or "").strip()
    meaning_line = str(parsed.get("meaning_line") or base.get("meaning_line") or "").strip()

    merged["proof_points"] = proof[:3]
    merged["community_impact_signals"] = impact[:3]
    merged["social_proof_signals"] = social_proof[:3] if social_proof else list(base.get("social_proof_signals") or [])[:3]
    merged["discovery_signals"] = _dedupe_list(discovery_signals[:3] or list(base.get("discovery_signals") or [])[:3])
    merged["opener_angles"] = _dedupe_list(opener_angles[:3] or list(base.get("opener_angles") or [])[:3])
    merged["language_cues"] = cues[:6] if cues else merged.get("language_cues", [])
    merged["human_thanks"] = human_thanks_line or human_thanks
    merged["human_thanks_line"] = human_thanks_line or human_thanks
    merged["why_business_matters_local"] = why_business_matters_local
    merged["who_they_serve"] = who_they_serve
    merged["persona_lens"] = persona_lens or merged.get("persona_lens") or _derive_persona_lens(merged.get("business_type", "general"))
    merged["industry_bucket"] = industry_bucket or str(base.get("industry_bucket") or merged.get("business_type") or "general")
    merged["source_truth"] = _dedupe_list(source_truth or list(base.get("source_truth") or []))
    merged["impact_subject"] = impact_subject or str(base.get("impact_subject") or "")
    merged["impact_core"] = _normalize_impact_core(
        impact_core or str(base.get("impact_core") or ""),
        business_type=merged.get("business_type", "general"),
        impact_subject=merged.get("impact_subject", ""),
    )
    merged["proof_phrase"] = proof_phrase or str(base.get("proof_phrase") or "")
    merged["meaning_line"] = meaning_line or str(base.get("meaning_line") or "")
    merged["opener_confidence"] = _safe_float(opener_confidence_raw, default=float(base.get("opener_confidence", 0.0) or 0.0))
    merged["evidence_quality"] = evidence_quality if evidence_quality in {"strong", "weak"} else str(base.get("evidence_quality") or "weak")

    # Normalize/repair meaning-first opener fields deterministically.
    merged_text = " ".join(
        [
            " ".join(merged.get("proof_points") or []),
            " ".join(merged.get("community_impact_signals") or []),
            " ".join(merged.get("social_proof_signals") or []),
            str(merged.get("why_business_matters_local") or ""),
            str(merged.get("who_they_serve") or ""),
        ]
    ).strip()
    merged["industry_bucket"] = _canonicalize_industry(
        merged.get("industry_label", ""),
        merged_text,
        {"company_name": ""},
    )
    if merged.get("industry_bucket") != "general":
        merged["business_type"] = merged.get("industry_bucket")
    merged["source_truth"] = _derive_source_truth(merged.get("discovery_signals") or merged.get("source_truth") or [])
    if not merged.get("impact_subject"):
        merged["impact_subject"] = _build_impact_subject(merged.get("who_they_serve", ""), merged.get("business_type", "general"))
    if not merged.get("impact_core"):
        merged["impact_core"] = _build_impact_core(
            business_type=merged.get("business_type", "general"),
            merged_text=merged_text,
            proof_points=merged.get("proof_points") or [],
            impact_signals=merged.get("community_impact_signals") or [],
            social_proof_signals=merged.get("social_proof_signals") or [],
        )
    merged["impact_core"] = _normalize_impact_core(
        merged.get("impact_core", ""),
        business_type=merged.get("business_type", "general"),
        impact_subject=merged.get("impact_subject", ""),
    )
    if not merged.get("proof_phrase"):
        merged["proof_phrase"] = _pick_proof_phrase(
            proof_points=merged.get("proof_points") or [],
            social_proof_signals=merged.get("social_proof_signals") or [],
            fallback=merged.get("why_business_matters_local", ""),
        )
    if not merged.get("meaning_line"):
        merged["meaning_line"] = _build_meaning_line(
            industry_bucket=merged.get("industry_bucket", "general"),
            impact_core=merged.get("impact_core", ""),
            impact_subject=merged.get("impact_subject", ""),
            proof_phrase=merged.get("proof_phrase", ""),
        )
    return merged


def score_evidence(evidence: dict, base_confidence: float) -> float:
    score = float(base_confidence or 0.0)
    if evidence.get("proof_points"):
        score += 0.25
    if evidence.get("community_impact_signals"):
        score += 0.20
    if evidence.get("social_proof_signals"):
        score += 0.20
    if evidence.get("human_thanks_line") or evidence.get("human_thanks"):
        score += 0.15
    if evidence.get("why_business_matters_local"):
        score += 0.10
    if evidence.get("who_they_serve"):
        score += 0.05
    if evidence.get("meaning_line"):
        score += 0.10
    if evidence.get("opener_angles"):
        score += 0.10
    score += min(0.15, float(evidence.get("opener_confidence") or 0.0) * 0.15)
    return round(min(score, 1.0), 2)


def validate_evidence(evidence: dict) -> tuple[bool, str]:
    if not str(evidence.get("industry_label") or "").strip():
        return False, "schema_invalid:missing_industry_label"
    if not isinstance(evidence.get("proof_points"), list):
        return False, "schema_invalid:proof_points_not_list"
    if len(evidence.get("proof_points") or []) < 1 and len(evidence.get("social_proof_signals") or []) < 1:
        return False, "schema_invalid:no_proof_or_social_proof"
    if len(evidence.get("opener_angles") or []) < 1:
        return False, "schema_invalid:opener_angles_empty"
    if not (
        evidence.get("human_thanks_line")
        or evidence.get("human_thanks")
        or evidence.get("community_impact_signals")
        or evidence.get("social_proof_signals")
        or evidence.get("why_business_matters_local")
    ):
        return False, "schema_invalid:no_human_context"
    if not str(evidence.get("meaning_line") or "").strip():
        return False, "schema_invalid:missing_meaning_line"
    if str(evidence.get("evidence_quality") or "").strip().lower() == "weak":
        return False, "low_confidence"
    if float(evidence.get("confidence") or 0.0) < float(config.EVIDENCE_MIN_CONFIDENCE):
        return False, "low_confidence"
    return True, ""


def enrich_evidence_with_llm(profile: dict, model: Optional[str] = None) -> dict:
    """
    Deterministic evidence pipeline with retries, validation, and diagnostics.
    """
    base = build_evidence_pack(profile)
    email = str(profile.get("email", "") or "")
    company = str(profile.get("company_name", "") or "")
    context = collect_raw_context(profile)

    last_error = None
    for attempt in range(1, config.EVIDENCE_MAX_RETRIES + 1):
        try:
            parsed, meta = extract_evidence_with_llm(context, model=model)
            merged = normalize_evidence(parsed, base)
            if not merged.get("discovery_signals"):
                merged["discovery_signals"] = _derive_discovery_signals(profile)
            if not merged.get("opener_angles"):
                merged["opener_angles"] = _build_opener_angles(
                    business_type=merged.get("business_type", "general"),
                    proof_points=merged.get("proof_points") or [],
                    social_proof_signals=merged.get("social_proof_signals") or [],
                    why_business_matters_local=merged.get("why_business_matters_local", ""),
                )
            merged["opener_confidence"] = max(
                _safe_float(merged.get("opener_confidence"), default=0.0),
                _score_opener_confidence(merged.get("discovery_signals") or [], merged.get("opener_angles") or []),
            )
            merged["evidence_quality"] = (
                "strong"
                if ((merged.get("opener_angles") or []) and ((merged.get("proof_points") or []) or (merged.get("social_proof_signals") or [])))
                else "weak"
            )
            merged["confidence"] = score_evidence(merged, base.get("confidence", 0.0))
            source_tags = list(merged.get("source_tags") or [])
            if "llm_evidence" not in source_tags:
                source_tags.append("llm_evidence")
            merged["source_tags"] = source_tags

            ok, reason = validate_evidence(merged)
            logger.info(
                "evidence_parse_status:%s:attempt=%d:ok=true:confidence=%.2f:proof_points=%d:impact_signals=%d:social_proof=%d:opener_angles=%d:discovery=%d:evidence_quality=%s",
                email or "unknown",
                attempt,
                float(merged.get("confidence", 0.0)),
                len(merged.get("proof_points") or []),
                len(merged.get("community_impact_signals") or []),
                len(merged.get("social_proof_signals") or []),
                len(merged.get("opener_angles") or []),
                len(merged.get("discovery_signals") or []),
                merged.get("evidence_quality", "unknown"),
            )
            if ok:
                return merged
            last_error = EvidenceExtractionError(
                f"Evidence validation failed: {reason}",
                stage="low_confidence" if reason == "low_confidence" else "schema_invalid",
                email=email,
                company=company,
                raw=(meta.get("raw") or "")[:400],
                finish_reason=meta.get("finish_reason", ""),
                request_id=meta.get("request_id", ""),
                attempts=attempt,
            )
        except EvidenceExtractionError as exc:
            exc.email = exc.email or email
            exc.company = exc.company or company
            exc.attempts = attempt
            logger.info(
                "evidence_parse_status:%s:attempt=%d:ok=false:stage=%s:finish_reason=%s",
                email or "unknown",
                attempt,
                exc.stage,
                exc.finish_reason or "<none>",
            )
            last_error = exc

    payload = {
        "timestamp": datetime.now().isoformat(),
        "email": email,
        "company": company,
        "stage": getattr(last_error, "stage", "unknown"),
        "message": str(last_error) if last_error else "Unknown evidence failure.",
        "request_id": getattr(last_error, "request_id", ""),
        "finish_reason": getattr(last_error, "finish_reason", ""),
        "raw_snippet": getattr(last_error, "raw", ""),
        "attempts": getattr(last_error, "attempts", config.EVIDENCE_MAX_RETRIES),
        "context": context,
        "base_evidence": base,
    }
    _record_evidence_failure(payload)

    if config.EVIDENCE_FAIL_FAST:
        if isinstance(last_error, EvidenceExtractionError):
            raise last_error
        raise EvidenceExtractionError(
            "Evidence extraction failed after retries.",
            stage="unknown",
            email=email,
            company=company,
            attempts=config.EVIDENCE_MAX_RETRIES,
        )

    return base


def _safe_parse_json_object(raw: str) -> Optional[dict]:
    text = (raw or "").strip()
    if not text:
        return None
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()
    import json
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        # Fallback: extract first JSON object-like substring.
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            snippet = text[start:end + 1]
            try:
                parsed = json.loads(snippet)
                return parsed if isinstance(parsed, dict) else None
            except Exception:
                return None
        return None


def _derive_business_type(industry: str, text: str) -> str:
    hay = f"{industry} {text}".lower()
    for label, terms in _BUSINESS_TYPE_RULES:
        if any(term in hay for term in terms):
            return label
    return "general"


def _canonicalize_industry(industry_label: str, text: str, profile: Optional[dict] = None) -> str:
    """
    Resolve to a canonical industry bucket used by opener logic.
    Avoid returning generic when concrete clues exist.
    """
    profile = profile or {}
    hay = " ".join(
        [
            str(industry_label or ""),
            str(text or ""),
            str(profile.get("company_name") or ""),
            str(profile.get("title") or ""),
        ]
    ).lower()
    bucket = _derive_business_type(industry_label, hay)
    if bucket != "general":
        return bucket

    hints = [
        ("restaurant", ["bbq", "grill", "catering", "steakhouse", "bistro", "eatery", "kitchen"]),
        ("saas", ["software", "platform", "app", "automation", "workflow", "cloud"]),
        ("real_estate", ["realty", "realtor", "properties", "brokerage", "leasing", "property"]),
        ("construction", ["construction", "contractor", "builder", "remodel", "renovation", "build"]),
        ("nonprofit", ["nonprofit", "foundation", "charity", "donation", "community program"]),
        ("events", ["events", "wedding", "venue", "planning", "hospitality group"]),
    ]
    for label, terms in hints:
        if any(term in hay for term in terms):
            return label
    return "general"


def _derive_source_truth(discovery_signals: list[str]) -> list[str]:
    ordered = []
    for token in discovery_signals or []:
        t = str(token or "").strip().lower()
        if t in {"website", "linkedin", "reviews"} and t not in ordered:
            ordered.append(t)
    return ordered


def _build_impact_subject(who_they_serve: str, business_type: str) -> str:
    subject = re.sub(r"\s+", " ", str(who_they_serve or "")).strip().strip(".")
    generic = {"people in your local community", "people in the area", "customers", "clients"}
    if subject and subject.lower() not in generic:
        return subject
    defaults = {
        "restaurant": "local diners and guests",
        "saas": "operators and teams",
        "real_estate": "buyers, sellers, and tenants",
        "construction": "owners and project teams",
        "nonprofit": "families and community members",
        "events": "hosts and guests",
        "general": "people in the area",
    }
    return defaults.get(str(business_type or "general"), defaults["general"])


def _build_impact_core(
    business_type: str,
    merged_text: str,
    proof_points: list[str],
    impact_signals: list[str],
    social_proof_signals: list[str],
) -> str:
    hay = " ".join(
        [
            str(merged_text or ""),
            " ".join(str(x) for x in (proof_points or [])),
            " ".join(str(x) for x in (impact_signals or [])),
            " ".join(str(x) for x in (social_proof_signals or [])),
        ]
    ).lower()

    if business_type == "restaurant":
        if any(tok in hay for tok in ["live music", "bull riding", "events", "entertainment"]):
            return "you create a place people choose for a real night out"
        if any(tok in hay for tok in ["bbq", "barbecue", "grill"]):
            return "you create a local spot people keep coming back to"
        if any(tok in hay for tok in ["french", "mediterranean", "chef", "fine dining"]):
            return "you create a dining experience people trust for a good night out"
        return "you create a local spot people keep coming back to"

    if business_type == "saas":
        if any(tok in hay for tok in ["food cost", "costing", "margin"]):
            return "you help teams keep food costs and operations under control"
        if any(tok in hay for tok in ["scheduling", "workflow", "automation"]):
            return "you help teams run day to day work with less friction"
        return "you help teams run day to day work with less friction"

    if business_type == "real_estate":
        return "you help people make big property decisions with more clarity"
    if business_type == "construction":
        return "you help owners move projects from planning to finished build"
    if business_type == "nonprofit":
        return "you support people in the community in a way they can feel"
    if business_type == "events":
        return "you help people pull off important events without the chaos"
    return "you do useful work people in the area rely on"


def _normalize_impact_core(core: str, business_type: str, impact_subject: str = "") -> str:
    text = re.sub(r"\s+", " ", str(core or "")).strip().strip(".,;:-")
    if not text:
        return ""
    lower = text.lower()

    # Repair common verb-first fragments from LLM output.
    verb_first = (
        "reduce ",
        "improve ",
        "provide ",
        "provides ",
        "offer ",
        "offers ",
        "help ",
        "helps ",
        "enable ",
        "enables ",
        "support ",
        "supports ",
        "create ",
        "creates ",
        "deliver ",
        "delivers ",
        "enjoy ",
        "enjoys ",
    )
    if lower.startswith(verb_first):
        if lower.startswith("helps "):
            text = "you help " + text[6:]
        elif lower.startswith("provides "):
            text = "you provide " + text[9:]
        elif lower.startswith("offers "):
            text = "you offer " + text[7:]
        elif lower.startswith("creates "):
            text = "you create " + text[8:]
        elif lower.startswith("delivers "):
            text = "you deliver " + text[9:]
        elif lower.startswith("supports "):
            text = "you support " + text[9:]
        elif lower.startswith("enables "):
            text = "you enable " + text[8:]
        elif lower.startswith("enjoy "):
            text = "people enjoy " + text[6:]
        elif lower.startswith("enjoys "):
            text = "people enjoy " + text[7:]
        else:
            text = "you " + text

    lower = text.lower()
    if not (lower.startswith("you ") or lower.startswith("people ") or lower.startswith("your team ")):
        text = "you " + text

    # Keep it concise.
    words = re.findall(r"\S+", text)
    if len(words) > 16:
        text = " ".join(words[:16]).strip(" ,.;:-")
    return text


def _pick_proof_phrase(proof_points: list[str], social_proof_signals: list[str], fallback: str) -> str:
    for source in (proof_points or []):
        phrase = _compact_phrase(source, 10)
        if phrase and not _is_low_information_phrase(phrase):
            return phrase
    for source in (social_proof_signals or []):
        phrase = _compact_phrase(source, 10)
        if phrase and not _is_low_information_phrase(phrase):
            return phrase
    phrase = _compact_phrase(fallback, 10)
    if phrase and not _is_low_information_phrase(phrase):
        return phrase
    return ""


def _build_meaning_line(industry_bucket: str, impact_core: str, impact_subject: str, proof_phrase: str) -> str:
    core = _normalize_impact_core(
        str(impact_core or ""),
        business_type=str(industry_bucket or "general"),
        impact_subject=str(impact_subject or ""),
    )
    if not core:
        return ""

    subject = re.sub(r"\s+", " ", str(impact_subject or "")).strip().strip(".,;:-")
    proof = _compact_phrase(str(proof_phrase or ""), 8)

    line = core
    if subject and subject.lower() not in line.lower():
        line = f"{line} for {subject}"

    if proof and not _is_low_information_phrase(proof):
        proof_norm = proof.lower()
        if proof_norm not in line.lower():
            line = f"{line}, and {proof_norm} stands out"

    words = re.findall(r"\S+", line)
    if len(words) > 20:
        line = " ".join(words[:20]).strip(" ,.;:-")
    line = re.sub(r"\s+", " ", line).strip(" ,.;:-")
    if not line:
        return ""
    return line + "."


def _compact_phrase(text: str, max_words: int) -> str:
    cleaned = re.sub(r"\s+", " ", str(text or "")).strip().strip(".,;:-")
    cleaned = re.sub(r"\brated\s*\d+(\.\d+)?\s*out of\s*\d+(\.\d+)?\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b\d+\s*(verified\s*)?(user\s*)?reviews?\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(on|from)\s+(tripadvisor|capterra|birdeye|yelp|google reviews)\b", "", cleaned, flags=re.IGNORECASE)
    words = re.findall(r"\S+", cleaned)
    if not words:
        return ""
    if len(words) > max_words:
        words = words[:max_words]
    return " ".join(words).strip(" ,.;:-")


def _is_low_information_phrase(text: str) -> bool:
    lower = re.sub(r"\s+", " ", str(text or "").strip().lower())
    if not lower:
        return True
    generic_patterns = [
        "local business community",
        "team contributes",
        "solid work",
        "work people in the area rely on",
        "serves people in the area",
    ]
    return any(pattern in lower for pattern in generic_patterns)


def _extract_proof_points(description: str, snippet: str, limit: int = 3) -> list[str]:
    text = " ".join(part for part in [description, snippet] if part).strip()
    if not text:
        return []
    sentences = _candidate_sentences(text)
    selected = []
    for sentence in sentences:
        cleaned = _clean_sentence(sentence)
        if len(cleaned.split()) < 5:
            continue
        if _looks_noise(cleaned):
            continue
        selected.append(cleaned)
        if len(selected) >= limit:
            break
    return selected


def _extract_impact_signals(text: str, limit: int = 3) -> list[str]:
    if not text:
        return []
    impact_regex = re.compile(
        r"\b(community|families|students|customers|clients|guests|local|support|care|help|service)\b",
        flags=re.IGNORECASE,
    )
    signals = []
    for sentence in _candidate_sentences(text):
        cleaned = _clean_sentence(sentence)
        if len(cleaned.split()) < 4:
            continue
        if impact_regex.search(cleaned):
            signals.append(cleaned)
        if len(signals) >= limit:
            break
    return signals


def _extract_social_proof_signals(review_signals: list[str], merged_text: str, limit: int = 3) -> list[str]:
    signals = []
    for line in review_signals or []:
        cleaned = _clean_sentence(line)
        if cleaned and len(cleaned.split()) >= 4:
            signals.append(cleaned)
        if len(signals) >= limit:
            break

    if len(signals) < limit:
        review_like = re.compile(
            r"\b(review|reviews|rated|rating|stars?|favorite|popular|known for|reputation)\b",
            flags=re.IGNORECASE,
        )
        for sentence in _candidate_sentences(merged_text or ""):
            cleaned = _clean_sentence(sentence)
            if len(cleaned.split()) < 4:
                continue
            if review_like.search(cleaned):
                signals.append(cleaned)
            if len(signals) >= limit:
                break

    return _dedupe_list(signals)[:limit]


def _derive_discovery_signals(profile: dict) -> list[str]:
    signals = []
    if str(profile.get("homepage_snippet") or "").strip():
        signals.append("website")
    if str(profile.get("linkedin_snippet") or "").strip():
        signals.append("linkedin")
    if profile.get("review_signals"):
        signals.append("reviews")
    return _dedupe_list(signals)


def _derive_persona_lens(business_type: str) -> str:
    mapping = {
        "restaurant": "food_lover",
        "saas": "builder_to_builder",
        "construction": "builder_to_builder",
        "real_estate": "business_owner",
        "events": "local_neighbor",
        "nonprofit": "local_neighbor",
        "general": "business_owner",
    }
    return mapping.get(str(business_type or "general"), "business_owner")


def _build_opener_angles(
    business_type: str,
    proof_points: list[str],
    social_proof_signals: list[str],
    why_business_matters_local: str,
) -> list[str]:
    angles = []
    for line in (social_proof_signals or [])[:2]:
        angles.append(_clean_sentence(line))
    for line in (proof_points or [])[:2]:
        angles.append(_clean_sentence(line))
    if why_business_matters_local:
        angles.append(_clean_sentence(why_business_matters_local))

    cleaned = [a for a in _dedupe_list([a for a in angles if a]) if len(a.split()) >= 4]
    if cleaned:
        return cleaned[:3]

    defaults = {
        "restaurant": "people in town keep talking about your food and hospitality",
        "saas": "your product helps teams run their day-to-day workflow",
        "real_estate": "your team is active in helping people with property decisions",
        "construction": "your projects are visible around the area",
        "events": "your team helps people pull off meaningful events",
        "nonprofit": "your organization supports people in the local community",
        "general": "the work your team does in the area",
    }
    return [defaults.get(business_type, defaults["general"])]


def _score_opener_confidence(discovery_signals: list[str], opener_angles: list[str]) -> float:
    score = 0.0
    score += min(0.45, 0.15 * len(discovery_signals or []))
    score += min(0.55, 0.20 * len(opener_angles or []))
    return round(min(score, 1.0), 2)


def _dedupe_list(values: list[str]) -> list[str]:
    seen = set()
    out = []
    for value in values or []:
        key = (value or "").strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append((value or "").strip())
    return out


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _derive_who_they_serve(text: str, business_type: str) -> str:
    lower = (text or "").lower()
    patterns = [
        r"(independent restaurant owners?)",
        r"(small businesses?)",
        r"(home buyers?|sellers?)",
        r"(families|students|children)",
        r"(event planners?|brides?|grooms?)",
        r"(contractors?|developers?)",
        r"(hotel guests?|diners?)",
    ]
    for pattern in patterns:
        match = re.search(pattern, lower, flags=re.IGNORECASE)
        if match:
            return match.group(1)

    defaults = {
        "restaurant": "local diners and guests",
        "saas": "teams and operators",
        "real_estate": "buyers, sellers, and tenants",
        "nonprofit": "families and community members",
        "construction": "property owners and project teams",
        "events": "hosts and guests",
        "general": "people in your local community",
    }
    return defaults.get(business_type, defaults["general"])


def _derive_why_business_matters_local(profile: dict, proof_points: list[str], impact_signals: list[str]) -> str:
    city = (profile.get("company_city") or profile.get("city") or "").strip()
    area = city if city else "the area"

    if impact_signals:
        signal = _clean_sentence(impact_signals[0]).rstrip(".")
        return f"In {area}, {signal.lower()}."

    if proof_points:
        proof = _clean_sentence(proof_points[0]).rstrip(".")
        return f"In {area}, you are known for {proof.lower()}."

    return f"In {area}, your team contributes to the local business community."


def _build_human_thanks_line(
    business_type: str,
    proof_points: list[str],
    impact_signals: list[str],
    who_they_serve: str,
) -> str:
    if impact_signals:
        signal = _clean_sentence(impact_signals[0]).rstrip(".")
        return f"Thank you for {signal.lower()}."

    if proof_points:
        proof = _clean_sentence(proof_points[0]).rstrip(".")
        return f"Thank you for the work you do around {proof.lower()}."

    defaults = {
        "restaurant": f"Thank you for creating a place locals can enjoy with {who_they_serve}.",
        "saas": f"Thank you for building tools that support {who_they_serve}.",
        "real_estate": f"Thank you for helping {who_they_serve} navigate important decisions.",
        "nonprofit": f"Thank you for supporting {who_they_serve} in the community.",
        "construction": f"Thank you for building for {who_they_serve} in the area.",
        "events": f"Thank you for helping {who_they_serve} create meaningful events.",
        "general": f"Thank you for serving {who_they_serve} in the area.",
    }
    return defaults.get(business_type, defaults["general"])


def _candidate_sentences(text: str) -> list[str]:
    # Sentence-like chunks from lightweight scraped/description text.
    raw = re.split(r"(?<=[\.\!\?])\s+|[\n\r]+|(?<=\w)\s+\|\s+", text)
    return [chunk.strip() for chunk in raw if chunk and chunk.strip()]


def _clean_sentence(sentence: str) -> str:
    cleaned = re.sub(r"\s+", " ", sentence).strip(" -|")
    # Keep concise evidence lines.
    if len(cleaned) > 160:
        cleaned = cleaned[:160].rsplit(" ", 1)[0].strip()
    return cleaned


def _looks_noise(sentence: str) -> bool:
    lower = sentence.lower()
    noise_tokens = ["cookie", "privacy policy", "terms", "javascript", "sign up", "log in"]
    return any(tok in lower for tok in noise_tokens)


def refresh_profile_homepage_snippet(profile: dict, force: bool = False) -> dict:
    """
    Ensure homepage snippet is present. Re-scrape when force=True.
    """
    if not isinstance(profile, dict):
        return profile
    if profile.get("homepage_snippet") and not force:
        return profile

    domain = (profile.get("company_domain") or "").strip()
    if not domain:
        return profile
    if not domain.startswith("http"):
        domain = f"https://{domain}"
    profile["homepage_snippet"] = _scrape_homepage(domain)
    return profile


def refresh_profile_linkedin_snippet(profile: dict, force: bool = False) -> dict:
    if not isinstance(profile, dict):
        return profile
    if profile.get("linkedin_snippet") and not force:
        return profile

    linkedin_url = (profile.get("company_linkedin") or "").strip()
    if not linkedin_url:
        profile["linkedin_snippet"] = ""
        return profile

    profile["linkedin_snippet"] = _scrape_linkedin_about(linkedin_url)
    return profile


def refresh_profile_review_signals(profile: dict, force: bool = False) -> dict:
    if not isinstance(profile, dict):
        return profile
    if profile.get("review_signals") and not force:
        return profile

    company = (profile.get("company_name") or "").strip()
    city = (profile.get("company_city") or profile.get("city") or "").strip()
    state = (profile.get("company_state") or profile.get("state") or "").strip()
    if not company:
        profile["review_signals"] = []
        profile["review_sources"] = []
        return profile

    snippets, sources = _search_review_signals(company, city=city, state=state, limit=3)
    profile["review_signals"] = snippets
    profile["review_sources"] = sources
    return profile


def gather_profile_context(profile: dict, force: bool = False) -> dict:
    """
    Gather all pre-writer context signals:
    - homepage snippet
    - linkedin snippet
    - review snippets/sources
    - discovery signal tags
    """
    if not isinstance(profile, dict):
        return profile

    refresh_profile_homepage_snippet(profile, force=force)
    refresh_profile_linkedin_snippet(profile, force=force)
    refresh_profile_review_signals(profile, force=force)
    profile["discovery_signals"] = _derive_discovery_signals(profile)
    return profile


def research_contact(person: dict, model: Optional[str] = None) -> dict:
    """
    Build a research profile for one enriched contact.
    Combines Apollo data with a quick homepage scrape.
    Returns a dict with all context needed for email writing.
    """
    org = person.get("organization", {}) or {}

    profile = {
        "apollo_id": person.get("id", ""),
        "first_name": person.get("first_name", ""),
        "last_name": person.get("last_name", ""),
        "full_name": person.get("name", ""),
        "email": person.get("email", ""),
        "title": person.get("title", ""),
        "headline": person.get("headline", ""),
        "linkedin_url": person.get("linkedin_url", ""),
        "city": person.get("city", ""),
        "state": person.get("state", ""),
        "company_name": org.get("name", ""),
        "company_domain": org.get("primary_domain", "") or org.get("website_url", ""),
        "company_industry": org.get("industry", ""),
        "company_city": org.get("city", ""),
        "company_state": org.get("state", ""),
        "company_description": org.get("short_description", "") or org.get("seo_description", ""),
        "company_employee_count": org.get("estimated_num_employees", ""),
        "company_founded_year": org.get("founded_year", ""),
        "company_linkedin": org.get("linkedin_url", ""),
        "homepage_snippet": "",
        "linkedin_snippet": "",
        "review_signals": [],
        "review_sources": [],
        "discovery_signals": [],
    }

    gather_profile_context(profile, force=False)
    profile["evidence_pack"] = enrich_evidence_with_llm(profile, model=model)

    return profile


def _scrape_homepage(url: str) -> str:
    """
    Grab the first ~500 chars of visible text from a company homepage.
    Fails silently on any error.
    """
    try:
        resp = requests.get(url, timeout=8, headers={
            "User-Agent": "Mozilla/5.0 (compatible; AOM-Research/1.0)"
        })
        if resp.status_code != 200:
            return ""
        soup = BeautifulSoup(resp.text, "html.parser")

        # Remove script/style
        for tag in soup(["script", "style", "nav", "header", "footer"]):
            tag.decompose()

        text = soup.get_text(separator=" ", strip=True)
        # Take first 500 chars
        snippet = text[:500].strip()
        return snippet
    except Exception as e:
        logger.debug(f"Homepage scrape failed for {url}: {e}")
        return ""


def _scrape_linkedin_about(url: str) -> str:
    """
    Best-effort LinkedIn company snippet extraction from public page metadata.
    """
    if not url:
        return ""
    try:
        resp = requests.get(url, timeout=8, headers={"User-Agent": "Mozilla/5.0 (compatible; AOM-Research/1.0)"})
        if resp.status_code != 200:
            return ""
        soup = BeautifulSoup(resp.text, "html.parser")
        candidates = []

        title = (soup.title.string or "").strip() if soup.title else ""
        if title:
            candidates.append(title)

        for meta_name in ["description", "og:description", "twitter:description"]:
            tag = soup.find("meta", attrs={"name": meta_name}) or soup.find("meta", attrs={"property": meta_name})
            if tag and tag.get("content"):
                candidates.append(str(tag.get("content")).strip())

        text = " ".join(candidates).strip()
        text = re.sub(r"\s+", " ", text)
        if not text:
            return ""
        lower = text.lower()
        if "sign in" in lower or "join now" in lower:
            return ""
        return text[:420]
    except Exception as exc:
        logger.debug(f"LinkedIn scrape failed for {url}: {exc}")
        return ""


def _search_review_signals(company: str, city: str = "", state: str = "", limit: int = 3) -> tuple[list[str], list[str]]:
    """
    Best-effort lightweight web search for review/reputation snippets.
    """
    query_parts = [company]
    if city:
        query_parts.append(city)
    if state:
        query_parts.append(state)
    query_parts.append("reviews")
    query = " ".join(part for part in query_parts if part).strip()
    if not query:
        return [], []

    search_url = f"https://duckduckgo.com/html/?q={quote_plus(query)}"
    snippets = []
    sources = []
    try:
        resp = requests.get(search_url, timeout=8, headers={"User-Agent": "Mozilla/5.0 (compatible; AOM-Research/1.0)"})
        if resp.status_code != 200:
            return [], []
        soup = BeautifulSoup(resp.text, "html.parser")
        result_nodes = soup.select("div.result")
        review_hint = re.compile(
            r"\b(review|reviews|rated|rating|stars?|favorite|popular|recommended|reputation)\b",
            flags=re.IGNORECASE,
        )
        for node in result_nodes:
            if len(snippets) >= limit:
                break
            link = node.select_one("a.result__a")
            snippet_node = node.select_one(".result__snippet")
            href = (link.get("href") if link else "") or ""
            title = link.get_text(" ", strip=True) if link else ""
            snippet_text = snippet_node.get_text(" ", strip=True) if snippet_node else ""
            joined = re.sub(r"\s+", " ", f"{title} {snippet_text}").strip()
            if not joined:
                continue
            if not review_hint.search(joined):
                continue
            lower_href = href.lower()
            if not any(domain in lower_href for domain in ["yelp", "google", "tripadvisor", "facebook", "opentable", "zomato", "bbb"]):
                # Keep non-review domains only if snippet clearly looks like social proof.
                if "review" not in joined.lower() and "rated" not in joined.lower():
                    continue
            snippets.append(joined[:220])
            sources.append(href[:320])
    except Exception as exc:
        logger.debug(f"Review search failed for '{query}': {exc}")
        return [], []

    return _dedupe_list(snippets)[:limit], _dedupe_list(sources)[:limit]


def research_batch(enriched_people: list[dict], model: Optional[str] = None) -> list[dict]:
    """Research all enriched contacts. Returns list of profile dicts."""
    profiles = []
    for i, person in enumerate(enriched_people):
        logger.info(f"Researching {i + 1}/{len(enriched_people)}: "
                     f"{person.get('first_name', '')} {person.get('last_name', '')} "
                     f"at {(person.get('organization') or {}).get('name', 'Unknown')}")
        profile = research_contact(person, model=model)
        ev = profile.get("evidence_pack", {}) or {}
        logger.info(
            f"evidence_pack:{profile.get('email','')}:{ev.get('business_type','general')}:{ev.get('confidence',0.0)}"
        )
        profiles.append(profile)
    return profiles
