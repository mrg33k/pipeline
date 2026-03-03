"""
Single-pass email writer.
Production path:
1) read one normalized Research Card
2) generate slots once
3) render deterministic template once
4) opener quality gate (one opener-only retry)
5) minimal hard checks

No production QA agent loop.
"""

import json
import logging
import os
import re
import hashlib
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from openai import OpenAI

import config
import research

logger = logging.getLogger(__name__)
client = OpenAI()

_WRITER_FAILURE_REPORT_PATH = None
_WRITER_DEBUG_REPORT_PATH = None
_LAST_BATCH_STATS = {
    "generated": 0,
    "drafted": 0,
    "skipped_by_critical": 0,
    "skipped_by_judge": 0,
    "failed_other": 0,
}

SYSTEM_PROMPT = """You write short cold outreach emails for Patrik Matheson, owner of Ahead of Market, a Phoenix-based video studio.

Your job is to rewrite the same outreach intent for each specific business using provided research.
Keep language natural, local, and human.
Do not sound like agency copy.
"""

HARD_ASK_LINE = "Are you already working with someone on web/social stuff?"
ASK_VARIANTS = [
    "Are you already working with someone on web/social stuff?",
    "Are you currently working with anyone on web/social stuff?",
    "Are you already partnered with someone for web/social stuff?",
]


class WriterValidationError(RuntimeError):
    def __init__(
        self,
        message: str,
        stage: str,
        email: str = "",
        company: str = "",
        issues: Optional[List[str]] = None,
        generated_body: str = "",
        judge_result: Optional[Dict[str, Any]] = None,
        repair_prompt_snippet: str = "",
        critical_check_failures: Optional[List[str]] = None,
        final_decision: str = "skipped",
    ):
        super().__init__(message)
        self.stage = stage
        self.email = email
        self.company = company
        self.issues = list(issues or [])
        self.generated_body = generated_body
        self.judge_result = judge_result or {}
        self.repair_prompt_snippet = repair_prompt_snippet
        self.critical_check_failures = list(critical_check_failures or [])
        self.final_decision = final_decision


def get_writer_failure_report_path() -> str:
    global _WRITER_FAILURE_REPORT_PATH
    if _WRITER_FAILURE_REPORT_PATH is None:
        os.makedirs(config.LOG_DIR, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        _WRITER_FAILURE_REPORT_PATH = os.path.join(config.LOG_DIR, f"writer_failures_{stamp}.jsonl")
    return _WRITER_FAILURE_REPORT_PATH


def _record_writer_failure(payload: dict) -> None:
    path = get_writer_failure_report_path()
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=True) + "\n")


def get_writer_debug_report_path() -> str:
    global _WRITER_DEBUG_REPORT_PATH
    if _WRITER_DEBUG_REPORT_PATH is None:
        os.makedirs(config.LOG_DIR, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        _WRITER_DEBUG_REPORT_PATH = os.path.join(config.LOG_DIR, f"writer_debug_{stamp}.jsonl")
    return _WRITER_DEBUG_REPORT_PATH


def _record_writer_debug(payload: dict) -> None:
    path = get_writer_debug_report_path()
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=True) + "\n")


def get_last_batch_stats() -> dict:
    return dict(_LAST_BATCH_STATS)


def write_email(profile: dict, system_prompt: Optional[str] = None, model: Optional[str] = None) -> dict:
    result = debug_write_one(
        profile=profile,
        system_prompt=system_prompt,
        model=model,
        use_judge=False,
        emit_debug=False,
    )

    if result.get("final_decision") != "drafted":
        _raise_writer_failure(
            profile=profile,
            stage=result.get("final_stage", "writer_failed"),
            message=f"Writer failed: {result.get('final_stage', 'writer_failed')}",
            issues=result.get("final_issues", []),
            generated_body=result.get("final_body", ""),
            judge_result=result.get("judge_2") or result.get("judge_1") or {},
            repair_prompt_snippet=result.get("repair_prompt_snippet", ""),
            critical_check_failures=result.get("critical_issues_after", []),
            final_decision=result.get("final_decision", "skipped"),
        )

    return {
        "subject": result["subject"],
        "body": result["final_body"],
    }


def debug_write_one(
    profile: dict,
    system_prompt: Optional[str] = None,
    model: Optional[str] = None,
    use_judge: bool = True,
    emit_debug: bool = True,
) -> dict:
    selected_prompt = (system_prompt or SYSTEM_PROMPT).strip()
    selected_model = (model or config.OPENAI_MODEL).strip()

    company = profile.get("company_name", "your company")
    subject = f"Video for {_company_initials(company)}"
    evidence = _get_evidence_pack(profile)
    research_card = _resolve_research_card(profile, evidence)
    anchor_card = _build_intent_anchor_card(profile, research_card)

    timeline: List[str] = []
    writer_trace: List[dict] = []
    slot_retry_used = False
    slot_source = "single_rewrite"
    patch_actions: List[str] = []
    opener_retry_used = False
    opener_initial = ""
    opener_retry = ""
    opener_final = ""
    opener_quality_trace: List[dict] = []
    source_truth_trace: List[str] = list(research_card.get("source_truth") or [])
    skip_reason = ""

    judge_1 = {"passes": True, "reasons": [], "severity": "low", "suggested_fix": ""}
    judge_2 = {"passes": True, "reasons": [], "severity": "low", "suggested_fix": ""}

    def _trace(stage: str, hard_check_issues: Optional[List[str]] = None, stage_skip_reason: str = "") -> None:
        timeline.append(f"writer_stage:{stage}")
        logger.info("writer_stage:%s", stage)
        writer_trace.append(
            {
                "stage": stage,
                "source_truth": list(source_truth_trace),
                "opener_text": opener_final or opener_initial,
                "hard_check_issues": list(hard_check_issues or []),
                "skip_reason": stage_skip_reason,
            }
        )

    logger.info("industry_bucket:%s", research_card.get("industry_bucket", "general"))
    logger.info("source_truth:%s", "|".join(source_truth_trace))
    _trace("prepare_research_card")

    body = ""
    raw_model_output = ""
    normalized_output = ""
    opener_meta: Dict[str, str] = {"sentence": "", "source": ""}
    final_decision = "drafted"
    final_stage = "final"
    final_issues: List[str] = []
    dominant_failure_category = ""
    missing_anchors: List[str] = []

    if _research_card_is_weak(research_card):
        final_decision = "skipped"
        final_stage = "weak_evidence"
        final_issues = ["weak_evidence"]
        dominant_failure_category = "weak_evidence"
        skip_reason = "weak_evidence"
        _trace("skip_weak_evidence", hard_check_issues=final_issues, stage_skip_reason=skip_reason)
    else:
        _trace("generate_initial")
        raw_model_output = _rewrite_email_from_anchor_card(
            profile=profile,
            research_card=research_card,
            anchor_card=anchor_card,
            evidence=evidence,
            selected_prompt=selected_prompt,
            selected_model=selected_model,
            missing_anchors=[],
            previous_core="",
        )
        body = _normalize_email_wrapper(raw_model_output, profile.get("first_name", "there"))
        normalized_output = body
        logger.info("writer_slot_source:%s", slot_source)

        opener_initial = _extract_first_body_paragraph(body)
        _trace("check_anchors")
        missing_anchors = _check_required_anchors(
            body=body,
            profile=profile,
            evidence=evidence,
            research_card=research_card,
            anchor_card=anchor_card,
        )

        if missing_anchors:
            opener_retry_used = True
            slot_retry_used = True
            _trace("retry_missing_anchors", hard_check_issues=missing_anchors)
            raw_retry = _retry_missing_anchors_once(
                profile=profile,
                research_card=research_card,
                anchor_card=anchor_card,
                evidence=evidence,
                selected_prompt=selected_prompt,
                selected_model=selected_model,
                missing_anchors=missing_anchors,
                previous_body=body,
            )
            if raw_retry.strip():
                raw_model_output = raw_retry
                body = _normalize_email_wrapper(raw_retry, profile.get("first_name", "there"))
                normalized_output = body
                patch_actions.append("anchor_retry_applied")
                missing_anchors = _check_required_anchors(
                    body=body,
                    profile=profile,
                    evidence=evidence,
                    research_card=research_card,
                    anchor_card=anchor_card,
                )

        opener_final = _extract_first_body_paragraph(body)
        opener_meta = {
            "sentence": opener_final,
            "source": _pick_source_from_truth(
                str(research_card.get("industry_bucket") or "general"),
                list(research_card.get("source_truth") or []),
            ),
            "persona_lens": str(evidence.get("persona_lens") or ""),
        }
        opener_quality_trace = [{"attempt": 1, "score": 100 if not missing_anchors else 0, "issues": list(missing_anchors), "opener": opener_final}]
        logger.info("opener_retry_used:%s", "true" if opener_retry_used else "false")

        if missing_anchors:
            final_decision = "skipped"
            final_stage = "anchor_failed"
            final_issues = list(missing_anchors)
            dominant_failure_category = _categorize_issues(final_issues)
            skip_reason = dominant_failure_category
            _trace("skip_anchor_failed", hard_check_issues=final_issues, stage_skip_reason=skip_reason)
        else:
            _trace("hard_checks_passed")

    style_score = _intent_similarity_score(body)
    logger.info("style_similarity_score:%.3f", style_score)

    if use_judge and body:
        judge_1 = _judge_naturalness(body, profile, style_score)
        judge_2 = judge_1
        logger.info("judge_result_1:%s", json.dumps(judge_1, ensure_ascii=True))
        logger.info("judge_result_2:%s", json.dumps(judge_2, ensure_ascii=True))
        if not judge_1.get("passes", False):
            _trace("judge_flagged_non_blocking", stage_skip_reason="judge_flagged_non_blocking")

    _trace("final", hard_check_issues=final_issues, stage_skip_reason=skip_reason)

    result = {
        "profile_id": profile.get("apollo_id", ""),
        "email": profile.get("email", ""),
        "company": profile.get("company_name", ""),
        "subject": subject,
        "initial_body": raw_model_output or body,
        "normalized_body": normalized_output or body,
        "final_body": body,
        "style_score": style_score,
        "judge_1": judge_1,
        "judge_2": judge_2,
        "repair_prompt_snippet": "",
        "repair_attempt_applied": bool(patch_actions),
        "collapse_recovery_attempted": False,
        "critical_issues_precheck": [],
        "critical_issues_after": list(final_issues),
        "final_decision": final_decision,
        "final_stage": final_stage,
        "final_issues": list(final_issues),
        "dominant_failure_category": dominant_failure_category,
        "missing_anchors": list(missing_anchors),
        "timeline": timeline,
        "writer_trace": writer_trace,
        "patch_actions": patch_actions,
        "writer_slot_source": slot_source,
        "slot_retry_used": slot_retry_used,
        "opener_retry_used": opener_retry_used,
        "opener_candidates": list(evidence.get("opener_angles") or []),
        "opener_selected": opener_meta.get("sentence", ""),
        "opener_source": opener_meta.get("source", ""),
        "opener_persona": opener_meta.get("persona_lens", ""),
        "opener_initial": opener_initial,
        "opener_retry": opener_retry,
        "opener_final": opener_final or _extract_first_body_paragraph(body),
        "opener_quality_trace": opener_quality_trace,
        "source_truth_trace": source_truth_trace,
        "skip_reason": skip_reason,
        "industry_bucket": research_card.get("industry_bucket", evidence.get("industry_bucket", "general")),
        "evidence_quality": evidence.get("evidence_quality", ""),
        "research_card": research_card,
        "input_anchor_card": anchor_card,
        "raw_model_output": raw_model_output,
        "normalized_output": normalized_output,
    }

    if emit_debug:
        _record_writer_debug(result)

    logger.info("repair_attempt_applied:%s", "true" if patch_actions else "false")
    logger.info("collapse_recovery_attempted:false")
    logger.info("writer_final_decision:%s (%s)", final_decision, final_stage)

    if final_decision == "drafted":
        logger.info(
            "Email written for %s %s at %s (model=%s)",
            profile.get("first_name", ""),
            profile.get("last_name", ""),
            company,
            selected_model,
        )

    return result


def write_emails_batch(
    profiles: list,
    system_prompt: Optional[str] = None,
    model: Optional[str] = None,
) -> list:
    results = []
    stats = {
        "generated": 0,
        "drafted": 0,
        "skipped_by_critical": 0,
        "skipped_by_judge": 0,
        "failed_other": 0,
    }

    for i, profile in enumerate(profiles):
        stats["generated"] += 1
        logger.info(
            "Writing email %d/%d: %s at %s",
            i + 1,
            len(profiles),
            profile.get("first_name", ""),
            profile.get("company_name", ""),
        )
        try:
            email = write_email(profile, system_prompt=system_prompt, model=model)
            results.append(
                {
                    "profile": profile,
                    "subject": email["subject"],
                    "body": email["body"],
                }
            )
            stats["drafted"] += 1
        except WriterValidationError as exc:
            if exc.stage.startswith("judge"):
                stats["skipped_by_judge"] += 1
            else:
                stats["skipped_by_critical"] += 1
            logger.warning(
                "Skipping contact due to writer failure: %s (%s)",
                profile.get("email", "unknown"),
                exc.stage,
            )
        except Exception as exc:
            stats["failed_other"] += 1
            logger.exception("Unexpected writer error for %s: %s", profile.get("email", "unknown"), exc)

    _LAST_BATCH_STATS.update(stats)
    logger.info(
        "writer_batch_summary:generated=%d drafted=%d skipped_by_critical=%d skipped_by_judge=%d failed_other=%d",
        stats["generated"],
        stats["drafted"],
        stats["skipped_by_critical"],
        stats["skipped_by_judge"],
        stats["failed_other"],
    )
    return results


def validate_generated_email(body: str, profile: dict) -> tuple:
    quality = evaluate_email_quality(body, profile)
    issues = list(quality.get("critical_issues", []))
    return len(issues) == 0, issues


def evaluate_email_quality(body: str, profile: dict) -> dict:
    evidence = _get_evidence_pack(profile)
    research_card = _resolve_research_card(profile, evidence)
    anchor_card = _build_intent_anchor_card(profile, research_card)
    normalized = body or ""
    critical = _check_required_anchors(
        body=normalized,
        profile=profile,
        evidence=evidence,
        research_card=research_card,
        anchor_card=anchor_card,
    )
    return {
        "passes": not critical,
        "critical_issues": critical,
        "judge": {"passes": True, "reasons": [], "severity": "low", "suggested_fix": ""},
    }


def debug_select_opener(profile: dict, preferred_text: str = "") -> dict:
    evidence = _get_evidence_pack(profile)
    return _select_opener_strategy(profile, evidence, preferred_text=preferred_text)


def _build_context(profile: dict, short: bool = False) -> str:
    evidence = _get_evidence_pack(profile)

    parts = [
        f"first_name: {profile.get('first_name', '')}",
        f"title: {profile.get('title', '')}",
        f"company_name: {profile.get('company_name', '')}",
        f"industry: {profile.get('company_industry', '')}",
        f"location: {profile.get('company_city', '')}, {profile.get('company_state', '')}",
    ]

    if not short:
        if profile.get("company_description"):
            parts.append(f"company_description: {str(profile.get('company_description'))[:420]}")
        if profile.get("homepage_snippet"):
            parts.append(f"homepage_snippet: {str(profile.get('homepage_snippet'))[:420]}")

    parts.extend(
        [
            f"evidence.proof_points: {' | '.join(evidence.get('proof_points', [])[:3])}",
            f"evidence.why_business_matters_local: {evidence.get('why_business_matters_local', '')}",
            f"evidence.who_they_serve: {evidence.get('who_they_serve', '')}",
            f"evidence.human_thanks_line: {evidence.get('human_thanks_line', '')}",
            f"evidence.discovery_signals: {', '.join(evidence.get('discovery_signals', [])[:3])}",
            f"evidence.social_proof_signals: {' | '.join(evidence.get('social_proof_signals', [])[:3])}",
            f"evidence.opener_angles: {' | '.join(evidence.get('opener_angles', [])[:3])}",
            f"evidence.source_truth: {', '.join(evidence.get('source_truth', [])[:3])}",
            f"evidence.industry_bucket: {evidence.get('industry_bucket', '')}",
            f"evidence.impact_subject: {evidence.get('impact_subject', '')}",
            f"evidence.impact_core: {evidence.get('impact_core', '')}",
            f"evidence.proof_phrase: {evidence.get('proof_phrase', '')}",
            f"evidence.persona_lens: {evidence.get('persona_lens', '')}",
            f"evidence.opener_confidence: {evidence.get('opener_confidence', 0.0)}",
            f"evidence.evidence_quality: {evidence.get('evidence_quality', '')}",
            f"evidence.language_cues: {', '.join(evidence.get('language_cues', [])[:6])}",
            f"evidence.confidence: {evidence.get('confidence', 0.0)}",
        ]
    )
    return "\n".join(parts)


def _resolve_research_card(profile: dict, evidence: dict) -> dict:
    card = profile.get("research_card") or {}
    if not isinstance(card, dict) or not card:
        card = research.build_research_card(profile, evidence_pack=evidence)
    else:
        # Re-normalize to keep card contract stable across callsites.
        card = research.build_research_card(profile, evidence_pack=dict(evidence, **card))

    source_truth = [str(x).strip().lower() for x in (card.get("source_truth") or []) if str(x).strip()]
    source_truth = [s for s in source_truth if s in {"website", "linkedin", "reviews"}]
    if not source_truth:
        source_truth = [str(x).strip().lower() for x in (evidence.get("source_truth") or []) if str(x).strip()]
        source_truth = [s for s in source_truth if s in {"website", "linkedin", "reviews"}]
    if not source_truth:
        source_truth = ["website"]

    industry_bucket = str(card.get("industry_bucket") or evidence.get("industry_bucket") or "general").strip().lower() or "general"
    confidence = float(card.get("confidence") or evidence.get("confidence") or 0.0)
    evidence_quality = str(card.get("evidence_quality") or evidence.get("evidence_quality") or "").strip().lower()
    if evidence_quality not in {"strong", "weak"}:
        evidence_quality = "strong" if confidence >= float(config.EVIDENCE_MIN_CONFIDENCE) else "weak"

    impact_subject = _clean_slot_text(str(card.get("impact_subject") or evidence.get("impact_subject") or evidence.get("who_they_serve") or ""))
    impact_core = _clean_slot_text(str(card.get("impact_core") or evidence.get("impact_core") or ""))
    if not impact_core:
        impact_core = _clean_slot_text(_impact_core_by_business_type(evidence, industry_bucket))
    proof_phrase = _clean_slot_text(str(card.get("proof_phrase") or evidence.get("proof_phrase") or _best_proof_phrase(evidence)))
    meaning_line = _clean_slot_text(str(card.get("meaning_line") or ""))
    if not meaning_line:
        meaning_line = _fallback_meaning_line(impact_core, impact_subject, proof_phrase)

    return {
        "industry_bucket": industry_bucket,
        "source_truth": source_truth,
        "impact_core": impact_core,
        "impact_subject": impact_subject,
        "proof_phrase": proof_phrase,
        "meaning_line": meaning_line,
        "confidence": confidence,
        "evidence_quality": evidence_quality,
    }


def _research_card_is_weak(research_card: dict) -> bool:
    if str(research_card.get("evidence_quality") or "").strip().lower() == "weak":
        return True
    if float(research_card.get("confidence") or 0.0) < float(config.EVIDENCE_MIN_CONFIDENCE):
        return True
    if not str(research_card.get("impact_core") or "").strip():
        return True
    if not str(research_card.get("proof_phrase") or "").strip():
        return True
    if not str(research_card.get("meaning_line") or "").strip():
        return True
    return False


def _fallback_meaning_line(impact_core: str, impact_subject: str, proof_phrase: str) -> str:
    core = _clean_slot_text(str(impact_core or ""))
    subject = _clean_slot_text(str(impact_subject or ""))
    proof = _clean_slot_text(str(proof_phrase or ""))
    if not core:
        return ""
    line = core
    if subject and subject.lower() not in line.lower():
        line = f"{line} for {subject}"
    if proof and proof.lower() not in line.lower():
        line = f"{line}, and {proof.lower()} stood out"
    return _sentenceize(line)


def _build_intent_anchor_card(profile: dict, research_card: dict) -> dict:
    return {
        "base_intent": (
            "Personalized opener from research, then a light web/social question, then a non-assumptive "
            "ideas line with a soft nearby or Zoom next step."
        ),
        "required_ask": "web/social question",
        "required_ask_line": HARD_ASK_LINE,
        "required_non_assumptive_intent": "have ideas but do not want to assume anything",
        "required_soft_close_intent": "nearby this week and can meet briefly or hop on Zoom",
        "required_intents": ["ask_web_social", "non_assumptive", "soft_close"],
        "truth_channels": list(research_card.get("source_truth") or []),
        "business_essence": {
            "impact_core": str(research_card.get("impact_core") or ""),
            "impact_subject": str(research_card.get("impact_subject") or ""),
            "proof_phrase": str(research_card.get("proof_phrase") or ""),
            "meaning_line": str(research_card.get("meaning_line") or ""),
        },
        "word_limit": 100,
        "first_name": str(profile.get("first_name") or "there"),
        "company_name": str(profile.get("company_name") or "your company"),
        "area": str(profile.get("company_city") or profile.get("city") or "the area"),
    }


def _rewrite_email_from_anchor_card(
    profile: dict,
    research_card: dict,
    anchor_card: dict,
    evidence: dict,
    selected_prompt: str,
    selected_model: str,
    missing_anchors: List[str],
    previous_core: str,
) -> str:
    mode = "initial_draft" if not missing_anchors else "anchor_retry"
    source_truth = ",".join(anchor_card.get("truth_channels") or [])
    guidance = (
        "Write the email body core only. Do not include greeting, signoff, subject line, markdown, or bullets. "
        "Use 3 short paragraphs in natural human tone. Keep under 90 words for the core text."
    )
    anchor_requirements = (
        "Required semantic anchors:\n"
        "- Include a direct question that contains 'web/social' and ends with '?'\n"
        "- Include a non-assumptive line about having ideas\n"
        "- Include a soft nearby/Zoom next step\n"
        "- Keep source claims truthful to source_truth only"
    )
    retry_note = ""
    if missing_anchors:
        retry_note = (
            f"Missing anchors from previous draft: {', '.join(missing_anchors)}.\n"
            "Edit only enough to satisfy missing anchors while keeping the same natural flow.\n"
            f"Previous draft core:\n{previous_core}\n"
        )

    prompt = (
        f"{selected_prompt}\n\n"
        f"{guidance}\n{anchor_requirements}\n\n"
        f"Mode: {mode}\n"
        f"source_truth: {source_truth}\n"
        f"industry_bucket: {research_card.get('industry_bucket','general')}\n"
        f"meaning_line: {research_card.get('meaning_line','')}\n"
        f"impact_core: {research_card.get('impact_core','')}\n"
        f"impact_subject: {research_card.get('impact_subject','')}\n"
        f"proof_phrase: {research_card.get('proof_phrase','')}\n"
        f"ask anchor: {anchor_card.get('required_ask_line', HARD_ASK_LINE)}\n"
        f"non-assumptive anchor: {anchor_card.get('required_non_assumptive_intent','')}\n"
        f"soft-close anchor: {anchor_card.get('required_soft_close_intent','')}\n"
        f"{retry_note}"
    )
    try:
        response = client.chat.completions.create(
            model=selected_model,
            messages=[
                {"role": "system", "content": "You rewrite outreach emails with natural tone and strict semantic anchors."},
                {"role": "user", "content": prompt},
            ],
            max_completion_tokens=240,
        )
        return (response.choices[0].message.content or "").strip()
    except Exception as exc:
        logger.info("rewrite_email_from_anchor_card_error:%s:%s", profile.get("email", ""), exc)
        return ""


def _normalize_email_wrapper(core_text: str, first_name: str) -> str:
    raw = (core_text or "").replace("\r\n", "\n").replace("\r", "\n")
    raw = raw.replace("\u2014", ",").replace("\u2013", ",")
    lines = []
    for line in raw.split("\n"):
        item = line.strip()
        if not item:
            lines.append("")
            continue
        if re.match(r"^subject\s*:", item, flags=re.IGNORECASE):
            continue
        if re.match(r"^hi\s+[A-Za-z][A-Za-z' -]{0,40},?$", item, flags=re.IGNORECASE):
            continue
        if re.match(r"^(best|cheers)\s*,?$", item, flags=re.IGNORECASE):
            continue
        lines.append(item)

    cleaned = "\n".join(lines)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
    if not cleaned:
        cleaned = "I wanted to introduce myself with a quick idea based on what I saw."
    paragraphs = [re.sub(r"\s+", " ", p).strip() for p in re.split(r"\n\s*\n", cleaned) if re.sub(r"\s+", " ", p).strip()]
    core = "\n\n".join(paragraphs).strip()
    first = (first_name or "there").strip() or "there"
    return f"Hi {first},\n\n{core}\n\nBest,"


def _check_required_anchors(
    body: str,
    profile: dict,
    evidence: dict,
    research_card: dict,
    anchor_card: dict,
) -> List[str]:
    issues = _minimal_checks(body, profile, evidence, research_card=research_card)
    text = (body or "").lower()
    core = _strip_signature_lines(re.sub(r"^\s*Hi\s+[^\n,]+,\s*", "", body or "", flags=re.IGNORECASE))
    if not _has_non_assumptive_intent(text):
        issues.append("missing_non_assumptive_intent")
    if not _has_soft_close_intent(text):
        issues.append("missing_soft_close_intent")
    if not _mentions_business_essence(core, research_card):
        issues.append("missing_business_essence")
    # preserve order and de-dupe
    seen = set()
    ordered = []
    for issue in issues:
        if issue in seen:
            continue
        seen.add(issue)
        ordered.append(issue)
    return ordered


def _retry_missing_anchors_once(
    profile: dict,
    research_card: dict,
    anchor_card: dict,
    evidence: dict,
    selected_prompt: str,
    selected_model: str,
    missing_anchors: List[str],
    previous_body: str,
) -> str:
    core = _extract_body_core(previous_body)
    return _rewrite_email_from_anchor_card(
        profile=profile,
        research_card=research_card,
        anchor_card=anchor_card,
        evidence=evidence,
        selected_prompt=selected_prompt,
        selected_model=selected_model,
        missing_anchors=missing_anchors,
        previous_core=core,
    )


def _extract_body_core(body: str) -> str:
    text = (body or "").strip()
    text = re.sub(r"^\s*Hi\s+[^\n,]+,\s*\n?", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\n?\s*(Best|Cheers)\s*,\s*$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _has_non_assumptive_intent(text: str) -> bool:
    patterns = [
        r"\bdid not want to assume\b",
        r"\bdidn't want to assume\b",
        r"\bdo not want to assume\b",
        r"\bdon't want to assume\b",
        r"\bnot .*assum",
        r"\bhave .*ideas.*assum",
    ]
    return any(re.search(p, text, flags=re.IGNORECASE) for p in patterns)


def _has_soft_close_intent(text: str) -> bool:
    nearby = bool(re.search(r"\b(nearby|in the area|around|this week|later this week|neighborhood)\b", text, flags=re.IGNORECASE))
    meet = bool(re.search(r"\b(meet|stop by|swing by|drop by|briefly|quick hello|introduce myself)\b", text, flags=re.IGNORECASE))
    zoom = bool(re.search(r"\bzoom\b", text, flags=re.IGNORECASE))
    return nearby and (meet or zoom)


def _mentions_business_essence(core_text: str, research_card: dict) -> bool:
    text = (core_text or "").lower()
    targets = [
        str(research_card.get("meaning_line") or ""),
        str(research_card.get("impact_core") or ""),
        str(research_card.get("impact_subject") or ""),
        str(research_card.get("proof_phrase") or ""),
    ]
    for target in targets:
        clean = re.sub(r"\s+", " ", target.lower()).strip()
        if not clean:
            continue
        tokens = [t for t in re.findall(r"[a-zA-Z][a-zA-Z0-9\-]{3,}", clean) if t not in {"with", "that", "your", "team", "company", "local", "area", "people"}]
        if not tokens:
            continue
        overlap = sum(1 for t in tokens if t in text)
        if overlap >= 2:
            return True
    return False


def _generate_slots_once(
    profile: dict,
    research_card: dict,
    evidence: dict,
    selected_prompt: str,
    selected_model: str,
) -> dict:
    context = {
        "first_name": profile.get("first_name", ""),
        "company_name": profile.get("company_name", ""),
        "company_city": profile.get("company_city", ""),
        "company_state": profile.get("company_state", ""),
        "research_card": research_card,
        "proof_points": list(evidence.get("proof_points") or [])[:3],
        "social_proof_signals": list(evidence.get("social_proof_signals") or [])[:3],
        "human_thanks_line": evidence.get("human_thanks_line", ""),
    }
    system = (
        f"{selected_prompt}\n\n"
        "Write a short outreach email as JSON with keys only: p1_observation, p2_ask, p3_next_step, ps_impact.\n"
        "Rules:\n"
        "- No greeting and no signoff in fields.\n"
        "- Keep total body under 100 words once rendered.\n"
        "- p1_observation: truthful, specific, calm opener from research.\n"
        "- p2_ask: direct light question that includes 'web/social' and ends with '?'.\n"
        "- p3_next_step: mention you have ideas without assuming + nearby/Zoom soft next step.\n"
        "- ps_impact: optional, only if evidence feels strong.\n"
        "- No subject line. No markdown."
    )
    try:
        response = client.chat.completions.create(
            model=selected_model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": json.dumps(context, ensure_ascii=True)},
            ],
            max_completion_tokens=260,
            response_format={"type": "json_object"},
        )
    except Exception as exc:
        logger.info("writer_slot_generation_error:%s", exc)
        return {}

    raw = (response.choices[0].message.content or "").strip()
    parsed = _safe_parse_json_object(raw) or {}
    return parsed if isinstance(parsed, dict) else {}


def _fallback_slots_from_research_card(profile: dict, research_card: dict, evidence: dict) -> dict:
    opener_context = _build_opener_context_from_research_card(
        profile=profile,
        research_card=research_card,
        evidence=evidence,
    )
    p1 = _compose_opener_from_context(opener_context) or _fallback_observation_line(profile, evidence)
    p3 = (
        "I had a couple ideas for you guys, but I did not want to assume anything, so I thought I would introduce myself first. "
        "I am in the area most of this week if you would prefer to meet briefly, otherwise I am happy to hop on Zoom as well."
    )
    ps = str(evidence.get("human_thanks_line") or evidence.get("human_thanks") or "").strip()
    return {
        "p1_observation": _sentenceize(p1),
        "p2_ask": _pick_ask_variant(profile),
        "p3_next_step": _sentenceize(p3),
        "ps_impact": _sentenceize(ps) if ps else "",
    }


def _compose_email_from_research_card(
    profile: dict,
    research_card: dict,
    evidence: dict,
    slots: dict,
    selected_model: str,
) -> Tuple[str, dict]:
    first_name = (profile.get("first_name") or "there").strip() or "there"
    opener_context = _build_opener_context_from_research_card(
        profile=profile,
        research_card=research_card,
        evidence=evidence,
    )

    p1_candidate = _sentenceize(_clean_slot_text(str((slots or {}).get("p1_observation") or "")))
    if p1_candidate and not _claims_unverified_visit(p1_candidate, evidence) and not _is_weak_generic_opener(p1_candidate):
        p1 = p1_candidate
    else:
        p1 = _compose_opener_from_context(opener_context) or _fallback_observation_line(profile, evidence)

    p2 = _normalize_ask_line(str((slots or {}).get("p2_ask") or ""), profile=profile)

    p3 = _sentenceize(_clean_slot_text(str((slots or {}).get("p3_next_step") or "")))
    if not p3:
        p3 = (
            "I had a couple ideas for you guys, but I did not want to assume anything, so I thought I would introduce myself first. "
            "I am in the area most of this week if you would prefer to meet briefly, otherwise I am happy to hop on Zoom as well."
        )
    if "assum" not in p3.lower():
        p3 = p3.rstrip(".") + ". I had a couple ideas for you guys, but I did not want to assume anything."
    if ("zoom" not in p3.lower()) and ("meet" not in p3.lower()) and ("stop by" not in p3.lower()):
        p3 = p3.rstrip(".") + " I am in the area most of this week if you would prefer to meet briefly, otherwise I am happy to hop on Zoom as well."
    p3 = _sentenceize(p3)

    ps = ""
    if _should_include_ps(evidence):
        ps_candidate = _sentenceize(_clean_slot_text(str((slots or {}).get("ps_impact") or "")))
        if ps_candidate:
            ps = ps_candidate
        else:
            raw_ps = str(evidence.get("human_thanks_line") or evidence.get("human_thanks") or "").strip()
            ps = _sentenceize(raw_ps) if raw_ps else ""

    p1, p2, p3, ps = _fit_components_to_limit(first_name, p1, p2, p3, ps)
    body = _rebuild_email(first_name, p1, p2, p3, ps)
    return body, {
        "sentence": p1,
        "source": _pick_source_from_truth(str(research_card.get("industry_bucket") or "general"), list(research_card.get("source_truth") or [])),
        "persona_lens": str(evidence.get("persona_lens") or ""),
    }


def _build_opener_context_from_research_card(
    profile: dict,
    research_card: dict,
    evidence: dict,
    preferred_source: str = "",
) -> dict:
    source_truth = [str(x).strip().lower() for x in (research_card.get("source_truth") or evidence.get("source_truth") or []) if str(x).strip()]
    source_truth = [s for s in source_truth if s in {"website", "linkedin", "reviews"}]
    if not source_truth:
        source_truth = ["website"]

    impact_core = _clean_slot_text(str(research_card.get("impact_core") or evidence.get("impact_core") or ""))
    if not impact_core:
        impact_core = _clean_slot_text(_impact_core_by_business_type(evidence, str(research_card.get("industry_bucket") or "general")))
    impact_subject = _clean_slot_text(str(research_card.get("impact_subject") or evidence.get("impact_subject") or evidence.get("who_they_serve") or ""))
    proof_phrase = _clean_slot_text(str(research_card.get("proof_phrase") or evidence.get("proof_phrase") or _best_proof_phrase(evidence)))

    return {
        "industry_bucket": str(research_card.get("industry_bucket") or evidence.get("industry_bucket") or "general"),
        "source_truth": source_truth,
        "impact_core": impact_core,
        "impact_subject": impact_subject,
        "proof_phrase": proof_phrase,
        "area": (profile.get("company_city") or profile.get("city") or "the area").strip(),
        "company": (profile.get("company_name") or "your company").strip(),
        "persona_lens": str(evidence.get("persona_lens") or ""),
        "preferred_source": str(preferred_source or "").strip().lower(),
    }


def _generate_slots(profile: dict, selected_prompt: str, selected_model: str) -> Tuple[dict, str, bool]:
    full_context = _build_context(profile, short=False)
    slots = _call_slots_model(profile, selected_prompt, selected_model, full_context, max_tokens=360)
    if _slots_valid(slots):
        return slots, "model", False

    short_context = _build_context(profile, short=True)
    retry_slots = _call_slots_model(profile, selected_prompt, selected_model, short_context, max_tokens=240)
    if _slots_valid(retry_slots):
        return retry_slots, "model", True

    return _fallback_slots_from_evidence(profile), "fallback", True


def _call_slots_model(
    profile: dict,
    selected_prompt: str,
    selected_model: str,
    context: str,
    max_tokens: int,
) -> dict:
    system = (
        f"{selected_prompt}\n\n"
        "Rewrite this outreach template for one specific target using provided research.\n"
        "Return strict JSON object only with keys:\n"
        "p1_observation, p2_ask, p3_next_step, ps_impact.\n"
        "Rules:\n"
        "- No greeting and no signoff in fields.\n"
        "- p1_observation: personalized reason for reaching out tied to research.\n"
        "- p2_ask: a light direct question that includes web/social intent and ends with '?'.\n"
        "- p3_next_step: ideas without assuming + nearby/Zoom soft next step.\n"
        "- ps_impact: optional sincere line tied to business/community impact.\n"
        "- Do not claim you visited in person unless context explicitly supports it.\n"
        "- No markdown. No Subject line."
    )
    try:
        response = client.chat.completions.create(
            model=selected_model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": context},
            ],
            max_completion_tokens=max_tokens,
            response_format={"type": "json_object"},
        )
    except Exception as exc:
        logger.warning("slots_model_error:%s:%s", profile.get("email", ""), exc)
        return {}

    raw = (response.choices[0].message.content or "").strip()
    parsed = _safe_parse_json_object(raw) or {}
    return parsed if isinstance(parsed, dict) else {}


def _slots_valid(slots: dict) -> bool:
    if not isinstance(slots, dict):
        return False
    p1 = str(slots.get("p1_observation") or "").strip()
    p3 = str(slots.get("p3_next_step") or "").strip()
    return _word_count(p1) >= 6 and _word_count(p3) >= 8


def _render_template(profile: dict, slots: dict, include_ps: bool) -> Tuple[str, dict]:
    first_name = (profile.get("first_name") or "there").strip() or "there"
    evidence = _get_evidence_pack(profile)
    opener_meta = _select_opener_strategy(profile, evidence, preferred_text=str(slots.get("p1_observation") or ""))

    p1 = _sentenceize(_clean_slot_text(str(slots.get("p1_observation") or "")))
    p2 = _normalize_ask_line(str(slots.get("p2_ask") or ""))
    p3 = _sentenceize(_clean_slot_text(str(slots.get("p3_next_step") or "")))
    ps = _sentenceize(_clean_slot_text(str(slots.get("ps_impact") or ""))) if include_ps else ""

    if (not p1) or _observation_needs_repair(p1) or _is_weak_generic_opener(p1) or _claims_unverified_visit(p1, evidence):
        p1 = opener_meta.get("sentence") or _fallback_observation_line(profile, evidence)
    else:
        # Keep model opener if valid, but still normalize punctuation.
        p1 = _sentenceize(p1)
    if not p3:
        p3 = _fallback_slots_from_evidence(profile)["p3_next_step"]

    if "assum" not in p3.lower():
        p3 = p3.rstrip(".") + ". I had a couple ideas, but I did not want to assume anything."
    if "zoom" not in p3.lower() and "meet" not in p3.lower() and "stop by" not in p3.lower():
        p3 = p3.rstrip(".") + " I am in the area this week and can meet briefly, or we can hop on Zoom."

    # Keep intent consistent and avoid drift from the hard ask.
    p2 = _normalize_ask_line(p2, profile=profile)

    p1, p2, p3, ps = _fit_components_to_limit(first_name, p1, p2, p3, ps)
    return _rebuild_email(first_name, p1, p2, p3, ps), opener_meta


def _fit_components_to_limit(first_name: str, p1: str, p2: str, p3: str, ps: str) -> Tuple[str, str, str, str]:
    while _word_count(_strip_signature_lines(_rebuild_email(first_name, p1, p2, p3, ps))) > 100:
        if ps:
            ps = ""
            continue
        if _word_count(p3) > 24:
            p3 = _shorten_text(p3, 24)
            continue
        if _word_count(p1) > 22:
            p1 = _shorten_text(p1, 22)
            continue
        if _word_count(p3) > 18:
            p3 = _shorten_text(p3, 18)
            continue
        if _word_count(p1) > 16:
            p1 = _shorten_text(p1, 16)
            continue
        # last resort: keep mandatory ask and trim p3 further
        if _word_count(p3) > 12:
            p3 = _shorten_text(p3, 12)
            continue
        break
    return p1, p2, p3, ps


def _shorten_text(text: str, max_words: int) -> str:
    words = re.findall(r"\S+", text or "")
    if len(words) <= max_words:
        return text
    shortened = " ".join(words[:max_words]).rstrip(",;:-")
    return _sentenceize(shortened)


def _rebuild_email(first_name: str, p1: str, p2: str, p3: str, ps: str) -> str:
    sections = [
        f"Hi {first_name},",
        "",
        p1,
        "",
        p2,
        "",
        p3,
    ]
    if ps:
        sections.extend(["", f"P.S. {ps}"])
    sections.extend(["", "Best,"])
    return "\n".join(sections).strip()


def _normalize_ask_line(text: str, profile: Optional[dict] = None) -> str:
    candidate = _sentenceize(_clean_slot_text(text))
    if ("web/social" in candidate.lower()) and candidate.endswith("?"):
        return candidate
    variant = _pick_ask_variant(profile or {})
    return variant


def _pick_ask_variant(profile: dict) -> str:
    key = "|".join(
        [
            str(profile.get("email") or ""),
            str(profile.get("company_name") or ""),
            str(profile.get("first_name") or ""),
        ]
    )
    if not key.strip():
        return HARD_ASK_LINE
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    idx = int(digest[:8], 16) % len(ASK_VARIANTS)
    return ASK_VARIANTS[idx]


def _clean_slot_text(text: str) -> str:
    cleaned = (text or "").replace("\u2014", ",").replace("\u2013", ",")
    cleaned = re.sub(r"\r\n?", "\n", cleaned)
    cleaned = re.sub(r"^\s*subject\s*:.*$", "", cleaned, flags=re.IGNORECASE | re.MULTILINE)
    cleaned = re.sub(r"^\s*(best|cheers)\s*,\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"^\s*hi\s+[^\n,]+,\s*$", "", cleaned, flags=re.IGNORECASE | re.MULTILINE)
    cleaned = re.sub(r"^\s*(best|cheers),\s*$", "", cleaned, flags=re.IGNORECASE | re.MULTILINE)
    cleaned = re.sub(r"\b(cheers|best)\s*,?\s*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bhi\s+[A-Za-z][A-Za-z' -]{0,40},\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\n+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip(" ,")


def _sentenceize(text: str) -> str:
    sentence = re.sub(r"\s+", " ", (text or "")).strip()
    if not sentence:
        return ""
    if sentence[-1] not in ".!?":
        sentence += "."
    return sentence


def _deterministic_correction(body: str, profile: dict, evidence: dict, slots: dict) -> Tuple[str, List[str]]:
    actions: List[str] = []
    issues = _minimal_checks(body, profile, evidence)

    working = dict(slots or {})

    if "missing_or_wrong_ask_line" in issues:
        working["p2_ask"] = _pick_ask_variant(profile)
        actions.append("enforced_hard_ask")

    if "empty_body" in issues or "collapsed_body" in issues:
        fallback = _fallback_slots_from_evidence(profile)
        working["p1_observation"] = fallback["p1_observation"]
        working["p2_ask"] = fallback["p2_ask"]
        working["p3_next_step"] = fallback["p3_next_step"]
        working["ps_impact"] = fallback.get("ps_impact", "")
        actions.append("collapse_fallback_slots")

    corrected, _ = _render_template(profile, working, include_ps=_should_include_ps(evidence))

    return corrected, actions


def _fallback_slots_from_evidence(profile: dict) -> dict:
    evidence = _get_evidence_pack(profile)

    p1 = _fallback_observation_line(profile, evidence)
    p3 = (
        "I had a couple ideas for you guys, but I did not want to assume anything, "
        "so I thought I would introduce myself first. "
        "I am in the area most of this week if you would prefer to meet briefly, "
        "otherwise I am happy to hop on Zoom as well."
    )

    ps_impact = evidence.get("human_thanks_line") or evidence.get("human_thanks") or ""

    return {
        "p1_observation": _sentenceize(p1),
        "p2_ask": _pick_ask_variant(profile),
        "p3_next_step": _sentenceize(p3),
        "ps_impact": _sentenceize(ps_impact) if ps_impact else "",
    }


def _best_proof_phrase(evidence: dict) -> str:
    for candidate in evidence.get("proof_points") or []:
        text = re.sub(r"\s+", " ", str(candidate)).strip().rstrip(".")
        if text:
            return text
    fallback = evidence.get("why_business_matters_local") or evidence.get("who_they_serve") or "the way you serve your customers"
    return str(fallback).strip().rstrip(".")


def _fallback_observation_line(profile: dict, evidence: dict) -> str:
    opener_meta = _select_opener_strategy(profile, evidence)
    sentence = opener_meta.get("sentence") or ""
    if sentence:
        return _sentenceize(sentence)
    proof = _best_proof_phrase(evidence)
    clause = _proof_to_observation_clause(proof)
    return _sentenceize(f"I was looking into your company and noticed {clause}.")


def _build_opener_context(profile: dict, evidence: dict, opener_meta: Optional[dict] = None) -> dict:
    company = (profile.get("company_name") or "your company").strip()
    area = (profile.get("company_city") or profile.get("city") or "the area").strip()
    industry_bucket = str(evidence.get("industry_bucket") or evidence.get("business_type") or "general").strip().lower()
    source_truth = [str(x).strip().lower() for x in (evidence.get("source_truth") or evidence.get("discovery_signals") or []) if str(x).strip()]
    source_truth = [s for s in source_truth if s in {"website", "linkedin", "reviews"}]
    if not source_truth:
        source_truth = _augment_discovery_with_profile(profile, [])
        source_truth = [s for s in source_truth if s in {"website", "linkedin", "reviews"}]
    if not source_truth:
        source_truth = ["website"]

    impact_core = _clean_slot_text(str(evidence.get("impact_core") or ""))
    impact_subject = _clean_slot_text(str(evidence.get("impact_subject") or evidence.get("who_they_serve") or ""))
    proof_phrase = _clean_slot_text(str(evidence.get("proof_phrase") or ""))
    if not proof_phrase:
        proof_phrase = _clean_slot_text(_best_proof_phrase(evidence))

    return {
        "industry_bucket": industry_bucket or "general",
        "source_truth": source_truth,
        "impact_core": impact_core,
        "impact_subject": impact_subject,
        "proof_phrase": proof_phrase,
        "area": area,
        "company": company,
        "persona_lens": str(evidence.get("persona_lens") or ""),
        "preferred_source": str((opener_meta or {}).get("source") or "").strip().lower(),
    }


def _compose_opener_from_context(context: dict) -> str:
    industry = str(context.get("industry_bucket") or "general").lower()
    source_truth = list(context.get("source_truth") or [])
    impact_core = _clean_slot_text(str(context.get("impact_core") or ""))
    impact_subject = _clean_slot_text(str(context.get("impact_subject") or ""))
    proof_phrase = _clean_slot_text(str(context.get("proof_phrase") or ""))
    area = str(context.get("area") or "the area").strip()
    company = str(context.get("company") or "your company").strip()
    preferred_source = str(context.get("preferred_source") or "").strip().lower()

    source = preferred_source if preferred_source in source_truth else ""
    if not source:
        source = _pick_source_from_truth(industry, source_truth)

    lead = "I was looking into your company"
    if source == "linkedin":
        if industry == "saas":
            lead = "You guys popped up on my LinkedIn"
        else:
            lead = "I came across your company on LinkedIn"
    elif source == "reviews":
        if industry == "restaurant":
            lead = f"I was looking at places around {area}"
        else:
            lead = f"I was reading what people say about {company}"
    elif source == "website":
        if industry == "restaurant":
            lead = f"I was looking at places around {area}"
        else:
            lead = "I spent a few minutes on your site"

    core = impact_core or "you seem to be doing solid work for people in the area"
    if impact_subject:
        if impact_subject.lower() not in core.lower():
            core = f"{core} for {impact_subject}"
    sentence_1 = _sentenceize(f"{lead}, and what stood out is that {core}.")

    if proof_phrase and _word_count(proof_phrase) >= 3 and not _is_generic_proof_phrase(proof_phrase):
        sentence_2 = _sentenceize(f"I noticed {proof_phrase}.")
        combined = f"{sentence_1} {sentence_2}"
    else:
        combined = sentence_1
    return _sentenceize(combined)


def _compose_opener_with_model(context: dict, selected_model: str) -> str:
    source_truth = ", ".join(context.get("source_truth") or [])
    prompt = (
        "Write one calm, natural cold-email opener sentence (max 35 words). "
        "No hype, no marketing tone, no fluff. "
        "Truthfulness rule: mention only discovery channels listed in source_truth. "
        "Use this structure: discovery lead + why they matter + one concrete anchor.\n\n"
        f"industry_bucket: {context.get('industry_bucket','general')}\n"
        f"source_truth: {source_truth}\n"
        f"impact_core: {context.get('impact_core','')}\n"
        f"impact_subject: {context.get('impact_subject','')}\n"
        f"proof_phrase: {context.get('proof_phrase','')}\n"
        f"area: {context.get('area','')}\n"
        f"company: {context.get('company','')}\n"
        "Return only the opener sentence."
    )
    try:
        response = client.chat.completions.create(
            model=selected_model,
            messages=[
                {"role": "system", "content": "You write grounded, natural outreach openers."},
                {"role": "user", "content": prompt},
            ],
            max_completion_tokens=90,
        )
        raw = _clean_slot_text((response.choices[0].message.content or "").strip())
        return _sentenceize(raw)
    except Exception as exc:
        logger.info("opener_model_error:%s", exc)
        return ""


def _retry_opener_once(context: dict, issues: List[str], selected_model: str) -> str:
    opener = _compose_opener_with_model(context, selected_model)
    if not opener:
        opener = _compose_opener_from_context(context)
    return _sentenceize(opener)


def _score_opener_quality(opener: str, context: dict) -> dict:
    text = _sentenceize(opener or "")
    lower = text.lower()
    issues: List[str] = []
    score = 100

    if _is_weak_generic_opener(text) or any(
        phrase in lower
        for phrase in [
            "solid work for people in the area",
            "do useful work people in the area rely on",
            "local business community",
            "create a local spot people keep coming back to",
            "pretty neat",
        ]
    ):
        issues.append("generic_opener")
        score -= 35

    source_truth = [str(x).strip().lower() for x in (context.get("source_truth") or []) if str(x).strip()]
    if "linkedin" in lower and "linkedin" not in source_truth:
        issues.append("source_claim_untrue")
        score -= 30
    if any(tok in lower for tok in ["site", "website", "page"]) and "website" not in source_truth:
        issues.append("source_claim_untrue")
        score -= 30
    if any(tok in lower for tok in ["review", "reviews", "what people say"]) and "reviews" not in source_truth:
        issues.append("source_claim_untrue")
        score -= 30

    impact_core = str(context.get("impact_core") or "")
    if not impact_core.strip():
        issues.append("no_meaning_signal")
        score -= 25
    elif _token_overlap(lower, impact_core.lower()) < 2:
        issues.append("no_meaning_signal")
        score -= 25

    proof_phrase = str(context.get("proof_phrase") or "")
    if not proof_phrase.strip() or _is_generic_proof_phrase(proof_phrase):
        issues.append("no_concrete_anchor")
        score -= 20
    elif _token_overlap(lower, proof_phrase.lower()) < 1:
        issues.append("no_concrete_anchor")
        score -= 20

    if re.search(
        r"\b(cutting-edge|world-class|transformative|game-changing|revolutionize|drive growth|maximize|optimize)\b",
        lower,
    ):
        issues.append("tone_too_marketing")
        score -= 20

    # Deduplicate while preserving order
    deduped = []
    seen = set()
    for issue in issues:
        if issue in seen:
            continue
        seen.add(issue)
        deduped.append(issue)

    quality_score = max(0, min(100, int(score)))
    return {
        "passes": quality_score >= 70,
        "quality_score": quality_score,
        "issues": deduped,
    }


def _token_overlap(a: str, b: str) -> int:
    a_tokens = set(re.findall(r"[a-zA-Z][a-zA-Z0-9\-]{3,}", a or ""))
    b_tokens = set(re.findall(r"[a-zA-Z][a-zA-Z0-9\-]{3,}", b or ""))
    if not a_tokens or not b_tokens:
        return 0
    stop = {"with", "from", "that", "this", "your", "have", "will", "team", "company", "business", "local", "area"}
    a_tokens = {t for t in a_tokens if t not in stop}
    b_tokens = {t for t in b_tokens if t not in stop}
    return len(a_tokens & b_tokens)


def _is_generic_proof_phrase(text: str) -> bool:
    lower = re.sub(r"\s+", " ", str(text or "").strip().lower())
    generic_patterns = [
        "local business community",
        "team contributes",
        "solid work",
        "work people in the area rely on",
        "serves people in the area",
    ]
    return any(pattern in lower for pattern in generic_patterns)


def _pick_source_from_truth(industry: str, source_truth: List[str]) -> str:
    if industry == "saas" and "linkedin" in source_truth:
        return "linkedin"
    if "website" in source_truth:
        return "website"
    if "linkedin" in source_truth:
        return "linkedin"
    if "reviews" in source_truth:
        return "reviews"
    return "website"


def _replace_first_body_paragraph(body: str, new_p1: str, profile: dict) -> str:
    original = body or ""
    p1 = _sentenceize(_clean_slot_text(new_p1))
    if not p1 or not original.strip():
        return original

    # Preferred path: replace paragraph immediately following greeting.
    pattern = re.compile(r"^(Hi[^\n]*,\s*\n\s*\n)(.*?)(\n\s*\n.*)$", flags=re.IGNORECASE | re.DOTALL)
    match = pattern.match(original)
    if match:
        return f"{match.group(1)}{p1}{match.group(3)}".strip()

    # Fallback: replace first paragraph-like block.
    parts = [p.strip() for p in re.split(r"\n\s*\n", original) if p.strip()]
    if not parts:
        return original
    idx = 1 if parts[0].lower().startswith("hi ") and len(parts) > 1 else 0
    parts[idx] = p1
    return "\n\n".join(parts).strip()


def _select_opener_strategy(profile: dict, evidence: dict, preferred_text: str = "") -> dict:
    company = (profile.get("company_name") or "your company").strip()
    area = (profile.get("company_city") or profile.get("city") or "the area").strip()
    business_type = str(evidence.get("business_type") or "general")
    persona_lens = str(evidence.get("persona_lens") or _default_persona_lens(business_type))
    discovery = [str(x).strip().lower() for x in (evidence.get("discovery_signals") or []) if str(x).strip()]
    discovery = _augment_discovery_with_profile(profile, discovery)
    proof_points = [str(x).strip() for x in (evidence.get("proof_points") or []) if str(x).strip()]
    opener_angles = [str(x).strip() for x in (evidence.get("opener_angles") or []) if str(x).strip()]
    social_proof = [str(x).strip() for x in (evidence.get("social_proof_signals") or []) if str(x).strip()]

    preferred = _clean_slot_text(preferred_text)
    can_use_preferred = bool(preferred) and not _is_weak_generic_opener(preferred)
    can_use_preferred = can_use_preferred and not _looks_like_full_opener_sentence(preferred)
    can_use_preferred = can_use_preferred and not _is_metric_heavy_snippet(preferred)

    angle = ""
    angle_source = "fallback"
    if can_use_preferred:
        angle = preferred
        angle_source = "preferred"
    elif proof_points:
        angle = proof_points[0]
        angle_source = "proof_points"
    elif opener_angles:
        angle = opener_angles[0]
        angle_source = "opener_angles"
    elif social_proof:
        angle = social_proof[0]
        angle_source = "social_proof"
    else:
        angle = _best_proof_phrase(evidence)
        angle_source = "fallback"

    angle = _sanitize_angle_for_observation(angle, company)
    if not angle:
        angle = _sanitize_angle_for_observation(_best_proof_phrase(evidence), company)
    if not angle:
        angle = "the way your team serves people"

    if angle_source == "social_proof":
        source = "reviews"
    elif angle_source == "proof_points":
        source = _best_discovery_source(discovery, default="website")
    elif angle_source == "opener_angles":
        source = _best_discovery_source(discovery, default="website")
    elif angle_source == "preferred":
        source = _infer_source_from_text(preferred, discovery)
    else:
        source = _best_discovery_source(discovery, default="general")

    observation = _proof_to_observation_clause(angle)
    observation = _strip_redundant_area_phrase(observation, area)

    opener_context = _build_opener_context(
        profile=profile,
        evidence=evidence,
        opener_meta={"source": source},
    )
    if not opener_context.get("impact_core"):
        opener_context["impact_core"] = _impact_core_by_business_type(evidence, business_type)
    if not opener_context.get("proof_phrase"):
        opener_context["proof_phrase"] = _sanitize_angle_for_observation(angle, company) or observation
    sentence = _compose_opener_from_context(opener_context)
    if not sentence:
        if source == "linkedin":
            lower_obs = observation.lower()
            if lower_obs.startswith(("your ", "the ", "a ", "an ")):
                sentence = f"I came across your company on LinkedIn and looked into {observation}."
            elif lower_obs.startswith("you "):
                sentence = f"I came across your company on LinkedIn and noticed {observation}."
            else:
                sentence = f"I came across your company on LinkedIn and looked into how {observation}."
        elif source == "reviews":
            theme = _humanize_review_theme(angle, business_type, evidence)
            if business_type == "restaurant":
                sentence = f"I was looking at places around {area} and kept seeing people mention {theme}."
            elif business_type == "saas":
                sentence = f"I was looking into tools in this space and kept seeing people mention {theme}."
            else:
                sentence = f"I was reading what people say about {company} and kept seeing {theme} come up."
        elif source == "website":
            if business_type == "restaurant":
                sentence = f"I was looking at spots around {area} and noticed {observation}."
            else:
                sentence = f"I spent a few minutes on your site and noticed {observation}."
        else:
            sentence = f"I was looking into {company} and noticed {observation}."

    sentence = _sentenceize(sentence)

    return {
        "source": source,
        "opener_angle": angle,
        "persona_lens": persona_lens,
        "sentence": _sentenceize(sentence),
        "discovery_signals": discovery,
    }


def _proof_to_observation_clause(proof: str) -> str:
    phrase = re.sub(r"\s+", " ", (proof or "")).strip().strip(",").rstrip(".")
    if not phrase:
        return "the way you serve your customers"
    phrase = _sanitize_angle_for_observation(phrase)
    if not phrase:
        return "the way your team serves customers"
    phrase = re.sub(r"^in [^,]+,\s*", "", phrase, flags=re.IGNORECASE)

    lower = phrase.lower()
    if lower.startswith("that "):
        phrase = phrase[5:].strip()
        lower = phrase.lower()
    if lower.startswith("you are known for "):
        return phrase
    if lower.startswith("you "):
        return phrase
    if lower.startswith("your "):
        return phrase
    if lower.startswith("people mention "):
        return "people mention " + phrase[len("people mention "):].strip()

    if re.match(r"^(located|based)\b", lower):
        return f"you are {phrase}"
    if re.match(r"^(is|are|was|were)\b", lower):
        return f"you {phrase}"
    first_word_match = re.match(r"^([a-z]+)\b", lower)
    if first_word_match:
        first = first_word_match.group(1)
        replacements = {
            "provides": "provide",
            "offers": "offer",
            "serves": "serve",
            "supports": "support",
            "helps": "help",
            "focuses": "focus",
            "specializes": "specialize",
            "builds": "build",
            "creates": "create",
            "delivers": "deliver",
            "runs": "run",
        }
        if first in replacements:
            phrase = replacements[first] + phrase[len(first):]
            lower = phrase.lower()
        if re.match(
            r"^(provide|providing|offer|offering|serve|serving|support|supporting|help|helping|focus|focusing|specialize|specializing|build|building|create|creating|deliver|delivering|run|running)\b",
            lower,
        ):
            return f"you {phrase}"
    if re.match(r"^(a|an|the)\b", lower):
        return phrase

    return f"your {phrase}" if not lower.startswith("your ") else phrase


def _best_discovery_source(discovery: List[str], default: str = "general") -> str:
    if "linkedin" in discovery:
        return "linkedin"
    if "website" in discovery:
        return "website"
    if "reviews" in discovery:
        return "reviews"
    return default


def _augment_discovery_with_profile(profile: dict, discovery: List[str]) -> List[str]:
    signals = [str(x).strip().lower() for x in (discovery or []) if str(x).strip()]
    if (profile.get("linkedin_url") or profile.get("company_linkedin") or profile.get("linkedin_snippet")) and "linkedin" not in signals:
        signals.append("linkedin")
    if (profile.get("company_domain") or profile.get("homepage_snippet")) and "website" not in signals:
        signals.append("website")
    if profile.get("review_signals") and "reviews" not in signals:
        signals.append("reviews")
    return signals


def _infer_source_from_text(text: str, discovery: List[str]) -> str:
    lower = (text or "").lower()
    if "linkedin" in lower:
        return "linkedin"
    if "review" in lower or "rated" in lower:
        return "reviews"
    if "site" in lower or "website" in lower or "page" in lower:
        return "website"
    return _best_discovery_source(discovery, default="general")


def _looks_like_full_opener_sentence(text: str) -> bool:
    lower = re.sub(r"\s+", " ", (text or "").strip().lower())
    if not lower:
        return False
    if " i was " in f" {lower} " or " i came across " in f" {lower} ":
        return True
    if re.match(r"^(i|we)\s+(was|were|noticed|saw|came|looked|checked|read)\b", lower):
        return True
    return lower.count(".") >= 1


def _is_metric_heavy_snippet(text: str) -> bool:
    lower = (text or "").lower()
    if re.search(r"\b\d+\s*(review|reviews|rating|ratings|stars?)\b", lower):
        return True
    if re.search(r"\brated\s*\d+(\.\d+)?\s*out of\s*\d+(\.\d+)?\b", lower):
        return True
    if re.search(r"\b(capterra|tripadvisor|birdeye|yelp|google reviews)\b", lower):
        return True
    return False


def _sanitize_angle_for_observation(text: str, company_name: str = "") -> str:
    cleaned = re.sub(r"\s+", " ", (text or "")).strip().strip(".,;:-")
    if not cleaned:
        return ""

    cleaned = re.sub(
        r"^(i\s+(was|were|noticed|saw|came|looked|checked|read)\b[^,]*,\s*)",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"^(people\s+(mention|keep mentioning|keep bringing up)\s+)",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"^(experience|discover|enjoy|join|explore|learn|see)\s+",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"^cut your ([^,.;]+?) with (.+)$",
        r"\2 for \1",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"^run your ([^,.;]+?) with (.+)$",
        r"\2 for \1",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"^simplify ([^,.;]+?) with (.+)$",
        r"\2 for \1",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"\brated\s*\d+(\.\d+)?\s*out of\s*\d+(\.\d+)?\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b\d+\s*(verified\s*)?(user\s*)?reviews?\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(on|from)\s+(tripadvisor|capterra|birdeye|yelp|google reviews)\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(positive|negative)\s+customer\s+reviews?\b", "customer feedback", cleaned, flags=re.IGNORECASE)
    if company_name:
        cleaned = re.sub(re.escape(company_name), "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip().strip(".,;:-")
    return cleaned


def _humanize_review_theme(angle: str, business_type: str, evidence: dict) -> str:
    cleaned = _sanitize_angle_for_observation(angle)
    lower = cleaned.lower()
    who = str(evidence.get("who_they_serve") or "").strip().lower()

    if business_type == "restaurant":
        if any(token in lower for token in ["bbq", "barbecue"]):
            return "your BBQ and the atmosphere you create"
        if "live music" in lower and "bull" in lower:
            return "your live music nights and bull riding events"
        if "live music" in lower:
            return "your live music nights"
        if any(token in lower for token in ["french", "mediterranean"]):
            return "your menu and dining experience"
        return "the experience your team creates for guests"

    if business_type == "saas":
        if any(token in lower for token in ["food cost", "costing"]):
            return "how your software helps restaurants track food costs"
        if "scheduling" in lower:
            return "how your platform helps teams manage scheduling"
        if "inventory" in lower:
            return "how your product helps with inventory workflows"
        if who:
            return f"how your product supports {who}"
        return "how your software helps teams day to day"

    if business_type == "real_estate":
        return "how your team helps people navigate property decisions"
    if business_type == "construction":
        return "the way your team handles projects and client work"
    if business_type == "nonprofit":
        return "the support your organization gives people in the community"

    if cleaned:
        return cleaned
    return "the way your team serves people in the area"


def _build_meaning_opener(
    profile: dict,
    evidence: dict,
    source: str,
    business_type: str,
    area: str,
    company: str,
    angle: str,
    observation: str,
) -> str:
    core = _impact_core_by_business_type(evidence, business_type)
    if not core:
        return ""

    anchor = _impact_anchor_by_business_type(evidence, business_type, angle, observation)
    core_phrase = core
    if anchor:
        core_phrase = f"{core_phrase} for {anchor}"
    core_phrase = re.sub(r"\s+", " ", core_phrase).strip().rstrip(".")
    core_phrase = _trim_words(core_phrase, 22)

    if source == "linkedin":
        if business_type == "saas":
            lead = "Your company popped up on my LinkedIn"
        else:
            lead = "I came across your company on LinkedIn"
    elif source == "reviews":
        if business_type == "restaurant":
            lead = f"I was looking at places around {area}"
        else:
            lead = f"I was reading what people say about {company}"
    elif source == "website":
        if business_type == "restaurant":
            lead = f"I was looking at places around {area}"
        else:
            lead = "I spent a few minutes on your site"
    else:
        lead = f"I was looking into {company}"

    sentence = f"{lead}, and what stood out is that {core_phrase}."
    return _sentenceize(sentence)


def _impact_core_by_business_type(evidence: dict, business_type: str) -> str:
    bank = _evidence_bank_text(evidence)
    who = str(evidence.get("who_they_serve") or "").strip().lower()

    if business_type == "restaurant":
        if any(token in bank for token in ["live music", "bull riding", "western", "events"]):
            return "you have built a place people choose when they want a real night out"
        if any(token in bank for token in ["french", "mediterranean", "fine dining", "chef"]):
            return "you have built a place people trust for a solid night out"
        if any(token in bank for token in ["bbq", "barbecue", "smokehouse", "grill"]):
            return "you have built a spot locals keep coming back to"
        return "you have built a place people in town keep coming back to"

    if business_type == "saas":
        if who:
            return f"you help {who} run day-to-day work with less friction"
        return "you help teams run day-to-day work with less friction"

    if business_type == "real_estate":
        return "you help people make big property decisions with more clarity"

    if business_type == "construction":
        return "you help owners move projects from planning to finished build"

    if business_type == "nonprofit":
        if who:
            return f"you support {who} in a way that feels tangible"
        return "you support people in the community in a way that feels tangible"

    if business_type == "events":
        return "you help people pull off important events without the chaos"

    return "you seem to be doing solid work for people in the area"


def _impact_anchor_by_business_type(
    evidence: dict,
    business_type: str,
    angle: str,
    observation: str,
) -> str:
    bank = _evidence_bank_text(evidence)
    raw = " ".join([angle or "", observation or ""]).lower()

    if business_type == "restaurant":
        if "live music" in bank and "bull" in bank:
            return "live music nights and bull riding events"
        if "live music" in bank:
            return "live music nights"
        if any(token in bank for token in ["bbq", "barbecue"]):
            return "good BBQ"
        if any(token in bank for token in ["french", "mediterranean"]):
            return "a strong menu and dining experience"
        if "hospitality" in bank or "service" in bank:
            return "how your team takes care of guests"
        return ""

    if business_type == "saas":
        if any(token in bank for token in ["food cost", "costing"]):
            return "food costing and margin control"
        if "scheduling" in bank:
            return "scheduling and team coordination"
        if "inventory" in bank:
            return "inventory workflows"
        if any(token in raw for token in ["software", "platform", "tool"]):
            return "practical tools teams can actually use"
        return ""

    if business_type == "real_estate":
        return "buying, selling, and leasing decisions"

    if business_type == "construction":
        return "project timelines and execution"

    if business_type == "nonprofit":
        return "real support people can feel"

    if business_type == "events":
        return "important moments done right"

    return ""


def _evidence_bank_text(evidence: dict) -> str:
    bank = []
    bank.extend([str(x) for x in (evidence.get("proof_points") or []) if str(x).strip()])
    bank.extend([str(x) for x in (evidence.get("community_impact_signals") or []) if str(x).strip()])
    bank.extend([str(x) for x in (evidence.get("social_proof_signals") or []) if str(x).strip()])
    bank.extend([str(x) for x in (evidence.get("opener_angles") or []) if str(x).strip()])
    for key in ["human_thanks_line", "human_thanks", "why_business_matters_local", "who_they_serve"]:
        val = str(evidence.get(key) or "").strip()
        if val:
            bank.append(val)
    return " ".join(bank).lower()


def _trim_words(text: str, max_words: int) -> str:
    words = re.findall(r"\S+", text or "")
    if len(words) <= max_words:
        return text.strip()
    return " ".join(words[:max_words]).strip(" ,.;:-")


def _strip_redundant_area_phrase(text: str, area: str) -> str:
    phrase = (text or "").strip()
    city = (area or "").strip()
    if not phrase or not city:
        return phrase
    pattern = rf"\b(in|around|near)\s+{re.escape(city)}\b"
    phrase = re.sub(pattern, "", phrase, flags=re.IGNORECASE)
    phrase = re.sub(r"\s{2,}", " ", phrase).strip(" ,.")
    return phrase


def _observation_needs_repair(p1: str) -> bool:
    text = (p1 or "").lower()
    return bool(
        re.search(
            r"\bnoticed that (located|provide|provides|offered|offer|offers|serve|serves|support|supports|help|helps|focus|focuses|specialize|specializes)\b",
            text,
        )
    )


def _is_weak_generic_opener(text: str) -> bool:
    lower = (text or "").lower()
    weak_patterns = [
        "local spots around",
        "pretty neat",
        "i wanted to reach out",
        "i recently came across",
    ]
    return any(pattern in lower for pattern in weak_patterns)


def _claims_unverified_visit(text: str, evidence: dict) -> bool:
    lower = (text or "").lower()
    if not re.search(r"\b(stopped by|swung by|passed by|came by|walked in)\b", lower):
        return False

    bank = []
    bank.extend(evidence.get("opener_angles") or [])
    bank.extend(evidence.get("social_proof_signals") or [])
    bank.append(str(evidence.get("why_business_matters_local") or ""))
    bank_text = " ".join(str(x).lower() for x in bank if str(x).strip())
    return "stopped by" not in bank_text and "in person" not in bank_text and "visited" not in bank_text


def _extract_first_body_paragraph(body: str) -> str:
    parts = [p.strip() for p in re.split(r"\n\s*\n", body or "") if p.strip()]
    if not parts:
        return ""
    if parts[0].lower().startswith("hi "):
        return parts[1] if len(parts) > 1 else ""
    return parts[0]


def _default_persona_lens(business_type: str) -> str:
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


def _should_include_ps(evidence: dict) -> bool:
    confidence = float(evidence.get("confidence", 0.0) or 0.0)
    ps_line = str(evidence.get("human_thanks_line") or evidence.get("human_thanks") or "").strip()
    return bool(ps_line) and confidence >= 0.70


def _minimal_checks(body: str, profile: dict, evidence: dict, research_card: Optional[dict] = None) -> List[str]:
    issues: List[str] = []
    text = body or ""

    first_name = (profile.get("first_name") or "there").strip() or "there"
    greeting_re = rf"^\s*Hi\s+{re.escape(first_name)}\s*,"
    if not re.search(greeting_re, text, flags=re.IGNORECASE):
        issues.append("greeting_malformed_or_wrong_recipient")

    if re.search(r"^\s*subject\s*:", text, flags=re.IGNORECASE | re.MULTILINE):
        issues.append("subject_line_leaked_into_body")

    best_count = len(re.findall(r"^\s*Best,\s*$", text, flags=re.IGNORECASE | re.MULTILINE))
    if best_count != 1:
        issues.append("duplicate_or_missing_signoff")

    if _word_count(_strip_signature_lines(text)) > 100:
        issues.append("exceeds_100_words")

    if not text.strip():
        issues.append("empty_body")
    if _is_collapsed_body(text):
        issues.append("collapsed_body")

    if _printable_ratio(text) < 0.92:
        issues.append("unreadable_output")

    if ("web/social" not in text.lower()) or ("?" not in text):
        issues.append("missing_or_wrong_ask_line")

    source_truth = [str(x).strip().lower() for x in ((research_card or {}).get("source_truth") or evidence.get("source_truth") or []) if str(x).strip()]
    source_truth = [s for s in source_truth if s in {"website", "linkedin", "reviews"}]
    if _mentions_untrusted_source_claim(text, source_truth):
        issues.append("source_claim_untrue")

    seen = set()
    ordered = []
    for issue in issues:
        if issue in seen:
            continue
        seen.add(issue)
        ordered.append(issue)
    return ordered


def _mentions_untrusted_source_claim(text: str, source_truth: List[str]) -> bool:
    lower = (text or "").lower()
    allowed = set(source_truth or [])
    if "linkedin" in lower and "linkedin" not in allowed:
        return True
    if re.search(r"\b(site|website|page)\b", lower) and "website" not in allowed:
        return True
    if re.search(r"\b(review|reviews|what people say|rated)\b", lower) and "reviews" not in allowed:
        return True
    return False


def _evidence_required(evidence: dict) -> bool:
    return bool(
        (evidence.get("proof_points") or [])
        or (evidence.get("community_impact_signals") or [])
        or (evidence.get("social_proof_signals") or [])
        or (evidence.get("opener_angles") or [])
        or str(evidence.get("human_thanks_line") or evidence.get("human_thanks") or "").strip()
        or str(evidence.get("why_business_matters_local") or "").strip()
    )


def _has_concrete_grounding_targets(evidence: dict) -> bool:
    candidates: List[str] = []
    candidates.extend([str(x) for x in (evidence.get("proof_points") or []) if str(x).strip()])
    candidates.extend([str(x) for x in (evidence.get("social_proof_signals") or []) if str(x).strip()])
    candidates.extend([str(x) for x in (evidence.get("community_impact_signals") or []) if str(x).strip()])
    candidates.extend([str(x) for x in (evidence.get("opener_angles") or []) if str(x).strip()])

    if not candidates:
        return False

    generic_markers = (
        "local business community",
        "serves people in the area",
        "team contributes",
        "work people in the area rely on",
    )
    for item in candidates:
        normalized = re.sub(r"\s+", " ", item.lower()).strip()
        if len(re.findall(r"[a-zA-Z][a-zA-Z0-9\-]{3,}", normalized)) < 4:
            continue
        if any(marker in normalized for marker in generic_markers):
            continue
        return True
    return False


def _is_collapsed_body(text: str) -> bool:
    stripped = (text or "").strip()
    if not stripped:
        return True

    core = _strip_signature_lines(stripped)
    core = re.sub(r"^\s*Hi\s+[^\n,]+,", "", core, flags=re.IGNORECASE).strip()
    return _word_count(core) < 12


def _soft_evidence_reference(text: str, evidence: dict) -> bool:
    lower_text = (text or "").lower()

    bank: List[str] = []
    bank.extend(evidence.get("proof_points") or [])
    bank.extend(evidence.get("community_impact_signals") or [])
    bank.extend(evidence.get("social_proof_signals") or [])
    bank.extend(evidence.get("opener_angles") or [])
    for key in ["human_thanks_line", "human_thanks", "why_business_matters_local", "who_they_serve"]:
        val = str(evidence.get(key) or "").strip()
        if val:
            bank.append(val)

    if not bank:
        return True

    discovery_signals = [str(x).strip().lower() for x in (evidence.get("discovery_signals") or []) if str(x).strip()]
    if "linkedin" in discovery_signals and "linkedin" in lower_text:
        return True
    if "website" in discovery_signals and any(token in lower_text for token in ("site", "website", "page")):
        return True
    if "reviews" in discovery_signals and any(token in lower_text for token in ("review", "reviews", "people mention", "what people say")):
        return True

    body_tokens = set(re.findall(r"[a-zA-Z][a-zA-Z0-9\-]{3,}", lower_text))
    if not body_tokens:
        return False

    stop = {
        "with",
        "from",
        "that",
        "this",
        "your",
        "have",
        "will",
        "they",
        "team",
        "company",
        "business",
        "local",
        "area",
    }

    for line in bank:
        tokens = [
            t
            for t in re.findall(r"[a-zA-Z][a-zA-Z0-9\-]{3,}", str(line).lower())
            if t not in stop
        ]
        overlap = sum(1 for t in tokens if t in body_tokens)
        if overlap >= 2:
            return True

    for line in bank:
        words = [w for w in re.findall(r"[a-zA-Z][a-zA-Z0-9\-]{2,}", str(line).lower())]
        if len(words) >= 4:
            for i in range(0, len(words) - 3):
                phrase = " ".join(words[i : i + 4])
                if phrase in lower_text:
                    return True

    cues = [c.lower() for c in (evidence.get("language_cues") or []) if str(c).strip()]
    cue_hit = any(cue in lower_text for cue in cues[:6])
    if cue_hit:
        noun_candidates = set()
        for line in bank:
            for tok in re.findall(r"[a-zA-Z][a-zA-Z0-9\-]{4,}", str(line).lower()):
                if tok not in {"community", "business", "service", "customers", "clients"}:
                    noun_candidates.add(tok)
        if body_tokens & noun_candidates:
            return True

    return False


def _judge_naturalness(body: str, profile: dict, intent_score: float) -> dict:
    judge_model = getattr(config, "JUDGE_MODEL", "gpt-4.1-mini")
    prompt = (
        "You are a strict readability and intent checker for outreach email copy. "
        "Return JSON only with keys: passes, reasons, severity, suggested_fix. "
        "Fail if the email does not read naturally, has contradictions, repeats lines awkwardly, "
        "or drifts away from this intent: personalized observation, web/social ask, "
        "not-assuming intro, soft nearby/Zoom next step."
    )

    try:
        response = client.chat.completions.create(
            model=judge_model,
            messages=[
                {"role": "system", "content": prompt},
                {
                    "role": "user",
                    "content": (
                        f"Recipient: {profile.get('first_name', '')}\n"
                        f"Intent score hint: {intent_score:.3f}\n"
                        f"Email:\n{body}"
                    ),
                },
            ],
            max_completion_tokens=220,
            response_format={"type": "json_object"},
        )
        raw = (response.choices[0].message.content or "").strip()
        parsed = _safe_parse_json_object(raw) or {}
        passes = bool(parsed.get("passes", False))
        reasons = parsed.get("reasons") or []
        if isinstance(reasons, str):
            reasons = [reasons]
        severity = str(parsed.get("severity", "low") or "low").lower()
        if severity not in {"low", "medium", "high"}:
            severity = "medium"
        suggested_fix = str(parsed.get("suggested_fix", "") or "").strip()
        return {
            "passes": passes,
            "reasons": [str(r).strip() for r in reasons if str(r).strip()],
            "severity": severity,
            "suggested_fix": suggested_fix,
        }
    except Exception as exc:
        logger.warning("judge_fallback_due_to_error:%s", exc)
        return {
            "passes": True,
            "reasons": [],
            "severity": "low",
            "suggested_fix": "",
        }


def _intent_similarity_score(body: str) -> float:
    text = (body or "").lower()
    anchors = [
        "web/social",
        "idea",
        "assume",
        "nearby",
        "zoom",
        "local",
    ]
    hits = sum(1 for a in anchors if a in text)
    return round(hits / float(len(anchors)), 3)


def _categorize_issues(issues: List[str]) -> str:
    issue_set = set(issues or [])
    if "source_claim_untrue" in issue_set:
        return "source_claim_untrue"
    if issue_set & {"missing_non_assumptive_intent", "missing_soft_close_intent", "missing_business_essence", "missing_or_wrong_ask_line"}:
        return "anchor_failed"
    if "anchor_failed" in issue_set:
        return "anchor_failed"
    if "opener_quality_failed" in issue_set:
        return "opener_quality_failed"
    if issue_set & {"generic_opener", "source_claim_untrue", "no_meaning_signal", "no_concrete_anchor", "tone_too_marketing"}:
        return "opener_quality_failed"
    if "weak_evidence" in issue_set:
        return "weak_evidence"
    if "opener_not_grounded" in issue_set:
        return "opener_not_grounded"
    if "empty_body" in issue_set or "collapsed_body" in issue_set:
        return "collapse"
    if "missing_evidence_reference" in issue_set:
        return "evidence"
    return "format_invalid"


def _safe_parse_json_object(raw: str) -> Optional[dict]:
    text = (raw or "").strip()
    if not text:
        return None
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z]*\n?", "", text)
        text = re.sub(r"\n?```$", "", text).strip()
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            try:
                parsed = json.loads(text[start : end + 1])
                return parsed if isinstance(parsed, dict) else None
            except Exception:
                return None
    return None


def _raise_writer_failure(
    profile: dict,
    stage: str,
    message: str,
    issues: list,
    generated_body: str,
    judge_result: Optional[dict] = None,
    repair_prompt_snippet: str = "",
    critical_check_failures: Optional[list] = None,
    final_decision: str = "skipped",
) -> None:
    payload = {
        "timestamp": datetime.now().isoformat(),
        "email": profile.get("email", ""),
        "company": profile.get("company_name", ""),
        "stage": stage,
        "message": message,
        "issues": issues,
        "judge_result": judge_result or {},
        "critical_check_failures": list(critical_check_failures or []),
        "repair_attempt_applied": bool(repair_prompt_snippet),
        "repair_prompt_snippet": repair_prompt_snippet[:500],
        "final_decision": final_decision,
        "generated_body": generated_body,
        "evidence_pack": _get_evidence_pack(profile),
    }
    _record_writer_failure(payload)

    raise WriterValidationError(
        message=message,
        stage=stage,
        email=profile.get("email", ""),
        company=profile.get("company_name", ""),
        issues=issues,
        generated_body=generated_body,
        judge_result=judge_result,
        repair_prompt_snippet=repair_prompt_snippet,
        critical_check_failures=critical_check_failures,
        final_decision=final_decision,
    )


def _company_initials(company_name: str) -> str:
    words = re.findall(r"[A-Za-z0-9]+", company_name or "")
    if words:
        return "".join(word[0] for word in words).upper()
    fallback = re.sub(r"[^A-Za-z0-9]", "", company_name or "").upper()
    return fallback[:3] if fallback else "CO"


def _word_count(text: str) -> int:
    return len(re.findall(r"\b\w+\b", text or ""))


def _printable_ratio(text: str) -> float:
    if not text:
        return 0.0
    printable = sum(1 for ch in text if ch.isprintable() or ch in "\n\t")
    return printable / max(1, len(text))


def _strip_signature_lines(text: str) -> str:
    out = re.sub(r"\n\s*Best,\s*$", "", text or "", flags=re.IGNORECASE).strip()
    out = re.sub(r"\n\s*Cheers,\s*$", "", out, flags=re.IGNORECASE).strip()
    return out


def _get_evidence_pack(profile: dict) -> dict:
    evidence = profile.get("evidence_pack") or {}
    if not isinstance(evidence, dict):
        evidence = {}

    human_thanks_line = str(
        evidence.get("human_thanks_line")
        or evidence.get("human_thanks")
        or ""
    ).strip()

    return {
        "industry_label": str(evidence.get("industry_label") or profile.get("company_industry") or "").strip(),
        "industry_bucket": str(evidence.get("industry_bucket") or evidence.get("business_type") or "general").strip().lower(),
        "business_type": str(evidence.get("business_type") or "general").strip(),
        "proof_points": [str(x).strip() for x in (evidence.get("proof_points") or []) if str(x).strip()],
        "proof_phrase": str(evidence.get("proof_phrase") or "").strip(),
        "community_impact_signals": [
            str(x).strip() for x in (evidence.get("community_impact_signals") or []) if str(x).strip()
        ],
        "social_proof_signals": [str(x).strip() for x in (evidence.get("social_proof_signals") or []) if str(x).strip()],
        "discovery_signals": [str(x).strip().lower() for x in (evidence.get("discovery_signals") or []) if str(x).strip()],
        "source_truth": [
            str(x).strip().lower()
            for x in (evidence.get("source_truth") or evidence.get("discovery_signals") or [])
            if str(x).strip()
        ],
        "opener_angles": [str(x).strip() for x in (evidence.get("opener_angles") or []) if str(x).strip()],
        "persona_lens": str(evidence.get("persona_lens") or _default_persona_lens(str(evidence.get("business_type") or "general"))).strip(),
        "opener_confidence": float(evidence.get("opener_confidence") or 0.0),
        "evidence_quality": str(evidence.get("evidence_quality") or "").strip().lower(),
        "human_thanks": str(evidence.get("human_thanks") or human_thanks_line).strip(),
        "human_thanks_line": human_thanks_line,
        "why_business_matters_local": str(evidence.get("why_business_matters_local") or "").strip(),
        "who_they_serve": str(evidence.get("who_they_serve") or "").strip(),
        "impact_subject": str(evidence.get("impact_subject") or "").strip(),
        "impact_core": str(evidence.get("impact_core") or "").strip(),
        "meaning_line": str(evidence.get("meaning_line") or "").strip(),
        "language_cues": [str(x).strip() for x in (evidence.get("language_cues") or []) if str(x).strip()],
        "confidence": float(evidence.get("confidence") or 0.0),
        "source_tags": list(evidence.get("source_tags") or []),
    }
