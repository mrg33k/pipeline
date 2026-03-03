#!/usr/bin/env python3
"""
Ahead of Market - Cold Outreach Automation Pipeline
====================================================
Run daily to generate up to 25 personalized Gmail drafts.

Usage:
    python3 run_pipeline.py
    python3 run_pipeline.py --dry-run        # skip Gmail draft creation
    python3 run_pipeline.py --max 10         # limit to 10 emails
    python3 run_pipeline.py --skip-drafts    # do everything except create drafts
    python3 run_pipeline.py --no-ui          # skip startup browser UI

Steps:
    1. Apollo free search (no credits) -> ~300 candidates
    2. LLM filters/ranks top 25 prospects
    3. Apollo enrichment (25 credits) -> get emails
    4. Lightweight company research
    5. LLM writes personalized emails
    6. Gmail API creates drafts
    7. CSV export + contacts DB update
"""

import argparse
import csv
import glob
import json
import logging
import os
import re
import sys
from datetime import datetime, timedelta
from typing import Optional
from openai import OpenAI

# Add project dir to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
from contacts_db import ContactsDB
import apollo_client
import llm_filter
import research
import email_writer
import gmail_drafter
import csv_export
from runtime_settings import RunSettings
from startup_ui import collect_run_settings

prompt_assistant_client = OpenAI()


def setup_logging():
    """Configure logging to both console and file."""
    os.makedirs(config.LOG_DIR, exist_ok=True)
    log_file = os.path.join(config.LOG_DIR, f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

    # Create formatters
    console_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
    file_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(console_fmt)

    # File handler
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(file_fmt)

    # Root logger
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(console)
    root.addHandler(fh)

    return log_file


def print_banner():
    print()
    print("=" * 60)
    print("  AHEAD OF MARKET - Cold Outreach Pipeline")
    print(f"  Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)
    print()


def validate_config(skip_gmail_checks: bool = False, require_apollo: bool = True):
    """Check that required API keys and files exist."""
    errors = []
    if require_apollo and not config.APOLLO_API_KEY:
        errors.append("APOLLO_API_KEY environment variable not set")
    if not config.OPENAI_API_KEY:
        errors.append("OPENAI_API_KEY environment variable not set")

    if not skip_gmail_checks:
        if not os.path.exists(config.GMAIL_CLIENT_SECRET):
            errors.append(f"Gmail client secret not found: {config.GMAIL_CLIENT_SECRET}")
        if not os.path.exists(config.GMAIL_TOKENS_PATH):
            errors.append(f"Gmail tokens not found: {config.GMAIL_TOKENS_PATH}")

    if errors:
        for e in errors:
            logging.error(f"Config error: {e}")
        return False
    return True


def _build_initial_settings(args: argparse.Namespace) -> RunSettings:
    settings = RunSettings(
        max_emails=args.max,
        pages=args.pages,
        dry_run=args.dry_run,
        skip_drafts=args.skip_drafts,
        openai_model=config.OPENAI_MODEL,
        email_system_prompt=email_writer.SYSTEM_PROMPT,
        filter_extra_directions="",
        rewrite_count=10,
        rewrite_confirmed=False,
    ).normalized()
    settings.validate()
    return settings


def choose_signature_html(logger: logging.Logger) -> Optional[str]:
    """
    Use Gmail account preset signature when accessible.
    Returns empty string when account signature is unavailable.
    """
    options = gmail_drafter.list_account_signatures()
    if not options:
        logger.warning(
            "No Gmail account signatures available via API. "
            "Draft will use no appended signature. "
            "Run `python3 reauth_gmail.py` to enable account default signature loading."
        )
        return ""

    # Prefer account default signature, then primary, then first available.
    selected = next((o for o in options if o.get("is_default")), None)
    if selected is None:
        selected = next((o for o in options if o.get("is_primary")), None)
    if selected is None:
        selected = options[0]

    label = selected.get("send_as_email", "") or selected.get("display_name", "unknown")
    logger.info(f"Using Gmail account signature: {label}")
    return selected.get("signature_html", "") or ""


def _log_evidence_failure(logger: logging.Logger, exc: Exception) -> None:
    if isinstance(exc, research.EvidenceExtractionError):
        logger.error(
            "Evidence extraction failed (stage=%s, email=%s, company=%s).",
            exc.stage,
            exc.email or "unknown",
            exc.company or "unknown",
        )
        logger.error("Evidence attempts: %s", exc.attempts or "<none>")
        logger.error("Evidence request id: %s", exc.request_id or "<none>")
        logger.error("Evidence finish reason: %s", exc.finish_reason or "<none>")
        raw_text = exc.raw if exc.raw else "<empty response content>"
        logger.error("Evidence raw response snippet: %s", raw_text)
        logger.error("Evidence failure report: %s", research.get_evidence_failure_report_path())
        logger.error("Run canceled due to evidence extraction failure.")
        return
    logger.error("Run canceled due to unexpected evidence failure: %s", exc)


def _log_writer_failure(logger: logging.Logger, exc: Exception) -> None:
    if isinstance(exc, email_writer.WriterValidationError):
        logger.error(
            "Writer failed (stage=%s, email=%s, company=%s).",
            exc.stage,
            exc.email or "unknown",
            exc.company or "unknown",
        )
        if exc.issues:
            logger.error("Writer issues: %s", ",".join(exc.issues))
        logger.error("Writer failure report: %s", email_writer.get_writer_failure_report_path())
        if exc.generated_body:
            logger.error("Writer body snapshot: %s", exc.generated_body[:400])
        logger.error("Tip: run `python3 run_pipeline.py --show-last-failure --failure-tail 5` for a quick failure digest.")
        logger.error("Run canceled due to writer validation failure.")
        return
    logger.error("Run canceled due to unexpected writer failure: %s", exc)


def _parse_export_timestamp(csv_path: str) -> datetime:
    """
    Parse outreach export timestamp from filename outreach_YYYY-MM-DD_HHMMSS.csv.
    Fallback to file modified time when parse fails.
    """
    name = os.path.basename(csv_path)
    match = re.search(r"outreach_(\d{4}-\d{2}-\d{2})_(\d{6})\.csv$", name)
    if match:
        date_part = match.group(1)
        time_part = match.group(2)
        try:
            return datetime.strptime(f"{date_part}_{time_part}", "%Y-%m-%d_%H%M%S")
        except ValueError:
            pass
    return datetime.fromtimestamp(os.path.getmtime(csv_path))


def _next_outreach_send_slot(now: Optional[datetime] = None) -> datetime:
    """
    Schedule slot rule:
    - Mon -> Tue 10:07
    - Tue -> Wed 10:07
    - Wed -> Thu 10:07
    - Thu/Fri/Sat/Sun -> next Tue 10:07
    """
    current = now or datetime.now()
    weekday = current.weekday()  # Mon=0 ... Sun=6

    if weekday <= 2:
        target_weekday = weekday + 1
    else:
        target_weekday = 1  # Tuesday

    days_ahead = (target_weekday - weekday) % 7
    if days_ahead == 0:
        days_ahead = 7

    target = current.replace(hour=10, minute=7, second=0, microsecond=0)
    return target + timedelta(days=days_ahead)


def _annotate_scheduled_send(emails: list[dict], logger: logging.Logger) -> None:
    """Attach planned send time metadata to each email for export/review."""
    slot = _next_outreach_send_slot()
    slot_text = slot.strftime("%Y-%m-%d 10:07 AM")
    for item in emails:
        item["scheduled_for_local"] = f"{slot_text} America/Phoenix"
    logger.info(f"planned_send_slot:{slot_text} America/Phoenix")


def _apollo_person_to_profile(person: dict) -> dict:
    """Convert Apollo enriched person payload to writer profile shape."""
    org = person.get("organization", {}) or {}
    return {
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


def _prepare_profile_for_writing(
    profile: dict,
    logger: logging.Logger,
    model: Optional[str] = None,
    force_refresh_homepage: bool = False,
    enrich_evidence: bool = True,
) -> dict:
    """
    Unified pre-write profile preparation for both new outreach and rewrite flows.
    """
    prepared = dict(profile)
    research.gather_profile_context(prepared, force=force_refresh_homepage)
    logger.info("research_sources:%s", "|".join(prepared.get("discovery_signals") or []))

    if enrich_evidence:
        try:
            prepared["evidence_pack"] = research.enrich_evidence_with_llm(
                prepared,
                model=model or config.EVIDENCE_MODEL,
            )
        except Exception as exc:
            logger.warning(
                "profile_evidence_fallback:%s:%s",
                prepared.get("email", ""),
                exc,
            )
            base = research.build_evidence_pack(prepared)
            base["evidence_quality"] = "weak"
            base["failure_reason"] = str(exc)[:220]
            prepared["evidence_pack"] = base
        ev = prepared.get("evidence_pack", {}) or {}
        logger.info(
            "profile_prepared:%s:confidence=%.2f:proof_points=%d:impact_signals=%d:social_proof=%d:opener_angles=%d:evidence_quality=%s",
            prepared.get("email", ""),
            float(ev.get("confidence", 0.0) or 0.0),
            len(ev.get("proof_points") or []),
            len(ev.get("community_impact_signals") or []),
            len(ev.get("social_proof_signals") or []),
            len(ev.get("opener_angles") or []),
            ev.get("evidence_quality", "unknown"),
        )
    else:
        prepared["evidence_pack"] = research.build_evidence_pack(prepared)

    prepared["research_card"] = research.build_research_card(
        prepared,
        evidence_pack=prepared.get("evidence_pack") or {},
    )
    card = prepared.get("research_card", {}) or {}
    logger.info(
        "research_card:%s:industry=%s:source_truth=%s:confidence=%.2f:quality=%s:meaning=%s",
        prepared.get("email", ""),
        card.get("industry_bucket", "general"),
        "|".join(card.get("source_truth") or []),
        float(card.get("confidence", 0.0) or 0.0),
        card.get("evidence_quality", "unknown"),
        (str(card.get("meaning_line") or "")[:100]).replace("\n", " "),
    )

    return prepared


def _latest_file(pattern: str) -> Optional[str]:
    candidates = sorted(glob.glob(pattern))
    return candidates[-1] if candidates else None


def _tail_jsonl(path: str, max_rows: int) -> list[dict]:
    if not path or not os.path.exists(path):
        return []
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                rows.append({"raw": line})
    return rows[-max_rows:] if max_rows > 0 else rows


def show_last_failures(logger: logging.Logger, tail: int = 5) -> int:
    """
    Print a concise digest of latest evidence/writer failures to make debugging quick.
    """
    tail = max(1, int(tail or 5))
    evidence_path = _latest_file(os.path.join(config.LOG_DIR, "evidence_failures_*.jsonl"))
    writer_path = _latest_file(os.path.join(config.LOG_DIR, "writer_failures_*.jsonl"))

    logger.info("failure_digest:tail=%d", tail)
    logger.info("failure_digest:evidence_file=%s", evidence_path or "<none>")
    logger.info("failure_digest:writer_file=%s", writer_path or "<none>")

    evidence_rows = _tail_jsonl(evidence_path, tail)
    writer_rows = _tail_jsonl(writer_path, tail)

    if not evidence_rows and not writer_rows:
        logger.info("No failure records found.")
        return 0

    if evidence_rows:
        logger.info("---- Evidence failures (latest %d) ----", len(evidence_rows))
        for i, row in enumerate(evidence_rows, start=1):
            logger.info(
                "[%d] ts=%s stage=%s email=%s company=%s finish_reason=%s request_id=%s",
                i,
                row.get("timestamp", "<none>"),
                row.get("stage", "<none>"),
                row.get("email", "<none>"),
                row.get("company", "<none>"),
                row.get("finish_reason", "<none>"),
                row.get("request_id", "<none>"),
            )
            if row.get("message"):
                logger.info("    message=%s", str(row.get("message"))[:220])
            if row.get("raw_snippet"):
                logger.info("    raw_snippet=%s", str(row.get("raw_snippet"))[:220])

    if writer_rows:
        logger.info("---- Writer failures (latest %d) ----", len(writer_rows))
        for i, row in enumerate(writer_rows, start=1):
            logger.info(
                "[%d] ts=%s stage=%s email=%s company=%s issues=%s parse_mode=%s",
                i,
                row.get("timestamp", "<none>"),
                row.get("stage", "<none>"),
                row.get("email", "<none>"),
                row.get("company", "<none>"),
                ",".join(row.get("issues", []) or []),
                row.get("parse_mode", "<none>"),
            )
            if row.get("patch_actions"):
                logger.info("    patch_actions=%s", ",".join(row.get("patch_actions", [])))
            if row.get("raw_model_output_snippet"):
                logger.info("    raw_model_output_snippet=%s", str(row.get("raw_model_output_snippet"))[:220])

    return 0


def debug_rewrite_match(logger: logging.Logger, tail: int = 10) -> int:
    """
    Non-mutating diagnostics for CSV-vs-Gmail rewrite matching.
    """
    tail = max(1, int(tail or 10))
    pattern = os.path.join(config.DAILY_CSV_DIR, "outreach_*.csv")
    csv_paths = sorted(glob.glob(pattern))
    if not csv_paths:
        logger.error("rewrite_debug: no outreach CSV exports found.")
        return 1

    rows_by_email = {}
    for csv_path in csv_paths:
        with open(csv_path, "r", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                email = (row.get("email") or "").strip().lower()
                if email:
                    rows_by_email[email] = dict(row)

    logger.info("rewrite_debug:csv_files=%d csv_unique_emails=%d", len(csv_paths), len(rows_by_email))
    if not rows_by_email:
        logger.error("rewrite_debug: no valid emails in CSV exports.")
        return 1

    live_drafts = gmail_drafter.list_recent_outreach_drafts(
        max_results=max(len(rows_by_email) * 4, 250),
        subject_filter=False,
    )
    live_by_id = {(d.get("draft_id") or "").strip(): d for d in live_drafts if (d.get("draft_id") or "").strip()}
    live_by_email = {}
    for d in live_drafts:
        em = (d.get("to") or "").strip().lower()
        if em and em not in live_by_email:
            live_by_email[em] = d

    logger.info(
        "rewrite_debug:live_drafts=%d live_id_index=%d live_email_index=%d",
        len(live_drafts),
        len(live_by_id),
        len(live_by_email),
    )

    matched_by_id = 0
    matched_by_email = 0
    unmatched = []
    for email, row in rows_by_email.items():
        csv_draft_id = (row.get("draft_id") or "").strip()
        if csv_draft_id and csv_draft_id in live_by_id:
            matched_by_id += 1
            continue
        if email in live_by_email:
            matched_by_email += 1
            continue
        unmatched.append((email, csv_draft_id))

    logger.info(
        "rewrite_debug:matched_by_id=%d matched_by_email=%d unmatched=%d",
        matched_by_id,
        matched_by_email,
        len(unmatched),
    )
    if unmatched:
        logger.info("rewrite_debug:unmatched_sample (max %d):", tail)
        for email, draft_id in unmatched[:tail]:
            logger.info("  email=%s csv_draft_id=%s", email, draft_id or "<none>")
    return 0


def _load_active_rewrite_candidates(
    logger: logging.Logger,
    model: Optional[str] = None,
    enrich_evidence: bool = True,
    limit: Optional[int] = None,
) -> list[dict]:
    """
    Return active draft rewrite candidates sorted newest-first by export timestamp.
    """
    pattern = os.path.join(config.DAILY_CSV_DIR, "outreach_*.csv")
    csv_paths = sorted(glob.glob(pattern))
    if not csv_paths:
        return []

    rows_by_email = {}
    for csv_path in csv_paths:
        export_ts = _parse_export_timestamp(csv_path)
        with open(csv_path, "r", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                email = (row.get("email") or "").strip().lower()
                if not email:
                    continue
                row_copy = dict(row)
                row_copy["_export_ts"] = export_ts
                rows_by_email[email] = row_copy

    if not rows_by_email:
        return []

    # Primary truth for rewrite: exported draft IDs that still exist in Gmail.
    unique_csv_draft_ids = sorted(
        {
            (row.get("draft_id") or "").strip()
            for row in rows_by_email.values()
            if (row.get("draft_id") or "").strip()
        }
    )
    live_by_verified_id = {}
    for draft_id in unique_csv_draft_ids:
        if gmail_drafter.draft_exists(draft_id):
            live_by_verified_id[draft_id] = {"draft_id": draft_id}

    # Secondary fallback: live Gmail drafts by recipient email when CSV id is stale.
    # We include more than row count to account for mixed mailbox noise.
    live_drafts = gmail_drafter.list_recent_outreach_drafts(
        max_results=max(len(rows_by_email) * 4, 200),
        subject_filter=False,
    )
    live_by_id = {}
    live_by_email = {}
    for d in live_drafts:
        did = (d.get("draft_id") or "").strip()
        to_email = (d.get("to") or "").strip().lower()
        if did:
            live_by_id[did] = d
        if to_email and to_email not in live_by_email:
            live_by_email[to_email] = d

    logger.info(
        "rewrite_live_drafts_fetched:%d (verified_csv_ids=%d,id_index=%d,email_index=%d)",
        len(live_drafts),
        len(live_by_verified_id),
        len(live_by_id),
        len(live_by_email),
    )

    matched_rows = []
    matched_by_verified_id = 0
    matched_by_live_id = 0
    matched_by_email_fallback = 0
    for email, row in rows_by_email.items():
        csv_draft_id = (row.get("draft_id") or "").strip()
        matched = None
        if csv_draft_id and csv_draft_id in live_by_verified_id:
            matched = live_by_verified_id[csv_draft_id]
            matched_by_verified_id += 1
        elif csv_draft_id and csv_draft_id in live_by_id:
            matched = live_by_id[csv_draft_id]
            matched_by_live_id += 1
        else:
            # Fallback: match by recipient email when CSV draft id is stale.
            matched = live_by_email.get(email)
            if matched:
                matched_by_email_fallback += 1

        if not matched:
            continue
        draft_id = (matched.get("draft_id") or "").strip()
        if not draft_id:
            continue
        matched_rows.append(
            {
                "email": email,
                "draft_id": draft_id,
                "row": row,
                "export_timestamp": row.get("_export_ts"),
            }
        )

    matched_rows.sort(key=lambda c: c.get("export_timestamp") or datetime.min, reverse=True)
    if limit and limit > 0:
        matched_rows = matched_rows[:limit]

    candidates = []
    for item in matched_rows:
        profile = _row_to_profile(
            item["row"],
            fallback_email=item["email"],
            model=model,
            enrich_evidence=enrich_evidence,
        )
        if enrich_evidence:
            ev = profile.get("evidence_pack", {}) or {}
            logger.info(
                "rewrite_evidence:%s:confidence=%.2f:proof_points=%d:impact_signals=%d:social_proof=%d:opener_angles=%d:evidence_quality=%s",
                profile.get("email", ""),
                float(ev.get("confidence", 0.0) or 0.0),
                len(ev.get("proof_points") or []),
                len(ev.get("community_impact_signals") or []),
                len(ev.get("social_proof_signals") or []),
                len(ev.get("opener_angles") or []),
                ev.get("evidence_quality", "unknown"),
            )
        candidates.append(
            {
                "email": profile.get("email", "").strip().lower(),
                "draft_id": item["draft_id"],
                "profile": profile,
                "export_timestamp": item.get("export_timestamp"),
            }
        )

    if limit and limit > 0:
        # Already limited before expensive prep; keep this as no-op safety.
        candidates = candidates[:limit]
    logger.info(
        "rewrite_match_summary:by_verified_id=%d by_live_id=%d by_email_fallback=%d",
        matched_by_verified_id,
        matched_by_live_id,
        matched_by_email_fallback,
    )
    logger.info(f"rewrite_candidates_total:{len(candidates)}")
    return candidates


def rewrite_todays_drafts(settings: RunSettings, logger: logging.Logger, limit: Optional[int]) -> int:
    """
    Delete and recreate currently existing outreach drafts.
    Uses export CSV rows as profile source and verifies active drafts by draft_id.
    """
    try:
        candidates = _load_active_rewrite_candidates(logger, model=config.EVIDENCE_MODEL, enrich_evidence=True)
    except Exception as exc:
        _log_evidence_failure(logger, exc)
        return 1
    if not candidates:
        logger.error(
            "No active outreach drafts found in Gmail to rewrite. "
            "Export draft IDs may be stale, or drafts were already sent/deleted."
        )
        return 1

    if limit is None or limit <= 0:
        selected = list(candidates)
    else:
        selected = candidates[: max(0, limit)]
    if not selected:
        logger.error("No drafts selected for rewrite. Increase rewrite count.")
        return 1

    newest_ts = selected[0].get("export_timestamp")
    newest_text = newest_ts.strftime("%Y-%m-%d %H:%M:%S") if isinstance(newest_ts, datetime) else "unknown"
    logger.info(f"rewrite_count_limit:{limit if limit and limit > 0 else 'ALL'}")
    logger.info(f"rewrite_selected_count:{len(selected)}")
    logger.info(f"rewrite_preview_mode:false")
    logger.info(f"rewrite_selected_newest:{newest_text}")

    profiles = [c["profile"] for c in selected]
    draft_ids_by_email = {c["profile"]["email"]: c["draft_id"] for c in selected}

    signature_html = choose_signature_html(logger)

    emails = email_writer.write_emails_batch(
        profiles,
        system_prompt=settings.email_system_prompt,
        model=settings.openai_model,
    )
    writer_stats = email_writer.get_last_batch_stats()
    logger.info(
        "rewrite_writer_stats:generated=%d drafted=%d skipped_by_critical=%d skipped_by_judge=%d failed_other=%d",
        writer_stats.get("generated", 0),
        writer_stats.get("drafted", 0),
        writer_stats.get("skipped_by_critical", 0),
        writer_stats.get("skipped_by_judge", 0),
        writer_stats.get("failed_other", 0),
    )
    if not emails:
        logger.error("No rewrite emails passed quality checks. Skipped all selected contacts.")
        return 1
    _annotate_scheduled_send(emails, logger)
    results = gmail_drafter.rewrite_drafts_batch(
        emails,
        draft_ids_by_email=draft_ids_by_email,
        create_missing=False,
        signature_html=signature_html,
    )

    csv_path = csv_export.export_daily_batch(emails, results)
    successful = sum(1 for r in results if r["success"])
    skipped_by_gmail = max(0, len(results) - successful)
    logger.info(
        "Rewrite complete: %d/%d drafts replaced (skipped_by_gmail=%d)",
        successful,
        len(results),
        skipped_by_gmail,
    )
    logger.info(f"CSV export: {csv_path}")
    return 0


def preview_rewrite_batch(settings: RunSettings, logger: logging.Logger) -> list[dict]:
    """
    Generate preview samples from the exact rewrite subset (newest-first, limited by rewrite_count).
    """
    candidates = _load_active_rewrite_candidates(logger, model=config.EVIDENCE_MODEL, enrich_evidence=True)
    selected = candidates[: settings.rewrite_count]
    logger.info(f"rewrite_count_limit:{settings.rewrite_count}")
    logger.info(f"rewrite_selected_count:{len(selected)}")
    logger.info("rewrite_preview_mode:true")

    if not selected:
        raise RuntimeError("No active rewrite candidates found.")

    previews = []
    for candidate in selected[:3]:
        profile = candidate["profile"]
        try:
            email = email_writer.write_email(
                profile,
                system_prompt=settings.email_system_prompt,
                model=settings.openai_model,
            )
            qa_ok, qa_issues = email_writer.validate_generated_email(email["body"], profile)
            if not qa_ok:
                logger.error(f"rewrite_preview_unresolved_issues:{profile.get('email','')}:{','.join(qa_issues)}")
            previews.append(
                {
                    "to": profile.get("email", ""),
                    "company": profile.get("company_name", ""),
                    "subject": email.get("subject", ""),
                    "body": email.get("body", ""),
                    "issues": qa_issues,
                }
            )
        except Exception as exc:
            logger.error("rewrite_preview_generation_failed:%s:%s", profile.get("email", ""), exc)
            previews.append(
                {
                    "to": profile.get("email", ""),
                    "company": profile.get("company_name", ""),
                    "subject": "",
                    "body": "",
                    "issues": ["preview_generation_failed"],
                }
            )
    return previews


def _row_to_profile(
    row: dict,
    fallback_email: str = "",
    model: Optional[str] = None,
    enrich_evidence: bool = True,
) -> dict:
    email = (row.get("email") or fallback_email).strip()
    profile = {
        "apollo_id": row.get("apollo_id", ""),
        "first_name": row.get("first_name", ""),
        "last_name": row.get("last_name", ""),
        "email": email,
        "title": row.get("title", ""),
        "company_name": row.get("company", ""),
        "company_industry": row.get("industry", ""),
        "company_city": row.get("city", ""),
        "company_state": row.get("state", ""),
        "company_domain": row.get("domain", ""),
        "company_linkedin": row.get("company_linkedin", ""),
        "company_description": "",
        "homepage_snippet": "",
        "linkedin_snippet": "",
        "review_signals": [],
        "review_sources": [],
        "discovery_signals": [],
        "company_employee_count": "",
        "company_founded_year": "",
    }
    return _prepare_profile_for_writing(
        profile,
        logger=logging.getLogger("pipeline"),
        model=model or config.EVIDENCE_MODEL,
        force_refresh_homepage=True,
        enrich_evidence=enrich_evidence,
    )


def _load_profiles_from_csv_pattern(pattern: str, limit: int = 0, model: Optional[str] = None) -> list[dict]:
    """Load unique profiles from CSV files matching pattern. Newer rows win by email."""
    csv_paths = sorted(glob.glob(pattern))
    row_by_email = {}

    for csv_path in csv_paths:
        with open(csv_path, "r", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                email = (row.get("email") or "").strip().lower()
                if email:
                    row_by_email[email] = row

    items = list(row_by_email.items())
    if limit > 0:
        items = items[:limit]

    profiles = [
        _row_to_profile(row, fallback_email=email, model=model or config.EVIDENCE_MODEL, enrich_evidence=True)
        for email, row in items
    ]

    if limit > 0:
        return profiles[:limit]
    return profiles


def _infer_name_from_email(email: str) -> str:
    local = (email or "").split("@", 1)[0]
    token = re.split(r"[._\-+0-9]+", local)[0].strip()
    return token.capitalize() if token else "there"


def _load_preview_profiles(limit: int, logger: logging.Logger, model: Optional[str] = None) -> list[dict]:
    """
    Preferred preview data source order:
    1) Today's CSV exports
    2) Most recent CSV export day
    3) Existing Gmail outreach drafts (subject starts with 'Video for ')
    """
    today = datetime.now().strftime("%Y-%m-%d")
    today_pattern = os.path.join(config.DAILY_CSV_DIR, f"outreach_{today}_*.csv")
    profiles = _load_profiles_from_csv_pattern(today_pattern, limit=limit, model=model)
    if profiles:
        logger.info(f"Preview source: today's CSV ({len(profiles)} profile(s))")
        return profiles

    any_pattern = os.path.join(config.DAILY_CSV_DIR, "outreach_*.csv")
    all_csv = sorted(glob.glob(any_pattern))
    if all_csv:
        latest_csv = all_csv[-1]
        profiles = _load_profiles_from_csv_pattern(latest_csv, limit=limit, model=model)
        if profiles:
            logger.info(
                f"Preview source: latest CSV fallback ({os.path.basename(latest_csv)}, {len(profiles)} profile(s))"
            )
            return profiles

    draft_rows = gmail_drafter.list_recent_outreach_drafts(max_results=max(limit * 4, 25))
    if draft_rows:
        seen = set()
        draft_profiles = []
        for d in draft_rows:
            email = (d.get("to") or "").strip().lower()
            if not email or email in seen:
                continue
            seen.add(email)
            draft_profiles.append(
                {
                    "apollo_id": "",
                    "first_name": _infer_name_from_email(email),
                    "last_name": "",
                    "email": email,
                    "title": "",
                    "company_name": "",
                    "company_industry": "",
                    "company_city": "",
                    "company_state": "",
                    "company_domain": "",
                    "company_linkedin": "",
                    "company_description": "",
                    "homepage_snippet": "",
                    "linkedin_snippet": "",
                    "review_signals": [],
                    "review_sources": [],
                    "discovery_signals": [],
                    "company_employee_count": "",
                    "company_founded_year": "",
                }
            )
            if len(draft_profiles) >= limit:
                break
        if draft_profiles:
            logger.info(f"Preview source: Gmail drafts fallback ({len(draft_profiles)} profile(s))")
            return draft_profiles

    return []


def preview_three_samples(settings: RunSettings, logger: logging.Logger) -> list[dict]:
    """
    Generate 3 live sample emails from today's profiles using current prompt controls.
    Returns preview dicts for UI rendering.
    """
    profiles = _load_preview_profiles(limit=3, logger=logger, model=settings.openai_model)
    if not profiles:
        raise RuntimeError(
            "No preview data found. Generate at least one run/export, or keep outreach drafts in Gmail."
        )

    previews = []
    for profile in profiles:
        try:
            email = email_writer.write_email(
                profile,
                system_prompt=settings.email_system_prompt,
                model=settings.openai_model,
            )
            qa_ok, qa_issues = email_writer.validate_generated_email(email["body"], profile)
            if not qa_ok:
                logger.error(f"preview_unresolved_issues:{profile.get('email','')}:{','.join(qa_issues)}")

            previews.append(
                {
                    "to": profile.get("email", ""),
                    "company": profile.get("company_name", ""),
                    "subject": email.get("subject", ""),
                    "body": email.get("body", ""),
                    "issues": qa_issues,
                }
            )
        except Exception as exc:
            logger.error("preview_generation_failed:%s:%s", profile.get("email", ""), exc)
            previews.append(
                {
                    "to": profile.get("email", ""),
                    "company": profile.get("company_name", ""),
                    "subject": "",
                    "body": "",
                    "issues": ["preview_generation_failed"],
                }
            )

    return previews


def prompt_assistant_refine(settings: RunSettings, instruction: str, logger: logging.Logger) -> dict:
    """
    Refine the email system prompt based on user instruction.
    Returns dict with updated_prompt and assistant_text.
    """
    model = (settings.openai_model or config.OPENAI_MODEL).strip()
    current_prompt = settings.email_system_prompt

    system_msg = (
        "You are a prompt engineer for cold outreach emails. "
        "Rewrite prompts safely and precisely. "
        "Return strict JSON only with keys: updated_prompt, notes."
    )
    user_msg = (
        "Current prompt:\n"
        f"{current_prompt}\n\n"
        "Requested change:\n"
        f"{instruction}\n\n"
        "Constraints:\n"
        "- Keep overall intent, structure, and safety constraints unless user asks otherwise.\n"
        "- Keep concise and practical language.\n"
        "- Do not remove core guardrails accidentally.\n"
        "- Output JSON only."
    )

    try:
        response = prompt_assistant_client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg},
            ],
            max_completion_tokens=2500,
        )
        raw = (response.choices[0].message.content or "").strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
            if raw.endswith("```"):
                raw = raw[:-3]
            raw = raw.strip()

        parsed = json.loads(raw)
        updated_prompt = (parsed.get("updated_prompt") or "").strip()
        notes = (parsed.get("notes") or "").strip()
        if not updated_prompt:
            raise ValueError("Prompt assistant returned empty updated_prompt.")

        return {
            "updated_prompt": updated_prompt,
            "assistant_text": notes or "Prompt updated.",
        }
    except Exception as exc:
        logger.error(f"prompt_assistant_failed:{exc}")
        return {
            "updated_prompt": current_prompt,
            "assistant_text": (
                "I couldn't refine the prompt automatically this time. "
                "Try a shorter instruction or rerun.\n\n"
                f"Error: {exc}"
            ),
        }


def debug_evidence(settings: RunSettings, logger: logging.Logger, limit: int) -> int:
    """
    Evidence-only debug path.
    - No Gmail draft writes
    - Emits concise pass/fail summary
    - Writes detailed failures to evidence_failures_*.jsonl
    """
    limit = max(1, int(limit or 10))
    logger.info(f"debug_evidence_mode:true:limit={limit}")
    logger.info(f"debug_evidence_model:{config.EVIDENCE_MODEL}")

    # Prefer active rewrite leads first.
    raw_candidates = _load_active_rewrite_candidates(
        logger,
        model=config.EVIDENCE_MODEL,
        enrich_evidence=False,
        limit=limit,
    )
    profiles = [c["profile"] for c in raw_candidates]
    source = "rewrite_candidates"

    # Fallback to fresh Apollo fetch when no rewrite candidates.
    if not profiles:
        source = "apollo_fallback"
        if not config.APOLLO_API_KEY:
            logger.error("debug_evidence: no rewrite candidates and APOLLO_API_KEY missing.")
            return 1
        candidates = apollo_client.search_all_pages(max_pages=min(settings.pages, 1))
        top_ids = llm_filter.filter_and_rank(
            candidates,
            already_contacted=set(),
            max_picks=limit,
            model=settings.openai_model,
            extra_directions=settings.filter_extra_directions,
        )
        enriched = apollo_client.enrich_batch(top_ids[:limit])
        for person in enriched[:limit]:
            base_profile = _apollo_person_to_profile(person)
            profile = _prepare_profile_for_writing(
                base_profile,
                logger=logger,
                model=config.EVIDENCE_MODEL,
                force_refresh_homepage=True,
                enrich_evidence=False,
            )
            profiles.append(profile)

    logger.info(f"debug_evidence_source:{source}:profiles={len(profiles)}")
    if not profiles:
        logger.error("debug_evidence: no profiles available to evaluate.")
        return 1

    passes = 0
    fails = 0
    for idx, profile in enumerate(profiles, start=1):
        email = profile.get("email", "")
        company = profile.get("company_name", "")
        try:
            ev = research.enrich_evidence_with_llm(profile, model=config.EVIDENCE_MODEL)
            profile["evidence_pack"] = ev
            passes += 1
            logger.info(
                "evidence_debug_pass %d/%d | %s | conf=%.2f | proof=%d | impact=%d | social=%d | opener=%d | quality=%s",
                idx,
                len(profiles),
                email or company or "unknown",
                float(ev.get("confidence", 0.0) or 0.0),
                len(ev.get("proof_points") or []),
                len(ev.get("community_impact_signals") or []),
                len(ev.get("social_proof_signals") or []),
                len(ev.get("opener_angles") or []),
                ev.get("evidence_quality", "unknown"),
            )
        except Exception as exc:
            fails += 1
            _log_evidence_failure(logger, exc)
            logger.error(
                "evidence_debug_fail %d/%d | %s | %s",
                idx,
                len(profiles),
                email or company or "unknown",
                str(exc),
            )

    report = research.get_evidence_failure_report_path()
    logger.info(f"debug_evidence_summary:pass={passes}:fail={fails}:report={report}")
    return 0 if fails == 0 else 1


def debug_writer(
    settings: RunSettings,
    logger: logging.Logger,
    source: str,
    limit: int,
    use_judge: bool = True,
) -> int:
    """
    Writer-only debug path (no Gmail mutations).
    """
    limit = max(1, int(limit or 3))
    source = (source or "rewrite").strip().lower()
    logger.info(
        "debug_writer_mode:true:source=%s:limit=%d:use_judge=%s",
        source,
        limit,
        "true" if use_judge else "false",
    )

    profiles = []
    if source == "rewrite":
        candidates = _load_active_rewrite_candidates(
            logger,
            model=config.EVIDENCE_MODEL,
            enrich_evidence=True,
            limit=limit,
        )
        profiles = [c["profile"] for c in candidates]
    elif source == "csv":
        any_pattern = os.path.join(config.DAILY_CSV_DIR, "outreach_*.csv")
        profiles = _load_profiles_from_csv_pattern(any_pattern, limit=limit, model=config.EVIDENCE_MODEL)
    elif source == "apollo":
        if not config.APOLLO_API_KEY:
            logger.error("debug_writer: APOLLO_API_KEY missing for source=apollo.")
            return 1
        candidates = apollo_client.search_all_pages(max_pages=min(settings.pages, 1))
        top_ids = llm_filter.filter_and_rank(
            candidates,
            already_contacted=set(),
            max_picks=limit,
            model=settings.openai_model,
            extra_directions=settings.filter_extra_directions,
        )
        enriched = apollo_client.enrich_batch(top_ids[:limit])
        for person in enriched[:limit]:
            profiles.append(
                _prepare_profile_for_writing(
                    _apollo_person_to_profile(person),
                    logger=logger,
                    model=config.EVIDENCE_MODEL,
                    force_refresh_homepage=False,
                    enrich_evidence=True,
                )
            )
    else:
        logger.error("debug_writer: unsupported source '%s'. Use rewrite|csv|apollo.", source)
        return 1

    logger.info("debug_writer_profiles:%d", len(profiles))
    if not profiles:
        logger.error("debug_writer: no profiles available.")
        return 1

    generated = 0
    passed = 0
    skipped_collapse = 0
    skipped_evidence = 0
    skipped_weak_evidence = 0
    skipped_anchor = 0
    skipped_opener = 0
    skipped_coherence = 0
    skipped_format = 0
    skipped_other = 0

    for idx, profile in enumerate(profiles, start=1):
        generated += 1
        res = email_writer.debug_write_one(
            profile=profile,
            system_prompt=settings.email_system_prompt,
            model=settings.openai_model,
            use_judge=use_judge,
            emit_debug=True,
        )
        decision = res.get("final_decision", "skipped")
        category = (res.get("dominant_failure_category", "") or "").lower()
        stage = res.get("final_stage", "")
        issues = ",".join(res.get("final_issues", []) or [])
        logger.info(
            "debug_writer_contact %d/%d | %s | decision=%s stage=%s category=%s issues=%s",
            idx,
            len(profiles),
            profile.get("email", ""),
            decision,
            stage,
            category or "<none>",
            issues or "<none>",
        )
        if decision == "drafted":
            passed += 1
            continue
        if category == "collapse":
            skipped_collapse += 1
        elif category == "weak_evidence":
            skipped_weak_evidence += 1
        elif category in {"anchor_failed", "source_claim_untrue"}:
            skipped_anchor += 1
        elif category in {"opener_not_grounded", "opener_quality_failed"}:
            skipped_opener += 1
        elif category == "evidence":
            skipped_evidence += 1
        elif category == "coherence":
            skipped_coherence += 1
        elif category in {"format", "format_invalid"}:
            skipped_format += 1
        else:
            skipped_other += 1

    pass_rate = (passed / generated) if generated else 0.0
    logger.info(
        "debug_writer_summary:generated=%d passed=%d pass_rate=%.2f skipped_collapse=%d skipped_weak_evidence=%d skipped_anchor=%d skipped_opener=%d skipped_evidence=%d skipped_coherence=%d skipped_format=%d skipped_other=%d debug_report=%s",
        generated,
        passed,
        pass_rate,
        skipped_collapse,
        skipped_weak_evidence,
        skipped_anchor,
        skipped_opener,
        skipped_evidence,
        skipped_coherence,
        skipped_format,
        skipped_other,
        email_writer.get_writer_debug_report_path(),
    )
    return 0 if pass_rate >= 0.50 else 1


def debug_opener(
    settings: RunSettings,
    logger: logging.Logger,
    source: str,
    limit: int,
) -> int:
    """
    Opener-only debug path (no Gmail mutations, no full writer run).
    """
    limit = max(1, int(limit or 3))
    source = (source or "rewrite").strip().lower()
    logger.info("debug_opener_mode:true:source=%s:limit=%d", source, limit)

    profiles = []
    if source == "rewrite":
        candidates = _load_active_rewrite_candidates(
            logger,
            model=config.EVIDENCE_MODEL,
            enrich_evidence=True,
            limit=limit,
        )
        profiles = [c["profile"] for c in candidates]
    elif source == "csv":
        any_pattern = os.path.join(config.DAILY_CSV_DIR, "outreach_*.csv")
        profiles = _load_profiles_from_csv_pattern(any_pattern, limit=limit, model=config.EVIDENCE_MODEL)
    elif source == "apollo":
        if not config.APOLLO_API_KEY:
            logger.error("debug_opener: APOLLO_API_KEY missing for source=apollo.")
            return 1
        candidates = apollo_client.search_all_pages(max_pages=min(settings.pages, 1))
        top_ids = llm_filter.filter_and_rank(
            candidates,
            already_contacted=set(),
            max_picks=limit,
            model=settings.openai_model,
            extra_directions=settings.filter_extra_directions,
        )
        enriched = apollo_client.enrich_batch(top_ids[:limit])
        for person in enriched[:limit]:
            profiles.append(
                _prepare_profile_for_writing(
                    _apollo_person_to_profile(person),
                    logger=logger,
                    model=config.EVIDENCE_MODEL,
                    force_refresh_homepage=False,
                    enrich_evidence=True,
                )
            )
    else:
        logger.error("debug_opener: unsupported source '%s'. Use rewrite|csv|apollo.", source)
        return 1

    logger.info("debug_opener_profiles:%d", len(profiles))
    if not profiles:
        logger.error("debug_opener: no profiles available.")
        return 1

    weak = 0
    strong = 0
    for idx, profile in enumerate(profiles, start=1):
        evidence = profile.get("evidence_pack", {}) or {}
        opener = email_writer.debug_select_opener(profile)
        quality = str(evidence.get("evidence_quality", "weak"))
        if quality == "strong":
            strong += 1
        else:
            weak += 1
        logger.info(
            "debug_opener_contact %d/%d | %s | quality=%s persona=%s source=%s angle=%s",
            idx,
            len(profiles),
            profile.get("email", ""),
            quality,
            opener.get("persona_lens", ""),
            opener.get("source", ""),
            opener.get("opener_angle", ""),
        )
        logger.info("debug_opener_sentence:%s", opener.get("sentence", ""))

    logger.info("debug_opener_summary:strong=%d weak=%d", strong, weak)
    return 0 if strong > 0 else 1


def main():
    parser = argparse.ArgumentParser(description="AOM Cold Outreach Pipeline")
    parser.add_argument("--dry-run", action="store_true", help="Skip Gmail draft creation")
    parser.add_argument("--skip-drafts", action="store_true", help="Do everything except create drafts")
    parser.add_argument("--max", type=int, default=config.MAX_DAILY_EMAILS, help="Max emails to generate (1-200)")
    parser.add_argument("--pages", type=int, default=config.APOLLO_SEARCH_PAGES, help="Apollo search pages to pull (1-10)")
    parser.add_argument("--no-ui", action="store_true", help="Skip browser setup form and run with CLI/default settings")
    parser.add_argument("--debug-evidence", action="store_true", help="Run evidence extraction diagnostics only")
    parser.add_argument("--debug-writer", action="store_true", help="Run writer/QA diagnostics only (no Gmail changes)")
    parser.add_argument("--debug-writer-source", type=str, default="rewrite", help="Writer debug source: rewrite|csv|apollo")
    parser.add_argument("--debug-writer-limit", type=int, default=3, help="Limit records for writer debug mode")
    parser.add_argument("--debug-writer-no-judge", action="store_true", help="Disable judge pass in writer debug mode")
    parser.add_argument("--debug-opener", action="store_true", help="Run opener selection diagnostics only (no Gmail changes)")
    parser.add_argument("--debug-opener-source", type=str, default="rewrite", help="Opener debug source: rewrite|csv|apollo")
    parser.add_argument("--debug-opener-limit", type=int, default=3, help="Limit records for opener debug mode")
    parser.add_argument("--limit", type=int, default=10, help="Limit records for debug modes")
    parser.add_argument("--show-last-failure", action="store_true", help="Print concise latest evidence/writer failure digest")
    parser.add_argument("--failure-tail", type=int, default=5, help="How many failure records to show")
    parser.add_argument("--debug-rewrite-match", action="store_true", help="Debug CSV-vs-Gmail rewrite matching")
    args = parser.parse_args()

    log_file = setup_logging()
    logger = logging.getLogger("pipeline")

    print_banner()

    try:
        settings = _build_initial_settings(args)
    except ValueError as exc:
        logger.error(f"Invalid startup settings: {exc}")
        sys.exit(1)

    if args.show_last_failure:
        exit_code = show_last_failures(logger, tail=args.failure_tail)
        sys.exit(exit_code)

    if args.debug_rewrite_match:
        exit_code = debug_rewrite_match(logger, tail=args.failure_tail)
        sys.exit(exit_code)

    if args.debug_evidence:
        # Evidence debug mode is intentionally CLI-only and non-mutating.
        exit_code = debug_evidence(settings, logger, limit=args.limit)
        sys.exit(exit_code)

    if args.debug_writer:
        # Writer debug mode is intentionally CLI-only and non-mutating.
        exit_code = debug_writer(
            settings,
            logger,
            source=args.debug_writer_source,
            limit=args.debug_writer_limit,
            use_judge=(not args.debug_writer_no_judge),
        )
        sys.exit(exit_code)

    if args.debug_opener:
        exit_code = debug_opener(
            settings,
            logger,
            source=args.debug_opener_source,
            limit=args.debug_opener_limit,
        )
        sys.exit(exit_code)

    if not args.no_ui:
        choice = collect_run_settings(
            settings,
            preview_callback=lambda s: preview_three_samples(s, logger),
            preview_rewrite_callback=lambda s: preview_rewrite_batch(s, logger),
            prompt_assist_callback=lambda s, instruction: prompt_assistant_refine(s, instruction, logger),
        )
        if choice is None:
            print("Run canceled before execution.")
            sys.exit(0)
        settings = choice.settings
        selected_action = choice.action
    else:
        selected_action = "start"

    # ── Validate ──────────────────────────────────────────────────────────
    skip_gmail_checks = settings.dry_run or settings.skip_drafts
    require_apollo = (selected_action not in {"rewrite_today", "rewrite_all"}) and (not args.debug_evidence)
    if not validate_config(skip_gmail_checks=skip_gmail_checks, require_apollo=require_apollo):
        print("\nConfiguration errors found. Fix them and try again.")
        sys.exit(1)

    logger.info(f"Max emails this run: {settings.max_emails}")
    logger.info(f"Search pages this run: {settings.pages}")
    logger.info(f"Model this run: {settings.openai_model}")
    logger.info(f"Dry run: {settings.dry_run or settings.skip_drafts}")

    if selected_action == "rewrite_today":
        exit_code = rewrite_todays_drafts(settings, logger, limit=settings.rewrite_count)
        sys.exit(exit_code)
    if selected_action == "rewrite_all":
        exit_code = rewrite_todays_drafts(settings, logger, limit=None)
        sys.exit(exit_code)

    # ── Load contacts DB ──────────────────────────────────────────────────
    db = ContactsDB(config.CONTACTS_DB_PATH)
    already_contacted = db.get_all_ids()
    logger.info(f"Previously contacted: {db.contacted_count()} people")

    # ══════════════════════════════════════════════════════════════════════
    # STEP 1: Apollo Free Search
    # ══════════════════════════════════════════════════════════════════════
    print("\n--- STEP 1: Apollo People Search (free, no credits) ---")
    candidates = apollo_client.search_all_pages(max_pages=settings.pages)

    if not candidates:
        logger.error("No candidates found from Apollo search. Exiting.")
        sys.exit(1)

    logger.info(f"Found {len(candidates)} total candidates from Apollo")

    # ══════════════════════════════════════════════════════════════════════
    # STEP 2: LLM Filter & Rank
    # ══════════════════════════════════════════════════════════════════════
    print("\n--- STEP 2: LLM Filtering & Ranking ---")
    top_ids = llm_filter.filter_and_rank(
        candidates,
        already_contacted,
        max_picks=settings.max_emails,
        model=settings.openai_model,
        extra_directions=settings.filter_extra_directions,
    )

    if not top_ids:
        logger.error("LLM returned no prospects. Exiting.")
        sys.exit(1)

    logger.info(f"Top {len(top_ids)} prospects selected for enrichment")

    # ══════════════════════════════════════════════════════════════════════
    # STEP 3: Apollo Enrichment (costs credits)
    # ══════════════════════════════════════════════════════════════════════
    print(f"\n--- STEP 3: Apollo Enrichment ({len(top_ids)} credits) ---")
    enriched = apollo_client.enrich_batch(top_ids)

    if not enriched:
        logger.error("No enrichment results. Exiting.")
        sys.exit(1)

    logger.info(f"Successfully enriched {len(enriched)} contacts with emails")

    # ══════════════════════════════════════════════════════════════════════
    # STEP 4: Research Each Contact
    # ══════════════════════════════════════════════════════════════════════
    print(f"\n--- STEP 4: Researching {len(enriched)} contacts ---")
    profiles = []
    try:
        for i, person in enumerate(enriched):
            logger.info(
                "Researching %d/%d: %s %s at %s",
                i + 1,
                len(enriched),
                person.get("first_name", ""),
                person.get("last_name", ""),
                (person.get("organization") or {}).get("name", "Unknown"),
            )
            base_profile = _apollo_person_to_profile(person)
            profiles.append(
                _prepare_profile_for_writing(
                    base_profile,
                    logger=logger,
                    model=config.EVIDENCE_MODEL,
                    force_refresh_homepage=False,
                    enrich_evidence=True,
                )
            )
    except Exception as exc:
        _log_evidence_failure(logger, exc)
        sys.exit(1)
    logger.info(f"Research complete for {len(profiles)} contacts")

    # ══════════════════════════════════════════════════════════════════════
    # STEP 5: Write Personalized Emails
    # ══════════════════════════════════════════════════════════════════════
    print(f"\n--- STEP 5: Writing {len(profiles)} personalized emails ---")
    emails = email_writer.write_emails_batch(
        profiles,
        system_prompt=settings.email_system_prompt,
        model=settings.openai_model,
    )
    writer_stats = email_writer.get_last_batch_stats()
    logger.info(
        "writer_stats:generated=%d drafted=%d skipped_by_critical=%d skipped_by_judge=%d failed_other=%d",
        writer_stats.get("generated", 0),
        writer_stats.get("drafted", 0),
        writer_stats.get("skipped_by_critical", 0),
        writer_stats.get("skipped_by_judge", 0),
        writer_stats.get("failed_other", 0),
    )
    if not emails:
        logger.error("No emails passed quality checks. All contacts were skipped.")
        sys.exit(1)
    _annotate_scheduled_send(emails, logger)
    logger.info(f"Emails written: {len(emails)}")

    # ══════════════════════════════════════════════════════════════════════
    # STEP 6: Create Gmail Drafts
    # ══════════════════════════════════════════════════════════════════════
    draft_results = []
    if settings.dry_run or settings.skip_drafts:
        print("\n--- STEP 6: SKIPPED (dry run / skip-drafts mode) ---")
        draft_results = [{"to": e["profile"]["email"], "subject": e["subject"],
                          "draft_id": "DRY_RUN", "success": True} for e in emails]
    else:
        print(f"\n--- STEP 6: Creating {len(emails)} Gmail Drafts ---")
        signature_html = choose_signature_html(logger)
        draft_results = gmail_drafter.create_drafts_batch(emails, signature_html=signature_html)

    # ══════════════════════════════════════════════════════════════════════
    # STEP 7: Export & Update DB
    # ══════════════════════════════════════════════════════════════════════
    print("\n--- STEP 7: Exporting CSV & Updating Contacts DB ---")
    csv_path = csv_export.export_daily_batch(emails, draft_results)

    # Update contacts DB
    for item in emails:
        profile = item["profile"]
        db.add_contact(profile["apollo_id"], {
            "first_name": profile["first_name"],
            "last_name": profile["last_name"],
            "email": profile["email"],
            "company": profile["company_name"],
            "title": profile["title"],
        })
    db.log_run(len(emails), csv_path)
    db.save()

    # ══════════════════════════════════════════════════════════════════════
    # Summary
    # ══════════════════════════════════════════════════════════════════════
    successful_drafts = sum(1 for r in draft_results if r["success"])
    skipped_by_gmail = max(0, len(draft_results) - successful_drafts)
    writer_stats = email_writer.get_last_batch_stats()

    print()
    print("=" * 60)
    print("  PIPELINE COMPLETE")
    print("=" * 60)
    print(f"  Candidates searched:    {len(candidates)}")
    print(f"  LLM selected:           {len(top_ids)}")
    print(f"  Enriched (with email):  {len(enriched)}")
    print(f"  Generated:              {writer_stats.get('generated', 0)}")
    print(f"  Draft-ready:            {writer_stats.get('drafted', len(emails))}")
    print(f"  Skipped by critical:    {writer_stats.get('skipped_by_critical', 0)}")
    print(f"  Skipped by judge:       {writer_stats.get('skipped_by_judge', 0)}")
    print(f"  Failed other:           {writer_stats.get('failed_other', 0)}")
    if emails:
        print(f"  Planned send slot:     {emails[0].get('scheduled_for_local', 'N/A')}")
    print(f"  Drafts created:         {successful_drafts}")
    print(f"  Skipped by Gmail:       {skipped_by_gmail}")
    print(f"  CSV export:             {csv_path}")
    print(f"  Log file:               {log_file}")
    print(f"  Total contacts in DB:   {db.contacted_count()}")
    print("=" * 60)

    # Print email previews
    print("\n--- Email Previews ---")
    for i, item in enumerate(emails[:3]):  # show first 3
        p = item["profile"]
        print(f"\n[{i + 1}] To: {p['first_name']} {p['last_name']} <{p['email']}> at {p['company_name']}")
        print(f"    Subject: {item['subject']}")
        print(f"    Preview: {item['body'][:150]}...")

    if len(emails) > 3:
        print(f"\n    ... and {len(emails) - 3} more emails")

    print()
    logger.info("Pipeline finished successfully.")


if __name__ == "__main__":
    main()
