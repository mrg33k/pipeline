#!/usr/bin/env python3
import argparse
import csv
import glob
import json
import logging
import os
from dataclasses import asdict
from datetime import datetime
from typing import Dict, List

import requests

import config
from export import export_rows
from gmail_client import choose_signature_html, create_draft, list_recent_outreach_drafts, rewrite_draft
from logging_utils import setup_logging
from models import Profile
from research import build_research_card, gather_profile_context
from writer import write_email

logger = logging.getLogger(__name__)


DEFAULT_TONE_TEMPLATE = (
    "Paragraph 1: personalized, truthful opener that shows you understand what the business really does and why it matters. "
    "Paragraph 2: light direct question about web/social. "
    "Paragraph 3: you have ideas but do not want to assume, and offer nearby brief meet or Zoom."
)


def _collect_export_paths(include_legacy: bool = True) -> List[str]:
    paths = sorted(glob.glob(os.path.join(config.EXPORT_DIR, "outreach_*.csv")))
    if include_legacy:
        legacy_dir = (config.LEGACY_EXPORT_DIR or "").strip()
        if legacy_dir and os.path.abspath(legacy_dir) != os.path.abspath(config.EXPORT_DIR):
            paths.extend(sorted(glob.glob(os.path.join(legacy_dir, "outreach_*.csv"))))

    seen = set()
    ordered = []
    for path in paths:
        if path in seen:
            continue
        seen.add(path)
        ordered.append(path)
    return ordered


def _bootstrap_legacy_exports_if_needed() -> None:
    local_exports = sorted(glob.glob(os.path.join(config.EXPORT_DIR, "outreach_*.csv")))
    if local_exports:
        logger.info("legacy_seed_skip:local_exports_present:%d", len(local_exports))
        return

    legacy_dir = (config.LEGACY_EXPORT_DIR or "").strip()
    if not legacy_dir or os.path.abspath(legacy_dir) == os.path.abspath(config.EXPORT_DIR):
        logger.info("legacy_seed_skip:legacy_dir_not_configured")
        return

    legacy_paths = sorted(glob.glob(os.path.join(legacy_dir, "outreach_*.csv")))
    if not legacy_paths:
        logger.info("legacy_seed_skip:no_legacy_exports")
        return

    rows_by_email: Dict[str, Dict] = {}
    for path in legacy_paths:
        with open(path, "r", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                email = (row.get("email") or "").strip().lower()
                if email:
                    rows_by_email[email] = dict(row)

    if not rows_by_email:
        logger.info("legacy_seed_skip:legacy_rows_empty")
        return

    os.makedirs(config.EXPORT_DIR, exist_ok=True)
    seed_path = os.path.join(config.EXPORT_DIR, f"outreach_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.csv")
    columns = [
        "first_name",
        "last_name",
        "email",
        "title",
        "company",
        "industry",
        "city",
        "state",
        "domain",
        "draft_id",
        "subject",
        "body",
        "status",
        "skip_reason",
        "company_description",
        "company_linkedin",
    ]
    with open(seed_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for email in sorted(rows_by_email):
            row = rows_by_email[email]
            writer.writerow({k: row.get(k, "") for k in columns})

    logger.info(
        "legacy_seed_imported:unique_emails=%d legacy_files=%d seed_csv=%s",
        len(rows_by_email),
        len(legacy_paths),
        seed_path,
    )


def _profile_from_row(row: Dict) -> Profile:
    return Profile(
        first_name=(row.get("first_name") or "").strip() or "there",
        last_name=(row.get("last_name") or "").strip(),
        email=(row.get("email") or "").strip().lower(),
        title=(row.get("title") or "").strip(),
        company_name=(row.get("company") or row.get("company_name") or "").strip(),
        company_domain=(row.get("domain") or row.get("company_domain") or "").strip(),
        company_industry=(row.get("industry") or row.get("company_industry") or "").strip(),
        company_city=(row.get("city") or row.get("company_city") or "").strip(),
        company_state=(row.get("state") or row.get("company_state") or "").strip(),
        company_description=(row.get("company_description") or "").strip(),
        linkedin_url=(row.get("company_linkedin") or row.get("linkedin_url") or "").strip(),
        source_draft_id=(row.get("draft_id") or "").strip(),
    )


def _load_csv_profiles(path: str, limit: int = 0) -> List[Profile]:
    rows: List[Profile] = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            p = _profile_from_row(row)
            if p.email:
                rows.append(p)
    if limit > 0:
        return rows[:limit]
    return rows


def _load_latest_export_profiles(limit: int = 0) -> List[Profile]:
    csvs = _collect_export_paths(include_legacy=True)
    if not csvs:
        return []
    return _load_csv_profiles(csvs[-1], limit=limit)


def _apollo_fetch_profiles(limit: int = 25, page: int = 1) -> List[Profile]:
    if not config.APOLLO_API_KEY:
        logger.error("APOLLO_API_KEY missing; cannot run new mode with Apollo.")
        return []

    url = "https://api.apollo.io/v1/mixed_people/search"
    payload = {
        "api_key": config.APOLLO_API_KEY,
        "page": page,
        "per_page": min(limit, 100),
        "person_titles": ["Owner", "Founder", "CEO", "President"],
        "organization_locations": ["Phoenix, Arizona, United States"],
    }
    try:
        resp = requests.post(url, json=payload, timeout=25)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.error("apollo_fetch_failed:%s", exc)
        return []

    people = data.get("people", []) or []
    profiles: List[Profile] = []
    for p in people:
        org = p.get("organization", {}) or {}
        email = (p.get("email") or "").strip().lower()
        if not email:
            continue
        profiles.append(
            Profile(
                first_name=(p.get("first_name") or "").strip() or "there",
                last_name=(p.get("last_name") or "").strip(),
                email=email,
                title=(p.get("title") or "").strip(),
                company_name=(org.get("name") or "").strip(),
                company_domain=(org.get("primary_domain") or org.get("website_url") or "").strip(),
                company_industry=(org.get("industry") or "").strip(),
                company_city=(org.get("city") or "").strip(),
                company_state=(org.get("state") or "").strip(),
                company_description=(org.get("short_description") or org.get("seo_description") or "").strip(),
                linkedin_url=(org.get("linkedin_url") or p.get("linkedin_url") or "").strip(),
            )
        )
        if len(profiles) >= limit:
            break

    return profiles


def _rewrite_candidates(limit: int, rewrite_all: bool) -> List[Profile]:
    # Base set from exported rows (source of truth for company context)
    rows_by_email: Dict[str, Profile] = {}
    export_paths = _collect_export_paths(include_legacy=True)
    for path in export_paths:
        with open(path, "r", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                p = _profile_from_row(row)
                if p.email:
                    rows_by_email[p.email] = p

    # Pull all recent drafts, then intersect with our known list.
    live = list_recent_outreach_drafts(max_results=500, subject_prefix="")
    live_by_email: Dict[str, Dict] = {}
    for d in live:
        email = (d.get("to") or "").strip().lower()
        if email and email not in live_by_email:
            live_by_email[email] = d

    selected: List[Profile] = []
    live_not_in_list = 0
    for email, d in live_by_email.items():
        base = rows_by_email.get(email)
        if not base:
            # Strict rule: rewrite only contacts that exist in our list/history.
            live_not_in_list += 1
            continue
        base.source_draft_id = d.get("draft_id", "")
        selected.append(base)

    if not rewrite_all:
        selected = selected[: max(1, limit)]

    logger.info("rewrite_export_sources:%d", len(export_paths))
    logger.info("rewrite_known_people:%d", len(rows_by_email))
    logger.info("rewrite_live_drafts_seen:%d", len(live_by_email))
    logger.info("rewrite_live_not_in_list:%d", live_not_in_list)
    logger.info("rewrite_candidates_total:%d", len(selected))
    return selected


def _run_writer_for_profiles(profiles: List[Profile], model_writer: str, model_research: str, debug_only: bool, debug_limit: int = 3) -> Dict:
    os.makedirs(config.DEBUG_DIR, exist_ok=True)
    debug_path = os.path.join(config.DEBUG_DIR, f"writer_debug_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl")

    if debug_only:
        profiles = profiles[: max(1, debug_limit)]

    signature_html = "" if debug_only else choose_signature_html()

    summary = {"generated": 0, "drafted": 0, "skipped": 0}
    export_rows_out = []

    for idx, profile in enumerate(profiles, start=1):
        summary["generated"] += 1
        logger.info("lead:%d/%d:%s:%s", idx, len(profiles), profile.email, profile.company_name)

        try:
            profile = gather_profile_context(profile)
            card = build_research_card(profile)
            logger.info("research_quality:%s:%.2f", card.quality, card.confidence)

            result, trace = write_email(
                profile=profile,
                card=card,
                tone_template=DEFAULT_TONE_TEMPLATE,
                model=model_writer,
                polish=True,
            )
            card_payload = asdict(card)
            for stage in trace.get("writer_stage", []):
                logger.info("writer_stage:%s", stage)
        except Exception as exc:
            logger.exception("lead_processing_failed:%s:%s", profile.email, exc)
            summary["skipped"] += 1
            debug_row = {
                "email": profile.email,
                "company": profile.company_name,
                "research_card": {},
                "opener_attempt_1": "",
                "opener_attempt_2": "",
                "opener_selected": "",
                "hard_issues": ["lead_processing_error"],
                "final_decision": "skipped",
                "skip_reason": "lead_processing_error",
                "final_body": "",
                "error": str(exc),
            }
            with open(debug_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(debug_row, ensure_ascii=True) + "\n")
            export_rows_out.append(
                {
                    "first_name": profile.first_name,
                    "last_name": profile.last_name,
                    "email": profile.email,
                    "title": profile.title,
                    "company": profile.company_name,
                    "industry": profile.company_industry,
                    "city": profile.company_city,
                    "state": profile.company_state,
                    "domain": profile.company_domain,
                    "draft_id": profile.source_draft_id,
                    "subject": "",
                    "body": "",
                    "status": "skipped",
                    "skip_reason": "lead_processing_error",
                }
            )
            continue

        debug_row = {
            "email": profile.email,
            "company": profile.company_name,
            "research_card": card_payload,
            "writer_stage": trace.get("writer_stage", []),
            "opener_attempt_1": trace.get("opener_attempt_1", ""),
            "opener_attempt_2": trace.get("opener_attempt_2", ""),
            "opener_raw_1": trace.get("opener_raw_1", ""),
            "opener_raw_2": trace.get("opener_raw_2", ""),
            "opener_finish_reason_1": trace.get("opener_finish_reason_1", ""),
            "opener_finish_reason_2": trace.get("opener_finish_reason_2", ""),
            "opener_selected": trace.get("opener_selected", ""),
            "hard_issues": trace.get("hard_issues", []),
            "final_decision": result.status,
            "skip_reason": result.skip_reason,
            "final_body": trace.get("final_body", result.body),
        }
        with open(debug_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(debug_row, ensure_ascii=True) + "\n")

        if result.status != "drafted":
            summary["skipped"] += 1
            logger.info("final_decision:skipped:%s", result.skip_reason)
            logger.error("failed_email:%s reason=%s", profile.email, result.skip_reason)
            logger.error("failed_email_body_start")
            logger.error("%s", result.body or "<empty>")
            logger.error("failed_email_body_end")
            export_rows_out.append(
                {
                    "first_name": profile.first_name,
                    "last_name": profile.last_name,
                    "email": profile.email,
                    "title": profile.title,
                    "company": profile.company_name,
                    "industry": profile.company_industry,
                    "city": profile.company_city,
                    "state": profile.company_state,
                    "domain": profile.company_domain,
                    "draft_id": profile.source_draft_id,
                    "subject": result.subject,
                    "body": result.body,
                    "status": "skipped",
                    "skip_reason": result.skip_reason,
                }
            )
            continue

        draft_id = ""
        if not debug_only:
            if profile.source_draft_id:
                replaced = rewrite_draft(
                    draft_id=profile.source_draft_id,
                    to_email=profile.email,
                    subject=result.subject,
                    body=result.body,
                    signature_html=signature_html,
                )
                if replaced.get("success"):
                    draft_id = replaced.get("new_draft_id", "")
                else:
                    summary["skipped"] += 1
                    logger.info("final_decision:skipped:gmail_rewrite_failed")
                    continue
            else:
                created = create_draft(
                    to_email=profile.email,
                    subject=result.subject,
                    body=result.body,
                    signature_html=signature_html,
                )
                draft_id = created.get("draft_id", "")

        summary["drafted"] += 1
        logger.info("final_decision:drafted")

        export_rows_out.append(
            {
                "first_name": profile.first_name,
                "last_name": profile.last_name,
                "email": profile.email,
                "title": profile.title,
                "company": profile.company_name,
                "industry": profile.company_industry,
                "city": profile.company_city,
                "state": profile.company_state,
                "domain": profile.company_domain,
                "draft_id": draft_id,
                "subject": result.subject,
                "body": result.body,
                "status": "drafted",
                "skip_reason": "",
            }
        )

    csv_path = export_rows(config.EXPORT_DIR, export_rows_out)
    logger.info("writer_debug_jsonl:%s", debug_path)
    logger.info("csv_export:%s", csv_path)
    logger.info("summary:generated=%d drafted=%d skipped=%d", summary["generated"], summary["drafted"], summary["skipped"])
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Core V1 outreach pipeline")
    parser.add_argument("--mode", choices=["new", "rewrite"], default="new")
    parser.add_argument("--rewrite-all", action="store_true")
    parser.add_argument("--rewrite-limit", type=int, default=config.DEFAULT_REWRITE_LIMIT)
    parser.add_argument("--new-source", choices=["apollo", "csv"], default="apollo")
    parser.add_argument("--new-csv", type=str, default="")
    parser.add_argument("--new-limit", type=int, default=25)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--debug-writer", action="store_true")
    parser.add_argument(
        "--debug-writer-limit",
        type=int,
        nargs="?",
        const=config.DEBUG_WRITER_LIMIT,
        default=config.DEBUG_WRITER_LIMIT,
        help=f"Debug sample size (default {config.DEBUG_WRITER_LIMIT}). If provided without a value, defaults to {config.DEBUG_WRITER_LIMIT}.",
    )
    parser.add_argument("--model-writer", type=str, default=config.WRITER_MODEL)
    parser.add_argument("--model-research", type=str, default=config.RESEARCH_MODEL)
    args = parser.parse_args()

    log_path = setup_logging(config.LOG_DIR)
    logger.info("log_file:%s", log_path)
    _bootstrap_legacy_exports_if_needed()

    if not config.OPENAI_API_KEY:
        logger.error("OPENAI_API_KEY missing")
        return 1

    debug_only = bool(args.debug_writer or args.dry_run)

    if args.mode == "rewrite":
        profiles = _rewrite_candidates(limit=args.rewrite_limit, rewrite_all=args.rewrite_all)
        if not profiles:
            logger.error("No active outreach drafts found to rewrite.")
            return 1
        _run_writer_for_profiles(
            profiles=profiles,
            model_writer=args.model_writer,
            model_research=args.model_research,
            debug_only=debug_only,
            debug_limit=args.debug_writer_limit,
        )
        return 0

    # new mode
    if args.new_source == "csv":
        if args.new_csv:
            profiles = _load_csv_profiles(args.new_csv, limit=args.new_limit)
        else:
            profiles = _load_latest_export_profiles(limit=args.new_limit)
        if not profiles:
            logger.error("No profiles available from CSV source.")
            return 1
    else:
        profiles = _apollo_fetch_profiles(limit=args.new_limit)
        if not profiles:
            logger.error("No profiles returned from Apollo.")
            return 1

    _run_writer_for_profiles(
        profiles=profiles,
        model_writer=args.model_writer,
        model_research=args.model_research,
        debug_only=debug_only,
        debug_limit=args.debug_writer_limit,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
