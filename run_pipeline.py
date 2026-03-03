#!/usr/bin/env python3
"""
Ahead of Market - Cold Outreach Automation Pipeline
====================================================

Three modes:
    python3 run_pipeline.py                     # full run (default)
    python3 run_pipeline.py --mode full         # same as above
    python3 run_pipeline.py --mode rewrite      # rewrite existing Gmail drafts
    python3 run_pipeline.py --mode draft        # draft emails for enriched contacts without drafts

CSV import:
    python3 run_pipeline.py --import contacts.csv

Options:
    --max N         Limit to N emails (default: 25)
    --dry-run       Skip Gmail draft creation (preview only)
"""

import argparse
import logging
import os
import re
import sys
import time
from datetime import datetime

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


# ═══════════════════════════════════════════════════════════════════════════════
# Logging & UI
# ═══════════════════════════════════════════════════════════════════════════════

def setup_logging():
    os.makedirs(config.LOG_DIR, exist_ok=True)
    log_file = os.path.join(config.LOG_DIR, f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

    console_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
    file_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(console_fmt)

    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(file_fmt)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(console)
    root.addHandler(fh)

    return log_file


def banner(mode):
    print()
    print("=" * 60)
    print("  AHEAD OF MARKET - Cold Outreach Pipeline")
    print(f"  Mode: {mode.upper()}")
    print(f"  Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)
    print()


def divider(title):
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}\n")


def print_db_stats(db):
    s = db.stats()
    print(f"  Contacts in DB:     {s['total']}")
    print(f"  Enriched (w/email): {s['enriched']}")
    print(f"  Drafted:            {s['drafted']}")
    print(f"  Pending draft:      {s['pending_draft']}")
    print()


def validate_config(mode):
    """Check required config for the given mode."""
    errors = []
    if mode == "full" and not config.APOLLO_API_KEY:
        errors.append("APOLLO_API_KEY not set (required for full mode)")
    if not config.OPENAI_API_KEY:
        errors.append("OPENAI_API_KEY not set")
    if not os.path.exists(config.GMAIL_CLIENT_SECRET):
        errors.append(f"Gmail client secret not found: {config.GMAIL_CLIENT_SECRET}")
    if not os.path.exists(config.GMAIL_TOKENS_PATH):
        errors.append(f"Gmail tokens not found: {config.GMAIL_TOKENS_PATH}")
    if errors:
        for e in errors:
            logging.error(f"Config error: {e}")
        return False
    return True


def prompt_daily_focus(mode: str) -> str:
    """
    Ask for a simple optional daily outreach focus in full mode.
    Returns empty string when skipped or unavailable.
    """
    if mode != "full":
        return ""
    if not sys.stdin.isatty():
        return ""

    print("Optional daily focus")
    print("  Who do you feel like reaching out to today?")
    print("  Example: restaurant owners in Scottsdale")
    focus = input("  Focus (press Enter to skip): ").strip()
    print()
    return focus


# ═══════════════════════════════════════════════════════════════════════════════
# MODE 1: FULL RUN
# ═══════════════════════════════════════════════════════════════════════════════

def mode_full(db, max_emails, dry_run, log_file, daily_focus: str = ""):
    """Full pipeline: Apollo search -> LLM filter -> enrich -> research -> write -> draft."""
    logger = logging.getLogger("pipeline.full")
    already_contacted = db.get_all_ids()
    logger.info(f"Previously tracked: {db.contacted_count()} contacts")
    if daily_focus:
        logger.info(f"Daily outreach focus: {daily_focus}")

    # ── Step 1: Apollo Free Search ───────────────────────────────────────
    divider("STEP 1: Apollo People Search (free, no credits)")
    candidates = apollo_client.search_all_pages(
        max_pages=config.APOLLO_SEARCH_PAGES,
        daily_focus=daily_focus,
    )

    if not candidates:
        logger.error("No candidates found from Apollo search. Exiting.")
        return

    logger.info(f"Found {len(candidates)} total candidates from Apollo")

    # ── Step 2: LLM Filter & Rank ────────────────────────────────────────
    divider("STEP 2: LLM Filtering & Ranking")
    top_ids = llm_filter.filter_and_rank(
        candidates,
        already_contacted,
        max_picks=max_emails,
        daily_focus=daily_focus,
    )

    if not top_ids:
        logger.error("LLM returned no prospects. Exiting.")
        return

    logger.info(f"Top {len(top_ids)} prospects selected for enrichment")

    # ── Step 3: Apollo Enrichment (costs credits) ────────────────────────
    divider(f"STEP 3: Apollo Enrichment ({len(top_ids)} credits)")
    enriched = apollo_client.enrich_batch(top_ids)

    if not enriched:
        logger.error("No enrichment results. Exiting.")
        return

    logger.info(f"Successfully enriched {len(enriched)} contacts with emails")

    # ── Step 4: Build Profiles + Company Fact Research ───────────────────
    divider(f"STEP 4: Building profiles and researching {len(enriched)} contacts")
    profiles = []
    for i, person in enumerate(enriched):
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
        }

        logger.info(
            f"Researching {i + 1}/{len(enriched)}: "
            f"{profile.get('first_name', '')} {profile.get('last_name', '')} "
            f"at {profile.get('company_name', 'Unknown')}"
        )

        # Research step — get one fact about the company
        company_fact = ""
        website = (org.get("website_url", "") or "").strip()
        if website:
            company_fact = research.get_company_fact(website)
            if company_fact:
                logger.info(f"  Research fact: {company_fact}")
            else:
                logger.info("  No research fact found, using generic opener")
        else:
            logger.info("  No website found, using generic opener")

        profile["company_fact"] = company_fact
        profiles.append(profile)

    logger.info(f"Profile build + research complete for {len(profiles)} contacts")

    # Save enriched contacts to DB immediately (so --mode draft can use them later)
    for profile in profiles:
        db.add_contact(profile["apollo_id"], {
            "first_name": profile["first_name"],
            "last_name": profile["last_name"],
            "email": profile["email"],
            "company": profile["company_name"],
            "title": profile["title"],
            "industry": profile.get("company_industry", ""),
            "city": profile.get("company_city", ""),
            "state": profile.get("company_state", ""),
            "domain": profile.get("company_domain", ""),
        })
        db.mark_enriched(profile["apollo_id"], profile["email"])
    db.save()
    logger.info("Enriched contacts saved to DB")

    # ── Step 5: Write Personalized Emails ────────────────────────────────
    divider(f"STEP 5: Writing {len(profiles)} personalized emails")
    emails = email_writer.write_emails_batch(profiles)
    logger.info(f"Emails written: {len(emails)}")

    # ── Step 6: Create Gmail Drafts ──────────────────────────────────────
    if dry_run:
        divider("STEP 6: SKIPPED (dry run)")
        _print_email_previews(emails)
    else:
        divider(f"STEP 6: Creating {len(emails)} Gmail Drafts")
        for item in emails:
            profile = item["profile"]
            to_email = profile["email"]
            logger.info(f"Creating draft for {to_email}...")
            draft = gmail_drafter.create_draft(to_email, item["subject"], item["body"])
            if draft:
                db.mark_drafted(profile["apollo_id"], draft["id"], item["subject"], item["body"])
                print(f"  [OK] {profile['first_name']} {profile['last_name']} <{to_email}> -> draft {draft['id']}")
            else:
                print(f"  [FAIL] {profile['first_name']} {profile['last_name']} <{to_email}>")

    # ── Step 7: Export & Save ────────────────────────────────────────────
    divider("STEP 7: Exporting CSV & Saving")
    draft_results = []
    for item in emails:
        p = item["profile"]
        contact = db.get_contact(p["apollo_id"])
        draft_results.append({
            "to": p["email"],
            "subject": item["subject"],
            "draft_id": contact.get("draft_id", ""),
            "success": contact.get("drafted", False),
        })

    csv_path = csv_export.export_daily_batch(emails, draft_results)
    db.log_run("full", len(emails), csv_path)
    db.save()

    # ── Summary ──────────────────────────────────────────────────────────
    drafted_count = sum(1 for c in [db.get_contact(item["profile"]["apollo_id"]) for item in emails] if c.get("drafted"))
    print()
    print("=" * 60)
    print("  FULL RUN COMPLETE")
    print("=" * 60)
    print(f"  Candidates searched:    {len(candidates)}")
    print(f"  LLM selected:           {len(top_ids)}")
    print(f"  Enriched (with email):  {len(enriched)}")
    print(f"  Emails written:         {len(emails)}")
    print(f"  Drafts created:         {drafted_count}")
    print(f"  CSV export:             {csv_path}")
    print(f"  Log file:               {log_file}")
    print_db_stats(db)
    _print_email_previews(emails, max_show=3)


# ═══════════════════════════════════════════════════════════════════════════════
# MODE 2: REWRITE EXISTING DRAFTS
# ═══════════════════════════════════════════════════════════════════════════════

def mode_rewrite(db, max_emails, dry_run, log_file):
    """
    Rewrite existing Gmail drafts with the latest email prompt/tone.
    Pulls drafts DIRECTLY from Gmail — no dependency on contacts_history.json.

    Detection strategy:
    - PRIMARY: match draft recipient email against known Apollo contacts in DB
    - FALLBACK: if DB is empty, detect outreach drafts by body content phrases
    """
    logger = logging.getLogger("pipeline.rewrite")

    # Default: process ALL outreach drafts (no hard cap)
    # --max flag overrides; dry-run defaults to 3 for quick preview
    if dry_run and max_emails == 0:
        limit = 3  # sensible default for dry-run preview
    elif max_emails > 0:
        limit = max_emails
    else:
        limit = 9999  # effectively unlimited

    # Build set of known Apollo contact emails for primary detection
    known_emails = db.get_all_emails()  # returns set of lowercase email strings
    if known_emails:
        logger.info(f"Using email-match detection against {len(known_emails)} known contacts")
    else:
        logger.info("contacts_history.json is empty — using body-content fallback detection")

    divider("Connecting to Gmail and fetching outreach drafts...")
    outreach_drafts = gmail_drafter.get_outreach_drafts(
        max_results=200,
        known_emails=known_emails,
    )

    if not outreach_drafts:
        print("No outreach drafts found in Gmail.")
        if known_emails:
            print(f"  Checked {len(known_emails)} known Apollo contacts — none matched draft recipients.")
        else:
            print("  DB is empty. Tried body-content detection but found no matching phrases.")
            print("  Run --import contacts.csv first to populate the contact database.")
        return

    to_rewrite = outreach_drafts[:limit]
    logger.info(f"Found {len(outreach_drafts)} outreach drafts, processing {len(to_rewrite)}")

    if dry_run:
        divider(f"DRY RUN: Showing BEFORE/AFTER for {len(to_rewrite)} drafts (no changes saved)")
    else:
        divider(f"REWRITING {len(to_rewrite)} DRAFTS IN GMAIL")

    rewritten_count = 0
    failed_count = 0
    apollo_lookup_cache = {}

    def _clean_text(value) -> str:
        """Normalize nullable/non-string values to a safe stripped string."""
        if isinstance(value, str):
            return value.strip()
        return ""

    for i, draft_info in enumerate(to_rewrite):
        draft_id = draft_info["draft_id"]
        to_email = draft_info["to_email"]
        old_body = draft_info["body_text"]

        # Apollo-first lookup by recipient email (cached), then draft greeting, then fallback.
        email_key = (to_email or "").strip().lower()
        if email_key in apollo_lookup_cache:
            apollo_person = apollo_lookup_cache[email_key]
        else:
            apollo_person = apollo_client.lookup_by_email(to_email)
            apollo_lookup_cache[email_key] = apollo_person

        apollo_org = (apollo_person.get("organization", {}) or {}) if apollo_person else {}
        apollo_first_name = _clean_text(apollo_person.get("first_name", "") if apollo_person else "")
        header_first_name = _clean_text(draft_info.get("first_name", ""))
        if not header_first_name:
            to_name = _clean_text(draft_info.get("to_name", ""))
            if to_name:
                header_candidate = to_name.split()[0]
                if re.match(r"^[A-Za-z][A-Za-z'\-]*$", header_candidate):
                    header_first_name = header_candidate
        body_first_name = _extract_first_name_from_draft_body(old_body)
        first_name = apollo_first_name or header_first_name or body_first_name or "there"
        last_name = _clean_text(apollo_person.get("last_name", "") if apollo_person else "")

        old_subject = draft_info["subject"]

        # DB fallback fields (used when Apollo lookup has gaps).
        db_contact = db.get_contact_by_email(to_email)
        if db_contact:
            db_company = _clean_text(db_contact.get("company", ""))
            db_industry = _clean_text(db_contact.get("industry", ""))
            db_city = _clean_text(db_contact.get("city", ""))
            db_state = _clean_text(db_contact.get("state", ""))
            db_title = _clean_text(db_contact.get("title", ""))
            db_domain = _clean_text(db_contact.get("domain", ""))
        else:
            db_company = ""
            db_industry = ""
            db_city = ""
            db_state = ""
            db_title = ""
            db_domain = ""

        company = (
            _clean_text(apollo_org.get("name", ""))
            or db_company
            or _clean_text(draft_info.get("company", ""))
        )
        industry = _clean_text(apollo_org.get("industry", "")) or db_industry
        city = (
            _clean_text(apollo_org.get("city", ""))
            or _clean_text(apollo_person.get("city", "") if apollo_person else "")
            or db_city
        )
        state = (
            _clean_text(apollo_org.get("state", ""))
            or _clean_text(apollo_person.get("state", "") if apollo_person else "")
            or db_state
        )
        title = _clean_text(apollo_person.get("title", "") if apollo_person else "") or db_title
        domain = (
            _clean_text(apollo_org.get("primary_domain", ""))
            or _clean_text(apollo_org.get("website_url", ""))
            or db_domain
        )

        company_fact = ""
        website = _clean_text(apollo_org.get("website_url", ""))
        if website:
            company_fact = research.get_company_fact(website)
            if company_fact:
                logger.info(f"  Research fact: {company_fact}")
            else:
                logger.info("  No research fact found, using generic opener")

        # Build a minimal profile for the email writer
        profile = {
            "apollo_id": to_email,
            "first_name": first_name,
            "last_name": last_name,
            "email": to_email,
            "title": title,
            "company_name": company,
            "company_industry": industry,
            "company_city": city or "Phoenix",
            "company_state": state or "AZ",
            "company_domain": domain,
            "company_fact": company_fact,
        }

        logger.info(f"Processing {i + 1}/{len(to_rewrite)}: {first_name} at {company} <{to_email}>")

        # Write new email — opener variety is handled by the LLM prompt
        new_email = email_writer.write_email(profile)
        new_body = new_email["body"]

        # Subject line: always 'quick question' (simple, consistent)
        new_subject = "quick question"

        # ── Print before/after comparison ──────────────────────────────
        print(f"\n--- DRAFT {i + 1} of {len(to_rewrite)} ---")
        print(f"To:      {to_email}")
        print(f"Company: {company}")
        print(f"Old subject: {old_subject}")
        print(f"New subject: {new_subject}")
        print()
        print("BEFORE:")
        for line in old_body.split("\n"):
            print(f"  {line}")
        print()
        print("AFTER:")
        for line in new_body.split("\n"):
            print(f"  {line}")
        if dry_run:
            print()
            print("  [DRY RUN — no changes made to Gmail]")
            continue

        # ── Replace the draft in Gmail ───────────────────────────────────────
        updated = gmail_drafter.update_draft(draft_id, to_email, new_subject, new_body)
        if updated:
            rewritten_count += 1
            new_id = updated.get("id", draft_id)
            print(f"  [OK] Draft updated -> {new_id}")
            # Update contacts_history.json if this contact is tracked
            if db.is_known(to_email):
                db.update_draft(to_email, new_id, new_body)
        else:
            failed_count += 1
            print(f"  [FAIL] Could not update draft")

    if not dry_run:
        db.log_run("rewrite", rewritten_count)
        db.save()

    print()
    print("=" * 60)
    if dry_run:
        print("  REWRITE DRY RUN COMPLETE")
    else:
        print("  REWRITE COMPLETE")
    print("=" * 60)
    print(f"  Outreach drafts found: {len(outreach_drafts)}")
    print(f"  Drafts processed:      {len(to_rewrite)}")
    if not dry_run:
        print(f"  Successfully updated:  {rewritten_count}")
        print(f"  Failed:                {failed_count}")
    print(f"  Log file:              {log_file}")


# ═══════════════════════════════════════════════════════════════════════════════
# MODE 3: DRAFT REMAINING (enriched but not yet drafted)
# ═══════════════════════════════════════════════════════════════════════════════

def mode_draft(db, max_emails, dry_run, log_file):
    """Write emails and create drafts for contacts that are enriched but not yet drafted."""
    logger = logging.getLogger("pipeline.draft")

    pending = db.get_enriched_not_drafted()
    if not pending:
        print("No pending contacts found. All enriched contacts already have drafts.")
        print("Use --mode full to search for new contacts, or --import a CSV first.")
        return

    to_draft = pending[:max_emails]
    logger.info(f"Found {len(pending)} pending contacts, drafting {len(to_draft)}")

    # ── Research ─────────────────────────────────────────────────────────
    divider(f"STEP 1: Researching {len(to_draft)} contacts")
    profiles = []
    for contact in to_draft:
        # Build a profile from stored data (minimal research since we already have info)
        profile = {
            "apollo_id": contact["id"],
            "first_name": contact.get("first_name", ""),
            "last_name": contact.get("last_name", ""),
            "email": contact.get("email", ""),
            "title": contact.get("title", ""),
            "company_name": contact.get("company", ""),
            "company_industry": contact.get("industry", ""),
            "company_city": contact.get("city", ""),
            "company_state": contact.get("state", ""),
            "company_domain": contact.get("domain", ""),
        }
        profiles.append(profile)
        logger.info(f"  {profile['first_name']} {profile['last_name']} at {profile['company_name']}")

    # ── Write Emails ─────────────────────────────────────────────────────
    divider(f"STEP 2: Writing {len(profiles)} personalized emails")
    emails = email_writer.write_emails_batch(profiles)
    logger.info(f"Emails written: {len(emails)}")

    # ── Create Drafts ────────────────────────────────────────────────────
    if dry_run:
        divider("STEP 3: SKIPPED (dry run)")
        _print_email_previews(emails)
    else:
        divider(f"STEP 3: Creating {len(emails)} Gmail Drafts")
        for item in emails:
            profile = item["profile"]
            to_email = profile["email"]
            logger.info(f"Creating draft for {to_email}...")
            draft = gmail_drafter.create_draft(to_email, item["subject"], item["body"])
            if draft:
                db.mark_drafted(profile["apollo_id"], draft["id"], item["subject"], item["body"])
                print(f"  [OK] {profile['first_name']} {profile['last_name']} <{to_email}> -> draft {draft['id']}")
            else:
                print(f"  [FAIL] {profile['first_name']} {profile['last_name']} <{to_email}>")

    # ── Export & Save ────────────────────────────────────────────────────
    divider("STEP 4: Exporting CSV & Saving")
    draft_results = []
    for item in emails:
        p = item["profile"]
        contact = db.get_contact(p["apollo_id"])
        draft_results.append({
            "to": p["email"],
            "subject": item["subject"],
            "draft_id": contact.get("draft_id", ""),
            "success": contact.get("drafted", False),
        })

    csv_path = csv_export.export_daily_batch(emails, draft_results)
    db.log_run("draft", len(emails), csv_path)
    db.save()

    drafted_count = sum(1 for r in draft_results if r["success"])
    print()
    print("=" * 60)
    print("  DRAFT MODE COMPLETE")
    print("=" * 60)
    print(f"  Emails written:   {len(emails)}")
    print(f"  Drafts created:   {drafted_count}")
    print(f"  CSV export:       {csv_path}")
    print(f"  Log file:         {log_file}")
    print_db_stats(db)
    _print_email_previews(emails, max_show=3)


# ═══════════════════════════════════════════════════════════════════════════════
# CSV IMPORT
# ═══════════════════════════════════════════════════════════════════════════════

def do_import(db, csv_path):
    """Import contacts from a CSV file into the contacts database."""
    divider(f"IMPORTING: {csv_path}")

    if not os.path.exists(csv_path):
        print(f"Error: File not found: {csv_path}")
        sys.exit(1)

    print(f"  Before import:")
    print_db_stats(db)

    count = db.import_from_csv(csv_path)
    db.save()

    print(f"  Imported {count} new contacts.")
    print()
    print(f"  After import:")
    print_db_stats(db)

    s = db.stats()
    if s["pending_draft"] > 0:
        print(f"  Next step: run 'python3 run_pipeline.py --mode draft' to create emails for {s['pending_draft']} pending contacts.")


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _extract_first_name_from_draft_body(body_text: str) -> str:
    """
    Extract greeting first name from a draft body that starts with:
      Hi [Name],
    Returns empty string when missing/invalid (e.g., "Hi ," or "Hi,").
    """
    match = re.match(r"^\s*Hi\s+([A-Za-z][A-Za-z'\-]*)\s*,", body_text or "")
    return match.group(1) if match else ""


def _print_email_previews(emails, max_show=5):
    """Print preview of generated emails."""
    print("\n--- Email Previews ---")
    for i, item in enumerate(emails[:max_show]):
        p = item["profile"]
        print(f"\n[{i + 1}] To: {p['first_name']} {p['last_name']} <{p['email']}> at {p.get('company_name', '')}")
        print(f"    Subject: {item['subject']}")
        print()
        for line in item["body"].split("\n"):
            print(f"    {line}")
    if len(emails) > max_show:
        print(f"\n    ... and {len(emails) - max_show} more emails (see CSV export)")


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="AOM Cold Outreach Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Modes:
  full      Search Apollo, filter, enrich, write emails, create drafts (default)
  rewrite   Rewrite existing Gmail drafts with updated tone/prompt
  draft     Write emails + create drafts for enriched contacts without drafts

Import:
  --import FILE.csv   Import contacts from CSV into the tracking database

Examples:
  python3 run_pipeline.py                          # full run, 25 emails
  python3 run_pipeline.py --mode draft             # draft pending contacts
  python3 run_pipeline.py --mode rewrite           # rewrite all existing drafts
  python3 run_pipeline.py --mode draft --max 10    # draft 10 contacts
  python3 run_pipeline.py --import contacts.csv    # import CSV then exit
  python3 run_pipeline.py --mode full --dry-run    # preview without creating drafts
        """,
    )
    parser.add_argument("--mode", choices=["full", "rewrite", "draft"], default="full",
                        help="Pipeline mode (default: full)")
    parser.add_argument("--import", dest="import_csv", metavar="FILE",
                        help="Import contacts from a CSV file")
    parser.add_argument("--max", type=int, default=0,
                        help="Max emails to process (default: 25 for full/draft, unlimited for rewrite)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview emails without creating Gmail drafts")
    args = parser.parse_args()

    log_file = setup_logging()
    logger = logging.getLogger("pipeline")

    # Load contacts DB
    db = ContactsDB(config.CONTACTS_DB_PATH)

    # ── Handle CSV import ────────────────────────────────────────────────
    if args.import_csv:
        banner("IMPORT")
        do_import(db, args.import_csv)
        return

    # ── Validate config ──────────────────────────────────────────────────
    mode = args.mode
    banner(mode)

    print("Database status:")
    print_db_stats(db)

    if not validate_config(mode):
        print("\nConfiguration errors found. Fix them and try again.")
        sys.exit(1)

    # ── Resolve max emails per mode ────────────────────────────────────
    max_emails = args.max
    if max_emails == 0 and mode in ("full", "draft"):
        max_emails = config.MAX_DAILY_EMAILS  # default 25 for these modes
    # For rewrite mode, 0 means unlimited (handled inside mode_rewrite)
    daily_focus = prompt_daily_focus(mode)

    # ── Run the selected mode ────────────────────────────────────────
    if mode == "full":
        mode_full(db, max_emails, args.dry_run, log_file, daily_focus=daily_focus)
    elif mode == "rewrite":
        mode_rewrite(db, max_emails, args.dry_run, log_file)
    elif mode == "draft":
        mode_draft(db, max_emails, args.dry_run, log_file)

    logger.info(f"Pipeline finished ({mode} mode).")


if __name__ == "__main__":
    main()
