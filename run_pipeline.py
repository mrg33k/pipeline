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


# ═══════════════════════════════════════════════════════════════════════════════
# MODE 1: FULL RUN
# ═══════════════════════════════════════════════════════════════════════════════

def mode_full(db, max_emails, dry_run, log_file):
    """Full pipeline: Apollo search -> LLM filter -> enrich -> research -> write -> draft."""
    logger = logging.getLogger("pipeline.full")
    already_contacted = db.get_all_ids()
    logger.info(f"Previously tracked: {db.contacted_count()} contacts")

    # ── Step 1: Apollo Free Search ───────────────────────────────────────
    divider("STEP 1: Apollo People Search (free, no credits)")
    candidates = apollo_client.search_all_pages(max_pages=config.APOLLO_SEARCH_PAGES)

    if not candidates:
        logger.error("No candidates found from Apollo search. Exiting.")
        return

    logger.info(f"Found {len(candidates)} total candidates from Apollo")

    # ── Step 2: LLM Filter & Rank ────────────────────────────────────────
    divider("STEP 2: LLM Filtering & Ranking")
    top_ids = llm_filter.filter_and_rank(candidates, already_contacted, max_picks=max_emails)

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

    # ── Step 4: Research Each Contact ────────────────────────────────────
    divider(f"STEP 4: Researching {len(enriched)} contacts")
    profiles = research.research_batch(enriched)
    logger.info(f"Research complete for {len(profiles)} contacts")

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
    """Rewrite existing Gmail drafts with the latest email prompt/tone."""
    logger = logging.getLogger("pipeline.rewrite")

    drafted = db.get_drafted_contacts()
    if not drafted:
        print("No drafted contacts found in the database. Nothing to rewrite.")
        return

    # Limit
    to_rewrite = drafted[:max_emails]
    logger.info(f"Found {len(drafted)} drafted contacts, rewriting {len(to_rewrite)}")

    divider(f"REWRITING {len(to_rewrite)} EMAILS")

    rewritten = []
    for i, contact in enumerate(to_rewrite):
        # Build a profile dict from the stored contact data
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

        logger.info(f"Rewriting {i + 1}/{len(to_rewrite)}: {profile['first_name']} at {profile['company_name']}")

        # Write new email
        email = email_writer.write_email(profile)
        new_body = email["body"]
        new_subject = email["subject"]

        old_body = contact.get("emailed_body", "")
        print(f"\n  [{i + 1}] {profile['first_name']} {profile['last_name']} <{profile['email']}>")
        print(f"      Company: {profile['company_name']}")
        print(f"      Old draft ID: {contact.get('draft_id', 'N/A')}")

        if dry_run:
            print(f"      NEW EMAIL:")
            for line in new_body.split("\n"):
                print(f"        {line}")
        else:
            # Delete old draft and create new one
            old_draft_id = contact.get("draft_id", "")
            if old_draft_id:
                gmail_drafter.delete_draft(old_draft_id)
                logger.info(f"  Deleted old draft: {old_draft_id}")

            new_draft = gmail_drafter.create_draft(profile["email"], new_subject, new_body)
            if new_draft:
                db.update_draft(contact["id"], new_draft["id"], new_body)
                print(f"      [OK] New draft: {new_draft['id']}")
            else:
                print(f"      [FAIL] Could not create new draft")

        rewritten.append({"profile": profile, "subject": new_subject, "body": new_body})

    db.log_run("rewrite", len(rewritten))
    db.save()

    print()
    print("=" * 60)
    print("  REWRITE COMPLETE")
    print("=" * 60)
    print(f"  Drafts rewritten: {len(rewritten)}")
    print(f"  Log file:         {log_file}")
    print_db_stats(db)


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
    parser.add_argument("--max", type=int, default=config.MAX_DAILY_EMAILS,
                        help=f"Max emails to process (default: {config.MAX_DAILY_EMAILS})")
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

    # ── Run the selected mode ────────────────────────────────────────────
    if mode == "full":
        mode_full(db, args.max, args.dry_run, log_file)
    elif mode == "rewrite":
        mode_rewrite(db, args.max, args.dry_run, log_file)
    elif mode == "draft":
        mode_draft(db, args.max, args.dry_run, log_file)

    logger.info(f"Pipeline finished ({mode} mode).")


if __name__ == "__main__":
    main()
