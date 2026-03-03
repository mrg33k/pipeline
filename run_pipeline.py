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


def validate_config(mock_mode=False):
    """Check that required API keys and files exist."""
    errors = []
    if not mock_mode and not config.APOLLO_API_KEY:
        errors.append("APOLLO_API_KEY environment variable not set")
    if not config.OPENAI_API_KEY:
        errors.append("OPENAI_API_KEY environment variable not set")
    if not os.path.exists(config.GMAIL_CLIENT_SECRET):
        errors.append(f"Gmail client secret not found: {config.GMAIL_CLIENT_SECRET}")
    if not os.path.exists(config.GMAIL_TOKENS_PATH):
        errors.append(f"Gmail tokens not found: {config.GMAIL_TOKENS_PATH}")

    if errors:
        for e in errors:
            logging.error(f"Config error: {e}")
        return False
    return True


def main():
    parser = argparse.ArgumentParser(description="AOM Cold Outreach Pipeline")
    parser.add_argument("--dry-run", action="store_true", help="Skip Gmail draft creation")
    parser.add_argument("--skip-drafts", action="store_true", help="Do everything except create drafts")
    parser.add_argument("--max", type=int, default=config.MAX_DAILY_EMAILS, help="Max emails to generate")
    parser.add_argument("--pages", type=int, default=config.APOLLO_SEARCH_PAGES, help="Apollo search pages to pull")
    args = parser.parse_args()

    log_file = setup_logging()
    logger = logging.getLogger("pipeline")

    print_banner()

    # ── Validate ──────────────────────────────────────────────────────────
    if not validate_config():
        print("\nConfiguration errors found. Fix them and try again.")
        sys.exit(1)

    logger.info(f"Max emails this run: {args.max}")
    logger.info(f"Dry run: {args.dry_run or args.skip_drafts}")

    # ── Load contacts DB ──────────────────────────────────────────────────
    db = ContactsDB(config.CONTACTS_DB_PATH)
    already_contacted = db.get_all_ids()
    logger.info(f"Previously contacted: {db.contacted_count()} people")

    # ══════════════════════════════════════════════════════════════════════
    # STEP 1: Apollo Free Search
    # ══════════════════════════════════════════════════════════════════════
    print("\n--- STEP 1: Apollo People Search (free, no credits) ---")
    candidates = apollo_client.search_all_pages(max_pages=args.pages)

    if not candidates:
        logger.error("No candidates found from Apollo search. Exiting.")
        sys.exit(1)

    logger.info(f"Found {len(candidates)} total candidates from Apollo")

    # ══════════════════════════════════════════════════════════════════════
    # STEP 2: LLM Filter & Rank
    # ══════════════════════════════════════════════════════════════════════
    print("\n--- STEP 2: LLM Filtering & Ranking ---")
    top_ids = llm_filter.filter_and_rank(candidates, already_contacted, max_picks=args.max)

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
    profiles = research.research_batch(enriched)
    logger.info(f"Research complete for {len(profiles)} contacts")

    # ══════════════════════════════════════════════════════════════════════
    # STEP 5: Write Personalized Emails
    # ══════════════════════════════════════════════════════════════════════
    print(f"\n--- STEP 5: Writing {len(profiles)} personalized emails ---")
    emails = email_writer.write_emails_batch(profiles)
    logger.info(f"Emails written: {len(emails)}")

    # ══════════════════════════════════════════════════════════════════════
    # STEP 6: Create Gmail Drafts
    # ══════════════════════════════════════════════════════════════════════
    draft_results = []
    if args.dry_run or args.skip_drafts:
        print("\n--- STEP 6: SKIPPED (dry run / skip-drafts mode) ---")
        draft_results = [{"to": e["profile"]["email"], "subject": e["subject"],
                          "draft_id": "DRY_RUN", "success": True} for e in emails]
    else:
        print(f"\n--- STEP 6: Creating {len(emails)} Gmail Drafts ---")
        draft_results = gmail_drafter.create_drafts_batch(emails)

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

    print()
    print("=" * 60)
    print("  PIPELINE COMPLETE")
    print("=" * 60)
    print(f"  Candidates searched:    {len(candidates)}")
    print(f"  LLM selected:           {len(top_ids)}")
    print(f"  Enriched (with email):  {len(enriched)}")
    print(f"  Emails written:         {len(emails)}")
    print(f"  Drafts created:         {successful_drafts}")
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
