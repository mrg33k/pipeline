from __future__ import annotations
"""
Export daily batch results to CSV for record keeping.
"""

import csv
import os
import logging
from datetime import datetime

import config

logger = logging.getLogger(__name__)


def export_daily_batch(emails: list[dict], draft_results: list[dict]) -> str:
    """
    Export the day's outreach batch to a CSV file.
    Returns the path to the created CSV.
    """
    os.makedirs(config.DAILY_CSV_DIR, exist_ok=True)

    date_str = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    csv_path = os.path.join(config.DAILY_CSV_DIR, f"outreach_{date_str}.csv")

    # Build lookup for draft results
    draft_lookup = {}
    for dr in draft_results:
        draft_lookup[dr["to"]] = dr

    rows = []
    for item in emails:
        profile = item["profile"]
        dr = draft_lookup.get(profile["email"], {})
        rows.append({
            "date": datetime.now().strftime("%Y-%m-%d"),
            "first_name": profile.get("first_name", ""),
            "last_name": profile.get("last_name", ""),
            "email": profile.get("email", ""),
            "title": profile.get("title", ""),
            "company": profile.get("company_name", ""),
            "industry": profile.get("company_industry", ""),
            "city": profile.get("company_city", ""),
            "state": profile.get("company_state", ""),
            "domain": profile.get("company_domain", ""),
            "subject": item.get("subject", ""),
            "draft_id": dr.get("draft_id", ""),
            "draft_created": dr.get("success", False),
            "apollo_id": profile.get("apollo_id", ""),
        })

    if rows:
        fieldnames = list(rows[0].keys())
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        logger.info(f"CSV exported: {csv_path} ({len(rows)} rows)")
    else:
        logger.warning("No rows to export to CSV.")

    return csv_path
