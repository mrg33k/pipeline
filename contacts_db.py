"""
JSON-based contacts history tracker.
Tracks each contact through the pipeline lifecycle:
  - enriched: has email from Apollo
  - drafted: Gmail draft created
  - draft_id: Gmail draft ID (for rewrites)
  - emailed_body: last email body text (for rewrite comparison)
Prevents contacting the same person twice across runs.
Supports CSV import for contacts already pulled from Apollo.
"""

import csv
import json
import os
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class ContactsDB:
    """Tracks every contact through the outreach pipeline."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.data = self._load()

    # ── Persistence ──────────────────────────────────────────────────────

    def _load(self) -> dict:
        if os.path.exists(self.db_path):
            try:
                with open(self.db_path, "r") as f:
                    content = f.read().strip()
                    if content:
                        return json.loads(content)
            except (json.JSONDecodeError, ValueError) as e:
                logger.warning(f"Could not parse {self.db_path}: {e}. Starting fresh.")
        return {"contacts": {}, "runs": []}

    def save(self):
        with open(self.db_path, "w") as f:
            json.dump(self.data, f, indent=2, default=str)

    # ── Lookups ──────────────────────────────────────────────────────────

    def is_known(self, contact_id: str) -> bool:
        """Check if a contact (by Apollo ID or email key) is already tracked."""
        return contact_id in self.data["contacts"]

    def get_contact(self, contact_id: str) -> dict:
        return self.data["contacts"].get(contact_id, {})

    def get_all_ids(self) -> set:
        return set(self.data["contacts"].keys())

    def get_all_emails(self) -> set:
        """Return a set of all known contact email addresses (lowercase) for draft matching."""
        emails = set()
        for info in self.data["contacts"].values():
            email = info.get("email", "")
            if email:
                emails.add(email.lower())
        return emails

    def get_contact_by_email(self, email: str) -> dict:
        """Look up a contact by email address (case-insensitive). Returns dict or None."""
        email_lower = email.lower()
        for cid, info in self.data["contacts"].items():
            if info.get("email", "").lower() == email_lower:
                return {"id": cid, **info}
        return None

    def contacted_count(self) -> int:
        return len(self.data["contacts"])

    def _is_recent_contact(self, info: dict, cutoff: datetime) -> bool:
        """
        Check whether a contact has recent outreach activity in DB.
        """
        for field in ("sent_at", "drafted_at", "rewritten_at"):
            value = info.get(field, "")
            if not isinstance(value, str) or not value.strip():
                continue
            try:
                when = datetime.fromisoformat(value.strip())
            except ValueError:
                continue
            if when >= cutoff:
                return True
        return False

    def get_recent_contact_ids(self, hours: int = 48) -> set:
        """
        Return contact IDs with outreach activity in the last N hours.
        """
        if hours <= 0:
            return set()
        cutoff = datetime.now() - timedelta(hours=hours)
        ids = set()
        for cid, info in self.data["contacts"].items():
            if self._is_recent_contact(info, cutoff):
                ids.add(cid)
        return ids

    def get_recent_contact_emails(self, hours: int = 48) -> set:
        """
        Return contact emails with outreach activity in the last N hours.
        """
        if hours <= 0:
            return set()
        cutoff = datetime.now() - timedelta(hours=hours)
        emails = set()
        for info in self.data["contacts"].values():
            if not self._is_recent_contact(info, cutoff):
                continue
            email = (info.get("email") or "").strip().lower()
            if email:
                emails.add(email)
        return emails

    # ── Filtered queries ─────────────────────────────────────────────────

    def get_enriched_not_drafted(self) -> list:
        """Return contacts that have emails but no Gmail draft yet."""
        results = []
        for cid, info in self.data["contacts"].items():
            if info.get("enriched") and info.get("email") and not info.get("drafted"):
                results.append({"id": cid, **info})
        return results

    def get_drafted_contacts(self) -> list:
        """Return contacts that have Gmail drafts (for rewrite mode)."""
        results = []
        for cid, info in self.data["contacts"].items():
            if info.get("drafted") and info.get("draft_id"):
                results.append({"id": cid, **info})
        return results

    # ── Add / Update ─────────────────────────────────────────────────────

    def add_contact(self, contact_id: str, info: dict):
        """Add or update a contact. Merges with existing data."""
        existing = self.data["contacts"].get(contact_id, {})
        existing.update({
            "first_name": info.get("first_name", existing.get("first_name", "")),
            "last_name": info.get("last_name", existing.get("last_name", "")),
            "email": info.get("email", existing.get("email", "")),
            "company": info.get("company", existing.get("company", "")),
            "title": info.get("title", existing.get("title", "")),
            "industry": info.get("industry", existing.get("industry", "")),
            "city": info.get("city", existing.get("city", "")),
            "state": info.get("state", existing.get("state", "")),
            "domain": info.get("domain", existing.get("domain", "")),
        })
        if "added" not in existing:
            existing["added"] = datetime.now().isoformat()
        self.data["contacts"][contact_id] = existing

    def mark_enriched(self, contact_id: str, email: str):
        """Mark a contact as enriched (has email from Apollo)."""
        if contact_id in self.data["contacts"]:
            self.data["contacts"][contact_id]["enriched"] = True
            self.data["contacts"][contact_id]["email"] = email
            self.data["contacts"][contact_id]["enriched_at"] = datetime.now().isoformat()

    def mark_drafted(self, contact_id: str, draft_id: str, subject: str, body: str):
        """Mark a contact as having a Gmail draft."""
        if contact_id in self.data["contacts"]:
            self.data["contacts"][contact_id]["drafted"] = True
            self.data["contacts"][contact_id]["draft_id"] = draft_id
            self.data["contacts"][contact_id]["subject"] = subject
            self.data["contacts"][contact_id]["emailed_body"] = body
            self.data["contacts"][contact_id]["drafted_at"] = datetime.now().isoformat()

    def update_draft(self, contact_id: str, draft_id: str, body: str):
        """Update draft info after a rewrite."""
        if contact_id in self.data["contacts"]:
            self.data["contacts"][contact_id]["draft_id"] = draft_id
            self.data["contacts"][contact_id]["emailed_body"] = body
            self.data["contacts"][contact_id]["rewritten_at"] = datetime.now().isoformat()

    def log_run(self, mode: str, count: int, csv_path: str = ""):
        self.data["runs"].append({
            "date": datetime.now().isoformat(),
            "mode": mode,
            "contacts_processed": count,
            "csv_export": csv_path,
        })

    def create_list(self, list_name: str, emails: list[str]):
        """Create or replace a named list of contact emails."""
        name = (list_name or "").strip()
        if not name:
            return
        normalized = sorted({(e or "").strip().lower() for e in emails if (e or "").strip()})
        lists = self.data.setdefault("lists", {})
        lists[name] = {
            "name": name,
            "emails": normalized,
            "count": len(normalized),
            "created_at": datetime.now().isoformat(),
        }
        self.save()

    # ── CSV Import ───────────────────────────────────────────────────────

    def import_from_csv(self, csv_path: str) -> int:
        """
        Import contacts from a CSV file (e.g., Apollo export).
        Tries to map common column names. Returns count of new contacts imported.

        Expected columns (flexible matching):
          first_name, last_name, email, title, company/organization,
          industry, city, state, domain/website
        """
        if not os.path.exists(csv_path):
            logger.error(f"CSV file not found: {csv_path}")
            return 0

        # Column name mappings (lowercase key -> our field name)
        col_map = {
            "first_name": "first_name",
            "first name": "first_name",
            "last_name": "last_name",
            "last name": "last_name",
            "email": "email",
            "email address": "email",
            "title": "title",
            "job title": "title",
            "person title": "title",
            "company": "company",
            "company name": "company",
            "organization": "company",
            "organization name": "company",
            "industry": "industry",
            "company industry": "industry",
            "city": "city",
            "company city": "city",
            "state": "state",
            "company state": "state",
            "domain": "domain",
            "company domain": "domain",
            "website": "domain",
            "website url": "domain",
            "primary domain": "domain",
            "apollo_id": "apollo_id",
            "id": "apollo_id",
            "person id": "apollo_id",
        }

        imported = 0
        skipped = 0

        with open(csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)

            for row in reader:
                # Map columns
                mapped = {}
                for csv_col, value in row.items():
                    key = csv_col.strip().lower()
                    if key in col_map:
                        mapped[col_map[key]] = value.strip() if value else ""

                # Determine a unique ID: prefer apollo_id, fall back to email
                contact_id = mapped.get("apollo_id", "") or mapped.get("email", "")
                if not contact_id:
                    skipped += 1
                    continue

                # Skip if already in DB
                if self.is_known(contact_id):
                    skipped += 1
                    continue

                # Add the contact
                has_email = bool(mapped.get("email"))
                self.add_contact(contact_id, mapped)
                if has_email:
                    self.mark_enriched(contact_id, mapped["email"])

                imported += 1

        logger.info(f"CSV import: {imported} new contacts imported, {skipped} skipped (duplicates or no ID)")
        return imported

    # ── Stats ────────────────────────────────────────────────────────────

    def stats(self) -> dict:
        """Return summary stats about the contacts database."""
        contacts = self.data["contacts"]
        total = len(contacts)
        enriched = sum(1 for c in contacts.values() if c.get("enriched"))
        drafted = sum(1 for c in contacts.values() if c.get("drafted"))
        pending = sum(1 for c in contacts.values() if c.get("enriched") and not c.get("drafted"))
        return {
            "total": total,
            "enriched": enriched,
            "drafted": drafted,
            "pending_draft": pending,
        }
