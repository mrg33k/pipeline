"""
Simple JSON-based contacts history tracker.
Prevents contacting the same person twice across runs.
"""

import json
import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class ContactsDB:
    """Tracks every contact that has been drafted/emailed to avoid duplicates."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.data = self._load()

    def _load(self) -> dict:
        if os.path.exists(self.db_path):
            with open(self.db_path, "r") as f:
                return json.load(f)
        return {"contacts": {}, "runs": []}

    def save(self):
        with open(self.db_path, "w") as f:
            json.dump(self.data, f, indent=2, default=str)

    def is_contacted(self, apollo_id: str) -> bool:
        return apollo_id in self.data["contacts"]

    def add_contact(self, apollo_id: str, info: dict):
        self.data["contacts"][apollo_id] = {
            "added": datetime.now().isoformat(),
            "first_name": info.get("first_name", ""),
            "last_name": info.get("last_name", ""),
            "email": info.get("email", ""),
            "company": info.get("company", ""),
            "title": info.get("title", ""),
        }

    def log_run(self, count: int, csv_path: str):
        self.data["runs"].append({
            "date": datetime.now().isoformat(),
            "contacts_drafted": count,
            "csv_export": csv_path,
        })

    def contacted_count(self) -> int:
        return len(self.data["contacts"])

    def get_all_ids(self) -> set:
        return set(self.data["contacts"].keys())
