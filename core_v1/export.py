import csv
import os
from datetime import datetime
from typing import List, Dict, Any


def export_rows(export_dir: str, rows: List[Dict[str, Any]]) -> str:
    os.makedirs(export_dir, exist_ok=True)
    stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    path = os.path.join(export_dir, f"outreach_{stamp}.csv")
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
    ]
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in columns})
    return path
