"""
Configuration for the Ahead of Market cold outreach pipeline.
All settings, API keys, and constants in one place.
"""

import os
from dotenv import load_dotenv

# Load .env file if it exists
load_dotenv()


def _int_env(name: str, default: int) -> int:
    value = os.environ.get(name, str(default)).strip()
    try:
        return int(value)
    except ValueError:
        return default


# ── API Keys ──────────────────────────────────────────────────────────────────
APOLLO_API_KEY = os.environ.get("APOLLO_API_KEY", "")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")

# ── Gmail OAuth Scopes ────────────────────────────────────────────────────────
GMAIL_SCOPES = [
    "https://www.googleapis.com/auth/gmail.compose",
    "https://www.googleapis.com/auth/gmail.settings.basic",
    "https://www.googleapis.com/auth/gmail.readonly",
]

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONTACTS_DB_PATH = os.path.join(BASE_DIR, "contacts_history.json")
DAILY_CSV_DIR = os.path.join(BASE_DIR, "daily_exports")
GMAIL_CLIENT_SECRET = os.path.join(BASE_DIR, "client_secret.json")
GMAIL_TOKENS_PATH = os.path.join(BASE_DIR, "gmail_tokens.json")
LOG_DIR = os.path.join(BASE_DIR, "logs")

# ── Apollo Search Filters ─────────────────────────────────────────────────────
PERSON_TITLES = [
    "CEO",
    "Founder",
    "Co-Founder",
    "Owner",
    "CMO",
    "VP Marketing",
    "Director of Marketing",
    "Marketing Manager",
]

PERSON_SENIORITIES = [
    "owner",
    "founder",
    "c_suite",
    "vp",
    "director",
    "manager",
]

ORGANIZATION_LOCATIONS = [
    "Phoenix, Arizona, United States",
    "Scottsdale, Arizona, United States",
    "Mesa, Arizona, United States",
    "Tempe, Arizona, United States",
    "Chandler, Arizona, United States",
    "Glendale, Arizona, United States",
    "Peoria, Arizona, United States",
]

EMPLOYEE_RANGES = ["11,50", "51,200"]

# Industry keywords for targeted Apollo searches (each becomes a separate query)
INDUSTRY_KEYWORDS = [
    "restaurant",
    "hospitality",
    "hotel",
    "fitness gym",
    "real estate",
    "construction",
    "nonprofit",
    "events",
    "software",
    "wellness spa",
]

# ── Pipeline Limits ───────────────────────────────────────────────────────────
MAX_DAILY_EMAILS = 25
APOLLO_SEARCH_PAGES = 3          # pages to pull from free search (300 results)
APOLLO_SEARCH_PER_PAGE = 100     # max per page
RECENT_CONTACT_HOURS = _int_env("RECENT_CONTACT_HOURS", 48)

# ── OpenAI ────────────────────────────────────────────────────────────────────
OPENAI_MODEL = "gpt-4.1-mini"

# ── Sender Info ───────────────────────────────────────────────────────────────
SENDER_EMAIL = "hello@aom-inhouse.com"
SENDER_NAME = "Patrik Matheson"
