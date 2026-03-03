"""
Configuration for the Ahead of Market cold outreach pipeline.
All settings, API keys, and constants in one place.
"""

import os
from dotenv import load_dotenv

# Load .env file if it exists
load_dotenv()

# ── API Keys ──────────────────────────────────────────────────────────────────
APOLLO_API_KEY = os.environ.get("APOLLO_API_KEY", "")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")

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

# ── OpenAI ────────────────────────────────────────────────────────────────────
OPENAI_MODEL = "gpt-4.1-mini"

# ── Sender Info ───────────────────────────────────────────────────────────────
SENDER_EMAIL = "hello@aom-inhouse.com"
SENDER_NAME = "Patrik Matheson"

# ── Email Signature (HTML) ────────────────────────────────────────────────────
EMAIL_SIGNATURE_HTML = """
<br><br>
<table cellpadding="0" cellspacing="0" border="0" style="font-family:Arial,Helvetica,sans-serif;font-size:13px;color:#333333;line-height:1.4;">
  <tr>
    <td style="padding-right:15px;vertical-align:top;">
      <img src="https://aheadofmarket.com/cdn/shop/files/patrik-headshot.jpg" alt="Patrik Matheson" width="90" height="90" style="border-radius:50%;display:block;" />
    </td>
    <td style="vertical-align:top;">
      <strong style="font-size:14px;color:#111111;">Patrik Matheson</strong><br>
      Digital Strategy<br>
      Video Marketing | Ahead of Market<br><br>
      <span style="font-size:12px;">
        <a href="tel:6023732164" style="color:#333333;text-decoration:none;">602.373.2164</a><br>
        <a href="mailto:Patrikmatheson@icloud.com" style="color:#1a73e8;text-decoration:none;">Patrikmatheson@icloud.com</a><br>
        <a href="https://aheadofmarket.com" style="color:#1a73e8;text-decoration:none;">aheadofmarket.com</a>
      </span>
      <br><br>
      <a href="https://aheadofmarket.com" style="display:inline-block;padding:6px 14px;background-color:#111111;color:#ffffff;text-decoration:none;border-radius:4px;font-size:12px;font-weight:bold;">Visit My Website</a>
    </td>
  </tr>
</table>
"""
