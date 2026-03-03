import os
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.dirname(BASE_DIR)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
APOLLO_API_KEY = os.getenv("APOLLO_API_KEY", "")

WRITER_MODEL = os.getenv("WRITER_MODEL", "gpt-5")
RESEARCH_MODEL = os.getenv("RESEARCH_MODEL", "gpt-4.1-mini")

LOG_DIR = os.path.join(BASE_DIR, "logs")
EXPORT_DIR = os.path.join(BASE_DIR, "daily_exports")
LEGACY_EXPORT_DIR = os.getenv("LEGACY_EXPORT_DIR", os.path.join(PROJECT_ROOT, "daily_exports"))
DEBUG_DIR = os.path.join(LOG_DIR, "debug")

GMAIL_CLIENT_SECRET = os.path.join(BASE_DIR, "client_secret.json")
GMAIL_TOKENS_PATH = os.path.join(BASE_DIR, "gmail_tokens.json")

WORD_LIMIT = 100
DEFAULT_REWRITE_LIMIT = 10
DEBUG_WRITER_LIMIT = 3

OUTREACH_SUBJECT_PREFIX = "Video for"
HARD_ASK_LINE = "Are you already working with someone on web/social stuff?"

EVIDENCE_MIN_CONFIDENCE = 0.45
