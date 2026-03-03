
import logging
import sys
import os
from dotenv import load_dotenv
load_dotenv()

# Override model if gpt-5 is set (just in case)
if os.getenv("OPENAI_MODEL") == "gpt-5":
    os.environ["OPENAI_MODEL"] = "gpt-4o"

from email_writer import write_email

# Mock profile
profile = {
    "first_name": "John",
    "last_name": "Doe",
    "email": "john.doe@example.com",
    "company_name": "Phoenix Tech Solutions",
    "company_industry": "Information Technology",
    "company_city": "Phoenix",
    "company_state": "AZ",
    "title": "CEO",
    "evidence_pack": {
        "industry_label": "Software Development",
        "business_type": "B2B SaaS",
        "proof_points": ["Helping local businesses automate their workflows.", "Recently launched a new client portal."],
        "community_impact_signals": ["Active member of the Phoenix tech community."],
        "human_thanks": "I really appreciate how your team supports local businesses.",
        "language_cues": ["automation", "client portal", "workflow"],
        "confidence": 0.9,
        "source_tags": ["manual"]
    }
}

logging.basicConfig(level=logging.INFO)

print("--- TESTING CURRENT EMAIL WRITER ---")
try:
    result = write_email(profile)
    print("\nSUBJECT:", result.get("subject"))
    print("\nBODY:\n")
    print(result.get("body"))
except Exception as e:
    print(f"\nERROR: {e}")
