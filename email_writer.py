from __future__ import annotations
"""
Writes short, casual cold outreach emails using gpt-4.1-mini.
Emails follow Patrik's personal style: brief, human, no pitch, just an intro.
"""

import logging
from openai import OpenAI

import config

logger = logging.getLogger(__name__)

client = OpenAI()

SYSTEM_PROMPT = """You write cold outreach emails for Patrik Matheson, who runs Ahead of Market, a small creative studio in the Phoenix, AZ area that does web and social video content for local businesses.

Write emails that sound exactly like a real person dashing off a quick note. Not a marketer. Not a salesperson. Patrik is a local who already knows this business and their work. Write from that perspective — familiar, not like an outsider who stumbled across their website.

EXACT FORMULA — each item gets its own paragraph with a blank line between them:
1. "Hi [First name]," — always start with Hi, never Dear or Hello
2. One casual sentence acknowledging what they do. Write as if Patrik already knows their work firsthand. No "I checked out your website" or "it looks like" or "from what I saw online" — those sound like an outsider. Just a plain, familiar observation.
3. One question asking if they already have someone handling their web/social stuff. Use the exact phrase "web/social" — not "web and social" or "social media" or any other variation.
4. One soft sentence mentioning you had a couple ideas but didn't want to assume anything, so you're introducing yourself first.
5. Offer to meet locally (say you're in the area) or hop on Zoom. Keep it low-pressure.
6. Sign off with just: Best,

FORMATTING:
- Put a blank line between every paragraph/thought — exactly like the example below
- No subject line in the body
- No em dashes, no semicolons, no colons, no bullet points
- Plain text only, no markdown

RULES:
- No elaborate descriptions of what Patrik does or what Ahead of Market offers
- No specific service pitches, no video concept breakdowns
- No outsider language: no "I came across," "I noticed," "I saw on your website," "it looks like," "from what I can see"
- No filler phrases like "I hope this email finds you well"
- No compliments like "amazing" or "incredible" — keep it plain and genuine
- Under 80 words total (not counting the sign-off)

EXAMPLE (match this tone, structure, and formatting exactly):
Hi Bryan,

You guys are doing great work helping hotels from what I saw on the website.

Are you already working with someone on web/social stuff?

I had a couple ideas for you guys, but I didn't want to assume anything, so I thought I'd introduce myself first. I'm in the area most of this week if you'd prefer to meet briefly, otherwise I'm happy to hop on Zoom as well.

Best,"""


def write_email(profile: dict) -> dict:
    """
    Write a short, casual email for one prospect.
    Returns dict with 'subject' and 'body'.
    """
    context = _build_context(profile)

    response = client.chat.completions.create(
        model=config.OPENAI_MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": context},
        ],
        temperature=0.8,
        max_tokens=200,
    )

    body = response.choices[0].message.content.strip()

    company = profile.get("company_name", "your company")
    subject = f"quick question for {company}"

    logger.info(f"Email written for {profile['first_name']} {profile['last_name']} at {company}")

    return {
        "subject": subject,
        "body": body,
    }


def write_emails_batch(profiles: list[dict]) -> list[dict]:
    """Write emails for all profiles. Returns list of dicts with subject, body, and profile."""
    results = []
    for i, profile in enumerate(profiles):
        logger.info(f"Writing email {i + 1}/{len(profiles)}: {profile['first_name']} at {profile['company_name']}")
        email = write_email(profile)
        results.append({
            "profile": profile,
            "subject": email["subject"],
            "body": email["body"],
        })
    return results


def _build_context(profile: dict) -> str:
    """Build a minimal user prompt. The LLM only needs the name, company, and a brief description."""
    first_name = profile.get("first_name", "there")
    company = profile.get("company_name", "your company")

    # Build a one-line company description from available data
    description_parts = []
    if profile.get("homepage_snippet"):
        description_parts.append(profile["homepage_snippet"][:200])
    elif profile.get("company_description"):
        description_parts.append(profile["company_description"][:200])

    industry = profile.get("company_industry", "")
    city = profile.get("company_city", "")

    description = description_parts[0] if description_parts else f"a {industry} business in {city}".strip(" in")

    return (
        f"Write a cold outreach email to {first_name} at {company}.\n"
        f"Brief company context: {description}\n\n"
        f"Keep it casual, short, and human. Follow the formula exactly."
    )
