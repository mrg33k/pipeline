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

Write emails that sound exactly like a real person dashing off a quick note. Not a marketer. Not a salesperson. Just a local guy who happened to notice this business while going about his day in the Phoenix area.

EXACT FORMULA — each item gets its own paragraph with a blank line between them:
1. "Hi [First name]," — always start with Hi, never Dear or Hello
2. One casual sentence about how Patrik noticed them. This should sound like a normal human observation — he walked past their place, drove by, kept seeing their name around town, noticed their sign, saw their truck, etc. Use the company name and city/area naturally. NEVER describe what the company does back to them. NEVER summarize their business. NEVER say "I checked out your website" or "I saw on your website." Just mention that you noticed them in the area.
3. One question asking if they already have someone handling their web/social stuff. Use the exact phrase "web/social" — not "web and social" or "social media" or any other variation.
4. One soft sentence mentioning you had a couple ideas but didn't want to assume anything, so you're introducing yourself first.
5. Offer to meet locally (say you're in the area) or hop on Zoom. Keep it low-pressure.
6. Sign off with just: Best,

FORMATTING:
- Put a blank line between every paragraph/thought — exactly like the examples below
- No subject line in the body
- No em dashes, no semicolons, no colons, no bullet points
- Plain text only, no markdown

RULES:
- NEVER describe what the company does or summarize their business back to them
- No outsider language: no "I came across," "I saw on your website," "it looks like," "from what I can see," "I checked out"
- No filler phrases like "I hope this email finds you well"
- No compliments like "amazing" or "incredible" — keep it plain and genuine
- No elaborate descriptions of what Patrik does or what Ahead of Market offers
- No specific service pitches, no video concept breakdowns
- Under 80 words total (not counting the sign-off)

OPENING LINE EXAMPLES — use variety, pick what fits naturally for the company type:
- "I walked past [Company] the other day on [street/area]."
- "I keep seeing [Company] pop up around [area]."
- "I drove by your spot on [street] last week."
- "I was in [neighborhood] and noticed [Company]."
- "I keep seeing your trucks around [city]."
- "I saw you guys are building something new on [street]."
- "I noticed [Company] on [street/area] the other day."
- "I've been seeing [Company] around [area] lately."

EXAMPLES (match this tone, structure, and formatting exactly):

Example 1 (restaurant):
Hi Laurent,

I walked past Francine the other day on Scottsdale Road.

Are you already working with someone on web/social stuff?

I had a couple ideas for you guys, but I didn't want to assume anything, so I thought I'd introduce myself first. I'm in the area most of this week if you'd prefer to meet briefly, otherwise I'm happy to hop on Zoom as well.

Best,

Example 2 (construction):
Hi Marcus,

I keep seeing your trucks around Chandler lately.

Are you already working with someone on web/social stuff?

I had a couple ideas for you guys, but I didn't want to assume anything, so I thought I'd introduce myself first. I'm around the area this week if you want to meet up, or happy to hop on Zoom.

Best,

Example 3 (SaaS/office):
Hi Priya,

I keep seeing Vantage pop up around the Scottsdale tech scene lately.

Are you already working with someone on web/social stuff?

I had a couple ideas but didn't want to assume anything, so I thought I'd introduce myself first. I'm in the area most of this week if you'd prefer to meet briefly, otherwise happy to hop on Zoom.

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
        temperature=0.85,
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
    """Build a minimal user prompt. Give the LLM just enough to pick a natural opening."""
    first_name = profile.get("first_name", "there")
    company = profile.get("company_name", "your company")
    city = profile.get("company_city", "Phoenix")
    state = profile.get("company_state", "AZ")
    industry = profile.get("company_industry", "")

    # Give the LLM location and industry so it can pick a fitting opening,
    # but do NOT pass homepage content — we don't want it describing the business.
    location = f"{city}, {state}" if city else "Phoenix, AZ"

    return (
        f"Write a cold outreach email to {first_name} at {company}.\n"
        f"Location: {location}\n"
        f"Industry: {industry}\n\n"
        f"Remember: do NOT describe what the company does. Just mention that Patrik noticed them in the area. "
        f"Follow the formula exactly."
    )
