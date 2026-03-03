from __future__ import annotations
"""
Writes short, casual cold outreach emails using gpt-4.1-mini.
Emails follow Patrik's personal style: brief, human, no pitch, just an intro.
"""

import logging
import random
from openai import OpenAI

import config

logger = logging.getLogger(__name__)

client = OpenAI()

# ── Opener patterns for variety tracking ─────────────────────────────────────
# Each batch rotates through these so consecutive emails never share a pattern.
OPENER_PATTERNS = [
    "walked past",
    "drove by",
    "noticed",
    "keep seeing",
    "was in [area] and saw",
    "saw your sign",
    "keep seeing your name around",
    "been seeing",
]

SYSTEM_PROMPT = """You write cold outreach emails for Patrik Matheson, who runs a small creative studio in the Phoenix, AZ area. He does web and social video content for local businesses.

Write emails that sound exactly like a real person dashing off a quick note. Not a marketer. Not a salesperson. Just a local guy who noticed this person while going about his day in the Phoenix metro area.

EXACT FORMULA — each item gets its own paragraph with a blank line between them:
1. "Hi [First name]," — ALWAYS include the first name. Never just "Hi," alone. Never "Dear" or "Hello."
2. One casual sentence about how Patrik noticed THEM (the person or their work). This is about noticing the person or their presence in the area — NOT about their company name. See OPENING LINE rules below.
3. "Are you already working with someone on web/social stuff?" — use this EXACT phrasing. Always "web/social" — never "web and social" or "social media" or any other variation.
4. One soft sentence mentioning you had a couple ideas but didn't want to assume anything, so you're introducing yourself first.
5. Offer to meet locally (say you're in the area) or hop on Zoom. Keep it low-pressure.
6. Sign off with just: Best,

OPENING LINE RULES (critical):
- The opening line is about Patrik noticing the PERSON or their work/presence in the area. It is NOT about describing their company.
- NEVER describe what the company does or summarize their business back to them.
- NEVER use the company name if it is an abbreviation, acronym, or sounds corporate/unnatural (e.g. "LCRETW", "GTICL", "MREG", "CFJPOGP"). If the company name is short and natural-sounding (like "Francine" or "Los Portales"), you MAY use it — but you don't have to.
- When in doubt, skip the company name entirely and just reference "you guys" or "your spot" or "your work."
- NEVER invent or guess a specific street name, address, or intersection. Only reference a specific location if it was explicitly provided in the contact data below. If no specific street/address is provided, stay general: "around Phoenix", "in Scottsdale", "around the area", "in the area", etc.
- No outsider language: no "I came across," "I saw on your website," "it looks like," "from what I can see," "I checked out"

VARIETY (critical):
- You will be told which opening pattern to use. Follow it.
- Never start with "I keep seeing" more than once in a batch.
- Vary the phrasing naturally — walked past, drove by, noticed, was in the area and saw, keep seeing your name around, etc.

FORMATTING:
- Put a blank line between every paragraph/thought
- No subject line in the body
- No em dashes, no semicolons, no colons, no bullet points
- Plain text only, no markdown

RULES:
- ALWAYS include the recipient's first name after "Hi"
- NEVER describe what the company does or summarize their business
- No filler phrases like "I hope this email finds you well"
- No compliments like "amazing" or "incredible"
- No elaborate descriptions of what Patrik does or what Ahead of Market offers
- No specific service pitches, no video concept breakdowns
- Under 80 words total (not counting the sign-off)

EXAMPLES (match this tone, structure, and formatting exactly):

Example 1:
Hi Bryan,

You guys are doing great work from what I've seen around Scottsdale.

Are you already working with someone on web/social stuff?

I had a couple ideas for you guys, but I didn't want to assume anything, so I thought I'd introduce myself first. I'm in the area most of this week if you'd prefer to meet briefly, otherwise I'm happy to hop on Zoom as well.

Best,

Example 2:
Hi Marcus,

I keep seeing your trucks around Chandler lately.

Are you already working with someone on web/social stuff?

I had a couple ideas for you guys, but I didn't want to assume anything, so I thought I'd introduce myself first. I'm around the area this week if you want to meet up, or happy to hop on Zoom.

Best,

Example 3:
Hi Lauren,

I drove by your spot in Mesa last week.

Are you already working with someone on web/social stuff?

I had a couple ideas but didn't want to assume anything, so I thought I'd introduce myself first. I'm in the area most of this week if you'd prefer to meet briefly, otherwise happy to hop on Zoom.

Best,"""


def write_email(profile: dict, opener_hint: str = "") -> dict:
    """
    Write a short, casual email for one prospect.
    opener_hint: suggested opener pattern for variety (e.g. "walked past", "drove by")
    Returns dict with 'subject' and 'body'.
    """
    context = _build_context(profile, opener_hint)

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

    # Use company name for subject only if it's short and natural
    company = profile.get("company_name", "")
    if company and len(company) <= 30 and company.upper() != company and not _is_abbreviation(company):
        subject = f"quick question for {company}"
    else:
        first = profile.get("first_name", "")
        subject = f"quick question for {first}" if first else "quick question"

    logger.info(f"Email written for {profile.get('first_name', '?')} at {company}")

    return {
        "subject": subject,
        "body": body,
    }


def write_emails_batch(profiles: list[dict]) -> list[dict]:
    """Write emails for all profiles with rotating opener patterns for variety."""
    results = []
    # Shuffle opener patterns and cycle through them
    openers = list(OPENER_PATTERNS)
    random.shuffle(openers)

    for i, profile in enumerate(profiles):
        opener_hint = openers[i % len(openers)]
        logger.info(f"Writing email {i + 1}/{len(profiles)}: {profile.get('first_name', '?')} at {profile.get('company_name', '?')} (opener: {opener_hint})")
        email = write_email(profile, opener_hint=opener_hint)
        results.append({
            "profile": profile,
            "subject": email["subject"],
            "body": email["body"],
        })
    return results


def _is_abbreviation(name: str) -> bool:
    """Check if a company name looks like an abbreviation or acronym."""
    # All uppercase and short
    if name.upper() == name and len(name) <= 10:
        return True
    # Contains no vowels (likely abbreviation)
    vowels = set("aeiouAEIOU")
    if len(name) <= 8 and not any(c in vowels for c in name):
        return True
    return False


def _build_context(profile: dict, opener_hint: str = "") -> str:
    """Build a minimal user prompt with variety hint and real location data."""
    first_name = profile.get("first_name", "there")
    company = profile.get("company_name", "")
    city = profile.get("company_city", "")
    state = profile.get("company_state", "AZ")
    industry = profile.get("company_industry", "")
    address = profile.get("company_address", "")

    # Build location string from real data only
    location_parts = []
    if address:
        location_parts.append(f"Address: {address}")
    if city:
        location_parts.append(f"City: {city}")
    if state:
        location_parts.append(f"State: {state}")
    location_str = ", ".join(location_parts) if location_parts else "Phoenix, AZ area"

    # Company name note
    company_note = ""
    if company and (_is_abbreviation(company) or len(company) > 30):
        company_note = (
            f"\nNOTE: The company name '{company}' is an abbreviation or very long. "
            f"Do NOT use it in the email. Just say 'you guys' or 'your spot' instead."
        )
    elif company:
        company_note = (
            f"\nThe company name is '{company}'. You may use it if it sounds natural, "
            f"but you don't have to. When in doubt, skip it."
        )

    # Opener variety instruction
    opener_instruction = ""
    if opener_hint:
        opener_instruction = (
            f"\nOPENER PATTERN TO USE: '{opener_hint}' — work this naturally into the opening line. "
            f"Do not use it word-for-word if it sounds forced; adapt it to fit."
        )

    return (
        f"Write a cold outreach email to {first_name}.\n"
        f"Company: {company}\n"
        f"Location: {location_str}\n"
        f"Industry: {industry}\n"
        f"{company_note}\n"
        f"{opener_instruction}\n\n"
        f"IMPORTANT: The first name is {first_name}. Start with 'Hi {first_name},'\n"
        f"IMPORTANT: Do NOT invent any street names or specific addresses. Only use location info provided above.\n"
        f"IMPORTANT: Do NOT describe what the company does.\n"
        f"Follow the formula exactly."
    )
