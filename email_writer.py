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

SYSTEM_PROMPT = """You write cold outreach emails for Patrik Matheson, who runs a small creative studio in the Phoenix, AZ area. He does web and social video content for local businesses.

Write emails that sound exactly like a real person dashing off a quick note. Not a marketer. Not a salesperson. Just a local guy who noticed this person.

EXACT FORMULA — each item gets its own paragraph with a blank line between them:
1. "Hi [First name]," — ALWAYS include the first name. Never just "Hi," alone. Never "Dear" or "Hello."
2. One simple ice breaker sentence acknowledging that Patrik came across them. See OPENER RULES below.
3. "Are you already working with someone on web/social stuff?" — use this EXACT phrasing. Always "web/social" — never "web and social" or "social media" or any other variation.
4. One soft sentence mentioning you had a couple ideas but didn't want to assume anything, so you're introducing yourself first.
5. Offer to meet locally (say you're in the area) or hop on Zoom. Keep it low-pressure.
6. Sign off with just: Best,

OPENER RULES (critical):
The opener is ONLY an ice breaker. It just acknowledges they exist. Nothing more.

GOOD openers — these work for ANY business type because they're vague and unchallengeable:
- "I came across you guys recently."
- "I've been seeing your name around lately."
- "You guys came up on my radar recently."
- "I came across your work the other day."
- "Your name keeps coming up."
- "I came across [Company] recently." (only if the name is short and natural-sounding)
- "You guys have been on my radar for a bit."
- "I came across [First name]'s work recently." (only if it sounds natural)

BAD openers — NEVER use any of these:
- "I walked past your spot" — too specific, might be wrong
- "I drove by" — doesn't work for SaaS, nonprofits, remote businesses
- "I noticed you guys on [street]" — never invent locations
- "I was in [neighborhood]" — too specific
- Any reference to a specific street, address, intersection, or neighborhood
- Any reference to walking, driving, or being physically somewhere
- "I checked out your website" — sounds researched, not natural
- "From what I saw on your website" — outsider language
- "It looks like you" — outsider language
- Describing what the company does back to them

VARIETY (critical): You MUST vary the opener. Never use the exact same opening sentence twice. Rotate through different phrasings:
- "I came across you guys recently."
- "I've been seeing your name around lately."
- "You guys came up on my radar recently."
- "I came across your work the other day."
- "Your name keeps coming up."
- "You guys have been on my radar for a bit."
Pick a different one each time. Do not default to the same phrase.

COMPANY NAME RULES:
- NEVER use the company name if it is an abbreviation, acronym, or sounds corporate/unnatural (e.g. "LCRETW", "GTICL", "MREG", "CFJPOGP"). Use "you guys" instead.
- Only use the company name if it's short and natural-sounding (like "Francine" or "Los Portales").
- NEVER describe what the company does or summarize their business.

LOCATION RULES:
- Do NOT reference any specific location in the opener.
- In the meeting offer (item 5), you CAN say "I'm in the area" or "I'm around the Phoenix area" — keep it general.
- NEVER invent or guess a specific street name, address, or neighborhood.

FORMATTING:
- Put a blank line between every paragraph/thought
- No subject line in the body
- No em dashes, no semicolons, no colons, no bullet points
- Plain text only, no markdown

RULES:
- ALWAYS include the recipient's first name after "Hi"
- Under 80 words total (not counting the sign-off)
- No filler phrases like "I hope this email finds you well"
- No compliments like "amazing" or "incredible"
- No elaborate descriptions of what Patrik does or what Ahead of Market offers
- No specific service pitches, no video concept breakdowns

EXAMPLES (match this tone, structure, and formatting exactly):

Example 1:
Hi Bryan,

I came across you guys recently.

Are you already working with someone on web/social stuff?

I had a couple ideas for you guys, but I didn't want to assume anything, so I thought I'd introduce myself first. I'm in the area most of this week if you'd prefer to meet briefly, otherwise I'm happy to hop on Zoom as well.

Best,

Example 2:
Hi Sarah,

Your name keeps coming up.

Are you already working with someone on web/social stuff?

I had a couple ideas but didn't want to assume anything, so I thought I'd introduce myself first. I'm around the area this week if you want to meet up, or happy to hop on Zoom.

Best,

Example 3:
Hi Marcus,

You guys came up on my radar recently.

Are you already working with someone on web/social stuff?

I had a couple ideas for you guys, but I didn't want to assume anything, so I thought I'd introduce myself first. I'm in the area most of this week if you'd prefer to meet briefly, otherwise happy to hop on Zoom.

Best,"""


def write_email(profile: dict, opener_hint: str = "") -> dict:
    """
    Write a short, casual email for one prospect.
    opener_hint is kept for API compatibility but no longer used for location-based openers.
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
    """Write emails for all profiles."""
    results = []
    for i, profile in enumerate(profiles):
        logger.info(f"Writing email {i + 1}/{len(profiles)}: {profile.get('first_name', '?')} at {profile.get('company_name', '?')}")
        email = write_email(profile)
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


def _build_context(profile: dict) -> str:
    """Build a minimal user prompt with only the data the LLM needs."""
    first_name = profile.get("first_name", "there")
    company = profile.get("company_name", "")
    industry = profile.get("company_industry", "")

    # Company name note
    if company and (_is_abbreviation(company) or len(company) > 30):
        company_note = (
            f"The company name '{company}' is an abbreviation or very long. "
            f"Do NOT use it in the email. Use 'you guys' instead."
        )
    elif company:
        company_note = (
            f"The company name is '{company}'. You may use it in the opener if it sounds natural and short, "
            f"but you don't have to. When in doubt, skip it and use 'you guys'."
        )
    else:
        company_note = "No company name available. Use 'you guys'."

    return (
        f"Write a cold outreach email to {first_name}.\n"
        f"Company: {company}\n"
        f"Industry: {industry}\n"
        f"{company_note}\n\n"
        f"IMPORTANT: Start with 'Hi {first_name},'\n"
        f"IMPORTANT: The opener must be a simple, vague ice breaker. "
        f"No walking, no driving, no streets, no neighborhoods, no physical locations.\n"
        f"IMPORTANT: Do NOT describe what the company does.\n"
        f"Follow the formula exactly."
    )
