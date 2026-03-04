from __future__ import annotations
"""
Writes short, casual cold outreach emails using gpt-4.1-mini.
Emails follow Patrik's personal style: brief, human, no pitch, just an intro.
"""

import logging
import re
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
THE OPENER (first line after greeting):
- If a "Verified fact" is provided in the context, write ONE short casual sentence that shows you're aware of what they do. Use the fact naturally. Examples:
  - Fact: "Mexican restaurant" → "I've eaten at Los Portales a few times."
  - Fact: "pool service software for contractors" → "I know you guys work in the pool service space."
  - Fact: "commercial roofing company" → "I know you guys do a lot of roofing work around here."
  - Fact: "yoga studio" → "I've seen your studio around town."
- For trade-style facts (construction, roofing, concrete, plumbing, HVAC, electrical, contractor), prefer wording with "a lot of" when natural.
- If NO fact is provided, use a simple generic opener like "I came across you guys recently." or "Your name came up recently."
- NEVER invent details that are not in the verified fact.
- NEVER mention their website, Google, LinkedIn, or how you found them.
- NEVER describe their full business model or compliment them.
- NEVER use specific street names, neighborhoods, or addresses.
- Keep it to ONE sentence. Short. Casual.

COMPANY NAME RULES:
- NEVER use the company name if it is an abbreviation, acronym, or sounds corporate/unnatural (e.g. "LCRETW", "GTICL", "MREG", "CFJPOGP"). Use "you guys" instead.
- Only use the company name if it's short and natural-sounding (like "Francine" or "Los Portales").
- Never use long, formal company names in the opener.
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

SUBJECT_TEMPLATE = config.EMAIL_SUBJECT_TEMPLATE
_raw_subject_company_mode = (config.SUBJECT_COMPANY_MODE or "full").strip().lower()
SUBJECT_COMPANY_MODE = "first_token" if _raw_subject_company_mode in {
    "first",
    "first_word",
    "first_name",
    "first_token",
} else "full"


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
    body = _normalize_trade_opener(body, profile)

    subject = _build_subject(profile)

    company = profile.get("company_name", "")
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
    first_name = (profile.get("first_name") or "there").strip()
    if not first_name:
        first_name = "there"
    company = profile.get("company_name", "")
    industry = profile.get("company_industry", "")
    company_fact = (profile.get("company_fact") or "").strip()

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

    fact_line = f"Verified fact about their company: {company_fact}\n" if company_fact else ""

    return (
        f"Write a cold outreach email to {first_name}.\n"
        f"Company: {company}\n"
        f"Industry: {industry}\n"
        f"{fact_line}"
        f"{company_note}\n\n"
        f"IMPORTANT: Start with 'Hi {first_name},'\n"
        f"IMPORTANT: If a verified fact is provided, opener should use only that fact in one short casual sentence. "
        f"If no verified fact is provided, use a generic opener.\n"
        f"IMPORTANT: No walking, no driving, no streets, no neighborhoods, no physical locations. "
        f"Do not mention website, Google, LinkedIn, or how you found them.\n"
        f"IMPORTANT: NEVER invent details beyond the verified fact.\n"
        f"Follow the formula exactly."
    )


def _build_subject(profile: dict) -> str:
    """Build subject using run-configurable template or legacy fallback logic."""
    first_name = (profile.get("first_name") or "").strip()
    company_name = (profile.get("company_name") or "").strip()
    company_short = _first_business_name_token(company_name)
    company_for_subject = _company_name_for_subject(company_name)

    template = (SUBJECT_TEMPLATE or "").strip()
    if template:
        subject = template
        replacements = {
            "{first_name}": first_name or "there",
            "{company_name}": company_for_subject,
            "{company_short}": company_short or company_for_subject,
            "{company}": company_for_subject,
        }
        for key, value in replacements.items():
            subject = subject.replace(key, value)
        subject = re.sub(r"\s{2,}", " ", subject).strip()
        return subject or "quick question"

    # Legacy fallback: prefer natural company names (respecting subject mode), otherwise recipient first name.
    name_for_subject = company_for_subject or company_name
    if (
        name_for_subject
        and len(name_for_subject) <= 30
        and name_for_subject.upper() != name_for_subject
        and not _is_abbreviation(name_for_subject)
    ):
        return f"quick question for {name_for_subject}"

    return f"quick question for {first_name}" if first_name else "quick question"


def _company_name_for_subject(company_name: str) -> str:
    """
    Resolve which company label to use in subject:
    - full: original company name
    - first_token: first meaningful token from the business name
    """
    mode = (SUBJECT_COMPANY_MODE or "full").strip().lower()
    if mode in {"first", "first_word", "first_name", "first_token"}:
        first_token = _first_business_name_token(company_name)
        if first_token:
            return first_token
    return (company_name or "").strip()


def _first_business_name_token(company_name: str) -> str:
    """
    Return first meaningful token from business name.
    Example: "Francine Restaurant" -> "Francine".
    """
    tokens = re.findall(r"[A-Za-z0-9][A-Za-z0-9&'\-]*", company_name or "")
    if not tokens:
        return ""
    for token in tokens:
        if token.lower() in {"the", "a", "an"}:
            continue
        return token
    return tokens[0]


def _is_trade_fact(company_fact: str) -> bool:
    fact = (company_fact or "").lower()
    if not fact:
        return False
    trade_tokens = (
        "construction",
        "concrete",
        "roof",
        "plumb",
        "hvac",
        "electrical",
        "contractor",
    )
    return any(token in fact for token in trade_tokens)


def _normalize_trade_opener(body: str, profile: dict) -> str:
    """
    For trade contexts, normalize opener phrase:
    "I know you guys do X work around here." -> "I know you guys do a lot of X work around here."
    """
    company_fact = (profile.get("company_fact") or "").strip()
    if not _is_trade_fact(company_fact):
        return body

    lines = body.split("\n")
    opener_idx = None
    for idx, line in enumerate(lines):
        stripped = line.strip()
        if stripped.lower().startswith("hi "):
            continue
        if stripped:
            opener_idx = idx
            break
    if opener_idx is None:
        return body

    opener = lines[opener_idx].strip()
    pattern = re.compile(r"^(I know you guys do)\s+(?!a lot of\b)(.+)$", re.IGNORECASE)
    match = pattern.match(opener)
    if not match:
        return body

    new_opener = f"{match.group(1)} a lot of {match.group(2)}"
    lines[opener_idx] = new_opener
    return "\n".join(lines)
