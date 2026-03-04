from __future__ import annotations
"""
Writes short, casual cold outreach emails using gpt-4.1-mini.
Emails follow Patrik's personal style: brief, human, no pitch, just an intro.
"""

import logging
import random
import re
from openai import OpenAI

import config

logger = logging.getLogger(__name__)

client = OpenAI()

SYSTEM_PROMPT = """You write cold outreach emails for Patrik Matheson, who runs a small creative studio in the Phoenix, AZ area. He does web and social video content for local businesses.

Write emails that sound exactly like a real person dashing off a quick note. Not a marketer. Not a salesperson. Just a local guy who noticed this person.

EMAIL FORMULA — each item gets its own paragraph with a blank line between them:

1. "Hi [First name]," — ALWAYS include the first name. Never "Hi," alone. If no name available, use "Hi there,".

2. THE OPENER: One short casual sentence acknowledging what they do. Keep the industry description to 1-3 words. Do not elaborate. Do not compliment. Do not describe their full business. Examples:
   - "I know you guys do concrete work."
   - "I've eaten at [restaurant name] a few times."
   - "I know you guys work in the pool service space."
   - "I've seen your studio around town."
   - If no company fact is provided, use: "I came across you guys recently." or "Your name came up recently."
   RULES: NEVER invent details not in the verified fact. NEVER mention their website. NEVER use more than one sentence. NEVER use adjectives like "great", "amazing", "impressive".

3. THE IDEA TEASE: One sentence that hints you have a relevant idea for them. An "idea tease" sentence will be provided in the context — use it exactly as given or adapt it very slightly to sound natural. Do NOT name a specific service. Do NOT say "video", "content creation", "marketing", "social media management", or any service name. Just tease that you have an idea.

4. THE CLOSE: "I didn't want to assume anything, so I thought I'd introduce myself first. I'm around the Phoenix area this week if you want to meet up, or happy to hop on Zoom."

5. Sign off with just: Best,

IMPORTANT: Do NOT include "Are you already working with someone on web/social stuff?" or any variation of that question. It is removed from the email entirely.

COMPANY NAME RULES:
- NEVER use the company name if it is an abbreviation, acronym, or sounds corporate/unnatural (e.g. "LCRETW", "GTICL", "MREG", "CFJPOGP"). Use "you guys" instead.
- Only use the company name if it's short and natural-sounding (like "Francine" or "Los Portales").
- Never use long, formal company names in the opener.
- NEVER describe what the company does or summarize their business.

LOCATION RULES:
- Do NOT reference any specific location in the opener.
- In the close sentence, keep location phrasing general.
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
- No specific service pitches

EXAMPLES (match this tone, structure, and formatting exactly):

Example 1:
Hi Paul,

I know you guys do concrete work.

I had an idea for showing off some of your project work.

I didn't want to assume anything, so I thought I'd introduce myself first. I'm around the Phoenix area this week if you want to meet up, or happy to hop on Zoom.

Best,

Example 2:
Hi Bryan,

I've eaten at Francine a few times.

I had an idea around keeping your social and menu content looking sharp.

I didn't want to assume anything, so I thought I'd introduce myself first. I'm around the Phoenix area this week if you want to meet up, or happy to hop on Zoom.

Best,

Example 3:
Hi Priya,

I know you guys work in the software space.

I had a thought about making what you guys built easier to understand at a glance.

I didn't want to assume anything, so I thought I'd introduce myself first. I'm around the Phoenix area this week if you want to meet up, or happy to hop on Zoom.

Best,"""

IDEA_TEASES = {
    "construction": [
        "I had an idea for showing off some of your project work.",
        "I had a thought about helping you guys stand out when you're bidding on jobs.",
        "I had an idea around helping your crew's work get the attention it deserves.",
    ],
    "restaurant": [
        "I had an idea around keeping your social and menu content looking sharp.",
        "I had a thought about getting your food looking as good online as it does in person.",
        "I had an idea for keeping your social presence fresh without adding to your plate.",
    ],
    "tech": [
        "I had an idea for showing off what your product does without it feeling like a sales pitch.",
        "I had a thought about making what you guys built easier to understand at a glance.",
        "I had an idea for showing people how your product actually works day to day.",
    ],
    "wellness": [
        "I had an idea for capturing what your space actually feels like.",
        "I had a thought about showing people the experience before they walk in.",
        "I had an idea around getting more people to see what you guys are about.",
    ],
    "real_estate": [
        "I had an idea for making your listings stand out before people even schedule a tour.",
        "I had a thought about showing off your properties in a way that stops the scroll.",
        "I had an idea around helping your brand stand out in a crowded market.",
    ],
    "hospitality": [
        "I had an idea for showing off the experience you guys deliver.",
        "I had a thought about capturing what makes your place worth visiting.",
        "I had an idea around helping more people discover what you guys are doing.",
    ],
    "trades": [
        "I had an idea for showing off some of your project work.",
        "I had a thought about helping you guys recruit better crews.",
        "I had an idea around making your work speak for itself online.",
    ],
    "nonprofit": [
        "I had an idea for helping more people understand the work you guys do.",
        "I had a thought about telling your story in a way that actually moves people.",
        "I had an idea around getting your mission in front of more of the right people.",
    ],
    "default": [
        "I had a couple ideas for you guys.",
        "I had a thought I wanted to run by you.",
        "I had an idea I think could be useful for you guys.",
    ],
}

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
    first_name = (profile.get("first_name") or "there").strip()
    if not first_name:
        first_name = "there"
    company = profile.get("company_name", "")
    company_fact = profile.get("company_fact", "")
    industry = profile.get("industry", "")
    city = profile.get("city", "")
    state = profile.get("state", "")

    industry_bucket = _classify_industry(company_fact, industry)
    tease_options = IDEA_TEASES.get(industry_bucket, IDEA_TEASES["default"])
    idea_tease = random.choice(tease_options)

    lines = [
        f"Write a cold outreach email to {first_name}.",
        f"Company: {company}" if company else "",
        f"Verified fact about their company: {company_fact}" if company_fact else "",
        f"City: {city}" if city else "",
        f"State: {state}" if state else "",
        "",
        f"Idea tease sentence to use: {idea_tease}",
        "",
        f"IMPORTANT: Start with 'Hi {first_name},'",
        "Follow the exact formula in the system prompt.",
    ]

    return "\n".join(line for line in lines if line is not None)


def _classify_industry(company_fact: str, apollo_industry: str) -> str:
    """Classify into a broad industry bucket using the company fact and Apollo industry tag."""
    fact_lower = ((company_fact or "") + " " + (apollo_industry or "")).lower()

    if any(
        w in fact_lower
        for w in [
            "concrete",
            "roofing",
            "plumbing",
            "electrical",
            "hvac",
            "construction",
            "building",
            "framing",
            "drywall",
            "painting contractor",
            "landscaping",
            "lawn",
            "tree",
            "irrigation",
            "fencing",
            "paving",
            "pool service",
            "cleaning",
        ]
    ):
        return "trades"
    if any(w in fact_lower for w in ["restaurant", "food", "dining", "cafe", "bar", "brewery", "pizza", "taco", "sushi", "bakery", "catering"]):
        return "restaurant"
    if any(w in fact_lower for w in ["software", "saas", "app", "platform", "tech", "ai", "data", "cloud"]):
        return "tech"
    if any(w in fact_lower for w in ["yoga", "fitness", "gym", "wellness", "spa", "pilates", "meditation", "health"]):
        return "wellness"
    if any(w in fact_lower for w in ["real estate", "realtor", "property", "homes", "mortgage", "brokerage"]):
        return "real_estate"
    if any(w in fact_lower for w in ["hotel", "resort", "hospitality", "lodging", "inn", "travel"]):
        return "hospitality"
    if any(w in fact_lower for w in ["nonprofit", "non-profit", "charity", "foundation", "philanthropy", "ministry", "church"]):
        return "nonprofit"

    return "default"


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
