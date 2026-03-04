from __future__ import annotations
"""
Lightweight company research utility.
Given a website URL, extract one verified short fact about what the company does.
"""

import logging

from openai import OpenAI
from playwright.sync_api import sync_playwright

logger = logging.getLogger(__name__)
client = OpenAI()


def get_company_fact(website_url: str) -> str:
    """
    Return one short verified company fact, or "" on any failure.
    Never raises.
    """
    website_url = (website_url or "").strip()
    if not website_url:
        return ""

    if not website_url.startswith(("http://", "https://")):
        website_url = f"https://{website_url}"

    extracted_website_text = ""
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch()
            try:
                page = browser.new_page()
                page.goto(website_url, timeout=10000)
                page.wait_for_load_state("networkidle", timeout=10000)
                extracted_website_text = page.locator("body").inner_text()
            finally:
                browser.close()
    except Exception as e:  # pylint: disable=broad-except
        logger.info(f"Website fetch failed for {website_url}: {e}")
        return ""

    extracted_website_text = " ".join((extracted_website_text or "").split())[:1500]
    if len(extracted_website_text) < 50:
        return ""

    try:
        completion = client.chat.completions.create(
            model="gpt-4.1-nano",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You extract what a company does from their website text. "
                        "Return ONLY 1-3 words describing their core business. Not a sentence. "
                        "Not a description. Just the simplest possible label.\n\n"
                        'Good examples: "concrete work" / "restaurants" / "pool service software" / '
                        '"yoga studio" / "custom homes" / "hotel staffing" / "roofing" / "landscaping" / '
                        '"Mexican food" / "fitness studio" / "HR software" / "commercial cleaning"\n\n'
                        'Bad examples (too long/detailed): "residential and commercial concrete and plumbing services" '
                        '/ "full-service digital marketing agency" / "award-winning Mediterranean restaurant"\n\n'
                        'Strip all adjectives. Strip "residential and commercial." Strip "full-service." '
                        "Just the core thing they do in 1-3 words.\n\n"
                        "If you cannot determine what they do, return exactly: UNKNOWN"
                    ),
                },
                {"role": "user", "content": extracted_website_text},
            ],
            temperature=0,
            max_tokens=40,
        )
        fact = (completion.choices[0].message.content or "").strip()
    except Exception as e:  # pylint: disable=broad-except
        logger.info(f"OpenAI fact extraction failed for {website_url}: {e}")
        return ""

    if fact == "UNKNOWN":
        return ""

    if len(fact.split()) > 15:
        return ""

    return fact
