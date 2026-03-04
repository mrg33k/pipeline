from __future__ import annotations
"""
Lightweight company research utility.
Given a website URL, extract one verified short fact about what the company does.
"""

import logging
import re
from html.parser import HTMLParser

import requests
from openai import OpenAI

logger = logging.getLogger(__name__)
client = OpenAI()


class _TextExtractor(HTMLParser):
    """Extract visible text while skipping script/style content."""

    def __init__(self):
        super().__init__()
        self.parts: list[str] = []
        self._skip_tag: str | None = None

    def handle_starttag(self, tag, attrs):  # noqa: ARG002
        if tag in {"script", "style"}:
            self._skip_tag = tag

    def handle_endtag(self, tag):
        if self._skip_tag == tag:
            self._skip_tag = None

    def handle_data(self, data):
        if self._skip_tag is None and data:
            self.parts.append(data)

    def text(self) -> str:
        return " ".join(self.parts)


def _strip_html(html: str) -> str:
    parser = _TextExtractor()
    parser.feed(html)
    text = parser.text()
    return re.sub(r"\s+", " ", text).strip()


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

    try:
        response = requests.get(website_url, timeout=5)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.info(f"Website fetch failed for {website_url}: {e}")
        return ""

    extracted_website_text = _strip_html(response.text)[:1500]
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
