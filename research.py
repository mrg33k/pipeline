from __future__ import annotations
"""
Lightweight research on each enriched contact.
Uses Apollo data + a quick web scrape of the company homepage.
"""

import logging
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


def research_contact(person: dict) -> dict:
    """
    Build a research profile for one enriched contact.
    Combines Apollo data with a quick homepage scrape.
    Returns a dict with all context needed for email writing.
    """
    org = person.get("organization", {}) or {}

    profile = {
        "apollo_id": person.get("id", ""),
        "first_name": person.get("first_name", ""),
        "last_name": person.get("last_name", ""),
        "full_name": person.get("name", ""),
        "email": person.get("email", ""),
        "title": person.get("title", ""),
        "headline": person.get("headline", ""),
        "linkedin_url": person.get("linkedin_url", ""),
        "city": person.get("city", ""),
        "state": person.get("state", ""),
        "company_name": org.get("name", ""),
        "company_domain": org.get("primary_domain", "") or org.get("website_url", ""),
        "company_industry": org.get("industry", ""),
        "company_city": org.get("city", ""),
        "company_state": org.get("state", ""),
        "company_description": org.get("short_description", "") or org.get("seo_description", ""),
        "company_employee_count": org.get("estimated_num_employees", ""),
        "company_founded_year": org.get("founded_year", ""),
        "company_linkedin": org.get("linkedin_url", ""),
        "homepage_snippet": "",
    }

    # Quick homepage scrape for extra context
    domain = profile["company_domain"]
    if domain and not domain.startswith("http"):
        domain = f"https://{domain}"

    if domain:
        profile["homepage_snippet"] = _scrape_homepage(domain)

    return profile


def _scrape_homepage(url: str) -> str:
    """
    Grab the first ~500 chars of visible text from a company homepage.
    Fails silently on any error.
    """
    try:
        resp = requests.get(url, timeout=8, headers={
            "User-Agent": "Mozilla/5.0 (compatible; AOM-Research/1.0)"
        })
        if resp.status_code != 200:
            return ""
        soup = BeautifulSoup(resp.text, "html.parser")

        # Remove script/style
        for tag in soup(["script", "style", "nav", "header", "footer"]):
            tag.decompose()

        text = soup.get_text(separator=" ", strip=True)
        # Take first 500 chars
        snippet = text[:500].strip()
        return snippet
    except Exception as e:
        logger.debug(f"Homepage scrape failed for {url}: {e}")
        return ""


def research_batch(enriched_people: list[dict]) -> list[dict]:
    """Research all enriched contacts. Returns list of profile dicts."""
    profiles = []
    for i, person in enumerate(enriched_people):
        logger.info(f"Researching {i + 1}/{len(enriched_people)}: "
                     f"{person.get('first_name', '')} {person.get('last_name', '')} "
                     f"at {(person.get('organization') or {}).get('name', 'Unknown')}")
        profile = research_contact(person)
        profiles.append(profile)
    return profiles
