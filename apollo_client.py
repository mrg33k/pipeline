from __future__ import annotations
"""
Apollo.io API client.
- People Search (free, no credits) to find prospects
- People Enrichment (costs credits) to get emails for top picks

Search strategy: run targeted keyword searches (one per industry keyword)
to build a pool of relevant candidates, then deduplicate. This yields
much better results than a single broad search with no keywords.
"""

import requests
import logging
import time
import re

import config

logger = logging.getLogger(__name__)

SEARCH_URL = "https://api.apollo.io/api/v1/mixed_people/api_search"
ENRICH_URL = "https://api.apollo.io/api/v1/people/match"
BULK_ENRICH_URL = "https://api.apollo.io/api/v1/people/bulk_match"
EMAIL_LOOKUP_URL = "https://api.apollo.io/v1/people/match"

HEADERS = {
    "Content-Type": "application/json",
    "Cache-Control": "no-cache",
    "accept": "application/json",
    "x-api-key": config.APOLLO_API_KEY,
}


def search_by_keyword(keyword: str, page: int = 1, per_page: int = 50) -> list:
    """
    Free people search filtered by a single industry keyword.
    Returns list of people dicts.
    """
    payload = {
        "person_titles": config.PERSON_TITLES,
        "person_seniorities": config.PERSON_SENIORITIES,
        "organization_locations": config.ORGANIZATION_LOCATIONS,
        "organization_num_employees_ranges": config.EMPLOYEE_RANGES,
        "q_keywords": keyword,
        "include_similar_titles": True,
        "page": page,
        "per_page": per_page,
    }

    resp = requests.post(SEARCH_URL, json=payload, headers=HEADERS, timeout=30)
    if not resp.ok:
        logger.error(f"Apollo search error {resp.status_code}: {resp.text[:500]}")
    resp.raise_for_status()
    data = resp.json()
    total = data.get("total_entries", 0)
    people = data.get("people", [])
    logger.info(f"  '{keyword}': {total} total in Apollo, {len(people)} returned")
    return people


def _build_keyword_list(daily_focus: str = "") -> list[str]:
    """
    Build ordered keyword list for Apollo search.
    If a daily focus is provided, search it first, then defaults.
    """
    defaults = list(config.INDUSTRY_KEYWORDS)
    focus = (daily_focus or "").strip()
    if not focus:
        return defaults

    # Allow comma/slash-separated focus phrases, while preserving full phrase fallback.
    focus_parts = [p.strip() for p in re.split(r"[,/]+", focus) if p.strip()]
    if not focus_parts:
        focus_parts = [focus]

    seen = set()
    ordered = []
    for keyword in focus_parts + defaults:
        norm = keyword.lower()
        if norm not in seen:
            seen.add(norm)
            ordered.append(keyword)
    return ordered


def search_all_pages(max_pages: int = 1, daily_focus: str = "") -> list:
    """
    Run targeted keyword searches across all configured industry keywords.
    Deduplicates by Apollo person ID. Returns a flat list of unique people.
    The max_pages parameter controls how many pages per keyword (usually 1 is enough).
    """
    all_people = []
    seen_ids = set()

    keywords = _build_keyword_list(daily_focus)
    if daily_focus:
        logger.info(f"Daily focus: '{daily_focus}'")
    logger.info(f"Running {len(keywords)} keyword searches across {len(config.ORGANIZATION_LOCATIONS)} locations")

    for keyword in keywords:
        for page in range(1, max_pages + 1):
            people = search_by_keyword(keyword, page=page, per_page=config.APOLLO_SEARCH_PER_PAGE)
            new_count = 0
            for p in people:
                pid = p.get("id")
                if pid and pid not in seen_ids:
                    seen_ids.add(pid)
                    all_people.append(p)
                    new_count += 1
            logger.info(f"    Page {page}: {new_count} new unique candidates")
            if len(people) < config.APOLLO_SEARCH_PER_PAGE:
                break  # no more pages for this keyword
            time.sleep(0.5)
        time.sleep(0.3)

    logger.info(f"Total unique candidates from all keyword searches: {len(all_people)}")
    return all_people


def enrich_person(person_id: str) -> dict | None:
    """
    Enrich a single person by Apollo ID. Costs 1 credit.
    Returns full person data including email.
    """
    payload = {"id": person_id}
    logger.info(f"Enriching person: {person_id}")
    resp = requests.post(ENRICH_URL, json=payload, headers=HEADERS, timeout=30)
    if not resp.ok:
        logger.error(f"Apollo enrich error {resp.status_code}: {resp.text[:500]}")
    resp.raise_for_status()
    data = resp.json()
    person = data.get("person")
    if person and person.get("email"):
        logger.info(f"  -> {person['first_name']} {person['last_name']} ({person['email']})")
    else:
        logger.warning(f"  -> No email found for {person_id}")
    return person


def lookup_by_email(email: str) -> dict:
    """
    Free Apollo lookup by email.
    Returns the matched person dict, or {} when not found or on error.
    """
    email = (email or "").strip()
    if not email:
        return {}

    payload = {"email": email}
    lookup_headers = {**HEADERS, "X-Api-Key": config.APOLLO_API_KEY}
    try:
        resp = requests.post(EMAIL_LOOKUP_URL, json=payload, headers=lookup_headers, timeout=30)
        if resp.status_code == 404:
            return {}
        if not resp.ok:
            logger.warning(f"Apollo email lookup error {resp.status_code}: {resp.text[:300]}")
            return {}
        data = resp.json()
    except requests.RequestException as e:
        logger.warning(f"Apollo email lookup request failed for {email}: {e}")
        return {}
    except ValueError:
        logger.warning(f"Apollo email lookup returned invalid JSON for {email}")
        return {}

    person = data.get("person") if isinstance(data, dict) else None
    if isinstance(person, dict) and person:
        return person
    return {}


def enrich_batch(person_ids: list[str]) -> list[dict]:
    """
    Enrich up to 10 people at a time using bulk endpoint.
    Returns list of enriched person dicts (only those with emails).
    """
    results = []
    for i in range(0, len(person_ids), 10):
        batch = person_ids[i:i + 10]
        details = [{"id": pid} for pid in batch]
        payload = {"details": details}

        logger.info(f"Bulk enriching {len(batch)} people (batch {i // 10 + 1})")
        resp = requests.post(BULK_ENRICH_URL, json=payload, headers=HEADERS, timeout=60)
        if not resp.ok:
            logger.error(f"Apollo bulk enrich error {resp.status_code}: {resp.text[:500]}")
        resp.raise_for_status()
        data = resp.json()

        credits_used = data.get("credits_consumed", 0)
        matches = data.get("matches", [])
        missing = data.get("missing_records", 0)
        logger.info(f"  -> {len(matches)} matches, {missing} missing, {credits_used} credits consumed")

        for match in matches:
            if match and match.get("email"):
                org = match.get("organization", {}) or {}
                logger.info(f"  -> {match['first_name']} {match['last_name']} | "
                            f"{org.get('name', '?')} | {match['email']}")
                results.append(match)
            else:
                name = match.get("name", "Unknown") if match else "Unknown"
                logger.warning(f"  -> No email for: {name}")

        time.sleep(1)

    return results
