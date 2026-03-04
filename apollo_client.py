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
CONTACTS_SEARCH_URLS = [
    "https://api.apollo.io/api/v1/contacts/search",
    "https://api.apollo.io/v1/contacts/search",
]

HEADERS = {
    "Content-Type": "application/json",
    "Cache-Control": "no-cache",
    "accept": "application/json",
    "x-api-key": config.APOLLO_API_KEY,
}


def search_by_keyword(
    keyword: str,
    page: int = 1,
    per_page: int = 50,
    organization_locations: list[str] | None = None,
) -> list:
    """
    Free people search filtered by a single industry keyword.
    Returns list of people dicts.
    """
    payload = {
        "person_titles": config.PERSON_TITLES,
        "person_seniorities": config.PERSON_SENIORITIES,
        "organization_locations": organization_locations or config.ORGANIZATION_LOCATIONS,
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


_US_STATE_ABBR_TO_NAME = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
    "DC": "District of Columbia",
}

_US_STATE_NAME_TO_NAME = {name.lower(): name for name in _US_STATE_ABBR_TO_NAME.values()}

_LOCATION_NOISE_WORDS = {
    "company",
    "companies",
    "business",
    "businesses",
    "owner",
    "owners",
    "restaurant",
    "restaurants",
    "software",
    "saas",
    "concrete",
    "construction",
    "roofing",
    "plumbing",
    "hvac",
    "electrical",
}


def _title_case_location(text: str) -> str:
    clean = re.sub(r"\s+", " ", (text or "").strip())
    return clean.title()


def _normalize_state_name(token: str) -> str:
    clean = re.sub(r"\s+", " ", (token or "").strip().replace(".", ""))
    if not clean:
        return ""
    abbr = clean.upper()
    if abbr in _US_STATE_ABBR_TO_NAME:
        return _US_STATE_ABBR_TO_NAME[abbr]
    lower = clean.lower()
    if lower in _US_STATE_NAME_TO_NAME:
        return _US_STATE_NAME_TO_NAME[lower]
    return ""


def _normalize_location_entry(raw_entry: str) -> str:
    """
    Normalize free-text location input to Apollo-friendly location strings.
    Output format:
    - City, State, United States
    - State, United States
    """
    entry = re.sub(r"\s+", " ", (raw_entry or "").strip())
    if not entry:
        return ""

    segments = [s.strip() for s in entry.split(",") if s.strip()]
    if len(segments) >= 2:
        city_or_state = segments[0]
        state_or_country = segments[1]
        state_name = _normalize_state_name(state_or_country)
        city_tokens = {
            t.lower() for t in re.findall(r"[A-Za-z0-9]+", city_or_state)
        }

        # Input like "Arizona, United States"
        if not state_name and state_or_country.lower() in {"united states", "usa", "us"}:
            state_name = _normalize_state_name(city_or_state)
            if state_name:
                return f"{state_name}, United States"
            return f"{_title_case_location(city_or_state)}, United States"

        if state_name:
            if city_tokens.intersection(_LOCATION_NOISE_WORDS):
                return f"{state_name}, United States"
            return f"{_title_case_location(city_or_state)}, {state_name}, United States"

        # Fallback: keep first two segments and add country if missing.
        city = _title_case_location(city_or_state)
        state = _title_case_location(state_or_country)
        if len(segments) >= 3 and segments[2].strip().lower() in {"united states", "usa", "us"}:
            return f"{city}, {state}, United States"
        return f"{city}, {state}, United States"

    # Single-segment input can be state, "city ST", or city.
    state_name = _normalize_state_name(entry)
    if state_name:
        return f"{state_name}, United States"

    tokens = entry.split()
    if len(tokens) >= 2:
        trailing_state = _normalize_state_name(tokens[-1])
        if trailing_state:
            city_tokens = {t.lower() for t in tokens[:-1]}
            if city_tokens.intersection(_LOCATION_NOISE_WORDS):
                return f"{trailing_state}, United States"
            city = _title_case_location(" ".join(tokens[:-1]))
            return f"{city}, {trailing_state}, United States"

    return f"{_title_case_location(entry)}, United States"


def resolve_search_locations(location_input: str = "") -> list[str]:
    """
    Resolve user location input to Apollo organization_locations values.
    - Empty input -> default ORGANIZATION_LOCATIONS from config
    - Multi-location input may be separated with ';' or '|'
    """
    raw = (location_input or "").strip()
    if not raw:
        return list(config.ORGANIZATION_LOCATIONS)

    parts = [p.strip() for p in re.split(r"[;|]+", raw) if p.strip()]
    if not parts:
        return list(config.ORGANIZATION_LOCATIONS)

    resolved = []
    seen = set()
    for part in parts:
        normalized = _normalize_location_entry(part)
        if not normalized:
            continue
        key = normalized.lower()
        if key not in seen:
            seen.add(key)
            resolved.append(normalized)

    return resolved or list(config.ORGANIZATION_LOCATIONS)


_FOCUS_STOPWORDS = {
    "in",
    "for",
    "near",
    "around",
    "the",
    "and",
    "companies",
    "company",
    "businesses",
    "business",
    "owners",
    "owner",
    "contractor",
    "contractors",
    "service",
    "services",
    "arizona",
    "az",
    "phoenix",
    "scottsdale",
    "mesa",
    "tempe",
    "chandler",
    "glendale",
    "peoria",
}

_FOCUS_TERM_EXPANSIONS = {
    "concrete": [
        "concrete",
        "concrete contractor",
        "concrete contractors",
        "concrete construction",
        "construction",
    ],
    "roofing": [
        "roofing",
        "roofing contractor",
        "roofing contractors",
        "commercial roofing",
        "construction",
    ],
    "plumbing": [
        "plumbing",
        "plumber",
        "plumbing contractor",
        "plumbing contractors",
    ],
    "plumber": [
        "plumbing",
        "plumber",
        "plumbing contractor",
    ],
    "hvac": [
        "hvac",
        "hvac contractor",
        "heating and cooling",
        "mechanical contractor",
    ],
    "electrical": [
        "electrical",
        "electrician",
        "electrical contractor",
    ],
    "electric": [
        "electrical",
        "electrician",
        "electrical contractor",
    ],
}

_FOCUS_PHRASE_EXPANSIONS = {
    "real estate": ["real estate", "real estate brokerage", "property management"],
    "software": ["software", "saas"],
    "construction": ["construction", "general contractor", "contractor"],
    "restaurant": ["restaurant", "restaurant group", "hospitality"],
}


def _singularize(word: str) -> str:
    """Best-effort singularization for simple plurals."""
    if len(word) <= 4:
        return word
    if word.endswith("ies") and len(word) > 5:
        return f"{word[:-3]}y"
    if word.endswith("ses"):
        return word[:-2]
    if word.endswith("s") and not word.endswith("ss"):
        return word[:-1]
    return word


def _build_focus_keywords(daily_focus: str) -> list[str]:
    """
    Build a small set of focused keyword variants from free-text user input.
    """
    focus = (daily_focus or "").strip()
    if not focus:
        return []

    variants = [focus]

    # Allow explicit multi-focus input via comma or slash.
    split_parts = [p.strip() for p in re.split(r"[,/]+", focus) if p.strip()]
    variants.extend(split_parts)

    # Extract a normalized core phrase and one/two-word short variants.
    words = re.findall(r"[A-Za-z0-9&+][A-Za-z0-9&+\-']*", focus.lower())
    core_words = [w for w in words if w not in _FOCUS_STOPWORDS]
    singular_words = [_singularize(w) for w in core_words]
    all_words = []
    for word in core_words + singular_words:
        if word and word not in all_words:
            all_words.append(word)

    if core_words:
        core_phrase = " ".join(core_words)
        variants.append(core_phrase)
        variants.append(core_words[0])
        if len(core_words) > 1:
            variants.append(" ".join(core_words[:2]))

        core_phrase_lower = core_phrase.lower()
        for phrase, extras in _FOCUS_PHRASE_EXPANSIONS.items():
            if phrase in core_phrase_lower:
                variants.extend(extras)

    for word in all_words:
        variants.extend(_FOCUS_TERM_EXPANSIONS.get(word, []))

    for word in all_words:
        if len(word) >= 4:
            variants.append(word)

    seen = set()
    ordered = []
    for keyword in variants:
        clean = keyword.strip()
        if not clean:
            continue
        norm = clean.lower()
        if norm not in seen:
            seen.add(norm)
            ordered.append(clean)
    return ordered[:12]


def _build_keyword_list(daily_focus: str = "", include_default_keywords: bool = True) -> list[str]:
    """
    Build ordered keyword list for Apollo search.
    If a daily focus is provided, search it first, then defaults.
    """
    focus_parts = _build_focus_keywords(daily_focus)
    defaults = list(config.INDUSTRY_KEYWORDS) if include_default_keywords else []
    if not focus_parts and include_default_keywords:
        return defaults
    if not focus_parts:
        return []

    seen = set()
    ordered = []
    for keyword in focus_parts + defaults:
        norm = keyword.lower()
        if norm not in seen:
            seen.add(norm)
            ordered.append(keyword)
    return ordered


def search_all_pages(
    max_pages: int = 1,
    daily_focus: str = "",
    include_default_keywords: bool = True,
    location_input: str = "",
) -> list:
    """
    Run targeted keyword searches across all configured industry keywords.
    Deduplicates by Apollo person ID. Returns a flat list of unique people.
    The max_pages parameter controls how many pages per keyword (usually 1 is enough).
    """
    all_people = []
    seen_ids = set()

    keywords = _build_keyword_list(
        daily_focus=daily_focus,
        include_default_keywords=include_default_keywords,
    )
    organization_locations = resolve_search_locations(location_input)
    if daily_focus:
        mode = "focus+default" if include_default_keywords else "focus-only"
        logger.info(f"Daily focus ({mode}): '{daily_focus}'")
    if not keywords:
        logger.info("No search keywords resolved. Returning no candidates.")
        return []
    logger.info(f"Running {len(keywords)} keyword searches across {len(organization_locations)} locations")
    logger.info(f"Apollo locations: {organization_locations}")

    for keyword in keywords:
        for page in range(1, max_pages + 1):
            people = search_by_keyword(
                keyword,
                page=page,
                per_page=config.APOLLO_SEARCH_PER_PAGE,
                organization_locations=organization_locations,
            )
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


def _extract_contacts_from_response(data: dict) -> list[dict]:
    """
    Extract a contacts list from Apollo contacts/search responses.
    Handles common response shapes without assuming one strict schema.
    """
    if not isinstance(data, dict):
        return []

    for key in ("contacts", "people", "records", "results"):
        value = data.get(key)
        if isinstance(value, list):
            return value

    nested = data.get("data")
    if isinstance(nested, list):
        return nested
    if isinstance(nested, dict):
        for key in ("contacts", "people", "records", "results"):
            value = nested.get(key)
            if isinstance(value, list):
                return value

    return []


def _extract_total_pages(data: dict) -> int:
    """
    Best-effort total pages extractor from Apollo pagination metadata.
    Returns 0 when unknown.
    """
    if not isinstance(data, dict):
        return 0

    candidates = []
    for key in ("total_pages", "num_pages"):
        value = data.get(key)
        if isinstance(value, int):
            candidates.append(value)

    pagination = data.get("pagination")
    if isinstance(pagination, dict):
        for key in ("total_pages", "num_pages"):
            value = pagination.get(key)
            if isinstance(value, int):
                candidates.append(value)

    return max(candidates) if candidates else 0


def _normalize_contact_row(row: dict) -> dict:
    """
    Normalize one Apollo contact row to a stable shape for the dashboard.
    """
    if not isinstance(row, dict):
        return {}

    contact = row.get("contact") if isinstance(row.get("contact"), dict) else row
    person = contact.get("person") if isinstance(contact.get("person"), dict) else {}
    org = (
        contact.get("organization")
        if isinstance(contact.get("organization"), dict)
        else person.get("organization") if isinstance(person.get("organization"), dict) else {}
    )

    first_name = (
        (contact.get("first_name") or "").strip()
        or (person.get("first_name") or "").strip()
    )
    last_name = (
        (contact.get("last_name") or "").strip()
        or (person.get("last_name") or "").strip()
    )
    email = (
        (contact.get("email") or "").strip().lower()
        or (person.get("email") or "").strip().lower()
    )
    contact_id = (
        (contact.get("id") or "").strip()
        or (person.get("id") or "").strip()
        or email
    )

    full_name = " ".join(part for part in [first_name, last_name] if part).strip()

    return {
        "id": contact_id,
        "email": email,
        "first_name": first_name,
        "last_name": last_name,
        "name": full_name or email,
        "title": (contact.get("title") or person.get("title") or "").strip(),
        "company": (org.get("name") or contact.get("company_name") or "").strip(),
        "industry": (org.get("industry") or contact.get("industry") or "").strip(),
        "city": (contact.get("city") or org.get("city") or "").strip(),
        "state": (contact.get("state") or org.get("state") or "").strip(),
        "domain": (org.get("primary_domain") or org.get("website_url") or "").strip(),
        "website_url": (org.get("website_url") or "").strip(),
    }


def list_owned_contacts(per_page: int = 100, max_pages: int = 200) -> list[dict]:
    """
    Fetch all Apollo contacts visible to this API key, paginated.
    Intended for dashboard use as the canonical Contacts source.
    """
    per_page = max(1, min(200, int(per_page or 100)))
    max_pages = max(1, int(max_pages or 1))

    seen_ids = set()
    all_contacts: list[dict] = []
    total_pages_hint = 0
    endpoint_used = ""
    effective_per_page = per_page

    for page in range(1, max_pages + 1):
        base_payload = {
            "page": page,
            "sort_by_field": "contact_last_activity_date",
            "sort_ascending": False,
        }
        # Some Apollo plans/endpoints reject specific per_page values.
        payload_options = [
            {**base_payload, "per_page": effective_per_page},
            {**base_payload, "per_page": 100},
            {**base_payload, "per_page": 50},
            {**base_payload, "per_page": 25},
            dict(base_payload),  # no per_page fallback
        ]
        deduped_payload_options = []
        seen_payload_keys = set()
        for candidate in payload_options:
            key = tuple(sorted(candidate.items()))
            if key in seen_payload_keys:
                continue
            seen_payload_keys.add(key)
            deduped_payload_options.append(candidate)

        data = None
        last_error = ""
        used_payload = {}
        for payload in deduped_payload_options:
            for url in CONTACTS_SEARCH_URLS:
                try:
                    resp = requests.post(url, json=payload, headers=HEADERS, timeout=30)
                except requests.RequestException as e:
                    last_error = str(e)
                    continue

                if resp.status_code in {404, 405}:
                    continue

                text = (resp.text or "")[:300]
                if resp.status_code == 422 and "per page" in text.lower():
                    last_error = f"{resp.status_code}: {text}"
                    # Try alternate endpoint with the same payload first.
                    continue

                if not resp.ok:
                    if page == 1:
                        raise RuntimeError(f"Apollo contacts request failed ({resp.status_code}): {text}")
                    logger.warning(f"Apollo contacts page {page} failed ({resp.status_code}): {text}")
                    return all_contacts
                try:
                    data = resp.json()
                    endpoint_used = url
                    used_payload = payload
                    break
                except ValueError:
                    last_error = "invalid JSON response"
                    continue
            if data is not None:
                break

        if data is None:
            if page == 1:
                raise RuntimeError(f"Apollo contacts fetch unavailable: {last_error or 'no usable endpoint'}")
            break

        rows = _extract_contacts_from_response(data)
        if "per_page" in used_payload and isinstance(used_payload.get("per_page"), int):
            effective_per_page = max(1, int(used_payload["per_page"]))
        if page == 1:
            logger.info(
                f"Apollo contacts source: {endpoint_used} "
                f"({len(rows)} contacts on first page)"
            )
        total_pages_hint = max(total_pages_hint, _extract_total_pages(data))

        if not rows:
            break

        for row in rows:
            normalized = _normalize_contact_row(row)
            if not normalized:
                continue
            key = normalized.get("id") or normalized.get("email")
            if not key or key in seen_ids:
                continue
            seen_ids.add(key)
            all_contacts.append(normalized)

        if total_pages_hint and page >= total_pages_hint:
            break
        if len(rows) < effective_per_page:
            break
        time.sleep(0.2)

    logger.info(f"Apollo contacts loaded: {len(all_contacts)}")
    return all_contacts


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
