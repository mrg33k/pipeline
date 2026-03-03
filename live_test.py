#!/usr/bin/env python3
"""
Live end-to-end test: 5 real Hospitality/Restaurant contacts in Scottsdale/Phoenix.
Searches Apollo -> LLM filters -> Enriches -> Researches -> Writes emails -> Creates Gmail drafts.
"""

import json
import logging
import os
import sys
import time
import csv
import base64
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import requests
from openai import OpenAI
from bs4 import BeautifulSoup
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# ── Config ────────────────────────────────────────────────────────────────────
APOLLO_API_KEY = os.environ.get("APOLLO_API_KEY", "")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SEARCH_URL = "https://api.apollo.io/api/v1/mixed_people/api_search"
ENRICH_URL = "https://api.apollo.io/api/v1/people/bulk_match"

APOLLO_HEADERS = {
    "Content-Type": "application/json",
    "Cache-Control": "no-cache",
    "accept": "application/json",
    "x-api-key": APOLLO_API_KEY,
}

openai_client = OpenAI()

GMAIL_CLIENT_SECRET = os.path.join(BASE_DIR, "client_secret.json")
GMAIL_TOKENS_PATH = os.path.join(BASE_DIR, "gmail_tokens.json")
SENDER_EMAIL = "hello@aom-inhouse.com"
SENDER_NAME = "Patrik Matheson"

SIGNATURE_HTML = """
<br><br>
<table cellpadding="0" cellspacing="0" border="0" style="font-family:Arial,Helvetica,sans-serif;font-size:13px;color:#333333;line-height:1.4;">
  <tr>
    <td style="padding-right:15px;vertical-align:top;">
      <img src="https://aheadofmarket.com/cdn/shop/files/patrik-headshot.jpg" alt="Patrik Matheson" width="90" height="90" style="border-radius:50%;display:block;" />
    </td>
    <td style="vertical-align:top;">
      <strong style="font-size:14px;color:#111111;">Patrik Matheson</strong><br>
      Digital Strategy<br>
      Video Marketing | Ahead of Market<br><br>
      <span style="font-size:12px;">
        <a href="tel:6023732164" style="color:#333333;text-decoration:none;">602.373.2164</a><br>
        <a href="mailto:Patrikmatheson@icloud.com" style="color:#1a73e8;text-decoration:none;">Patrikmatheson@icloud.com</a><br>
        <a href="https://aheadofmarket.com" style="color:#1a73e8;text-decoration:none;">aheadofmarket.com</a>
      </span>
      <br><br>
      <a href="https://aheadofmarket.com" style="display:inline-block;padding:6px 14px;background-color:#111111;color:#ffffff;text-decoration:none;border-radius:4px;font-size:12px;font-weight:bold;">Visit My Website</a>
    </td>
  </tr>
</table>
"""

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("live_test")


def divider(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}\n")


# ══════════════════════════════════════════════════════════════════════════════
# STEP 1: Apollo Free Search (targeted keyword searches)
# ══════════════════════════════════════════════════════════════════════════════
def apollo_search():
    divider("STEP 1: Apollo People Search (FREE, no credits)")

    locations = [
        "Scottsdale, Arizona, United States",
        "Phoenix, Arizona, United States",
        "Tempe, Arizona, United States",
        "Mesa, Arizona, United States",
        "Chandler, Arizona, United States",
        "Glendale, Arizona, United States",
    ]

    base_payload = {
        "person_titles": ["Owner", "Founder", "CEO", "Marketing Director"],
        "organization_locations": locations,
        "organization_num_employees_ranges": ["11,50", "51,200"],
        "page": 1,
        "per_page": 50,
    }

    all_people = []
    seen_ids = set()

    # Run targeted keyword searches to find hospitality/restaurant contacts
    for keyword in ["restaurant", "hospitality", "hotel", "bar food dining"]:
        payload = {**base_payload, "q_keywords": keyword}
        log.info(f"Searching Apollo with q_keywords='{keyword}'...")
        resp = requests.post(SEARCH_URL, json=payload, headers=APOLLO_HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        people = data.get("people", [])
        total = data.get("total_entries", 0)

        new_count = 0
        for p in people:
            if p["id"] not in seen_ids:
                seen_ids.add(p["id"])
                all_people.append(p)
                new_count += 1

        log.info(f"  '{keyword}': {total} total in Apollo, {len(people)} returned, {new_count} new")
        time.sleep(0.5)

    log.info(f"Total unique candidates: {len(all_people)}")

    # Show all with email
    with_email = [p for p in all_people if p.get("has_email")]
    log.info(f"Candidates with email available: {len(with_email)}")

    for i, p in enumerate(with_email[:15]):
        org = p.get("organization", {}) or {}
        org_name = org.get("name", "?") if isinstance(org, dict) else "?"
        print(f"  [{i+1}] {p.get('first_name','')} {p.get('last_name_obfuscated','')} "
              f"| {p.get('title','')} | {org_name}")

    if len(with_email) > 15:
        print(f"  ... and {len(with_email) - 15} more with email")

    return all_people


# ══════════════════════════════════════════════════════════════════════════════
# STEP 2: LLM Filter & Rank (pick best 5)
# ══════════════════════════════════════════════════════════════════════════════
def llm_filter(candidates):
    divider("STEP 2: LLM Filtering (pick best 5)")

    with_email = [p for p in candidates if p.get("has_email")]

    summaries = []
    for p in with_email:
        org = p.get("organization", {}) or {}
        org_name = org.get("name", "Unknown") if isinstance(org, dict) else "Unknown"
        summaries.append({
            "id": p["id"],
            "name": f"{p.get('first_name', '')} {p.get('last_name_obfuscated', '')}".strip(),
            "title": p.get("title", ""),
            "company": org_name,
        })

    prompt = f"""From the following list of {len(summaries)} prospects in the Phoenix/Scottsdale area, select the 5 BEST for cold outreach from a video production studio.

We want people who run or lead restaurants, bars, hotels, resorts, or hospitality businesses. These are the ideal clients for story-driven video content.

Prioritize:
1. Company name clearly indicates a restaurant, bar, hotel, resort, or food/beverage business
2. Title is Owner, Founder, CEO, or similar decision-maker
3. Pick one person per company (no duplicates)
4. Skip industry associations or suppliers. We want the actual restaurant/hospitality operators.

Return ONLY a JSON array of exactly 5 person IDs. Example: ["id1", "id2", "id3", "id4", "id5"]

Candidates:
{json.dumps(summaries, indent=None)}"""

    log.info(f"Sending {len(summaries)} candidates to LLM...")
    response = openai_client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[
            {"role": "system", "content": "You are a sales prospecting assistant. Return only a valid JSON array of IDs."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.2,
        max_tokens=500,
    )

    raw = response.choices[0].message.content.strip()
    log.info(f"LLM raw response: {raw[:300]}")

    # Strip markdown fences
    if raw.startswith("```"):
        lines = raw.split("\n")
        raw = "\n".join(lines[1:])
        if raw.endswith("```"):
            raw = raw[:-3]
        raw = raw.strip()

    try:
        picked_ids = json.loads(raw)
    except json.JSONDecodeError:
        log.error(f"LLM returned invalid JSON, falling back to first 5 with email")
        picked_ids = [s["id"] for s in summaries[:5]]

    log.info(f"LLM selected {len(picked_ids)} prospects:")
    id_to_summary = {s["id"]: s for s in summaries}
    for i, pid in enumerate(picked_ids):
        s = id_to_summary.get(pid, {})
        print(f"  [{i+1}] {s.get('name', '?')} | {s.get('title', '?')} | {s.get('company', '?')}")

    return picked_ids[:5]


# ══════════════════════════════════════════════════════════════════════════════
# STEP 3: Apollo Enrichment
# ══════════════════════════════════════════════════════════════════════════════
def apollo_enrich(person_ids):
    divider(f"STEP 3: Apollo Enrichment ({len(person_ids)} credits)")

    details = [{"id": pid} for pid in person_ids]
    payload = {"details": details}

    log.info(f"Enriching {len(person_ids)} people via bulk endpoint...")
    resp = requests.post(ENRICH_URL, json=payload, headers=APOLLO_HEADERS, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    credits = data.get("credits_consumed", "?")
    matches = data.get("matches", [])
    missing = data.get("missing_records", 0)

    log.info(f"Credits consumed: {credits}")
    log.info(f"Matches returned: {len(matches)}")
    log.info(f"Missing records: {missing}")

    enriched = []
    for m in matches:
        if m and m.get("email"):
            org = m.get("organization", {}) or {}
            print(f"  -> {m['first_name']} {m['last_name']} | {m.get('title','')} "
                  f"| {org.get('name','')} | {m['email']}")
            enriched.append(m)
        else:
            name = m.get("name", "Unknown") if m else "Unknown"
            log.warning(f"  -> No email for: {name}")

    return enriched


# ══════════════════════════════════════════════════════════════════════════════
# STEP 4: Research Each Contact
# ══════════════════════════════════════════════════════════════════════════════
def research_contacts(enriched):
    divider(f"STEP 4: Researching {len(enriched)} contacts")

    profiles = []
    for i, person in enumerate(enriched):
        org = person.get("organization", {}) or {}
        domain = org.get("primary_domain", "") or org.get("website_url", "")

        profile = {
            "apollo_id": person.get("id", ""),
            "first_name": person.get("first_name", ""),
            "last_name": person.get("last_name", ""),
            "email": person.get("email", ""),
            "title": person.get("title", ""),
            "headline": person.get("headline", ""),
            "city": person.get("city", ""),
            "state": person.get("state", ""),
            "company_name": org.get("name", ""),
            "company_domain": domain,
            "company_industry": org.get("industry", ""),
            "company_city": org.get("city", ""),
            "company_state": org.get("state", ""),
            "company_description": org.get("short_description", "") or org.get("seo_description", ""),
            "company_employee_count": org.get("estimated_num_employees", ""),
            "company_founded_year": org.get("founded_year", ""),
            "homepage_snippet": "",
        }

        if domain:
            url = domain if domain.startswith("http") else f"https://{domain}"
            log.info(f"  [{i+1}] Scraping {url}...")
            try:
                r = requests.get(url, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
                if r.status_code == 200:
                    soup = BeautifulSoup(r.text, "html.parser")
                    for tag in soup(["script", "style", "nav", "header", "footer"]):
                        tag.decompose()
                    text = soup.get_text(separator=" ", strip=True)
                    profile["homepage_snippet"] = text[:400]
                    log.info(f"      Got {len(profile['homepage_snippet'])} chars from homepage")
                else:
                    log.warning(f"      HTTP {r.status_code}")
            except Exception as e:
                log.warning(f"      Scrape failed: {e}")
        else:
            log.info(f"  [{i+1}] No domain, skipping scrape")

        print(f"  [{i+1}] {profile['first_name']} {profile['last_name']} | {profile['company_name']} "
              f"| {profile['company_industry']} | {profile['company_city']}, {profile['company_state']}")
        if profile["company_description"]:
            print(f"       Desc: {profile['company_description'][:150]}")
        profiles.append(profile)

    return profiles


# ══════════════════════════════════════════════════════════════════════════════
# STEP 5: Write Personalized Emails
# ══════════════════════════════════════════════════════════════════════════════
SYSTEM_PROMPT = """You write cold outreach emails for Patrik Matheson, who runs Ahead of Market, a small creative studio in Phoenix, AZ. The studio turns products into story-driven video content for web and social.

WRITING STYLE RULES (MUST FOLLOW):
- Clear, simple language. Spartan and informative.
- Short, impactful sentences. Active voice only.
- Use "you" and "your" to address the reader directly.
- NO markdown, no bold, no italics, no headers, no asterisks, no hashtags
- NO em dashes. Only commas or periods.
- NO semicolons or colons
- NO bullet points (simple dashes only if list needed)
- NO "not just this, but also this" or "while X, Y" constructions
- NO sandwiching (no intro sentences, no "here is your info," no "hope this helps")
- NO setup/wrap-up language like "in conclusion," "in closing," etc.

BANNED VERBS (never use): delve, embark, craft, crafting, unlock, discover, revolutionize, utilize, utilizing, illuminate, unveil, elucidate, harness, navigate, foster, enrich, empower, optimize, maximize, spearhead, facilitate, coordinate, leverage, synergize, provide, offer, serve, helps, provides, offers, serves

BANNED ADJECTIVES (never use): vibrant, robust, seamless, bespoke, pivotal, intricate, multifaceted, exciting, groundbreaking, cutting-edge, remarkable, powerful, ever-evolving, comprehensive, global, world-class, state-of-the-art, top-tier, future-proof, visionary, transformative, impactful, game-changing, innovative, nuanced, stark, enlightening, esteemed

BANNED NOUNS (never use): realm, abyss, tapestry, landscape, testament, glimpse, inquiries, cornerstone, paradigm shift, holistic, nuance, perspective, insights, deep dive, dive deep, journey, bridge the gap, solution, ecosystem, synergy, bandwidth, mission-critical

BANNED TRANSITIONS (never use): hence, furthermore, however, moreover, additionally, notably, importantly, essentially, basically, actually, literally, certain, probably, maybe, just, that, very, really, nevertheless, nonetheless, indeed, clearly, obviously, effectively, specifically

BANNED PHRASES (never use): "in a world where," "not alone," "it remains to be seen," "only time will tell," "the key to," "look no further," "at its core," "more than just," "it's not about X, it's about Y," "it's worth noting," "it's important to understand," "take a moment to," "think of it as," "digital age," "rapidly changing," "looking ahead," "to be clear"

EMAIL STRUCTURE:
1. Open by admiring something specific about their business. Be genuine, not generic.
2. Pitch a specific video idea tailored to their business and industry.
3. Show local awareness. Reference their city or area in Phoenix metro.
4. Close with a low-pressure invitation to connect.

Sign off with: Cheers,
(The signature block is added separately.)

Keep the email to 4-6 short paragraphs. Under 150 words total. No subject line in the body. Address them by first name only."""


def write_emails(profiles):
    divider(f"STEP 5: Writing {len(profiles)} personalized emails")

    results = []
    for i, p in enumerate(profiles):
        parts = [f"Write a cold outreach email to this person:\n"]
        parts.append(f"Name: {p['first_name']} {p['last_name']}")
        parts.append(f"Title: {p['title']}")
        parts.append(f"Company: {p['company_name']}")
        if p.get("company_industry"):
            parts.append(f"Industry: {p['company_industry']}")
        loc_parts = []
        if p.get("company_city"):
            loc_parts.append(p["company_city"])
        if p.get("company_state"):
            loc_parts.append(p["company_state"])
        if loc_parts:
            parts.append(f"Location: {', '.join(loc_parts)}")
        if p.get("company_description"):
            parts.append(f"About the company: {p['company_description']}")
        if p.get("company_domain"):
            parts.append(f"Website: {p['company_domain']}")
        if p.get("homepage_snippet"):
            parts.append(f"From their website: {p['homepage_snippet'][:300]}")
        if p.get("company_employee_count"):
            parts.append(f"Employees: ~{p['company_employee_count']}")
        if p.get("company_founded_year"):
            parts.append(f"Founded: {p['company_founded_year']}")

        context = "\n".join(parts)

        log.info(f"Writing email {i+1}/{len(profiles)}: {p['first_name']} at {p['company_name']}")
        response = openai_client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": context},
            ],
            temperature=0.7,
            max_tokens=500,
        )

        body = response.choices[0].message.content.strip()
        subject = f"video for {p['company_name']}"

        print(f"\n  {'─'*56}")
        print(f"  Email {i+1}: To {p['first_name']} {p['last_name']} <{p['email']}>")
        print(f"  Subject: {subject}")
        print(f"  {'─'*56}")
        for line in body.split("\n"):
            print(f"  {line}")
        print(f"  {'─'*56}\n")

        results.append({
            "profile": p,
            "subject": subject,
            "body": body,
        })

    return results


# ══════════════════════════════════════════════════════════════════════════════
# STEP 6: Create Gmail Drafts
# ══════════════════════════════════════════════════════════════════════════════
def create_gmail_drafts(emails):
    divider(f"STEP 6: Creating {len(emails)} Gmail Drafts")

    with open(GMAIL_TOKENS_PATH, "r") as f:
        token_data = json.load(f)
    with open(GMAIL_CLIENT_SECRET, "r") as f:
        client_data = json.load(f)

    installed = client_data.get("installed", client_data.get("web", {}))
    creds = Credentials(
        token=token_data.get("access_token"),
        refresh_token=token_data.get("refresh_token"),
        token_uri=installed.get("token_uri", "https://oauth2.googleapis.com/token"),
        client_id=installed["client_id"],
        client_secret=installed["client_secret"],
        scopes=["https://www.googleapis.com/auth/gmail.compose"],
    )

    if not creds.valid:
        log.info("Refreshing Gmail access token...")
        creds.refresh(Request())
        new_token_data = {
            "access_token": creds.token,
            "refresh_token": creds.refresh_token,
            "scope": " ".join(creds.scopes) if creds.scopes else token_data.get("scope", ""),
            "token_type": "Bearer",
        }
        with open(GMAIL_TOKENS_PATH, "w") as f:
            json.dump(new_token_data, f, indent=2)
        log.info("Token refreshed and saved.")

    service = build("gmail", "v1", credentials=creds)

    draft_results = []
    for i, item in enumerate(emails):
        p = item["profile"]
        to_email = p["email"]
        subject = item["subject"]
        body_text = item["body"]

        body_html = body_text.replace("\n", "<br>\n")
        full_html = f"""<div style="font-family:Arial,Helvetica,sans-serif;font-size:14px;color:#222222;line-height:1.5;">
{body_html}
{SIGNATURE_HTML}
</div>"""

        message = MIMEMultipart("alternative")
        message["to"] = to_email
        message["from"] = f"{SENDER_NAME} <{SENDER_EMAIL}>"
        message["subject"] = subject

        plain_fallback = body_text + "\n\nCheers,\nPatrik Matheson\nDigital Strategy\nVideo Marketing | Ahead of Market\n602.373.2164\naheadofmarket.com"
        message.attach(MIMEText(plain_fallback, "plain"))
        message.attach(MIMEText(full_html, "html"))

        raw = base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")

        log.info(f"Creating draft {i+1}/{len(emails)}: {to_email}")
        try:
            draft = service.users().drafts().create(
                userId="me",
                body={"message": {"raw": raw}}
            ).execute()
            draft_id = draft["id"]
            log.info(f"  -> Draft created. ID: {draft_id}")
            draft_results.append({"to": to_email, "subject": subject, "draft_id": draft_id, "success": True})
        except Exception as e:
            log.error(f"  -> FAILED: {e}")
            draft_results.append({"to": to_email, "subject": subject, "draft_id": None, "success": False, "error": str(e)})

    return draft_results


# ══════════════════════════════════════════════════════════════════════════════
# STEP 7: Export CSV
# ══════════════════════════════════════════════════════════════════════════════
def export_csv(emails, draft_results):
    divider("STEP 7: Exporting CSV")

    os.makedirs(os.path.join(BASE_DIR, "daily_exports"), exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    csv_path = os.path.join(BASE_DIR, "daily_exports", f"live_test_{date_str}.csv")

    draft_lookup = {dr["to"]: dr for dr in draft_results}

    rows = []
    for item in emails:
        p = item["profile"]
        dr = draft_lookup.get(p["email"], {})
        rows.append({
            "date": datetime.now().strftime("%Y-%m-%d"),
            "first_name": p.get("first_name", ""),
            "last_name": p.get("last_name", ""),
            "email": p.get("email", ""),
            "title": p.get("title", ""),
            "company": p.get("company_name", ""),
            "industry": p.get("company_industry", ""),
            "city": p.get("company_city", ""),
            "domain": p.get("company_domain", ""),
            "subject": item.get("subject", ""),
            "body": item.get("body", ""),
            "draft_id": dr.get("draft_id", ""),
            "draft_created": dr.get("success", False),
        })

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    log.info(f"CSV exported: {csv_path}")
    return csv_path


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
def main():
    print()
    print("=" * 60)
    print("  AHEAD OF MARKET - LIVE TEST (5 contacts)")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("  Target: Hospitality / Restaurants in Scottsdale/Phoenix")
    print("=" * 60)

    # Step 1
    candidates = apollo_search()
    if not candidates:
        log.error("No candidates found. Exiting.")
        return

    # Step 2
    top_ids = llm_filter(candidates)
    if not top_ids:
        log.error("LLM returned no picks. Exiting.")
        return

    # Step 3
    enriched = apollo_enrich(top_ids)
    if not enriched:
        log.error("No enrichment results. Exiting.")
        return

    # Step 4
    profiles = research_contacts(enriched)

    # Step 5
    emails = write_emails(profiles)

    # Step 6
    draft_results = create_gmail_drafts(emails)

    # Step 7
    csv_path = export_csv(emails, draft_results)

    # ── Final Summary ─────────────────────────────────────────────────────
    divider("FINAL SUMMARY")
    successful = sum(1 for r in draft_results if r["success"])
    failed = sum(1 for r in draft_results if not r["success"])

    print(f"  Candidates from Apollo search:  {len(candidates)}")
    print(f"  LLM selected:                   {len(top_ids)}")
    print(f"  Enriched with email:             {len(enriched)}")
    print(f"  Emails written:                  {len(emails)}")
    print(f"  Gmail drafts created:            {successful}")
    print(f"  Gmail drafts failed:             {failed}")
    print(f"  CSV export:                      {csv_path}")
    print()

    if failed > 0:
        print("  DRAFT FAILURES:")
        for r in draft_results:
            if not r["success"]:
                print(f"    {r['to']}: {r.get('error', 'unknown')}")
        print()

    for r in draft_results:
        status = "OK" if r["success"] else "FAIL"
        print(f"  [{status}] {r['to']} | {r['subject']} | draft_id={r.get('draft_id', 'N/A')}")

    print()


if __name__ == "__main__":
    main()
