from __future__ import annotations
"""
Gmail API client for creating, reading, updating, and deleting draft emails.
Uses OAuth2 credentials with refresh token.
Creates DRAFTS only, never sends.
"""

import base64
import email as email_lib
import json
import logging
import os
import re
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from html.parser import HTMLParser

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

import config

logger = logging.getLogger(__name__)

# Cache the service to avoid re-authenticating on every call
_service_cache = None
_signature_cache = None
_signature_loaded = False

# Body content phrases that identify an outreach email (fallback detection)
OUTREACH_BODY_PHRASES = [
    "introduce myself",
    "web/social",
    "working with someone",
    "meet briefly",
    "hop on zoom",
    "jump on zoom",
    "came up on my radar",
    "came across you",
    "came across your",
    "keeping coming up",
    "keeps coming up",
    "been on my radar",
]


# ═══════════════════════════════════════════════════════════════════════════════
# Auth
# ═══════════════════════════════════════════════════════════════════════════════

def _get_gmail_service():
    """Build and return an authenticated Gmail API service."""
    global _service_cache
    if _service_cache is not None:
        return _service_cache

    with open(config.GMAIL_TOKENS_PATH, "r") as f:
        token_data = json.load(f)

    with open(config.GMAIL_CLIENT_SECRET, "r") as f:
        client_data = json.load(f)

    installed = client_data.get("installed", client_data.get("web", {}))
    client_id = installed["client_id"]
    client_secret = installed["client_secret"]
    token_uri = installed.get("token_uri", "https://oauth2.googleapis.com/token")
    token_scope_str = (token_data.get("scope") or "").strip()
    token_scopes = token_scope_str.split() if token_scope_str else list(config.GMAIL_SCOPES)

    creds = Credentials(
        token=token_data.get("access_token"),
        refresh_token=token_data.get("refresh_token"),
        token_uri=token_uri,
        client_id=client_id,
        client_secret=client_secret,
        scopes=token_scopes,
    )

    if creds.expired or not creds.valid:
        logger.info("Refreshing Gmail access token...")
        creds.refresh(Request())
        new_token_data = {
            "access_token": creds.token,
            "refresh_token": creds.refresh_token,
            "scope": " ".join(creds.scopes) if creds.scopes else token_data.get("scope", ""),
            "token_type": "Bearer",
        }
        with open(config.GMAIL_TOKENS_PATH, "w") as f:
            json.dump(new_token_data, f, indent=2)
        logger.info("Token refreshed and saved.")

    _service_cache = build("gmail", "v1", credentials=creds)
    return _service_cache


def _get_account_signature_html() -> str:
    """
    Fetch and cache the Gmail account default signature HTML.
    Returns empty string when unavailable.
    """
    global _signature_cache, _signature_loaded
    if _signature_loaded:
        return _signature_cache or ""

    _signature_loaded = True
    _signature_cache = ""
    try:
        service = _get_gmail_service()
        data = service.users().settings().sendAs().list(userId="me").execute()
        send_as_entries = data.get("sendAs", []) or []
        if not send_as_entries:
            return ""

        selected = None
        sender = (config.SENDER_EMAIL or "").strip().lower()
        for entry in send_as_entries:
            if entry.get("isPrimary"):
                selected = entry
                break
        if selected is None and sender:
            for entry in send_as_entries:
                if (entry.get("sendAsEmail", "") or "").strip().lower() == sender:
                    selected = entry
                    break
        if selected is None:
            selected = send_as_entries[0]

        _signature_cache = (selected.get("signature", "") or "").strip()
    except Exception as e:
        logger.info(f"Could not fetch Gmail account signature from settings: {e}")

    return _signature_cache or ""


# ═══════════════════════════════════════════════════════════════════════════════
# HTML / body parsing
# ═══════════════════════════════════════════════════════════════════════════════

class _HTMLTextExtractor(HTMLParser):
    """Strip HTML tags and return plain text."""
    def __init__(self):
        super().__init__()
        self.parts = []
        self._skip_tags = {"style", "script", "head"}
        self._current_skip = None

    def handle_starttag(self, tag, attrs):
        if tag in self._skip_tags:
            self._current_skip = tag
        if tag == "br":
            self.parts.append("\n")

    def handle_endtag(self, tag):
        if tag == self._current_skip:
            self._current_skip = None
        if tag in ("p", "div", "tr", "li"):
            self.parts.append("\n")

    def handle_data(self, data):
        if self._current_skip is None:
            self.parts.append(data)

    def get_text(self):
        return "".join(self.parts)


def _html_to_text(html: str) -> str:
    """Convert HTML email body to plain text."""
    parser = _HTMLTextExtractor()
    parser.feed(html)
    text = parser.get_text()
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _decode_part(part) -> str:
    """Decode a base64url-encoded message part body."""
    data = part.get("body", {}).get("data", "")
    if not data:
        return ""
    try:
        return base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="replace")
    except Exception:
        return ""


def _extract_body_from_payload(payload: dict) -> tuple[str, str]:
    """
    Recursively extract plain text and HTML body from a Gmail message payload.
    Returns (plain_text, html_text).
    """
    mime_type = payload.get("mimeType", "")
    parts = payload.get("parts", [])

    if mime_type == "text/plain":
        return _decode_part(payload), ""
    if mime_type == "text/html":
        return "", _decode_part(payload)

    plain, html = "", ""
    for part in parts:
        p, h = _extract_body_from_payload(part)
        if p:
            plain = p
        if h:
            html = h

    return plain, html


def _extract_first_name_from_body(body_text: str) -> str:
    """
    Parse the recipient's first name from the email greeting.
    Looks for patterns like 'Hi Robert,' or 'Hey Sarah,' or 'Hello Mike,'
    """
    m = re.match(r"^\s*(?:Hi|Hey|Hello)\s+([A-Z][a-z]+)", body_text)
    if m:
        return m.group(1)
    return ""


def _is_outreach_by_body(body_text: str) -> bool:
    """
    Fallback: check if a draft body looks like an outreach email
    by scanning for characteristic phrases.
    """
    body_lower = body_text.lower()
    matches = sum(1 for phrase in OUTREACH_BODY_PHRASES if phrase in body_lower)
    return matches >= 1  # at least one phrase is enough


def get_outreach_drafts(max_results: int = 200, known_emails: set = None) -> list[dict]:
    """
    Fetch all Gmail drafts and return only the ones that are outreach emails.

    Detection strategy (in priority order):
    1. PRIMARY: If known_emails is provided, match draft recipient against that set.
       Any draft sent to a known Apollo contact is an outreach draft.
    2. FALLBACK: If known_emails is empty/None, check body content for outreach phrases.

    Returns a list of dicts with:
        draft_id, subject, to_email, to_name, first_name, company, body_text
    """
    if known_emails is None:
        known_emails = set()

    service = _get_gmail_service()

    try:
        result = service.users().drafts().list(userId="me", maxResults=max_results).execute()
        draft_stubs = result.get("drafts", [])
    except Exception as e:
        logger.error(f"Failed to list drafts: {e}")
        return []

    use_email_matching = len(known_emails) > 0
    logger.info(
        f"Found {len(draft_stubs)} total drafts in Gmail. "
        f"Detection mode: {'email-match against {len(known_emails)} known contacts' if use_email_matching else 'body-content fallback'}"
    )

    outreach_drafts = []
    for stub in draft_stubs:
        draft_id = stub["id"]
        try:
            full = service.users().drafts().get(
                userId="me", id=draft_id, format="full"
            ).execute()
        except Exception as e:
            logger.warning(f"Could not fetch draft {draft_id}: {e}")
            continue

        msg = full.get("message", {})
        headers = {h["name"].lower(): h["value"] for h in msg.get("payload", {}).get("headers", [])}

        subject = headers.get("subject", "")
        to_raw = headers.get("to", "")

        # Parse To: header — "First Last <email@domain.com>" or just "email@domain.com"
        to_name = ""
        to_email = to_raw.strip()
        m = re.match(r"^(.*?)\s*<([^>]+)>$", to_raw)
        if m:
            to_name = m.group(1).strip().strip('"')
            to_email = m.group(2).strip()

        to_email_lower = to_email.lower()

        # Extract body text
        payload = msg.get("payload", {})
        plain_text, html_text = _extract_body_from_payload(payload)

        if plain_text:
            body_text = plain_text
        elif html_text:
            body_text = _html_to_text(html_text)
        else:
            body_text = ""

        # ── Determine if this is an outreach draft ──────────────────────
        if use_email_matching:
            # PRIMARY: check if recipient is a known Apollo contact
            is_outreach = to_email_lower in known_emails
            if not is_outreach:
                logger.debug(f"Skipping draft (not an Apollo contact): {to_email} | '{subject}'")
                continue
        else:
            # FALLBACK: check body content for outreach phrases
            is_outreach = _is_outreach_by_body(body_text)
            if not is_outreach:
                logger.debug(f"Skipping draft (no outreach phrases in body): '{subject}'")
                continue

        # Extract first name — try To: header first, then parse from email body
        first_name = to_name.split()[0] if to_name else ""
        if not first_name and body_text:
            first_name = _extract_first_name_from_body(body_text)

        # Extract company from subject line (best-effort, may be empty)
        company = _extract_company_from_subject(subject)

        outreach_drafts.append({
            "draft_id": draft_id,
            "subject": subject,
            "to_email": to_email,
            "to_name": to_name,
            "first_name": first_name,
            "company": company,
            "body_text": body_text.strip(),
        })

    logger.info(f"Found {len(outreach_drafts)} outreach drafts to rewrite")
    return outreach_drafts


def _extract_company_from_subject(subject: str) -> str:
    """
    Try to extract company name from subject like 'quick question for Acme Corp'.
    Returns empty string if not parseable.
    """
    m = re.search(r"(?:quick question for|video for|question for|idea for)\s+(.+)", subject, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return ""


# ═══════════════════════════════════════════════════════════════════════════════
# Draft creation & management
# ═══════════════════════════════════════════════════════════════════════════════

def _build_message(to_email: str, subject: str, body_text: str) -> str:
    """Build a MIME message and return base64url-encoded raw string."""
    signature_html = _get_account_signature_html()
    signature_plain = _html_to_text(signature_html) if signature_html else ""

    full_plain = body_text + (f"\n\n{signature_plain}" if signature_plain else "")
    full_html = (
        '<div style="font-family:Arial,Helvetica,sans-serif;font-size:14px;'
        'color:#222222;line-height:1.5;">\n'
        + body_text.replace("\n", "<br>\n")
        + (f"<br><br>\n{signature_html}" if signature_html else "")
        + "\n</div>"
    )

    message = MIMEMultipart("alternative")
    message["to"] = to_email
    message["from"] = f"{config.SENDER_NAME} <{config.SENDER_EMAIL}>"
    message["subject"] = subject

    plain_part = MIMEText(full_plain, "plain")
    html_part = MIMEText(full_html, "html")
    message.attach(plain_part)
    message.attach(html_part)

    return base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")


def create_draft(to_email: str, subject: str, body_text: str) -> dict | None:
    """Create a Gmail draft. Returns the draft resource dict or None on failure."""
    service = _get_gmail_service()
    raw = _build_message(to_email, subject, body_text)
    try:
        draft = service.users().drafts().create(
            userId="me", body={"message": {"raw": raw}}
        ).execute()
        logger.info(f"Draft created: {draft['id']} -> {to_email} ({subject})")
        return draft
    except Exception as e:
        logger.error(f"Failed to create draft for {to_email}: {e}")
        return None


def delete_draft(draft_id: str) -> bool:
    """Delete a Gmail draft by ID. Returns True on success."""
    service = _get_gmail_service()
    try:
        service.users().drafts().delete(userId="me", id=draft_id).execute()
        logger.info(f"Draft deleted: {draft_id}")
        return True
    except Exception as e:
        logger.warning(f"Failed to delete draft {draft_id}: {e}")
        return False


def update_draft(draft_id: str, to_email: str, subject: str, body_text: str) -> dict | None:
    """
    Update an existing Gmail draft with new content.
    Falls back to delete+create if the update API call fails.
    """
    service = _get_gmail_service()
    raw = _build_message(to_email, subject, body_text)
    try:
        draft = service.users().drafts().update(
            userId="me",
            id=draft_id,
            body={"message": {"raw": raw}},
        ).execute()
        logger.info(f"Draft updated: {draft_id} -> {to_email} ({subject})")
        return draft
    except Exception as e:
        logger.warning(f"Failed to update draft {draft_id}, trying delete+create: {e}")
        delete_draft(draft_id)
        return create_draft(to_email, subject, body_text)


def create_drafts_batch(emails: list[dict]) -> list[dict]:
    """
    Create drafts for a batch of emails.
    Each item should have: profile (with email), subject, body.
    Returns list of results with draft IDs.
    """
    results = []
    for i, item in enumerate(emails):
        to_email = item["profile"]["email"]
        subject = item["subject"]
        body = item["body"]
        logger.info(f"Creating draft {i + 1}/{len(emails)}: {to_email}")
        draft = create_draft(to_email, subject, body)
        results.append({
            "to": to_email,
            "subject": subject,
            "draft_id": draft["id"] if draft else None,
            "success": draft is not None,
        })
    successful = sum(1 for r in results if r["success"])
    logger.info(f"Drafts created: {successful}/{len(emails)}")
    return results
