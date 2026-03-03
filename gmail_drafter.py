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

# Subject line patterns that identify outreach drafts from this pipeline
OUTREACH_SUBJECT_PATTERNS = [
    r"quick question for ",
    r"video for ",
    r"a quick question for ",
    r"question for ",
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

    creds = Credentials(
        token=token_data.get("access_token"),
        refresh_token=token_data.get("refresh_token"),
        token_uri=token_uri,
        client_id=client_id,
        client_secret=client_secret,
        scopes=["https://www.googleapis.com/auth/gmail.compose"],
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


# ═══════════════════════════════════════════════════════════════════════════════
# Draft reading & parsing
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
    # Collapse excessive blank lines
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


def _strip_signature(text: str) -> str:
    """
    Remove Patrik's signature block from plain text.
    The signature starts after 'Best,' or 'Cheers,'.
    """
    # Find the sign-off line and keep everything up to and including it
    for marker in ["Best,", "Cheers,"]:
        idx = text.find(marker)
        if idx != -1:
            return text[: idx + len(marker)].strip()
    return text.strip()


def _is_outreach_draft(subject: str) -> bool:
    """Return True if the subject looks like an outreach email from this pipeline."""
    subject_lower = subject.lower()
    for pattern in OUTREACH_SUBJECT_PATTERNS:
        if re.search(pattern, subject_lower):
            return True
    return False


def _extract_company_from_subject(subject: str) -> str:
    """
    Try to extract company name from subject like 'quick question for Acme Corp'.
    Returns empty string if not parseable.
    """
    for pattern in OUTREACH_SUBJECT_PATTERNS:
        m = re.search(pattern, subject, re.IGNORECASE)
        if m:
            return subject[m.end():].strip()
    return ""


def get_outreach_drafts(max_results: int = 200) -> list[dict]:
    """
    Fetch all Gmail drafts and return only the ones that look like
    outreach emails from this pipeline.

    Returns a list of dicts with:
        draft_id, subject, to_email, to_name, company, body_text
    """
    service = _get_gmail_service()

    # List all drafts
    try:
        result = service.users().drafts().list(userId="me", maxResults=max_results).execute()
        draft_stubs = result.get("drafts", [])
    except Exception as e:
        logger.error(f"Failed to list drafts: {e}")
        return []

    logger.info(f"Found {len(draft_stubs)} total drafts in Gmail, filtering for outreach...")

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

        # Filter: only process outreach drafts
        if not _is_outreach_draft(subject):
            logger.debug(f"Skipping non-outreach draft: '{subject}'")
            continue

        # Parse To: header — "First Last <email@domain.com>" or just "email@domain.com"
        to_name = ""
        to_email = to_raw.strip()
        m = re.match(r"^(.*?)\s*<([^>]+)>$", to_raw)
        if m:
            to_name = m.group(1).strip().strip('"')
            to_email = m.group(2).strip()

        # Extract first name from To name
        first_name = to_name.split()[0] if to_name else ""

        # Extract company from subject line
        company = _extract_company_from_subject(subject)

        # Extract body text
        payload = msg.get("payload", {})
        plain_text, html_text = _extract_body_from_payload(payload)

        if plain_text:
            body_text = plain_text
        elif html_text:
            body_text = _html_to_text(html_text)
        else:
            body_text = ""

        # Strip the signature to get just the email body
        body_clean = _strip_signature(body_text)

        outreach_drafts.append({
            "draft_id": draft_id,
            "subject": subject,
            "to_email": to_email,
            "to_name": to_name,
            "first_name": first_name,
            "company": company,
            "body_text": body_clean,
        })

    logger.info(f"Found {len(outreach_drafts)} outreach drafts to rewrite")
    return outreach_drafts


# ═══════════════════════════════════════════════════════════════════════════════
# Draft creation & management
# ═══════════════════════════════════════════════════════════════════════════════

def _build_message(to_email: str, subject: str, body_text: str) -> str:
    """Build a MIME message and return base64url-encoded raw string."""
    body_html = body_text.replace("\n", "<br>\n")

    full_html = (
        '<div style="font-family:Arial,Helvetica,sans-serif;font-size:14px;'
        'color:#222222;line-height:1.5;">\n'
        + body_html
        + "\n"
        + config.EMAIL_SIGNATURE_HTML
        + "\n</div>"
    )

    message = MIMEMultipart("alternative")
    message["to"] = to_email
    message["from"] = f"{config.SENDER_NAME} <{config.SENDER_EMAIL}>"
    message["subject"] = subject

    plain_part = MIMEText(
        body_text
        + "\n\nCheers,\nPatrik Matheson\nDigital Strategy\n"
        "Video Marketing | Ahead of Market\n602.373.2164\naheadofmarket.com",
        "plain",
    )
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
