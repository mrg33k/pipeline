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
import time
from datetime import datetime
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


def _hours_to_newer_than_days(hours: int) -> int:
    """Convert hour window to Gmail query day window."""
    if hours <= 0:
        return 1
    return max(1, (hours + 23) // 24)


def _normalize_email(email: str) -> str:
    return (email or "").strip().lower()


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


def ensure_sent_check_available() -> None:
    """
    Validate Gmail SENT-query access required for duplicate protection.
    Raises RuntimeError when unavailable.
    """
    service = _get_gmail_service()
    try:
        service.users().messages().list(
            userId="me",
            labelIds=["SENT"],
            q="in:sent newer_than:1d",
            maxResults=1,
        ).execute()
    except Exception as e:  # pylint: disable=broad-except
        raise RuntimeError(
            "Gmail sent-mail duplicate check is unavailable. "
            "Run ./venv/bin/python reauth_gmail.py to grant required scopes."
        ) from e


def was_sent_to_recipient(email: str, hours: int = 48) -> bool:
    """
    Check whether a message was sent to recipient within the given time window.
    Raises RuntimeError on Gmail API failures.
    """
    email = _normalize_email(email)
    if not email:
        return False
    if hours <= 0:
        hours = 48

    service = _get_gmail_service()
    cutoff_ms = int((time.time() - (hours * 3600)) * 1000)
    days = _hours_to_newer_than_days(hours)
    query = f'in:sent to:"{email}" newer_than:{days}d'

    try:
        result = service.users().messages().list(
            userId="me",
            labelIds=["SENT"],
            q=query,
            maxResults=25,
        ).execute()
        stubs = result.get("messages", []) or []
        if not stubs:
            return False

        # Verify precise cutoff by message internalDate.
        for stub in stubs:
            msg = service.users().messages().get(
                userId="me",
                id=stub["id"],
                format="metadata",
                metadataHeaders=["To", "Cc", "Bcc"],
            ).execute()
            internal_ms = int(msg.get("internalDate", "0") or "0")
            if internal_ms and internal_ms < cutoff_ms:
                continue

            payload = msg.get("payload", {}) or {}
            headers = payload.get("headers", []) or []
            addrs = []
            for h in headers:
                name = (h.get("name", "") or "").lower()
                if name in {"to", "cc", "bcc"}:
                    addrs.append(h.get("value", "") or "")
            parsed = email_lib.utils.getaddresses(addrs)
            recipients = {_normalize_email(addr) for _, addr in parsed if _normalize_email(addr)}
            if email in recipients:
                return True
        return False
    except Exception as e:  # pylint: disable=broad-except
        raise RuntimeError(f"Failed sent-mail duplicate check for {email}: {e}") from e


def delete_outreach_draft_if_exists(
    to_email: str,
    dry_run: bool = False,
    known_emails: set | None = None,
    max_results: int = 500,
) -> dict:
    """
    Delete outreach drafts for a single recipient email.
    Returns summary dict with found/deleted counts.
    """
    target = _normalize_email(to_email)
    if not target:
        return {"found": 0, "deleted": 0, "failed": 0, "would_delete": 0}

    drafts = get_outreach_drafts(max_results=max_results, known_emails=known_emails or set())
    found = [d for d in drafts if _normalize_email(d.get("to_email", "")) == target]
    deleted = 0
    failed = 0
    would_delete = 0

    for draft in found:
        draft_id = draft["draft_id"]
        if dry_run:
            would_delete += 1
            logger.info(f"WOULD DELETE duplicate outreach draft: {draft_id} -> {target}")
            continue
        ok = delete_draft(draft_id)
        if ok:
            deleted += 1
        else:
            failed += 1

    return {
        "found": len(found),
        "deleted": deleted,
        "failed": failed,
        "would_delete": would_delete,
    }


def cleanup_duplicate_outreach_drafts(
    hours: int = 48,
    dry_run: bool = False,
    known_emails: set | None = None,
    max_results: int = 500,
) -> dict:
    """
    Find outreach drafts whose recipients were already emailed within window and delete them.
    Returns cleanup summary.
    """
    drafts = get_outreach_drafts(max_results=max_results, known_emails=known_emails or set())
    sent_cache: dict[str, bool] = {}
    duplicates_detected = 0
    deleted = 0
    failed = 0
    would_delete = 0

    for draft in drafts:
        to_email = _normalize_email(draft.get("to_email", ""))
        if not to_email:
            continue
        if to_email in sent_cache:
            already_sent = sent_cache[to_email]
        else:
            already_sent = was_sent_to_recipient(to_email, hours=hours)
            sent_cache[to_email] = already_sent
        if not already_sent:
            continue

        duplicates_detected += 1
        draft_id = draft["draft_id"]
        if dry_run:
            would_delete += 1
            logger.info(f"WOULD DELETE duplicate outreach draft: {draft_id} -> {to_email}")
            continue
        ok = delete_draft(draft_id)
        if ok:
            deleted += 1
        else:
            failed += 1

    return {
        "total_outreach_drafts": len(drafts),
        "duplicates_detected": duplicates_detected,
        "deleted": deleted,
        "failed": failed,
        "would_delete": would_delete,
    }


def get_recent_sent_recipients(hours: int = 48, max_results: int = 500) -> set[str]:
    """
    Return recipient email addresses from SENT messages in the last N hours.
    Best-effort: returns an empty set if API access is unavailable.
    """
    if hours <= 0:
        return set()

    service = _get_gmail_service()
    recipients = set()
    cutoff_ms = int((time.time() - (hours * 3600)) * 1000)
    days = max(1, (hours + 23) // 24)
    query = f"in:sent newer_than:{days}d"

    fetched = 0
    page_token = None
    try:
        while fetched < max_results:
            remaining = max_results - fetched
            result = service.users().messages().list(
                userId="me",
                labelIds=["SENT"],
                q=query,
                maxResults=min(200, remaining),
                pageToken=page_token,
            ).execute()
            stubs = result.get("messages", []) or []
            if not stubs:
                break

            for stub in stubs:
                if fetched >= max_results:
                    break
                msg = service.users().messages().get(
                    userId="me",
                    id=stub["id"],
                    format="metadata",
                    metadataHeaders=["To", "Cc", "Bcc"],
                ).execute()
                fetched += 1

                internal_ms = int(msg.get("internalDate", "0") or "0")
                if internal_ms and internal_ms < cutoff_ms:
                    continue

                payload = msg.get("payload", {}) or {}
                headers = payload.get("headers", []) or []
                to_values = []
                for h in headers:
                    name = (h.get("name", "") or "").lower()
                    if name in {"to", "cc", "bcc"}:
                        to_values.append(h.get("value", "") or "")
                if not to_values:
                    continue

                parsed = email_lib.utils.getaddresses(to_values)
                for _, email_addr in parsed:
                    email_clean = (email_addr or "").strip().lower()
                    if email_clean:
                        recipients.add(email_clean)

            page_token = result.get("nextPageToken")
            if not page_token:
                break
    except Exception as e:
        logger.warning(f"Could not fetch recent sent recipients: {e}")
        return set()

    logger.info(f"Recent sent recipients ({hours}h): {len(recipients)}")
    return recipients


def _header_value(headers: list[dict], name: str) -> str:
    lower = name.lower()
    for header in headers or []:
        if (header.get("name", "") or "").lower() == lower:
            return header.get("value", "") or ""
    return ""


def _header_emails(headers: list[dict], names: list[str]) -> set[str]:
    """
    Extract normalized email addresses from one or more headers.
    """
    values = []
    lookup = {n.lower() for n in names}
    for header in headers or []:
        if (header.get("name", "") or "").lower() in lookup:
            values.append(header.get("value", "") or "")
    parsed = email_lib.utils.getaddresses(values)
    return {_normalize_email(addr) for _, addr in parsed if _normalize_email(addr)}


def get_sent_history_for_recipient(email: str, days: int = 365, max_results: int = 8) -> list[dict]:
    """
    Fetch SENT messages to one recipient for dashboard history.
    Returns newest-first list. Best-effort; returns [] on failure.
    """
    target = _normalize_email(email)
    if not target:
        return []
    if days <= 0:
        days = 30

    service = _get_gmail_service()
    query = f'in:sent to:"{target}" newer_than:{days}d'
    try:
        result = service.users().messages().list(
            userId="me",
            labelIds=["SENT"],
            q=query,
            maxResults=max_results,
        ).execute()
        stubs = result.get("messages", []) or []
    except Exception as e:
        logger.warning(f"Could not fetch sent history for {target}: {e}")
        return []

    history: list[dict] = []
    for stub in stubs:
        try:
            msg = service.users().messages().get(
                userId="me",
                id=stub["id"],
                format="full",
            ).execute()
        except Exception as e:
            logger.warning(f"Could not fetch sent message {stub.get('id')}: {e}")
            continue

        payload = msg.get("payload", {}) or {}
        headers = payload.get("headers", []) or []
        subject = _header_value(headers, "Subject")
        to_raw = _header_value(headers, "To")
        date_raw = _header_value(headers, "Date")
        snippet = msg.get("snippet", "") or ""

        plain_text, html_text = _extract_body_from_payload(payload)
        if plain_text:
            body_text = plain_text.strip()
        elif html_text:
            body_text = _html_to_text(html_text).strip()
        else:
            body_text = ""

        internal_ms = int(msg.get("internalDate", "0") or "0")
        sent_at = (
            datetime.fromtimestamp(internal_ms / 1000).isoformat()
            if internal_ms
            else ""
        )

        history.append({
            "message_id": msg.get("id", ""),
            "to": to_raw,
            "subject": subject,
            "date_header": date_raw,
            "sent_at": sent_at,
            "snippet": snippet,
            "body_text": body_text[:6000],
        })

    return history


def get_thread_history_for_recipient(
    email: str,
    days: int = 365,
    max_threads: int = 10,
    max_messages: int = 40,
) -> list[dict]:
    """
    Fetch bidirectional Gmail thread messages with one recipient.
    Returns newest-first messages across matching threads.
    """
    target = _normalize_email(email)
    if not target:
        return []
    if days <= 0:
        days = 365
    if max_threads <= 0:
        max_threads = 10
    if max_messages <= 0:
        max_messages = 40

    service = _get_gmail_service()
    query = f'in:anywhere (to:"{target}" OR from:"{target}") newer_than:{days}d'

    try:
        result = service.users().messages().list(
            userId="me",
            q=query,
            maxResults=max(20, max_threads * 5),
        ).execute()
        stubs = result.get("messages", []) or []
    except Exception as e:
        logger.warning(f"Could not fetch thread history index for {target}: {e}")
        return []

    if not stubs:
        return []

    thread_ids = []
    seen_threads = set()
    for stub in stubs:
        tid = (stub.get("threadId") or "").strip()
        if not tid or tid in seen_threads:
            continue
        seen_threads.add(tid)
        thread_ids.append(tid)
        if len(thread_ids) >= max_threads:
            break

    history: list[dict] = []
    for tid in thread_ids:
        try:
            thread = service.users().threads().get(
                userId="me",
                id=tid,
                format="full",
            ).execute()
        except Exception as e:
            logger.warning(f"Could not fetch Gmail thread {tid}: {e}")
            continue

        messages = thread.get("messages", []) or []
        messages.sort(key=lambda m: int(m.get("internalDate", "0") or "0"))

        for msg in messages:
            payload = msg.get("payload", {}) or {}
            headers = payload.get("headers", []) or []

            from_raw = _header_value(headers, "From")
            to_raw = _header_value(headers, "To")
            subject = _header_value(headers, "Subject")
            date_raw = _header_value(headers, "Date")
            snippet = msg.get("snippet", "") or ""

            from_emails = _header_emails(headers, ["From"])
            recipient_emails = _header_emails(headers, ["To", "Cc", "Bcc"])
            if target not in from_emails and target not in recipient_emails:
                continue

            direction = "received" if target in from_emails else "sent"
            plain_text, html_text = _extract_body_from_payload(payload)
            if plain_text:
                body_text = plain_text.strip()
            elif html_text:
                body_text = _html_to_text(html_text).strip()
            else:
                body_text = ""

            internal_ms = int(msg.get("internalDate", "0") or "0")
            sent_at = (
                datetime.fromtimestamp(internal_ms / 1000).isoformat()
                if internal_ms
                else ""
            )

            history.append({
                "thread_id": tid,
                "message_id": msg.get("id", ""),
                "direction": direction,
                "from": from_raw,
                "to": to_raw,
                "subject": subject,
                "date_header": date_raw,
                "sent_at": sent_at,
                "snippet": snippet,
                "body_text": body_text[:8000],
            })
            if len(history) >= max_messages:
                break
        if len(history) >= max_messages:
            break

    history.sort(
        key=lambda item: item.get("sent_at", "") or item.get("date_header", ""),
        reverse=True,
    )
    return history


def get_recent_sent_activity(
    hours: int = 24 * 30,
    max_results: int = 120,
    allowed_recipients: set[str] | None = None,
) -> list[dict]:
    """
    Fetch recent SENT activity feed items.
    Returns newest-first list of summary dicts. Best-effort; returns [] on failure.
    """
    if hours <= 0:
        hours = 24
    service = _get_gmail_service()
    allowed = {_normalize_email(x) for x in (allowed_recipients or set()) if _normalize_email(x)}
    days = _hours_to_newer_than_days(hours)
    query = f"in:sent newer_than:{days}d"

    try:
        result = service.users().messages().list(
            userId="me",
            labelIds=["SENT"],
            q=query,
            maxResults=max_results,
        ).execute()
        stubs = result.get("messages", []) or []
    except Exception as e:
        logger.warning(f"Could not fetch sent activity: {e}")
        return []

    activity: list[dict] = []
    for stub in stubs:
        try:
            msg = service.users().messages().get(
                userId="me",
                id=stub["id"],
                format="metadata",
                metadataHeaders=["To", "Subject", "Date"],
            ).execute()
        except Exception as e:
            logger.warning(f"Could not fetch sent activity message {stub.get('id')}: {e}")
            continue

        payload = msg.get("payload", {}) or {}
        headers = payload.get("headers", []) or []
        to_raw = _header_value(headers, "To")
        subject = _header_value(headers, "Subject")
        date_raw = _header_value(headers, "Date")
        snippet = msg.get("snippet", "") or ""
        internal_ms = int(msg.get("internalDate", "0") or "0")
        sent_at = (
            datetime.fromtimestamp(internal_ms / 1000).isoformat()
            if internal_ms
            else ""
        )

        parsed = email_lib.utils.getaddresses([to_raw])
        _, to_email = parsed[0] if parsed else ("", "")
        to_email_norm = _normalize_email(to_email)
        if allowed and to_email_norm not in allowed:
            continue
        activity.append({
            "message_id": msg.get("id", ""),
            "to_email": to_email_norm,
            "to_raw": to_raw,
            "subject": subject,
            "date_header": date_raw,
            "sent_at": sent_at,
            "snippet": snippet,
        })

    return activity


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
