"""
Gmail API client for creating draft emails.
Uses OAuth2 credentials with refresh token.
Creates DRAFTS only, never sends.
"""

import base64
import json
import logging
import os
import re
from typing import Optional
from email.utils import parseaddr
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import config

logger = logging.getLogger(__name__)

GMAIL_COMPOSE_SCOPE = "https://www.googleapis.com/auth/gmail.compose"
GMAIL_SETTINGS_SCOPE = "https://www.googleapis.com/auth/gmail.settings.basic"
_SERVICE_CACHE = {}


def _get_gmail_service(scopes: Optional[list] = None):
    """Build and return an authenticated Gmail API service."""
    resolved_scopes = tuple(sorted(scopes or [GMAIL_COMPOSE_SCOPE]))
    cache_key = "|".join(resolved_scopes)
    cached = _SERVICE_CACHE.get(cache_key)
    if cached is not None:
        return cached

    # Load tokens
    with open(config.GMAIL_TOKENS_PATH, "r") as f:
        token_data = json.load(f)

    # Load client secret for client_id and client_secret
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
        scopes=list(resolved_scopes),
    )

    # Refresh if expired
    if creds.expired or not creds.valid:
        logger.info("Refreshing Gmail access token...")
        creds.refresh(Request())
        # Save updated tokens
        new_token_data = {
            "access_token": creds.token,
            "refresh_token": creds.refresh_token,
            "scope": " ".join(creds.scopes) if creds.scopes else token_data.get("scope", ""),
            "token_type": "Bearer",
        }
        with open(config.GMAIL_TOKENS_PATH, "w") as f:
            json.dump(new_token_data, f, indent=2)
        logger.info("Token refreshed and saved.")

    # cache_discovery=False prevents noisy "file_cache is only supported ..." logs.
    service = build("gmail", "v1", credentials=creds, cache_discovery=False)
    _SERVICE_CACHE[cache_key] = service
    return service


def create_draft(
    to_email: str,
    subject: str,
    body_text: str,
    signature_html: Optional[str] = None,
) -> Optional[dict]:
    """
    Create a Gmail draft with HTML body (plain text email + HTML signature).
    Returns the draft resource or None on failure.
    """
    service = _get_gmail_service()
    raw = _build_raw_message(to_email, subject, body_text, signature_html=signature_html)

    try:
        draft = service.users().drafts().create(
            userId="me",
            body={"message": {"raw": raw}}
        ).execute()
        logger.info(f"Draft created: {draft['id']} -> {to_email} ({subject})")
        return draft
    except Exception as e:
        logger.error(f"Failed to create draft for {to_email}: {e}")
        return None


def update_draft(
    draft_id: str,
    to_email: str,
    subject: str,
    body_text: str,
    signature_html: Optional[str] = None,
) -> Optional[dict]:
    """Update an existing Gmail draft by ID."""
    if not draft_id:
        return None

    service = _get_gmail_service()
    raw = _build_raw_message(to_email, subject, body_text, signature_html=signature_html)

    try:
        draft = service.users().drafts().update(
            userId="me",
            id=draft_id,
            body={"id": draft_id, "message": {"raw": raw}},
        ).execute()
        logger.info(f"Draft updated: {draft['id']} -> {to_email} ({subject})")
        return draft
    except Exception as e:
        logger.error(f"Failed to update draft {draft_id} for {to_email}: {e}")
        return None


def delete_draft(draft_id: str) -> bool:
    """Delete an existing Gmail draft by ID."""
    if not draft_id:
        return False

    service = _get_gmail_service()
    try:
        service.users().drafts().delete(userId="me", id=draft_id).execute()
        logger.info(f"Draft deleted: {draft_id}")
        return True
    except HttpError as e:
        # Treat "already gone" as successful deletion for rewrite flow.
        if getattr(e, "status_code", None) == 404 or "404" in str(e):
            logger.info(f"Draft already missing, continuing: {draft_id}")
            return True
        logger.error(f"Failed to delete draft {draft_id}: {e}")
        return False
    except Exception as e:
        logger.error(f"Failed to delete draft {draft_id}: {e}")
        return False


def draft_exists(draft_id: str) -> bool:
    """Return True when a draft id currently exists in the mailbox."""
    if not draft_id:
        return False
    service = _get_gmail_service()
    try:
        service.users().drafts().get(userId="me", id=draft_id, format="minimal").execute()
        return True
    except HttpError as e:
        if getattr(e, "status_code", None) == 404 or "404" in str(e):
            return False
        logger.debug(f"Unable to verify draft {draft_id}: {e}")
        return False
    except Exception as e:
        logger.debug(f"Unable to verify draft {draft_id}: {e}")
        return False


def create_drafts_batch(
    emails: list[dict],
    signature_html: Optional[str] = None,
) -> list[dict]:
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
        draft = create_draft(to_email, subject, body, signature_html=signature_html)

        results.append({
            "to": to_email,
            "subject": subject,
            "draft_id": draft["id"] if draft else None,
            "success": draft is not None,
        })

    successful = sum(1 for r in results if r["success"])
    logger.info(f"Drafts created: {successful}/{len(emails)}")
    return results


def rewrite_drafts_batch(
    emails: list[dict],
    draft_ids_by_email: dict,
    create_missing: bool = True,
    signature_html: Optional[str] = None,
) -> list[dict]:
    """
    Replace today's drafts by deleting existing ones and creating new drafts.
    If create_missing=True, create a draft when no prior draft_id exists.
    """
    results = []
    for i, item in enumerate(emails):
        to_email = item["profile"]["email"]
        subject = item["subject"]
        body = item["body"]
        existing_id = (draft_ids_by_email.get(to_email) or "").strip()

        logger.info(f"Replacing draft {i + 1}/{len(emails)}: {to_email}")
        if existing_id:
            deleted = delete_draft(existing_id)
            if deleted:
                draft = create_draft(to_email, subject, body, signature_html=signature_html)
            else:
                draft = None
        elif create_missing:
            draft = create_draft(to_email, subject, body, signature_html=signature_html)
        else:
            draft = None
            logger.warning(f"No existing draft_id for {to_email}; skipped.")

        results.append(
            {
                "to": to_email,
                "subject": subject,
                "draft_id": draft["id"] if draft else existing_id or None,
                "success": draft is not None if existing_id or create_missing else False,
            }
        )

    successful = sum(1 for r in results if r["success"])
    logger.info(f"Drafts rewritten: {successful}/{len(emails)}")
    return results


def _build_raw_message(
    to_email: str,
    subject: str,
    body_text: str,
    signature_html: Optional[str] = None,
) -> str:
    """Build a raw MIME message for Gmail drafts API."""
    return _build_raw_message_with_signature(to_email, subject, body_text, signature_html or "")


def _build_raw_message_with_signature(
    to_email: str,
    subject: str,
    body_text: str,
    signature_html: str,
) -> str:
    """Build MIME message from body + selected HTML signature."""
    # Convert plain text body to HTML (preserve line breaks)
    body_html = body_text.replace("\n", "<br>\n")

    # Build full HTML email
    full_html = f"""<div style="font-family:Arial,Helvetica,sans-serif;font-size:14px;color:#222222;line-height:1.5;">
{body_html}
{signature_html}
</div>"""

    message = MIMEMultipart("alternative")
    message["to"] = to_email
    message["from"] = f"{config.SENDER_NAME} <{config.SENDER_EMAIL}>"
    message["subject"] = subject

    # Plain text fallback (no hardcoded app signature)
    plain_part = MIMEText(body_text, "plain")
    html_part = MIMEText(full_html, "html")

    message.attach(plain_part)
    message.attach(html_part)

    return base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")


def list_account_signatures() -> list[dict]:
    """
    Return available Gmail send-as signatures.
    Requires Gmail settings API scope; returns [] when unavailable.
    """
    service = _get_gmail_service(scopes=[GMAIL_SETTINGS_SCOPE])
    try:
        result = service.users().settings().sendAs().list(userId="me").execute()
    except HttpError as e:
        message = str(e)
        if "insufficient" in message.lower() or "invalid_scope" in message.lower() or "403" in message or "401" in message:
            logger.warning(
                "signature_scope_missing: Gmail signature access unavailable. "
                "Run `python3 reauth_gmail.py` to enable account default signature loading."
            )
            return []
        logger.debug(f"Unable to list Gmail account signatures: {e}")
        return []
    except Exception as e:
        logger.debug(f"Unable to list Gmail account signatures: {e}")
        return []

    options = []
    for item in result.get("sendAs", []):
        sig = item.get("signature", "")
        if not sig:
            continue
        options.append(
            {
                "send_as_email": item.get("sendAsEmail", ""),
                "display_name": item.get("displayName", ""),
                "is_primary": bool(item.get("isPrimary", False)),
                "is_default": bool(item.get("isDefault", False)),
                "signature_html": sig,
            }
        )
    return options


def list_recent_outreach_drafts(max_results: int = 50, subject_filter: bool = True) -> list[dict]:
    """
    Return recent outreach drafts created by this pipeline.
    Uses subject prefix filter and extracts To/Subject from metadata.
    """
    service = _get_gmail_service(scopes=[GMAIL_COMPOSE_SCOPE])
    results = []
    page_token = None
    fetched = 0
    page_size = min(max(max_results, 1), 500)

    try:
        while True:
            req = service.users().drafts().list(
                userId="me",
                maxResults=page_size,
                pageToken=page_token,
            )
            listing = req.execute()
            drafts = listing.get("drafts", [])
            if not drafts:
                break
            fetched += len(drafts)

            for item in drafts:
                draft_id = item.get("id", "")
                if not draft_id:
                    continue
                try:
                    draft = service.users().drafts().get(
                        userId="me",
                        id=draft_id,
                        format="metadata",
                        metadataHeaders=["To", "Subject"],
                    ).execute()
                except Exception as e:
                    logger.debug(f"Unable to read draft metadata for {draft_id}: {e}")
                    continue

                headers = draft.get("message", {}).get("payload", {}).get("headers", [])
                to_header = ""
                subject = ""
                for h in headers:
                    name = (h.get("name") or "").lower()
                    if name == "to":
                        to_header = h.get("value", "")
                    elif name == "subject":
                        subject = h.get("value", "")
                _, to_email = parseaddr(to_header)
                to_email = (to_email or "").strip()
                if not to_email and to_header:
                    m = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", to_header)
                    if m:
                        to_email = m.group(0).strip()
                if not to_email:
                    continue

                lower_subject = (subject or "").strip().lower()
                if subject_filter:
                    if not (
                        lower_subject.startswith("video for ")
                        or lower_subject.startswith("video idea for ")
                    ):
                        continue

                results.append(
                    {
                        "draft_id": draft_id,
                        "to": to_email,
                        "subject": subject,
                    }
                )
                if len(results) >= max_results:
                    return results

            page_token = listing.get("nextPageToken")
            if not page_token:
                break
            if fetched >= max_results * 5:
                # Safety cap to avoid excessive API calls on very large mailboxes.
                break
    except Exception as e:
        logger.debug(f"Unable to list outreach drafts: {e}")
        return []

    return results
