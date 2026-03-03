from __future__ import annotations
"""
Gmail API client for creating, updating, and deleting draft emails.
Uses OAuth2 credentials with refresh token.
Creates DRAFTS only, never sends.
"""

import base64
import json
import logging
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

import config

logger = logging.getLogger(__name__)

# Cache the service to avoid re-authenticating on every call
_service_cache = None


def _get_gmail_service():
    """Build and return an authenticated Gmail API service."""
    global _service_cache
    if _service_cache is not None:
        return _service_cache

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
        scopes=["https://www.googleapis.com/auth/gmail.compose"],
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

    _service_cache = build("gmail", "v1", credentials=creds)
    return _service_cache


def _build_message(to_email: str, subject: str, body_text: str) -> str:
    """Build a MIME message and return base64url-encoded raw string."""
    # Convert plain text body to HTML (preserve line breaks)
    body_html = body_text.replace("\n", "<br>\n")

    full_html = f"""<div style="font-family:Arial,Helvetica,sans-serif;font-size:14px;color:#222222;line-height:1.5;">
{body_html}
{config.EMAIL_SIGNATURE_HTML}
</div>"""

    message = MIMEMultipart("alternative")
    message["to"] = to_email
    message["from"] = f"{config.SENDER_NAME} <{config.SENDER_EMAIL}>"
    message["subject"] = subject

    # Plain text fallback
    plain_part = MIMEText(
        body_text + "\n\nCheers,\nPatrik Matheson\nDigital Strategy\n"
        "Video Marketing | Ahead of Market\n602.373.2164\naheadofmarket.com",
        "plain",
    )
    html_part = MIMEText(full_html, "html")

    message.attach(plain_part)
    message.attach(html_part)

    return base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")


def create_draft(to_email: str, subject: str, body_text: str) -> dict | None:
    """
    Create a Gmail draft with HTML body + signature.
    Returns the draft resource dict or None on failure.
    """
    service = _get_gmail_service()
    raw = _build_message(to_email, subject, body_text)

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
    Returns the updated draft resource or None on failure.
    """
    service = _get_gmail_service()
    raw = _build_message(to_email, subject, body_text)

    try:
        draft = service.users().drafts().update(
            userId="me",
            id=draft_id,
            body={"message": {"raw": raw}}
        ).execute()
        logger.info(f"Draft updated: {draft_id} -> {to_email} ({subject})")
        return draft
    except Exception as e:
        logger.warning(f"Failed to update draft {draft_id}, will try delete+create: {e}")
        # Fallback: delete old and create new
        delete_draft(draft_id)
        return create_draft(to_email, subject, body_text)


def list_drafts(max_results: int = 100) -> list[dict]:
    """List Gmail drafts. Returns list of draft summary dicts."""
    service = _get_gmail_service()
    try:
        result = service.users().drafts().list(userId="me", maxResults=max_results).execute()
        drafts = result.get("drafts", [])
        logger.info(f"Found {len(drafts)} drafts in Gmail")
        return drafts
    except Exception as e:
        logger.error(f"Failed to list drafts: {e}")
        return []


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
