import base64
import json
import logging
import os
from email.utils import getaddresses
from email.mime.text import MIMEText
from typing import Dict, List

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

import config

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/gmail.compose",
    "https://www.googleapis.com/auth/gmail.settings.basic",
]


def _load_creds() -> Credentials:
    if not os.path.exists(config.GMAIL_TOKENS_PATH):
        raise RuntimeError(f"Missing Gmail token file: {config.GMAIL_TOKENS_PATH}")
    with open(config.GMAIL_TOKENS_PATH, "r", encoding="utf-8") as f:
        token_data = json.load(f)

    # Fast path for canonical google-auth authorized_user format.
    try:
        creds = Credentials.from_authorized_user_info(token_data, SCOPES)
    except Exception:
        # Compat path for older token payloads (e.g. access_token/scope only).
        normalized = dict(token_data)
        if "token" not in normalized and normalized.get("access_token"):
            normalized["token"] = normalized.get("access_token")
        if "scopes" not in normalized and normalized.get("scope"):
            scope_val = normalized.get("scope")
            if isinstance(scope_val, str):
                normalized["scopes"] = [s for s in scope_val.split() if s]

        client_info = {}
        if os.path.exists(config.GMAIL_CLIENT_SECRET):
            with open(config.GMAIL_CLIENT_SECRET, "r", encoding="utf-8") as f:
                secret_doc = json.load(f)
            client_info = secret_doc.get("installed") or secret_doc.get("web") or {}

        if "client_id" not in normalized and client_info.get("client_id"):
            normalized["client_id"] = client_info.get("client_id")
        if "client_secret" not in normalized and client_info.get("client_secret"):
            normalized["client_secret"] = client_info.get("client_secret")
        if "token_uri" not in normalized:
            normalized["token_uri"] = client_info.get("token_uri") or "https://oauth2.googleapis.com/token"

        try:
            creds = Credentials.from_authorized_user_info(normalized, SCOPES)
        except Exception as exc:
            raise RuntimeError(
                "Invalid Gmail token format. Run re-auth to regenerate gmail_tokens.json "
                "with client_id/client_secret fields."
            ) from exc

    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return creds


def _service():
    return build("gmail", "v1", credentials=_load_creds(), cache_discovery=False)


def _build_raw_message(to_email: str, subject: str, body: str, signature_html: str = "") -> str:
    full = body
    if signature_html:
        full = f"{body}\n\n--\n{signature_html}"
    msg = MIMEText(full, _charset="utf-8")
    msg["to"] = to_email
    msg["subject"] = subject
    return base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")


def choose_signature_html() -> str:
    try:
        svc = _service()
        send_as = svc.users().settings().sendAs().list(userId="me").execute().get("sendAs", [])
        if not send_as:
            return ""
        selected = None
        for item in send_as:
            if item.get("isDefault"):
                selected = item
                break
        if selected is None:
            selected = send_as[0]
        logger.info("Using Gmail account signature: %s", selected.get("sendAsEmail", ""))
        return selected.get("signature", "") or ""
    except Exception as exc:
        logger.info("signature_lookup_failed:%s", exc)
        return ""


def create_draft(to_email: str, subject: str, body: str, signature_html: str = "") -> Dict:
    svc = _service()
    raw = _build_raw_message(to_email, subject, body, signature_html)
    resp = svc.users().drafts().create(userId="me", body={"message": {"raw": raw}}).execute()
    return {"success": True, "draft_id": resp.get("id", "")}


def delete_draft(draft_id: str) -> bool:
    try:
        _service().users().drafts().delete(userId="me", id=draft_id).execute()
        return True
    except Exception as exc:
        logger.info("draft_delete_failed:%s:%s", draft_id, exc)
        return False


def _extract_primary_email(to_header: str) -> str:
    if not to_header:
        return ""
    parsed = getaddresses([to_header])
    for _, addr in parsed:
        email = (addr or "").strip().lower()
        if email:
            return email
    return ""


def list_recent_outreach_drafts(max_results: int = 200, subject_prefix: str = "") -> List[Dict]:
    svc = _service()
    # Pull draft IDs first, then fetch minimal headers.
    resp = svc.users().drafts().list(userId="me", maxResults=max_results).execute()
    drafts = resp.get("drafts", []) or []
    rows: List[Dict] = []
    skipped_subject = 0
    skipped_no_to = 0
    skipped_errors = 0
    for d in drafts:
        did = d.get("id", "")
        if not did:
            continue
        try:
            # `users.drafts.get` does not accept `metadataHeaders` in this client version.
            # Use full payload and extract headers from message payload.
            full = svc.users().drafts().get(userId="me", id=did, format="full").execute()
            msg = full.get("message", {})
            payload = msg.get("payload", {})
            headers = {h.get("name", "").lower(): h.get("value", "") for h in (payload.get("headers", []) or [])}
            subject = (headers.get("subject", "") or "").strip()
            if subject_prefix and not subject.startswith(subject_prefix):
                skipped_subject += 1
                continue
            raw_to = headers.get("to", "") or ""
            to_email = _extract_primary_email(raw_to)
            if not to_email:
                skipped_no_to += 1
                continue
            rows.append({"draft_id": did, "to": to_email, "to_raw": raw_to, "subject": subject})
        except Exception as exc:
            logger.info("draft_header_parse_failed:%s:%s", did, exc)
            skipped_errors += 1
            continue
    logger.info(
        "draft_scan:total=%d kept=%d skipped_subject=%d skipped_no_to=%d skipped_errors=%d",
        len(drafts),
        len(rows),
        skipped_subject,
        skipped_no_to,
        skipped_errors,
    )
    return rows


def rewrite_draft(draft_id: str, to_email: str, subject: str, body: str, signature_html: str = "") -> Dict:
    ok = delete_draft(draft_id)
    if not ok:
        return {"success": False, "reason": "delete_failed", "draft_id": draft_id}
    created = create_draft(to_email=to_email, subject=subject, body=body, signature_html=signature_html)
    return {"success": True, "old_draft_id": draft_id, "new_draft_id": created.get("draft_id", "")}
