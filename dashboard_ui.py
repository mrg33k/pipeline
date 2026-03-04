from __future__ import annotations

import json
import threading
import time
import webbrowser
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import apollo_client
import gmail_drafter


def _parse_iso(ts: str) -> datetime:
    try:
        return datetime.fromisoformat((ts or "").strip())
    except Exception:
        return datetime.min


def _latest_contact_ts(info: dict) -> str:
    candidates = [
        info.get("rewritten_at", ""),
        info.get("drafted_at", ""),
        info.get("sent_at", ""),
        info.get("enriched_at", ""),
        info.get("added", ""),
    ]
    candidates = [c for c in candidates if isinstance(c, str) and c.strip()]
    if not candidates:
        return ""
    return max(candidates, key=_parse_iso)


def _coalesce_name(first_name: str, last_name: str, fallback: str = "") -> str:
    full = " ".join(part for part in [first_name, last_name] if part).strip()
    if full:
        return full
    return fallback or ""


class _DashboardState:
    def __init__(self, db):
        self.db = db
        self._lock = threading.Lock()
        self._cache: dict[str, dict] = {}

    def _get_cached(self, key: str, ttl_seconds: int, loader):
        now = time.time()
        with self._lock:
            entry = self._cache.get(key)
            if entry and now - entry["ts"] < ttl_seconds:
                return entry["value"]

        value = loader()
        with self._lock:
            self._cache[key] = {"ts": now, "value": value}
        return value

    def clear_cache(self):
        with self._lock:
            self._cache = {}

    def sent_access(self) -> tuple[bool, str]:
        def _load():
            try:
                gmail_drafter.ensure_sent_check_available()
                return True, ""
            except Exception as e:  # pylint: disable=broad-except
                return False, str(e)

        return self._get_cached("sent_access", 30, _load)

    def _db_contacts_by_email(self) -> dict[str, dict]:
        contacts = (self.db.data or {}).get("contacts", {}) or {}
        by_email: dict[str, dict] = {}
        for cid, info in contacts.items():
            email = (info.get("email") or "").strip().lower()
            if not email:
                continue
            by_email[email] = {"id": cid, **info}
        return by_email

    def contacts(self) -> tuple[list[dict], str]:
        def _load():
            db_by_email = self._db_contacts_by_email()
            try:
                apollo_contacts = apollo_client.list_owned_contacts(per_page=100, max_pages=200)
            except Exception as e:  # pylint: disable=broad-except
                return [], f"Apollo contacts load failed: {e}"

            results: list[dict] = []
            for contact in apollo_contacts:
                email = (contact.get("email") or "").strip().lower()
                db_info = db_by_email.get(email, {}) if email else {}
                merged_ts_source = {
                    "rewritten_at": db_info.get("rewritten_at", ""),
                    "drafted_at": db_info.get("drafted_at", ""),
                    "sent_at": db_info.get("sent_at", ""),
                    "enriched_at": db_info.get("enriched_at", ""),
                    "added": db_info.get("added", ""),
                }

                results.append({
                    "id": contact.get("id") or db_info.get("id", ""),
                    "email": email,
                    "first_name": contact.get("first_name", "") or db_info.get("first_name", ""),
                    "last_name": contact.get("last_name", "") or db_info.get("last_name", ""),
                    "name": contact.get("name") or _coalesce_name(
                        db_info.get("first_name", ""),
                        db_info.get("last_name", ""),
                        fallback=email,
                    ),
                    "company": contact.get("company", "") or db_info.get("company", ""),
                    "title": contact.get("title", "") or db_info.get("title", ""),
                    "industry": contact.get("industry", "") or db_info.get("industry", ""),
                    "city": contact.get("city", "") or db_info.get("city", ""),
                    "state": contact.get("state", "") or db_info.get("state", ""),
                    "domain": contact.get("domain", "") or db_info.get("domain", ""),
                    "enriched": bool(db_info.get("enriched")) or bool(email),
                    "drafted": bool(db_info.get("drafted")),
                    "draft_id": db_info.get("draft_id", ""),
                    "subject": db_info.get("subject", ""),
                    "emailed_body": db_info.get("emailed_body", ""),
                    "enriched_at": db_info.get("enriched_at", ""),
                    "drafted_at": db_info.get("drafted_at", ""),
                    "rewritten_at": db_info.get("rewritten_at", ""),
                    "sent_at": db_info.get("sent_at", ""),
                    "latest_ts": _latest_contact_ts(merged_ts_source),
                })

            results.sort(key=lambda item: _parse_iso(item.get("latest_ts", "")), reverse=True)
            return results, ""

        return self._get_cached("contacts", 180, _load)

    def apollo_email_set(self) -> tuple[set[str], str]:
        contacts, err = self.contacts()
        emails = {
            (c.get("email") or "").strip().lower()
            for c in contacts
            if (c.get("email") or "").strip()
        }
        return emails, err

    def outreach_drafts(self) -> list[dict]:
        def _load():
            try:
                drafts = gmail_drafter.get_outreach_drafts(max_results=500)
                for draft in drafts:
                    draft["to_email"] = (draft.get("to_email") or "").strip().lower()
                return drafts
            except Exception:
                return []

        return self._get_cached("drafts", 30, _load)

    def sent_activity(self) -> tuple[list[dict], str]:
        can_read_sent, sent_error = self.sent_access()
        if not can_read_sent:
            return [], sent_error

        known_emails, contacts_error = self.apollo_email_set()
        if contacts_error:
            return [], contacts_error

        def _load():
            try:
                activity = gmail_drafter.get_recent_sent_activity(
                    hours=24 * 365 * 5,
                    max_results=120,
                )
                accounted = {
                    (item.get("to_email") or "").strip().lower()
                    for item in activity
                    if (item.get("to_email") or "").strip()
                }
                recent_db_emails = self.db.get_recent_contact_emails(hours=24 * 365 * 5)
                db_contacts = (self.db.data or {}).get("contacts", {}) or {}
                latest_by_email: dict[str, str] = {}
                for info in db_contacts.values():
                    email = (info.get("email") or "").strip().lower()
                    if not email or email not in recent_db_emails:
                        continue
                    ts = _latest_contact_ts(info)
                    if ts:
                        current = latest_by_email.get(email, "")
                        if not current or _parse_iso(ts) > _parse_iso(current):
                            latest_by_email[email] = ts

                for email in sorted(recent_db_emails):
                    if email in accounted:
                        continue
                    activity.append({
                        "message_id": f"local:{email}",
                        "to_email": email,
                        "to_raw": email,
                        "subject": "(from local contact log)",
                        "date_header": "",
                        "sent_at": latest_by_email.get(email, ""),
                        "snippet": "",
                        "source": "local_db",
                    })
                return activity
            except Exception:
                return []

        return self._get_cached("sent_activity", 45, _load), ""

    def sent_history(self, email: str) -> tuple[list[dict], str]:
        target = (email or "").strip().lower()
        if not target:
            return [], ""

        can_read_sent, sent_error = self.sent_access()
        if not can_read_sent:
            return [], sent_error

        _, contacts_error = self.apollo_email_set()
        if contacts_error:
            return [], contacts_error

        def _load():
            try:
                return gmail_drafter.get_thread_history_for_recipient(
                    target,
                    days=365 * 5,
                    max_threads=12,
                    max_messages=60,
                )
            except Exception:
                return []

        return self._get_cached(f"sent_history:{target}", 45, _load), ""


def _match_query(item: dict, q: str) -> bool:
    if not q:
        return True
    blob = " ".join(
        str(item.get(key, ""))
        for key in ("name", "email", "company", "title", "industry", "subject", "snippet", "to_email", "to_raw")
    ).lower()
    return all(token in blob for token in q.lower().split())


def _build_activity_feed(db_runs: list[dict], contacts: list[dict], sent_activity: list[dict]) -> list[dict]:
    feed: list[dict] = []
    for run in db_runs[-40:]:
        feed.append({
            "kind": "run",
            "title": f"Pipeline run ({run.get('mode', 'unknown')})",
            "subtitle": f"{run.get('contacts_processed', 0)} processed",
            "ts": run.get("date", ""),
        })

    for contact in contacts[:200]:
        if contact.get("rewritten_at"):
            feed.append({
                "kind": "rewrite",
                "title": f"Draft rewritten for {contact.get('name') or contact.get('email')}",
                "subtitle": contact.get("company", ""),
                "ts": contact.get("rewritten_at", ""),
            })
        elif contact.get("drafted_at"):
            feed.append({
                "kind": "draft",
                "title": f"Draft created for {contact.get('name') or contact.get('email')}",
                "subtitle": contact.get("subject", ""),
                "ts": contact.get("drafted_at", ""),
            })
        elif contact.get("enriched_at"):
            feed.append({
                "kind": "apollo",
                "title": f"Apollo enriched {contact.get('name') or contact.get('email')}",
                "subtitle": contact.get("company", ""),
                "ts": contact.get("enriched_at", ""),
            })

    for sent in sent_activity[:120]:
        target = sent.get("to_raw") or sent.get("to_email")
        feed.append({
            "kind": "sent",
            "title": f"Sent to {target}",
            "subtitle": sent.get("subject", ""),
            "ts": sent.get("sent_at", ""),
        })

    feed.sort(key=lambda item: _parse_iso(item.get("ts", "")), reverse=True)
    return feed[:120]


def _build_dashboard_payload(state: _DashboardState, fast: bool = False) -> dict:
    contacts, contacts_error = state.contacts()
    if fast:
        drafts = []
        sent_activity = []
        sent_error = ""
    else:
        drafts = state.outreach_drafts()
        sent_activity, sent_error = state.sent_activity()

    drafts_by_email: dict[str, list[dict]] = {}
    for draft in drafts:
        email = (draft.get("to_email") or "").strip().lower()
        if not email:
            continue
        drafts_by_email.setdefault(email, []).append(draft)

    sent_by_email: dict[str, dict] = {}
    for sent in sent_activity:
        email = (sent.get("to_email") or "").strip().lower()
        if not email:
            continue
        current = sent_by_email.get(email)
        if not current or _parse_iso(sent.get("sent_at", "")) > _parse_iso(current.get("sent_at", "")):
            sent_by_email[email] = sent

    contact_emails = {item.get("email", "") for item in contacts if item.get("email")}
    contacts_by_email = {
        (item.get("email") or "").strip().lower(): item
        for item in contacts
        if (item.get("email") or "").strip()
    }
    results: list[dict] = []

    for contact in contacts:
        email = contact.get("email", "")
        results.append({
            "type": "contact",
            "id": contact.get("id", ""),
            "name": contact.get("name", ""),
            "email": email,
            "company": contact.get("company", ""),
            "title": contact.get("title", ""),
            "industry": contact.get("industry", ""),
            "subject": contact.get("subject", ""),
            "latest_ts": contact.get("latest_ts", ""),
            "has_draft": email in drafts_by_email,
            "has_sent": email in sent_by_email,
        })

    for draft in drafts:
        email = draft.get("to_email", "")
        contact = contacts_by_email.get(email, {})
        results.append({
            "type": "draft",
            "id": draft.get("draft_id", ""),
            "name": contact.get("name", "") or draft.get("to_name", "") or draft.get("first_name", "") or email,
            "email": email,
            "company": contact.get("company", "") or draft.get("company", ""),
            "title": contact.get("title", ""),
            "industry": contact.get("industry", ""),
            "subject": draft.get("subject", ""),
            "latest_ts": "",
            "has_draft": True,
            "has_sent": email in sent_by_email,
        })

    for email, sent in sent_by_email.items():
        contact = contacts_by_email.get(email, {})
        results.append({
            "type": "sent",
            "id": sent.get("message_id", ""),
            "name": contact.get("name", "") or sent.get("to_raw", "") or email,
            "email": email,
            "company": contact.get("company", ""),
            "title": contact.get("title", ""),
            "industry": contact.get("industry", ""),
            "subject": sent.get("subject", ""),
            "latest_ts": sent.get("sent_at", ""),
            "has_draft": email in drafts_by_email,
            "has_sent": True,
        })

    results.sort(
        key=lambda item: (
            0 if item.get("has_sent") else 1,
            _parse_iso(item.get("latest_ts", "")),
        ),
        reverse=True,
    )

    runs = ((state.db.data or {}).get("runs", []) or [])
    activity = _build_activity_feed(runs, contacts, sent_activity)

    summary = {
        "apollo_contacts": len(contacts),
        "enriched_contacts": sum(1 for c in contacts if c.get("enriched")),
        "drafted_contacts": sum(1 for c in contacts if c.get("drafted")),
        "outreach_drafts": len(drafts),
        "sent_messages_window": len(sent_activity),
        "sent_unique_recipients": len(sent_by_email),
        "apollo_contact_emails": len(contact_emails),
    }

    return {
        "summary": summary,
        "results": results[:350],
        "activity": activity[:120],
        "sent_access_error": sent_error,
        "contacts_error": contacts_error,
    }


def _build_history_payload(state: _DashboardState, email: str) -> dict:
    target = (email or "").strip().lower()
    if not target:
        return {"error": "email is required"}

    contacts, contacts_error = state.contacts()
    contact = {}
    for item in contacts:
        if (item.get("email") or "").strip().lower() == target:
            contact = item
            break

    if not contact:
        db_contact = state.db.get_contact_by_email(target) or {}
        if db_contact:
            contact = {
                "id": db_contact.get("id", ""),
                "first_name": db_contact.get("first_name", ""),
                "last_name": db_contact.get("last_name", ""),
                "name": _coalesce_name(
                    db_contact.get("first_name", ""),
                    db_contact.get("last_name", ""),
                    fallback=target,
                ),
                "email": target,
                "company": db_contact.get("company", ""),
                "title": db_contact.get("title", ""),
                "industry": db_contact.get("industry", ""),
                "city": db_contact.get("city", ""),
                "state": db_contact.get("state", ""),
                "subject": db_contact.get("subject", ""),
                "drafted_at": db_contact.get("drafted_at", ""),
                "rewritten_at": db_contact.get("rewritten_at", ""),
                "sent_at": db_contact.get("sent_at", ""),
            }

    drafts = [d for d in state.outreach_drafts() if (d.get("to_email") or "").strip().lower() == target]
    sent_history, sent_error = state.sent_history(target)

    contact_payload = {
        "id": contact.get("id", ""),
        "first_name": contact.get("first_name", ""),
        "last_name": contact.get("last_name", ""),
        "name": contact.get("name", "") or _coalesce_name(
            contact.get("first_name", ""),
            contact.get("last_name", ""),
            fallback=target,
        ),
        "email": target,
        "company": contact.get("company", ""),
        "title": contact.get("title", ""),
        "industry": contact.get("industry", ""),
        "city": contact.get("city", ""),
        "state": contact.get("state", ""),
        "subject": contact.get("subject", ""),
        "drafted_at": contact.get("drafted_at", ""),
        "rewritten_at": contact.get("rewritten_at", ""),
        "sent_at": contact.get("sent_at", ""),
    }

    return {
        "contact": contact_payload,
        "drafts": drafts,
        "sent": sent_history,
        "sent_access_error": sent_error,
        "contacts_error": contacts_error,
    }


def _dashboard_page() -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>AOM Outreach Dashboard</title>
  <style>
    :root {
      --bg: #090c12;
      --surface: #0f151f;
      --surface-2: #151f2c;
      --ink: #e8edf6;
      --muted: #91a1b7;
      --line: #283447;
      --chip: #1a2433;
      --accent: #86d3ff;
      --warn: #ffd166;
      --ok: #96f2b0;
      --danger: #ff8fa3;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      height: 100vh;
      width: 100vw;
      overflow: hidden;
      background:
        radial-gradient(90rem 40rem at 10% -10%, rgba(74, 126, 206, .22), transparent 50%),
        radial-gradient(80rem 30rem at 90% 110%, rgba(42, 98, 130, .20), transparent 50%),
        var(--bg);
      color: var(--ink);
      font-family: "SF Pro Text", "Helvetica Neue", Helvetica, Arial, sans-serif;
      font-size: 12px;
      line-height: 1.4;
    }
    .shell {
      display: flex;
      flex-direction: column;
      height: 100vh;
      width: 100vw;
      overflow: hidden;
      padding: 12px;
      gap: 12px;
      background: linear-gradient(180deg, rgba(21,30,44,.65), rgba(10,15,22,.85));
    }
    .topbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
    }
    .topbar .title { font-weight: 700; font-size: 14px; letter-spacing: .03em; }
    .btn {
      border: 1px solid #32425a;
      border-radius: 10px;
      background: #121b29;
      color: #dce5f3;
      padding: 8px 11px;
      cursor: pointer;
      font-weight: 600;
      font-size: 11px;
    }
    .btn:disabled { opacity: 0.55; cursor: not-allowed; }
    .warn {
      border: 1px solid rgba(255, 209, 102, .45);
      background: rgba(255, 209, 102, .12);
      color: #ffe7aa;
      border-radius: 9px;
      padding: 8px 10px;
      display: none;
    }
    .errorbox {
      border: 1px solid rgba(255, 143, 163, .45);
      background: rgba(255, 143, 163, .11);
      color: #ffc0ce;
      border-radius: 9px;
      padding: 8px 10px;
      display: none;
    }
    .stats {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
      gap: 10px;
    }
    .stat {
      border: 1px solid var(--line);
      border-radius: 10px;
      background: linear-gradient(180deg, rgba(21,31,46,.90), rgba(13,20,31,.95));
      padding: 10px;
      min-height: 78px;
    }
    .stat .label { color: var(--muted); text-transform: uppercase; font-size: 10px; letter-spacing: .08em; }
    .stat .value { margin-top: 6px; font-size: 20px; font-weight: 700; }

    .content {
      display: grid;
      grid-template-columns: 320px 1fr 1.2fr;
      gap: 12px;
      height: 100%;
      min-height: 0;
    }

    .sidebar {
      background: rgba(5,8,12,.85);
      border: 1px solid #1b2430;
      border-radius: 12px;
      padding: 16px;
      display: flex;
      flex-direction: column;
      gap: 18px;
      min-height: 0;
    }
    .sidebar h3 { margin: 0; font-size: 13px; letter-spacing: .04em; text-transform: uppercase; color: #c3d2e7; }
    .filter-block { display: grid; gap: 6px; }
    .filter-block label { color: var(--muted); font-size: 11px; }
    #search {
      border: 1px solid var(--line);
      border-radius: 10px;
      background: #0c121b;
      color: var(--ink);
      padding: 10px 12px;
      font: inherit;
      outline: none;
      width: 100%;
    }
    #search:focus { border-color: #3e5878; }
    select, .chip-btn {
      border: 1px solid #2f3f56;
      border-radius: 9px;
      background: #141d2b;
      color: #dce5f3;
      padding: 8px 10px;
      font: inherit;
      cursor: pointer;
    }
    .chip-row { display: flex; flex-wrap: wrap; gap: 6px; }
    .chip-btn.active {
      background: #e7eef9;
      border-color: #d4deec;
      color: #101825;
      font-weight: 700;
    }

    .contact-list-pane, .detail-pane {
      border: 1px solid var(--line);
      border-radius: 12px;
      background: rgba(12,18,27,.84);
      display: grid;
      grid-template-rows: auto 1fr;
      min-height: 0;
    }
    .pane-head {
      border-bottom: 1px solid #253146;
      padding: 12px 14px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 8px;
    }
    .pane-head h3 { margin: 0; font-size: 14px; }
    .pane-head .meta { color: var(--muted); }

    #results, #detail-body {
      min-height: 0;
      overflow-y: auto;
      padding: 10px;
      display: grid;
      gap: 8px;
      align-content: start;
    }
    .row {
      border: 1px solid #2a374d;
      border-radius: 9px;
      background: #121a27;
      padding: 8px 10px;
      cursor: pointer;
      display: grid;
      gap: 6px;
    }
    .row.active { border-color: #6dbde8; box-shadow: inset 0 0 0 1px rgba(109, 189, 232, .25); }
    .row .line {
      display: flex;
      gap: 8px;
      justify-content: space-between;
      align-items: center;
    }
    .name { font-size: 12px; font-weight: 600; }
    .meta { color: var(--muted); font-size: 11px; }
    .status-dot {
      width: 10px;
      height: 10px;
      border-radius: 50%;
      display: inline-block;
    }
    .dot-sent { background: #53e68c; }
    .dot-draft { background: #f5c86c; }
    .dot-none { background: #4b5568; }

    .detail-body .msg {
      border: 1px solid #2d3b52;
      border-radius: 8px;
      background: #111928;
      padding: 8px 9px;
      display: grid;
      gap: 4px;
    }
    .msg .ts { color: #9eb1cb; font-size: 10px; }
    .msg .badge { border-radius: 6px; padding: 2px 6px; font-size: 10px; text-transform: uppercase; }
    .badge.sent { background: rgba(83,230,140,.12); border: 1px solid rgba(83,230,140,.45); color: #b7f4ce; }
    .badge.draft { background: rgba(245,200,108,.12); border: 1px solid rgba(245,200,108,.45); color: #ffe7aa; }
    .badge.recv { background: rgba(134,211,255,.12); border: 1px solid rgba(134,211,255,.45); color: #d2ecff; }
    .msg pre { background: #0b111b; border: 1px solid #1f2a3d; border-radius: 8px; padding: 8px; margin: 0; max-height: 220px; overflow: auto; color: #d2deee; white-space: pre-wrap; }

    @media (max-width: 1100px) {
      .content { grid-template-columns: 1fr; grid-template-rows: auto auto auto; height: auto; }
      .shell { height: auto; min-height: 100vh; }
    }
  </style>
</head>
<body>
  <div class=\"shell\">
    <div class=\"topbar\">
      <div class=\"title\">AOM Outreach Dashboard</div>
      <div class=\"actions\" style=\"display:flex; gap:8px;\">
        <button id=\"refresh\" class=\"btn\">Sync Gmail</button>
        <button id=\"clear\" class=\"btn\">Clear</button>
      </div>
    </div>
    <div id=\"sentWarn\" class=\"warn\"></div>
    <div id=\"errorBox\" class=\"errorbox\"></div>
    <section id=\"stats\" class=\"stats\"></section>
    <div class=\"content\">
      <aside class=\"sidebar\">
        <h3>Filters</h3>
        <div id=\"search-container\" class=\"filter-block\">
          <label for=\"search\">Search</label>
          <input id=\"search\" placeholder=\"Search contacts, drafts, sent...\" />
        </div>
        <div id=\"status-filter-container\" class=\"filter-block\">
          <label>Status</label>
          <div id=\"status-chips\" class=\"chip-row\"></div>
        </div>
        <div id=\"industry-filter-container\" class=\"filter-block\">
          <label for=\"industry-filter\">Industry</label>
          <select id=\"industry-filter\"></select>
        </div>
        <div id=\"sort-container\" class=\"filter-block\">
          <label>Sort</label>
          <div id=\"sort-chips\" class=\"chip-row\"></div>
        </div>
      </aside>
      <main class=\"contact-list-pane\">
        <div class=\"pane-head\">
          <h3>Contacts</h3>
          <span id=\"results-count\" class=\"meta\"></span>
        </div>
        <div id=\"results\" class=\"list\"></div>
      </main>
      <aside class=\"detail-pane\">
        <div class=\"pane-head\">
          <div>
            <h3 id=\"detail-name\">Select a contact</h3>
            <div id=\"detail-meta\" class=\"meta\"></div>
          </div>
        </div>
        <div id=\"detail-body\" class=\"detail-body\"></div>
      </aside>
    </div>
  </div>
  <script src=\"/static/dashboard.js\"></script>
</body>
</html>
"""


def _dashboard_script() -> str:
    return """
const state = {
  q: "",
  selectedEmail: "",
  industryFilter: "all",
  statusFilter: "all",
  sortBy: "latest_activity",
};

let allData = {
  summary: {},
  results: [],
  activity: [],
  sent_access_error: "",
  contacts_error: "",
};

const byId = (id) => document.getElementById(id);
const esc = (v) => (v || "").replace(/[&<>"']/g, (ch) => ({
  "&": "&amp;",
  "<": "&lt;",
  ">": "&gt;",
  '"': "&quot;",
  "'": "&#39;",
}[ch]));

function debounce(fn, delayMs) {
  let timer = null;
  return (...args) => {
    if (timer) clearTimeout(timer);
    timer = setTimeout(() => fn(...args), delayMs);
  };
}

function fmtTs(ts) {
  if (!ts) return "";
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return ts;
  return d.toLocaleString();
}

function matchQuery(item, q) {
  if (!q) return true;
  const haystack = [
    item.name,
    item.email,
    item.company,
    item.title,
    item.industry,
    item.subject,
    item.snippet,
    item.to_email,
    item.to_raw,
  ].join(" ").toLowerCase();
  return q.split(/\\s+/).filter(Boolean).every((part) => haystack.includes(part));
}

function getFilteredResults() {
  const items = Array.isArray(allData.results) ? allData.results : [];
  const q = (state.q || "").trim().toLowerCase();
  const industryFilter = (state.industryFilter || "all").toLowerCase();
  const statusFilter = state.statusFilter || "all";
  const sortBy = state.sortBy || "latest_activity";

  let filtered = items;

  if (statusFilter !== "all") {
    filtered = filtered.filter((item) => {
      const hasDraft = !!item.has_draft;
      const hasSent = !!item.has_sent;
      if (statusFilter === "drafted") return hasDraft;
      if (statusFilter === "sent") return hasSent;
      if (statusFilter === "not_contacted") return !hasDraft && !hasSent;
      return true;
    });
  }

  if (industryFilter !== "all") {
    filtered = filtered.filter((item) => (item.industry || "").toLowerCase() === industryFilter);
  }

  if (q) {
    filtered = filtered.filter((item) => matchQuery(item, q));
  }

  const sorter = (a, b) => {
    const tsA = a.latest_ts || "";
    const tsB = b.latest_ts || "";
    if (sortBy === "date_added") {
      return tsA < tsB ? 1 : tsA > tsB ? -1 : 0;
    }
    // latest_activity default
    return tsA < tsB ? 1 : tsA > tsB ? -1 : 0;
  };

  return [...filtered].sort(sorter);
}

function renderStats(summary) {
  const cards = [
    ["Apollo Contacts", summary.apollo_contacts],
    ["Enriched", summary.enriched_contacts],
    ["Drafted", summary.drafted_contacts],
    ["Gmail Drafts", summary.outreach_drafts],
    ["Sent (All-Time)", summary.sent_messages_window],
    ["Unique Sent", summary.sent_unique_recipients],
  ];
  byId("stats").innerHTML = cards.map(([label, value]) => `
    <div class="stat">
      <div class="label">${esc(label)}</div>
      <div class="value">${esc(String(value ?? 0))}</div>
    </div>
  `).join("");
}

function renderStatusChips() {
  const container = byId("status-chips");
  if (!container) return;
  const options = [
    ["all", "All"],
    ["not_contacted", "Not Contacted"],
    ["drafted", "Drafted"],
    ["sent", "Sent"],
  ];
  container.innerHTML = options.map(([key, label]) => {
    const active = state.statusFilter === key ? "active" : "";
    return `<button class="chip-btn ${active}" data-value="${key}">${esc(label)}</button>`;
  }).join("");
  [...container.querySelectorAll("button")].forEach((btn) => {
    btn.addEventListener("click", () => {
      state.statusFilter = btn.dataset.value || "all";
      renderStatusChips();
      renderFilteredData();
    });
  });
}

function renderIndustryOptions() {
  const select = byId("industry-filter");
  if (!select) return;
  const options = new Set();
  (allData.results || []).forEach((item) => {
    const v = (item.industry || "").trim();
    if (v) options.add(v);
  });
  const sorted = [...options].sort((a, b) => a.localeCompare(b));
  const current = select.value || "all";
  select.innerHTML = ['<option value="all">All Industries</option>', ...sorted.map((v) => `<option value="${esc(v)}">${esc(v)}</option>`)].join("");
  if ([...select.options].some((o) => o.value === current)) {
    select.value = current;
  } else {
    select.value = "all";
    state.industryFilter = "all";
  }
}

function renderSortChips() {
  const container = byId("sort-chips");
  if (!container) return;
  const options = [
    ["latest_activity", "Latest Activity"],
    ["date_added", "Date Added"],
  ];
  container.innerHTML = options.map(([key, label]) => {
    const active = state.sortBy === key ? "active" : "";
    return `<button class="chip-btn ${active}" data-value="${key}">${esc(label)}</button>`;
  }).join("");
  [...container.querySelectorAll("button")].forEach((btn) => {
    btn.addEventListener("click", () => {
      state.sortBy = btn.dataset.value || "latest_activity";
      renderSortChips();
      renderFilteredData();
    });
  });
}

function statusDotClass(item) {
  if (item.has_sent) return "dot-sent";
  if (item.has_draft) return "dot-draft";
  return "dot-none";
}

function renderResults(items) {
  const resultsNode = byId("results");
  const countNode = byId("results-count");
  if (countNode) countNode.textContent = `${items.length} result${items.length === 1 ? "" : "s"}`;
  if (!items.length) {
    resultsNode.innerHTML = '<div class="meta">No matches</div>';
    return;
  }
  resultsNode.innerHTML = items.map((item) => {
    const active = state.selectedEmail && item.email === state.selectedEmail ? "active" : "";
    const dot = statusDotClass(item);
    return `
      <div class="row ${active}" data-email="${esc(item.email)}">
        <div class="line">
          <div class="name">${esc(item.name || item.email || "(unknown)")}</div>
          <div class="meta">${esc(fmtTs(item.latest_ts))}</div>
        </div>
        <div class="line">
          <div class="meta">${esc(item.company || item.title || item.email || "")}</div>
          <span class="status-dot ${dot}"></span>
        </div>
        <div class="meta">${esc(item.industry || "")}</div>
      </div>`;
  }).join("");

  [...resultsNode.querySelectorAll(".row[data-email]")].forEach((node) => {
    node.addEventListener("click", () => {
      state.selectedEmail = node.dataset.email || "";
      [...resultsNode.querySelectorAll(".row")].forEach((x) => x.classList.remove("active"));
      node.classList.add("active");
      loadHistory(state.selectedEmail);
    });
  });
}

function showError(message) {
  const box = byId("errorBox");
  if (!message) {
    box.style.display = "none";
    box.textContent = "";
    return;
  }
  box.style.display = "block";
  box.textContent = message;
}

function renderDetail(payload) {
  const detailName = byId("detail-name");
  const detailMeta = byId("detail-meta");
  const bodyNode = byId("detail-body");
  if (!bodyNode) return;

  if (payload.contacts_error) {
    showError(payload.contacts_error);
  }

  const contact = payload.contact || {};
  detailName.textContent = contact.name || contact.email || "Contact";
  detailMeta.textContent = [contact.email, contact.company, contact.title].filter(Boolean).join(" • ");

  const drafts = payload.drafts || [];
  const sent = payload.sent || [];

  const threads = {};
  drafts.forEach((d) => {
    const tid = d.thread_id || `draft-${d.draft_id || d.to_email || "draft"}`;
    threads[tid] = threads[tid] || [];
    threads[tid].push({ kind: "draft", data: d });
  });
  sent.forEach((s) => {
    const tid = s.thread_id || "thread";
    threads[tid] = threads[tid] || [];
    threads[tid].push({ kind: "sent", data: s });
  });

  const threadEntries = Object.entries(threads).map(([tid, msgs]) => {
    const sorted = msgs.sort((a, b) => {
      const ta = a.data.sent_at || a.data.date_header || a.data.drafted_at || "";
      const tb = b.data.sent_at || b.data.date_header || b.data.drafted_at || "";
      return ta < tb ? -1 : ta > tb ? 1 : 0;
    });
    const latest = sorted[sorted.length - 1].data.sent_at || sorted[sorted.length - 1].data.date_header || sorted[sorted.length - 1].data.drafted_at || "";
    return { tid, msgs: sorted, latest };
  }).sort((a, b) => (a.latest < b.latest ? 1 : a.latest > b.latest ? -1 : 0));

  if (!threadEntries.length) {
    bodyNode.innerHTML = '<div class="meta">No drafts or sent history yet.</div>';
    return;
  }

  bodyNode.innerHTML = threadEntries.map(({ tid, msgs }) => {
    const messagesHtml = msgs.map((entry) => {
      const d = entry.data;
      const ts = fmtTs(d.sent_at || d.date_header || d.drafted_at || "");
      const subject = d.subject || "(no subject)";
      const direction = d.direction || (entry.kind === "draft" ? "draft" : "sent");
      const badgeClass = direction === "received" ? "recv" : (entry.kind === "draft" ? "draft" : "sent");
      const body = d.body_text || d.snippet || "";
      return `
        <div class="msg">
          <div class="line">
            <div>${esc(subject)}</div>
            <div class="ts">${esc(ts)}</div>
          </div>
          <div class="line" style="gap:6px;">
            <span class="badge ${badgeClass}">${esc(direction)}</span>
            <span class="meta">${esc(d.from || "")} → ${esc(d.to || d.to_email || "")}</span>
          </div>
          <div class="meta">${esc(d.snippet || "")}</div>
          <details>
            <summary class="meta">Body</summary>
            <pre>${esc(body)}</pre>
          </details>
        </div>`;
    }).join("");
    return `
      <div class="msg" style="border-color:#3b4a62;">
        <div class="line">
          <div class="name">Thread ${esc(tid)}</div>
          <div class="meta">${esc(msgs.length)} item${msgs.length === 1 ? "" : "s"}</div>
        </div>
        <div class="detail-body-group">${messagesHtml}</div>
      </div>`;
  }).join("");
}

function renderFilteredData({ allowHistory = true } = {}) {
  const filtered = getFilteredResults();
  renderResults(filtered);

  if (!filtered.length) {
    state.selectedEmail = "";
    byId("detail-body").innerHTML = '<div class="meta">No results. Adjust your filters.</div>';
    return;
  }

  if ((!state.selectedEmail) || !filtered.some((x) => x.email === state.selectedEmail)) {
    state.selectedEmail = filtered[0].email || "";
  }

  if (state.selectedEmail && allowHistory) {
    loadHistory(state.selectedEmail);
  }
}

async function fetchPayload(fast) {
  const qs = new URLSearchParams({ fast: fast ? "1" : "0" });
  const res = await fetch(`/api/data?${qs.toString()}`);
  if (!res.ok) throw new Error(`Dashboard API error (${res.status})`);
  return res.json();
}

function renderPayload(payload, { allowHistory = true } = {}) {
  allData = {
    summary: payload.summary || {},
    results: payload.results || [],
    activity: payload.activity || [],
    sent_access_error: payload.sent_access_error || "",
    contacts_error: payload.contacts_error || "",
  };

  renderStats(allData.summary || {});
  renderIndustryOptions();
  renderStatusChips();
  renderSortChips();

  const warn = byId("sentWarn");
  const warnParts = [];
  if (allData.contacts_error) warnParts.push(allData.contacts_error);
  if (allData.sent_access_error) warnParts.push(allData.sent_access_error);
  if (warnParts.length) {
    warn.style.display = "block";
    warn.textContent = warnParts.join(" | ");
  } else {
    warn.style.display = "none";
    warn.textContent = "";
  }

  renderFilteredData({ allowHistory });
}

async function loadData({ withGmail = false } = {}) {
  const reqId = Date.now() + Math.random();
  state._lastReqId = reqId;
  showError("");
  byId("results").innerHTML = '<div class="meta">Loading contacts...</div>';
  try {
    const fastPayload = await fetchPayload(true);
    if (state._lastReqId !== reqId) return;
    renderPayload(fastPayload, { allowHistory: false });
  } catch (err) {
    const msg = (err && err.message) ? err.message : "Failed to load dashboard data.";
    showError(msg);
    return;
  }

  if (!withGmail) return;

  try {
    const fullPayload = await fetchPayload(false);
    if (state._lastReqId !== reqId) return;
    renderPayload(fullPayload, { allowHistory: true });
  } catch (err) {
    const msg = (err && err.message) ? err.message : "Failed to load Gmail-backed activity.";
    showError(msg);
    if (state.selectedEmail) loadHistory(state.selectedEmail);
  }
}

async function loadHistory(email) {
  if (!email) return;
  try {
    const qs = new URLSearchParams({ email });
    const res = await fetch(`/api/history?${qs.toString()}`);
    if (!res.ok) throw new Error(`History API error (${res.status})`);
    const payload = await res.json();
    renderDetail(payload);
  } catch (err) {
    const msg = (err && err.message) ? err.message : "Failed to load contact history.";
    byId("detail-body").innerHTML = `<div class="meta">${esc(msg)}</div>`;
  }
}

const onSearch = debounce((value) => {
  state.q = (value || "").trim();
  renderFilteredData();
}, 300);

byId("search").addEventListener("input", (e) => onSearch(e.target.value || ""));

byId("refresh").addEventListener("click", async (e) => {
  const button = e.currentTarget;
  const original = button.textContent || "Sync Gmail";
  button.disabled = true;
  button.textContent = "Syncing...";
  try {
    await fetch("/api/refresh", { method: "POST" });
    await loadData({ withGmail: true });
  } finally {
    button.disabled = false;
    button.textContent = original || "Sync Gmail";
  }
});

byId("clear").addEventListener("click", () => {
  state.q = "";
  state.selectedEmail = "";
  state.industryFilter = "all";
  state.statusFilter = "all";
  state.sortBy = "latest_activity";
  byId("search").value = "";
  const ind = byId("industry-filter");
  if (ind) ind.value = "all";
  renderStatusChips();
  renderSortChips();
  renderFilteredData();
});

byId("industry-filter").addEventListener("change", (e) => {
  state.industryFilter = (e.target.value || "all");
  renderFilteredData();
});

renderStatusChips();
renderSortChips();
loadData({ withGmail: false });
"""


def launch_outreach_dashboard(db) -> None:
    """
    Open a local outreach dashboard with:
    - Search across Apollo contacts, drafts, and sent history
    - Contact detail timeline (drafts + sent messages)
    - Combined activity feed
    """
    state = _DashboardState(db)

    class Handler(BaseHTTPRequestHandler):
        def _json(self, payload: dict, status: int = 200):
            raw = json.dumps(payload, ensure_ascii=True).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def _html(self, page: str):
            raw = page.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def do_GET(self):
            parsed = urlparse(self.path)
            try:
                if parsed.path == "/":
                    self._html(_dashboard_page())
                    return
                if parsed.path == "/static/dashboard.js":
                    raw = _dashboard_script().encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/javascript; charset=utf-8")
                    self.send_header("Content-Length", str(len(raw)))
                    self.end_headers()
                    self.wfile.write(raw)
                    return
                if parsed.path == "/api/data":
                    qs = parse_qs(parsed.query)
                    fast = (qs.get("fast", ["0"])[0] or "0").strip() in {"1", "true", "yes"}
                    payload = _build_dashboard_payload(state, fast=fast)
                    self._json(payload)
                    return
                if parsed.path == "/api/history":
                    qs = parse_qs(parsed.query)
                    email = (qs.get("email", [""])[0] or "").strip().lower()
                    if not email:
                        self._json({"error": "email is required"}, status=400)
                        return
                    payload = _build_history_payload(state, email=email)
                    self._json(payload)
                    return

                self.send_response(404)
                self.end_headers()
            except Exception as e:  # pylint: disable=broad-except
                self._json({"error": str(e)}, status=500)

        def do_POST(self):
            parsed = urlparse(self.path)
            if parsed.path == "/api/refresh":
                state.clear_cache()
                self._json({"ok": True})
                return

            self.send_response(404)
            self.end_headers()

        def log_message(self, format, *args):  # pylint: disable=redefined-builtin
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    host, port = server.server_address
    url = f"http://{host}:{port}/"

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(f"\nOutreach Dashboard: {url}")
    webbrowser.open(url, new=2)

    try:
        while thread.is_alive():
            time.sleep(0.25)
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        server.server_close()
