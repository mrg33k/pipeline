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
                known_emails, _ = self.apollo_email_set()
                if not known_emails:
                    return []
                drafts = gmail_drafter.get_outreach_drafts(max_results=200, known_emails=known_emails)
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
                if not known_emails:
                    return []
                return gmail_drafter.get_recent_sent_activity(
                    hours=24 * 45,
                    max_results=120,
                    allowed_recipients=known_emails,
                )
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

        known_emails, contacts_error = self.apollo_email_set()
        if contacts_error:
            return [], contacts_error
        if known_emails and target not in known_emails:
            return [], "Selected email is not currently in Apollo-owned contacts."

        def _load():
            try:
                return gmail_drafter.get_thread_history_for_recipient(
                    target,
                    days=365,
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


def _build_dashboard_payload(state: _DashboardState, q: str, view: str, fast: bool = False) -> dict:
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
    q = (q or "").strip()
    view = (view or "all").strip().lower()
    if view not in {"all", "apollo", "drafts", "sent"}:
        view = "all"

    if view in {"all", "apollo"}:
        for contact in contacts:
            email = contact.get("email", "")
            item = {
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
            }
            if _match_query(item, q):
                results.append(item)

    if view in {"all", "drafts"}:
        for draft in drafts:
            email = draft.get("to_email", "")
            if view == "all" and email in contact_emails:
                continue
            contact = contacts_by_email.get(email, {})
            item = {
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
            }
            if _match_query(item, q):
                results.append(item)

    if view in {"all", "sent"}:
        for email, sent in sent_by_email.items():
            if view == "all" and email in contact_emails:
                continue
            contact = contacts_by_email.get(email, {})
            item = {
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
            }
            if _match_query(item, q):
                results.append(item)

    results.sort(
        key=lambda item: (
            0 if item.get("has_sent") else 1,
            _parse_iso(item.get("latest_ts", "")),
        ),
        reverse=True,
    )

    runs = ((state.db.data or {}).get("runs", []) or [])
    activity = _build_activity_feed(runs, contacts, sent_activity)
    if q:
        activity = [item for item in activity if _match_query(item, q)]

    summary = {
        "apollo_contacts": len(contacts),
        "enriched_contacts": sum(1 for c in contacts if c.get("enriched")),
        "drafted_contacts": sum(1 for c in contacts if c.get("drafted")),
        "outreach_drafts": len(drafts),
        "sent_messages_window": len(sent_activity),
        "sent_unique_recipients": len(sent_by_email),
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
      min-height: 100vh;
      background:
        radial-gradient(90rem 40rem at 10% -10%, rgba(74, 126, 206, .22), transparent 50%),
        radial-gradient(80rem 30rem at 90% 110%, rgba(42, 98, 130, .20), transparent 50%),
        var(--bg);
      color: var(--ink);
      font-family: "SF Pro Text", "Helvetica Neue", Helvetica, Arial, sans-serif;
      font-size: 12px;
      line-height: 1.4;
      display: grid;
      place-items: center;
      padding: 16px;
    }
    .shell {
      width: min(1440px, 98vw);
      height: min(920px, 95vh);
      border: 1px solid var(--line);
      border-radius: 14px;
      background: linear-gradient(180deg, rgba(21,30,44,.65), rgba(10,15,22,.85));
      box-shadow: 0 18px 65px rgba(0,0,0,.45);
      display: grid;
      grid-template-columns: 82px 1fr;
      overflow: hidden;
    }
    .rail {
      background: rgba(5,8,12,.85);
      border-right: 1px solid #1b2430;
      display: flex;
      flex-direction: column;
      align-items: center;
      gap: 10px;
      padding: 12px 8px;
    }
    .rail .logo {
      width: 48px;
      height: 48px;
      border-radius: 12px;
      display: grid;
      place-items: center;
      background: #f6f8fc;
      color: #111721;
      font-weight: 700;
      font-size: 15px;
      margin-bottom: 10px;
    }
    .rail button {
      width: 46px;
      height: 46px;
      border: 1px solid #2a3447;
      border-radius: 12px;
      background: #0f1623;
      color: #b5c4d9;
      cursor: pointer;
      font-size: 11px;
    }
    .rail button.active {
      background: #e9edf7;
      color: #101724;
      border-color: #d4dceb;
      font-weight: 700;
    }
    .main {
      padding: 14px 16px 16px;
      display: grid;
      grid-template-rows: auto auto 1fr;
      gap: 12px;
    }
    .topbar {
      display: grid;
      grid-template-columns: 1fr auto auto;
      gap: 10px;
      align-items: center;
    }
    .search {
      border: 1px solid var(--line);
      border-radius: 10px;
      background: #0c121b;
      color: var(--ink);
      padding: 10px 12px;
      font: inherit;
      outline: none;
    }
    .search:focus { border-color: #3e5878; }
    .btn {
      border: 1px solid #32425a;
      border-radius: 10px;
      background: #121b29;
      color: #dce5f3;
      padding: 9px 11px;
      cursor: pointer;
      font-weight: 600;
      font-size: 11px;
    }
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
      margin-top: 6px;
    }
    .stats {
      display: grid;
      grid-template-columns: repeat(6, minmax(120px, 1fr));
      gap: 10px;
    }
    .stat {
      border: 1px solid var(--line);
      border-radius: 10px;
      background: linear-gradient(180deg, rgba(21,31,46,.90), rgba(13,20,31,.95));
      padding: 10px;
    }
    .stat .label { color: var(--muted); text-transform: uppercase; font-size: 10px; letter-spacing: .08em; }
    .stat .value { margin-top: 6px; font-size: 20px; font-weight: 700; }
    .panes {
      min-height: 0;
      display: grid;
      grid-template-columns: 1.05fr 1fr;
      gap: 12px;
    }
    .card {
      border: 1px solid var(--line);
      border-radius: 10px;
      background: rgba(12,18,27,.84);
      min-height: 0;
      display: grid;
      grid-template-rows: auto 1fr;
    }
    .card-head {
      border-bottom: 1px solid #253146;
      padding: 10px 12px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 8px;
    }
    .tabs { display: inline-flex; gap: 6px; }
    .tab {
      border: 1px solid #2f3f56;
      border-radius: 8px;
      padding: 6px 8px;
      color: #b9c6da;
      background: #141d2b;
      cursor: pointer;
      font-size: 11px;
    }
    .tab.active {
      background: #e7eef9;
      border-color: #d4deec;
      color: #101825;
      font-weight: 700;
    }
    .list {
      min-height: 0;
      overflow: auto;
      padding: 8px;
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
    .chips { display: flex; gap: 6px; flex-wrap: wrap; }
    .chip {
      border: 1px solid #34455f;
      border-radius: 99px;
      background: var(--chip);
      color: #c5d2e3;
      padding: 2px 7px;
      font-size: 10px;
      letter-spacing: .01em;
    }
    .chip.ok { border-color: rgba(150, 242, 176, .5); color: #bff9cf; }
    .chip.warn { border-color: rgba(255, 209, 102, .5); color: #ffe7aa; }
    .detail {
      min-height: 0;
      overflow: auto;
      padding: 12px;
      display: grid;
      gap: 12px;
      align-content: start;
    }
    .detail h3 { margin: 0; font-size: 14px; }
    .detail .subtitle { color: var(--muted); margin-top: 2px; }
    .timeline {
      display: grid;
      gap: 8px;
    }
    .event {
      border: 1px solid #2d3b52;
      border-radius: 8px;
      background: #111928;
      padding: 8px 9px;
      display: grid;
      gap: 4px;
    }
    .event .ts { color: #9eb1cb; font-size: 10px; }
    .event .sub { color: #9cb0cb; font-size: 11px; }
    .pre {
      white-space: pre-wrap;
      color: #d2deee;
      font-size: 11px;
      border-top: 1px dashed #2f3d53;
      padding-top: 6px;
      margin-top: 2px;
    }
    .activity .row { cursor: default; }
    @media (max-width: 1200px) {
      .stats { grid-template-columns: repeat(3, minmax(140px, 1fr)); }
      .panes { grid-template-columns: 1fr; }
      .shell { height: auto; min-height: 95vh; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <aside class="rail">
      <div class="logo">AOM</div>
      <button class="active" title="Overview">OV</button>
      <button title="Apollo">AP</button>
      <button title="Drafts">DR</button>
      <button title="Sent">SE</button>
    </aside>
    <main class="main">
      <div class="topbar">
        <input id="search" class="search" placeholder="Search Apollo contacts, drafts, sent history..." />
        <button id="refresh" class="btn">Sync Gmail</button>
        <button id="clear" class="btn">Clear</button>
      </div>
      <div id="sentWarn" class="warn"></div>
      <div id="errorBox" class="errorbox"></div>
      <section id="stats" class="stats"></section>
      <section class="panes">
        <article class="card">
          <div class="card-head">
            <strong>Search Results</strong>
            <div class="tabs">
              <button class="tab active" data-view="all">All</button>
              <button class="tab" data-view="apollo">Apollo</button>
              <button class="tab" data-view="drafts">Drafts</button>
              <button class="tab" data-view="sent">Sent</button>
            </div>
          </div>
          <div id="results" class="list"></div>
        </article>
        <article class="card">
          <div class="card-head">
            <strong>Contact Activity</strong>
            <span id="detailMeta" class="meta"></span>
          </div>
          <div id="detail" class="detail"></div>
        </article>
      </section>
      <section class="card activity">
        <div class="card-head"><strong>Activity Feed</strong><span class="meta">Runs, drafts, and sent events</span></div>
        <div id="activity" class="list"></div>
      </section>
    </main>
  </div>
  <script src="/static/dashboard.js"></script>
</body>
</html>
"""


def _dashboard_script() -> str:
    return """
const state = { q: "", view: "all", selectedEmail: "" };

const byId = id => document.getElementById(id);
const esc = (v) => (v || "").replace(/[&<>"']/g, ch => ({
  "&": "&amp;",
  "<": "&lt;",
  ">": "&gt;",
  '"': "&quot;",
  "'": "&#39;"
}[ch]));

function fmtTs(ts) {
  if (!ts) return "";
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return ts;
  return d.toLocaleString();
}

function rowChips(item) {
  const chips = [];
  chips.push(`<span class="chip">${esc(item.type || "contact")}</span>`);
  if (item.has_draft) chips.push('<span class="chip warn">draft</span>');
  if (item.has_sent) chips.push('<span class="chip ok">sent</span>');
  return chips.join("");
}

function renderStats(summary) {
  const cards = [
    ["Apollo Contacts", summary.apollo_contacts],
    ["Enriched", summary.enriched_contacts],
    ["Drafted", summary.drafted_contacts],
    ["Gmail Drafts", summary.outreach_drafts],
    ["Sent (45d)", summary.sent_messages_window],
    ["Unique Sent", summary.sent_unique_recipients],
  ];
  byId("stats").innerHTML = cards.map(([label, value]) => `
    <div class="stat">
      <div class="label">${esc(label)}</div>
      <div class="value">${esc(String(value))}</div>
    </div>
  `).join("");
}

function renderResults(items) {
  const html = items.map(item => {
    const active = state.selectedEmail && item.email === state.selectedEmail ? "active" : "";
    return `
      <div class="row ${active}" data-email="${esc(item.email)}">
        <div class="line">
          <div class="name">${esc(item.name || item.email || "(unknown)")}</div>
          <div class="meta">${esc(fmtTs(item.latest_ts))}</div>
        </div>
        <div class="meta">${esc(item.company || item.email || "")}</div>
        <div class="line">
          <div class="meta">${esc(item.subject || item.title || "")}</div>
          <div class="chips">${rowChips(item)}</div>
        </div>
      </div>`;
  }).join("");
  byId("results").innerHTML = html || '<div class="meta">No matches</div>';
  [...byId("results").querySelectorAll(".row[data-email]")].forEach(node => {
    node.addEventListener("click", () => {
      state.selectedEmail = node.dataset.email || "";
      [...byId("results").querySelectorAll(".row")].forEach(x => x.classList.remove("active"));
      node.classList.add("active");
      loadHistory(state.selectedEmail);
    });
  });
}

function renderActivity(items) {
  const html = items.map(item => `
    <div class="row">
      <div class="line">
        <div class="name">${esc(item.title || "")}</div>
        <div class="meta">${esc(fmtTs(item.ts))}</div>
      </div>
      <div class="meta">${esc(item.subtitle || "")}</div>
    </div>
  `).join("");
  byId("activity").innerHTML = html || '<div class="meta">No activity yet</div>';
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

async function fetchPayload(fast) {
  const qs = new URLSearchParams({ q: state.q, view: state.view, fast: fast ? "1" : "0" });
  const res = await fetch(`/api/data?${qs.toString()}`);
  if (!res.ok) throw new Error(`Dashboard API error (${res.status})`);
  return res.json();
}

function renderPayload(payload, { allowHistory = true } = {}) {
  renderStats(payload.summary || {});
  renderResults(payload.results || []);
  renderActivity(payload.activity || []);

  const warn = byId("sentWarn");
  const warnParts = [];
  if (payload.contacts_error) warnParts.push(payload.contacts_error);
  if (payload.sent_access_error) warnParts.push(payload.sent_access_error);
  if (warnParts.length) {
    warn.style.display = "block";
    warn.textContent = warnParts.join(" | ");
  } else {
    warn.style.display = "none";
    warn.textContent = "";
  }

  if ((!state.selectedEmail || !(payload.results || []).some(x => x.email === state.selectedEmail)) && (payload.results || []).length) {
    state.selectedEmail = payload.results[0].email || "";
  }
  if (state.selectedEmail && allowHistory) {
    loadHistory(state.selectedEmail);
  } else if (!state.selectedEmail) {
    byId("detail").innerHTML = '<div class="meta">Select a contact to inspect drafts and sent history.</div>';
    byId("detailMeta").textContent = "";
  }
}

async function loadData({ withGmail = false } = {}) {
  const reqId = Date.now() + Math.random();
  state._lastReqId = reqId;
  showError("");
  byId("results").innerHTML = '<div class="meta">Loading contacts...</div>';
  byId("activity").innerHTML = '<div class="meta">Loading activity...</div>';

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
    if (state.selectedEmail) {
      loadHistory(state.selectedEmail);
    }
  }
}

async function loadHistory(email) {
  if (!email) return;
  try {
    const qs = new URLSearchParams({ email });
    const res = await fetch(`/api/history?${qs.toString()}`);
    if (!res.ok) throw new Error(`History API error (${res.status})`);
    const payload = await res.json();
    const contact = payload.contact || {};
    const drafts = payload.drafts || [];
    const sent = payload.sent || [];

    byId("detailMeta").textContent = contact.email || email;
    let html = `
      <div>
        <h3>${esc(contact.name || email)}</h3>
        <div class="subtitle">${esc([contact.company, contact.title].filter(Boolean).join(" • "))}</div>
        <div class="meta">${esc([contact.city, contact.state, contact.industry].filter(Boolean).join(" • "))}</div>
      </div>
    `;

    if (payload.contacts_error) {
      html += `<div class="warn" style="display:block">${esc(payload.contacts_error)}</div>`;
    }
    if (payload.sent_access_error) {
      html += `<div class="warn" style="display:block">${esc(payload.sent_access_error)}</div>`;
    }

    html += '<div><strong>Drafts</strong><div class="timeline">';
    html += drafts.map(d => `
      <div class="event">
        <div class="line"><div>${esc(d.subject || "(no subject)")}</div><div class="ts">${esc(d.draft_id || "")}</div></div>
        <div class="sub">${esc(d.to_email || "")}</div>
        <div class="pre">${esc((d.body_text || "").slice(0, 900))}</div>
      </div>
    `).join("") || '<div class="meta">No drafts found for this contact.</div>';
    html += '</div></div>';

    html += '<div><strong>Thread (You + Contact)</strong><div class="timeline">';
    html += sent.map(s => `
      <div class="event">
        <div class="line"><div>${esc(s.subject || "(no subject)")}</div><div class="ts">${esc(fmtTs(s.sent_at || s.date_header || ""))}</div></div>
        <div class="line"><div class="chip ${s.direction === "received" ? "ok" : "warn"}">${esc(s.direction || "message")}</div><div class="meta">${esc(s.thread_id || "")}</div></div>
        <div class="sub">From: ${esc(s.from || "")}</div>
        <div class="sub">To: ${esc(s.to || "")}</div>
        <div class="sub">${esc(s.snippet || "")}</div>
        <div class="pre">${esc((s.body_text || "").slice(0, 1200))}</div>
      </div>
    `).join("") || '<div class="meta">No thread messages found for this contact in the current lookup window.</div>';
    html += '</div></div>';

    byId("detail").innerHTML = html;
  } catch (err) {
    const msg = (err && err.message) ? err.message : "Failed to load contact history.";
    byId("detail").innerHTML = `<div class="meta">${esc(msg)}</div>`;
  }
}

byId("search").addEventListener("input", (e) => {
  state.q = (e.target.value || "").trim();
  loadData({ withGmail: false });
});
byId("refresh").addEventListener("click", async () => {
  await fetch("/api/refresh", { method: "POST" });
  loadData({ withGmail: true });
});
byId("clear").addEventListener("click", () => {
  state.q = "";
  state.selectedEmail = "";
  byId("search").value = "";
  loadData({ withGmail: false });
});
[...document.querySelectorAll(".tab")].forEach(node => {
  node.addEventListener("click", () => {
    [...document.querySelectorAll(".tab")].forEach(x => x.classList.remove("active"));
    node.classList.add("active");
    state.view = node.dataset.view || "all";
    state.selectedEmail = "";
    loadData({ withGmail: false });
  });
});
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
                    q = (qs.get("q", [""])[0] or "").strip()
                    view = (qs.get("view", ["all"])[0] or "all").strip().lower()
                    fast = (qs.get("fast", ["0"])[0] or "0").strip() in {"1", "true", "yes"}
                    payload = _build_dashboard_payload(state, q=q, view=view, fast=fast)
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
