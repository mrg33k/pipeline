from __future__ import annotations

import html
import json
import threading
import webbrowser
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional
from urllib.parse import parse_qs


@dataclass
class PreflightSettings:
    mode: str
    max_emails: int
    dry_run: bool
    daily_focus: str
    daily_location: str
    recent_hours: int
    subject_template: str
    subject_company_mode: str
    email_system_prompt: str
    idea_teases_json: str


class _State:
    def __init__(self, defaults: PreflightSettings):
        self.defaults = defaults
        self.result: Optional[PreflightSettings] = None
        self.canceled = False
        self.done = threading.Event()


def _to_bool(value: str) -> bool:
    return value in {"1", "true", "on", "yes"}


def _validate_idea_teases_json(raw: str) -> None:
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"idea_teases_json must be valid JSON: {exc.msg}") from exc

    if not isinstance(parsed, dict):
        raise ValueError("idea_teases_json must be a JSON object")
    if "default" not in parsed:
        raise ValueError('idea_teases_json must include a "default" key')

    for key, value in parsed.items():
        if not isinstance(key, str) or not key.strip():
            raise ValueError("idea_teases_json keys must be non-empty strings")
        if not isinstance(value, list) or not value:
            raise ValueError(f'idea_teases_json["{key}"] must be a non-empty list')
        if not all(isinstance(item, str) and item.strip() for item in value):
            raise ValueError(f'idea_teases_json["{key}"] items must be non-empty strings')


def _parse_form(defaults: PreflightSettings, body: bytes) -> PreflightSettings:
    values = parse_qs(body.decode("utf-8"), keep_blank_values=True)

    mode = (values.get("mode", [defaults.mode])[0] or "").strip().lower()
    if mode not in {"full", "rewrite", "draft"}:
        raise ValueError("mode must be one of: full, rewrite, draft")

    max_emails_raw = (values.get("max_emails", [str(defaults.max_emails)])[0] or "").strip()
    recent_hours_raw = (values.get("recent_hours", [str(defaults.recent_hours)])[0] or "").strip()
    if not max_emails_raw.isdigit():
        raise ValueError("max_emails must be a number")
    if not recent_hours_raw.isdigit():
        raise ValueError("recent_hours must be a number")

    max_emails = int(max_emails_raw)
    recent_hours = int(recent_hours_raw)
    if not (1 <= max_emails <= 200):
        raise ValueError("max_emails must be between 1 and 200")
    if not (1 <= recent_hours <= 168):
        raise ValueError("recent_hours must be between 1 and 168")

    prompt = values.get("email_system_prompt", [defaults.email_system_prompt])[0].strip()
    if not prompt:
        raise ValueError("email_system_prompt cannot be empty")

    subject_template = values.get("subject_template", [defaults.subject_template])[0].strip()
    subject_company_mode = (values.get("subject_company_mode", [defaults.subject_company_mode])[0] or "").strip().lower()
    if subject_company_mode not in {"full", "first_token"}:
        raise ValueError("subject_company_mode must be one of: full, first_token")
    idea_teases_json = values.get("idea_teases_json", [defaults.idea_teases_json])[0].strip()
    if not idea_teases_json:
        raise ValueError("idea_teases_json cannot be empty")
    _validate_idea_teases_json(idea_teases_json)

    return PreflightSettings(
        mode=mode,
        max_emails=max_emails,
        dry_run=_to_bool(values.get("dry_run", ["0"])[0]),
        daily_focus=(values.get("daily_focus", [defaults.daily_focus])[0] or "").strip(),
        daily_location=(values.get("daily_location", [defaults.daily_location])[0] or "").strip(),
        recent_hours=recent_hours,
        subject_template=subject_template,
        subject_company_mode=subject_company_mode,
        email_system_prompt=prompt,
        idea_teases_json=idea_teases_json,
    )


def _render_page(defaults: PreflightSettings, error: str = "") -> str:
    mode_full = "selected" if defaults.mode == "full" else ""
    mode_rewrite = "selected" if defaults.mode == "rewrite" else ""
    mode_draft = "selected" if defaults.mode == "draft" else ""
    subject_company_full = "selected" if defaults.subject_company_mode == "full" else ""
    subject_company_first_token = "selected" if defaults.subject_company_mode == "first_token" else ""
    checked_dry = "checked" if defaults.dry_run else ""
    error_block = f"<div class='error'>{html.escape(error)}</div>" if error else ""

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>AOM Run Control</title>
  <style>
    :root {{
      --bg: #090b0f;
      --panel: #0f131a;
      --panel-2: #121823;
      --ink: #d9e1ec;
      --muted: #8fa0b7;
      --line: #232d3c;
      --accent: #7fc3ff;
      --danger: #ff7a7a;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: radial-gradient(circle at 15% -10%, #152233 0%, var(--bg) 40%);
      color: var(--ink);
      font-family: "SF Pro Text", "Helvetica Neue", Helvetica, Arial, sans-serif;
      font-size: 12px;
      line-height: 1.45;
      min-height: 100vh;
      display: grid;
      place-items: center;
      padding: 22px;
    }}
    .frame {{
      width: min(980px, 96vw);
      background: linear-gradient(180deg, var(--panel-2), var(--panel));
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 18px;
      box-shadow: 0 20px 45px rgba(0,0,0,0.42);
    }}
    h1 {{
      margin: 0 0 4px;
      font-size: 16px;
      letter-spacing: 0.01em;
      font-weight: 600;
    }}
    .sub {{
      margin: 0 0 16px;
      color: var(--muted);
    }}
    .grid {{
      display: grid;
      grid-template-columns: 320px 1fr;
      gap: 12px;
    }}
    .pane {{
      border: 1px solid var(--line);
      border-radius: 8px;
      background: rgba(10,14,20,0.56);
      padding: 12px;
    }}
    .pane-title {{
      margin: 0 0 10px;
      color: #bcd0ea;
      font-weight: 600;
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}
    label {{
      display: block;
      margin-top: 10px;
      margin-bottom: 4px;
      color: #c7d4e6;
      font-weight: 500;
    }}
    input, select, textarea {{
      width: 100%;
      border: 1px solid #2a3648;
      border-radius: 6px;
      background: #0b1119;
      color: var(--ink);
      padding: 8px 10px;
      font: inherit;
      outline: none;
    }}
    textarea {{
      min-height: 470px;
      resize: vertical;
      line-height: 1.35;
      font-family: "SF Mono", Menlo, Consolas, monospace;
      font-size: 11px;
    }}
    input:focus, select:focus, textarea:focus {{
      border-color: #3a5f86;
      box-shadow: 0 0 0 1px rgba(127,195,255,0.18);
    }}
    .hint {{
      color: var(--muted);
      margin-top: 4px;
      font-size: 11px;
    }}
    .check {{
      display: flex;
      align-items: center;
      gap: 8px;
      margin-top: 12px;
    }}
    .check input {{
      width: auto;
      margin: 0;
    }}
    .actions {{
      margin-top: 14px;
      display: flex;
      gap: 8px;
    }}
    button {{
      border: 1px solid #304154;
      border-radius: 6px;
      background: #0c1420;
      color: #d5e0ef;
      padding: 8px 12px;
      font-weight: 600;
      cursor: pointer;
    }}
    button.primary {{
      background: linear-gradient(180deg, #89caff, #5ba8ea);
      color: #09111b;
      border-color: #4f9ce2;
    }}
    .error {{
      margin-bottom: 10px;
      border: 1px solid rgba(255,122,122,0.45);
      background: rgba(255,122,122,0.08);
      color: var(--danger);
      border-radius: 6px;
      padding: 8px 10px;
    }}
    details.advanced {{
      margin-top: 10px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: rgba(8,12,18,0.45);
      padding: 8px 10px;
    }}
    details.advanced > summary {{
      cursor: pointer;
      color: #c7d4e6;
      font-weight: 600;
      list-style: none;
    }}
    details.advanced > summary::-webkit-details-marker {{
      display: none;
    }}
    textarea.prompt {{
      min-height: 280px;
    }}
    textarea.teases {{
      min-height: 220px;
    }}
    @media (max-width: 900px) {{
      .grid {{ grid-template-columns: 1fr; }}
      textarea {{ min-height: 240px; }}
    }}
  </style>
</head>
<body>
  <main class="frame">
    <h1>AOM Preflight</h1>
    <p class="sub">Set run variables and prompt once, then launch.</p>
    {error_block}
    <form method="post" action="/start">
      <section class="grid">
        <div class="pane">
          <div class="pane-title">Run Variables</div>

          <label for="mode">Mode</label>
          <select id="mode" name="mode">
            <option value="full" {mode_full}>full</option>
            <option value="rewrite" {mode_rewrite}>rewrite</option>
            <option value="draft" {mode_draft}>draft</option>
          </select>

          <label for="max_emails">Max emails</label>
          <input id="max_emails" name="max_emails" type="number" min="1" max="200" value="{defaults.max_emails}" required>

          <label for="recent_hours">Recent-contact block (hours)</label>
          <input id="recent_hours" name="recent_hours" type="number" min="1" max="168" value="{defaults.recent_hours}" required>
          <div class="hint">Skip recent contacts before enrichment/draft.</div>

          <label for="daily_focus">Focus</label>
          <input id="daily_focus" name="daily_focus" type="text" value="{html.escape(defaults.daily_focus)}" placeholder="e.g. concrete companies">

          <label for="daily_location">Location</label>
          <input id="daily_location" name="daily_location" type="text" value="{html.escape(defaults.daily_location)}" placeholder="e.g. AZ | Phoenix, AZ | Dallas, TX">
          <div class="hint">Use ';' for multiple locations.</div>

          <label for="subject_template">Subject template</label>
          <input id="subject_template" name="subject_template" type="text" value="{html.escape(defaults.subject_template)}" placeholder="e.g. quick question for {{company_short}}">
          <div class="hint">Placeholders: {'{first_name}'}, {'{company_name}'}, {'{company_short}'}.</div>

          <label for="subject_company_mode">Business name in subject</label>
          <select id="subject_company_mode" name="subject_company_mode">
            <option value="full" {subject_company_full}>full business name</option>
            <option value="first_token" {subject_company_first_token}>first token only</option>
          </select>
          <div class="hint">Applies to {'{company_name}'}/{'{company_short}'} and fallback subject logic.</div>

          <label class="check"><input type="checkbox" name="dry_run" value="1" {checked_dry}> Dry run (no Gmail draft updates)</label>

          <div class="actions">
            <button class="primary" type="submit">Start</button>
            <button type="button" onclick="cancelRun()">Cancel</button>
          </div>
        </div>

        <div class="pane">
          <div class="pane-title">Templates</div>
          <div class="hint">Adjust template structures for this run without editing code.</div>

          <details class="advanced" open>
            <summary>Advanced settings</summary>
            <label for="email_system_prompt">Email system prompt template</label>
            <textarea class="prompt" id="email_system_prompt" name="email_system_prompt" required>{html.escape(defaults.email_system_prompt)}</textarea>
            <div class="hint">Full body-template rules for generated emails.</div>

            <label for="idea_teases_json">Idea tease templates (JSON)</label>
            <textarea class="teases" id="idea_teases_json" name="idea_teases_json" required>{html.escape(defaults.idea_teases_json)}</textarea>
            <div class="hint">JSON object: industry bucket -> list of tease sentence templates.</div>
          </details>
        </div>
      </section>
    </form>
  </main>
  <script>
    function cancelRun() {{
      fetch('/cancel', {{ method: 'POST' }}).then(() => {{
        document.body.innerHTML = '<main class="frame"><h1>Canceled</h1><p class="sub">No changes were run.</p></main>';
      }});
    }}
  </script>
</body>
</html>"""


def collect_preflight_settings(defaults: PreflightSettings) -> Optional[PreflightSettings]:
    """
    Open a local browser UI and return settings, or None if canceled.
    """
    state = _State(defaults=defaults)

    class Handler(BaseHTTPRequestHandler):
        def _send(self, status: int, page: str):
            payload = page.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self):
            if self.path != "/":
                self.send_response(404)
                self.end_headers()
                return
            self._send(200, _render_page(state.defaults))

        def do_POST(self):
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length)

            if self.path == "/start":
                try:
                    parsed = _parse_form(state.defaults, body)
                    state.result = parsed
                    state.done.set()
                    self._send(200, "<html><body><h2>Starting...</h2><p>You can close this tab.</p></body></html>")
                    threading.Thread(target=self.server.shutdown, daemon=True).start()
                    return
                except Exception as exc:  # pylint: disable=broad-except
                    self._send(400, _render_page(state.defaults, error=str(exc)))
                    return

            if self.path == "/cancel":
                state.canceled = True
                state.done.set()
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Content-Length", "2")
                self.end_headers()
                self.wfile.write(b"ok")
                threading.Thread(target=self.server.shutdown, daemon=True).start()
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
    print(f"\nPreflight UI: {url}")
    webbrowser.open(url, new=2)

    try:
        state.done.wait()
    except KeyboardInterrupt:
        state.canceled = True
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=1)

    if state.canceled or state.result is None:
        return None
    return state.result
