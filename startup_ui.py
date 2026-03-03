import html
import threading
import webbrowser
from dataclasses import dataclass, replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable, Optional
from urllib.parse import parse_qs

from runtime_settings import RunSettings


@dataclass
class StartupChoice:
    action: str
    settings: RunSettings


class _UIState:
    def __init__(self, defaults: RunSettings):
        self.defaults = defaults
        self.result: Optional[RunSettings] = None
        self.action = "start"
        self.canceled = False
        self.rewrite_preview_signature: Optional[str] = None
        self.done = threading.Event()


def _to_bool(value: str) -> bool:
    return value in {"1", "true", "on", "yes"}


def _parse_form_settings(state: _UIState, body: bytes) -> RunSettings:
    values = parse_qs(body.decode("utf-8"), keep_blank_values=True)

    max_emails = int((values.get("max_emails", [str(state.defaults.max_emails)])[0] or "").strip())
    pages = int((values.get("pages", [str(state.defaults.pages)])[0] or "").strip())
    openai_model = values.get("openai_model", [state.defaults.openai_model])[0].strip()
    email_system_prompt = values.get("email_system_prompt", [state.defaults.email_system_prompt])[0]
    filter_extra_directions = values.get("filter_extra_directions", [state.defaults.filter_extra_directions])[0]
    rewrite_count = int((values.get("rewrite_count", [str(state.defaults.rewrite_count)])[0] or "").strip())
    rewrite_confirmed = _to_bool(values.get("rewrite_confirmed", ["0"])[0])

    dry_run = _to_bool(values.get("dry_run", ["0"])[0])
    skip_drafts = _to_bool(values.get("skip_drafts", ["0"])[0])

    candidate = RunSettings(
        max_emails=max_emails,
        pages=pages,
        dry_run=dry_run,
        skip_drafts=skip_drafts,
        openai_model=openai_model,
        email_system_prompt=email_system_prompt,
        filter_extra_directions=filter_extra_directions,
        rewrite_count=rewrite_count,
        rewrite_confirmed=rewrite_confirmed,
    ).normalized()
    candidate.validate()
    return candidate


def _rewrite_signature(settings: RunSettings) -> str:
    return "|".join(
        [
            str(settings.rewrite_count),
            settings.openai_model,
            str(settings.pages),
            str(int(settings.dry_run)),
            str(int(settings.skip_drafts)),
            settings.email_system_prompt.strip(),
            settings.filter_extra_directions.strip(),
        ]
    )


def _render_preview_block(previews: Optional[list]) -> str:
    if not previews:
        return ""

    cards = []
    for i, item in enumerate(previews, start=1):
        to_label = html.escape(item.get("to", ""))
        company = html.escape(item.get("company", ""))
        subject = html.escape(item.get("subject", ""))
        body = html.escape(item.get("body", ""))
        issues = item.get("issues", []) or []
        issues_text = ", ".join(issues) if issues else "none"
        issues_text = html.escape(issues_text)

        cards.append(
            f"""
            <div class="preview-card">
              <div class="preview-meta"><strong>Sample {i}</strong> | {to_label} | {company}</div>
              <div class="preview-meta"><strong>Subject:</strong> {subject}</div>
              <div class="preview-meta"><strong>QA issues:</strong> {issues_text}</div>
              <pre>{body}</pre>
            </div>
            """
        )

    return f"""
      <section class="preview-wrap">
        <h2>Preview (3 Samples)</h2>
        <p class="hint">Adjust prompt controls and click Preview again until this looks right.</p>
        {''.join(cards)}
      </section>
    """


def _render_prompt_assist_block(prompt_assist_response: str = "") -> str:
    if not prompt_assist_response:
        return ""

    return f"""
      <section class="assist-wrap">
        <h2>Prompt Assistant</h2>
        <pre>{html.escape(prompt_assist_response)}</pre>
      </section>
    """


def _render_form(
    defaults: RunSettings,
    error: str = "",
    previews: Optional[list] = None,
    prompt_assist_response: str = "",
) -> str:
    checked_dry = "checked" if defaults.dry_run else ""
    checked_skip = "checked" if defaults.skip_drafts else ""
    error_block = (
        f"<div class='error'>{html.escape(error)}</div>" if error else ""
    )
    preview_block = _render_preview_block(previews)
    assist_block = _render_prompt_assist_block(prompt_assist_response)

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>AOM Pipeline Setup</title>
  <style>
    :root {{
      --bg: #090d14;
      --bg-grad: #111a29;
      --panel: #121b29;
      --panel-soft: #0f1622;
      --ink: #e7edf8;
      --muted: #97a9c6;
      --line: #273449;
      --accent: #4ea5ff;
      --accent-2: #2b79d8;
      --danger: #ff6b6b;
    }}
    body {{ margin: 0; font-family: "Avenir Next", "Helvetica Neue", Helvetica, Arial, sans-serif; background: radial-gradient(circle at top, var(--bg-grad), var(--bg)); color: var(--ink); }}
    .layout {{ max-width: 1220px; margin: 24px auto; display: grid; grid-template-columns: 360px 1fr; gap: 18px; padding: 0 14px; box-sizing: border-box; }}
    .sidebar, .dashboard {{ background: linear-gradient(180deg, #141f2f, var(--panel)); border: 1px solid var(--line); border-radius: 14px; box-shadow: 0 18px 46px rgba(0,0,0,0.34); }}
    .sidebar {{ padding: 22px; }}
    .dashboard {{ padding: 22px; min-height: 80vh; }}
    h1 {{ margin: 0 0 8px; font-size: 26px; letter-spacing: 0.2px; }}
    p {{ margin: 0 0 14px; color: var(--muted); }}
    .dash-title {{ margin: 0 0 8px; font-size: 22px; }}
    .dash-sub {{ margin-bottom: 18px; }}
    label {{ font-weight: 600; display: block; margin: 14px 0 6px; }}
    input[type="number"], input[type="text"], textarea {{ width: 100%; box-sizing: border-box; border: 1px solid var(--line); border-radius: 10px; padding: 10px 12px; font: inherit; background: var(--panel-soft); color: var(--ink); }}
    textarea {{ min-height: 180px; resize: vertical; }}
    details {{ margin-top: 16px; border: 1px solid var(--line); border-radius: 10px; padding: 10px 12px; background: rgba(255,255,255,0.02); }}
    summary {{ cursor: pointer; font-weight: 700; }}
    .checks {{ display: flex; flex-wrap: wrap; gap: 18px; margin-top: 10px; }}
    .checks label {{ font-weight: 500; margin: 0; display: flex; gap: 8px; align-items: center; }}
    .actions {{ display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-top: 20px; }}
    .actions button.primary {{ grid-column: span 2; }}
    button {{ border: 1px solid transparent; border-radius: 10px; padding: 10px 12px; font-weight: 700; cursor: pointer; }}
    button.primary {{ background: linear-gradient(180deg, var(--accent), var(--accent-2)); color: #061321; }}
    button.secondary {{ background: #1a2434; color: #dbe7fb; border-color: #2a3a51; }}
    .hint {{ font-size: 13px; color: var(--muted); margin-top: 4px; }}
    .error {{ margin: 12px 0; background: rgba(255,107,107,0.12); border: 1px solid rgba(255,107,107,0.4); color: var(--danger); padding: 10px 12px; border-radius: 8px; }}
    .status-card {{ border: 1px solid var(--line); border-radius: 10px; padding: 12px; background: rgba(10,16,26,0.62); margin-bottom: 16px; }}
    .preview-wrap {{ margin-top: 18px; border-top: 1px solid var(--line); padding-top: 16px; }}
    .preview-wrap h2 {{ margin: 0 0 8px; font-size: 22px; }}
    .preview-card {{ border: 1px solid var(--line); border-radius: 10px; padding: 12px; margin-top: 12px; background: rgba(9,15,25,0.6); }}
    .preview-meta {{ font-size: 13px; color: #b8c6de; margin-bottom: 6px; }}
    .assist-wrap {{ margin-top: 18px; border-top: 1px solid var(--line); padding-top: 16px; }}
    .assist-wrap h2 {{ margin: 0 0 8px; font-size: 22px; }}
    .assist-input {{ min-height: 96px !important; }}
    pre {{ margin: 0; white-space: pre-wrap; background: #0e1622; border: 1px solid var(--line); border-radius: 8px; padding: 10px; color: #d9e6fc; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; font-size: 12px; line-height: 1.45; }}
    .loading-overlay {{ position: fixed; inset: 0; background: rgba(8,12,18,0.68); display: none; align-items: center; justify-content: center; z-index: 9999; backdrop-filter: blur(3px); }}
    .loading-box {{ width: min(460px, 92vw); border: 1px solid #2a3950; background: linear-gradient(180deg, #0f1827, #0d1521); border-radius: 14px; padding: 18px; box-shadow: 0 18px 40px rgba(0,0,0,0.45); }}
    .loading-head {{ display: flex; align-items: center; gap: 12px; margin-bottom: 10px; }}
    .spinner {{ width: 18px; height: 18px; border: 2px solid rgba(255,255,255,0.25); border-top-color: #66b3ff; border-radius: 50%; animation: spin 0.9s linear infinite; }}
    .loading-title {{ font-weight: 700; }}
    .loading-msg {{ color: var(--muted); font-size: 13px; margin-bottom: 12px; }}
    .progress {{ width: 100%; height: 8px; border-radius: 999px; background: #132032; overflow: hidden; border: 1px solid #22344a; }}
    .progress-bar {{ height: 100%; width: 42%; background: linear-gradient(90deg, #3b92ee, #77bfff); border-radius: 999px; animation: glide 1.3s ease-in-out infinite; }}
    @keyframes spin {{ to {{ transform: rotate(360deg); }} }}
    @keyframes glide {{ 0% {{ transform: translateX(-115%); }} 50% {{ transform: translateX(110%); }} 100% {{ transform: translateX(-115%); }} }}
    @media (max-width: 980px) {{
      .layout {{ grid-template-columns: 1fr; }}
      .dashboard {{ min-height: auto; }}
    }}
  </style>
</head>
<body>
  <div id="loadingOverlay" class="loading-overlay" aria-hidden="true">
    <div class="loading-box">
      <div class="loading-head">
        <div class="spinner"></div>
        <div id="loadingTitle" class="loading-title">Processing...</div>
      </div>
      <div id="loadingMsg" class="loading-msg">Working through your request.</div>
      <div class="progress"><div class="progress-bar"></div></div>
    </div>
  </div>
  <main class="layout">
    <aside class="sidebar">
      <h1>Pipeline Controls</h1>
      <p>Run settings for this session only.</p>
      {error_block}
      <form id="start-form" method="post" action="/start">
      <label for="max_emails">Max emails</label>
      <input id="max_emails" name="max_emails" type="number" min="1" max="200" value="{defaults.max_emails}" required>
      <div class="hint">Allowed range: 1 to 200</div>

      <label for="rewrite_count">Rewrite count</label>
      <input id="rewrite_count" name="rewrite_count" type="number" min="1" max="200" value="{defaults.rewrite_count}" required>
      <div class="hint">When rewriting, only this many newest active drafts are processed.</div>

      <label for="email_system_prompt">Email AI directions</label>
      <textarea id="email_system_prompt" name="email_system_prompt" required>{html.escape(defaults.email_system_prompt)}</textarea>
      <label for="prompt_assist_input">Ask AI to refine this prompt</label>
      <textarea id="prompt_assist_input" name="prompt_assist_input" class="assist-input" placeholder="Example: make paragraph 2 less salesy and more conversational."></textarea>
      <input type="hidden" id="rewrite_confirmed" name="rewrite_confirmed" value="0">

      <details>
        <summary>Advanced</summary>

        <label for="filter_extra_directions">Filter AI extra directions</label>
        <textarea id="filter_extra_directions" name="filter_extra_directions" style="min-height:100px;">{html.escape(defaults.filter_extra_directions)}</textarea>

        <label for="openai_model">OpenAI model</label>
        <input id="openai_model" name="openai_model" type="text" value="{html.escape(defaults.openai_model)}" required>

        <label for="pages">Apollo search pages</label>
        <input id="pages" name="pages" type="number" min="1" max="10" value="{defaults.pages}" required>
        <div class="hint">Allowed range: 1 to 10</div>

        <div class="checks">
          <label><input type="checkbox" name="dry_run" value="1" {checked_dry}> Dry run</label>
          <label><input type="checkbox" name="skip_drafts" value="1" {checked_skip}> Skip drafts</label>
        </div>
      </details>

        <div class="actions">
          <button class="primary" type="submit">Start Pipeline</button>
          <button class="secondary" type="button" onclick="promptAssist()">Refine Prompt with AI</button>
          <button class="secondary" type="button" onclick="previewThree()">Preview 3 Samples</button>
          <button class="secondary" type="button" onclick="previewRewriteBatch()">Preview Rewrite Batch</button>
          <button class="secondary" type="button" onclick="confirmRewrite()">Confirm Rewrite</button>
          <button class="secondary" type="button" onclick="rewriteAll()">Rewrite All</button>
          <button class="secondary" type="button" onclick="cancelRun()">Cancel</button>
        </div>
      </form>
    </aside>
    <section class="dashboard">
      <h2 class="dash-title">Run Dashboard</h2>
      <p class="dash-sub">Preview tone, QA fit, and rewrite targets before running.</p>
      <div class="status-card">
        <strong>Status:</strong> Ready. Use preview actions before rewrite for faster QA cycles.
      </div>
      {assist_block}
      {preview_block}
    </section>
  </main>

  <script>
    let submitted = false;
    function setLoading(active, title, message) {{
      const overlay = document.getElementById('loadingOverlay');
      if (!overlay) return;
      if (active) {{
        document.getElementById('loadingTitle').textContent = title || 'Processing...';
        document.getElementById('loadingMsg').textContent = message || 'Working through your request.';
        overlay.style.display = 'flex';
      }} else {{
        overlay.style.display = 'none';
      }}
    }}
    document.getElementById('start-form').addEventListener('submit', () => {{
      submitted = true;
      setLoading(true, 'Starting pipeline', 'Preparing run and launching processing steps.');
    }});
    function cancelRun() {{
      submitted = true;
      setLoading(true, 'Canceling', 'Stopping this setup flow.');
      fetch('/cancel', {{ method: 'POST' }}).then(() => {{
        document.body.innerHTML = '<main><h1>Run canceled</h1><p>You can close this tab.</p></main>';
      }});
    }}
    function previewRewriteBatch() {{
      submitted = false;
      setLoading(true, 'Building rewrite preview', 'Generating 3 samples from the selected rewrite subset.');
      const form = document.getElementById('start-form');
      document.getElementById('rewrite_confirmed').value = '0';
      const data = new URLSearchParams(new FormData(form));
      fetch('/preview-rewrite', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8' }},
        body: data.toString()
      }}).then((resp) => resp.text()).then((htmlText) => {{
        document.open();
        document.write(htmlText);
        document.close();
      }}).catch(() => {{
        setLoading(false);
      }});
    }}
    function confirmRewrite() {{
      submitted = true;
      setLoading(true, 'Rewriting drafts', 'Replacing selected drafts now.');
      const form = document.getElementById('start-form');
      document.getElementById('rewrite_confirmed').value = '1';
      const data = new URLSearchParams(new FormData(form));
      fetch('/rewrite-today', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8' }},
        body: data.toString()
      }}).then((resp) => {{
        if (resp.ok) {{
          document.body.innerHTML = '<main><h1>Rewriting today\\'s drafts...</h1><p>You can close this tab.</p></main>';
          return;
        }}
        return resp.text().then((htmlText) => {{
          document.open();
          document.write(htmlText);
          document.close();
          submitted = false;
          setLoading(false);
        }});
      }}).catch(() => {{
        submitted = false;
        setLoading(false);
      }});
    }}
    function rewriteAll() {{
      const ok = window.confirm("Rewrite ALL active drafts? This will replace every matched draft.");
      if (!ok) return;
      submitted = true;
      setLoading(true, 'Rewriting all drafts', 'Replacing every active matched draft.');
      const form = document.getElementById('start-form');
      document.getElementById('rewrite_confirmed').value = '1';
      const data = new URLSearchParams(new FormData(form));
      fetch('/rewrite-all', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8' }},
        body: data.toString()
      }}).then((resp) => {{
        if (resp.ok) {{
          document.body.innerHTML = '<main><h1>Rewriting all active drafts...</h1><p>You can close this tab.</p></main>';
          return;
        }}
        return resp.text().then((htmlText) => {{
          document.open();
          document.write(htmlText);
          document.close();
          submitted = false;
          setLoading(false);
        }});
      }}).catch(() => {{
        submitted = false;
        setLoading(false);
      }});
    }}
    function previewThree() {{
      submitted = false;
      setLoading(true, 'Generating samples', 'Creating 3 sample emails with current settings.');
      const form = document.getElementById('start-form');
      const data = new URLSearchParams(new FormData(form));
      fetch('/preview', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8' }},
        body: data.toString()
      }}).then((resp) => resp.text()).then((htmlText) => {{
        document.open();
        document.write(htmlText);
        document.close();
      }}).catch(() => {{
        setLoading(false);
      }});
    }}
    function promptAssist() {{
      submitted = false;
      setLoading(true, 'Refining prompt', 'Applying your prompt instruction with AI.');
      const form = document.getElementById('start-form');
      const data = new URLSearchParams(new FormData(form));
      fetch('/prompt-assist', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8' }},
        body: data.toString()
      }}).then((resp) => resp.text()).then((htmlText) => {{
        document.open();
        document.write(htmlText);
        document.close();
      }}).catch(() => {{
        setLoading(false);
      }});
    }}
    window.addEventListener('beforeunload', () => {{
      if (!submitted) {{
        navigator.sendBeacon('/cancel', '');
      }}
    }});
  </script>
</body>
</html>"""


def collect_run_settings(
    defaults: RunSettings,
    preview_callback: Optional[Callable[[RunSettings], list]] = None,
    preview_rewrite_callback: Optional[Callable[[RunSettings], list]] = None,
    prompt_assist_callback: Optional[Callable[[RunSettings, str], dict]] = None,
) -> Optional[StartupChoice]:
    """Open local browser form and return startup action + run settings, or None if canceled."""
    state = _UIState(defaults=defaults)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != "/":
                self.send_response(404)
                self.end_headers()
                return

            page = _render_form(state.defaults)
            payload = page.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_POST(self):
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length)

            if self.path == "/start":
                try:
                    parsed = _parse_form_settings(state, body)
                except Exception as exc:  # pylint: disable=broad-except
                    page = _render_form(state.defaults, error=str(exc))
                    payload = page.encode("utf-8")
                    self.send_response(400)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                    return

                state.result = parsed
                state.action = "start"
                state.done.set()
                payload = b"<html><body><h2>Starting pipeline...</h2><p>You can close this tab.</p></body></html>"
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                threading.Thread(target=self.server.shutdown, daemon=True).start()
                return

            if self.path == "/preview":
                try:
                    parsed = _parse_form_settings(state, body)
                    state.defaults = parsed
                    state.rewrite_preview_signature = None
                    previews = preview_callback(parsed) if preview_callback else []
                    page = _render_form(state.defaults, previews=previews)
                    payload = page.encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                    return
                except Exception as exc:  # pylint: disable=broad-except
                    page = _render_form(state.defaults, error=str(exc))
                    payload = page.encode("utf-8")
                    self.send_response(400)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                    return

            if self.path == "/preview-rewrite":
                try:
                    parsed = _parse_form_settings(state, body)
                    state.defaults = parsed
                    previews = preview_rewrite_callback(parsed) if preview_rewrite_callback else []
                    state.rewrite_preview_signature = _rewrite_signature(parsed)
                    page = _render_form(state.defaults, previews=previews)
                    payload = page.encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                    return
                except Exception as exc:  # pylint: disable=broad-except
                    page = _render_form(state.defaults, error=str(exc))
                    payload = page.encode("utf-8")
                    self.send_response(400)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                    return

            if self.path == "/prompt-assist":
                try:
                    parsed = _parse_form_settings(state, body)
                    values = parse_qs(body.decode("utf-8"), keep_blank_values=True)
                    instruction = (values.get("prompt_assist_input", [""])[0] or "").strip()
                    if not instruction:
                        raise ValueError("Please enter a prompt change request first.")

                    result = (
                        prompt_assist_callback(parsed, instruction)
                        if prompt_assist_callback
                        else {"updated_prompt": parsed.email_system_prompt, "assistant_text": "Prompt assistant unavailable."}
                    )
                    updated_prompt = (result.get("updated_prompt") or parsed.email_system_prompt).strip()
                    assistant_text = (result.get("assistant_text") or "").strip()
                    parsed.email_system_prompt = updated_prompt
                    state.defaults = parsed
                    state.rewrite_preview_signature = None

                    page = _render_form(state.defaults, prompt_assist_response=assistant_text)
                    payload = page.encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                    return
                except Exception as exc:  # pylint: disable=broad-except
                    page = _render_form(state.defaults, error=str(exc))
                    payload = page.encode("utf-8")
                    self.send_response(400)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                    return

            if self.path == "/rewrite-today":
                try:
                    parsed = _parse_form_settings(state, body)
                    state.defaults = parsed
                except Exception as exc:  # pylint: disable=broad-except
                    page = _render_form(state.defaults, error=str(exc))
                    payload = page.encode("utf-8")
                    self.send_response(400)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                    return

                if not parsed.rewrite_confirmed:
                    page = _render_form(state.defaults, error="Preview rewrite batch first, then click Confirm Rewrite.")
                    payload = page.encode("utf-8")
                    self.send_response(400)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                    return

                if state.rewrite_preview_signature is None or state.rewrite_preview_signature != _rewrite_signature(parsed):
                    page = _render_form(state.defaults, error="Preview rewrite batch first, then click Confirm Rewrite.")
                    payload = page.encode("utf-8")
                    self.send_response(400)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                    return

                state.result = parsed
                state.action = "rewrite_today"
                state.done.set()
                payload = b"<html><body><h2>Rewriting today's drafts...</h2><p>You can close this tab.</p></body></html>"
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                threading.Thread(target=self.server.shutdown, daemon=True).start()
                return

            if self.path == "/rewrite-all":
                try:
                    parsed = _parse_form_settings(state, body)
                    state.defaults = parsed
                except Exception as exc:  # pylint: disable=broad-except
                    page = _render_form(state.defaults, error=str(exc))
                    payload = page.encode("utf-8")
                    self.send_response(400)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                    return

                if not parsed.rewrite_confirmed:
                    page = _render_form(state.defaults, error="Confirm rewrite is required.")
                    payload = page.encode("utf-8")
                    self.send_response(400)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                    return

                state.result = parsed
                state.action = "rewrite_all"
                state.done.set()
                payload = b"<html><body><h2>Rewriting all active drafts...</h2><p>You can close this tab.</p></body></html>"
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                threading.Thread(target=self.server.shutdown, daemon=True).start()
                return

            if self.path == "/cancel":
                state.canceled = True
                state.done.set()
                payload = b"ok"
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                threading.Thread(target=self.server.shutdown, daemon=True).start()
                return

            self.send_response(404)
            self.end_headers()

        def log_message(self, format, *args):
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    host, port = server.server_address
    url = f"http://{host}:{port}/"

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    print(f"\nStartup config UI: {url}")
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

    return StartupChoice(action=state.action, settings=replace(state.result))
