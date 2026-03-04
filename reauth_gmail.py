#!/usr/bin/env python3
"""
Gmail OAuth Re-authentication Script
=====================================
Run this when your gmail_tokens.json is missing, expired, or invalid.

Usage:
    python3 reauth_gmail.py

What it does:
    1. Reads client_secret.json from this directory
    2. Opens your browser for the Google OAuth consent screen
    3. Saves the resulting tokens to gmail_tokens.json in this directory

Requirements:
    pip install google-auth-oauthlib google-auth-httplib2 google-api-python-client

The script requests:
- 'gmail.compose' (create/manage drafts)
- 'gmail.settings.basic' (read account signature settings)
- 'gmail.readonly' (read recent sent recipients for duplicate protection)
It cannot read your inbox body or send emails on its own.
"""

import json
import os
import sys

# ── Dependency check ──────────────────────────────────────────────────────────
try:
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
except ImportError:
    print("Error: Required packages not installed.")
    print("Run: pip install google-auth-oauthlib google-auth-httplib2 google-api-python-client")
    sys.exit(1)

# ── Paths ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CLIENT_SECRET_PATH = os.path.join(SCRIPT_DIR, "client_secret.json")
TOKENS_PATH = os.path.join(SCRIPT_DIR, "gmail_tokens.json")

# Compose + settings scope (used to read the account default signature)
SCOPES = [
    "https://www.googleapis.com/auth/gmail.compose",
    "https://www.googleapis.com/auth/gmail.settings.basic",
    "https://www.googleapis.com/auth/gmail.readonly",
]


def _has_required_scopes(token_data: dict) -> bool:
    granted = set((token_data.get("scope") or "").split())
    required = set(SCOPES)
    return required.issubset(granted)


def main():
    print()
    print("=" * 55)
    print("  Gmail OAuth Re-authentication")
    print("  Ahead of Market Outreach Pipeline")
    print("=" * 55)
    print()

    # ── Check client_secret.json ──────────────────────────────────────────
    if not os.path.exists(CLIENT_SECRET_PATH):
        print(f"Error: client_secret.json not found at:")
        print(f"  {CLIENT_SECRET_PATH}")
        print()
        print("Download it from Google Cloud Console:")
        print("  1. Go to https://console.cloud.google.com/")
        print("  2. APIs & Services > Credentials")
        print("  3. Download your OAuth 2.0 Client ID JSON")
        print("  4. Save it as 'client_secret.json' in this directory")
        sys.exit(1)

    print(f"Found client_secret.json: {CLIENT_SECRET_PATH}")

    # ── Check for existing valid tokens ──────────────────────────────────
    if os.path.exists(TOKENS_PATH):
        print(f"Found existing tokens: {TOKENS_PATH}")
        try:
            with open(TOKENS_PATH) as f:
                token_data = json.load(f)
            with open(CLIENT_SECRET_PATH) as f:
                client_data = json.load(f)

            installed = client_data.get("installed", client_data.get("web", {}))
            creds = Credentials(
                token=token_data.get("access_token"),
                refresh_token=token_data.get("refresh_token"),
                token_uri=installed.get("token_uri", "https://oauth2.googleapis.com/token"),
                client_id=installed["client_id"],
                client_secret=installed["client_secret"],
                scopes=SCOPES,
            )
            if creds.valid and _has_required_scopes(token_data):
                print("Existing tokens are valid. No re-authentication needed.")
                print("Delete gmail_tokens.json and re-run this script to force a new login.")
                return
            if creds.valid and not _has_required_scopes(token_data):
                print("Existing tokens are valid but missing required scopes.")
                print("Starting OAuth flow to grant required Gmail scopes...")
            elif creds.expired and creds.refresh_token and _has_required_scopes(token_data):
                print("Tokens are expired. Attempting to refresh...")
                creds.refresh(Request())
                _save_tokens(creds, token_data)
                print("Tokens refreshed successfully.")
                return
        except Exception as e:
            print(f"Could not use existing tokens ({e}). Starting fresh OAuth flow.")

    # ── Run the OAuth flow ────────────────────────────────────────────────
    print()
    print("Starting OAuth flow...")
    print("A browser window will open. Sign in with the Google account")
    print("that owns the hello@aom-inhouse.com Gmail inbox.")
    print()

    try:
        flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_PATH, SCOPES)
        # run_local_server opens the browser and handles the callback automatically
        creds = flow.run_local_server(
            port=0,
            prompt="consent",
            access_type="offline",
            success_message=(
                "Authentication successful! You can close this tab and return to the terminal."
            ),
        )
    except Exception as e:
        print(f"OAuth flow failed: {e}")
        print()
        print("If the browser did not open, try running with --no-browser and")
        print("manually visiting the URL printed in the terminal.")
        sys.exit(1)

    # ── Save tokens ───────────────────────────────────────────────────────
    _save_tokens(creds)

    print()
    print("=" * 55)
    print("  Authentication successful!")
    print("=" * 55)
    print(f"  Tokens saved to: {TOKENS_PATH}")
    print()
    print("You can now run the pipeline:")
    print("  python3 run_pipeline.py --mode rewrite --dry-run")
    print("  python3 run_pipeline.py --mode draft")
    print("  python3 run_pipeline.py")
    print()


def _save_tokens(creds, existing_data: dict = None):
    """Save credentials to gmail_tokens.json."""
    token_data = {
        "access_token": creds.token,
        "refresh_token": creds.refresh_token,
        "scope": " ".join(creds.scopes) if creds.scopes else "",
        "token_type": "Bearer",
    }
    # Preserve any extra fields from existing token file
    if existing_data:
        for k, v in existing_data.items():
            if k not in token_data:
                token_data[k] = v

    with open(TOKENS_PATH, "w") as f:
        json.dump(token_data, f, indent=2)
    print(f"Tokens saved: {TOKENS_PATH}")


if __name__ == "__main__":
    main()
