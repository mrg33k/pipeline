#!/usr/bin/env python3
"""
One-time Gmail OAuth re-auth to include compose + settings scopes.
Run this when account signature lookup is unavailable due to missing scope.
"""

import json

from google_auth_oauthlib.flow import InstalledAppFlow

import config


SCOPES = [
    "https://www.googleapis.com/auth/gmail.compose",
    "https://www.googleapis.com/auth/gmail.settings.basic",
]


def main() -> None:
    print("Starting Gmail OAuth re-auth...")
    print(f"Client secret: {config.GMAIL_CLIENT_SECRET}")
    print(f"Token output:   {config.GMAIL_TOKENS_PATH}")

    flow = InstalledAppFlow.from_client_secrets_file(config.GMAIL_CLIENT_SECRET, SCOPES)
    creds = flow.run_local_server(port=0, access_type="offline", prompt="consent")

    token_data = {
        "access_token": creds.token,
        "refresh_token": creds.refresh_token,
        "scope": " ".join(creds.scopes or SCOPES),
        "token_type": "Bearer",
    }

    with open(config.GMAIL_TOKENS_PATH, "w", encoding="utf-8") as f:
        json.dump(token_data, f, indent=2)

    print("Saved refreshed Gmail token with settings scope.")
    if not creds.refresh_token:
        print("Warning: refresh_token was not returned. Future refresh may fail.")


if __name__ == "__main__":
    main()
