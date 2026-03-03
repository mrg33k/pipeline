#!/usr/bin/env python3
import json
from google_auth_oauthlib.flow import InstalledAppFlow

import config

SCOPES = [
    "https://www.googleapis.com/auth/gmail.compose",
    "https://www.googleapis.com/auth/gmail.settings.basic",
]


def main() -> int:
    flow = InstalledAppFlow.from_client_secrets_file(config.GMAIL_CLIENT_SECRET, SCOPES)
    creds = flow.run_local_server(port=0)
    payload = {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "scopes": creds.scopes,
    }
    with open(config.GMAIL_TOKENS_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print(f"Wrote refreshed token file: {config.GMAIL_TOKENS_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
