"""
Generate an OAuth refresh token with the Google Content API scope.

Run this locally (not on Render) to get a refresh token for an account,
then paste the result into Render as an environment variable.

Usage:
    python generate_content_token.py happy    → generates HAPPY_CONTENT_REFRESH_TOKEN
    python generate_content_token.py upscale  → generates UPSCALE_CONTENT_REFRESH_TOKEN

Requirements:
    pip install google-auth-oauthlib

You'll need HAPPY_CLIENT_ID and HAPPY_CLIENT_SECRET from your Google Cloud project.
These are the same OAuth credentials used for the Happy Mondays MCC.
"""

import sys
import os

try:
    from google_auth_oauthlib.flow import InstalledAppFlow
except ImportError:
    print("Run: pip install google-auth-oauthlib")
    sys.exit(1)

SCOPES = [
    "https://www.googleapis.com/auth/adwords",
    "https://www.googleapis.com/auth/content",
]

CLIENT_ID     = os.environ.get("HAPPY_CLIENT_ID", "").strip()
CLIENT_SECRET = os.environ.get("HAPPY_CLIENT_SECRET", "").strip()

if not CLIENT_ID or not CLIENT_SECRET:
    print("Set HAPPY_CLIENT_ID and HAPPY_CLIENT_SECRET as env vars before running.")
    sys.exit(1)

account = sys.argv[1].lower() if len(sys.argv) > 1 else "happy"
if account not in ("happy", "upscale"):
    print("Usage: python generate_content_token.py [happy|upscale]")
    sys.exit(1)

env_var = "HAPPY_CONTENT_REFRESH_TOKEN" if account == "happy" else "UPSCALE_CONTENT_REFRESH_TOKEN"

print(f"\nGenerating Content API refresh token for: {account}")
print(f"Sign in with the Google account that owns the {account.title()} GMC accounts.\n")

client_config = {
    "installed": {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "redirect_uris": ["urn:ietf:wg:oauth:2.0:oob", "http://localhost"],
    }
}

flow = InstalledAppFlow.from_client_config(client_config, scopes=SCOPES)
creds = flow.run_local_server(port=0)

print("\n" + "="*60)
print(f"Add this to Render environment variables:")
print(f"\n  {env_var}={creds.refresh_token}\n")
print("="*60)
