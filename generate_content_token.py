"""
Generate an OAuth refresh token with the Google Content API scope.
Prints an auth URL — open it in your browser, sign in, paste the code back.
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
env_var = "HAPPY_CONTENT_REFRESH_TOKEN" if account == "happy" else "UPSCALE_CONTENT_REFRESH_TOKEN"

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
flow.redirect_uri = "urn:ietf:wg:oauth:2.0:oob"

auth_url, _ = flow.authorization_url(prompt="consent", access_type="offline")

print("\n" + "="*60)
print(f"Sign in with your {account.upper()} Google account:")
print(f"\n{auth_url}\n")
print("="*60)

code = input("Paste the authorisation code here: ").strip()

flow.fetch_token(code=code)
creds = flow.credentials

print("\n" + "="*60)
print(f"Add this to Render environment variables:\n")
print(f"  Key:   {env_var}")
print(f"  Value: {creds.refresh_token}")
print("="*60 + "\n")
