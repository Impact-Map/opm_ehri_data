"""Send Buttondown email notification for new OPM data."""
import os
import json
import urllib.request
import sys

api_key = os.environ.get("BUTTONDOWN_API_KEY", "").strip()
subject = os.environ.get("EMAIL_SUBJECT", "")
body = os.environ.get("EMAIL_BODY", "")

if not api_key:
    print("ERROR: BUTTONDOWN_API_KEY not set")
    sys.exit(1)

payload = json.dumps({"subject": subject, "body": body, "status": "about_to_send"}).encode()

req = urllib.request.Request(
    "https://api.buttondown.email/v1/emails",
    data=payload,
    headers={
        "Authorization": f"Token {api_key}",
        "Content-Type": "application/json",
        "X-Buttondown-Live-Dangerously": "true",
    },
    method="POST",
)

try:
    with urllib.request.urlopen(req) as r:
        print(f"Sent: {r.status} {r.read().decode()}")
except urllib.error.HTTPError as e:
    print(f"ERROR {e.code}: {e.read().decode()}")
    sys.exit(1)
