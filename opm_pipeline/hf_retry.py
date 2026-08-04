"""Retry helper for HuggingFace metadata calls.

HF rate-limits its JSON API (429) independently of the file CDN. The cron runs
hourly on days 1-7, so back-to-back runs can trip a 429 on a plain repo listing
even though nothing is wrong with the token or the data. Uploads already retry
(see uploader.upload_to_huggingface); this covers the read-side calls.
"""

import time


def is_rate_limit(e: Exception) -> bool:
    """True if the exception looks like HF throttling rather than a real error."""
    msg = str(e).lower()
    return "rate limit" in msg or "429" in msg or "too many requests" in msg


def hf_call(fn, *args, max_retries: int = 4, **kwargs):
    """Call an HF API function, retrying only on rate limits with backoff.

    Waits 2s, 4s, 8s between attempts. Non-429 errors raise immediately so
    genuine failures (bad token, missing repo) still surface loudly.
    """
    for attempt in range(max_retries):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if attempt < max_retries - 1 and is_rate_limit(e):
                wait = 2 ** (attempt + 1)
                print(f"  HF rate-limited ({fn.__name__}); retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise
