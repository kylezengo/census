"""Shared helpers for the ACS download scripts."""

import os
import sys
import time

from dotenv import load_dotenv

MAX_RETRIES = 3
MAX_WORKERS = 6


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def require_api_key():
    """Census API key from .env. Fails loudly — an unkeyed request returns an
    HTML 'Missing Key' page, which surfaces as a confusing JSON decode error."""
    load_dotenv()
    key = os.getenv("census_api_key")
    if not key:
        raise SystemExit(
            "census_api_key not set — add it to .env (see .env.example). "
            "Free key: https://api.census.gov/data/key_signup.html"
        )
    return key


def skip_if_downloaded(outputs, what):
    """Exit early unless --force, when every output already exists."""
    if "--force" not in sys.argv and all(os.path.exists(f) for f in outputs):
        log(f"{what} already downloaded. Run with --force to re-download.")
        sys.exit(0)
