"""Shared HTTP fetching for the ingest layer.

Three fetchers, all fixture-aware: `INGEST_FIXTURES=1` routes them through
`ingest.fixtures` instead of the network, which is what makes CI offline. They
share one retry loop, `_fetch`, which logs every failed attempt and wait.

The gold-prices example reads its CSV through `get_bytes`, not by handing
Polars the URL, which tries once. `get_json` is here because the *next* source
is usually a JSON API, and because the fixture-awareness has to live wherever
the fetch does.

**Call these as `http.get_json(...)`, never `from ingest.http import get_json`.**
A unit test for a source patches `setattr(http, "get_json", …)` to hand it a
payload; a name bound into the source module at import time is not looked up
through that patch, so the test would pass while exercising the real fetch.
"""

from __future__ import annotations

import gzip
import json
import logging
import time
from collections.abc import Callable

import requests

from ingest import fixtures

# Logged, not printed: Dagster captures a step's log records, and a minute of
# silent retries reads as a hang.
log = logging.getLogger(__name__)

# Waits of 4, 8, 16 and 32 s between five attempts: a minute, which outlasts the
# transient error pages public APIs serve. A healthy fetch never sleeps.
RETRIES = 5
BACKOFF_SECONDS = 4.0


# Errors in the request itself, which a later identical request repeats: the
# same rule as a 4xx. Most are also `ValueError`s, so they are named before the
# `ValueError` that means an unparseable body.
#
# `InvalidJSONError` is deliberately absent. It means a request body that would
# not serialise, which nothing here sends, but `resp.json()` raises its subclass
# `requests.exceptions.JSONDecodeError` for an error page served with a 200: the
# transient case `get_json` exists to retry.
PERMANENT_ERRORS = (
    requests.exceptions.InvalidHeader,
    requests.exceptions.InvalidURL,
    requests.exceptions.InvalidSchema,
    requests.exceptions.MissingSchema,
    requests.exceptions.URLRequired,
    requests.exceptions.TooManyRedirects,
)

# Request Timeout and Too Many Requests: the server ran out of time or patience,
# and the same request later can pass.
TRANSIENT_CLIENT_ERRORS = (408, 429)


def retryable(exc: Exception) -> bool:
    """Whether a later identical request could succeed.

    A 5xx, 408, 429, timeout, reset or unparseable body could: 408 and 429 are
    the 4xx that report the server's state, not the request's. Any other 4xx
    could not: the request itself is wrong, and a minute of retries would get the
    same answer. Nor could a malformed URL or a redirect loop (`PERMANENT_ERRORS`).

    A source whose API charges every attempt against a quota wants a stricter
    rule of its own: giving up on every 4xx but 429, a 408 included.
    """
    if isinstance(exc, PERMANENT_ERRORS):
        return False
    if isinstance(exc, requests.HTTPError):
        status = exc.response.status_code if exc.response is not None else 0
        return not (400 <= status < 500 and status not in TRANSIENT_CLIENT_ERRORS)
    return isinstance(exc, (requests.RequestException, ValueError))  # ValueError = bad JSON


def backoff(url: str, attempt: int, retries: int, exc: Exception) -> None:
    """Log a failed attempt and sleep before the next one; nothing after the last."""
    if attempt >= retries - 1:
        return
    wait = BACKOFF_SECONDS * 2**attempt
    log.info("%s: attempt %d of %d failed (%s); retrying in %.0f s",
             url, attempt + 1, retries, exc, wait)  # fmt: skip
    time.sleep(wait)


def _fetch[T](
    url: str,
    *,
    what: str,
    timeout: int,
    retries: int,
    read: Callable[[requests.Response], T],
) -> T:
    """GET with retries, `read` turning the response into the result, and
    `what` naming the result in the error raised when every attempt failed.

    `raise_for_status()` comes first: without it an HTML or JSON error body
    would read fine and be handed on as if it were data. A `ValueError` from
    `read` (an unparseable body) is retried like a 5xx.
    """
    last: Exception | None = None
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            return read(resp)
        except (requests.RequestException, ValueError) as exc:
            if not retryable(exc):
                raise
            last = exc
            backoff(url, attempt, retries, exc)
    raise RuntimeError(f"failed to fetch {what} from {url}: {last}")


def get_json(url: str, *, timeout: int = 120, retries: int = RETRIES) -> dict | list:
    """GET + parse JSON with retries — public APIs return a transient error
    page or a non-JSON body often enough to be worth waiting out.

    Retried with doubling waits and ultimately raised; see `retryable` for what
    is retried and `_fetch` for why an error status is never parsed as data.
    """
    if fixtures.enabled():
        path = fixtures.path_for(url)
        # A fixture may be gzipped — a whole API response is often megabytes of
        # JSON, and it is kept whole so the parsing path is the real one.
        if path.suffix == ".gz":
            return json.loads(gzip.decompress(path.read_bytes()))
        return json.loads(path.read_text())
    return _fetch(url, what="JSON", timeout=timeout, retries=retries, read=lambda resp: resp.json())


def get_bytes(url: str, *, timeout: int = 120, retries: int = RETRIES) -> bytes:
    """GET a whole file's body with the same retries — the gold-prices CSV."""
    if fixtures.enabled():
        # A gzipped fixture is un-gzipped: what comes back is what the URL serves.
        path = fixtures.path_for(url)
        body = path.read_bytes()
        return gzip.decompress(body) if path.suffix == ".gz" else body
    return _fetch(
        url, what="the file", timeout=timeout, retries=retries, read=lambda resp: resp.content
    )


def get_json_object(url: str, *, timeout: int = 120, retries: int = RETRIES) -> dict:
    """`get_json` for an endpoint that documents a JSON *object*.

    `get_json` returns `dict | list`, because plenty of APIs answer with a
    top-level array (`[metadata, [records…]]` is a common shape); a caller that
    knows better narrows it here rather than asserting by hand. An error object
    served with a 200 is something public APIs do, so the narrowing is a real
    check and not a cast.
    """
    payload = get_json(url, timeout=timeout, retries=retries)
    if not isinstance(payload, dict):
        # TRY004 asks for TypeError, but no argument was wrong — the server sent
        # the wrong shape, and that is a runtime failure of the fetch.
        raise RuntimeError(  # noqa: TRY004
            f"expected a JSON object from {url}, got {payload!r:.300}"
        )
    return payload
