"""Shared HTTP fetching for the ingest layer.

Two functions, both fixture-aware: `INGEST_FIXTURES=1` routes them through
`ingest.fixtures` instead of the network, which is what makes CI offline.

Nothing here is used by the gold-prices example, which is a whole-file CSV read
straight by Polars. It is here because the *next* source is usually a JSON API,
and because the fixture-awareness has to live wherever the fetch does.

**Call these as `http.get_json(...)`, never `from ingest.http import get_json`.**
A unit test for a source patches `setattr(http, "get_json", …)` to hand it a
payload; a name bound into the source module at import time is not looked up
through that patch, so the test would pass while exercising the real fetch.
"""

from __future__ import annotations

import gzip
import json
import time

import requests

from ingest import fixtures


def get_json(url: str, *, timeout: int = 120, retries: int = 3) -> dict | list:
    """GET + parse JSON with a few retries — public APIs return a transient
    error page or a non-JSON body often enough to be worth handling once.

    A non-2xx status is retried and ultimately raised: without the
    `raise_for_status()` an HTML/JSON error body would parse fine and be handed
    on as if it were data.
    """
    if fixtures.enabled():
        path = fixtures.path_for(url)
        # A fixture may be gzipped — a whole API response is often megabytes of
        # JSON, and it is kept whole so the parsing path is the real one.
        if path.suffix == ".gz":
            return json.loads(gzip.decompress(path.read_bytes()))
        return json.loads(path.read_text())

    last: Exception | None = None
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, ValueError) as exc:  # ValueError = JSONDecodeError
            last = exc
            if attempt < retries - 1:
                time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"failed to fetch JSON from {url}: {last}")


def get_json_object(url: str, *, timeout: int = 120, retries: int = 3) -> dict:
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
