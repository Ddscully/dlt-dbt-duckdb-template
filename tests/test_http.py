"""`ingest.http`: the fetch every JSON source goes through.

Nothing in the gold-prices example calls it, so no pipeline run exercises it;
the next source will, and its retry schedule is the difference between a
transient error page and a red nightly run.
"""

from __future__ import annotations

import json
import time

import pytest
import requests

from ingest import fixtures, http


class FakeResponse:
    """Minimal stand-in for `requests.Response`."""

    def __init__(self, payload=None, *, status: int = 200, body: str | None = None):
        self._payload = payload
        self._body = body
        self.status_code = status

    def raise_for_status(self):
        if self.status_code >= 400:
            # `response=self`, as `requests` does: `get_json` reads the status off it.
            raise requests.HTTPError(f"{self.status_code} error", response=self)

    def json(self):
        if self._body is not None:
            return json.loads(self._body)  # raises ValueError on a non-JSON body
        return self._payload


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """The retry backoff is real seconds; tests shouldn't pay for it."""
    monkeypatch.setattr(time, "sleep", lambda _: None)


@pytest.fixture(autouse=True)
def _fixtures_off(monkeypatch):
    """These tests exercise the network path, so make sure an inherited
    INGEST_FIXTURES from the caller's shell can't quietly bypass it."""
    monkeypatch.delenv(fixtures.ENV_VAR, raising=False)


def _mock_get(monkeypatch, responses):
    """Serve `responses` in order, recording the URLs requested."""
    seen: list[str] = []
    queue = list(responses)

    def fake_get(url, timeout=None):
        seen.append(url)
        result = queue.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(requests, "get", fake_get)
    return seen


def test_get_json_retries_then_succeeds(monkeypatch):
    seen = _mock_get(
        monkeypatch,
        [requests.ConnectionError("boom"), FakeResponse({"ok": True})],
    )
    assert http.get_json("https://example.test/x") == {"ok": True}
    assert len(seen) == 2


def test_get_json_raises_on_persistent_server_error(monkeypatch):
    """A 500 must not be handed on as data.

    Without `raise_for_status()` an HTML error page parses to *something* and
    flows into the landing zone.
    """
    seen = _mock_get(monkeypatch, [FakeResponse(status=500)] * http.RETRIES)
    with pytest.raises(RuntimeError, match="failed to fetch JSON"):
        http.get_json("https://example.test/x")
    assert len(seen) == http.RETRIES


def test_get_json_raises_on_non_json_body(monkeypatch):
    _mock_get(monkeypatch, [FakeResponse(body="<html>maintenance</html>")] * http.RETRIES)
    with pytest.raises(RuntimeError, match="failed to fetch JSON"):
        http.get_json("https://example.test/x")


def test_get_json_waits_out_a_minute_long_outage(monkeypatch):
    """The waits double, so the retries span a minute. Upstream's first
    schedule gave up inside 4.5 s, and a publisher's error page that had
    cleared by the time anyone looked became a red live run."""
    sleeps: list[float] = []
    monkeypatch.setattr(time, "sleep", sleeps.append)
    _mock_get(monkeypatch, [FakeResponse(status=503)] * (http.RETRIES - 1) + [FakeResponse({})])

    assert http.get_json("https://example.test/x") == {}
    assert sleeps == [4.0, 8.0, 16.0, 32.0]
    assert sum(sleeps) >= 60


def test_get_json_raises_a_client_error_at_once(monkeypatch):
    """A 404 or 400 says the request is wrong; a minute of retries gets the same answer."""
    seen = _mock_get(monkeypatch, [FakeResponse(status=404)])
    with pytest.raises(requests.HTTPError):
        http.get_json("https://example.test/x")
    assert len(seen) == 1


def test_get_json_retries_a_rate_limit(monkeypatch):
    """429 is the one 4xx that a later identical request can pass."""
    seen = _mock_get(monkeypatch, [FakeResponse(status=429), FakeResponse({"ok": True})])
    assert http.get_json("https://example.test/x") == {"ok": True}
    assert len(seen) == 2


def test_get_json_object_refuses_an_array(monkeypatch):
    """An endpoint documented as an object that answers with an array is a
    failed fetch, not data of another shape."""
    _mock_get(monkeypatch, [FakeResponse([1, 2])])
    with pytest.raises(RuntimeError, match="expected a JSON object"):
        http.get_json_object("https://example.test/x")
