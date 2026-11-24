"""`ingest.http`: the fetch every source goes through.

The gold-prices example reads its CSV through `get_bytes`; `get_json` waits for
the next source, usually a JSON API. Their shared retry loop is the difference
between a transient error page and a red nightly run.
"""

from __future__ import annotations

import json
import time

import pytest
import requests

from ingest import fixtures, http
from ingest.sources import gold


class FakeResponse:
    """Minimal stand-in for `requests.Response`."""

    def __init__(self, payload=None, *, status: int = 200, body: str | None = None):
        self._payload = payload
        self._body = body
        self.status_code = status
        self.content = (body or "").encode()

    def raise_for_status(self):
        if self.status_code >= 400:
            # `response=self`, as `requests` does: `get_json` reads the status off it.
            raise requests.HTTPError(f"{self.status_code} error", response=self)

    def json(self):
        if self._body is not None:
            # requests' own type, as `Response.json` raises: it subclasses
            # `InvalidJSONError` as well as `ValueError`, so a retry rule that
            # names the one can catch it before the other.
            try:
                return json.loads(self._body)
            except json.JSONDecodeError as exc:
                raise requests.exceptions.JSONDecodeError(exc.msg, exc.doc, exc.pos) from exc
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


def test_a_real_non_json_response_is_retried(monkeypatch):
    """A maintenance page served with a 200, through requests' own `Response`.

    `Response.json` raises `requests.exceptions.JSONDecodeError`, which is an
    `InvalidJSONError` too. Naming that among the permanent errors made this
    fail on its first attempt upstream, while a fake raising Python's own
    `json.JSONDecodeError` kept the test above green.
    """
    page = requests.Response()
    page.status_code = 200
    page._content = b"<html>maintenance</html>"
    seen = _mock_get(monkeypatch, [page, FakeResponse({"ok": True})])
    assert http.get_json("https://example.test/x") == {"ok": True}
    assert len(seen) == 2


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


@pytest.mark.parametrize("status", [408, 429])
def test_get_json_retries_a_timeout_or_a_rate_limit(monkeypatch, status):
    """408 and 429 are the 4xx that a later identical request can pass."""
    seen = _mock_get(monkeypatch, [FakeResponse(status=status), FakeResponse({"ok": True})])
    assert http.get_json("https://example.test/x") == {"ok": True}
    assert len(seen) == 2


def test_every_failed_attempt_is_logged_with_its_wait(monkeypatch, caplog):
    """A minute of silent retries reads as a hang in a run's log; each one says
    which URL failed, how, and how long the wait is."""
    caplog.set_level("INFO", logger=http.log.name)
    _mock_get(
        monkeypatch,
        [FakeResponse(status=503), requests.ConnectionError("reset"), FakeResponse({})],
    )

    http.get_json("https://example.test/x")
    lines = [r.getMessage() for r in caplog.records if r.name == http.log.name]
    assert lines == [
        "https://example.test/x: attempt 1 of 5 failed (503 error); retrying in 4 s",
        "https://example.test/x: attempt 2 of 5 failed (reset); retrying in 8 s",
    ]


@pytest.mark.parametrize(
    "error",
    [
        requests.exceptions.MissingSchema("no scheme"),
        requests.exceptions.InvalidURL("bad host"),
        requests.exceptions.TooManyRedirects("loop"),
        requests.exceptions.InvalidHeader("newline in a header"),
    ],
)
def test_a_request_that_can_never_succeed_is_not_retried(monkeypatch, error):
    """A malformed URL or a redirect loop is the 4xx rule's case: the request is
    wrong, and a minute of retries would end in the same error."""
    seen = _mock_get(monkeypatch, [error])
    with pytest.raises(type(error)):
        http.get_bytes("https://example.test/x.csv")
    assert len(seen) == 1


def test_get_bytes_retries_like_get_json(monkeypatch):
    seen = _mock_get(
        monkeypatch, [requests.ConnectionError("reset"), FakeResponse(body="a,b\n1,2\n")]
    )
    assert http.get_bytes("https://example.test/x.csv") == b"a,b\n1,2\n"
    assert len(seen) == 2


def test_get_bytes_raises_after_the_last_attempt(monkeypatch):
    _mock_get(monkeypatch, [FakeResponse(status=502)] * http.RETRIES)
    with pytest.raises(RuntimeError, match="failed to fetch the file"):
        http.get_bytes("https://example.test/x.csv")


def test_get_bytes_reads_the_fixture_offline(monkeypatch):
    monkeypatch.setenv(fixtures.ENV_VAR, "1")
    monkeypatch.setattr(requests, "get", lambda *a, **kw: pytest.fail("went to the network"))
    assert http.get_bytes(gold.GOLD_PRICES_MONTHLY).startswith(b"Date,Price")


def test_gold_prices_survive_a_dropped_connection(monkeypatch):
    """The source handed Polars the URL, which tries once, so one reset
    connection failed the nightly run outright."""
    seen = _mock_get(
        monkeypatch,
        [requests.ConnectionError("reset"), FakeResponse(body="Date,Price\n1833-01,18.93\n")],
    )
    rows = list(gold.gold_prices_monthly())
    assert rows == [{"Date": "1833-01", "Price": 18.93}]
    assert seen == [gold.GOLD_PRICES_MONTHLY, gold.GOLD_PRICES_MONTHLY]


def test_get_json_object_refuses_an_array(monkeypatch):
    """An endpoint documented as an object that answers with an array is a
    failed fetch, not data of another shape."""
    _mock_get(monkeypatch, [FakeResponse([1, 2])])
    with pytest.raises(RuntimeError, match="expected a JSON object"):
        http.get_json_object("https://example.test/x")
