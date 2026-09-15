"""Guards that keep the recorded fixtures in step with the pipeline.

The failure this exists to prevent: a URL constant changes, or a source grows a
parameter, and the fixture-backed CI job keeps passing against a payload that no
longer represents what the pipeline asks for.
"""

from __future__ import annotations

import re

import pytest

from modern_data_stack import fixtures as _fixtures
from ingest import fixtures, pipeline
from ingest.sources.gold import GOLD_PRICES_MONTHLY

ALL_URLS = [GOLD_PRICES_MONTHLY]


@pytest.mark.parametrize("url", ALL_URLS)
def test_every_pipeline_url_has_a_fixture(url):
    """Each URL the pipeline can build resolves to a file that exists."""
    path = fixtures.path_for(url)
    assert path.exists(), f"missing fixture {path.name} — run `just record-fixtures`"


def test_unmapped_url_raises_rather_than_falling_back_to_the_network():
    """A silent fall-through would turn 'offline CI' into 'CI that is online on
    Tuesdays'."""
    with pytest.raises(KeyError, match="no fixture mapped"):
        fixtures.path_for("https://example.test/something-new.json")


def test_a_broken_route_is_not_reported_as_a_missing_fixture(tmp_path):
    """`str.format` raises `KeyError` both for an unmapped URL and for a template
    naming something the pattern doesn't capture — and `path_for` rewrites
    `KeyError` as "no fixture mapped … see scripts/record_fixtures.py". For a URL
    that matched a route and then failed to format, that sends you off to
    re-record fixtures for a URL that was fine. Hence the different type.
    """
    routes = [(re.compile(r"series/(?P<code>\S+)"), "series_{name}.json")]
    with pytest.raises(ValueError, match="not a named group"):
        _fixtures.resolve("https://example.test/series/prices", routes, tmp_path)


def test_enabled_reads_the_env_var(monkeypatch):
    monkeypatch.delenv(fixtures.ENV_VAR, raising=False)
    assert fixtures.enabled() is False
    monkeypatch.setenv(fixtures.ENV_VAR, "1")
    assert fixtures.enabled() is True
    monkeypatch.setenv(fixtures.ENV_VAR, "0")
    assert fixtures.enabled() is False


def _routes_matching(url: str) -> list[_fixtures.Route]:
    """Every route whose pattern claims `url`, in the order `resolve` walks them."""
    return [route for route in fixtures._ROUTES if route[0].search(url)]


def test_no_two_routes_claim_the_same_url():
    """A route the pipeline can never reach is dead weight that looks alive.

    `_fixtures.resolve` returns at the *first* pattern that matches, so a route
    added after one that already covers its URLs is silently shadowed — the
    fixture it names is recorded, committed, and never once served. The existing
    URL guard cannot see it: the URL still resolves, and still resolves to a file
    that exists, just not the intended one.

    Checked over `ALL_URLS` rather than by comparing patterns to each other,
    because regex containment is undecidable in general and the URLs the pipeline
    actually builds are the only ones that matter.
    """
    clashes = []
    for url in ALL_URLS:
        hits = [f"{pattern.pattern} -> {template}" for pattern, template in _routes_matching(url)]
        if len(hits) > 1:
            clashes.append(f"{url}\n      " + "\n      ".join(hits))

    assert not clashes, "a URL is claimed by more than one route:\n    " + "\n    ".join(clashes)


def test_every_route_is_reachable_from_some_pipeline_url():
    """`_ROUTES` and `ALL_URLS` are both maintained by hand; this is what makes
    them agree.

    It is the only check here that can see a *missing* `ALL_URLS` entry. The
    other two iterate URLs, so a source absent from the list is absent from
    them as well, and its route is never exercised.
    """
    reached = {template for url in ALL_URLS for _, template in _routes_matching(url)}
    unreachable = [template for _, template in fixtures._ROUTES if template not in reached]

    assert not unreachable, (
        f"routes no URL in ALL_URLS reaches: {unreachable} — either the pipeline stopped "
        f"building that URL, or ALL_URLS is missing it"
    )


def test_no_recorded_fixture_is_orphaned():
    """The reverse direction: a file in `tests/fixtures/ingest/` nothing serves.

    `record_fixtures.py` writes files and never deletes them, so dropping or
    renaming a source leaves its payload behind. An orphan is
    harmless at runtime, which is exactly why it needs a test — it is committed
    data that nothing reads and that looks like coverage.
    """
    served = {fixtures.path_for(url).name for url in ALL_URLS}
    orphaned = {p.name for p in fixtures.FIXTURE_DIR.iterdir() if p.is_file()} - served

    assert not orphaned, (
        f"recorded fixtures nothing serves: {sorted(orphaned)} — delete them, or add "
        f"the URL that reads them to ALL_URLS"
    )


# A fixture is named after the resource it feeds. A source whose fixture cannot
# be — a real archive standing in for a real download, keeping the upstream
# file's name — is declared here rather than excused, as `{resource: filename}`.
FIXTURE_NAME_EXCEPTIONS: dict[str, str] = {}


def test_every_resource_in_the_source_has_a_fixture_route():
    """A resource added without a route fails only in `just test-pipeline`, and
    reports `no fixture mapped for <url>` — the right message a tier too late,
    naming the URL rather than the resource that started building it.

    Enumerated off `public_indicators()` for the same reason
    `test_load_groups_covers_every_resource_in_the_source_exactly_once` is: the
    source is the thing that decides what gets fetched.
    """
    templates = [template for _, template in fixtures._ROUTES]

    for name in sorted(r.name for r in pipeline.public_indicators().resources.values()):
        declared = FIXTURE_NAME_EXCEPTIONS.get(name)
        if declared is not None:
            assert declared in templates, f"{name} declares fixture {declared!r}, which is no route"
        else:
            assert any(t.startswith(name) for t in templates), f"no fixture route for {name!r}"
