"""The eight `@dg.asset_check` bodies, run without materializing anything.

`tests/test_definitions.py` proves each check is *registered*; this proves each
would *notice*. Otherwise the bodies run only in a full materialize, and
`just test` cannot tell a working check from one whose logic has inverted.

Every case below has a failing half, because a check that only ever sees
healthy input is green and measures nothing.

`AssetChecksDefinition` is callable and none of these take a `context`, so they
need no execution harness: point the module's `DUCKDB_PATH` at a throwaway file
— or its `LAKEHOUSE_DIR` at a throwaway catalog, for the checks that read `raw`
— call the check, read the `AssetCheckResult`.

**Which of the two a check reads is itself the thing to get right**, and
patching only one hides it: a check reading the warehouse for `raw` passes here
against a fixture that has the table, and fails in CI, where `raw` lives only in
the catalog. So the lakehouse cases assert `indicators_loaded` as well as the
verdict — an unpatched `LAKEHOUSE_DIR` reads the developer's real catalog, where
the verdict alone still looks right.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

from lake.lakehouse import ATTACH_ALIAS, catalog_path, data_path
from modern_data_stack.ducklake import attach
from orchestration.resources import dbt_project

# Importing `orchestration.assets` needs the manifest, which does not exist when
# ci.yml first runs pytest. CI re-runs this file after `dbt parse`, which
# `tests/test_workflows.py` enforces.
pytestmark = pytest.mark.skipif(
    not dbt_project.manifest_path.exists(),
    reason="needs dbt/target/manifest.json — run `just dbt-deps` and `dbt parse` first",
)

# The dlt-pipeline-deactivation fixture this file needs (importing the
# orchestration layer leaves a dlt pipeline active process-wide) lives in
# `tests/conftest.py`, shared with `test_definitions.py`.


@pytest.fixture(scope="module")
def assets():
    """The orchestration module, imported lazily so the skipif above can fire."""
    from orchestration import assets as module

    return module


def _warehouse(tmp_path: Path, *statements: str) -> str:
    """A throwaway DuckDB file built from `statements`, returned as a path str."""
    path = tmp_path / "warehouse.duckdb"
    con = duckdb.connect(str(path))
    try:
        for statement in statements:
            con.execute(statement)
    finally:
        con.close()
    return str(path)


def _lakehouse(tmp_path: Path, *statements: str) -> Path:
    """A throwaway DuckLake built from `statements`, returned as a directory str.

    `_warehouse`'s counterpart for the landing zone. The layout comes from
    `lake.lakehouse`'s own `catalog_path`/`data_path` rather than being spelled
    again here, so a test cannot be built against a shape production does not
    use. Statements run against the attached alias, so they name tables exactly
    as the check does.
    """
    lakehouse_dir = tmp_path / "lakehouse"
    data_path(lakehouse_dir).mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        attach(con, catalog_path(lakehouse_dir), data_path(lakehouse_dir), alias=ATTACH_ALIAS)
        for statement in statements:
            con.execute(statement)
    finally:
        con.close()
    return lakehouse_dir


def _meta(result, key):
    """The plain Python value behind a `MetadataValue` on an `AssetCheckResult`."""
    value = result.metadata[key]
    return getattr(value, "value", value)


def _sql_literal(v) -> str:
    if v is None:
        return "null"
    if isinstance(v, str):
        return f"'{v}'"
    return str(v)


def _values_clause(rows: list[tuple]) -> str:
    return ", ".join("(" + ", ".join(_sql_literal(v) for v in r) + ")" for r in rows)


# --------------------------------------------------------------------------- #
# analytics/pipeline_status — run_history_records_this_build
# --------------------------------------------------------------------------- #


def _run_results(tmp_path: Path, invocation_id: str) -> Path:
    """A minimal `run_results.json` announcing one invocation id.

    The check reads nothing else out of the artifact, so nothing else is posed:
    a fixture carrying `results` would suggest the check counts them, and it
    deliberately does not — `build_runs` owns that, and `pipeline_runs` is what
    this asks about.
    """
    path = tmp_path / "run_results.json"
    path.write_text(json.dumps({"metadata": {"invocation_id": invocation_id}}))
    return path


def _history(tmp_path: Path, *invocation_ids: str) -> str:
    return _warehouse(
        tmp_path,
        "create schema analytics",
        "create table analytics.pipeline_runs (invocation_id varchar)",
        *(f"insert into analytics.pipeline_runs values ('{i}')" for i in invocation_ids),
    )


def test_run_history_check_passes_when_this_build_left_rows(tmp_path, monkeypatch, assets):
    warehouse = _history(tmp_path, "abc-123", "abc-123", "older-run")
    monkeypatch.setattr(assets, "DUCKDB_PATH", warehouse)
    monkeypatch.setattr(
        assets, "dbt_run_results_path", lambda: str(_run_results(tmp_path, "abc-123"))
    )

    result = assets.run_history_records_this_build()

    assert result.passed
    assert _meta(result, "nodes_recorded") == 2
    assert _meta(result, "invocations_in_history") == 2


def test_run_history_check_fails_when_the_build_appended_nowhere(tmp_path, monkeypatch, assets):
    """The build appended nothing, but the table is not empty.

    If dbt writes `run_results.json` somewhere `build_runs` does not read, it
    appends nothing — yet on a developer's machine the table still holds earlier
    runs, so `count(*) > 0` would pass. The history here has rows and none of
    them are this build's.
    """
    warehouse = _history(tmp_path, "an-earlier-run", "an-earlier-run")
    monkeypatch.setattr(assets, "DUCKDB_PATH", warehouse)
    monkeypatch.setattr(
        assets,
        "dbt_run_results_path",
        lambda: str(_run_results(tmp_path, "the-build-that-just-ran")),
    )

    result = assets.run_history_records_this_build()

    assert not result.passed
    assert _meta(result, "nodes_recorded") == 0
    assert _meta(result, "invocations_in_history") == 1


def test_run_history_check_fails_when_dbt_left_no_artifact_at_all(tmp_path, monkeypatch, assets):
    """A missing artifact is a failure, not a pass by absence.

    `build_runs` tolerates it — a fresh clone is a real state and the other
    three tables do not need it — so the tolerance has to end somewhere, and it
    ends here: by the time `pipeline_status` materializes in the graph, a dbt
    build has run upstream of it and an absent artifact means the path moved.
    """
    monkeypatch.setattr(assets, "DUCKDB_PATH", _history(tmp_path, "whatever"))
    monkeypatch.setattr(assets, "dbt_run_results_path", lambda: str(tmp_path / "nowhere.json"))

    result = assets.run_history_records_this_build()

    assert not result.passed
    assert "run_results" in _meta(result, "reason")


# --------------------------------------------------------------------------- #
# reports/evidence_site — site_pages_all_rendered
# --------------------------------------------------------------------------- #


def _routes(tmp_path: Path, sizes: dict[str, int | None]) -> dict[str, Path]:
    """`{slug: path}`, writing a file of `size` bytes where size is not None."""
    routes = {}
    for slug, size in sizes.items():
        path = tmp_path / slug / "index.html"
        if size is not None:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"x" * size)
        routes[slug] = path
    return routes


def test_site_check_passes_when_every_page_rendered(tmp_path, monkeypatch, assets):
    routes = _routes(tmp_path, {"index": 19_000, "gold": 92_000})
    monkeypatch.setattr(assets, "page_routes", lambda: routes)

    result = assets.site_pages_all_rendered()

    assert result.passed
    assert _meta(result, "pages_expected") == 2


def test_site_check_fails_a_page_that_never_rendered(tmp_path, monkeypatch, assets):
    """`evidence build` exits 0 for a site missing a page."""
    routes = _routes(tmp_path, {"index": 19_000, "gold": None})
    monkeypatch.setattr(assets, "page_routes", lambda: routes)

    result = assets.site_pages_all_rendered()

    assert not result.passed
    assert _meta(result, "missing") == ["gold"]


def test_site_check_fails_a_route_that_emitted_only_the_shell(tmp_path, monkeypatch, assets):
    """Present and non-empty, and still not a page. This is the failure that
    looks most like success, which is why the check measures size at all."""
    routes = _routes(tmp_path, {"index": 19_000, "gold": 900})
    monkeypatch.setattr(assets, "page_routes", lambda: routes)

    result = assets.site_pages_all_rendered()

    assert not result.passed
    assert _meta(result, "suspiciously_small") == {"gold": 900}
