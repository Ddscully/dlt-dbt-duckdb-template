"""Asset checks — the ones dbt can't express.

One module rather than one beside each asset, because `tests/test_asset_checks.py`
points the checks at throwaway files by patching this module's globals:
`DUCKDB_PATH`, `page_routes` and `dbt_run_results_path`. Each must be a name *in
this module* that the checks read at call time. One imported into another module
by name would keep the real value there, and its check would read the real
warehouse with the test green.
"""

# No `from __future__ import annotations`, to match the asset modules beside it.

import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import dagster as dg
import duckdb

from modern_data_stack.db import scalar
from modern_data_stack.paths import dbt_run_results_path, warehouse_path
from orchestration.defs.analytics import pipeline_status
from orchestration.defs.site import EVIDENCE_SITE
from publish.build_report import BUILD_DIR, page_routes

# The warehouse the asset checks read; their tests point it at a throwaway file.
DUCKDB_PATH = str(warehouse_path())


def _scalar(query: str, params: Sequence[Any] | None = None):
    """`db.scalar` against a fresh read-only connection to the warehouse."""
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    try:
        return scalar(con, query, params)
    finally:
        con.close()


@dg.asset_check(asset=pipeline_status, blocking=True)
def run_history_records_this_build() -> dg.AssetCheckResult:
    """The dbt build that just ran left rows in `analytics.pipeline_runs`.

    A wiring check, not a data check. `build_runs` reads `run_results.json` by
    path, and when that path and the build's target path disagreed every
    orchestrated run wrote the table empty with nothing failing. `count(*) > 0`
    would pass on that state, because earlier builds' rows are still there, so
    this asserts the invocation `run_results.json` names is in the table. A stale
    artifact from an earlier build passes, correctly: its rows were appended then.
    """
    path = Path(dbt_run_results_path())
    if not path.exists():
        return dg.AssetCheckResult(
            passed=False,
            metadata={
                "run_results_path": str(path),
                "reason": "no dbt run_results.json here — nothing could have been appended",
            },
        )
    invocation = (json.loads(path.read_text()).get("metadata") or {}).get("invocation_id")
    recorded = _scalar(
        "select count(*) from analytics.pipeline_runs where invocation_id = ?",
        [invocation],
    )
    return dg.AssetCheckResult(
        passed=recorded > 0,
        metadata={
            "invocation_id": invocation or "",
            "nodes_recorded": recorded,
            "invocations_in_history": _scalar(
                "select count(distinct invocation_id) from analytics.pipeline_runs"
            ),
        },
    )


@dg.asset_check(asset=EVIDENCE_SITE, blocking=True)
def site_pages_all_rendered() -> dg.AssetCheckResult:
    """Every page in `reports/pages/` has HTML in `reports/build/`.

    `evidence build` exits 0 for a site that is missing a page, and nothing
    downstream reads the output — so without this a half-rendered dashboard would
    materialise green and deploy. Checks the file is non-trivial as well as
    present: a route that rendered nothing but the shell is the failure that looks
    most like success.
    """
    routes = page_routes()
    # Real pages render at over 20 kB; 8 kB catches a route that emitted only
    # the SvelteKit shell.
    empty = {
        slug: path.stat().st_size
        for slug, path in routes.items()
        if path.exists() and path.stat().st_size < 8_000
    }
    missing = sorted(slug for slug, path in routes.items() if not path.exists())
    return dg.AssetCheckResult(
        passed=not missing and not empty,
        metadata={
            "pages_expected": len(routes),
            "missing": missing,
            "suspiciously_small": empty,
            "build_dir": dg.MetadataValue.path(str(BUILD_DIR)),
        },
    )
