"""Layer 4 — the Evidence site, built from the warehouse it reports on."""

# No `from __future__ import annotations`: Dagster refuses a string `context` annotation.

import dagster as dg
from dagster import AssetExecutionContext
from dagster_dbt import get_asset_key_for_model

from orchestration.defs import models
from orchestration.freshness import MODELLED_FRESHNESS
from publish.build_report import TABLE_TO_ASSET_KEY, TABLE_TO_DBT_MODEL, run as build_report

EVIDENCE_SITE = dg.AssetKey(["reports", "evidence_site"])

# One dep per table the source queries read, not one edge to order it last, so
# the graph shows which models a stale page depends on. `publish.build_report`
# owns the mapping and `tests/test_report.py` holds it to the SQL.
SITE_DEPS = [
    *(
        get_asset_key_for_model([models.dbt_models], model)
        for model in sorted(set(TABLE_TO_DBT_MODEL.values()))
    ),
    # dict.fromkeys: the four `pipeline_*` tables share one asset, and Dagster
    # rejects a duplicated dep.
    *(dg.AssetKey(list(key)) for key in dict.fromkeys(TABLE_TO_ASSET_KEY.values())),
]


@dg.asset(
    key=EVIDENCE_SITE,
    deps=SITE_DEPS,
    group_name="reports",
    kinds={"evidence", "duckdb"},
    freshness_policy=MODELLED_FRESHNESS,
    description=(
        "The Evidence dashboard as a static site in `reports/build/`: extracts "
        "the warehouse tables to parquet (`npm run sources:strict`), then renders "
        "every page under `reports/pages/` against them."
    ),
)
def evidence_site(context: AssetExecutionContext) -> dg.MaterializeResult:
    # Needs Node on PATH, which is why this asset is *excluded* from the
    # `full_refresh` job — see orchestration/definitions.py.
    summary = build_report()
    context.log.info(
        "built %s pages from %s source queries (%s files, %.1f MB)",
        summary["pages"],
        summary["source_queries"],
        summary["files"],
        summary["bytes"] / 1e6,
    )
    return dg.MaterializeResult(
        metadata={
            "pages": summary["pages"],
            "files": summary["files"],
            "bytes": summary["bytes"],
            "warehouse_tables": summary["warehouse_tables"],
            "build_dir": dg.MetadataValue.path(summary["build_dir"]),
        }
    )
