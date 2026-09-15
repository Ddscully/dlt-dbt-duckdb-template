"""The pipeline as a Dagster asset graph.

    raw/*  (dlt -> DuckLake)  ->  staging/stg_*  ->  marts/*  (dbt)  +->  analytics/pipeline_status
                                                                    +->  reports/evidence_site  (Evidence)

The layers are wired by *asset key*, not by ordering:

* the dlt resources are keyed ``["raw", <resource>]`` to match the asset keys
  dagster-dbt derives from `dbt/models/staging/_sources.yml`;
* dagster-dbt reads `manifest.json`, so the model-to-model edges come from dbt's
  own `ref()` graph;
* the Evidence site declares one dep per table its source queries read, from
  the maps in `publish/build_report.py`.
"""

# NB: no `from __future__ import annotations` here — Dagster inspects the
# `context` parameter's annotation object, and a stringified one fails its check.

import json
from collections.abc import Mapping, Sequence
from datetime import timedelta
from pathlib import Path
from typing import Any

import dagster as dg
import duckdb
from dagster import AssetExecutionContext
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets, get_asset_key_for_model
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData

from gold_warehouse.db import scalar
from gold_warehouse.paths import dbt_run_results_path, dbt_target_path, warehouse_path
from ingest.pipeline import (
    FULL_REFRESH_RESOURCES,
    INCREMENTAL_RESOURCES,
    PARTITIONED_RESOURCES,
    build_pipeline,
    load_groups,
    public_indicators,
)
from orchestration.resources import dbt_project
from publish.build_report import (
    BUILD_DIR,
    TABLE_TO_ASSET_KEY,
    TABLE_TO_DBT_MODEL,
    page_routes,
    run as build_report,
)
from transform.pipeline_status import run as run_pipeline_status

DUCKDB_PATH = str(warehouse_path())

# --------------------------------------------------------------------------- #
# Freshness policies
# --------------------------------------------------------------------------- #
# These state what *should* be true regardless of whether a run happened, so a
# schedule that quietly stopped firing shows up as a stale asset in the UI
# instead of having to be inferred from an absent run.

# Raw pulls: upstream publishers push on their own cadence, so two days without
# a successful load is worth a warning and a week is a failure.
RAW_FRESHNESS = dg.FreshnessPolicy.time_window(
    fail_window=timedelta(days=7),
    warn_window=timedelta(days=2),
)

# Modelled layers hang off the daily 06:00 schedule: rebuilt by 08:00 UTC from
# data no older than the preceding midnight.
MODELLED_FRESHNESS = dg.FreshnessPolicy.cron(
    deadline_cron="0 8 * * *",
    lower_bound_delta=timedelta(hours=8),
)


# --------------------------------------------------------------------------- #
# Layer 1 — dlt ingestion
# --------------------------------------------------------------------------- #

RAW_DESCRIPTIONS = {
    "gold_prices_monthly": "Monthly gold prices since 1833, USD per troy ounce (CSV).",
}

UNPARTITIONED_RESOURCES = (
    *FULL_REFRESH_RESOURCES,
    *(name for name in INCREMENTAL_RESOURCES if name not in PARTITIONED_RESOURCES),
)


class RawSchemaDltTranslator(DagsterDltTranslator):
    """Key dlt resources as ``raw/<resource>``.

    dagster-dlt would otherwise name them ``dlt_public_indicators_<resource>``,
    which wouldn't line up with the ``raw`` source keys dagster-dbt generates —
    and the two halves of the graph would sit side by side, unconnected.
    """

    def get_asset_spec(self, data: DltResourceTranslatorData) -> dg.AssetSpec:
        name = data.resource.name
        return (
            super()
            .get_asset_spec(data)
            .replace_attributes(
                key=dg.AssetKey(["raw", name]),
                group_name="ingestion",
                description=RAW_DESCRIPTIONS.get(name),
                freshness_policy=RAW_FRESHNESS,
                # dlt resources are independent HTTP pulls; the default
                # translator would make them depend on a synthetic source asset.
                deps=[],
            )
        )


@dlt_assets(
    # Everything that isn't year-partitioned: the four `replace` resources plus
    # `ecb_fx_rates`, which merges but has no per-year fetch to express. The
    # mixed dispositions are fine here because the body asks `load_groups` for
    # the kwargs rather than spelling them.
    dlt_source=public_indicators().with_resources(*UNPARTITIONED_RESOURCES),
    dlt_pipeline=build_pipeline(),
    dagster_dlt_translator=RawSchemaDltTranslator(),
    name="ingest_public_indicators",
)
def raw_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    # One op for all five: the catalog takes a single writer, so parallel steps
    # would only contend for it. `load_groups` supplies the `run()` kwargs, as it
    # does for the CLI, and takes the selection so one asset means one load.
    selected = {key.path[-1] for key in context.selected_asset_keys}
    for names, kwargs in load_groups(selected):
        context.log.info("loading %s (%s)", ", ".join(names), kwargs)
        yield from dlt.run(
            context=context,
            dlt_source=public_indicators().with_resources(*names),
            **kwargs,
        )


# --------------------------------------------------------------------------- #
# Layer 2 — dbt staging + marts
# --------------------------------------------------------------------------- #


class FolderGroupDbtTranslator(DagsterDbtTranslator):
    """Group dbt assets by top-level folder, key versioned models under their
    schema, and give every model a freshness policy."""

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> dg.AssetKey:
        # The default keys an unversioned model `[schema, name]` but a versioned
        # one `[alias]` alone (`fct_emissions_energy`, `fct_emissions_energy_v1`).
        # Prefixing the schema keeps `marts/...` keys, the `key:"marts/*"`
        # selection and the materialisation history unchanged by versioning.
        key = super().get_asset_key(dbt_resource_props)
        if not dbt_resource_props.get("version"):
            return key
        schema = dbt_resource_props.get("config", {}).get("schema")
        return key.with_prefix(schema) if schema else key

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str | None:
        # Snapshots live directly in `snapshots/`, so there's no folder to take —
        # and the default would name the group after the snapshot itself.
        if dbt_resource_props.get("resource_type") == "snapshot":
            return dbt_resource_props.get("schema")
        fqn = dbt_resource_props.get("fqn") or []
        # fqn is [project, <subfolders...>, name]
        return fqn[1] if len(fqn) > 2 else super().get_group_name(dbt_resource_props)

    def get_asset_spec(self, manifest, unique_id, project) -> dg.AssetSpec:
        spec = super().get_asset_spec(manifest, unique_id, project)
        return spec.replace_attributes(freshness_policy=MODELLED_FRESHNESS)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=FolderGroupDbtTranslator(),
)
def dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    # `build` runs the tests too, so they surface as asset checks on their models.
    # `target_path` is required: by default dagster-dbt writes each invocation's
    # artifacts to a unique subdirectory, and `pipeline_status` reads
    # `run_results.json` from this fixed path — without it `analytics.pipeline_runs`
    # gets no rows. `run_history_records_this_build` guards it.
    yield from dbt.cli(["build"], context=context, target_path=Path(dbt_target_path())).stream()


# --------------------------------------------------------------------------- #
# Layer 3 — Polars derived metrics
# --------------------------------------------------------------------------- #


@dg.asset(
    key=dg.AssetKey(["analytics", "pipeline_status"]),
    # Inventories every modelled layer, so it follows the whole dbt build.
    deps=list(dbt_models.keys),
    group_name="analytics",
    kinds={"polars", "duckdb"},
    freshness_policy=MODELLED_FRESHNESS,
    description=(
        "Pipeline observability: dlt load times per source, row counts and year "
        "spans per layer, and the stored-failure count for every dbt test. "
        "Rendered by the Evidence 'Pipeline' page."
    ),
)
def pipeline_status(context: AssetExecutionContext) -> dg.MaterializeResult:
    written = run_pipeline_status()
    for name, rows in written.items():
        context.log.info("wrote analytics.%s (%s rows)", name, rows)
    return dg.MaterializeResult(
        metadata={"dagster/row_count": sum(written.values()), "tables": written}
    )


# --------------------------------------------------------------------------- #
# Layer 4 — the Evidence site
# --------------------------------------------------------------------------- #

EVIDENCE_SITE = dg.AssetKey(["reports", "evidence_site"])

# One dep per table the source queries read, not one edge to order it last, so
# the graph shows which models a stale page depends on. `publish.build_report`
# owns the mapping and `tests/test_report.py` holds it to the SQL.
SITE_DEPS = [
    *(
        get_asset_key_for_model([dbt_models], model)
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
        "every page under `reports/pages/` against them. Published by "
        "`.github/workflows/pages.yml`."
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


# --------------------------------------------------------------------------- #
# Asset checks — the ones dbt can't express
# --------------------------------------------------------------------------- #


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


__all__ = [
    "EVIDENCE_SITE",
    "dbt_models",
    "evidence_site",
    "pipeline_status",
    "raw_assets",
]
