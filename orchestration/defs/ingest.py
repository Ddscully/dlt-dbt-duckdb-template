"""Layer 1 — dlt ingestion into the DuckLake landing zone, and its expiry."""

# No `from __future__ import annotations`: Dagster inspects the `context`
# parameter's annotation object, and a stringified one fails its check.

import dagster as dg
from dagster import AssetExecutionContext
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData

from ingest.pipeline import (
    FULL_REFRESH_RESOURCES,
    INCREMENTAL_RESOURCES,
    PARTITIONED_RESOURCES,
    build_pipeline,
    load_groups,
    public_indicators,
)
from lake.lakehouse import HISTORY_TABLE, KEEP_LOADS, expire as expire_lakehouse
from orchestration.defs.analytics import pipeline_status
from orchestration.freshness import RAW_FRESHNESS

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
    # Everything the orchestration layer does not partition. Mixed load
    # dispositions are fine here because the body asks `load_groups` for each
    # group's `run()` kwargs rather than spelling them.
    dlt_source=public_indicators().with_resources(*UNPARTITIONED_RESOURCES),
    dlt_pipeline=build_pipeline(),
    dagster_dlt_translator=RawSchemaDltTranslator(),
    name="ingest_public_indicators",
)
def raw_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    # One op for every resource: the catalog takes a single writer, so parallel
    # steps would only contend for it. `load_groups` supplies the `run()` kwargs,
    # as it does for the CLI, and takes the selection so one asset means one
    # load.
    selected = {key.path[-1] for key in context.selected_asset_keys}
    for names, kwargs in load_groups(selected):
        context.log.info("loading %s (%s)", ", ".join(names), kwargs)
        yield from dlt.run(
            context=context,
            dlt_source=public_indicators().with_resources(*names),
            **kwargs,
        )


@dg.asset(
    key=dg.AssetKey(["lake", "snapshot_expiry"]),
    # Last in `full_refresh`, as in `just run`: a step whose upstream failed is
    # skipped, so a load that breaks the dbt build expires nothing. On the raw
    # assets alone it ran straight after the load, whatever followed, and two bad
    # loads in a row expired the last good one out of the only copy.
    # `tests/test_definitions.py` holds every other asset in the job upstream.
    deps=[pipeline_status],
    group_name="raw",
    kinds={"ducklake"},
    description=(
        f"Expires lakehouse snapshots before the last {KEEP_LOADS} loads of "
        f"`{HISTORY_TABLE}` and deletes the files only they read. Counted in "
        "loads, not days, so a catalog left idle keeps its diffable pair. Runs "
        "last, so a failed build upstream expires nothing."
    ),
)
def snapshot_expiry(context: AssetExecutionContext) -> dg.MaterializeResult:
    freed = expire_lakehouse()
    context.log.info("expired %(snapshots)s snapshots, %(files)s files, %(orphans)s orphans", freed)
    return dg.MaterializeResult(metadata=freed)
