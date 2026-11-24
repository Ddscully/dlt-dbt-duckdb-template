"""Layer 3 — the Polars derived metrics, and the pipeline's report on itself."""

# No `from __future__ import annotations`: Dagster refuses a string `context` annotation.

import dagster as dg
from dagster import AssetExecutionContext

from orchestration.defs.models import FCT_GOLD_PRICE_MONTH, dbt_models
from orchestration.freshness import MODELLED_FRESHNESS
from transform.gold_price_trend import run as run_gold_price_trend
from transform.pipeline_status import run as run_pipeline_status


@dg.asset(
    key=dg.AssetKey(["analytics", "gold_price_trend"]),
    # The one mart it reads, by name: a Polars asset that depended on the whole
    # dbt build would serialise behind models it never touches.
    deps=[FCT_GOLD_PRICE_MONTH],
    group_name="analytics",
    kinds={"polars", "duckdb"},
    freshness_policy=MODELLED_FRESHNESS,
    description=(
        "The rolling twelve-month average gold price and the change on a year "
        "earlier, derived from `marts.fct_gold_price_month`."
    ),
)
def gold_price_trend(context: AssetExecutionContext) -> dg.MaterializeResult:
    rows = run_gold_price_trend()
    context.log.info("wrote analytics.gold_price_trend (%s rows)", rows)
    return dg.MaterializeResult(metadata={"dagster/row_count": rows})


@dg.asset(
    key=dg.AssetKey(["analytics", "pipeline_status"]),
    # Inventories every modelled layer, so it follows the whole dbt build and
    # the Polars tables it should count the rows of.
    deps=[*dbt_models.keys, gold_price_trend],
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
