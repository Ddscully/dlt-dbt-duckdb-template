"""dlt ingestion: pull the public sources into the DuckLake landing zone.

Set ``INGEST_FIXTURES=1`` to read checked-in payloads instead of the live
endpoints — see `ingest/fixtures.py`. That's what CI does on pull requests.

Run:  uv run python -m ingest.pipeline
"""

from __future__ import annotations

import os
from collections.abc import Iterable

import dlt

from ingest import fixtures
from ingest.sources.gold import gold_prices_monthly
from lake.lakehouse import dlt_credentials


@dlt.source
def public_indicators():
    """Every resource as one dlt source."""
    return [gold_prices_monthly()]


# Drop and re-infer the schema of the resources being loaded, so a type or column
# change at the source is not masked by dlt's persisted, widen-only schema.
# `drop_sources` would also drop the resources a partial (Dagster) run skipped.
REFRESH = "drop_resources"

# `refresh` applies to a whole run, and it would drop an incremental resource's
# table and watermark. So the dispositions load in two calls: replace with
# `refresh`, merge without it.
FULL_REFRESH_RESOURCES: tuple[str, ...] = ("gold_prices_monthly",)
INCREMENTAL_RESOURCES: tuple[str, ...] = ()

# Which resources the orchestration layer partitions — a different question from
# which merge.
PARTITIONED_RESOURCES: tuple[str, ...] = ()


def load_groups(resources: Iterable[str] | None = None) -> list[tuple[list[str], dict]]:
    """The resource groups to load, in order, each with the `run()` kwargs it needs."""
    wanted = None if resources is None else set(resources)
    groups = []
    for names, kwargs in (
        (FULL_REFRESH_RESOURCES, {"refresh": REFRESH}),
        (INCREMENTAL_RESOURCES, {}),
    ):
        selected = [name for name in names if wanted is None or name in wanted]
        if selected:
            groups.append((selected, kwargs))
    return groups


# The dataset dlt loads into — the `raw` schema every landing table lands in.
PIPELINE_DATASET = "raw"


def pipeline_name() -> str:
    """`gold_warehouse`, or `gold_warehouse_fixtures` under fixtures."""
    return f"gold_warehouse{'_fixtures' if fixtures.enabled() else ''}"


def build_pipeline() -> dlt.Pipeline:
    """The one dlt pipeline definition, shared by the CLI and the Dagster assets."""
    os.environ.setdefault("NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_LOAD_ID", "true")
    return dlt.pipeline(
        pipeline_name=pipeline_name(),
        destination=dlt.destinations.ducklake(dlt_credentials()),
        dataset_name=PIPELINE_DATASET,
    )


def main() -> None:
    pipeline = build_pipeline()
    for names, kwargs in load_groups():
        print(pipeline.run(public_indicators().with_resources(*names), **kwargs))


if __name__ == "__main__":
    main()
