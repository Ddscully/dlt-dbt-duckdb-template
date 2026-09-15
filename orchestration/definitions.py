"""Dagster entry point: `dagster dev` (see `[tool.dagster]` in pyproject.toml).

Everything `just run` does, as one asset graph, plus the Evidence site. Three jobs:

* `load_retail` — the month-partitioned retail load. `define_asset_job` resolves
  a selection to one `partitions_def` or raises, with no opt-out for a named job,
  and retail is monthly where `raw/wb_wdi` and `raw/om_weather_daily` are yearly.
* `full_refresh` — everything else bar the site. Pure Python, so `ci.yml`,
  `nightly.yml` and `release-data.yml` run it without Node; the daily schedule
  targets it.
* `publish_site` — `full_refresh` plus `reports/evidence_site`, which shells out
  to npm. `pages.yml` runs it.

`load_retail` must run first, because `dbt build` reads
`raw.retail_invoice_lines`; the justfile recipes and all four workflows pair the
jobs. It is not called `ingest_retail` because jobs share a namespace with ops,
and the retail `@dlt_assets` op has that name.

Both selections name what they exclude, so a new asset joins `full_refresh`
automatically; a second npm-shaped or differently-partitioned asset has to be
excluded by hand.
"""

from __future__ import annotations

import dagster as dg
from dagster import in_process_executor

from orchestration import assets
from orchestration.resources import RESOURCES

site = dg.AssetSelection.assets(assets.EVIDENCE_SITE)

full_refresh_job = dg.define_asset_job(
    name="full_refresh",
    selection=dg.AssetSelection.all() - site,
    description=(
        "Ingest every source, rebuild the dbt models, recompute derived metrics. "
        "Excludes the Evidence site, which needs Node."
    ),
)

publish_site_job = dg.define_asset_job(
    name="publish_site",
    selection=dg.AssetSelection.all(),
    description="`full_refresh`, then build the Evidence site from it. Requires Node >= 18.",
)

daily_schedule = dg.ScheduleDefinition(
    name="daily_refresh",
    job=full_refresh_job,
    cron_schedule="0 6 * * *",
    execution_timezone="UTC",
    # Off by default: this is a demo repo, and `dagster dev` shouldn't start
    # hitting public APIs on a timer just because someone opened the UI.
    default_status=dg.DefaultScheduleStatus.STOPPED,
)

defs = dg.Definitions(
    # Listed explicitly: an asset defined in `assets.py` but missing here is not
    # in the graph at all, and `AssetSelection.all()` won't tell you.
    assets=[
        assets.raw_assets,
        assets.dbt_models,
        assets.gold_price_trend,
        assets.pipeline_status,
        assets.evidence_site,
    ],
    asset_checks=[
        assets.run_history_records_this_build,
        assets.site_pages_all_rendered,
    ],
    jobs=[full_refresh_job, publish_site_job],
    schedules=[daily_schedule],
    resources=RESOURCES,
    # DuckDB takes one writer. The default multiprocess executor would run steps
    # side by side and lose the race for the lock; one process serialises them.
    executor=in_process_executor,
)
