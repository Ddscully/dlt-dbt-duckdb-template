"""Dagster entry point: `dagster dev` (see `[tool.dagster]` in pyproject.toml).

Everything `just run` does, as one asset graph, plus the Evidence site. Two jobs:

* `full_refresh` — everything bar the site. Pure Python, so every workflow can
  run it without Node; the daily schedule targets it.
* `publish_site` — `full_refresh` plus `reports/evidence_site`, which shells out
  to npm.

`full_refresh` is defined by *exclusion* (`AssetSelection.all() - site`), so a
new asset joins it automatically. Two things have to be excluded by hand: a
second asset that needs Node, and a **partitioned** one. A job takes its
partitions definition from its assets, so one partitioned asset partitions the
whole job with nothing raised, and a partitioned job's Materialize button in the
UI launches a backfill of every partition — its dialog has no "no partition"
choice. `tests/test_definitions.py` fails on that. It is when a project grows a
third job, named for the load rather than the op, because jobs and ops share a
namespace; and it is worth doing only where every partition together is a
routine-sized run (`ingest/pipeline.py`).
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
    # hitting public APIs on a timer just because someone opened the UI. Once
    # started, a daemon that was down at 06:00 UTC launches that tick's run as
    # soon as it comes back (the latest missed tick only), so a Materialize click
    # just after `just dagster` starts is the case `.dagster/dagster.yaml`'s
    # one-run queue exists for.
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
