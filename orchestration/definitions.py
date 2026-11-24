"""Dagster entry point: `dagster dev` (see `[tool.dagster]` in pyproject.toml).

Everything `just run` does, as one asset graph, plus the Evidence site. The
assets and checks are `orchestration/defs/`, loaded whole; what is written out
here is what has no layer of its own. Two jobs:

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

import importlib
import pkgutil
from collections.abc import Iterable
from types import ModuleType

import dagster as dg
from dagster import in_process_executor

from orchestration import defs as defs_package
from orchestration.defs.site import EVIDENCE_SITE
from orchestration.resources import RESOURCES

site = dg.AssetSelection.assets(EVIDENCE_SITE)

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

# What the package loaders walk past. By class name over the MRO, because some
# (what `define_asset_job` and `build_schedule_from_partitioned_job` return) are
# not public names. `ResourceDefinition` covers `@io_manager` and `@resource`, and
# `ConfigurableResourceFactory` their class-based forms: an IO manager left here
# is the silent one, its key falling back to Dagster's default. `PartialResource`
# is what `configure_at_launch()` returns, a pydantic model with neither of those
# among its bases, and covers `PartialIOManager`. A resource *class* imported for
# an annotation is a metaclass instance and passes.
NOT_LOADED_FROM_DEFS = (
    "SensorDefinition",
    "ScheduleDefinition",
    "JobDefinition",
    "GraphDefinition",
    "RepositoryDefinition",
    "ResourceDefinition",
    "ConfigurableResourceFactory",
    "PartialResource",
    "LoggerDefinition",
    "ExecutorDefinition",
    "Definitions",
)


def refuse_what_the_loaders_skip(modules: Iterable[ModuleType]) -> None:
    """Fail the import on a sensor, schedule, job, graph, resource, IO manager,
    logger, executor, repository or `Definitions` in a defs module.

    The loaders below take assets, specs and checks and say nothing about the
    rest, so a schedule written in `orchestration/defs/` would never fire and
    `just validate` would pass. Here it stops the load and names the object.
    """
    skipped = [
        f"{module.__name__}.{name}"
        for module in modules
        for name, value in vars(module).items()
        if any(
            cls.__module__.startswith("dagster.") and cls.__name__.endswith(NOT_LOADED_FROM_DEFS)
            for cls in type(value).__mro__
        )
    ]
    if skipped:
        raise dg.DagsterInvalidDefinitionError(
            f"{', '.join(sorted(skipped))}: only assets, asset specs and asset checks load "
            "from orchestration/defs/. Define a sensor, schedule, job, resource, IO "
            "manager, logger or executor in orchestration/definitions.py and pass it to "
            "`Definitions` there: a graph as a job, through `.to_job()`, and a "
            "repository not at all, since that `Definitions` replaces it."
        )


refuse_what_the_loaders_skip(
    importlib.import_module(found.name)
    for found in pkgutil.walk_packages(defs_package.__path__, f"{defs_package.__name__}.")
)

defs = dg.Definitions(
    # Every asset, asset spec and check a module under `orchestration/defs/` holds
    # at module level, found by import: there is no list here for a new one to be
    # missing from. Nothing else loads from there, and the check above refuses
    # what would be skipped, so the jobs, schedule and resources are written out
    # here. The package loaders rather than `dg.load_from_defs_folder`, which
    # needs a `[tool.dg]` table this project does not have, and which returns a
    # defs module's own stray `dg.Definitions` in place of every asset and check
    # beside it, with no error (measured in the repo this template was cut
    # from); two stable walks rather than the `@preview`
    # `load_definitions_from_package_module`.
    assets=dg.load_assets_from_package_module(defs_package, include_specs=True),
    asset_checks=dg.load_asset_checks_from_package_module(defs_package),
    jobs=[full_refresh_job, publish_site_job],
    schedules=[daily_schedule],
    resources=RESOURCES,
    # DuckDB takes one writer. The default multiprocess executor would run steps
    # side by side and lose the race for the lock; one process serialises them.
    executor=in_process_executor,
)
