"""What the graph `orchestration/definitions.py` builds must hold.

Every asset and check under `orchestration/defs/` is loaded by import, so there
is no list for one to be missing from; what can still drop them is
`Definitions` not being handed what the loaders found, which one test below
catches for the checks. What is written by hand is the jobs' selections, and
reachable through them is what runs.
"""

from __future__ import annotations

import dagster as dg
import pytest

from orchestration.resources import dbt_project

# `just test` runs before `dbt deps && dbt parse` in ci.yml, and importing
# `orchestration.defs` needs the manifest that parse writes. CI re-runs this
# file after the parse step; skipping keeps the unit-test tier importable in a
# fresh clone rather than failing it for a missing build artifact.
pytestmark = pytest.mark.skipif(
    not dbt_project.manifest_path.exists(),
    reason="needs dbt/target/manifest.json — run `just dbt-deps` and `dbt parse` first",
)

# The dlt-pipeline-deactivation fixture this file needs (loading the definitions
# imports orchestration.defs.ingest, which leaves a dlt pipeline active
# process-wide) lives in `tests/conftest.py`.


@dg.op
def _op():
    pass


@dg.graph
def _graph():
    _op()


@dg.repository
def _repository():
    return []


@dg.io_manager
def _io_manager(_):
    return dg.InMemoryIOManager()


class _Resource(dg.ConfigurableResource):
    pass


@dg.logger
def _logger(_):
    pass


@pytest.mark.parametrize(
    "stray",
    [
        dg.ScheduleDefinition(name="s", cron_schedule="0 1 * * *", target=dg.AssetSelection.all()),
        dg.define_asset_job("j", selection=dg.AssetSelection.all()),
        _graph,
        _repository,
        _io_manager,
        dg.FilesystemIOManager(),
        dg.ResourceDefinition.hardcoded_resource(1),
        _Resource.configure_at_launch(),
        _logger,
        dg.in_process_executor,
        dg.Definitions(),
    ],
    ids=[
        "schedule",
        "job",
        "graph",
        "repository",
        "io_manager",
        "io_manager_class_based",
        "resource",
        "resource_configured_at_launch",
        "logger",
        "executor",
        "definitions",
    ],
)
def test_a_defs_module_holding_what_the_loaders_skip_fails_the_load(stray):
    """The package loaders take assets, specs and checks, and skip the rest silently.

    A schedule written in `orchestration/defs/` would never fire with `just
    validate` green, so `definitions.py` raises on one. The real package passing
    is every other test here; this is the half that has to raise.
    """
    from types import ModuleType

    from orchestration.definitions import refuse_what_the_loaders_skip

    module = ModuleType("orchestration.defs.example")
    vars(module).update(fine=dg.AssetSpec("a"), stray=stray)
    with pytest.raises(dg.DagsterInvalidDefinitionError, match=r"example\.stray: only assets"):
        refuse_what_the_loaders_skip([module])


def test_the_refusal_reads_the_package_init_too(monkeypatch):
    """The loaders read `orchestration/defs/__init__.py` before the modules under
    it, so a schedule left there is skipped like one in `defs/ingest.py`. Walking
    only the modules under the package would let that one through."""
    from orchestration import defs as defs_package
    from orchestration.definitions import defs_modules, refuse_what_the_loaders_skip

    stray = dg.ScheduleDefinition(
        name="s", cron_schedule="0 1 * * *", target=dg.AssetSelection.all()
    )
    monkeypatch.setattr(defs_package, "stray", stray, raising=False)
    with pytest.raises(dg.DagsterInvalidDefinitionError, match=r"orchestration\.defs\.stray: only"):
        refuse_what_the_loaders_skip(defs_modules())


def test_every_asset_check_written_in_defs_is_in_the_graph():
    """`test_asset_checks.py` calls the check bodies directly, so it passes with
    no check registered at all; and `just validate` passes on a `Definitions`
    without its `asset_checks=` line. What the modules hold has to be in the
    graph, which also holds every dbt test as a check of its own."""
    from orchestration.definitions import defs, defs_modules

    written = {
        key
        for module in defs_modules()
        for value in vars(module).values()
        if isinstance(value, dg.AssetChecksDefinition)
        for key in value.check_keys
    }
    assert written, "no asset check found under orchestration/defs/"
    missing = written - set(defs.resolve_asset_graph().asset_check_keys)
    assert not missing, f"written in orchestration/defs/ but not in the graph: {sorted(missing)}"


def test_every_raw_resource_has_an_asset_description():
    """`RAW_DESCRIPTIONS` re-enumerates the dlt resources, and cannot be derived:
    the prose is not computable from the source, so it is held to it instead.

    `orchestration/defs/ingest.py` reads it as `RAW_DESCRIPTIONS.get(name)`, which returns `None`
    for an unlisted resource. The asset then materialises with no description
    and nothing is red: the Dagster UI shows a blank where every sibling has a
    sentence.

    Lives here rather than beside the source's own tests because reading the dict
    means importing `orchestration.defs`, which needs dagster (an optional
    group) and the manifest; this module already carries that skip and is
    re-run by CI after `dbt parse`.
    """
    from ingest import pipeline
    from orchestration.defs.ingest import RAW_DESCRIPTIONS

    resources = {r.name for r in pipeline.public_indicators().resources.values()}

    # Both directions, as separate assertions rather than one set equality: they
    # catch different bugs and the failure message should say which happened.
    undescribed = resources - RAW_DESCRIPTIONS.keys()
    assert not undescribed, (
        "dlt resources with no entry in orchestration/defs/ingest.py RAW_DESCRIPTIONS "
        f"(they materialise with no description): {sorted(undescribed)}"
    )

    # The reverse direction is the one nothing else could surface. `.get()`
    # never consults a key no resource matches, so a stale entry left by a
    # rename is invisible — where a *missing* one at least shows as a blank.
    orphaned = RAW_DESCRIPTIONS.keys() - resources
    assert not orphaned, (
        "RAW_DESCRIPTIONS entries naming no dlt resource — renamed or removed "
        f"upstream and left behind here: {sorted(orphaned)}"
    )

    # A key check alone is satisfied by an empty string, which renders as the
    # same blank the missing key does. The floor is deliberately a length rather
    # than truthiness: `" "` is falsy nowhere and blank everywhere.
    blank = sorted(name for name, text in RAW_DESCRIPTIONS.items() if not text.strip())
    assert not blank, f"RAW_DESCRIPTIONS entries that render blank: {blank}"


def _job_keys(name: str) -> set[dg.AssetKey]:
    """The assets a job actually *materializes*.

    Not `get_all_asset_keys()` — a job's graph also carries the upstream assets
    it only reads, as unexecutable nodes. A raw asset excluded from a job still
    appears in it that way, because the dbt models depend on it, so the wider
    set would make an exclusion test pass while asserting nothing.
    """
    from orchestration.definitions import defs

    return set(defs.resolve_job_def(name).asset_layer.asset_graph.executable_asset_keys)


def test_the_jobs_between_them_cover_every_asset():
    """Registered is not the same as reachable, and the second one is what runs.

    An asset excluded from `full_refresh` — because it needs Node, or because it
    is partitioned (below) — has to be paid for by another job rather than
    dropped. Anything in no job is built by no workflow, and nothing else says
    so: `dagster definitions validate` passes on it.
    """
    from orchestration.definitions import defs
    from orchestration.defs.site import EVIDENCE_SITE

    defined = set(defs.resolve_asset_graph().executable_asset_keys)
    covered = _job_keys("full_refresh")

    assert defined - covered == {EVIDENCE_SITE}, (
        "the Evidence site is the only asset no pure-Python job may build; "
        f"unreachable: {sorted(k.to_user_string() for k in defined - covered)}"
    )
    assert EVIDENCE_SITE in _job_keys("publish_site")


def test_snapshot_expiry_follows_every_other_asset_in_full_refresh():
    """Expiry deletes from the only copy of every landing table, so it must run
    only once the load has proved itself: a step whose upstream failed is
    skipped, and nothing else stops it. Placed on the raw assets alone it ran
    second, straight after the load, and expired on through a failed dbt build.
    """
    from orchestration.definitions import defs
    from orchestration.defs.ingest import snapshot_expiry

    expiry = snapshot_expiry.key
    graph = defs.resolve_job_def("full_refresh").asset_layer.asset_graph
    upstream = dg.AssetSelection.assets(expiry).upstream().resolve(graph)
    not_before = _job_keys("full_refresh") - upstream
    assert not not_before, (
        "these assets can run after lakehouse expiry, or fail without stopping it: "
        f"{sorted(k.to_user_string() for k in not_before)}"
    )


def test_the_routine_jobs_are_not_partitioned():
    """A partitioned job's Materialize button in the Dagster UI launches a
    backfill, and its dialog has no "no partition" choice: only the Launchpad
    runs the job plain.

    A job takes its partitions definition from its assets, so one partitioned
    asset joining these selections brings that on with nothing else red — the
    selection raises nothing and `dagster definitions validate` passes. In the
    repo this template was cut from, yearly partitions on two API sources made
    the button a load of every year since 1960, cancelled after ten minutes,
    where the same job from the Launchpad finished in 1m38s (2026-09-13).
    """
    from orchestration.definitions import defs

    for name in ("full_refresh", "publish_site"):
        partitions = defs.resolve_job_def(name).partitions_def
        assert partitions is None, (
            f"`{name}` is partitioned ({type(partitions).__name__}), so its Materialize "
            "button in the Dagster UI is a backfill of every partition. Give the "
            "partitioned asset a job of its own and subtract it from this selection, "
            "or take its backfill window as run config (ingest/pipeline.py says when)."
        )


def test_the_dbt_build_writes_its_run_results_where_the_reader_looks():
    """`dbt_models` pins the target path, and `pipeline_runs` depends on it.

    Left to itself dagster-dbt gives every invocation a unique target directory
    (`target/<op>-<run id>-<uuid>/`) so concurrent invocations cannot overwrite
    each other's artifacts. Nothing here is concurrent, and
    `observability.build_runs` reads `run_results.json` *by path*, so without the
    pin every orchestrated build writes `analytics.pipeline_runs` with zero rows.

    This asserts the *call site* rather than the artifact: a real invocation
    pointed at an explicit path proves dagster-dbt honours the argument, and
    stays green when the argument is dropped.
    """
    from pathlib import Path
    from typing import Any

    from modern_data_stack.paths import dbt_run_results_path, dbt_target_path
    from orchestration.defs import models

    called_with: list[str] = []
    kwargs_seen: dict[str, Any] = {}

    class _Invocation:
        def stream(self):
            return iter(())

    class _Dbt:
        def cli(self, args, **kwargs):
            called_with.extend(args)
            kwargs_seen.update(kwargs)
            return _Invocation()

    # `decorated_fn` is the undecorated generator, so this exercises the call
    # site with no execution harness: nothing is materialized and no output is
    # yielded, which is exactly the part being asserted. Dagster types
    # `compute_fn` as a union that does not narrow to the decorated half, so the
    # annotation states the gap rather than adding a `ty: ignore`.
    compute: Any = models.dbt_models.op.compute_fn
    list(compute.decorated_fn(context=None, dbt=_Dbt()))

    assert called_with == ["build"]
    target = kwargs_seen["target_path"]
    assert Path(target) == Path(dbt_target_path())
    # The property that actually matters, stated as the two paths agreeing: the
    # artifact the build writes is the one `transform.pipeline_status` reads.
    assert Path(target) / "run_results.json" == Path(dbt_run_results_path())
