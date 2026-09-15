"""The lakehouse: dlt's landing zone, and the revision log derived from it.

dlt writes the catalog directly, so what is left to guard is the substitute for
DuckLake's change feed.

**Why there is a substitute at all** is the finding these tests exist to hold.
`ducklake_table_changes()` is the obvious answer and it does not work behind
dlt: reloading identical rows through `write_disposition="merge"` reports every
one of them as an update, because dlt regenerates `_dlt_id` *and* `_dlt_load_id`
on every row it touches. The feed is faithful and the writer is what makes it
useless. `revisions()` diffs two snapshots with `EXCEPT` instead, projecting
those columns away.

The failure that matters is not an exception. Drop a column from the ignore list
and the diff returns *every* row as revised — a plausible number, in the right
shape, that reads as a catastrophic upstream restatement. So the tests here
assert the zero as hard as they assert the one.
"""

from __future__ import annotations

import duckdb
import pytest

from modern_data_stack.ducklake import attach, revisions, table_versions
from lake import lakehouse

TABLE = "raw.gold_prices_monthly"

# Two months. Small enough to read, and two rows is the minimum that can
# distinguish "one row changed" from "everything changed".
LOAD = [("2021-11", "2021-11-01", 1_820.0), ("2021-12", "2021-12-01", 1_795.0)]


def _write(lake_dir, loads: list[list[tuple]]) -> None:
    """Write the landing table once per entry in `loads`.

    Every write stamps fresh `_dlt_load_id`/`_dlt_id` values, which is what dlt
    does on every merge and the whole reason the diff has to ignore them. Plain
    SQL rather than a dlt run: what is under test is the diff, and a loader in
    the loop would make these tests about dlt's merge instead.
    """
    (lake_dir / "data").mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    attach(con, lake_dir / "catalog.duckdb", lake_dir / "data", alias="lakehouse")
    con.execute("create schema if not exists lakehouse.raw")
    try:
        for n, rows in enumerate(loads):
            values = ", ".join(
                f"('{label}', date '{day}', {price}, 'load_{n}', 'id_{n}_{i}')"
                for i, (label, day, price) in enumerate(rows)
            )
            con.execute(f"drop table if exists lakehouse.{TABLE}")
            con.execute(
                f"create table lakehouse.{TABLE} as select * from (values {values}) as t"
                "(date, month_start, price, _dlt_load_id, _dlt_id)"
            )
    finally:
        con.close()


def _connect(lake_dir):
    con = duckdb.connect()
    attach(con, lake_dir / "catalog.duckdb", lake_dir / "data", alias="lakehouse", read_only=True)
    return con


def test_an_identical_reload_yields_no_revisions(tmp_path):
    """The routine case, and the one the change feed gets wrong.

    Every ingest re-merges the whole lookback window whether the publisher
    restated anything or not. If that reads as a revision per row the log is
    noise, which is precisely what `ducklake_table_changes()` reports here.
    """
    _write(tmp_path, [LOAD, LOAD])
    con = _connect(tmp_path)
    try:
        versions = table_versions(con, "lakehouse", TABLE)
        assert len(versions) >= 2
        changed = revisions(
            con, "lakehouse", TABLE, versions[-2], versions[-1], ignore=lakehouse.DLT_COLUMNS
        )
    finally:
        con.close()
    assert changed == []


def test_one_restated_value_yields_exactly_that_row(tmp_path):
    _write(tmp_path, [LOAD, [("2021-11", "2021-11-01", 1_806.5), *LOAD[1:]]])
    con = _connect(tmp_path)
    try:
        versions = table_versions(con, "lakehouse", TABLE)
        changed = revisions(
            con, "lakehouse", TABLE, versions[-2], versions[-1], ignore=lakehouse.DLT_COLUMNS
        )
    finally:
        con.close()
    assert len(changed) == 1
    assert changed[0][0] == "2021-11"
    assert changed[0][2] == 1_806.5


def test_forgetting_the_provenance_columns_reports_the_whole_table(tmp_path):
    """The mutation that proves the ignore list is load-bearing.

    This is the bug the design exists to avoid, run deliberately: compare
    without ignoring anything and an identical reload reports both rows. It
    raises nothing and returns nothing malformed — a wrong answer of the right
    shape.
    """
    _write(tmp_path, [LOAD, LOAD])
    con = _connect(tmp_path)
    try:
        versions = table_versions(con, "lakehouse", TABLE)
        unfiltered = revisions(con, "lakehouse", TABLE, versions[-2], versions[-1], ignore=())
        filtered = revisions(
            con, "lakehouse", TABLE, versions[-2], versions[-1], ignore=lakehouse.DLT_COLUMNS
        )
    finally:
        con.close()
    assert len(unfiltered) == len(LOAD)
    assert filtered == []


def test_ignoring_every_column_is_refused_rather_than_answered(tmp_path):
    """`ignore` covering the whole table would compare nothing and return nothing
    — indistinguishable from "no revisions" and wrong in the safe-looking
    direction. It raises instead."""
    _write(tmp_path, [LOAD])
    con = _connect(tmp_path)
    try:
        versions = table_versions(con, "lakehouse", TABLE)
        all_columns = ("date", "month_start", "price", *lakehouse.DLT_COLUMNS)
        with pytest.raises(ValueError, match="no columns left"):
            revisions(con, "lakehouse", TABLE, versions[0], None, ignore=all_columns)
    finally:
        con.close()


def test_a_table_the_catalog_does_not_hold_is_named_in_the_error(tmp_path):
    _write(tmp_path, [LOAD])
    con = _connect(tmp_path)
    try:
        with pytest.raises(ValueError, match="raw.not_a_table"):
            revisions(con, "lakehouse", "raw.not_a_table", 0, None, ignore=())
    finally:
        con.close()


def test_an_unqualified_table_name_is_refused(tmp_path):
    _write(tmp_path, [LOAD])
    con = _connect(tmp_path)
    try:
        with pytest.raises(ValueError, match="schema-qualified"):
            revisions(con, "lakehouse", "gold_prices_monthly", 0, None, ignore=())
    finally:
        con.close()


def test_the_attach_alias_is_the_database_dbt_declares():
    """dbt's `_sources.yml` says `database: lakehouse` and `profiles.yml` attaches
    under that alias. Both are the constant here; a change to one of the three
    that misses the others means dbt cannot resolve a single source."""
    from pathlib import Path

    import yaml

    sources = yaml.safe_load(Path("dbt/models/staging/_sources.yml").read_text())
    raw = next(s for s in sources["sources"] if s["name"] == "raw")
    assert raw["database"] == lakehouse.ATTACH_ALIAS

    profile = yaml.safe_load(Path("dbt/profiles.yml").read_text())
    attached = profile["my_warehouse"]["outputs"]["dev"]["attach"]
    assert [a["alias"] for a in attached] == [lakehouse.ATTACH_ALIAS]
