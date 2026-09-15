"""The lakehouse: where dlt lands `raw`, and how to read what changed.

dlt writes straight into this DuckLake catalog, dbt reads `raw` from it, and
`data/warehouse.duckdb` holds only what dbt builds.

Run:  uv run python -m lake.lakehouse       (report the catalog's snapshots)

## Reading what changed

`ducklake_table_changes()` is useless behind dlt: dlt regenerates `_dlt_id` and
`_dlt_load_id` on every row it re-merges, so reloading 500 identical rows reports
500 updates. `revisions()` diffs two snapshots with `EXCEPT` instead, projecting
those columns away — measured at 0 rows for an identical reload and 1 for a
one-row change. It works between any two snapshots and needs no bookkeeping, so
it needs no bookkeeping of its own. The cost is two scans.

## Why the working paths are absolute

A DuckLake catalog stores its `data_path` as given and compares it as a string
on every attach. The catalog is read by dlt from the repo root and by dbt from
`dbt/`, so the path must be absolute or one directory becomes two strings and
the attach is refused — which is why `just` exports an absolute `LAKEHOUSE_DIR`.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from modern_data_stack.ducklake import (
    attach,
    revisions as diff_snapshots,
    row_count,
    snapshots,
    table_versions,
)
from modern_data_stack.paths import lakehouse_dir as default_lakehouse_dir

# LAKEHOUSE_DIR mirrors WAREHOUSE_PATH: the tests point it at a temp directory so
# a fixture run cannot write over the real catalog.
LAKEHOUSE_DIR = default_lakehouse_dir()

# The catalog is a DuckDB file beside `data/`, which holds only Parquet. (Reading
# that Parquet directly is not reading the table — see `modern_data_stack.ducklake`.)
CATALOG_NAME = "catalog.duckdb"
DATA_DIRNAME = "data"

# The ATTACH name, and therefore the catalog every piece of SQL in the project
# spells out. dbt's `_sources.yml` says `database: lakehouse`; changing this
# without changing that splits the graph exactly the way a renamed dlt resource
# does.
ATTACH_ALIAS = "lakehouse"

# dlt's per-row provenance, regenerated on every re-merge whether the data moved
# or not — see the module docstring. Every comparison here projects them away.
DLT_COLUMNS = ("_dlt_load_id", "_dlt_id")

__all__ = [
    "ATTACH_ALIAS",
    "CATALOG_NAME",
    "DATA_DIRNAME",
    "DLT_COLUMNS",
    "LAKEHOUSE_DIR",
    "catalog_path",
    "data_path",
    "dlt_credentials",
    "is_catalog",
    "main",
    "read_only_connection",
    "revisions",
    "rows",
    "run",
    "versions",
]


def catalog_path(lakehouse_dir: str | Path = LAKEHOUSE_DIR) -> Path:
    return Path(lakehouse_dir) / CATALOG_NAME


def data_path(lakehouse_dir: str | Path = LAKEHOUSE_DIR) -> Path:
    return Path(lakehouse_dir) / DATA_DIRNAME


def dlt_credentials(lakehouse_dir: str | Path = LAKEHOUSE_DIR):
    """The destination `ingest.pipeline` loads into.

    Built here rather than in `ingest/` so that the one place that knows where
    the lakehouse *is* is the one place that knows what it is called. dlt takes
    the catalog as a connection string and the storage as a URL; both are
    absolute for the reason in the module docstring.
    """
    from dlt.destinations.impl.ducklake.configuration import DuckLakeCredentials

    # Required: dlt will not create the catalog's parent directory. Because
    # importing `orchestration.assets` builds the pipeline, that import creates an
    # empty `data/lakehouse/` before anything has loaded — which `is_catalog`
    # exists to tell apart from a real one.
    lake = Path(lakehouse_dir)
    lake.mkdir(parents=True, exist_ok=True)
    data_path(lake).mkdir(parents=True, exist_ok=True)
    return DuckLakeCredentials(
        ducklake_name=ATTACH_ALIAS,
        catalog=f"duckdb:///{catalog_path(lake)}",
        storage=f"file://{data_path(lake)}",
    )


def is_catalog(lakehouse_dir: str | Path = LAKEHOUSE_DIR) -> bool:
    """Whether there is a readable DuckLake catalog here.

    **A file at the path is not enough**, which is not a hypothetical: dbt's
    `ATTACH IF NOT EXISTS` leaves an *empty DuckDB file* at `catalog.duckdb` on
    any build that runs before the first ingest, and a read-only attach of that
    fails with `Existing DuckLake at metadata catalog … does not exist - and
    creating a new DuckLake is explicitly disabled`. So the question is whether
    DuckLake's own metadata table is in it.
    """
    catalog = catalog_path(lakehouse_dir)
    if not catalog.exists():
        return False
    con = duckdb.connect(str(catalog), read_only=True)
    try:
        return bool(
            con.execute(
                "select 1 from duckdb_tables() where table_name = 'ducklake_metadata'"
            ).fetchone()
        )
    except duckdb.Error:
        return False
    finally:
        con.close()


def read_only_connection(lakehouse_dir: str | Path = LAKEHOUSE_DIR) -> duckdb.DuckDBPyConnection:
    """An in-memory DuckDB with the lakehouse attached read-only.

    Every caller is a reader. Read-only readers can share the catalog with each
    other, never with a writer — the same rule as the warehouse file.
    """
    con = duckdb.connect()
    attach(
        con,
        catalog_path(lakehouse_dir),
        data_path(lakehouse_dir),
        alias=ATTACH_ALIAS,
        read_only=True,
    )
    return con


def revisions(
    table: str,
    since: int,
    until: int | None = None,
    lakehouse_dir: str | Path = LAKEHOUSE_DIR,
) -> list[tuple]:
    """Rows of `table` that genuinely differ between two snapshots.

    `table` is schema-qualified (`raw.gold_prices_monthly`). Provenance columns are
    projected away, which is the whole point — see the module docstring.
    """
    con = read_only_connection(lakehouse_dir)
    try:
        return diff_snapshots(con, ATTACH_ALIAS, table, since, until, ignore=DLT_COLUMNS)
    finally:
        con.close()


def versions(table: str, lakehouse_dir: str | Path = LAKEHOUSE_DIR) -> list[int]:
    """Snapshots in which `table` changed, oldest first — the diffable points."""
    con = read_only_connection(lakehouse_dir)
    try:
        return table_versions(con, ATTACH_ALIAS, table)
    finally:
        con.close()


def rows(table: str, lakehouse_dir: str | Path = LAKEHOUSE_DIR) -> int:
    con = read_only_connection(lakehouse_dir)
    try:
        return row_count(con, ATTACH_ALIAS, table)
    finally:
        con.close()


def run(lakehouse_dir: str | Path = LAKEHOUSE_DIR) -> dict:
    """What the catalog currently holds: tables, rows and snapshot lineage.

    An empty result for a landing zone nothing has loaded yet. dlt creates the
    catalog on its first load, and a read-only attach of a path that is not one
    fails with `Cannot open database … in read-only mode`, so a fresh clone
    would otherwise get a traceback where "nothing here yet" is the answer.
    """
    if not is_catalog(lakehouse_dir):
        return {"tables": {}, "snapshots": []}
    con = read_only_connection(lakehouse_dir)
    try:
        tables = con.execute(
            f"""
            select table_schema, table_name
            from information_schema.tables
            where table_catalog = '{ATTACH_ALIAS}' and table_type = 'BASE TABLE'
            order by 1, 2
            """
        ).fetchall()
        counts = {
            f"{schema}.{name}": row_count(con, ATTACH_ALIAS, f"{schema}.{name}")
            for schema, name in tables
        }
        return {"tables": counts, "snapshots": snapshots(con, ATTACH_ALIAS)}
    finally:
        con.close()


def main() -> None:
    summary = run()
    snaps = summary["snapshots"]
    if not summary["tables"] and not snaps:
        print(f"{catalog_path()} — no catalog yet; run `just ingest`")
        return
    print(f"{catalog_path()} — {len(snaps)} snapshots, newest {snaps[-1] if snaps else '(none)'}")
    for table, rows in summary["tables"].items():
        print(f"  {table:40} {rows:>10,} rows")


if __name__ == "__main__":
    main()
