"""Attach a DuckLake catalog, and read what changed between two snapshots.

DuckLake is a table format: plain Parquet under a data directory, plus a
catalog database holding schema, snapshot lineage and per-file statistics. This
module is the domain-neutral half — attaching one, listing its snapshots, and
diffing a table across two of them. What lives in the lakehouse, and where it
sits, is `lake/lakehouse.py`.

## Why the diff is here rather than `ducklake_table_changes()`

DuckLake's change feed is faithful to the writer, and useless when the writer
rewrites unchanged rows — dlt regenerates `_dlt_id` and `_dlt_load_id` on every
merge, so 500 identical rows reloaded report 500 updates. `revisions()` compares
two versions with `EXCEPT` instead, projecting away the columns the caller names
as provenance. `ignore` names columns the writer owns, so the caller supplies it.

## `read_parquet` over the data directory is not the table

At the default `data_inlining_row_limit` of 10 a small change is written into
the catalog database, so the files return the superseded value. At 0 it reaches
Parquet, but with a `…-delete.parquet` of `(file_path, pos)` that breaks a glob's
schema — or, excluded by name, returns both versions of the row. Read through
the catalog.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from .db import scalar

__all__ = [
    "attach",
    "revisions",
    "row_count",
    "snapshots",
    "table_versions",
]


def meta_alias(alias: str) -> str:
    """The (undocumented) name DuckLake attaches the catalog database under.

    Attaching the catalog file again under another name is refused as a
    `Unique file handle conflict`; it is already attached under this one.
    """
    return f"__ducklake_metadata_{alias}"


def attach(
    con: duckdb.DuckDBPyConnection,
    catalog_path: str | Path,
    data_path: str | Path,
    alias: str,
    read_only: bool = False,
    data_inlining_row_limit: int | None = None,
) -> None:
    """Attach the DuckLake at `catalog_path` as `alias`, and its catalog beside it.

    `data_path` is passed although the catalog records it, because DuckLake
    checks the two agree and refuses a mismatch (a moved lakehouse).

    DuckLake also attaches the catalog database, as `meta_alias(alias)`.
    `table_versions` reads it, because the query surface cannot say which
    snapshots changed one table; the catalog schema is part of the DuckLake 1.0
    spec, not an internal.
    """
    con.execute("install ducklake")
    con.execute("load ducklake")

    options = [f"data_path '{Path(data_path)}/'"]
    if read_only:
        options.append("read_only")
    if data_inlining_row_limit is not None:
        options.append(f"data_inlining_row_limit {int(data_inlining_row_limit)}")

    # ATTACH takes literals, not bind parameters — `attach $path` is a parser
    # error — so the paths are interpolated rather than bound as parameters.
    con.execute(f"attach 'ducklake:duckdb:{Path(catalog_path)}' as {alias} ({', '.join(options)})")


def snapshots(con: duckdb.DuckDBPyConnection, alias: str) -> list[int]:
    """Every snapshot id in the catalog, oldest first."""
    return [
        row[0] for row in con.execute(f"select snapshot_id from {alias}.snapshots()").fetchall()
    ]


def table_versions(con: duckdb.DuckDBPyConnection, alias: str, table: str) -> list[int]:
    """Snapshots in which `table` actually changed, oldest first.

    A dlt load writes several snapshots (staging, merge, cleanup), so most say
    nothing about a given table; this is what makes "the previous version" mean
    the previous version of *this* table.
    """
    schema, name = _split(table)
    meta = meta_alias(alias)
    ids = [
        row[0]
        for row in con.execute(
            f"""
            select t.table_id
            from {meta}.ducklake_table t
            join {meta}.ducklake_schema s on s.schema_id = t.schema_id
            where t.table_name = $table and s.schema_name = $schema
            """,
            {"table": name, "schema": schema},
        ).fetchall()
    ]
    if not ids:
        return []
    id_list = ", ".join(str(int(i)) for i in ids)

    # Files *and* inlined data: a change of `data_inlining_row_limit` rows or
    # fewer (default 10) is written into the catalog, leaving no
    # `ducklake_data_file` row, and a file-only list silently skips that load.
    sources = [
        f"select begin_snapshot from {meta}.ducklake_data_file where table_id in ({id_list})"
    ]
    inlined = con.execute(
        f"select table_name from {meta}.ducklake_inlined_data_tables where table_id in ({id_list})"
    ).fetchall()
    sources += [f'select begin_snapshot from {meta}."{row[0]}"' for row in inlined]

    rows = con.execute(
        f"select distinct begin_snapshot from ({' union all '.join(sources)}) order by 1"
    ).fetchall()
    return [row[0] for row in rows]


def revisions(
    con: duckdb.DuckDBPyConnection,
    alias: str,
    table: str,
    since: int,
    until: int | None = None,
    ignore: tuple[str, ...] = (),
) -> list[tuple]:
    """Rows of `table` at `until` that are not present, identically, at `since`.

    Inserts and updates alike — what the table says now that it did not then.
    Deletions are not returned; swap the arguments for those.
    """
    columns = [c for c in _columns(con, alias, table) if c not in ignore]
    if not columns:
        raise ValueError(f"{table} has no columns left to compare after ignoring {ignore}")
    projection = ", ".join(f'"{c}"' for c in columns)

    at_until = "" if until is None else f" at (version => {until})"
    return con.execute(
        f"""
        select {projection} from {alias}.{table}{at_until}
        except
        select {projection} from {alias}.{table} at (version => {since})
        """
    ).fetchall()


def _split(table: str) -> tuple[str, str]:
    if table.count(".") != 1:
        raise ValueError(f"{table!r} is not a schema-qualified table name")
    schema, name = table.split(".")
    return schema, name


def _columns(con: duckdb.DuckDBPyConnection, alias: str, table: str) -> list[str]:
    schema, name = _split(table)
    rows = con.execute(
        """
        select column_name from information_schema.columns
        where table_catalog = $catalog and table_schema = $schema and table_name = $table
        order by ordinal_position
        """,
        {"catalog": alias, "schema": schema, "table": name},
    ).fetchall()
    if not rows:
        raise ValueError(f"{alias}.{table} does not exist")
    return [row[0] for row in rows]


def row_count(con: duckdb.DuckDBPyConnection, alias: str, table: str) -> int:
    return scalar(con, f"select count(*) from {alias}.{table}")
