"""Attach a DuckLake catalog, read what changed between two snapshots, expire old ones.

DuckLake is a table format: plain Parquet under a data directory, plus a
catalog database holding schema, snapshot lineage and per-file statistics. This
module is the domain-neutral half — attaching one, listing its snapshots,
diffing a table across two of them, and expiring the snapshots nothing needs.
What lives in the lakehouse, where it sits and what it keeps is
`lake/lakehouse.py`.

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

from collections.abc import Mapping
from pathlib import Path

import duckdb

from .db import scalar

__all__ = [
    "attach",
    "expire",
    "revisions",
    "row_count",
    "snapshots",
    "sql_identifier",
    "sql_literal",
    "storage",
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
    storage_secret: Mapping[str, str] | None = None,
) -> None:
    """Attach the DuckLake at `catalog_path` as `alias`, and its catalog beside it.

    `data_path` is passed although the catalog records it, because DuckLake
    checks the two agree and refuses a mismatch (a moved lakehouse).

    DuckLake also attaches the catalog database, as `meta_alias(alias)`.
    `table_versions` reads it, because the query surface cannot say which
    snapshots changed one table; the catalog schema is part of the DuckLake 1.0
    spec, not an internal.

    A `data_path` URL (`s3://…`) is kept as a string, because `Path` collapses
    `s3://` to `s3:/`. `storage_secret` — `key_id`, `secret`, `endpoint` (host
    and port), `use_ssl` (`"true"`/`"false"`), `region` — is created first as an
    S3 secret scoped to that URL, path-style as S3-compatible stores expect.
    DuckDB reads no endpoint from the environment, so every connection needs it.
    """
    con.execute("install ducklake")
    con.execute("load ducklake")

    if "://" in str(data_path):
        data_path_sql = str(data_path).rstrip("/") + "/"
    else:
        data_path_sql = f"{Path(data_path)}/"
    if storage_secret is not None:
        use_ssl = "true" if str(storage_secret["use_ssl"]).lower() == "true" else "false"
        # Installed as well as loaded: a fresh machine has no httpfs until
        # something downloads it, and a bare `load` fails there.
        con.execute("install httpfs")
        con.execute("load httpfs")
        # No bind parameters here either, so the values are quoted literals.
        con.execute(
            f"create or replace secret {alias}_storage (type s3, "
            f"key_id {sql_literal(storage_secret['key_id'])}, "
            f"secret {sql_literal(storage_secret['secret'])}, "
            f"endpoint {sql_literal(storage_secret['endpoint'])}, use_ssl {use_ssl}, "
            f"region {sql_literal(storage_secret['region'])}, url_style 'path', "
            f"scope {sql_literal(data_path_sql)})"
        )

    options = [f"data_path {sql_literal(data_path_sql)}"]
    if read_only:
        options.append("read_only")
    if data_inlining_row_limit is not None:
        options.append(f"data_inlining_row_limit {int(data_inlining_row_limit)}")

    # ATTACH takes literals, not bind parameters — `attach $path` is a parser
    # error — so the paths are quoted literals, which a `'` in a directory name
    # would otherwise end early.
    catalog_sql = sql_literal(f"ducklake:duckdb:{Path(catalog_path)}")
    con.execute(f"attach {catalog_sql} as {alias} ({', '.join(options)})")


def snapshots(con: duckdb.DuckDBPyConnection, alias: str) -> list[int]:
    """Every snapshot id in the catalog, oldest first."""
    return [
        row[0] for row in con.execute(f"select snapshot_id from {alias}.snapshots()").fetchall()
    ]


def table_versions(con: duckdb.DuckDBPyConnection, alias: str, table: str) -> list[int]:
    """Snapshots in which `table` actually changed, oldest first.

    A dlt load writes several snapshots (staging, merge, cleanup), so most say
    nothing about a given table; this is what makes "the previous version" mean
    the previous version of *this* table. **They are writes, not loads**: a
    writer can fill one table across several snapshots, and every one but its
    last is a partial state. Grouping them into loads takes the writer's own
    record of where a load ended, which is the caller's (`lake.lakehouse`).
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
    sources += [f"select begin_snapshot from {meta}.{sql_identifier(row[0])}" for row in inlined]

    # A live file can begin at an expired snapshot; the change is readable from
    # the next surviving one. Dropping the id would end the list a change early.
    rows = con.execute(
        f"""
        select distinct (
            select min(s.snapshot_id) from {meta}.ducklake_snapshot s
            where s.snapshot_id >= v.begin_snapshot
        ) as version
        from ({" union all ".join(sources)}) v
        order by 1
        """
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


def expire(
    con: duckdb.DuckDBPyConnection,
    alias: str,
    before: int | None,
    delete_orphans: bool,
    orphan_grace: str = "1 day",
) -> dict[str, int]:
    """Expire every snapshot older than `before`, then delete the files only they read.

    `before` survives, so a diff from it still reads; None expires nothing but
    still deletes files that earlier expiries left. Orphans are Parquet the
    catalog never recorded, such as a crashed load's; `delete_orphans` lists the
    whole data path, so pass it only for a path this catalog owns outright, and
    `orphan_grace` spares a write still in flight.
    """
    expired = [] if before is None else [s for s in snapshots(con, alias) if s < before]
    if expired:
        ids = ", ".join(str(int(i)) for i in expired)
        con.execute(f"call ducklake_expire_snapshots({sql_literal(alias)}, versions => [{ids}])")
    cleaned = con.execute(
        f"call ducklake_cleanup_old_files({sql_literal(alias)}, cleanup_all => true)"
    ).fetchall()
    orphans = []
    if delete_orphans:
        orphans = con.execute(
            f"call ducklake_delete_orphaned_files({sql_literal(alias)}, "
            f"older_than => now() - interval {sql_literal(orphan_grace)})"
        ).fetchall()
    return {"snapshots": len(expired), "files": len(cleaned), "orphans": len(orphans)}


def storage(con: duckdb.DuckDBPyConnection, alias: str) -> dict[str, int]:
    """Bytes of the files the catalog records, and of those the current snapshot reads.

    The difference is what `expire` can free. Orphans are not recorded, so not counted.
    """
    meta = meta_alias(alias)
    total, live = con.execute(
        f"""
        select coalesce(sum(file_size_bytes), 0),
               coalesce(sum(file_size_bytes) filter (where end_snapshot is null), 0)
        from (
            select file_size_bytes, end_snapshot from {meta}.ducklake_data_file
            union all
            select file_size_bytes, end_snapshot from {meta}.ducklake_delete_file
        )
        """
    ).fetchall()[0]
    return {"bytes": int(total), "live_bytes": int(live)}


def sql_literal(value: str | Path) -> str:
    """A SQL string literal, for the statements that take no bind parameters.

    Public so that every ATTACH or `call` built around a path or a name quotes it
    the same way: a value quoted in one place and interpolated raw in another is
    the bug a second copy of these two lines produces.
    """
    return "'" + str(value).replace("'", "''") + "'"


def sql_identifier(name: str) -> str:
    """A quoted SQL identifier, for names read out of the catalog rather than typed."""
    return '"' + str(name).replace('"', '""') + '"'


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
