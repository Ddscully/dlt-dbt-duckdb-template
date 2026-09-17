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

## The Parquet in a bucket

`LAKEHOUSE_DATA_PATH=s3://bucket/prefix/` puts the data files on S3-compatible
storage; the catalog stays a local file either way. dlt, dbt and every reader
here then need the endpoint and keys on each connection (`storage_secret`).
"""

from __future__ import annotations

import os
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

# The Parquet can live in a bucket instead of `data/` — see the module docstring.
# The endpoint has its own variable because DuckDB reads none from the
# environment; the keys are the standard AWS pair. The region is a default most
# S3-compatible stores ignore and a signature still needs.
DATA_PATH_ENV_VAR = "LAKEHOUSE_DATA_PATH"
S3_ENDPOINT_ENV_VAR = "LAKEHOUSE_S3_ENDPOINT"
DEFAULT_S3_REGION = "us-east-1"

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
    "DATA_PATH_ENV_VAR",
    "DEFAULT_S3_REGION",
    "DLT_COLUMNS",
    "LAKEHOUSE_DIR",
    "S3_ENDPOINT_ENV_VAR",
    "catalog_path",
    "data_path",
    "dlt_credentials",
    "is_catalog",
    "main",
    "read_only_connection",
    "revisions",
    "rows",
    "run",
    "storage_secret",
    "versions",
]


def catalog_path(lakehouse_dir: str | Path = LAKEHOUSE_DIR) -> Path:
    return Path(lakehouse_dir) / CATALOG_NAME


def data_path(lakehouse_dir: str | Path = LAKEHOUSE_DIR) -> str | Path:
    """Where the Parquet lives: `data/` beside the catalog, or a bucket.

    A bucket is `LAKEHOUSE_DATA_PATH`, returned as the string it was given:
    `Path` collapses `s3://` to `s3:/`, and dbt reads the same variable, which
    DuckLake compares as a string. **The variable wins over `lakehouse_dir`**,
    so a caller pointing at another catalog must redirect or clear it:
    `just test-pipeline` redirects it, and the test suite clears it.
    """
    url = os.environ.get(DATA_PATH_ENV_VAR)
    if not url:
        return Path(lakehouse_dir) / DATA_DIRNAME
    if not url.startswith("s3://"):
        raise ValueError(
            f"{DATA_PATH_ENV_VAR}={url!r} is not an s3:// URL. It only moves the "
            "Parquet to S3-compatible storage; unset it for a landing zone on disk, "
            "which LAKEHOUSE_DIR places."
        )
    return url


def storage_secret() -> dict[str, str] | None:
    """The S3 secret a connection to a bucket `data_path` needs, or None on disk.

    Needed on *every* connection: DuckDB takes no endpoint from the environment,
    and with no secret it sends the request to AWS, access key id included.
    `attach()` creates this one, `dbt/profiles.yml` spells the same for dbt, and
    `dlt_credentials` hands dlt the parts it builds its own from.
    """
    return _s3_secret() if isinstance(data_path(), str) else None


def _s3_secret() -> dict[str, str]:
    endpoint = _s3_setting(S3_ENDPOINT_ENV_VAR)
    scheme, _, host = endpoint.partition("://")
    if scheme not in ("http", "https") or not host:
        raise ValueError(
            f"{S3_ENDPOINT_ENV_VAR}={endpoint!r} needs its scheme, http:// or https:// — "
            "it decides whether the connection uses TLS."
        )
    return {
        "key_id": _s3_setting("AWS_ACCESS_KEY_ID"),
        "secret": _s3_setting("AWS_SECRET_ACCESS_KEY"),
        "endpoint": host.rstrip("/"),
        "use_ssl": "true" if scheme == "https" else "false",
        "region": os.environ.get("AWS_REGION") or DEFAULT_S3_REGION,
    }


def _s3_setting(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"{DATA_PATH_ENV_VAR} names a bucket, so {name} must be set too (see .env.example)."
        )
    return value


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
    data = data_path(lake)
    if isinstance(data, Path):
        data.mkdir(parents=True, exist_ok=True)
        storage = f"file://{data}"
    else:
        from dlt.common.configuration.specs import AwsCredentials
        from dlt.common.storages.configuration import FilesystemConfiguration

        # dlt builds its DuckDB secret from these: `http://` in the endpoint
        # turns TLS off. It needs no s3fs, which it uses for local storage only.
        # The URL is rebuilt from the secret, not read from the variable, because
        # dlt strips only the scheme: a trailing slash would reach its secret and
        # no other.
        secret = _s3_secret()
        scheme = "https" if secret["use_ssl"] == "true" else "http"
        storage = FilesystemConfiguration(
            bucket_url=data,
            credentials=AwsCredentials(
                aws_access_key_id=secret["key_id"],
                aws_secret_access_key=secret["secret"],
                endpoint_url=f"{scheme}://{secret['endpoint']}",
                region_name=secret["region"],
                s3_url_style="path",
            ),
        )
    return DuckLakeCredentials(
        ducklake_name=ATTACH_ALIAS,
        catalog=f"duckdb:///{catalog_path(lake)}",
        storage=storage,
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
        storage_secret=storage_secret(),
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
