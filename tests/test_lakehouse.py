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

The last section guards the landing zone with its Parquet in an S3-compatible
bucket, where every failure is quiet in a different way: a wrong spelling of the
endpoint is a 403 that reads as a wrong key, and a connection with no secret
sends the access key id to AWS.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import duckdb
import pytest

from lake import lakehouse
from modern_data_stack.ducklake import attach, revisions, table_versions

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


# --------------------------------------------------------------------------- #
# The Parquet in a bucket — LAKEHOUSE_DATA_PATH
# --------------------------------------------------------------------------- #

BUCKET = "s3://lake/prefix/"


@pytest.fixture
def bucket(monkeypatch: pytest.MonkeyPatch) -> None:
    """The variables a `.env` set up for S3 holds. `tests/conftest.py` clears the
    data path for every other test; this puts it back.

    The endpoint carries a trailing slash on purpose: DuckDB then requests
    `http://host:8333//lake/…`, the signed path no longer matches, and SeaweedFS
    answers the write and the read with 403 Forbidden (measured 2026-09-17). So
    every spelling of the secret has to strip it, and these tests only see that
    if the input has one.
    """
    monkeypatch.setenv(lakehouse.DATA_PATH_ENV_VAR, BUCKET)
    monkeypatch.setenv(lakehouse.S3_ENDPOINT_ENV_VAR, "http://127.0.0.1:8333/")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testtest")
    monkeypatch.delenv("AWS_REGION", raising=False)


def _secrets(con) -> list[dict[str, str]]:
    """`duckdb_secrets()` as dicts, parsed from its redacted `secret_string`."""
    return [
        dict(part.split("=", 1) for part in row[0].split(";"))
        for row in con.execute("select secret_string from duckdb_secrets()").fetchall()
    ]


def test_a_bucket_data_path_reaches_the_catalog_as_the_string_it_was_given(tmp_path, bucket):
    """`Path("s3://lake/prefix/")` is `s3:/lake/prefix`, and DuckLake compares the
    stored data path as a string — so a URL that passed through `Path` anywhere
    between the variable and the ATTACH is a catalog dbt then refuses to open,
    since the profile hands DuckLake the variable verbatim.

    Attaching an `s3://` data path writes nothing to the bucket, so this runs
    offline."""
    assert lakehouse.data_path(tmp_path) == BUCKET

    con = duckdb.connect()
    try:
        attach(
            con,
            lakehouse.catalog_path(tmp_path),
            lakehouse.data_path(tmp_path),
            alias="lakehouse",
            storage_secret=lakehouse.storage_secret(),
        )
        stored = con.execute(
            "select value from __ducklake_metadata_lakehouse.ducklake_metadata "
            "where key = 'data_path'"
        ).fetchone()
    finally:
        con.close()
    assert stored == (BUCKET,)


def test_a_bucket_connection_installs_httpfs_before_loading_it(tmp_path, bucket):
    """The S3 secret needs httpfs, and a bare `load httpfs` fails on a machine
    that has never downloaded it: `Extension "httpfs" … not found`. This
    template's CI hit exactly that on the branch that added bucket support
    (2026-09-17).

    Any machine that has run dbt has httpfs, because the profile lists it, so a
    real attach cannot fail locally, and making a machine without it means a
    download inside a unit test. So this spies on a real connection and holds
    the order of the statements `attach` sends. Attaching an `s3://` data path
    writes nothing to the bucket, so the attach itself runs offline.
    """
    con = duckdb.connect()
    spy = MagicMock(wraps=con)
    try:
        attach(
            spy,
            lakehouse.catalog_path(tmp_path),
            lakehouse.data_path(tmp_path),
            alias="lakehouse",
            storage_secret=lakehouse.storage_secret(),
        )
    finally:
        con.close()

    statements = [call.args[0].strip().lower() for call in spy.execute.call_args_list]
    assert "load httpfs" in statements, "a bucket attach no longer loads httpfs at all"
    loaded = statements.index("load httpfs")
    assert "install httpfs" in statements[:loaded], (
        "httpfs is loaded without being installed first, which fails on a fresh machine"
    )


def test_every_reader_connection_carries_a_secret_scoped_to_the_bucket(tmp_path, bucket):
    """DuckDB reads no S3 endpoint from the environment, so a connection without
    this secret sends its request to AWS — and it *does* read the keys, so the
    access key id goes with it. `read_only_connection` is what every Python
    reader of the landing zone opens."""
    con = duckdb.connect()
    attach(con, lakehouse.catalog_path(tmp_path), BUCKET, alias="lakehouse")
    con.close()

    con = lakehouse.read_only_connection(tmp_path)
    try:
        secrets = _secrets(con)
    finally:
        con.close()
    assert len(secrets) == 1, "a reader of a bucket lakehouse opened with no S3 secret"
    assert secrets[0]["scope"] == BUCKET
    assert secrets[0]["endpoint"] == "127.0.0.1:8333"
    assert secrets[0]["use_ssl"] == "false"
    assert secrets[0]["url_style"] == "path"


def test_on_disk_a_connection_creates_no_secret(tmp_path):
    """The other half: unset, the behaviour is exactly the on-disk one."""
    _write(tmp_path, [LOAD])
    assert lakehouse.storage_secret() is None
    con = lakehouse.read_only_connection(tmp_path)
    try:
        assert _secrets(con) == []
    finally:
        con.close()


def test_dlt_is_handed_the_endpoint_without_its_trailing_slash(tmp_path, bucket):
    """dlt strips only the scheme from `endpoint_url`, so a slash in the variable
    reached dlt's secret and no other — the 403 in the `bucket` fixture, on the
    one writer. The URL is rebuilt from `storage_secret()`'s parts instead."""
    storage = lakehouse.dlt_credentials(tmp_path).storage
    assert storage.bucket_url == BUCKET
    assert storage.credentials.endpoint_url == "http://127.0.0.1:8333"
    assert storage.credentials.s3_url_style == "path"
    assert not (tmp_path / lakehouse.DATA_DIRNAME).exists(), "made a local data dir for a bucket"


def test_the_dbt_profile_spells_the_same_secret_and_data_path(bucket, monkeypatch):
    """The secret is spelled three times — `storage_secret()`, the dbt profile and
    `just sql` — and a disagreement is a 403 inside `dbt build` that reads as a
    wrong key. This renders the profile's Jinja with dbt's `env_var` semantics
    and holds its spelling to Python's; with the variable unset its data path
    must fall back to the directory beside the catalog."""
    import os
    from pathlib import Path

    import jinja2
    import yaml

    def render(value: str) -> str:
        def env_var(name: str, default: str | None = None) -> str:
            value = os.environ.get(name, default)
            assert value is not None, f"the profile reads {name} with no default"
            return value

        return jinja2.Template(value).render(env_var=env_var)

    profile = yaml.safe_load(Path("dbt/profiles.yml").read_text())
    output = profile["my_warehouse"]["outputs"]["dev"]
    (secret,) = output["secrets"]
    rendered = {key: render(str(value)) for key, value in secret.items()}
    python = lakehouse.storage_secret()
    assert python is not None
    for key in ("key_id", "secret", "endpoint", "use_ssl", "region"):
        assert rendered[key] == python[key], f"profile and storage_secret() disagree on {key}"
    assert rendered["scope"] == BUCKET
    assert render(output["attach"][0]["options"]["data_path"]) == BUCKET

    monkeypatch.delenv(lakehouse.DATA_PATH_ENV_VAR)
    monkeypatch.setenv("LAKEHOUSE_DIR", "/abs/lakehouse")
    assert render(output["attach"][0]["options"]["data_path"]) == "/abs/lakehouse/data/"


def test_a_bucket_with_a_setting_missing_is_refused_by_name(tmp_path, bucket, monkeypatch):
    """Unrefused, a missing key reaches DuckDB as the text `None` and comes back
    as a 403 — the same symptom as a wrong key, naming neither. A data path that
    is not `s3://` is refused too: the variable only ever moves the Parquet to a
    bucket, and a local path belongs in `LAKEHOUSE_DIR`."""
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY")
    with pytest.raises(RuntimeError, match="AWS_SECRET_ACCESS_KEY"):
        lakehouse.storage_secret()

    monkeypatch.setenv(lakehouse.DATA_PATH_ENV_VAR, str(tmp_path / "data"))
    with pytest.raises(ValueError, match="not an s3:// URL"):
        lakehouse.data_path(tmp_path)
