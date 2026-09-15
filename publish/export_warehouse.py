"""Package the built warehouse as a publishable artifact.

Produces `data/export/`:

  warehouse.duckdb                  a checkpointed copy of the whole warehouse
  <schema>__<table>.parquet         one file per modelled table (zstd)
  manifest.json                     row counts, year coverage, sha256, provenance
  SHA256SUMS                        `sha256sum -c`-compatible
  ATTRIBUTION.md                    who owns the data (it isn't us)
  RELEASE_NOTES.md                  the GitHub release body

Run:  uv run python -m publish.export_warehouse            (or `just export-data`)

`.github/workflows/release-data.yml` runs this after materializing the asset graph
against the live sources and uploads the directory as a dated GitHub release.
Nothing here touches the network: it reads a warehouse that already exists.

The DuckDB file holds what dbt builds — `staging`, `marts`, `analytics`,
`history` — and never `raw`, which lives in the DuckLake catalog. The one landing
table a rebuild cannot afford to refetch ships separately as `lakehouse.tar.gz`
(see `publish_lakehouse`); the rest of `raw`, including dlt's merge scratch full
of clear customer ids, is never written to a published file.

The `staging` views are materialised on the way out (`solidify_staging`): dbt
writes them against the attached catalog, so in a file without it they raise
`Catalog "lakehouse" does not exist!`. And the copy keeps the file name
`warehouse.duckdb`, because DuckDB names the catalog after the stem and the
views' SQL is qualified with it.

The packaging itself is `gold_warehouse.export`; this module is what is
specific to this dataset — which schemas ship, attribution, the personal-data
policy, the manifest's extras and the release notes.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import UTC
from pathlib import Path

import duckdb

from gold_warehouse import privacy
from gold_warehouse.ducklake import catalog_metadata
from gold_warehouse.export import default_tag, export, loaded_at
from gold_warehouse.paths import dbt_manifest_path, warehouse_path

DUCKDB_PATH = warehouse_path()
MANIFEST_PATH = dbt_manifest_path()

# `default_tag` is re-exported: `release-data.yml` and the tests name the tag
# through this module rather than reaching past it.
__all__ = [
    "ATTRIBUTION",
    "DUCKDB_PATH",
    "EXPORT_DIR",
    "EXTRA_ADDITIVITY",
    "EXTRA_CLASSIFICATIONS",
    "LAKEHOUSE_ASSET",
    "MASKED_LABELS",
    "MAX_PUBLISHED_LAKE_VERSION",
    "MAX_PUBLISHED_STORAGE_VERSION",
    "MIN_READER_VERSION",
    "PUBLISHED_SCHEMAS",
    "SALT_ENV",
    "additivity",
    "default_tag",
    "landed_at",
    "main",
    "prepare_published_copy",
    "pseudonymise",
    "publish_lakehouse",
    "release_notes",
    "run",
]

EXPORT_DIR = "data/export"

# The published landing zone, beside `warehouse.duckdb`, so the next release can
# carry the weather archive forward instead of cold-starting it. What may go in
# it is `lake.lakehouse.PUBLISHED_TABLES`, an allowlist for disclosure as well as
# cost. A tarball because a release asset is one file: one download to restore,
# one line in `SHA256SUMS`.
LAKEHOUSE_ASSET = "lakehouse.tar.gz"

# The layers published as Parquet. `raw` is not in the file at all, and dbt's
# `main` (the seeds) and `history` ship only inside `warehouse.duckdb`.
PUBLISHED_SCHEMAS = ("staging", "marts", "analytics")

# The storage format the published `warehouse.duckdb` may not exceed, and so the
# oldest DuckDB that can open it. 64 is DuckDB 1.x's default, so this costs
# nothing today; it is a tripwire for a DuckDB bump (2.0 changes the default
# format) that every test would pass, because they write and read with one
# binary. Raising it strands every reader below the new floor, so it is a
# decision, not a lockfile edit. The Parquet files carry no such constraint.
MAX_PUBLISHED_STORAGE_VERSION = 64
MIN_READER_VERSION = "0.10.0"

# The same ceiling for `lakehouse.tar.gz`'s DuckLake spec. It moves differently:
# the extension comes from extensions.duckdb.org, unpinned, so a newer spec can
# arrive with no change in this repo and no PR to fail — the next CI run is what
# notices. 1.0 is what the installed extension writes today.
MAX_PUBLISHED_LAKE_VERSION = "1.0"

ATTRIBUTION = """\
# Data attribution

This artifact is a *derived* dataset: the pipeline that produced it fetches
public data at run time, cleans it, and joins it. The underlying data belongs to
its publishers and is redistributed here under their licences.

| Source | Publisher | Licence |
|--------|-----------|---------|
| Monthly gold prices | [datasets/gold-prices](https://github.com/datasets/gold-prices) | [ODC-PDDL 1.0](https://opendatacommons.org/licenses/pddl/1-0/) |

Attribute the publishers, not this repository, when you use the numbers. Neither
the publishers nor this project warrant the data; the transformations are the
pipeline's and any error in them is ours.

The pipeline code is MIT licensed.
"""


# Which classification gets rewritten on the way out. `quasi_identifier` columns
# identify a customer between them but are published anyway: generalising them
# would destroy the analysis they exist for. `docs/DATA_PROTECTION.md` measures
# what they give away.
MASKED_LABELS = ("direct_identifier",)

# Required, never defaulted: an unsalted digest of a five-digit id is reversed by
# hashing every candidate, in milliseconds, and an invertible hex column reads as
# though something was done.
SALT_ENV = "PII_SALT"

# Whether this project publishes personal data at all. Declared, never inferred:
# `pseudonymise` refuses an empty classified set because "nothing classified"
# reads exactly like "nothing to classify", so a project with no personal data
# has to say so here or it cannot publish.
PERSONAL_DATA = False

# The first paragraph of every release body: what this data is, at what grain.
RELEASE_SUMMARY = (
    "Monthly gold prices since 1833, one row per month, in US dollars per troy "
    "ounce — the output of this repo's pipeline, so you can use the data without "
    "running dlt, dbt or DuckDB yourself."
)

# Classifications dbt cannot hold, because Polars writes `analytics` downstream
# of it. Named rather than inferred: the name-based sweep would catch
# `customer_id`, but not a column renamed on the way into Polars.
EXTRA_CLASSIFICATIONS: dict[tuple[str, str, str], str] = {}


# Additivity labels for the `analytics` tables, which dbt cannot see for the same
# reason. Stated rather than copied from the mart at runtime: a derived copy
# would silently lose a label when a mart column is renamed, where a stated one
# fails `test_a_copied_column_keeps_the_label_the_mart_gave_it`.
EXTRA_ADDITIVITY: dict[tuple[str, str, str], str] = {
    ("analytics", "pipeline_sources", "rows"): "additive",
    ("analytics", "pipeline_sources", "year_min"): "not_a_measure",
    ("analytics", "pipeline_sources", "year_max"): "not_a_measure",
    ("analytics", "pipeline_tables", "rows"): "additive",
    ("analytics", "pipeline_tables", "year_min"): "not_a_measure",
    ("analytics", "pipeline_tables", "year_max"): "not_a_measure",
    ("analytics", "pipeline_tests", "failing_rows"): "additive",
    # The run history's timings sum across nodes and across runs, but to
    # thread-seconds: dbt builds four nodes at once, so a run's summed
    # `execution_time_s` is several times the wall clock it took.
    ("analytics", "pipeline_runs", "execution_time_s"): "additive",
    ("analytics", "pipeline_runs", "compile_time_s"): "additive",
    ("analytics", "pipeline_runs", "execute_time_s"): "additive",
}


def classifications(manifest_path: str = MANIFEST_PATH) -> dict[tuple[str, str, str], str]:
    """Every classified column in the warehouse: dbt's, plus `analytics`.

    Without a manifest (`dbt/target/` is gitignored) it degrades to
    `EXTRA_CLASSIFICATIONS` rather than raising. That is safe only because
    `pseudonymise` expands by column name across every relation, and
    `customer_id` is named there; what is lost is a future identifier known only
    to dbt, which is why the manifest records the fallback.
    """
    path = Path(manifest_path)
    if not path.exists():
        return dict(EXTRA_CLASSIFICATIONS)
    return privacy.classifications(json.loads(path.read_text())) | EXTRA_CLASSIFICATIONS


def additivity(manifest_path: str = MANIFEST_PATH) -> dict[str, dict[str, str]] | None:
    """Which published columns may be summed, keyed by published relation.

    A Parquet file carries names and types, nothing that says `co2_mt` sums and
    `renewables_share_pct` does not. The labels are declared as
    `meta: {additivity: …}` in the marts ymls; this carries them into the release.
    Keyed by `schema.alias`, the name each Parquet file ships under — so the
    versioned model appears as both `fct_emissions_energy` and `_v1`.

    Without a manifest it returns `None` ("dbt was not asked"), not `{}` ("no
    labelled columns"), and drops `EXTRA_ADDITIVITY` too. Unlike
    `classifications`, a partial answer here is worse than none: a consumer could
    not tell unlabelled marts from marts with nothing to label.
    """
    path = Path(manifest_path)
    if not path.exists():
        return None
    nodes = json.loads(path.read_text()).get("nodes", {}).values()
    out: dict[str, dict[str, str]] = {}
    for node in nodes:
        if node.get("resource_type") != "model":
            continue
        labels: dict[str, str] = {}
        for column, spec in (node.get("columns") or {}).items():
            label = (spec.get("meta") or {}).get("additivity")
            if label:
                labels[column] = label
        if labels:
            relation = f"{node['schema']}.{node.get('alias') or node['name']}"
            out[relation] = labels
    for (schema, table, column), label in EXTRA_ADDITIVITY.items():
        out.setdefault(f"{schema}.{table}", {})[column] = label
    return {relation: dict(sorted(cols.items())) for relation, cols in sorted(out.items())}


def publish_lakehouse(dest_dir: Path, lakehouse_dir: str | Path | None = None) -> dict:
    """Write the publishable landing tables into `dest_dir` as `LAKEHOUSE_ASSET`.

    A second asset because it is the unreproducible part: `warehouse.duckdb` can
    be rebuilt from the sources, the weather archive cannot within a day's API
    budget. Relocatable (relative `data_path`), so a consumer opens it with a
    bare `ATTACH` wherever they unpack it; `lake.lakehouse.restore` puts the
    absolute form back. `lakehouse_dir` is explicit so the output cannot depend
    on whichever landing zone the machine has.
    """
    import tarfile
    import tempfile

    from lake import lakehouse

    lake_dir = lakehouse.LAKEHOUSE_DIR if lakehouse_dir is None else Path(lakehouse_dir)

    # No landing zone is a legitimate export, but in a release it means next
    # month's weather archive cold-starts. So record it — `"rows": 0`, never an
    # omitted key — for `release-data.yml` to assert on.
    if not lakehouse.is_catalog(lake_dir):
        return {
            "lakehouse": {
                "file": None,
                "spec_version": None,
                "created_by": None,
                "tables": {},
                "rows": 0,
            }
        }

    with tempfile.TemporaryDirectory() as staging:
        built = Path(staging) / "lakehouse"
        copied = lakehouse.publish(built, lake_dir, MAX_PUBLISHED_LAKE_VERSION)
        # A catalog holding none of the published tables counts as absent: dbt's
        # `ATTACH IF NOT EXISTS` creates a real, empty DuckLake on a build before
        # the first ingest, and a tarball of nothing must not pass as published.
        if not copied:
            return {
                "lakehouse": {
                    "file": None,
                    "spec_version": None,
                    "created_by": None,
                    "tables": {},
                    "rows": 0,
                }
            }

        # Read off the built catalog, so the manifest describes what shipped.
        # `spec_version` is what decides whether a consumer can open it;
        # `created_by` (a DuckDB git hash) only says who wrote it.
        catalog = catalog_metadata(built / lakehouse.CATALOG_NAME)

        archive = dest_dir / LAKEHOUSE_ASSET
        with tarfile.open(archive, "w:gz") as tar:
            # Under a `lakehouse/` directory rather than the archive root, so it
            # unpacks self-describing; the restore expects that directory.
            tar.add(built, arcname="lakehouse")

    return {
        "lakehouse": {
            "file": LAKEHOUSE_ASSET,
            "catalog": lakehouse.CATALOG_NAME,
            "spec_version": catalog["version"],
            "created_by": catalog["created_by"],
            "bytes": archive.stat().st_size,
            "tables": copied,
            "rows": sum(copied.values()),
        }
    }


def prepare_published_copy(
    con: duckdb.DuckDBPyConnection, lakehouse_dir: str | Path | None = None
) -> dict:
    """Everything the copy needs before it is read: stand alone, then anonymise.

    The order matters. `solidify_staging` writes tables holding whatever their
    views selected, clear customer ids included; pseudonymising first would leave
    them untouched, and the published `staging` would disagree with `marts` about
    who a customer is, with matching row counts and no error.
    """
    if not PERSONAL_DATA:
        return {
            **solidify_staging(con, lakehouse_dir),
            "privacy": {"policy": "none", "reason": "the project declares no personal data"},
        }
    return {**solidify_staging(con, lakehouse_dir), **pseudonymise(con)}


def solidify_staging(
    con: duckdb.DuckDBPyConnection, lakehouse_dir: str | Path | None = None
) -> dict:
    """Turn the `staging` views into tables so the published file stands alone.

    dbt writes the staging views against the attached catalog
    (`select * from lakehouse.raw.owid_co2`), so in the published file alone
    every one raises `Catalog "lakehouse" does not exist!` while the marts work.
    Materialised here rather than in `dbt_project.yml`, so the local build keeps
    its free views and only the copy pays.

    The resulting tables hold the original customer ids, which is why this runs
    before `pseudonymise` — see `prepare_published_copy`.
    """
    from gold_warehouse.ducklake import attach
    from lake.lakehouse import ATTACH_ALIAS, LAKEHOUSE_DIR, catalog_path, data_path

    # The caller's landing zone, so the views are solidified against the catalog
    # that belongs to the database being packaged.
    lake_dir = LAKEHOUSE_DIR if lakehouse_dir is None else Path(lakehouse_dir)

    defined = con.execute(
        "select view_name, sql from duckdb_views() where schema_name = 'staging' order by 1"
    ).fetchall()
    views = [name for name, _ in defined]
    if not views:
        return {"staging_views_materialised": []}

    # Attach only if a view names the catalog: a warehouse with its own `raw`
    # (as `tests/test_export.py` builds) is exportable with no catalog at all.
    needs_catalog = any(f"{ATTACH_ALIAS}." in (sql or "") for _, sql in defined)
    if needs_catalog:
        attach(con, catalog_path(lake_dir), data_path(lake_dir), ATTACH_ALIAS, read_only=True)
    try:
        for name in views:
            # Two statements: DuckDB will not `create or replace table` over a
            # view of the same name, and the temp relation keeps the rows alive
            # across the drop.
            con.execute(
                f'create or replace table staging."_solid_{name}" as select * from staging."{name}"'
            )
            con.execute(f'drop view staging."{name}"')
            con.execute(f'alter table staging."_solid_{name}" rename to "{name}"')
    finally:
        if needs_catalog:
            con.execute(f"detach {ATTACH_ALIAS}")
    return {"staging_views_materialised": views}


def landed_at(
    con: duckdb.DuckDBPyConnection, lakehouse_dir: str | Path | None = None
) -> str | None:
    """`data_loaded_at` for the manifest, read from wherever `raw` actually is.

    dlt's `_dlt_loads` lives in the DuckLake catalog, not the file being
    packaged. An unqualified read fails, `loaded_at` swallows the error, and the
    release says "Data last landed: unknown" — so the catalog is attached
    (read-only) when there is one. A warehouse carrying its own `raw` reads that;
    where both exist the catalog wins, since an in-file `raw` left by an older
    layout is the stale copy.
    """
    from gold_warehouse.ducklake import attach
    from lake.lakehouse import ATTACH_ALIAS, LAKEHOUSE_DIR, catalog_path, data_path, is_catalog

    lake_dir = LAKEHOUSE_DIR if lakehouse_dir is None else Path(lakehouse_dir)
    if not is_catalog(lake_dir):
        return loaded_at(con)

    attach(con, catalog_path(lake_dir), data_path(lake_dir), ATTACH_ALIAS, read_only=True)
    try:
        return loaded_at(con, raw_database=ATTACH_ALIAS)
    finally:
        con.execute(f"detach {ATTACH_ALIAS}")


def pseudonymise(
    con: duckdb.DuckDBPyConnection, manifest_path: str = MANIFEST_PATH, salt: str | None = None
) -> dict:
    """Rewrite every direct identifier in the published copy. Returns provenance.

    Runs against the copy, across every schema in it. The declared columns are
    expanded by name because the file holds undeclared copies of the identifier:
    the `dbt_test__audit` tables (`store_failures` is project-wide, so a failing
    retail test writes customer rows there) and the `staging` tables
    `solidify_staging` just wrote. `privacy.verify` then checks every rewritten
    column.
    """
    salt = salt if salt is not None else os.environ.get(SALT_ENV, "")
    if not salt:
        raise privacy.PolicyError(
            f"{SALT_ENV} is not set. The export rewrites classified identifiers and an "
            "unsalted digest of a five-digit id is reversed in milliseconds, so there is "
            "no safe default. Set a stable secret for a release "
            "(`export PII_SALT=\"$(uv run python -c 'import secrets;print(secrets.token_hex(32))')\"` "
            "for a throwaway one)."
        )

    known = classifications(manifest_path)
    declared = sorted(c for c, label in known.items() if label in MASKED_LABELS)
    if not declared:
        # Otherwise fail-open: a typo in a `meta: {pii: …}` key, a dbt that stops
        # surfacing column meta, or an emptied EXTRA_CLASSIFICATIONS would all
        # publish every identifier in the clear under a manifest claiming a policy.
        raise privacy.PolicyError(
            "no columns are classified as "
            f"{'/'.join(MASKED_LABELS)} — refusing to publish. Either the classification "
            "is broken or the labels have been renamed; an export that masks nothing "
            "must not look like one that masked everything."
        )
    columns = privacy.expand_by_name(con, declared)
    touched = privacy.apply_pseudonymisation(con, columns, salt)
    privacy.verify(con, columns)
    return {
        "privacy": {
            "policy": "salted-sha256",
            "labels_rewritten": list(MASKED_LABELS),
            "pseudonym_length": privacy.PSEUDONYM_LENGTH,
            # False when the export ran without a dbt manifest, which narrows the
            # declared set to `EXTRA_CLASSIFICATIONS`. A consumer can tell the two
            # apart; a release built by `release-data.yml` always has one.
            "declared_from_dbt_manifest": Path(manifest_path).exists(),
            "columns": [f"{s}.{t}.{c}" for s, t, c in touched],
        }
    }


def _history(con: duckdb.DuckDBPyConnection) -> dict | None:
    """How much revision history the published snapshot carries.

    `release-data.yml` restores `history` from the previous release before it
    builds (see `publish/restore_history.py`), so this grows release over
    release. `None` when there is no snapshot to describe at all — an export of
    a warehouse whose mart wasn't built, rather than one that has simply never
    seen a revision, which reports zero.
    """
    try:
        row = con.execute(
            """
            select
                count(*) as country_years,
                count(*) filter (where is_revised) as revised,
                sum(version_count) as versions,
                min(first_loaded_at) as watching_since
            from marts.fct_co2_estimate_versions
            """
        ).fetchone()
    except duckdb.Error:
        return None
    if not row or not row[0]:
        return None
    country_years, revised, versions, since = row
    return {
        "country_years": country_years,
        "revised": revised,
        "versions": versions,
        "watching_since": since.astimezone(UTC).isoformat(timespec="seconds") if since else None,
    }


def release_notes(manifest: dict, repo: str, tag: str) -> str:
    """The GitHub release body: what it is, how to query it, what's inside."""
    base = f"https://github.com/{repo}/releases"
    latest = f"{base}/latest/download"
    warehouse = manifest["warehouse"]
    # The ATTACH alias isn't cosmetic: it has to match the catalog the views were
    # created against, which DuckDB took from the file stem. Normally `warehouse`,
    # but a WAREHOUSE_PATH run can name the file anything.
    alias = Path(warehouse["file"]).stem
    mart = next(
        (t for t in manifest["tables"] if t["table"] == "marts.fct_emissions_energy"),
        manifest["tables"][0],
    )

    def size(n: int) -> str:
        return f"{n / 1e6:.1f} MB" if n >= 1e6 else f"{n / 1e3:.0f} kB"

    # Conditional: an export without a landing zone records `file: None`, and a
    # bullet reading "spec None" is worse than no bullet.
    lake = manifest["lakehouse"]
    lake_version_note = (
        f"- **The landing zone has its own version, and it is not that one.** "
        f"`{LAKEHOUSE_ASSET}` is a DuckLake catalog written against **spec "
        f"{lake['spec_version']}** by {lake['created_by']}; what has to be new enough to open "
        f"it is your `ducklake` extension, not your DuckDB. `INSTALL ducklake` fetches the "
        f"build matching whatever DuckDB you are running, so in practice this only bites a "
        f"client pinned to an older extension.\n"
        if lake["file"]
        else ""
    )

    # The snapshot is the one table a rebuild can't reproduce, so say plainly how
    # much of it there is — including when the answer is "none yet".
    history = manifest.get("history")
    if history and history["revised"]:
        revisions = history["versions"] - history["country_years"]
        history_note = (
            f"{revisions:,} restatement{'s' if revisions != 1 else ''} across "
            f"{history['revised']:,} of {history['country_years']:,} country-years, "
            f"first recorded {(history.get('watching_since') or '')[:10] or 'unknown'}"
        )
    elif history:
        history_note = (
            f"{history['country_years']:,} country-years, all on version 1 — nothing restated yet"
        )
    else:
        history_note = "none — this snapshot is empty"

    rows = [
        f"| `{t['file']}` | `{t['table']}` | {t['rows']:,} | "
        f"{'–'.join(str(y) for y in t['years']) if t.get('years') else '—'} | {size(t['bytes'])} |"
        for t in manifest["tables"]
    ]

    labelled = sum(len(columns) for columns in (manifest.get("additivity") or {}).values())

    published_lake = manifest.get("lakehouse") or {}
    lakehouse_row = (
        f"| `{published_lake['file']}` | the landing zone dlt wrote (DuckLake) "
        f"| {published_lake['rows']:,} | | {size(published_lake['bytes'])} |"
        if published_lake.get("file")
        else ""
    )

    return f"""\
{RELEASE_SUMMARY}

Built from the live sources on {manifest["generated_at"][:10]}, from commit
{f"`{manifest['git_sha'][:7]}`" if manifest.get("git_sha") else "the tip of `main`"}.

## Query it without downloading it

```sql
INSTALL httpfs; LOAD httpfs;
-- Alias it `{alias}`, not something shorter: the `staging` views store
-- fully-qualified SQL and resolve against that catalog name.
ATTACH '{latest}/{warehouse["file"]}' AS {alias} (READ_ONLY);
SELECT * FROM {alias}.{mart["table"]} LIMIT 10;
```

Or a single table, no DuckDB file at all:

```sql
SELECT * FROM read_parquet('{latest}/{mart["file"]}') LIMIT 10;
```

`latest/download/…` always points at the newest snapshot. Pin a run by swapping
it for `{base}/download/{tag}/…`.

## What's in it

| Asset | Table | Rows | Years | Size |
|-------|-------|-----:|-------|-----:|
| `{warehouse["file"]}` | everything below, plus `history` | | | {size(warehouse["bytes"])} |
{lakehouse_row}
{chr(10).join(rows)}

`manifest.json` carries the row counts, year coverage and SHA-256 of every asset;
`SHA256SUMS` is `sha256sum -c`-compatible.

**`manifest.json` also says which columns may be summed.** Its `additivity` map
labels every numeric column of every `marts` and `analytics` table published
here — {labelled:,} of them: `additive`, `semi_additive`, `non_additive` and
`not_a_measure`. A Parquet file has no way of telling you which is which.

- **Any DuckDB from {MIN_READER_VERSION} on can open this file.** DuckDB {manifest["duckdb_version"]} wrote it;
  the storage format, {manifest["storage_version"]}, is what decides whether you can read it.
{lake_version_note}- **Data last landed:** {manifest.get("data_loaded_at") or "unknown"}.
- **Revision history:** {history_note}.

{ATTRIBUTION}"""


def run(
    duckdb_path: str = DUCKDB_PATH,
    out_dir: str = EXPORT_DIR,
    tag: str | None = None,
    repo: str | None = None,
    lakehouse_dir: str | Path | None = None,
) -> dict:
    """Build `out_dir` from `duckdb_path`. Returns the manifest.

    `lakehouse_dir` (default: the project's) decides which catalog ships as the
    second asset, which one `data_loaded_at` is read from, and which one the
    `staging` views are materialised against. It is bound here, once, so the
    three cannot disagree.
    """
    if not Path(duckdb_path).exists():
        raise FileNotFoundError(f"no warehouse at {duckdb_path} — run `just run` first")
    return export(
        duckdb_path,
        out_dir,
        schemas=PUBLISHED_SCHEMAS,
        attribution=ATTRIBUTION,
        release_notes=release_notes,
        tag=tag,
        repo=repo,
        grain="(country_iso3, year)",
        extra_manifest=lambda con: {"history": _history(con), "additivity": additivity()},
        read_loaded_at=lambda con: landed_at(con, lakehouse_dir),
        prepare_copy=lambda con: prepare_published_copy(con, lakehouse_dir),
        extra_artifacts=lambda dest: publish_lakehouse(dest, lakehouse_dir),
        max_storage_version=MAX_PUBLISHED_STORAGE_VERSION,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--warehouse", default=DUCKDB_PATH, help="source DuckDB file")
    parser.add_argument("--out", default=EXPORT_DIR, help="output directory")
    parser.add_argument("--tag", default=None, help="release tag (default: data-YYYY-MM-DD)")
    parser.add_argument(
        "--repo",
        default=None,
        help="owner/name for the download URLs (default: $GITHUB_REPOSITORY or the origin remote)",
    )
    args = parser.parse_args()

    manifest = run(args.warehouse, args.out, args.tag, args.repo)
    # Parquet files, the database, and the landing zone when it shipped.
    assets = len(manifest["tables"]) + 1 + (1 if manifest["lakehouse"]["file"] else 0)
    total = manifest["warehouse"]["bytes"] + sum(t["bytes"] for t in manifest["tables"])
    print(f"{manifest['tag']}: {assets} assets, {total / 1e6:.1f} MB in {args.out}")
    for table in manifest["tables"]:
        print(f"  {table['file']:44} {table['rows']:>8,} rows")


if __name__ == "__main__":
    main()
