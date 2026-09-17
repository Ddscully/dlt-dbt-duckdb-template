---
name: adding-a-data-source
description: End-to-end workflow for adding a data source to this warehouse — dlt resource, dbt source and staging model, mart, Dagster asset key, fixture and dashboard page. Use whenever adding, renaming or removing a source, a dlt resource or a raw table, and when replacing the gold-prices example the template ships with.
---

# Adding a data source

Every layer has to learn the new name, and most of the failures are silent: the
graph splits in two, the fixture is never committed, the page renders empty.
Work through the list in order — each step is small, and the last three are the
ones people forget.

The gold-prices source that ships with the template is the worked example. Read
it first (`ingest/sources/gold.py` → `stg_gold_prices` → `fct_gold_price_month`
→ `reports/pages/gold.md`), then either follow it or delete it once yours runs.

## 1. The dlt resource — `ingest/sources/<publisher>.py`

One module per publisher, not per resource. Put the URL in a module constant:
the fixture router and the tests both read it by name.

```python
@dlt.resource(name="gold_prices_monthly", write_disposition="replace")
def gold_prices_monthly():
    yield pl.read_csv(_csv_source(GOLD_PRICES_MONTHLY), infer_schema_length=None).to_dicts()
```

- **`infer_schema_length=None` scans the whole file.** Polars types a column
  from the first rows otherwise, so a column that is empty at the top of the
  file arrives as null.
- **Reach shared helpers through the module** — `http.get_json(...)`, never
  `from ingest.http import get_json`. A unit test for the source patches them
  through the module; a by-name import binds the unpatched original, and the
  test then passes while exercising the real fetch.
- Then register it in `ingest/pipeline.py`: add it to the `@dlt.source`, and to
  **exactly one** of `FULL_REFRESH_RESOURCES` or `INCREMENTAL_RESOURCES`.

**A load is two `run()` calls, and that is deliberate.** `refresh=
"drop_resources"` re-infers the schema (dlt otherwise only ever *widens* types,
so a narrowed upstream column is masked), but applied to a merge resource it
would drop the table *and its watermark*. So the replace group runs with
`refresh` and the merge group without.

**A backfill window is run config, not a partition, unless every partition
together is a routine load.** A partitioned asset partitions any job that
selects it, and a partitioned job's Materialize button in the Dagster UI is a
backfill of every partition, with no "no partition" choice —
`test_the_routine_jobs_are_not_partitioned` fails on it. So a source that loads
a rolling lookback routinely and a year or date range on demand gets a
`dg.Config` on its op, unset meaning the lookback, and stays in `full_refresh`.
Only a source like the months of one static file, where every partition costs
one fetch, belongs in `PARTITIONED_RESOURCES`, with a job of its own. A recipe
that passes the range with `dagster asset materialize --config-json` needs a
test holding the op name it spells: given a name it does not know, the command
ignores the config, loads the default window and exits 0 (measured on 1.13.22).

## 2. The landing table — `dbt/models/staging/_sources.yml`

Add a `- name:` under the `raw` source, with a description. The name must be the
dlt resource's name, exactly.

**This is where the asset graph joins.** `orchestration/assets.py` keys dlt
resources as `raw/<resource>` so they meet the keys dagster-dbt derives from
this file. Spell one of them differently and the graph silently splits into two
halves that both still run. `just materialize-preview '*'` shows the keys.

## 3. The staging model — `dbt/models/staging/stg_<entity>.sql`

Rename, cast, filter. No joins to other sources. Declare the grain's uniqueness
test in `_staging.yml` — that test is what the bus matrix later reads as the
grain.

## 4. The mart — `dbt/models/marts/<group>/`

A `fct_` or `dim_` model with `contract: {enforced: true}`, every column typed,
every numeric column carrying `meta: {additivity: …}`. If the group is new, add
it to `dbt/models/_groups.yml` and set `+group:` on the folder in
`dbt_project.yml`.

**Join the fact onto a dimension that decides its population** (the template
uses a `dim_month` date spine). A period the publisher has not filled then
arrives as a row with nulls rather than as a missing row, and "is this gap real"
becomes answerable.

## 5. The fixture — CI runs offline

CI sets `INGEST_FIXTURES=1`, so the source cannot pass `just test-pipeline`
until its fixture exists. Two edits and one command:

1. a route in `ingest/fixtures.py`'s `_ROUTES` — `(pattern, filename)`, and the
   filename is named after the resource;
2. a call in `scripts/record_fixtures.py`;
3. `just record-fixtures`, which hits the network once.

Then **check git actually took the file**: `git status --short
tests/fixtures/ingest/`. `.gitignore` carries `*.csv` with an exception for that
directory; a fixture written anywhere else, or in another ignored format, is
skipped by `git add -A` without a word, passes in your working tree, and fails
in CI with `No such file or directory`.

`tests/test_fixtures.py` then holds the whole set together: every URL resolves,
no two routes claim one URL, every route is reachable, no fixture is orphaned.

## 6. The orchestration layer — `orchestration/`

- `RAW_DESCRIPTIONS` in `assets.py` needs an entry per resource;
  `tests/test_definitions.py` fails without one.
- A new Polars transform is a new `@dg.asset`, and it must also be listed by
  hand in `definitions.py`. **An asset missing from that list is not in the
  graph at all**, and `dagster definitions validate` still passes.
- `transform/pipeline_status.py`'s `SOURCE_TABLES` lists the landing tables the
  observability page inventories.

## 7. The dashboard — `reports/`

A query in `reports/sources/warehouse/<name>.sql` and a page in
`reports/pages/`. **Name the columns; never `select *`** — an Evidence source
ships every column it selects to every visitor of the built site.

Then `publish/build_report.py`'s `TABLE_TO_DBT_MODEL` (for dbt models) or
`TABLE_TO_ASSET_KEY` (for Polars output), and an exposure in
`dbt/models/_exposures.yml`. `tests/test_report.py` and `tests/test_exposures.py`
hold both to the SQL the pages actually run.

## 8. Run it

```bash
just test            # the guards above
just test-pipeline   # the real thing, offline, into a throwaway warehouse
just materialize     # the same, through the asset graph
```

`just dbt-freshness` and the nightly workflow take it from there.

## Removing a source

The same list in reverse, and the guards will find what you miss: the fixture
(orphan test), `RAW_DESCRIPTIONS` (definitions test), the exposure (exposures
test), the report maps (report test). The one nothing checks is the landing
table itself — dropping the resource leaves the rows in the DuckLake catalog,
where earlier snapshots keep them readable.
