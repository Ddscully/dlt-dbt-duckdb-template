# The warehouse

Two files on disk, and one rule about which holds what.

```
data/lakehouse/     the DuckLake catalog — `raw`, written by dlt
data/warehouse.duckdb   everything dbt and Polars build — staging, marts, analytics
```

`just where` prints both. Nothing else in the repo names a warehouse path: every
layer resolves it through `modern_data_stack.paths`, which reads
`WAREHOUSE_PATH` and `LAKEHOUSE_DIR` when they are set — which is how
`just test-pipeline` builds into a throwaway file. The lakehouse's Parquet can
also live in a bucket instead ([below](#the-parquet-in-an-s3-compatible-bucket)).

## The layers

| Schema | Built by | Materialisation | Contents |
|---|---|---|---|
| `raw` (in the lakehouse) | dlt | DuckLake tables | one table per resource, as the publisher serves it, plus dlt's `_dlt_*` columns |
| `staging` | dbt | views | one `stg_*` per landing table: renamed, cast, no joins to other sources |
| `intermediate` | dbt | views | `int_*`, only where two models would otherwise repeat a join. Empty in this template |
| `marts` | dbt | tables | the interface: `dim_*` and `fct_*`, contracts enforced, one folder per group |
| `analytics` | Polars | tables | derived metrics dbt models badly — window arithmetic, ranking |
| `history` | dbt snapshots | tables | slowly-changing history of a source that restates. Empty in this template |

**`raw` is in the lakehouse, not the DuckDB file.** dbt's `_sources.yml` says
`database: lakehouse`, which is the ATTACH alias in `dbt/profiles.yml` and
`lake.lakehouse.ATTACH_ALIAS`. Those three spellings have to agree or dbt cannot
resolve a source; `tests/test_lakehouse.py` holds them together.

That also means `staging` is a set of views over another database. A bare
`duckdb data/warehouse.duckdb` fails them with `Catalog "lakehouse" does not
exist!` — use `just sql`, which attaches both.

## What a rebuild cannot make again

`data/lakehouse/` is the only copy of every landing table, so `just clean` never
touches it. Re-making it costs whatever the sources charge to fetch again, which
for this template is nothing and for a rate-limited or paid API is the whole
archive.

`analytics.pipeline_runs` is the other one: one row per dbt node per invocation,
appended, because each build overwrites the artifact the previous one was read
from. Deleting `data/warehouse.duckdb` destroys that history. A project that
adds a dbt snapshot puts a second unreproducible table in the same file.

## The Parquet in an S3-compatible bucket

The catalog is always a local file, but the Parquet under it can live in a
bucket instead of `data/lakehouse/data/`. One variable switches it,
`LAKEHOUSE_DATA_PATH=s3://bucket/prefix/`, and three more say how to reach the
store: `LAKEHOUSE_S3_ENDPOINT` and the standard `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY`. [`.env.example`](../.env.example) has all four, and
`just where` prints the data path the recipes will use. Unset, everything above
holds unchanged.

Measured on 2026-09-17 against SeaweedFS in Docker, which is enough to try it
(`--rm` and no named volume, so stopping the container deletes the bucket):

```sh
docker run -d --rm --name mds-s3 -p 127.0.0.1:8333:8333 \
  -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=testtest -e S3_BUCKET=lake \
  chrislusf/seaweedfs mini -dir=/data
cp .env.example .env    # then uncomment the S3 block
```

`just test-pipeline`, `just materialize` (13 of 13 asset checks) and `just sql`
all ran against it with no Parquet on disk, and `sum(price)` over
`raw.gold_prices_monthly` read back the same as a run on disk.

- **Choose before the first `just ingest`.** The catalog records its data path
  and DuckLake refuses to attach it with any other, so setting the variable over
  an existing landing zone fails with `DATA_PATH parameter … does not match`.
  Moving one means copying the Parquet and rewriting that record, and nothing
  here does it.
- **Every connection needs the endpoint and keys.** DuckDB reads no endpoint from
  the environment, and with no secret it sends the request to AWS, access key id
  included. So they are spelled three times: `storage_secret()` in
  `lake/lakehouse.py` (dlt and every Python reader), the `secrets:` block in
  `dbt/profiles.yml`, and `just sql`. `tests/test_lakehouse.py` holds the first
  two to each other.
- **A trailing slash on the endpoint is a 403, not a 404.** DuckDB requests
  `http://host:8333//lake/…`, the signed path no longer matches, and the store
  refuses the write and the read as Forbidden — which reads as a wrong key.
  Every spelling strips it.
- **Throwaway runs keep the storage, not the place.** `just test-pipeline` writes
  its fixture Parquet under `test-pipeline/` in the same bucket; the test suite
  is always on disk.
- **A green `just lakehouse` does not prove the keys**, and nor does a
  `count(*)` or a numeric `max()`: all three are answered from the catalog's
  statistics and read no Parquet, so they answer with a wrong key — through the
  `staging` view too. Reading values fails with a 403 as it should:
  `select sum(price) from lakehouse.raw.gold_prices_monthly` in `just sql`.

## The bus matrix

Business processes down, conformed dimensions across — Kimball's planning
artifact, and the one thing `_groups.yml` (who owns it), `_exposures.yml` (who
reads it) and the contracts (what shape it is) do not say. **It is derived from
`manifest.json`, never written**: the grain comes from each model's own
uniqueness tests and the columns from its enforced contract, so a mart added
without a conformed key shows up as a hole rather than as nothing.

Two rules decide what a mark means. A uniqueness test carrying a `where` is not
a grain — a filtered uniqueness test ("one row per entity *where current*") read
as a grain would make a versioned reference table look like a conformed
dimension. And conformance is **exact column-name matching**, deliberately: an
alias list would render two spellings of the same key as a tidy row of marks,
and those marks are the defect worth finding.

Regenerate with `just bus-matrix`; `tests/test_bus_matrix.py` fails if the block
below is stale.

<!-- bus-matrix:begin -->

<!-- Generated by `just bus-matrix`. Do not edit between the markers. -->

| Business process (fact) | Grain | dim_month |
|---|---|---|
| `fct_gold_price_month` | `month_start` | ✅ |

<!-- bus-matrix:end -->

## The example that ships

One source, end to end, so the graph has something to run:

- `raw.gold_prices_monthly` — monthly gold prices since 1833 from
  [datasets/gold-prices](https://github.com/datasets/gold-prices) (ODC-PDDL),
  one whole-file CSV, `write_disposition="replace"`.
- `staging.stg_gold_prices` — one row per `price_month`.
- `marts.dim_month` — a date spine, so the fact's population is decided here
  rather than by whichever months the publisher happened to price.
- `marts.fct_gold_price_month` — the fact, left-joined onto the spine: a month
  nobody has priced yet is a row with a null, not a missing row.
- `analytics.gold_price_trend` — the Polars layer: rolling twelve-month average
  and the change on a year earlier.

Replacing it is the `adding-a-data-source` skill, in reverse order: add yours,
then delete these five and their tests, fixture, page and exposure.
