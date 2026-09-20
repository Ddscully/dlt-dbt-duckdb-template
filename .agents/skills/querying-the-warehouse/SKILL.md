---
name: querying-the-warehouse
description: How to inspect data/warehouse.duckdb in this project — read-only connections, the single-writer lock, where raw actually lives, and checking column names before writing SQL. Use before writing SQL against this warehouse, or when a DuckDB connection fails with a lock or a missing-catalog error.
---

# Querying the warehouse

## Two files, and `raw` is not in the one you expect

| What | Where |
|---|---|
| `staging`, `marts`, `analytics`, `history` | `data/warehouse.duckdb` |
| `raw` | the DuckLake catalog under `data/lakehouse/` |

`just where` prints both paths for the current environment. Never hard-code
them: `WAREHOUSE_PATH` and `LAKEHOUSE_DIR` are set by `just`, by CI and by
`just test-pipeline`, and a literal path will happily query the wrong database.

Because `staging` is a set of views over the lakehouse, a bare
`duckdb data/warehouse.duckdb` fails them with `Catalog "lakehouse" does not
exist!`. Use:

```bash
just sql            # read-only, lakehouse attached
just sql write      # the same, writable
```

## The lock: one writer XOR many readers

DuckDB takes a single writer per file, across processes. So:

- an open `just sql` session blocks `just run`, `just dbt-build` and
  `just materialize` — and the failure names the other process, not the session
  you forgot;
- a read-only connection **also** fails while a build holds the file.

Two readers are fine. Close the CLI before building; if a command reports the
file is locked, look for your own shell first.

**Inside one process the rule is different — and it is still not "a writer plus
readers".** DuckDB caches one instance per file and `read_only` is part of its
configuration, so a second `connect()` asking for the *other* mode fails in both
orders with `Can't open a connection to same database file with a different
configuration than existing connections`. That is a configuration error, with no
lock and no PID to go hunting for. A second connection in the *same* mode shares
the cached instance and works, and MVCC gives a reader a consistent snapshot
while a write is in flight. So read-only is a property of the instance, not a
privilege on a connection: there is no restricted handle onto a writable
warehouse to hand a caller, which is why every reader here opens its own.

Mid-build, the one read that works is the lakehouse:
`lake.lakehouse.read_only_connection()` attaches the catalog read-only, and dbt
holds the *warehouse* file, not that one.

## With the Parquet in a bucket (`LAKEHOUSE_DATA_PATH`)

Attach through `just sql` or `read_only_connection()`, never by hand: both
create the S3 secret first, and a bare `ATTACH` has none, so DuckDB sends the
request — access key id included — to AWS.

**A `count(*)` or a numeric `max()` is not a read.** DuckLake answers both from
its per-file statistics without opening the Parquet, so they succeed with a
wrong key, through the `staging` views as well. To check the keys, read values:
`select sum(price) from lakehouse.raw.gold_prices_monthly` gets the 403.

## A read-only connection, from Python

```python
import duckdb
from modern_data_stack.paths import warehouse_path

con = duckdb.connect(warehouse_path(), read_only=True)
print(con.sql("select * from marts.fct_gold_price_month limit 5"))
con.close()
```

`read_only=True` is not politeness: without it DuckDB **creates** the file if it
is missing, so a typo in a path gives an empty database and a query that
succeeds with zero rows instead of an error.

For a one-liner:

```bash
uv run python -c "import duckdb; from modern_data_stack.paths import warehouse_path; \
  print(duckdb.connect(warehouse_path(), read_only=True).sql('select 1'))"
```

## Schema names are clean

`dbt/macros/generate_schema_name.sql` overrides dbt's default, which would give
`main_marts`. Write `marts.fct_gold_price_month`, `staging.stg_gold_prices`,
`analytics.gold_price_trend`.

## Check the columns before writing the query

Column names come from the staging layer's renaming, not from the publisher, and
a guess that is nearly right returns a confident wrong answer:

```sql
describe marts.fct_gold_price_month;
select * from duckdb_tables() where schema_name = 'marts';
```

Or read `dbt/models/marts/**/_*.yml`, where every mart column is declared with
its type under an enforced contract.

## Before trusting a number

- **`meta: {additivity: …}`** on the column says whether it may be summed. A
  price, a rate or anything ending `_pct` does not add up, whatever SQL lets you
  write.
- **A left-joined fact carries nulls on purpose.** The fact sits on a spine, so
  a period with no observation is a row with a null. `count(*)` counts the
  spine; `count(<measure>)` counts the observations.
- **A join is not a census.** To ask how often two models disagree, first count
  the rows that only one of them has.
- **Failing test rows are already stored**: `select * from
  dbt_test__audit.<test_name>` after a red `dbt build`.
