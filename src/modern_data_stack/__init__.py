"""modern_data_stack: the domain-neutral half of this pipeline.

The project layers (`ingest/`, `dbt/`, `transform/`, `lake/`, `reports/`,
`orchestration/`) know what the data is about. This package is what is left when
you take the dataset out of them, and it keeps its name whatever the project is
called:

* `paths`         — where the project root, the warehouse and the lakehouse are
* `fixtures`      — serving recorded payloads instead of live endpoints
* `ducklake`      — attaching a DuckLake catalog, and diffing two snapshots
* `observability` — dlt/dbt/DuckDB metadata as queryable tables
* `bus_matrix`    — facts against conformed dimensions, out of the dbt manifest
* `db`            — single-row and scalar reads, without the Optional

Each takes its configuration as arguments; the project modules that call them
hold the constants and stay the entry points, so `python -m lake.lakehouse` and
friends keep working. **Nothing here knows what this project's data is about** —
that is the rule that lets the package be copied into the next project whole.

Importing this package pulls in none of the submodules, so it costs no Polars or
DuckDB import to a caller that only wants `paths`.

Deliberately *not* here: `RawSchemaDltTranslator`, which stays in
`orchestration/assets.py`. Keying dlt resources as `raw/<resource>` so they meet
the keys dagster-dbt derives is the convention worth keeping, but the code for
it is twenty lines wrapped around two of that module's constants, and moving it
would put Dagster (an optional dependency group) behind a package import.

The `mds` console script is a thin convenience wrapper; the canonical entry
points are the `just` recipes and the module runners under ingest/ and
transform/.
"""


def main() -> None:
    print(
        "my-warehouse\n"
        "  just run        # ingest -> dbt build -> polars transform\n"
        "  just sql        # explore the DuckDB warehouse (DuckDB CLI)\n"
        "See README.md for the full pipeline."
    )
