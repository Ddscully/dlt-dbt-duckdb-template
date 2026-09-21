# Lightweight orchestration. `just <recipe>`; run `just` to list.
# (Install: `uv tool install rust-just` or use your package manager.)
#
# `just --list` shows only the comment line directly above a recipe, so each
# recipe's one-line summary is the last line of its comment block.

set dotenv-load := true

# Dagster run/event storage (gitignored except dagster.yaml). `env(...)` so a
# caller's value wins — `.github/actions/setup` sets it in CI. A directory
# without dagster.yaml falls back to Dagster's defaults, ten runs at once against
# a file DuckDB lets one process write among them; a symlink to the checked-in
# file works.
export DAGSTER_HOME := env("DAGSTER_HOME", justfile_directory() / ".dagster")

# Absolute on purpose. DuckLake records the catalog's `data_path` as given and
# compares it as a string on every attach; dbt runs from `dbt/` and the Python
# layers from the repo root, so one relative path becomes two strings and the
# attach is refused. The warehouse file needs no such care: it records no path.
export LAKEHOUSE_DIR := env("LAKEHOUSE_DIR", justfile_directory() / "data/lakehouse")

default:
    @just --list

# dbt's log names the target (`target='dev'`, the only one) and never the file,
# so every recipe that writes to the warehouse or the landing zone depends on
# this. `test-pipeline` does not: it exports its own WAREHOUSE_PATH, and this
# would print the outer value.
# Print which warehouse file and landing zone the pipeline recipes will use
where: _no-dbt-dotenv
    @echo "warehouse: ${WAREHOUSE_PATH:-(unset - this repo's data/warehouse.duckdb)}"
    @echo "lakehouse: $LAKEHOUSE_DIR"
    @echo "lakehouse data: ${LAKEHOUSE_DATA_PATH:-(unset - $LAKEHOUSE_DIR/data/)}"

# Every recipe that writes depends on this, through `where` or directly (the ones
# that export their own WAREHOUSE_PATH). dbt 1.12 loads a .env from its working
# directory, which is dbt/ for every recipe, and a value there fills any variable
# the shell leaves unset — WAREHOUSE_PATH in `where`'s recipes, and whatever a
# recipe does not export in the rest — so dbt would build against a file no
# recipe printed. The repo-root .env is safe: `dotenv-load` exports it to the
# recipes, so `where` shows it.
_no-dbt-dotenv:
    @if [ -e "{{ justfile_directory() }}/dbt/.env" ]; then \
        echo "refusing: dbt/.env exists, and dbt reads it for any variable this recipe leaves unset." >&2; \
        echo "Move its values to the repo-root .env, which just exports and 'just where' prints, then delete it." >&2; \
        exit 1; \
    fi

# DuckLake is a binary from extensions.duckdb.org that no lockfile can name.
# DuckDB would autoload it on first use; installing it here moves the download,
# and any network failure, out of a `dbt build` inside a Dagster op. DuckDB
# fetches the build for its own version, so under `uv run` it matches uv.lock.
# One-time: install runtime + dev deps into the uv-managed venv, and the DuckLake extension
setup:
    uv sync --group dev --group orchestration
    uv run python -c "import duckdb; duckdb.connect().execute('install ducklake')"

# EL: pull public sources into the DuckLake landing zone
ingest: where
    uv run python -m ingest.pipeline

# The state lives in dlt's own directory (`~/.dlt/pipelines/<name>`), keyed on
# the pipeline name and not on the destination, so no warehouse query can show
# it. `just dlt-state my_warehouse_fixtures` reads the fixture pipeline's — the
# separate name is what stops a fixture run handing its watermarks to a real one.
# Show dlt's incremental state — each resource's watermark
dlt-state pipeline="my_warehouse":
    uv run dlt pipeline {{ pipeline }} info -v

# The mkdir is needed because dbt's profile attaches the DuckLake catalog on
# every invocation (`dbt parse` and sqlfluff's templater included), and DuckLake
# will not create the catalog's parent directory.
# Install dbt packages (dbt_utils) into the gitignored dbt/dbt_packages/
dbt-deps:
    mkdir -p "$LAKEHOUSE_DIR"
    cd dbt && uv run dbt deps

# T: build + test dbt models
dbt-build: where dbt-deps
    cd dbt && uv run dbt build

# `@dbt_assets` reads dbt/target/manifest.json at import time and dbt/target/ is
# gitignored, so every headless `dagster` recipe depends on this. `just dagster`
# does not: `prepare_if_dev()` parses under the dev CLI.
# Write dbt/target/manifest.json — the Dagster graph won't load without it
dbt-parse: dbt-deps
    cd dbt && uv run dbt parse

# Selects nothing until a model carries a `unit_tests:` block — the fixtures go
# in `dbt/tests/fixtures/`, which `.gitignore` already excepts from `*.csv`. The
# models' parents must exist in the warehouse (schema only); run `just dbt-build`
# once if they don't.
# dbt unit tests only — mocked inputs, the inner loop for model logic
dbt-unit-test: dbt-deps
    cd dbt && uv run dbt test --select test_type:unit

# Thresholds are in models/staging/_sources.yml. `_dlt_load_id` is stamped at
# ingest, so this measures when the pipeline last ran, not when a publisher last
# published.
# Is the warehouse stale? `dbt source freshness` against dlt's load ids
dbt-freshness: dbt-deps
    cd dbt && uv run dbt source freshness

# Needs a built warehouse, or the catalog's columns come back untyped.
# Render the dbt metadata layer to dbt/target/ — columns, contracts, groups, exposures, versions, tests
dbt-docs: dbt-deps
    cd dbt && uv run dbt docs generate

# Serve the dbt docs site on :8080, regenerated first (blocks; ctrl-c to stop)
dbt-docs-serve: dbt-docs
    cd dbt && uv run dbt docs serve

# Report what the DuckLake landing zone holds — tables, rows, snapshots (read-only)
lakehouse:
    uv run python -m lake.lakehouse

# Polars derived metrics
transform: where
    uv run python -m transform.gold_price_trend

# Run after dbt-build: it reads dbt_test__audit and dbt's artifacts.
# Pipeline observability tables (load times, layer inventory, dbt test failures)
pipeline-status: where
    uv run python -m transform.pipeline_status

# Full pipeline via shell ordering (see `just materialize` for the graph-aware one)
run: ingest dbt-build transform pipeline-status

# Unit tests — mocked API payloads, no network, no warehouse
test:
    uv run pytest

# Two commands so a failing suite stops before a percentage is printed.
# Line + branch coverage of `just test` — reports, gates nothing
coverage:
    uv run coverage run -m pytest
    uv run coverage report

# The whole pipeline against checked-in fixtures, into a throwaway warehouse — what CI runs
test-pipeline: _no-dbt-dotenv
    #!/usr/bin/env bash
    set -euo pipefail
    export INGEST_FIXTURES=1
    # Every piece of state the next real command reads is redirected, or the
    # fixture run leaks into it: the warehouse, the landing zone (which holds
    # the weather archive no rebuild can afford), and dbt's artifacts (which
    # `pipeline-status` files into `analytics.pipeline_runs`).
    export WAREHOUSE_PATH="$(mktemp -d)/warehouse.duckdb"
    export LAKEHOUSE_DIR="$(dirname "$WAREHOUSE_PATH")/lakehouse"
    export DBT_TARGET_PATH="$(dirname "$WAREHOUSE_PATH")/dbt-target"
    export DBT_MANIFEST_PATH="$DBT_TARGET_PATH/manifest.json"
    export DBT_RUN_RESULTS_PATH="$DBT_TARGET_PATH/run_results.json"
    # A landing zone in a bucket keeps its storage and loses its prefix: the
    # fixture Parquet goes under `test-pipeline/<tmp>/` in the same bucket, so an
    # S3 setup is what gets tested, under a prefix no real run uses.
    if [[ "${LAKEHOUSE_DATA_PATH:-}" == s3://* ]]; then
      bucket="${LAKEHOUSE_DATA_PATH#s3://}"
      export LAKEHOUSE_DATA_PATH="s3://${bucket%%/*}/test-pipeline/$(basename "$(dirname "$WAREHOUSE_PATH")")/"
      echo "fixture lakehouse data: $LAKEHOUSE_DATA_PATH"
    fi
    echo "fixture warehouse: $WAREHOUSE_PATH"
    uv run python -m ingest.pipeline
    cd dbt && uv run dbt deps && uv run dbt build --target-path "$DBT_TARGET_PATH" && cd ..
    uv run python -m transform.gold_price_trend
    uv run python -m transform.pipeline_status
    uv run python -m lake.lakehouse

# Reads dbt/target/manifest.json (`just dbt-parse`), never the warehouse: grains
# come from the uniqueness tests, columns from the enforced contracts.
# Which conformed dimensions does each fact carry? -> docs/WAREHOUSE.md
bus-matrix:
    uv run python -m publish.bus_matrix

# Re-record the fixtures from the live APIs (hits the network; commit the diff)
record-fixtures:
    uv run python -m scripts.record_fixtures

# Prints a SupersessionWarning naming `dg dev`. Every Dagster CLI command here
# carries one, and none is on a removal clock (AGENTS.md's orchestration section).
# Dagster UI on :3000 — asset graph, run history, freshness, checks
dagster:
    mkdir -p "$DAGSTER_HOME"
    uv run --group orchestration dagster dev

# See orchestration/definitions.py for why the site is a second job.
# Full pipeline ordered by the asset graph, minus the Evidence site
materialize: where dbt-parse
    mkdir -p "$DAGSTER_HOME"
    uv run --group orchestration dagster job execute -m orchestration.definitions -j full_refresh

# The same graph with the Evidence site on the end of it (needs Node)
materialize-site: where dbt-parse
    mkdir -p "$DAGSTER_HOME"
    uv run --group orchestration dagster job execute -m orchestration.definitions -j publish_site

# A bare prefix is not a glob: `marts/*` means "downstream of the key `marts/`",
# matches nothing and exits 0. Write `key:"marts/*"`; `group:`, `kind:`,
# `sinks(...)` and `roots(...)` also work. Check with `just materialize-preview`.
# Materialize a selection, e.g. `just materialize-select 'raw/gold_prices_monthly*'` (* = all downstream, + = one layer)
materialize-select selection: where dbt-parse
    mkdir -p "$DAGSTER_HOME"
    uv run --group orchestration dagster asset materialize \
        -m orchestration.definitions --select '{{ selection }}'

# A selection matching no assets is not an error to `materialize`, so look first.
# Print the assets a selection resolves to, without materializing any of them
materialize-preview selection: dbt-parse
    uv run --group orchestration dagster asset list \
        -m orchestration.definitions --select '{{ selection }}'

# `dbt-parse` first: the code location imports the dbt project, so without a
# manifest it fails to load rather than reporting what is unregistered. No `-m`,
# which leaves the location named as `[tool.dagster]` names it.
# Check the code location loads and every definition is registered
validate: dbt-parse
    uv run --group orchestration dagster definitions validate

# Read-only unless `write`, so a session cannot change anything by accident.
# Either mode blocks a build while it is open: DuckDB allows one writer or many
# readers, never both. The lakehouse is attached in the same mode because the
# staging views read `lakehouse.raw`; without it they fail with `Catalog
# "lakehouse" does not exist!`. The CLI is the `duckdb-cli` dev dependency.
#
# With LAKEHOUSE_DATA_PATH set it needs the S3 secret too — the third spelling,
# after `storage_secret()` and the dbt profile. The keys go in through the CLI's
# `getenv`, so they never appear in the process list.
# Open the warehouse in the DuckDB CLI (`just sql write` for a writer)
sql mode="read":
    #!/usr/bin/env bash
    set -euo pipefail
    warehouse="${WAREHOUSE_PATH:-data/warehouse.duckdb}"
    data="${LAKEHOUSE_DATA_PATH:-$LAKEHOUSE_DIR/data/}"
    secret=""
    if [ -n "${LAKEHOUSE_DATA_PATH:-}" ]; then
      unset_msg="is unset, and LAKEHOUSE_DATA_PATH names a bucket (see .env.example)"
      endpoint="${LAKEHOUSE_S3_ENDPOINT:?$unset_msg}"
      : "${AWS_ACCESS_KEY_ID:?$unset_msg}" "${AWS_SECRET_ACCESS_KEY:?$unset_msg}"
      ssl=true; [[ "$endpoint" == http://* ]] && ssl=false
      host="${endpoint#*://}"
      secret="install httpfs; load httpfs; create secret (type s3, key_id getenv('AWS_ACCESS_KEY_ID'), secret getenv('AWS_SECRET_ACCESS_KEY'), endpoint '${host%/}', use_ssl $ssl, region '${AWS_REGION:-us-east-1}', url_style 'path', scope '$data');"
    fi
    attach="install ducklake; load ducklake; $secret attach 'ducklake:duckdb:$LAKEHOUSE_DIR/catalog.duckdb' as lakehouse (data_path '$data'"
    if [ "{{ mode }}" = "write" ]; then
      uv run duckdb "$warehouse" -cmd "$attach);"
    else
      uv run duckdb -readonly "$warehouse" -cmd "$attach, read_only);"
    fi

# From dbt/, because the dbt templater resolves the profile's relative
# `../data/…` default against the working directory. `snapshots` is passed only
# when it exists: sqlfluff exits `User Error: Specified path does not exist` on a
# directory git cannot track once its .gitkeep is gone, which would red-light CI
# for a project that simply has no snapshots.
# Lint the dbt models and snapshots with sqlfluff
lint: dbt-deps
    cd dbt && uv run sqlfluff lint models $([ -d snapshots ] && echo snapshots)

# ty is pre-1.0 and runs in neither pre-commit nor CI; `uv run` so the locked
# version answers. Suppressions go inline as `# ty: ignore[rule]`.
# Type-check the Python — reports, gates nothing
typecheck:
    uv run ty check

# The same module the `reports/evidence_site` asset calls.
# Build the Evidence dashboard (requires Node)
report:
    uv run python -m publish.build_report

# Evidence caches each source's schema and does not notice a column change, so
# use this rather than `report` after any mart or analytics column changes.
# Drop Evidence's schema cache, re-extract the sources, then build
report-clean:
    uv run python -m publish.build_report --clean

# `deep` adds reports/node_modules (restored by `just report`, needs Node).
#
# data/warehouse.duckdb is deliberately not a scope here. `analytics.pipeline_runs`
# accumulates one row per dbt node per invocation and no rebuild reproduces it,
# and a project that adds a dbt snapshot puts a second unreproducible table in
# the same file. Deleting it is `rm data/warehouse.duckdb`, which at least looks
# like what it is.
# Reclaim gitignored build output (`deep` also drops reports/node_modules)
clean scope="safe":
    #!/usr/bin/env bash
    set -euo pipefail
    cd "{{ justfile_directory() }}"

    freed=0
    drop() {
      for target in "$@"; do
        [ -e "$target" ] || continue
        size=$(du -sm "$target" 2>/dev/null | cut -f1)
        rm -rf "$target"
        freed=$((freed + size))
        printf '  removed %-28s %5s MB\n' "$target" "$size"
      done
    }

    # Regenerable, holding no state:
    #   dbt/target        `dbt parse` / `just dbt-build`   (the manifest)
    #   dbt/dbt_packages  `just dbt-deps`
    #   dbt/logs          any dbt command
    #   reports/build     `just report`
    #   reports/.evidence `just report-clean`
    #
    # data/lakehouse is deliberately absent: it is the only copy of every landing
    # table, so re-making it costs whatever the sources charge to fetch again.
    # `drop` takes exact paths and never globs, which is what keeps it out.
    drop dbt/target dbt/dbt_packages dbt/logs reports/build reports/.evidence

    # Dagster run/event storage. `.dagster/dagster.yaml` is checked in and stays.
    find .dagster -mindepth 1 -maxdepth 1 ! -name dagster.yaml -exec rm -rf {} + 2>/dev/null || true

    if [ "{{ scope }}" = "deep" ]; then
      drop reports/node_modules
    fi

    printf 'freed %s MB\n' "$freed"
