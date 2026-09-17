# Tests

Two tiers, deliberately separated.

## `just test` — unit tests, mocked

`tests/test_*.py`. Every HTTP call is mocked; nothing touches the network or the
warehouse. Most of what is here is not testing the example's data — it is
holding the *plumbing* to the tree:

| File | What it pins down |
|---|---|
| `test_fixtures.py` | that every URL the pipeline can build resolves to a recorded fixture, that no two routes claim one URL, and that no fixture is orphaned |
| `test_definitions.py` | that every asset and check is registered, and that every dlt resource has a description and a row in the observability list |
| `test_asset_checks.py` | the asset checks' own logic, against throwaway DuckDB files |
| `test_exposures.py` | that `_exposures.yml` still describes what the Evidence pages read — a stale exposure is invisible, since `dbt build` stays green |
| `test_report.py` | that the table-to-model maps in `publish/build_report.py` match the SQL the pages actually run |
| `test_additivity.py` | that every numeric mart column declares whether it may be summed |
| `test_bus_matrix.py` | that the matrix in `docs/WAREHOUSE.md` is what the manifest currently implies |
| `test_lakehouse.py` | the substitute for DuckLake's change feed, and that the three spellings of the `lakehouse` alias agree |
| `test_workflows.py` | the hand-maintained lists in `.github/` and the justfile |
| `test_skills.py` | that every path and `just` recipe the skills cite exists |
| `test_agent_instructions.py` | that each agent's entry point still reaches `AGENTS.md` |

Several of these guard **a duplication that has to exist** — a list in one file
that must agree with the tree, where nothing else would notice the drift. That
is the kind of test worth adding here. Testing that dbt joins correctly is dbt's
job (`just dbt-unit-test`), and testing the data itself is a dbt data test.

## `just test-pipeline` — integration against fixtures

Runs the real `ingest → dbt build → Polars` into a throwaway DuckDB file with
`INGEST_FIXTURES=1`, so dlt's schema inference, every dbt model and test, and
the Polars layer all execute — offline and deterministically. This is what
`.github/workflows/ci.yml` runs (via the Dagster asset graph, so the asset
checks are evaluated too).

It sets `WAREHOUSE_PATH` to a temp file, `LAKEHOUSE_DIR` to a temp directory
beside it, and dbt's artifact paths with them — and, when the landing zone's
Parquet is in a bucket, `LAKEHOUSE_DATA_PATH` to a `test-pipeline/` prefix in
that bucket. **Don't drop any of them**:
without them a fixture run overwrites the real warehouse and landing zone, and
files the fixture build's timings in the real build history. The fixture run
passes either way — what breaks is the next command against real data.

## `just coverage`

`coverage run -m pytest`, configured in `pyproject.toml`. It reports and gates
nothing: there is no `fail_under`, nothing in CI runs it, and pytest runs
*under* coverage rather than loading a plugin, so there is no flag to leave
switched on by accident.

It measures the first tier only, so the layers `just test-pipeline` exercises
end to end read low — understated, not untested.

## `tests/fixtures/ingest/` — the recorded payloads

Produced by `just record-fixtures` (`scripts/record_fixtures.py`), which fetches
each live source once and writes it here.

Rows may be trimmed to keep the files small; **columns never are**. Dropping
unused columns would let a renamed upstream field pass CI against a fixture that
agrees with a `stg_` model no longer matching reality. Keep each fixture in the
source's own format — a gzipped CSV stays a gzipped CSV — so the parsing path
production uses runs in CI too.

**`.gitignore` excepts `tests/fixtures/ingest/*.csv` from its `*.csv` rule.**
Without that line a recorded CSV passes locally and is never committed, and CI
fails with `No such file or directory`. A fixture in a new directory needs its
own exception.

Re-record when a source changes shape, and commit the result.
`.github/workflows/nightly.yml` is what tells you it is time: it runs the same
graph against the live endpoints daily and opens an issue when they have moved.
