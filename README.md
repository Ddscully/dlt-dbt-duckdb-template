# my-warehouse

A template for a small, local, end-to-end analytics stack. Everything runs on
one machine with `uv` against a single DuckDB file — no cloud warehouse, no
containers, no accounts.

```
dlt (EL) → DuckLake (raw) → dbt (staging/marts) → Polars (heavy T) → Evidence (BI)
   data/lakehouse/            └────────▶ data/warehouse.duckdb
                    all orchestrated by Dagster
```

It ships **one worked source end to end** — monthly gold prices, a CSV with no
credentials — so that `just test-pipeline` and the Dagster asset graph are green
from the first commit, and so there is a working example of each layer to copy
rather than a skeleton to guess at.

## Start

```bash
uv tool install rust-just     # if you don't have `just`
just setup                    # venv + the DuckLake extension
just test-pipeline            # the whole pipeline against the recorded fixture
just run                      # the whole pipeline against the live source
just sql                      # poke at the result
```

Then make it yours:

```bash
uv run python -m scripts.rename_project acme_metrics   # see "Renaming", below
```

`just` on its own lists every recipe. The ones you will use most:

| Command | What it does |
|---|---|
| `just run` | ingest → dbt build → Polars → pipeline status, in shell order |
| `just materialize` | the same pipeline ordered by the Dagster asset graph |
| `just dagster` | the Dagster UI on :3000 — graph, runs, freshness, checks |
| `just test` | the mocked unit tests: no network, no warehouse |
| `just test-pipeline` | the real modules end to end against checked-in fixtures |
| `just report` | build the Evidence dashboard (needs Node) |
| `just where` | which warehouse file and landing zone the recipes will use |

## What is where

```
ingest/     dlt — `sources/` is one module per publisher; `pipeline.py` is
            coordination (which resources exist, how they group, the pipeline)
lake/       the DuckLake landing zone, where `raw` lives
dbt/        staging → marts, with contracts, tests and groups
transform/  the Polars layer, for what SQL models badly
orchestration/  the Dagster asset graph over all of the above
publish/    the boundary outward: the Evidence site and the bus matrix
reports/    the Evidence dashboard
scripts/    one-off utilities: fixture recording, renaming the project
src/modern_data_stack/   the domain-neutral mechanisms every layer calls
tests/      two tiers — see tests/README.md
```

[`docs/WAREHOUSE.md`](docs/WAREHOUSE.md) is what each schema holds and why `raw`
is in a separate file. [`docs/STYLE_GUIDE.md`](docs/STYLE_GUIDE.md) is how the
SQL is written. [`AGENTS.md`](AGENTS.md) is the instructions file for coding
agents, and is worth reading as a human too — it is the short version of
everything that bites.

## Adding your own source

The whole loop is the `adding-a-data-source` skill in
[`.agents/skills/`](.agents/skills/adding-a-data-source/SKILL.md). In short: a
dlt resource in `ingest/sources/`, a row in `_sources.yml`, a `stg_` model, a
mart with a contract and a uniqueness test, a recorded fixture so CI stays
offline, and a page in `reports/pages/`. Then delete the gold example.

## Renaming (template scaffolding — delete this section once you have)

Out of the box the project is called `my_warehouse` / `my-warehouse`. That name
appears in `pyproject.toml`, `dbt_project.yml` (four keys), `dbt/profiles.yml`,
the dlt pipeline name, the Dagster code location and the Evidence package.
`scripts/rename_project.py` replaces both spellings across every tracked file —
including this paragraph, which is why it is worth deleting rather than
believing afterwards. Run it on a clean tree: `git checkout .` is how you undo
a typo'd name, since the script has only the one placeholder to look for.

The **package** stays `modern_data_stack` whatever the project is called:
`[tool.uv.build-backend] module-name` in `pyproject.toml` decouples the two, so
renaming the project does not mean rewriting every import.

Two names must not be changed: the `lakehouse` ATTACH alias, which is baked into
stored view SQL, and the `warehouse.duckdb` filename, which dbt writes into
fully-qualified view definitions.

Renaming the *directory* is a separate thing, and it invalidates an existing
DuckLake catalog: the catalog stores its `data_path` as an absolute string, so
the next command fails with `DATA_PATH parameter ... does not match existing
data path`, naming two paths rather than the move. After moving the project,
delete `data/` and re-run, or attach once with `OVERRIDE_DATA_PATH`. Moving it
also leaves `.venv/bin/activate` pointing at the old path — `uv run` does not
read that file, so only `source .venv/bin/activate` notices; `uv sync` alone
does not repair it, so recreate the venv.

Also yours to edit: the `authors` line in `pyproject.toml`, the owner in
`dbt/models/_groups.yml` and `_exposures.yml`, and `LICENSE`.

## Requirements

- Python 3.13 (`.python-version`), via `uv`
- `just`
- Node 18+, but only for `just report` and the Evidence site

## Licence

MIT — see [LICENSE](LICENSE). The example source, monthly gold prices from
[datasets/gold-prices](https://github.com/datasets/gold-prices), is ODC-PDDL.
