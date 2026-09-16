# AGENTS.md

Guidance for coding agents (and humans) working in this repo.

## Reading this file

This is the one instructions file for every agent. `CLAUDE.md` imports it and
adds only Claude Code's plugin declarations; the project skills live in
`.agents/skills/`, with `.claude/skills` a symlink to them — Claude Code reads
only its own directory, and Codex only this one.

- **It stays under 32 KiB, because that is all Codex reads by default**, and it
  cuts the rest with nothing but a trace-log line. `project_doc_max_bytes` is
  user configuration the repo cannot set, so `tests/test_agent_instructions.py`
  holds the size instead: a section that would push past it belongs in a skill.
- **Gemini CLI reads `GEMINI.md`** unless `context.fileName` in its settings
  names `AGENTS.md`.

## What this is

A template for a local, end-to-end analytics stack. Everything runs with `uv`
against a single DuckDB file — no cloud warehouse.

```
dlt (EL) → DuckLake (raw) → dbt (staging/marts) → Polars (heavy T) → Evidence (BI)
   data/lakehouse/            └────────▶ data/warehouse.duckdb
                    all orchestrated by Dagster
```

One source ships end to end (monthly gold prices) so every layer has a worked
example. Replacing it is the `adding-a-data-source` skill.

The README is the tour; [`docs/WAREHOUSE.md`](docs/WAREHOUSE.md) is what each
schema holds, and [`docs/STYLE_GUIDE.md`](docs/STYLE_GUIDE.md) is how the SQL is
written. What it cost to learn sits here and in the skills, so a change to how a
layer works usually needs an edit in `docs/` **and** in one of those.

## The layers, and what each directory is for

```
ingest/     dlt — `sources/` is one module per publisher; `pipeline.py` is
            coordination (which resources exist, how they group, the pipeline)
lake/       the DuckLake landing zone
dbt/        staging → marts
transform/  the Polars derived metrics
orchestration/  the Dagster asset graph over all of the above
publish/    the boundary outward: the Evidence site and the bus matrix
scripts/    genuinely one-off: fixture recording, renaming the project
src/modern_data_stack/   the domain-neutral mechanisms every layer calls
```

- **`ingest/` is one module per publisher**, plus `pipeline.py`'s coordination
  tuples. Shared helpers are reached as `http.get_json(...)`, never imported by
  name, so a source's unit test can patch them through the module — a name bound
  at import time is not looked up through that patch.
- **The package takes its configuration as arguments.** `lake/lakehouse.py`,
  `transform/*.py` and the rest hold this project's constants and stay the entry
  points. Nothing under `src/` knows what this project's data is about.
- **`modern_data_stack.paths` raises when it cannot find the project**, and a cwd
  fallback must not be added: it would resolve `./data/warehouse.duckdb`, which
  DuckDB *creates*, so a run outside the tree goes green against an empty
  database.

## Commands

Use the `justfile` recipes (they map to plain `uv run …` commands); `just` on
its own lists them all.

| Command | What it does |
|---|---|
| `just setup` | `uv sync --group dev --group orchestration`, then `install ducklake` — an extension binary no lockfile can name |
| `just ingest` | run the dlt pipeline → `raw` in the DuckLake catalog |
| `just dbt-build` | `dbt deps` then `dbt build` |
| `just dbt-parse` | write `dbt/target/manifest.json` — the Dagster graph will not load without it |
| `just transform` | the Polars derived metrics → `analytics` |
| `just pipeline-status` | load times, layer inventory, dbt test state → `analytics.pipeline_*` |
| `just run` | ingest → dbt-build → transform → pipeline-status (shell ordering) |
| `just materialize` | the same pipeline, ordered by the asset graph |
| `just materialize-site` | the same, plus the Evidence site (needs Node) |
| `just materialize-preview '<sel>'` | what a selection resolves to, materializing nothing — zero matches still exits 0 |
| `just dagster` | Dagster UI on :3000 |
| `just test` / `just test-pipeline` | mocked unit tests; the whole pipeline against fixtures |
| `just lint` / `just typecheck` | sqlfluff over the dbt models; ty, gating nothing |
| `just report` / `just report-clean` | build the Evidence site (`--clean` drops the schema cache) |
| `just where` | which warehouse file and landing zone the recipes will use |
| `just sql` | the warehouse in the DuckDB CLI with the lakehouse attached, read-only |
| `just bus-matrix` | regenerate the bus matrix block in `docs/WAREHOUSE.md` |
| `just clean` | delete the gitignored build output |

Always run tools through `uv run` so they use the project venv, with
`--group orchestration` for anything that imports Dagster. dbt commands must run
from the `dbt/` directory (that's where `profiles.yml` lives).

**`uv sync` strips the venv; `uv run` does not.** A bare `uv sync` installs
`dev` alone and removes the `orchestration` group from under anything running.

## Agent skills

Skills follow the [Agent Skills](https://agentskills.io) standard, so one copy
serves every agent: they live in `.agents/skills/`, and `.claude/skills` is a
symlink to it.

| Skill | Load it for |
|-------|-------------|
| `adding-a-data-source` | a new source, resource or raw table, across every layer |
| `querying-the-warehouse` | SQL against the warehouse: read-only connections, the lock, schema names |
| `pipeline-observability` | `transform/pipeline_status.py` and the `pipeline_*` tables |
| `linting-and-type-checking` | sqlfluff, ruff and ty |

`tests/test_skills.py` checks every path and `just` recipe a skill cites, so a
citation cannot rot silently. **A new section in *this* file is a question about
where it belongs**: it stays only if every session needs it — not knowing it
does irreversible damage, gives a silent wrong answer, or is needed to find
everything else.

## Warehouse schemas

dlt lands `raw` in the DuckLake catalog under `data/lakehouse/`; dbt builds
everything else into `data/warehouse.duckdb`. The full account is
[`docs/WAREHOUSE.md`](docs/WAREHOUSE.md).

- **Clean schema names** come from `dbt/macros/generate_schema_name.sql`, which
  overrides dbt's default `<target>_<custom>` (which would give `main_marts`).
  Reference marts as `marts.fct_gold_price_month`, not `main_marts.…`.
- **One dbt target, by decision.** A target separates schemas *inside one
  database*; here `WAREHOUSE_PATH` swaps the whole database, and the macro keeps
  schema names identical everywhere. The cost: dbt's one "where am I" line names
  the target and never the file, so `just where` exists and every writing recipe
  depends on it.
- **The DuckDB lock is one writer XOR many readers**, across processes: a
  read-only connection fails while a build holds the file, and a build fails
  while anyone is reading it (`querying-the-warehouse`).
- **`LAKEHOUSE_DIR` must be absolute, so `just` exports it for every recipe.**
  DuckLake compares the stored `data_path` as a string, so dlt and dbt spelling
  one directory two ways is refused inside `dbt build`, a layer downstream of the
  cause. `.github/actions/setup` is the one definition of that environment for
  the workflows.
- **`data/lakehouse/` is the only copy of every landing table**, so `just clean`
  never takes it, and `analytics.pipeline_runs` is appended rather than rebuilt.

## dbt

- **`dbt deps` first, always.** `dbt/dbt_packages/` and the manifest are
  gitignored, so a fresh clone needs `dbt deps` before `dbt build`, `dbt parse`
  or `sqlfluff`, and `dbt parse` before the asset graph will load.
- **Test args go under `arguments:`, the key is `data_tests:`, and a yml holds
  one `unit_tests:` key** — a second block parses, and dbt merges the lists with
  only a deprecation warning.
- **Every test's failures are stored**: a red check's rows are in
  `select * from dbt_test__audit.<test_name>`.
- **Contracts are enforced on every mart model**, so a column changing type
  fails the build before it writes. **Never round-trip these ymls through
  PyYAML**: it reflows every description to add a scalar.
- **Select an asset prefix as `key:"marts/*"`.** A bare `marts/*` materialises
  nothing and exits 0; `just materialize-preview` shows what a selection
  resolves to.
- **dlt only widens types, and a load is two `run()`s**: the `replace` resources
  with `refresh="drop_resources"`, the `merge` resources without, or the refresh
  drops a merge table and its watermark. A new resource must join
  `FULL_REFRESH_RESOURCES` or `INCREMENTAL_RESOURCES`.

## Orchestration (`orchestration/`)

Dagster wraps the existing layers rather than replacing them: `ingest`, `dbt`
and `transform` stay independently runnable.

- **Asset keys are the join between the layers.** Rename a dbt source table
  without renaming the dlt resource and the graph silently splits in two — both
  halves still run.
- **Everything runs in one process**, because DuckDB takes one writer at a time.
- **Every asset and check is listed by hand in `definitions.py`**, and an
  omission is silent — `dagster definitions validate` passes.
- **`orchestration/assets.py` must not use `from __future__ import annotations`**:
  Dagster inspects the `context` parameter's annotation object.
- **The site is its own job** (`publish_site`), because it shells out to npm and
  every other job must run without Node.

## Testing (`tests/`)

Two tiers, and the split is the point — see [`tests/README.md`](tests/README.md).

- `just test` — mocked-payload unit tests over the Python layers. No network, no
  warehouse.
- `just test-pipeline` — the real modules end to end with `INGEST_FIXTURES=1`,
  every source served from `tests/fixtures/ingest/`, into a throwaway warehouse
  and landing zone. This is what CI runs, so a red PR build means the repo broke,
  not that a publisher was down.
- `.github/workflows/nightly.yml` runs the graph against the *live* sources daily
  and opens an issue — the signal that the fixtures have drifted.

- **A fixture run leaks through any state it does not override** —
  `WAREHOUSE_PATH`, `LAKEHOUSE_DIR` and dbt's artifact paths. The fixture run
  passes either way; the *next* command against the real warehouse is the one
  that is wrong.
- **A recorded fixture can be ignored by git and pass everywhere but CI.**
  `.gitignore` excepts `tests/fixtures/ingest/*.csv` from its `*.csv` rule, and
  `dbt/tests/fixtures/*.csv` for when this project grows dbt unit tests. A new
  directory of CSV fixtures needs its own line.
- **A test earns its place by mutation**: break the thing it guards and check
  that it goes red. A test that cannot be made to fail is decoration.
- **A wall-clock figure drifts, and nothing can guard it.** Date a timing when
  you write it.

## Style

SQL and model conventions are [`docs/STYLE_GUIDE.md`](docs/STYLE_GUIDE.md). How
sqlfluff, ruff and ty behave is the `linting-and-type-checking` skill.

- **Never run a bare `ruff format .`**: unlike the hook it is not scoped, and it
  rewrites deliberately aligned code blocks in markdown.
- **ruff's `--fix` deletes a comment that starts `# noqa`, even when it is
  prose.** Put the rule after the explanation and keep the real directive, with
  its colon, on the code line.

## Renaming this project (template scaffolding)

Until it is renamed the project is called `my_warehouse` / `my-warehouse`, and
`uv run python -m scripts.rename_project <name>` replaces both across every
tracked file — this paragraph included, so delete it once the rename is done
rather than reading it afterwards. `scripts/rename_project.py` skips itself and
stays the one honest record of what the placeholder was; `git checkout .` undoes
a typo'd run.

The package stays `modern_data_stack`, and two names must not change: the
`lakehouse` ATTACH alias and the `warehouse.duckdb` filename, both baked into
stored view SQL.
