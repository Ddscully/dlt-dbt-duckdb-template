# Style guide

How SQL and models are written here. Adapted from dbt Labs'
[How we style our dbt projects](https://docs.getdbt.com/best-practices/how-we-style/0-how-we-style-our-dbt-projects),
trimmed to what applies to a DuckDB-backed project and reconciled with the rules
`sqlfluff` already enforces.

Two markers below:

- **[lint]** — enforced by [`.sqlfluff`](../.sqlfluff); `just lint` fails on it.
- **[convention]** — not machine-checked. Reviewers (and agents) enforce it.

dbt Labs' point stands: the specific rules matter less than having them written
down and applied consistently. **Where you depart from them, write the departure
down** — a deliberate choice that is not recorded reads as an oversight, and the
next person "fixes" it.

## SQL formatting

- **[lint]** Four-space indents, spaces not tabs.
- **[lint]** Keywords, function names, identifiers and literals are lowercase.
- **[lint]** Trailing commas — `a,` at the end of a line, never `, a` leading.
- **[lint]** `as` is explicit when aliasing a column or a table:
  `price as price_usd_per_troy_oz`, `from prices as p`.
- **[lint]** Lines wrap at 120 characters.
- **[convention]** `union all` over `union` unless you mean to dedupe.
- **[convention]** Write the join type out: `left join`, `inner join` — never a
  bare `join`.
- **[convention]** Joins move left to right. A `right join` is a signal that the
  `from` table is the wrong one.
- **[convention]** Use `--` for comments that should survive into compiled SQL,
  and `{# #}` for notes meant only for the reader of the model file.
- **[lint]** A `filter` on a windowed aggregate dedents to the function's own
  indent and the `over` that follows it indents one level further. It looks
  wrong and it is what `layout.indent` wants; `sqlfluff fix --rules
  layout.indent` settles the argument in one pass. That command reaches the dbt
  templater, so it needs an absolute `LAKEHOUSE_DIR` like everything else —
  prefer `just lint` to find the line.

Deviations already taken: 120 columns rather than 80 (`max_line_length` in
`.sqlfluff`), short table aliases allowed in joins, and `group by` names the
columns rather than their positions — the grain is the contract, and spelling it
out makes a regression visible in the diff. dbt's sort/dist-key advice has no
DuckDB equivalent; materialization is set per directory in `dbt_project.yml`.

## Model structure

- **[convention]** Every `{{ ref() }}` and `{{ source() }}` goes in an import CTE
  at the top of the file, named after what it imports. The body then reads as
  SQL over local names.
- **[convention]** One model per landing table in `staging`, doing renaming,
  casting and filtering — not joining. Where a staging model does join or
  aggregate, say why in its description: it is a departure.
- **[convention]** `marts` is the interface. Everything a dashboard, a notebook
  or another project reads is a mart, contracts enforced, with `dim_`/`fct_`
  naming.
- **[convention]** Reach for `intermediate` only when two models would otherwise
  repeat the same join. `view`, not `ephemeral`: an ephemeral model has no
  relation, so it cannot carry unit tests.

## Naming

- **[convention]** Decide the grain first and name its keys the same everywhere.
  The fact and the dimension it joins to must spell the key identically — the
  bus matrix in [`WAREHOUSE.md`](WAREHOUSE.md) matches on the column name, so two
  spellings of one key show up as a missing mark rather than as a quiet
  mis-join.
- **[convention]** Renaming to the contract is the staging layer's job. A
  publisher's `iso_code`, `ccy` or `dt` becomes the project's name on the way
  through, once.
- **[convention]** Spell things out: `country_iso3`, not `cty`. Readability
  beats brevity; these names end up in a dashboard.
- **[convention]** Booleans read as assertions: `is_latest`, `has_price`.
- **[convention]** Timestamps carry their unit or zone when it is not obvious:
  `loaded_at`, `price_month`, `revenue_gbp`.

### Column ordering

Keys first, then dimensions, then measures, grouped by source with a `--`
comment naming the group. A wide fact is readable only if its columns arrive in
a predictable order.

## Tests and documentation

- **[convention]** Every model has a `description`, and every column whose
  meaning is not its name. "The month start" on `month_start` is noise; "null
  until the publisher prices the month" is not.
- **[convention]** Every grain gets a uniqueness test — on the key column, or
  `dbt_utils.unique_combination_of_columns` for a composite one. This is what
  the bus matrix reads as the grain.
- **[convention]** Every numeric mart column carries `meta: {additivity: …}`.
  `tests/test_additivity.py` fails on one that does not, and on a label that
  contradicts a ratio-shaped name.
- **[convention]** A relationship you expect to hold is a `relationships` test,
  not a comment — and the join that feeds it stays a `left join`, or the rows it
  exists to catch are deleted before it can see them.

## Python (ingest / transform / orchestration)

- **[convention]** ruff's defaults, 100 columns, run through pre-commit.
- **[convention]** Module docstrings say what the module is *for* and how to run
  it. Comments say why, not what.
- **[convention]** Configuration is a module-level constant in the project layer
  (`ingest/`, `lake/`, `transform/`), never inside `src/modern_data_stack/`.
  Nothing under `src/` knows what this project's data is about.
- **[convention]** A number in a comment or a docstring is a measurement. Date
  it, or leave it out.

## Where the rules live

| Rule | Enforced by |
|---|---|
| SQL layout, casing, line length | `.sqlfluff` → `just lint` (and the pre-commit hook) |
| Python formatting and lint | `.pre-commit-config.yaml` → ruff |
| Types | `just typecheck` → ty (reports, gates nothing) |
| Model contracts, tests, groups | `dbt/models/**/_*.yml` → `dbt build` |
| Everything else here | review |
