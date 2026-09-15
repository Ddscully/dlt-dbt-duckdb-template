# gold-warehouse

Monthly gold prices since 1833, loaded, modelled and published by a small,
local data stack:

```
dlt (EL) → DuckLake (raw) → dbt (staging/marts) → Evidence (BI)
             all orchestrated by Dagster, against one DuckDB file
```

## Quick start

```bash
just setup      # uv sync runtime + dev + orchestration
just run        # ingest -> dbt build -> pipeline status
just dagster    # ...or the same pipeline as an asset graph, UI on :3000
just sql        # the warehouse in the DuckDB CLI, lakehouse attached
```

No credentials at any point. `just test-pipeline` runs the whole pipeline
offline against recorded fixtures, into a throwaway warehouse.

## Tests

```bash
just test           # pytest: mocked payloads, no network
just test-pipeline  # the whole pipeline against recorded fixtures
```

CI runs both, plus the Dagster asset graph and the asset checks, entirely
offline. A nightly workflow runs the same graph against the live source.

## Where things are

[`AGENTS.md`](./AGENTS.md) is the guide for agents and people working in the
repo; [`docs/WAREHOUSE.md`](./docs/WAREHOUSE.md) describes the schemas,
[`docs/ORCHESTRATION.md`](./docs/ORCHESTRATION.md) the asset graph and
[`docs/STYLE_GUIDE.md`](./docs/STYLE_GUIDE.md) the SQL conventions.

## License

Code is [MIT](./LICENSE). The data is not this project's to license: the monthly
gold prices come from [datasets/gold-prices](https://github.com/datasets/gold-prices)
under the [ODC-PDDL 1.0](https://opendatacommons.org/licenses/pddl/1-0/) public
domain dedication, which asks for no attribution — the release credits the
publisher anyway, because a downloader should be able to find the source without
reading this repository. Re-check the licence whenever a source is added: the
release redistributes the data, which turns "we use public data" into "we
redistribute public data", and that is an obligation of its own.
