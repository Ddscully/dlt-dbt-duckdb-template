"""Monthly gold prices: one whole-file CSV download.

`datasets/gold-prices` on GitHub (ODC-PDDL): one row per month since 1833, the
price in US dollars per troy ounce. No auth, no pagination, no incremental
state, so it lands with `write_disposition="replace"`.
"""

from __future__ import annotations

import dlt
import polars as pl

from ingest import http

GOLD_PRICES_MONTHLY = "https://raw.githubusercontent.com/datasets/gold-prices/main/data/monthly.csv"


# Fetched through `http.get_bytes`, with its retries, rather than by handing
# Polars the URL, which tries once. infer_schema_length=None scans the whole
# file, so a column empty in its first rows is still typed from the rows that
# have it.
@dlt.resource(name="gold_prices_monthly", write_disposition="replace")
def gold_prices_monthly():
    yield pl.read_csv(http.get_bytes(GOLD_PRICES_MONTHLY), infer_schema_length=None).to_dicts()
