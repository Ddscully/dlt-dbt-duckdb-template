"""Polars transform: read a dbt mart from DuckDB, derive a trend, and write it
back as a table Evidence can query.

This layer is for what SQL models badly — window arithmetic over a long series,
anything needing a real dataframe. Keep the *joining* in dbt, where the tests,
contracts and lineage are, and read one mart here.

Run:  uv run python -m transform.gold_price_trend
"""

from __future__ import annotations

import duckdb
import polars as pl

from modern_data_stack import db
from modern_data_stack.paths import warehouse_path

DUCKDB_PATH = warehouse_path()

# A year of monthly observations. Named rather than inlined because the column
# name states it: change one and the other is wrong without failing.
WINDOW_MONTHS = 12


def build_gold_price_trend(df: pl.DataFrame) -> pl.DataFrame:
    """The rolling year and the change on a year earlier, per priced month.

    **Both windows are computed over the complete `dim_month` spine, and the
    unpriced months are dropped afterwards.** That ordering is the whole point
    of the fact sitting on a spine: over the spine, `shift(12)` is twelve
    *months*, and `rolling_mean(12)` is a calendar year. Filter first and both
    become "twelve priced rows", so a series with a gap in it — most real
    monthly series have one — would compare the wrong period and report it as a
    year-on-year change, with nothing to say it had.

    A window that overlaps an unpriced month yields null rather than a number
    computed from fewer observations, which is the honest answer and the reason
    this is worth doing in Polars rather than in SQL.
    """
    spine = df.sort("month_start")
    return spine.with_columns(
        pl.col("price_usd_per_troy_oz")
        .rolling_mean(window_size=WINDOW_MONTHS)
        .alias("rolling_12m_avg_usd"),
        (
            (pl.col("price_usd_per_troy_oz") / pl.col("price_usd_per_troy_oz").shift(WINDOW_MONTHS))
            - 1
        ).alias("yoy_change_pct"),
    ).filter(pl.col("price_usd_per_troy_oz").is_not_null())


def run(duckdb_path: str = DUCKDB_PATH) -> int:
    """Read the mart, derive the trend, write `analytics.gold_price_trend`.

    Returns the row count written (used as asset metadata by the orchestrator).
    """
    con = duckdb.connect(duckdb_path)
    try:
        out = build_gold_price_trend(con.sql("select * from marts.fct_gold_price_month").pl())
        db.write_frames(con, {"gold_price_trend": out}, "analytics")
        return out.height
    finally:
        con.close()


def main() -> None:
    print(f"wrote analytics.gold_price_trend ({run()} rows)")


if __name__ == "__main__":
    main()
