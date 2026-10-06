"""`transform.gold_price_trend`: windows over the month spine, read lazily.

The transform reads the mart as a lazy plan, so Polars may move a filter into
the DuckDB scan. The one filter here must not move: it drops unpriced months
*after* the windows, and run before them it turns "twelve months" into "twelve
priced rows" and reports the wrong period as a year-on-year change. So the test
runs `run()` itself, through the real scan, on a spine with a gap in it.
"""

from __future__ import annotations

import datetime as dt

import duckdb
import pytest

from transform import gold_price_trend

# Thirty months from January 2020, with June 2020 unpriced: the gap every real
# monthly series eventually has.
GAP = dt.date(2020, 6, 1)


def _month(i: int) -> dt.date:
    return dt.date(2020 + i // 12, i % 12 + 1, 1)


@pytest.fixture
def trend(tmp_path) -> dict[dt.date, tuple]:
    path = tmp_path / "warehouse.duckdb"
    con = duckdb.connect(str(path))
    try:
        con.execute("create schema marts")
        con.execute(
            "create table marts.fct_gold_price_month "
            "(month_start date, price_usd_per_troy_oz double)"
        )
        con.executemany(
            "insert into marts.fct_gold_price_month values (?, ?)",
            [(_month(i), None if _month(i) == GAP else 100.0 + i) for i in range(30)],
        )
    finally:
        con.close()

    gold_price_trend.run(str(path))

    con = duckdb.connect(str(path), read_only=True)
    try:
        rows = con.execute(
            "select month_start, price_usd_per_troy_oz, rolling_12m_avg_usd, yoy_change_pct "
            "from analytics.gold_price_trend"
        ).fetchall()
    finally:
        con.close()
    return {row[0]: row[1:] for row in rows}


def test_unpriced_months_are_dropped_after_the_windows(trend):
    assert GAP not in trend
    assert len(trend) == 29


def test_the_change_on_a_year_earlier_is_twelve_calendar_months(trend):
    """January 2021 against January 2020: over the spine, `shift(12)` is a
    year. Filter first and it is twelve priced rows, which reach back past the
    start of the series."""
    price, _, yoy = trend[dt.date(2021, 1, 1)]
    assert price == 112.0
    assert yoy == pytest.approx(112.0 / 100.0 - 1)


def test_a_window_over_the_gap_is_null_rather_than_short(trend):
    """June 2021's year-earlier month is the gap, and every rolling year that
    contains it is missing a month. Each is null; computed after a filter,
    each would be a number from the wrong months."""
    assert trend[dt.date(2021, 6, 1)][2] is None
    assert trend[dt.date(2021, 3, 1)][1] is None
    # The first full year clear of the gap is a number again.
    assert trend[dt.date(2021, 6, 1)][1] is not None
