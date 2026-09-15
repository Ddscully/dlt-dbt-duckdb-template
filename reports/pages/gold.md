---
title: Gold
---

```sql gold
select month_start, price_usd_per_troy_oz
from warehouse.gold_price_month
order by month_start
```

The monthly price, as the dbt mart publishes it.

<LineChart data={gold} x=month_start y=price_usd_per_troy_oz/>

```sql trend
select month_start, rolling_12m_avg_usd, yoy_change_pct
from warehouse.gold_price_trend
order by month_start
```

The rolling twelve-month average and the change on a year earlier, both from
the Polars layer (`transform/gold_price_trend.py`).

<LineChart data={trend} x=month_start y=rolling_12m_avg_usd/>

<LineChart data={trend} x=month_start y=yoy_change_pct yFmt=pct1/>
