---
title: Gold
---

```sql gold
select month_start, price_usd_per_troy_oz
from warehouse.gold_price_month
order by month_start
```

<LineChart data={gold} x=month_start y=price_usd_per_troy_oz/>
