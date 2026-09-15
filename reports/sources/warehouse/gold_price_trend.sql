-- Named columns, never `select *`: an Evidence source ships every column it
-- selects to every visitor of the site.
select month_start, rolling_12m_avg_usd, yoy_change_pct
from analytics.gold_price_trend
