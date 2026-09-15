-- The monthly gold price a consumer reads, on the month spine: a month the
-- publisher has not priced yet is a row with a null price, not an absent row.
with months as (
    select * from {{ ref('dim_month') }}
),

prices as (
    select * from {{ ref('stg_gold_prices') }}
)

select
    months.month_start,
    prices.price_usd_per_troy_oz
from months
left join prices
    on months.month_start = prices.price_month
