-- Monthly gold prices, cleaned to (price_month) grain.
with source as (
    select * from {{ source('raw', 'gold_prices_monthly') }}
)

select
    cast(strptime(date, '%Y-%m') as date) as price_month,
    price as price_usd_per_troy_oz,
    to_timestamp(cast(_dlt_load_id as double)) as source_loaded_at
from source
