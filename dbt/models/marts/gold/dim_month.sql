-- One row per calendar month from the first price to the current month: the
-- spine the fact joins onto, so a month with no price is a row with a null
-- rather than a missing row.
with spine as (
    {{ dbt_utils.date_spine(
        datepart="month",
        start_date="cast('1833-01-01' as date)",
        end_date="cast(date_trunc('month', current_date) + interval 1 month as date)"
    ) }}
)

select
    cast(date_month as date) as month_start,
    cast(extract(year from date_month) as integer) as calendar_year,
    cast(extract(month from date_month) as integer) as calendar_month
from spine
