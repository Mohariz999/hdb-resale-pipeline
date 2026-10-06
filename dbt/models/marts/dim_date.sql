-- Dimension: one row per month, the grain of the source data.
--
-- generate_series fills every month between the first and last sale, so a month with no
-- sales in a town still exists here (useful for charts with no gaps).
with bounds as (
    select min(transaction_month) as first_month, max(transaction_month) as last_month
    from {{ ref('stg_resale_transactions') }}
),

months as (
    select generate_series(first_month, last_month, interval '1 month')::date as month_start
    from bounds
)

select
    -- 202609: readable, sorts correctly, and the usual convention for date keys
    (extract(year from month_start) * 100 + extract(month from month_start))::int as date_key,
    month_start,
    extract(year from month_start)::int as year,
    extract(quarter from month_start)::int as quarter,
    extract(month from month_start)::int as month_number,
    trim(to_char(month_start, 'Month')) as month_name,
    to_char(month_start, 'YYYY-MM') as year_month
from months
