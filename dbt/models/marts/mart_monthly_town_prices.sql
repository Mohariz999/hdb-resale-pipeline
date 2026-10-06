-- Mart: the reusable business metric. One row per month x town x flat type.
--
-- Built from the star (fact joined to its dimensions), and denormalised on purpose:
-- business users and dashboards get readable names and need no joins.
select
    d.month_start,
    d.year,
    d.year_month,
    t.town,
    ft.flat_type,

    count(*) as transactions,
    -- Median, not average: a handful of very expensive flats drags an average up,
    -- while the median is the price of the "typical" sale.
    round(percentile_cont(0.5) within group (order by f.price_per_sqm)::numeric, 2)
        as median_price_per_sqm,
    round(percentile_cont(0.5) within group (order by f.resale_price)::numeric, 0)
        as median_resale_price,
    -- Sums let you roll months up correctly: sum(...) / sum(transactions) over a year
    -- is the true yearly average. Averaging monthly averages would weight a quiet month
    -- the same as a busy one.
    sum(f.price_per_sqm) as sum_price_per_sqm,
    round(avg(f.price_per_sqm), 2) as avg_price_per_sqm

from {{ ref('fct_resale_transactions') }} as f
join {{ ref('dim_date') }} as d on f.date_key = d.date_key
join {{ ref('dim_town') }} as t on f.town_key = t.town_key
join {{ ref('dim_flat_type') }} as ft on f.flat_type_key = ft.flat_type_key
group by 1, 2, 3, 4, 5
