-- Fact: one row per resale transaction (the grain).
--
-- What goes where: anything you'd filter or group by and that is shared by many sales
-- (town, flat type, model, month) lives in a dimension and appears here only as a key.
-- The numbers you add up or average (the measures) live here. Block, street and storey
-- describe only this one sale, so they stay on the fact too ("degenerate dimensions").
select
    (extract(year from s.transaction_month) * 100 + extract(month from s.transaction_month))::int
        as date_key,
    {{ surrogate_key('s.town') }} as town_key,
    {{ surrogate_key('s.flat_type') }} as flat_type_key,
    {{ surrogate_key('s.flat_model') }} as flat_model_key,

    s.block,
    s.street_name,
    s.storey_range,
    s.storey_min,
    s.storey_max,
    s.lease_commence_year,

    -- measures
    s.resale_price,
    s.floor_area_sqm,
    s.price_per_sqm,
    s.remaining_lease_months,

    s.loaded_at
from {{ ref('stg_resale_transactions') }} as s
