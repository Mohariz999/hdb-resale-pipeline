-- Staging: one clean, typed row per resale transaction.
--
-- raw.resale_transactions is TEXT-only, exactly as data.gov.sg sent it. Everything that
-- turns that text into usable data happens here, in one place, so every model downstream
-- (the star schema in marts/) reads clean data and never touches raw.

with source as (

    select * from {{ source('raw', 'resale_transactions') }}

),

cleaned as (

    select
        -- "2026-09" is a month, so store it as the first day of that month
        to_date(month, 'YYYY-MM') as transaction_month,

        -- The source shouts ("ANG MO KIO"); initcap makes it readable, trim drops stray spaces
        initcap(trim(town)) as town,
        upper(trim(flat_type)) as flat_type,          -- "4 ROOM": an abbreviation, keep it upper
        upper(trim(flat_model)) as flat_model,        -- "DBSS", "3Gen": initcap would mangle these
        initcap(trim(street_name)) as street_name,
        upper(trim(block)) as block,                  -- "174A", the letter stays upper

        trim(storey_range) as storey_range,
        -- "01 TO 03" -> 1 and 3, so you can filter on "high floor" later
        split_part(trim(storey_range), ' TO ', 1)::int as storey_min,
        split_part(trim(storey_range), ' TO ', 2)::int as storey_max,

        floor_area_sqm::numeric as floor_area_sqm,
        lease_commence_date::int as lease_commence_year,

        -- "61 years 04 months" -> 736. The months part is missing on exact years
        -- ("61 years"), so coalesce it to 0.
        (
            coalesce(substring(remaining_lease from '(\d+)\s*year')::int, 0) * 12
            + coalesce(substring(remaining_lease from '(\d+)\s*month')::int, 0)
        ) as remaining_lease_months,

        resale_price::numeric as resale_price,
        loaded_at

    from source

)

select
    *,
    -- The headline metric: price per square metre makes flats of different sizes comparable.
    -- nullif guards against a divide-by-zero if the source ever sends 0.
    round(resale_price / nullif(floor_area_sqm, 0), 2) as price_per_sqm
from cleaned
