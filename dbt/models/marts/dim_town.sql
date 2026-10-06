-- Dimension: one row per town.
select distinct
    {{ surrogate_key('town') }} as town_key,
    town
from {{ ref('stg_resale_transactions') }}
