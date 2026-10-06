-- Dimension: one row per flat model ("IMPROVED", "NEW GENERATION", "DBSS", ...).
select distinct
    {{ surrogate_key('flat_model') }} as flat_model_key,
    flat_model
from {{ ref('stg_resale_transactions') }}
