-- Dimension: one row per flat type ("1 ROOM" ... "EXECUTIVE", "MULTI-GENERATION").
select distinct
    {{ surrogate_key('flat_type') }} as flat_type_key,
    flat_type,
    -- "4 ROOM" -> 4, so you can filter "3 rooms or more". EXECUTIVE and MULTI-GENERATION
    -- aren't counted in rooms, so they stay null rather than getting a made-up number.
    case
        when flat_type like '% ROOM' then split_part(flat_type, ' ', 1)::int
    end as room_count
from {{ ref('stg_resale_transactions') }}
