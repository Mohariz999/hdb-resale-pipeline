-- A singular test: dbt runs this query, and the test FAILS if it returns any rows.
--
-- A sale with a zero or negative price or floor area can only be a data error, and it
-- would quietly drag down every average and median built on top of it.
select *
from {{ ref('fct_resale_transactions') }}
where resale_price <= 0
   or floor_area_sqm <= 0
