select distinct
    id,name
from test.pricing_tests
where
    end_date is null
order by 1
