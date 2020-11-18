select distinct
    id,description
from test.pricing_tests
where
    id={0}
order by 1
