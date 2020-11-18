select
    pricing_ies_tests.test_id,
    pricing_ies_tests.offers
from test.pricing_ies_tests
where
    pricing_ies_tests.university_id={0}
order by 1
