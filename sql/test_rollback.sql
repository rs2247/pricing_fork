select distinct
    offer_id,
    original_price pre_enrollment_fees
from test.pricing_offers
    join querobolsa_production.offers on offers.id=offer_id
where
    test_id={0}
    and end_date is null
limit 16384
