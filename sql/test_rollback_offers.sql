select
    pricing_offers.offer_id,
    pricing_offers.original_price pre_enrollment_fees
from test.pricing_offers
    join querobolsa_production.offers on offers.id=pricing_offers.offer_id
    --join querobolsa_production.pre_enrollment_fees pef on pef.offer_id=offers.id
where
    offers.id in {0}
    and offers.enabled
    --and pef.enabled
--group by 1,2
--having count(distinct pef.id) >1

limit 16384
