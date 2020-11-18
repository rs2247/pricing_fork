with offers as(
    select distinct on 
        (pricing_alternatives.offer_id,
        pricing_alternatives.alternative)
        
        pricing_alternatives.offer_id,
        pricing_alternatives.price,
        pricing_alternatives.alternative
    from test.pricing_offers 
        join test.pricing_alternatives
            on pricing_alternatives.offer_id=pricing_offers.offer_id
    where
        pricing_offers.test_id={0}
        and pricing_alternatives.alternative<={1}
        and pricing_offers.end_date is null
	and pricing_offers.offer_id not in {2}
    order by 
        pricing_alternatives.offer_id,
        pricing_alternatives.alternative,
        pricing_alternatives.id desc)
        
select
    offer_id,
    array_agg(price) prices,
    array_agg(alternative) alternatives
from offers
group by 1
limit 16384
