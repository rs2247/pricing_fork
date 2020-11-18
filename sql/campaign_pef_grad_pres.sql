select
    offers.id offer_id,
    greatest((case when 
        offers.discount_percentage-coalesce(offers.commercial_discount,0)<40
            then (offers.offered_price+university_offers.full_price)/2
            else least(2*offers.offered_price,university_offers.full_price) end)*0.7,149.9) pre_enrollment_fees
from offers
    join university_offers on university_offers.id=offers.university_offer_id
    join courses on courses.id=offers.course_id
    join levels on levels.name=courses.level
    join kinds on kinds.name=courses.kind
where
    1=1
    and offers.enabled
    and levels.parent_id=1
    and kinds.parent_id=1
    and not coalesce(offers.billing_integration,false)
    and offers.university_id={0}
