select distinct
    offers.id offer_id,
    offers.offered_price pre_enrollment_fees
from offers
    join university_offers on university_offers.id=offers.university_offer_id
    join courses on courses.id=offers.course_id
    join levels on levels.name=courses.level
    join kinds on kinds.name=courses.kind
where
    1=1
    and offers.enabled
    {1}and levels.parent_id={2}
    {3}and kinds.parent_id={4}
    and not coalesce(offers.billing_integration,false)
    and offers.university_id={0}
