select 
    offers.id offer_id,
    university_offers.full_price pre_enrollment_fees
from offers
    join university_offers on university_offers.id=offers.university_offer_id
    join pre_enrollment_fees on pre_enrollment_fees.offer_id=offers.id
    join courses on courses.id=offers.course_id
    join levels on levels.name=courses.level
        and levels.parent_id is not null
    join kinds on kinds.name=courses.kind
        and kinds.parent_id is not null
    
where 
    1=1
    and offers.enabled
    and pre_enrollment_fees.enabled
    and levels.parent_id=1
    and kinds.parent_id<>1
    and offers.university_id not in (2892,2901,19,27,30)
    and abs(university_offers.full_price-pre_enrollment_fees.value)>.1
    and not coalesce(offers.billing_integration,false)
--group by 1
--having count(distinct pre_enrollment_fees.id)<>1
limit 16384 
