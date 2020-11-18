select distinct
    offers.id offer_id,
    greatest(case when universities.education_group_id=12 and kinds.parent_id=3 then 149.9
    else (case when offers.campaign='semana-inicio-imediato' 
      then (case when kinds.parent_id=1 then 1.15 else 1.2 end) 
      else 1 end)* 
    (case when 
        offers.discount_percentage-coalesce(offers.commercial_discount,0)<40
            then offers.offered_price
            else least(2*offers.offered_price,university_offers.full_price) end) end,149.9) pre_enrollment_fees
from offers
    join universities on universities.id=offers.university_id
    join university_offers on university_offers.id=offers.university_offer_id
    join courses on courses.id=offers.course_id
    join levels on levels.name=courses.level
    join kinds on kinds.name=courses.kind
    join pre_enrollment_fees on pre_enrollment_fees.offer_id=offers.id
where
    1=1
    and offers.enabled
    and levels.parent_id<>1
    and kinds.parent_id is not null
    and not coalesce(offers.billing_integration,false)
    and offers.campaign='semana-inicio-imediato'
    and pre_enrollment_fees.enabled
    and abs((case when kinds.parent_id=1 then 1.15 else 1.2 end) * 
    greatest(case when 
        offers.discount_percentage-coalesce(offers.commercial_discount,0)<40
            then offers.offered_price
            else least(2*offers.offered_price,university_offers.full_price) end,149.9)-pre_enrollment_fees.value)>1
limit 16384
