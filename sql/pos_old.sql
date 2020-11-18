select
  offers.id offer_id
  ,case when offers.discount_percentage-offers.commercial_discount<40
      then greatest(university_offers.full_price-offers.offered_price
                    ,149.9)
    else greatest(university_offers.full_price-offers.offered_price
                  ,offers.offered_price
                  ,149.9) end pre_enrollment_fees
from offers
  join university_offers on university_offers.id=offers.university_offer_id
  join courses on courses.id=offers.course_id
  join levels on levels.name=courses.level
  join kinds on kinds.name=courses.kind
where
  1=1
  and offers.enabled
  and kinds.parent_id is null
  and levels.parent_id<>1
  and offers.university_id in ({0})
    
