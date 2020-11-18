with c as (
    select
        courses.id
        ,count(offers.id) offers
        ,max(offers.discount_percentage) max_dp
    from offers
        join courses on courses.id=offers.course_id
        join levels on levels.name=courses.level
        join kinds on kinds.name=courses.kind
    where
        1=1
        and offers.university_id in {0}
        and levels.parent_id=1
        and kinds.parent_id=1
        and offers.enabled
        and not offers.restricted
        and courses.enabled
    group by 1
    having
        count(offers.id)>1
)
select
    offers.id offer_id
    ,(case when 
        offers.discount_percentage-coalesce(offers.commercial_discount,0)<40
            then (offers.offered_price+university_offers.full_price)/2
            else least(2*offers.offered_price,university_offers.full_price) end) pre_enrollment_fees
from offers
    join c on offers.course_id=c.id
    join university_offers on university_offers.id=offers.university_offer_id
where
    1=1
    and offers.enabled
    and not offers.restricted
    and offers.discount_percentage=c.max_dp
