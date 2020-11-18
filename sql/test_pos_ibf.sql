select distinct
    offer_id,
    price,
    alternative,
    original_price,
    pre_enrollment_fees
from (
    select
        offers.id offer_id,
        o.elem price,
        o.nr alternative,
        pre_enrollment_fees.value original_price,
        concat(pre_enrollment_fees.value,'|',139.9) pre_enrollment_fees,
        count(pre_enrollment_fees.value) over (partition by offers.id) pefs,
        min(pre_enrollment_fees.value) over (partition by offers.id) pef
    from offers
        join universities on universities.id=offers.university_id
        join university_deal_owners on university_deal_owners.university_id=universities.id
        join university_offers on university_offers.id=offers.university_offer_id
        join pre_enrollment_fees on pre_enrollment_fees.offer_id=offers.id
        join admin_users on admin_users.id=university_deal_owners.admin_user_id
        join courses on courses.id=offers.course_id
        join kinds on kinds.name=courses.kind
        join levels on levels.name=courses.level
        left join offer_special_conditions_offers osco on osco.offer_id=offers.id
        left join offer_special_conditions osc on osc.id=osco.offer_special_condition_id
        JOIN LATERAL unnest(array[pre_enrollment_fees.value,139.9])
                WITH ORDINALITY AS o(elem, nr) ON TRUE
            
    where 
        offers.enabled
        and not offers.restricted
        and not coalesce(offers.forced_disabled,false)
        and pre_enrollment_fees.enabled
        and university_deal_owners.end_date is null
        and kinds.parent_id = 3
        and levels.parent_id=7
        and universities.status='partner'
        and offers.university_id = 2738
        and (offers.campaign<>'credit' or offers.campaign is null)
    group by 1,2,3,4
    )aux
where
    pefs=2
    and pef<139.9
limit 32768
