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
        concat(pre_enrollment_fees.value,'|',
        case when offers.discount_percentage-offers.commercial_discount<40
            then round(offers.offered_price,2)
        else greatest(2*offers.offered_price,university_offers.full_price) end) pre_enrollment_fees,
        count(pre_enrollment_fees.value) over (partition by offers.id) pefs
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
        left join university_billing_configurations on
            university_billing_configurations.university_id=universities.id
            and university_billing_configurations.kind_id=kinds.parent_id
            and university_billing_configurations.level_id=levels.parent_id
        JOIN LATERAL unnest(array[pre_enrollment_fees.value,
            case when offers.discount_percentage-offers.commercial_discount<40
            then round(offers.offered_price,2)
        else least(2*offers.offered_price,university_offers.full_price) end])
                WITH ORDINALITY AS o(elem, nr) ON TRUE
            
    where 
        offers.enabled
        and not offers.restricted
        and not coalesce(offers.forced_disabled,false)
        and pre_enrollment_fees.enabled
        and university_deal_owners.end_date is null
        and kinds.parent_id = 1
        and levels.parent_id=7
        and university_billing_configurations.id is null
        and universities.status='partner'
        and offers.university_id not in (3272,3149)
        and (offers.campaign<>'credit' or offers.campaign is null)
    group by 1,2,3,4,university_offers.full_price
    --having max(coalesce(osc.discount_percentage,0))>=100.
    )aux
where
    pefs=2
    and substring(pre_enrollment_fees from 1 for position('|' in pre_enrollment_fees)-1)::numeric
    <>substring(pre_enrollment_fees from position('|' in pre_enrollment_fees)+1)::numeric
limit 32768
