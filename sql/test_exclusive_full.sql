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
        university_offers.full_price,
        concat(pre_enrollment_fees.value,'|',
        round((offers.offered_price+university_offers.full_price)/2,2),'|',
        round(offers.offered_price,2)) pre_enrollment_fees,
        count(pre_enrollment_fees.value) over (partition by offers.id) pefs
    from querobolsa_production.offers
        join querobolsa_production.universities on universities.id=offers.university_id
        join querobolsa_production.university_offers on university_offers.id=offers.university_offer_id
        join querobolsa_production.pre_enrollment_fees on pre_enrollment_fees.offer_id=offers.id
        join querobolsa_production.courses on courses.id=offers.course_id
        join querobolsa_production.kinds on kinds.name=courses.kind
        join querobolsa_production.levels on levels.name=courses.level
        left join querobolsa_production.offer_special_conditions_offers osco on osco.offer_id=offers.id
        left join querobolsa_production.offer_special_conditions osc on osc.id=osco.offer_special_condition_id
        left join querobolsa_production.university_billing_configurations on
            university_billing_configurations.university_id=universities.id
            and university_billing_configurations.kind_id=kinds.parent_id
            and university_billing_configurations.level_id=levels.parent_id
        JOIN LATERAL unnest(array[pre_enrollment_fees.value,
        round((offers.offered_price+university_offers.full_price)/2,2),
        round(offers.offered_price,2)])
                WITH ORDINALITY AS o(elem, nr) ON TRUE
            
    where 
        offers.enabled
        and not offers.restricted
        and not coalesce(offers.forced_disabled,false)
        and pre_enrollment_fees.enabled
        and kinds.parent_id=1
        and levels.parent_id=1
        --and offers.position=1
        and abs(university_offers.full_price-offers.expected_revenue)<0.1
        and university_billing_configurations.id is null
        and universities.status='partner'
        and offers.university_id not in (3369,1639)
        and university_offers.enrollment_semester='2019.1'
        and (offers.campaign<>'credit' or offers.campaign is null)
    group by 1,2,3,4,5
    )aux
where
    pefs=3
limit 32768