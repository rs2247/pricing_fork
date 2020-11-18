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
        round(pre_enrollment_fees.value*1.1,2),'|',
        round(pre_enrollment_fees.value*1.1+0.01,2),'|',
        round(pre_enrollment_fees.value*1.2,2),'|',
        round(pre_enrollment_fees.value*1.2+0.01,2)) pre_enrollment_fees,
        count(pre_enrollment_fees.value) over (partition by offers.id) pefs,
        min(pre_enrollment_fees.value) over (partition by offers.id) pef
    from querobolsa_production.offers
        join querobolsa_production.universities on universities.id=offers.university_id
        join querobolsa_production.university_deal_owners on university_deal_owners.university_id=universities.id
        join querobolsa_production.university_offers on university_offers.id=offers.university_offer_id
        join querobolsa_production.pre_enrollment_fees on pre_enrollment_fees.offer_id=offers.id
        join querobolsa_production.admin_users on admin_users.id=university_deal_owners.admin_user_id
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
            round(pre_enrollment_fees.value*1.1,2),
            round(pre_enrollment_fees.value*1.2,2)])
                WITH ORDINALITY AS o(elem, nr) ON TRUE
            
    where 
        offers.enabled
        and not offers.restricted
        and not coalesce(offers.forced_disabled,false)
        and pre_enrollment_fees.enabled
        and university_deal_owners.end_date is null
        and kinds.parent_id is not null
        and levels.parent_id=1
        and university_billing_configurations.id is null
        and universities.status='partner'
        and offers.university_id not in (3369,1639)
        and abs(university_offers.full_price-offers.expected_revenue)>0.1
        and (offers.campaign<>'credit' or offers.campaign is null)
    group by 1,2,3,4
    having max(coalesce(osc.discount_percentage,0))>=100.
    )aux
where
    pefs=3
    and pef>300.
    and pef<=600.
limit 65536
