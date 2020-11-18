select distinct
    offer_id,
    price,
    alternative,
    original_price,
    pre_enrollment_fees
from (
    select
        --universities.id,
        --universities.name,
        --universities.account_type,
        --admin_users.name ka,
        offers.id offer_id,
        --courses.name,
        --courses.shift
        o.elem price,
        o.nr alternative,
        pre_enrollment_fees.value original_price,
        concat(greatest(pre_enrollment_fees.value,149.9),'|',
        round(greatest(pre_enrollment_fees.value,149.9)*1.1,2),'|',
        round(greatest(pre_enrollment_fees.value,149.9)*1.1+0.01,2),'|',
        round(greatest(pre_enrollment_fees.value,149.9)*1.1+0.02,2),'|',
        round(greatest(pre_enrollment_fees.value,149.9)*1.1+0.03,2)) pre_enrollment_fees,
        count(pre_enrollment_fees.value) over (partition by offers.id) pefs
    from querobolsa_production.offers
        join querobolsa_production.universities on universities.id=offers.university_id
        join querobolsa_production.university_deal_owners on university_deal_owners.university_id=universities.id
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
        JOIN LATERAL unnest(array[greatest(pre_enrollment_fees.value,149.9),round(greatest(pre_enrollment_fees.value,149.9)*1.1,2)])
                        WITH ORDINALITY AS o(elem, nr) ON TRUE
            
    where 
        offers.enabled
        and not offers.restricted
        and not coalesce(offers.forced_disabled,false)
        and pre_enrollment_fees.enabled
        and university_deal_owners.end_date is null
        and kinds.parent_id is not null
        --and kinds.parent_id=1
        and levels.parent_id=1
        and university_billing_configurations.id is null
        and universities.status='partner'
        and offers.pre_enrollment_fee_type<>'experiment'
        --and osc.id=3
        and offers.university_id not in ('3369')
    group by 1,2,3,4
    having max(coalesce(osc.discount_percentage,0))>=100.
    )aux
where
    pefs=2
limit 65536

