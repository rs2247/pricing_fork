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
        149.9) pre_enrollment_fees,
        count(pre_enrollment_fees.value) over (partition by offers.id) pefs
    from querobolsa_production.offers
        join querobolsa_production.universities on universities.id=offers.university_id
        join querobolsa_production.pre_enrollment_fees on pre_enrollment_fees.offer_id=offers.id
        join querobolsa_production.courses on courses.id=offers.course_id
        join querobolsa_production.kinds on kinds.name=courses.kind
        join querobolsa_production.levels on levels.name=courses.level
        left join querobolsa_production.university_billing_configurations on
            university_billing_configurations.university_id=universities.id
            and university_billing_configurations.kind_id=kinds.parent_id
            and university_billing_configurations.level_id=levels.parent_id
        join lateral unnest(array[pre_enrollment_fees.value,149.9])
                        with ordinality as o(elem, nr) on TRUE
            
    where 
        offers.enabled
        and not offers.restricted
        and not coalesce(offers.forced_disabled,false)
        and pre_enrollment_fees.enabled
        and kinds.parent_id is not null
        and levels.parent_id is not null
        and university_billing_configurations.id is null
        and universities.status='partner'
        and offers.university_id not in ('3369')
        and pre_enrollment_fees.value<=149.9
        and (offers.campaign<>'credit' or offers.campaign is null)
        )aux
where
    pefs=2
    and pre_enrollment_fees not in ('149.9|149.9','149.90|149.9','149.90|149.90','149.9|149.90')
limit 65536
