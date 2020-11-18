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
        case when offers.discount_percentage=40. then round(0.75*university_offers.full_price,2)
            when offers.discount_percentage=45. then round(0.85*university_offers.full_price,2)
            when offers.discount_percentage=50. then round(0.95*university_offers.full_price,2) end) pre_enrollment_fees,
        count(pre_enrollment_fees.value) over (partition by offers.id) pefs
    from querobolsa_production.offers
        join querobolsa_production.universities on universities.id=offers.university_id
        join querobolsa_production.university_offers on university_offers.id=offers.university_offer_id
        join querobolsa_production.pre_enrollment_fees on pre_enrollment_fees.offer_id=offers.id
        join querobolsa_production.courses on courses.id=offers.course_id
        join querobolsa_production.levels on levels.name=courses.level
        join querobolsa_production.kinds on kinds.name=courses.kind
        JOIN LATERAL unnest(array[pre_enrollment_fees.value,
        case when offers.discount_percentage=40. then  round(0.75*university_offers.full_price,2)
            when offers.discount_percentage=45. then round(0.85*university_offers.full_price,2)
            when offers.discount_percentage=50. then round(0.95*university_offers.full_price,2) end])
                        WITH ORDINALITY AS o(elem, nr) ON TRUE
    where
        offers.enabled
        and not offers.restricted
        and not coalesce(offers.forced_disabled,false)
        and offers.university_id=19
        and kinds.parent_id=1
        and levels.parent_id=7
        and discount_percentage not in (35.,55.)
        and (offers.campaign<>'credit' or offers.campaign is null)
    )aux
where
    pefs=2
limit 65536
