select
    offers.id offer_id,
    139.9/0.6 pre_enrollment_fees
from offers
    join pre_enrollment_fees pef
        on pef.offer_id=offers.id
    join courses on courses.id=offers.course_id
    join kinds on kinds.name=courses.kind
    join levels on levels.name=courses.level
    left join university_billing_configurations on
        university_billing_configurations.university_id=courses.university_id
        and university_billing_configurations.kind_id=kinds.parent_id
        and university_billing_configurations.level_id=levels.parent_id
where 
    --offers.university_id=2738
    pef.enabled
    and offers.enabled
    and levels.parent_id=7
    and university_billing_configurations.id is not null
group by
    offers.id 
having min(pef.value)<139.9
