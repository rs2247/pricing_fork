--select dblink_connect_u('ppa', 'dbname=ppa port=5432');

select distinct
    offers.id,
    offers.university_id,
    universities.name ies,
    ppa.goal,
    ka.name ka,
    universities.account_type,
    courses.name course
from offers
    join universities on universities.id=offers.university_id
    join university_deal_owners on universities.id=university_deal_owners.university_id 
        and university_deal_owners.end_date is null
    join admin_users as ka on ka.id = university_deal_owners.admin_user_id
    join offer_special_conditions_offers 
        on offer_special_conditions_offers.offer_id=offers.id
    join offer_special_conditions 
        on offer_special_conditions_offers.offer_special_condition_id=offer_special_conditions.id
    join courses on courses.id = offers.course_id
    join levels on levels.name=courses.level 
    join kinds on kinds.name=courses.kind 
    left join university_billing_configurations 
        on university_billing_configurations.university_id=courses.university_id
        and university_billing_configurations.level_id=levels.parent_id
        and university_billing_configurations.kind_id=kinds.parent_id
    join     
        dblink('ppa', 'select
                farm_university_goals.university_id,
                farm_university_goals.goal
            from
                farm_university_goals
            where
                farm_university_goals.active = true
                and farm_university_goals.capture_period_id = 5') 
            as ppa(university_id int, goal numeric)
        on ppa.university_id=universities.id
where
    offers.enabled
    and offers.visible
    and offer_special_conditions.discount_percentage=100.0
    and levels.parent_id=1
    and kinds.parent_id=1
    and university_billing_configurations.id is null
order by 4 desc
