select
    follow_ups.university_id,
    universities.name,
    count(*),
    sum(line_items.price)
from follow_ups
    join universities on universities.id=follow_ups.university_id
    join orders on orders.id=follow_ups.order_id
    join line_items on line_items.order_id=orders.id
    join offers on offers.id=line_items.offer_id
    join university_offers on university_offers.id=offers.university_offer_id
    join courses on courses.id=follow_ups.course_id
    join levels on levels.name=courses.level
    join kinds on kinds.name=courses.kind
    join university_billing_configurations ubc
        on ubc.university_id=universities.id
        and ubc.kind_id=kinds.parent_id
        and ubc.level_id=levels.parent_id
where
    orders.created_at>'2018-10-01'
    and line_items.price<150
    and greatest(case when offers.discount_percentage>=40 
        then greatest(offers.offered_price,university_offers.full_price*offers.discount_percentage/100)
        else university_offers.full_price*offers.discount_percentage/100 end)<150
    and levels.parent_id=7
group by 1,2
order by 4 desc
