select
    offers.id,
    offers.offered_price,
    orders.checkout_step,
    offers.university_id,
    universities.name,
    courses.level,
    courses.kind,
    line_items.price
from querobolsa_production.orders
    join querobolsa_production.base_users on base_users.id=orders.base_user_id
    join querobolsa_production.line_items on line_items.order_id=orders.id
    join querobolsa_production.offers on offers.id=line_items.offer_id
    join querobolsa_production.courses on courses.id=offers.course_id
    join querobolsa_production.universities on universities.id=offers.university_id
    join querobolsa_production.university_offers on university_offers.id=offers.university_offer_id
where
    base_users.customer_id in {0}
    and orders.created_at>='{1}'
    --and offers.university_id in (2892,2901)
    --and offers.expected_revenue>0
    --and offers.expected_revenue<300
    --and line_items.price<600
    --and universities.account_type=5
    --and offers.discount_percentage>=80.
    --and offers.discount_percentage<100
    --and offers.discount_percentage<>100.
    --and line_items.price/university_offers.full_price/offers.discount_percentage*100>=2.
    --and line_items.price/university_offers.full_price/offers.discount_percentage*100<4.
    --and university_offers.full_price*offers.discount_percentage/100
    --    *university_offers.max_payments/line_items.price>=60
    --and university_offers.full_price*offers.discount_percentage/100
    --    *university_offers.max_payments/line_items.price<80
    
