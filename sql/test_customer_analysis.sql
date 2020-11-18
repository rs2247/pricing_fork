select
    --count(case when orders.checkout_step='registered' then orders.id else null end) registered,
    --count(case when orders.checkout_step='commited' then orders.id else null end) commited,
    count(case when orders.checkout_step='paid' and orders.paid_at is not null then orders.id else null end) paid,
    --count(case when orders.checkout_step='paid' and orders.paid_at is null then orders.id else null end) refunded,
    count(case when orders.checkout_step<>'initiated' then orders.id else null end) total,
    count(distinct base_users.customer_id) customers,
    round(sum(case when orders.checkout_step='paid' 
            and orders.paid_at is not null 
        then line_items.price else 0. end),2) revenue,
    round(avg(case when orders.checkout_step='paid' 
            and orders.paid_at is not null 
        then line_items.price else null end),2) aov,
    round(sum(case when orders.checkout_step='paid' and orders.paid_at is not null then line_items.price else 0. end)
    /count(case when orders.checkout_step<>'initiated' then orders.id else null end),2) rpo,
    round(sum(case when orders.checkout_step='paid' and orders.paid_at is not null then line_items.price else 0. end)
    /count(distinct base_users.customer_id),2) rpc
from querobolsa_production.orders
    join querobolsa_production.base_users on base_users.id=orders.base_user_id
    join querobolsa_production.line_items on line_items.order_id=orders.id
    join querobolsa_production.offers on offers.id=line_items.offer_id
    join querobolsa_production.universities on universities.id=offers.university_id
    join querobolsa_production.university_offers on university_offers.id=offers.university_offer_id
where
    1=1
    and base_users.customer_id in {0}
    and orders.created_at>='{1}'
    --and offers.university_id in (1601)
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
    
