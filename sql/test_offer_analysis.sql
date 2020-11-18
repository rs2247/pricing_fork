with test_offers as (
    select distinct
        pricing_offers.start_date::timestamp at time zone 'America/Sao_Paulo' start_date,
        (case when pricing_offers.end_date is null then 
            now()::timestamp at time zone 'America/Sao_Paulo' 
            else pricing_offers.end_date::timestamp at time zone 'America/Sao_Paulo' end) end_date,
        pricing_offers.offer_id,
        pricing_offers.original_price,
        pricing_alternatives.price,
        pricing_alternatives.alternative
    from
        test.pricing_offers
        join test.pricing_alternatives on 
            pricing_alternatives.offer_id=pricing_offers.offer_id
            and pricing_alternatives.test_id=pricing_offers.test_id
    where pricing_offers.test_id={0}
    --where pricing_offers.test_id=1
    )

select distinct on (orders.id)
    orders.id,
    orders.created_at as order_created_at,
    orders.paid_at as order_paid_at,
    offers.id as offer_id,
    trim(lower(campuses.state)) state,
    base_users.customer_id,
    levels.parent_id level_id,
    universities.id as university_id,
    universities.name as university_name,
    (case when orders.checkout_step='paid' and orders.paid_at is not null then 'paid'
            when orders.checkout_step = 'paid' and orders.refunded_at is not null then 'refund'
            else orders.checkout_step end) as checkout_step,
    university_offers.full_price,
    offers.offered_price,
    line_items.price as coupon_price,
    round(offers.discount_percentage) as discount_percentage,
    round(test_offers.price,1) as price,
    test_offers.original_price,
    test_offers.alternative
from
    querobolsa_production.orders
    join querobolsa_production.order_origins on order_origins.order_id=orders.id
    join querobolsa_production.base_users on orders.base_user_id = base_users.id
    join querobolsa_production.line_items on line_items.order_id = orders.id
    join querobolsa_production.offers on line_items.offer_id = offers.id
    join querobolsa_production.university_offers on offers.university_offer_id = university_offers.id
    join querobolsa_production.courses on courses.id = university_offers.course_id
    join querobolsa_production.levels on levels.name=courses.level
        and levels.parent_id is not null
    join querobolsa_production.kinds on kinds.name=courses.kind
        and kinds.parent_id is not null
    join querobolsa_production.universities on universities.id = courses.university_id
    join querobolsa_production.campuses on campuses.id=courses.campus_id
    --join querobolsa_production.pre_enrollment_fees on line_items.pre_enrollment_fee_id = pre_enrollment_fees.id
    join test_offers on test_offers.offer_id = offers.id
    --left join customer_contacts on customer_contacts.customer_id=base_users.customer_id
    --    and customer_contacts.created_at::timestamp at time zone 'utc' > 
    --(select offers_per_test.test_start from offers_per_test)::timestamp 
    --    at time zone 'America/Sao_Paulo'
    
where
    orders.checkout_step <> 'initiated'
    and orders.created_at::timestamp at time zone 'utc' > test_offers.start_date
    and orders.created_at::timestamp at time zone 'utc' < test_offers.end_date
    and order_origins.origin<>'OPA'
    --and customer_contacts.id is null
    and kinds.parent_id=8
    and abs(round(test_offers.price,2)-round(line_items.price,2))<.1
order by
    orders.id,
    --orders.updated_at desc,
    offers.id
