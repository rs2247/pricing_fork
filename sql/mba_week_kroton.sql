select
    offers.id offer_id,
    round(university_offers.full_price*1.1,2) pre_enrollment_fees
from offers
    join universities on universities.id=offers.university_id
    join university_offers on university_offers.id=offers.university_offer_id
    join pre_enrollment_fees on pre_enrollment_fees.offer_id=offers.id
        and pre_enrollment_fees.enabled
where
    1=1
    and offers.enabled
    and offers.campaign='mba_week'
    and abs(university_offers.full_price*1.1-pre_enrollment_fees.value)>.1
    and universities.education_group_id=12
limit 16384
