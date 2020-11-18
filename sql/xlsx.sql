select
  offers.id offer_id
  ,offers.course_id
  ,offers.commercial_discount commercial_old
from offers
where
  1=1
  and offers.enabled
  and offers.course_id in ({0})
