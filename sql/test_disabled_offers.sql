select
    offers.id
from offers
where
    not offers.enabled
    and offers.id in {0}
