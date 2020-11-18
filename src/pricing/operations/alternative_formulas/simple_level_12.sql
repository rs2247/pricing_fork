
CASE WHEN offers.offered_price*1.2 < {min_12} THEN {min_12}
ELSE offers.offered_price*1.2 
END