case when billing_integration then
  case when {min_qap_12} > original_value - (0.4  * offered_price) then
	  {min_qap_12}
	else
	  original_value - (0.4  * offered_price)
	end
else
  original_value
end
