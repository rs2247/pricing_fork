
case when university_id in ({fixed_99}) THEN
  99.0
else
  case when level_id = 1 then
    case when {min_qap_1} > original_value*(1-{level_1_subsidy}) then
      {min_qap_1}
    else
      original_value*(1-{level_1_subsidy})
    end
  else
    case when {min_qap_7_lowest} > (original_value - (0.4 * offered_price)) then
      {min_qap_7_lowest}
    else
      (original_value - (0.4 * offered_price))
    end
  end
end