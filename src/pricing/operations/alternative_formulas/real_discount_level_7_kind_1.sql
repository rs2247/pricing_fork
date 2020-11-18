CASE WHEN university_offers.max_payments = 1 
THEN {offered_qb}
ELSE
  CASE WHEN offers.university_id IN ({full_fixed_pos_vitrine}) AND offers.position IN (1)
  THEN university_offers.full_price
  ELSE
    CASE WHEN offers.university_id IN ({fixed_139}) 
    THEN 139.90 
    ELSE 
      CASE WHEN offers.university_id IN ({fixed_336}) 
      THEN 336.00
      ELSE 
        CASE WHEN offers.university_id IN ({fixed_99}) 
        THEN 99.00 
        ELSE
          CASE WHEN offers.university_id IN ({fixed_f_o}) 
          THEN university_offers.full_price - {offered_qb}
          ELSE
            CASE WHEN offers.university_id IN ({pef_old}) AND offers.discount_percentage < 40 
            THEN
              CASE WHEN {offered_qb} > CASE WHEN {k} > 1 THEN {min_7_plus} ELSE {min} END 
              THEN {offered_qb} 
              ELSE CASE WHEN {k} > 1 THEN {min_7_plus} ELSE {min} END
              END            
            ELSE
              CASE WHEN offers.university_id IN ({pef_old}) AND offers.discount_percentage >= 40 
              THEN
                CASE WHEN (university_offers.full_price - {offered_qb}) > CASE WHEN {k} > 1 THEN {min_7_plus} ELSE {min} END
                THEN (university_offers.full_price - {offered_qb}) 
                ELSE CASE WHEN {k} > 1 THEN {min_7_plus} ELSE {min} END
                END 
              ELSE
                CASE WHEN {commercial_discount} < offers.discount_percentage 
                THEN
                  CASE WHEN ({k} * (({real_discount}) * 1 + 0.6) * ({offered_balcao})) > CASE WHEN {k} > 1 THEN {min_7_plus} ELSE {min} END
                  THEN ({k} * (({real_discount}) * 1 + 0.6) * ({offered_balcao})) 
                  ELSE CASE WHEN {k} > 1 THEN {min_7_plus} ELSE {min} END
                  END
                ELSE
                  CASE WHEN ({offered_qb} * 0.6) > CASE WHEN {k} > 1 THEN {min_7_plus} ELSE {min} END
                  THEN ({offered_qb} * 0.6) 
                  ELSE CASE WHEN {k} > 1 THEN {min_7_plus} ELSE {min} END
                  END
                END    
              END    
            END       
          END       
        END
      END
    END
  END
END 