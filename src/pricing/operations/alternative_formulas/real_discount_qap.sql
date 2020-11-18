
CASE WHEN offers.university_id IN ({fixed_139}) THEN 139.90 
ELSE  
  CASE WHEN levels.parent_id = 1 THEN
    CASE WHEN kinds.parent_id = 1 THEN
      CASE WHEN offers.university_id IN ({average_offered_full_fixed_qap}) THEN
        CASE WHEN ({offered_qb} + university_offers.full_price)/2 > {min}
        THEN ({offered_qb} + university_offers.full_price)/2
        ELSE {min}
        END
      ELSE
        CASE WHEN offers.university_id IN ({full_fixed_qap}) THEN
          CASE WHEN university_offers.full_price > {min}
          THEN university_offers.full_price
          ELSE {min}
          END
        ELSE  
          CASE WHEN NOT courses.digital_admission AND (0.1 > {real_discount}) THEN
            CASE WHEN ({k} * (0.8 * ({offered_balcao}))) < {min} THEN {min}
            ELSE 
              CASE WHEN ({k} * (0.8 * ({offered_balcao}))) > university_offers.full_price THEN
                CASE WHEN university_offers.full_price > {min} THEN {min}
                ELSE university_offers.full_price
                END
              ELSE
                CASE WHEN {commercial_discount} > offers.discount_percentage THEN
                  CASE WHEN 2.8 * {offered_qb} < {min} THEN {min}
                  ELSE
                    CASE WHEN 2.8 * {offered_qb} > university_offers.full_price THEN university_offers.full_price
                    ELSE 2.8 * {offered_qb}
                    END
                  END
                ELSE ({k} * (0.8 * ({offered_balcao})))
                END
              END  
            END
          ELSE
            CASE WHEN ({k} * (({real_discount}) * 1 + 0.7) * ({offered_balcao})) < {min} THEN {min}
            ELSE
              CASE WHEN ({k} * (({real_discount}) * 1 + 0.7) * ({offered_balcao})) > university_offers.full_price THEN
                CASE WHEN full_price < {min} THEN full_price
                ELSE full_price
                END
              ELSE ({k} * (({real_discount}) * 1 + 0.7) * ({offered_balcao}))
              END
            END  
          END
        END
      END  
    ELSE
      CASE WHEN kinds.parent_id = 3 THEN
        CASE WHEN NOT courses.digital_admission AND (0.1 > {real_discount}) THEN
          CASE WHEN ({k} * (1.4 * ({offered_balcao}))) < {min} THEN {min}
          ELSE 
            CASE WHEN ({k} * (1.4 * ({offered_balcao}))) > university_offers.full_price THEN
              CASE WHEN university_offers.full_price > {min} THEN {min}
              ELSE university_offers.full_price
              END
            ELSE
              CASE WHEN {commercial_discount} > offers.discount_percentage THEN
                CASE WHEN 3.5 * {offered_qb} < {min} THEN {min}
                ELSE
                  CASE WHEN 3.5 * {offered_qb} > university_offers.full_price THEN university_offers.full_price
                  ELSE 3.5 * {offered_qb}
                  END
                END
              ELSE ({k} * (1.4 * ({offered_balcao})))
              END
            END  
          END
        ELSE
          CASE WHEN ({k} * (({real_discount}) * 1 + 1.3) * ({offered_balcao})) < {min} THEN {min}
          ELSE
            CASE WHEN ({k} * (({real_discount}) * 1 + 1.3) * ({offered_balcao})) > university_offers.full_price THEN
              CASE WHEN full_price < {min} THEN full_price
              ELSE full_price
              END
            ELSE ({k} * (({real_discount}) * 1 + 1.3) * ({offered_balcao}))
            END
          END  
        END  
      ELSE       
        CASE WHEN NOT courses.digital_admission AND (0.1 > {real_discount}) THEN
          CASE WHEN ({k} * (1.4 * ({offered_balcao}))) < {min} THEN {min}
          ELSE 
            CASE WHEN ({k} * (1.4 * ({offered_balcao}))) > university_offers.full_price THEN
              CASE WHEN university_offers.full_price > {min} THEN {min}
              ELSE university_offers.full_price
              END
            ELSE
              CASE WHEN {commercial_discount} > offers.discount_percentage THEN
                CASE WHEN 2.6 * {offered_qb} < {min} THEN {min}
                ELSE
                  CASE WHEN 2.6 * {offered_qb} > university_offers.full_price THEN university_offers.full_price
                  ELSE 2.6 * {offered_qb}
                  END
                END
              ELSE ({k} * (1.4 * ({offered_balcao})))
              END
            END  
          END
        ELSE
          CASE WHEN ({k} * (({real_discount}) * 1 + 1.3) * ({offered_balcao})) < {min} THEN {min}
          ELSE
            CASE WHEN ({k} * (({real_discount}) * 1 + 1.3) * ({offered_balcao})) > university_offers.full_price THEN
              CASE WHEN full_price < {min} THEN full_price
              ELSE full_price
              END
            ELSE ({k} * (({real_discount}) * 1 + 1.3) * ({offered_balcao}))
            END
          END  
        END
      END
    END     
  ELSE
    CASE WHEN university_offers.max_payments = 1 
    THEN {offered_qb}
    ELSE
      CASE WHEN offers.university_id IN ({full_fixed_qap_pos})
      THEN 
        CASE WHEN university_offers.full_price < (CASE WHEN ({k}) = 1.0 THEN {min} ELSE ({min_qap_7_lowest}) END)
        THEN (CASE WHEN ({k}) = 1.0 THEN {min} ELSE ({min_qap_7_lowest}) END)
        ELSE university_offers.full_price
        END
      ELSE  
        CASE WHEN kinds.parent_id = 1 THEN 
          CASE WHEN ({k} * (({real_discount}) * 1 + 0.6) * ({offered_balcao})) < (CASE WHEN ({k}) = 1.0 THEN {min} ELSE ({min_qap_7_lowest}) END)
          THEN (CASE WHEN ({k}) = 1.0 THEN {min} ELSE ({min_qap_7_lowest}) END)
          ELSE 
            CASE WHEN ({k} * (({real_discount}) * 1 + 0.6) * ({offered_balcao})) > university_offers.full_price 
            THEN university_offers.full_price
            ELSE ({k} * (({real_discount}) * 1 + 0.6) * ({offered_balcao}))
            END
          END  
        ELSE 
          CASE WHEN ({k} * (({real_discount}) * 1 + 0.5) * ({offered_balcao})) < (CASE WHEN ({k}) = 1.0 THEN {min} ELSE ({min_qap_7_lowest}) END)
          THEN (CASE WHEN ({k}) = 1.0 THEN {min} ELSE ({min_qap_7_lowest}) END)
          ELSE 
            CASE WHEN ({k} * (({real_discount}) * 1 + 0.5) * ({offered_balcao})) > university_offers.full_price 
            THEN university_offers.full_price
            ELSE ({k} * (({real_discount}) * 1 + 0.5) * ({offered_balcao}))
            END
          END
        END
      END
    END  
  END  
END