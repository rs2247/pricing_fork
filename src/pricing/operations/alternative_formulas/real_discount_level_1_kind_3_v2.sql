CASE WHEN offers.university_id IN ({average_offered_full_fixed_ead}) THEN
  CASE WHEN (offers.offered_price + university_offers.full_price)/2 > {min}
  THEN (offers.offered_price + university_offers.full_price)/2
  ELSE {min}
  END
ELSE  
  CASE WHEN offers.university_id IN ({fixed_99}) or offers.university_id IN ({fixed_99_ead}) THEN 99.0
  ELSE
    CASE WHEN offers.university_id IN ({full_fixed}) THEN
      CASE WHEN university_offers.full_price > {min} 
      THEN university_offers.full_price
      ELSE {min}
      END
    ELSE
        CASE WHEN offers.university_id IN ({offered_fixed}) THEN
          CASE WHEN offers.offered_price > {min} 
          THEN offers.offered_price
          ELSE {min}
          END
        ELSE  
            CASE WHEN (((NOT courses.digital_admission) AND (offers.university_id NOT IN ({fake_admission}))) AND (0.1 > {real_discount})) THEN
              CASE WHEN {commercial_discount} >= offers.discount_percentage THEN -- canibalizacao
                CASE WHEN university_offers.full_price * ({fator_wtp}) < {min}
                THEN {min}
                ELSE university_offers.full_price * ({fator_wtp})
                END
              ELSE
                CASE WHEN ({k} * (1.4 * ({offered_balcao}))) * ({fator_wtp}) < {min} 
                THEN {min}
                ELSE
                  CASE WHEN ({k} * (1.4 * ({offered_balcao}))) > university_offers.full_price THEN
                    CASE WHEN university_offers.full_price * ({fator_wtp}) < {min} 
                    THEN {min}
                    ELSE university_offers.full_price * ({fator_wtp})
                    END
                  ELSE ({k} * (1.4 * ({offered_balcao}))) * ({fator_wtp})
                  END
                END
              END
            ELSE 
              CASE WHEN ({k} * (({real_discount}) * 1 + 1.3) * ({offered_balcao})) * ({fator_wtp}) < {min} THEN {min}
              ELSE 
                CASE WHEN ({k} * (({real_discount}) * 1 + 1.3) * ({offered_balcao})) > university_offers.full_price
                THEN
                  CASE WHEN university_offers.full_price * ({fator_wtp}) < {min} THEN {min}
                  ELSE university_offers.full_price * ({fator_wtp})
                  END
                ELSE
                    CASE WHEN {commercial_discount} >= offers.discount_percentage THEN -- canibalizacao (admissão digital)
                      CASE WHEN 1.4 * offers.offered_price * ({fator_wtp}) < {min} 
                      THEN {min}
                      ELSE 
                        CASE WHEN 1.4 * offers.offered_price > university_offers.full_price
                        THEN 
                          CASE WHEN university_offers.full_price * ({fator_wtp}) < ({min})
                          THEN ({min})
                          ELSE university_offers.full_price * ({fator_wtp})
                          END
                        ELSE 1.4 * offers.offered_price * ({fator_wtp})
                        END
                      END
                    ELSE 
                      CASE WHEN ({k} * (({real_discount}) * 1 + 1.3) * ({offered_balcao})) > university_offers.full_price
                      THEN 
                        CASE WHEN university_offers.full_price * ({fator_wtp}) < ({min})
                        THEN ({min})
                        ELSE university_offers.full_price * ({fator_wtp})
                        END
                      ELSE ({k} * (({real_discount}) * 1 + 1.3) * ({offered_balcao})) * ({fator_wtp})
                      END
                    END
                END  
              END    
            END   
        END    
    END        
  END
END
 