
CASE WHEN offers.course_id in (5944046,5944047) THEN offers.offered_price/2  -- 595944046/5944047 are two MEDICINA courses agreed to be in offered
ELSE
  CASE WHEN offers.university_id IN ({fixed_99}) THEN 99.0
  ELSE
    CASE WHEN offers.university_id IN ({full_fixed}) or offers.university_id IN ({full_fixed_presencial}) THEN
      CASE WHEN university_offers.full_price > {min} THEN university_offers.full_price
      ELSE {min}
      END
    ELSE
      CASE WHEN offers.university_id IN ({1_5_full_fixed_presencial}) THEN
        CASE WHEN 1.5 * university_offers.full_price > {min} THEN 1.5 * university_offers.full_price
        ELSE {min}
        END
      ELSE
        CASE WHEN offers.university_id IN ({offered_fixed}) THEN
          CASE WHEN offers.offered_price > {min} THEN offers.offered_price
          ELSE {min}
          END
        ELSE
          CASE WHEN offers.university_id IN ({offered_fixed_presencial}) THEN -- offered_fixed_presencial
            CASE WHEN offers.offered_price > {min} THEN offers.offered_price
            ELSE {min}
            END
          ELSE
            CASE WHEN offers.university_id IN ({average_offered_full_fixed}) THEN
              CASE WHEN (offers.offered_price + university_offers.full_price)/2 > {min}
              THEN (offers.offered_price + university_offers.full_price)/2
              ELSE {min}
              END
            ELSE
              CASE WHEN offers.university_id IN ({offered_one_half}) THEN
                CASE WHEN offers.offered_price * 1.5 > {min}
                THEN offers.offered_price * 1.5
                ELSE {min}
                END
              ELSE
                CASE WHEN (((NOT courses.digital_admission) AND (offers.university_id NOT IN ({fake_admission}))) AND (0.1 > {real_discount})) THEN
                  CASE WHEN {commercial_discount} >= offers.discount_percentage THEN -- canibalização
                    CASE WHEN offers.university_id IN ({offered_limit})
                    THEN
                      CASE WHEN offers.offered_price > {min}
                      THEN offers.offered_price
                      ELSE {min}
                      END
                    ELSE
                      CASE WHEN university_offers.full_price > {min}
                      THEN university_offers.full_price
                      ELSE {min}
                      END
                    END
                  ELSE
                    CASE WHEN ({k} * (0.8 * ({offered_balcao}))) * ({fator_wtp})  < {min} THEN {min}
                    ELSE
                      CASE WHEN ({k} * (0.8 * ({offered_balcao}))) * ({fator_wtp}) > university_offers.full_price THEN
                        CASE WHEN university_offers.full_price < {min} THEN {min}
                        ELSE
                          CASE WHEN offers.university_id IN ({offered_limit})
                          THEN
                            CASE WHEN offers.offered_price > {min}
                            THEN offers.offered_price
                            ELSE {min}
                            END
                          ELSE university_offers.full_price
                          END
                        END
                      ELSE
                        CASE WHEN offers.university_id IN ({offered_limit}) THEN
                          CASE WHEN ({k} * (0.8 * ({offered_balcao}))) * ({fator_wtp}) > offers.offered_price THEN
                            CASE WHEN offers.offered_price > {min}
                            THEN offers.offered_price
                            ELSE {min}
                            END
                          ELSE
                            CASE WHEN ({k} * (0.8 * ({offered_balcao}))) * ({fator_wtp}) > {min}
                            THEN ({k} * (0.8 * ({offered_balcao}))) * ({fator_wtp})
                            ELSE {min}
                            END
                          END
                        ELSE
                          CASE WHEN ({k} * (0.8 * ({offered_balcao}))) * ({fator_wtp}) > {min}
                          THEN ({k} * (0.8 * ({offered_balcao}))) * ({fator_wtp}) -- Não precisa verificação de full (está no else da condição maior que full)
                          ELSE {min}
                          END
                        END
                      END
                    END
                  END
                ELSE
                  CASE WHEN ({k} * (({real_discount}) * 1 + 0.7) * ({offered_balcao})) * ({fator_wtp}) < {min} THEN {min}
                  ELSE
                    CASE WHEN ({k} * (({real_discount}) * 1 + 0.7) * ({offered_balcao})) * ({fator_wtp}) > university_offers.full_price
                    THEN
                      CASE WHEN offers.university_id IN ({offered_limit}) THEN
                        CASE WHEN offers.offered_price > {min}
                        THEN offers.offered_price
                        ELSE {min}
                        END
                      ELSE
                        CASE WHEN university_offers.full_price < {min}
                        THEN {min}
                        ELSE university_offers.full_price
                        END
                      END
                    ELSE
                        CASE WHEN {commercial_discount} >= offers.discount_percentage THEN -- Canibalização (admissão digital)
                          CASE WHEN 0.8 * offers.offered_price * ({fator_wtp}) < {min}
                          THEN {min}
                          ELSE 0.8 * offers.offered_price * ({fator_wtp})
                          END
                        ELSE
                          CASE WHEN offers.university_id IN ({offered_limit}) THEN
                            CASE WHEN ({k} * (({real_discount}) * 1 + 0.7) * ({offered_balcao})) * ({fator_wtp}) > offers.offered_price
                            THEN
                              CASE WHEN offers.offered_price > {min}
                              THEN offers.offered_price
                              ELSE {min}
                              END
                            ELSE
                              CASE WHEN ({k} * (({real_discount}) * 1 + 0.7) * ({offered_balcao})) * ({fator_wtp}) > {min}
                              THEN ({k} * (({real_discount}) * 1 + 0.7) * ({offered_balcao})) * ({fator_wtp})
                              ELSE {min}
                              END
                            END
                          ELSE
                            CASE WHEN ({k} * (({real_discount}) * 1 + 0.7) * ({offered_balcao})) * ({fator_wtp}) > {min}
                            THEN ({k} * (({real_discount}) * 1 + 0.7) * ({offered_balcao})) * ({fator_wtp})
                            ELSE {min}
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
      END
    END
  END
END