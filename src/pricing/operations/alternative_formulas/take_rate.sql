
CASE WHEN offers.university_id IN ({fixed_99})
THEN 99.0
ELSE
    CASE WHEN offers.university_id IN ({full_fixed}) THEN
      CASE WHEN university_offers.full_price < {min}
      THEN {min}
      ELSE university_offers.full_price
      END  
    ELSE
      CASE WHEN offers.university_id IN ({offered_fixed}) THEN
        CASE WHEN offers.offered_price < {min} 
        THEN {min}
        ELSE offers.offered_price
        END
      ELSE
        CASE WHEN offers.university_id IN ({offered_fixed_presencial}) AND kinds.parent_id = 1
        THEN 
          CASE WHEN offers.offered_price < {min}
          THEN {min}
          ELSE offers.offered_price
          END
        ELSE     
          CASE WHEN offers.offered_price * {tr} * (CASE 
                                                  WHEN max_payments <= 12 THEN ((((max_payments - 2) * 0.0846312) + 0.09884) * 9)
                                                  WHEN max_payments <= 24 THEN ((((max_payments - 12) * 0.0603743) + 0.94515) * 9)
                                                  WHEN max_payments <= 36 THEN ((((max_payments - 24) * 0.0436079) + 1.66965) * 9)
                                                  WHEN max_payments <= 48 THEN ((((max_payments - 36) * 0.0357802) + 2.19294) * 9)
                                                  WHEN max_payments <= 60 THEN ((((max_payments - 48) * 0.0307322) + 2.62230) * 9)
                                                  ELSE (2.69199)
                                                  END) < {min}
          THEN {min}
          ELSE offers.offered_price * {tr} * 	  (CASE 
                                                  WHEN max_payments <= 12 THEN ((((max_payments - 2) * 0.0846312) + 0.09884) * 9)
                                                  WHEN max_payments <= 24 THEN ((((max_payments - 12) * 0.0603743) + 0.94515) * 9)
                                                  WHEN max_payments <= 36 THEN ((((max_payments - 24) * 0.0436079) + 1.66965) * 9)
                                                  WHEN max_payments <= 48 THEN ((((max_payments - 36) * 0.0357802) + 2.19294) * 9)
                                                  WHEN max_payments <= 60 THEN ((((max_payments - 48) * 0.0307322) + 2.62230) * 9)
                                                  ELSE (2.69199)
                                                  END)
          END   
        END                                                                         
      END                                          
    END
END  