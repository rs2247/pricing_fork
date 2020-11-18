CASE WHEN level_id = 1 
THEN
  CASE WHEN university_id in ({pass_along_presencial_semi}) AND kind_id IN (1,8)
  THEN
    CASE WHEN {min_qap_1} > original_value / 2 
    THEN {min_qap_1}
    ELSE original_value / 2
    END
  ELSE
    CASE WHEN university_id in ({pass_along_ead}) AND kind_id IN (3)
    THEN
      CASE WHEN {min_qap_1} > original_value / 2
      THEN {min_qap_1}
      ELSE original_value / 2
      END
    ELSE
      CASE WHEN university_id in ({pass_along_semi}) AND kind_id IN (8)
      THEN
        CASE WHEN {min_qap_1} > original_value / 2
        THEN {min_qap_1}
        ELSE original_value / 2
        END
      ELSE     
        CASE WHEN university_id in ({pass_along}) AND course_id NOT IN (5944046,5944047)
        THEN
          CASE WHEN {min_qap_1} > original_value / 2 
          THEN {min_qap_1}
          ELSE original_value / 2
          END  
        ELSE
          CASE WHEN campus_id IN ({pass_along_campuses})
          THEN
            CASE WHEN {min_qap_1} > original_value / 2 
            THEN {min_qap_1}
            ELSE original_value / 2
            END
          ELSE original_value
          END   
        END
      END  
    END
  END
WHEN level_id = 7 THEN
  CASE WHEN university_id in ({pass_along_pos}) 
  THEN
    CASE WHEN {min_qap_7_lowest} > original_value - (0.4 * offered_price) 
    THEN {min_qap_7_lowest}
    ELSE original_value - (0.4 * offered_price)
    END
  ELSE original_value
  END
END
