
from pricing.operations.utils import *
import os 

def get_fee_formula(k=1):

	fee_formula = '''
	 if (in university_id [{fixed_99}])
	    (99.0)
	    if (in university_id [{full_fixed}])
	      if (> ({min}) (full_price))
	        ({min})
	        (full_price)  
	      if (in university_id [{offered_fixed}])
	        if (> ({min}) (offered_price))
	          ({min})
	          (offered_price) 
	        if (& (= digital_admission false) (> 0.1 (- 1 (/ (offered_price) (/ (* (full_price) (- (100) (commercial_discount))) (100))))))
	          if (> ({min}) (* (* 1.4 (/ (* (full_price) (- (100) (commercial_discount))) (100))) {k}))
	            ({min})
	            if (< full_price (* (* 1.4 (/ (* (full_price) (- (100) (commercial_discount))) (100))) {k}))
	              if (> ({min}) (full_price))
	                ({min})
	                (full_price)
	              if (> commercial_discount discount_percentage)
	                if (> ({min}) (* 3.5 offered_price))
	                  {min}
	                  (* 3.5 offered_price)  
	                (* (* 1.4 (/ (* (full_price) (- (100) (commercial_discount))) (100))) {k})  
	          if (> ({min}) (* (- (/ (* (* (2.3) (full_price)) (- (100) (commercial_discount))) (100)) (offered_price)) {k}))
	            ({min})
	            if (< full_price (* (- (/ (* (* (2.3) (full_price)) (- (100) (commercial_discount))) (100)) (offered_price)) {k}))
	              if (> ({min}) (full_price))
	                ({min})
	                (full_price)
	              if (> commercial_discount discount_percentage)
	                if (> ({min}) (* 1.4 offered_price))
	                  {min}
	                  (* 1.4 offered_price)  
	                (* (- (/ (* (* (2.3) (full_price)) (- (100) (commercial_discount))) (100)) (offered_price)) {k})
	'''

	return format_pql(fee_formula,{'k':k})

def get_fee_formula_sql(k=1):
	
	sql_filepath = __file__[:-3] + '.sql'
	return format_sql(sql_filepath,{'k':k})		


