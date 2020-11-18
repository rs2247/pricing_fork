
from pricing.operations.utils import *

def get_fee_formula(k=1):

	fee_formula = '''
		if (= level_parent_id 1)
		  if (= kind_parent_id 1)
		    if (& (= digital_admission false) (> 0.1 (- 1 (/ (offered_price) (/ (* (full_price) (- (100) (commercial_discount))) (100))))))
		      if (> ({min}) (* (* 0.8 (/ (* (full_price) (- (100) (commercial_discount))) (100))) {k}))
		        ({min})
		        if (< full_price (* (* 0.8 (/ (* (full_price) (- (100) (commercial_discount))) (100))) {k}))
		          if (> ({min}) (full_price))
		            ({min})
		            (full_price)
		          if (> commercial_discount discount_percentage)
		            if (> ({min}) (* 2.8 offered_price))
		                {min}
		                if (< (full_price) (* 2.8 offered_price))
		                  (full_price)
		                  (* 2.8 offered_price)
		            (* (* 0.8 (/ (* (full_price) (- (100) (commercial_discount))) (100))) {k})
		      if (> ({min}) (* (- (/ (* (* (1.7) (full_price)) (- (100) (commercial_discount))) (100)) (offered_price)) {k}))
		        ({min})
		        if (< full_price (* (- (/ (* (* (1.7) (full_price)) (- (100) (commercial_discount))) (100)) (offered_price)) {k}))
		          if (> ({min}) (full_price))
		            ({min})
		            (full_price)       
		          (* (- (/ (* (* (1.7) (full_price)) (- (100) (commercial_discount))) (100)) (offered_price)) {k})
		          
		    if (= kind_parent_id 3)
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
		                if (< (full_price) (* 3.5 offered_price))
		                  (full_price)
		                  (* 3.5 offered_price)
		              (* (* 1.4 (/ (* (full_price) (- (100) (commercial_discount))) (100))) {k})
		        if (> ({min}) (* (- (/ (* (* (2.3) (full_price)) (- (100) (commercial_discount))) (100)) (offered_price)) {k}))
		          ({min})
		          if (< full_price (* (- (/ (* (* (2.3) (full_price)) (- (100) (commercial_discount))) (100)) (offered_price)) {k}))
		            if (> ({min}) (full_price))
		              ({min})
		              (full_price)       
		            (* (- (/ (* (* (2.3) (full_price)) (- (100) (commercial_discount))) (100)) (offered_price)) {k})
		      if (& (= digital_admission false) (> 0.1 (- 1 (/ (offered_price) (/ (* (full_price) (- (100) (commercial_discount))) (100))))))
		        if (> ({min}) (* (* 1.4 (/ (* (full_price) (- (100) (commercial_discount))) (100))) {k}))
		          ({min})
		          if (< full_price (* (* 1.4 (/ (* (full_price) (- (100) (commercial_discount))) (100))) {k}))
		            if (> ({min}) (full_price))
		              ({min})
		              (full_price)  
		            if (> commercial_discount discount_percentage)
		              if (> ({min}) (* 2.6 offered_price))
		                {min}
		                if (< (full_price) (* 2.6 offered_price))
		                  (full_price)
		                  (* 2.6 offered_price)
		              (* (* 1.4 (/ (* (full_price) (- (100) (commercial_discount))) (100))) {k})
		        if (> ({min}) (* (- (/ (* (* (2.3) (full_price)) (- (100) (commercial_discount))) (100)) (offered_price)) {k}))
		          ({min})
		          if (< full_price (* (- (/ (* (* (2.3) (full_price)) (- (100) (commercial_discount))) (100)) (offered_price)) {k}))
		            if (> ({min}) (full_price))
		              ({min})
		              (full_price)       
		            (* (- (/ (* (* (2.3) (full_price)) (- (100) (commercial_discount))) (100)) (offered_price)) {k})
		  if (= kind_parent_id 1)
		    if (> ({min}) (* (- (* 1.6 (/ (* full_price (- 100 commercial_discount)) 100)) offered_price) {k}))
		      ({min})
		      if (< full_price (* (- (* 1.6 (/ (* full_price (- 100 commercial_discount)) 100)) offered_price) {k}))
		        full_price
		        (* (- (* 1.6 (/ (* full_price (- 100 commercial_discount)) 100)) offered_price) {k})
		    if (> ({min}) (* (- (* 1.5 (/ (* full_price (- 100 commercial_discount)) 100)) offered_price) {k}))
		      ({min}) 
		      if (< full_price (* (- (* 1.5 (/ (* full_price (- 100 commercial_discount)) 100)) offered_price) {k}))
		        full_price
		        (* (- (* 1.5 (/ (* full_price (- 100 commercial_discount)) 100)) offered_price) {k})
	'''

	return format_pql(fee_formula,{'k':k})


def get_fee_formula_sql(k=1):
	
	raise NotImplemented()	
	# sql_filepath = __file__[:-3] + '.sql'
	# return format_sql(sql_filepath,{'k':k})		
