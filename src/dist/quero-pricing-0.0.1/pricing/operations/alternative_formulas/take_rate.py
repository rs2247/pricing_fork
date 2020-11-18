
from pricing.operations.utils import *

def get_fee_formula(tr):

	n_months = format_pql('''
	if (<= max_payments 12)
	    (* 9 (+ 0.09884 (* 0.0846312 (- max_payments 2))))
	    if (<= max_payments 24)
	      (* 9 (+ 0.94515 (* 0.0603743 (- max_payments 12))))
	      if (<= max_payments 36)
	        (* 9 (+ 1.66965 (* 0.0436079 (- max_payments 24))))
	        if (<= max_payments 48)
	          (* 9 (+ 2.19294 (* 0.0357802 (- max_payments 36))))
	          if (<= max_payments 60)
	            (* 9 (+ 2.62230 (* 0.0307322 (- max_payments 48))))
	            (2.69199)
	''') 

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
		      if (> {min} (* (* {tr} offered_price) ({n_months})) )
		        {min}
		        (* (* {tr} offered_price) ({n_months}))
	'''

	return format_pql(fee_formula,{'tr':tr,'n_months':n_months})

def get_fee_formula_sql(k=1):
	
	sql_filepath = __file__[:-3] + '.sql'
	return format_sql(sql_filepath,{'k':k})		

