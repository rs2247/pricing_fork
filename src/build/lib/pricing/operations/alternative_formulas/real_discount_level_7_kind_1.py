
from pricing.operations.utils import *

def get_fee_formula(k=1):

	fee_formula = '''
		if (in university_id [{fixed_336}])
		  (336.0)
		  if (in university_id [{fixed_99}])
		    (99.0)
		    if (in university_id [{fixed_f_o}])
		      (- full_price offered_price)    
		      if (in university_id [{pef_old}]) 
		        if (< discount_percentage 40) 
		          if (> ({min}) offered_price) 
		            ({min}) 
		            offered_price 
		          if (> ({min}) (- full_price offered_price)) 
		            ({min}) 
		            (- full_price offered_price)
		        if (< commercial_discount discount_percentage)
		          if (> ({min}) (* (- (* 1.6 (/ (* full_price (- 100 commercial_discount)) 100)) offered_price) {k}))
		            ({min}) 
		            (* (- (* 1.6 (/ (* full_price (- 100 commercial_discount)) 100)) offered_price) {k})
		          if (> ({min}) (* 0.6 offered_price))
		            ({min})
		            (* 0.6 offered_price)         
	'''

	return format_pql(fee_formula,{'k':k})


def get_fee_formula_sql(k=1):
	
	sql_filepath = __file__[:-3] + '.sql'
	return format_sql(sql_filepath,{'k':k})		
