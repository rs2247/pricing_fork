
from pricing.operations.utils import *

def get_fee_subsidy_formula():

	fee_formula = '''    
		if (in university_id [{fixed_99}])
		  99.0
		  if (= level_parent_id 1)
		    if (> ({min_qap_1}) (/ original_value 2))
		      ({min_qap_1})
		      (/ original_value 2)
		    if (> ({min_qap_7}) (- original_value (* 0.4 offered_price)))
		      ({min_qap_7})
		      (- original_value  (* 0.4 offered_price))
	'''

	return format_pql(fee_formula)


def get_fee_formula_sql(k=1):
	
	raise NotImplemented()	
	# sql_filepath = __file__[:-3] + '.sql'
	# return format_sql(sql_filepath,{'k':k})		
