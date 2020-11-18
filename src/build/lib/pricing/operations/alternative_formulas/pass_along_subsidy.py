
from pricing.operations.utils import *

def get_fee_subsidy_formula():

	fee_formula = '''    
		if (in university_id [{pass_along}])
		    if (> ({min_qap_1}) (/ original_value 2))
				({min_qap_1})
				(/ original_value 2)
		    (original_value)
	'''

	return format_pql(fee_formula)

def get_fee_formula_sql(k=1):
	
	raise NotImplemented()
	# sql_filepath = __file__[:-3] + '.sql'
	# return format_sql(sql_filepath,{'k':k})		
