
import pandas as pd 
from string import Formatter
from pricing.operations.utils import EXCEPTIONS,GLOBAL_PARAMS

def format_pql(formula,params={}):
	formula = formula.replace('\t',' ')
	aux = ''
	while aux != formula:
	  aux = formula
	  formula = formula.replace('  ',' ')

	formula = formula.replace('\n','').strip(' ')

	params.update(EXCEPTIONS)
	params.update(GLOBAL_PARAMS)

	return formula.format_map(params)



def format_sql(filepath,params={}):

	with open(filepath, 'r') as file:
		fee_formula = file.read().strip('\n')

	#EXCEPTIONS -> SQL EXCEPTIONS
	format_dict = {key:value.replace(' ',',') for key,value in EXCEPTIONS.items()}
	format_dict.update(GLOBAL_PARAMS)
	format_dict.update(params)

	return fee_formula.format_map(format_dict)
	