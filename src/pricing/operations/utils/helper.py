
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


def format_sql_formula(fee_formula,params={}):

	ALIASES = {
		# 'commercial_discount': 'coalesce(university_offers.commercial_discount,offers.commercial_discount)',
		'commercial_discount': 'coalesce(pricing_offers_regressive_discounts.commercial_weighted_discount,coalesce(university_offers.commercial_discount,offers.commercial_discount))',

		# 'real_discount': '(1-(offered_price/((university_offers.full_price * (100 - coalesce(university_offers.commercial_discount,offers.commercial_discount)))/100)))',
		'real_discount': '(1-(offered_price/((university_offers.full_price * (100 - coalesce(pricing_offers_regressive_discounts.commercial_weighted_discount,coalesce(university_offers.commercial_discount,offers.commercial_discount))))/100)))',

		# 'offered_balcao': 'university_offers.full_price * ((100 - coalesce(university_offers.commercial_discount,offers.commercial_discount))/100)',
		'offered_balcao': 'university_offers.full_price * ((100 - coalesce(pricing_offers_regressive_discounts.commercial_weighted_discount,coalesce(university_offers.commercial_discount,offers.commercial_discount)))/100)',

		'offered_qb': 'coalesce(pricing_offers_regressive_discounts.weighted_offered_price,offers.offered_price)',

		'fator_wtp': 'CASE WHEN (pricing_factors.target_offered IS NULL) THEN 1 ELSE CASE WHEN (pricing_factors.target_offered/offers.offered_price) > 1.3 THEN (1.3) ELSE CASE WHEN (pricing_factors.target_offered/offers.offered_price) < (0.7) THEN 0.7 ELSE (pricing_factors.target_offered/offers.offered_price) END END END'
	}

	#EXCEPTIONS -> SQL EXCEPTIONS
	format_dict = {key:value.replace(' ',',') for key,value in EXCEPTIONS.items()}
	format_dict.update(GLOBAL_PARAMS)
	format_dict.update(params)

	#ALIASES
	format_dict.update(ALIASES)

	return fee_formula.format_map(format_dict)
