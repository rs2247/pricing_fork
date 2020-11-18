from pricing.operations.experiments.helper import *

from pricing.operations.alternative_formulas import real_discount_level_7_kind_3_8,pass_along_subsidy

def get_experiment():
	return {

	"name": "kroton pos",
	"target_offers_sql": load_target_offers(__file__),
	"fee_experiment_alternatives":[
		{
			"name":'baseline',
			"fee_formula_sql":"237.04",
			"fee_subsidy_formula_sql":"99.99",
			"fee_discount_formula_sql":""
		},
	  	{
			"name":'price 0',
			"fee_formula_sql":"238.05",
			"fee_subsidy_formula_sql":"100.99",
			"fee_discount_formula_sql":""
	 	},
		{
			"name":'price 30',
			"fee_formula_sql":"237.04",
			"fee_subsidy_formula_sql":"99.9",
			"fee_discount_formula_sql":""
	  	},
	  	{
			"name":'price 60',
			"fee_formula_sql":"237.04",
			"fee_subsidy_formula_sql":"99.9",
			"fee_discount_formula_sql":""
	  	},
		{
			"name":'dummy'
		}
]}
