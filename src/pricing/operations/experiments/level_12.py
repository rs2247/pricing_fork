from pricing.operations.experiments.helper import *

from pricing.operations.alternative_formulas import simple_level_12,level_12_subsidy

def get_experiment():
	return {

	"name": "level 12",
	"target_offers_sql": load_target_offers(__file__),
	"fee_experiment_alternatives":[
		{
			"name":'baseline',
			"fee_formula_sql":simple_level_12.get_fee_formula_sql(),
			"fee_subsidy_formula_sql":level_12_subsidy.get_fee_formula_sql(),
			"fee_discount_formula_sql":""
		},
		{
			"name":'dummy'
		}
]}
