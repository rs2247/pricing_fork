from pricing.operations.experiments.helper import *

from pricing.operations.alternative_formulas import real_discount_level_7_kind_1,\
	 take_rate,pass_along_subsidy

def get_experiment():
	name = "level 7 kind 1"
	dynamic_k = get_dynamic_k_for_experiment(name)
	return {

	"name": name,
	"target_offers_sql": load_target_offers(__file__),
	"fee_experiment_alternatives":[
		{
			"name":'baseline',
			"fee_formula_sql":real_discount_level_7_kind_1.get_fee_formula_sql(k=1.),
			"fee_subsidy_formula_sql":pass_along_subsidy.get_fee_formula_sql(),
			"fee_discount_formula_sql":""
		},
	  	{
			"name":'dynamic pricing',
			"fee_formula_sql":real_discount_level_7_kind_1.get_fee_formula_sql(k=dynamic_k),
			"fee_subsidy_formula_sql":pass_along_subsidy.get_fee_formula_sql(),
			"fee_discount_formula_sql":""
	 	},
		{
			"name":'seasonality plus',
			"fee_formula_sql":real_discount_level_7_kind_1.get_fee_formula_sql(k=1.1),
			"fee_subsidy_formula_sql":pass_along_subsidy.get_fee_formula_sql(),
			"fee_discount_formula_sql":""
	  	},
	  	{
			"name":'seasonality minus',
			"fee_formula_sql":real_discount_level_7_kind_1.get_fee_formula_sql(k=0.9),
			"fee_subsidy_formula_sql":pass_along_subsidy.get_fee_formula_sql(),
			"fee_discount_formula_sql":""
	  	},
	  	{
			"name":'seasonality lowest',
			"fee_formula_sql":real_discount_level_7_kind_1.get_fee_formula_sql(k=0.5),
			"fee_subsidy_formula_sql":pass_along_subsidy.get_fee_formula_sql(),
			"fee_discount_formula_sql":""
	  	},
	  	{
			"name":'seasonality minus 25',
			"fee_formula_sql":real_discount_level_7_kind_1.get_fee_formula_sql(k=0.75),
			"fee_subsidy_formula_sql":pass_along_subsidy.get_fee_formula_sql(),
			"fee_discount_formula_sql":""
	  	},
	  	{
			"name":'seasonality plus 25',
			"fee_formula_sql":real_discount_level_7_kind_1.get_fee_formula_sql(k=1.25),
			"fee_subsidy_formula_sql":pass_along_subsidy.get_fee_formula_sql(),
			"fee_discount_formula_sql":""
	  	},
		{
			"name":'dummy'
		}
]}
