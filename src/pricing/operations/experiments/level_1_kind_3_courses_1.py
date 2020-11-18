from pricing.operations.experiments.helper import *

from pricing.operations.alternative_formulas import real_discount_level_1_kind_3,\
	 pass_along_subsidy,take_rate,campaign_discount,real_discount_level_1_kind_3_v2

def get_experiment():
	name = "level 1 kind 3 courses 1"
	dynamic_k = get_dynamic_k_for_experiment(name)
	baseline_k = 0.9

	return {

	"name": name,
	"target_offers_sql": load_target_offers(__file__),
	"fee_experiment_alternatives":[
		{
			"name":'baseline',
			"fee_formula_sql":real_discount_level_1_kind_3.get_fee_formula_sql(k=baseline_k),
			"fee_subsidy_formula_sql":pass_along_subsidy.get_fee_formula_sql(),
			"fee_discount_formula_sql":campaign_discount.get_fee_formula_sql()
		},
	  	{
			"name":'dynamic pricing',
			"fee_formula_sql":real_discount_level_1_kind_3.get_fee_formula_sql(k=baseline_k * dynamic_k),
			"fee_subsidy_formula_sql":pass_along_subsidy.get_fee_formula_sql(),
			"fee_discount_formula_sql":campaign_discount.get_fee_formula_sql()
	 	},
		{
			"name":'seasonality plus',
			"fee_formula_sql":real_discount_level_1_kind_3.get_fee_formula_sql(k=1.1*baseline_k),
			"fee_subsidy_formula_sql":pass_along_subsidy.get_fee_formula_sql(),
			"fee_discount_formula_sql":campaign_discount.get_fee_formula_sql()
	  	},
	  	{
			"name":'seasonality minus',
			"fee_formula_sql":real_discount_level_1_kind_3.get_fee_formula_sql(k=0.9*baseline_k),
			"fee_subsidy_formula_sql":pass_along_subsidy.get_fee_formula_sql(),
			"fee_discount_formula_sql":campaign_discount.get_fee_formula_sql()
	  	},
		{
			"name":'seasonality lower',
			"fee_formula_sql":real_discount_level_1_kind_3.get_fee_formula_sql(k=0.75*baseline_k),
			"fee_subsidy_formula_sql":pass_along_subsidy.get_fee_formula_sql(),
			"fee_discount_formula_sql":campaign_discount.get_fee_formula_sql()
	  	},
		{
			"name":'seasonality lowest',
			"fee_formula_sql":real_discount_level_1_kind_3.get_fee_formula_sql(k=0.5*baseline_k),
			"fee_subsidy_formula_sql":pass_along_subsidy.get_fee_formula_sql(),
			"fee_discount_formula_sql":campaign_discount.get_fee_formula_sql()
	  	},
		{
			"name":'seasonality minus 75',
			"fee_formula_sql":real_discount_level_1_kind_3.get_fee_formula_sql(k=0.25*baseline_k),
			"fee_subsidy_formula_sql":pass_along_subsidy.get_fee_formula_sql(),
			"fee_discount_formula_sql":campaign_discount.get_fee_formula_sql()
	  	}, 
		{
			"name":'seasonality minus 25',
			"fee_formula_sql":real_discount_level_1_kind_3.get_fee_formula_sql(k=0.75*baseline_k),
			"fee_subsidy_formula_sql":pass_along_subsidy.get_fee_formula_sql(),
			"fee_discount_formula_sql":campaign_discount.get_fee_formula_sql()
	  	}, 
		{
			"name":'seasonality plus 25',
			"fee_formula_sql":real_discount_level_1_kind_3.get_fee_formula_sql(k=1.25*baseline_k),
			"fee_subsidy_formula_sql":pass_along_subsidy.get_fee_formula_sql(),
			"fee_discount_formula_sql":campaign_discount.get_fee_formula_sql()
	  	}, 	
		{
			"name":'take rate',
			"fee_formula_sql":take_rate.get_fee_formula_sql(tr=0.11),
			"fee_subsidy_formula_sql":pass_along_subsidy.get_fee_formula_sql(),
			"fee_discount_formula_sql":campaign_discount.get_fee_formula_sql()
	  	},
		{
			"name":'customer wtp',
			"fee_formula_sql":real_discount_level_1_kind_3_v2.get_fee_formula_sql(k=1),
			"fee_subsidy_formula_sql":pass_along_subsidy.get_fee_formula_sql(),
			"fee_discount_formula_sql":campaign_discount.get_fee_formula_sql()
		},
		{
			"name":'dummy'
		}
]}
