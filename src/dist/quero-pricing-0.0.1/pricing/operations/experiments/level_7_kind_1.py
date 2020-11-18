
from pricing.operations.alternative_formulas import real_discount_level_7_kind_1,\
	 take_rate

def get_experiment(): 
	return {

	"name": "level 7 kind 1",
	"fee_experiment_alternatives":[
		{
			"name":'baseline',
			"fee_formula":real_discount_level_7_kind_1.get_fee_formula(k=1.),
			"fee_subsidy_formula":""
		},
	  	{
			"name":'dynamic pricing',
			"fee_formula":real_discount_level_7_kind_1.get_fee_formula(k=1.),
			"fee_subsidy_formula":""
	 	},
		{
			"name":'seasonality plus',
			"fee_formula":real_discount_level_7_kind_1.get_fee_formula(k=1.1),
			"fee_subsidy_formula":""
	  	},
	  	{
			"name":'seasonality minus',
			"fee_formula":real_discount_level_7_kind_1.get_fee_formula(k=0.9),
			"fee_subsidy_formula":""
	  	},
		{
			"name":'dummy'
		}
]}