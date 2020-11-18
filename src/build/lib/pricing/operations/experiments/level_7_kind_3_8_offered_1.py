
from pricing.operations.alternative_formulas import real_discount_level_7_kind_3_8


def get_experiment(): 
	return {

	"name": "level 7 kinds 3 8 offered 1",
	"fee_experiment_alternatives":[
		{
			"name":'baseline',
			"fee_formula":real_discount_level_7_kind_3_8.get_fee_formula(k=1.),
			"fee_subsidy_formula":""
		},
	  	{
			"name":'dynamic pricing',
			"fee_formula":real_discount_level_7_kind_3_8.get_fee_formula(k=1.),
			"fee_subsidy_formula":""
	 	},
		{
			"name":'seasonality plus',
			"fee_formula":real_discount_level_7_kind_3_8.get_fee_formula(k=1.1),
			"fee_subsidy_formula":""
	  	},
	  	{
			"name":'seasonality minus',
			"fee_formula":real_discount_level_7_kind_3_8.get_fee_formula(k=0.9),
			"fee_subsidy_formula":""
	  	},
		{
			"name":'dummy'
		}
]}