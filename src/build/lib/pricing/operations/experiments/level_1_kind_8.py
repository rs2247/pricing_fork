
from pricing.operations.alternative_formulas import real_discount_level_1_kind_8,\
	 pass_along_subsidy,take_rate

def get_experiment(): 
	return {

	"name": "level 1 kind 8",
	"fee_experiment_alternatives":[
		{
			"name":'baseline',
			"fee_formula":real_discount_level_1_kind_8.get_fee_formula(k=1.),
			"fee_subsidy_formula":pass_along_subsidy.get_fee_subsidy_formula()
		},
	  	{
			"name":'dynamic pricing',
			"fee_formula":real_discount_level_1_kind_8.get_fee_formula(k=1.),
			"fee_subsidy_formula":pass_along_subsidy.get_fee_subsidy_formula()
	 	},
		{
			"name":'seasonality plus',
			"fee_formula":real_discount_level_1_kind_8.get_fee_formula(k=1.1),
			"fee_subsidy_formula":pass_along_subsidy.get_fee_subsidy_formula()
	  	},
	  	{
			"name":'seasonality minus',
			"fee_formula":real_discount_level_1_kind_8.get_fee_formula(k=0.9),
			"fee_subsidy_formula":pass_along_subsidy.get_fee_subsidy_formula()
	  	},
		{
			"name":'take rate',
			"fee_formula":take_rate.get_fee_formula(tr=0.07),
			"fee_subsidy_formula":pass_along_subsidy.get_fee_subsidy_formula()
	  	},	  	
		{
			"name":'dummy'
		}
]}