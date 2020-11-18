

from pricing.operations.alternative_formulas import real_discount_qap,\
	 qap_subsidy

def get_experiment(): 
	return {

	"name": "qap",
	"fee_experiment_alternatives":[
		{
			"name":'baseline',
			"fee_formula":real_discount_qap.get_fee_formula(k=1.),
			"fee_subsidy_formula":qap_subsidy.get_fee_subsidy_formula()
		},
		{
			"name":'dummy'
		}
]}


