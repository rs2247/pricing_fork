from pricing.operations.utils import *
from pricing.operations import experiments

def get_experiment_by_name(experiment_name):
	
	for experiment in experiments.experiments_list:
		if experiment['name'] == experiment_name:
			return experiment

	raise ValueError("No code for experiment with name: " + experiment_name + ". Did you add the code to pricing/operations/experiments/ ? Did you imported the experiment in the folder's __init__ ? ")

def get_alternative_from_experiment(experiment,alternative_name):

	for alternative in experiment['fee_experiment_alternatives']:
		if alternative['name']== alternative_name:
			return alternative

	raise ValueError("No alternative " + alternative + " in experiment " + experiment['name'])
