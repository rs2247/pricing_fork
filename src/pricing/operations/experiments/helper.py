import os
from pricing.operations.utils import *
from pricing.operations import experiments

from pricing.utils import db_connection
import pandas as pd

def load_target_offers(filename):

  if filename is not None:
    with open(filename.replace('.py', '.sql'), 'r') as file:
      return file.read().strip('\n')
  else:
      return ''


def get_experiment_by_name(experiment_name):

	for experiment in experiments.experiments_list:
		if experiment.get_experiment()['name'] == experiment_name:
			return experiment

	raise ValueError("No code for experiment with name: " + experiment_name + ". Did you add the code to pricing/operations/experiments/ ? Did you imported the experiment in the folder's __init__ ? ")

def get_alternative_from_experiment(experiment,alternative_name):

	for alternative in experiment['fee_experiment_alternatives']:
		if alternative['name']== alternative_name:
			return alternative

	raise ValueError("No alternative " + alternative + " in experiment " + experiment['name'])

def update_dynamic_k_for_experiment(name,k_value):
    cursor = db_connection.get_cursor()
    update_query = "update data_science.experiments_dynamic_ratios set k_value = {k_value} where name = '{name}'".format(name=name,k_value=k_value)
    cursor.execute(update_query)
    db_connection.put_cursor(cursor,commit=True)

def get_dynamic_k_for_experiment(name):
    conn = db_connection.get_conn()
    df = pd.read_sql_query("select k_value from data_science.experiments_dynamic_ratios where name = '{name}'".format(name=name), conn)
    db_connection.put_conn(conn)
    return df.k_value[0]
