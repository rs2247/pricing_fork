import json
import requests
from datetime import datetime
from pricing.operations.experiments import * 
import os
import plotly.express as px

def validate_formula(spark,formula):

	df = spark.sql("""
		with offers as (
		select
		  *,
		  1000 * (1 - (discount_percentage / 100)) as offered_price
		from (
		  select *, explode(sequence(1,99)) as discount_percentage from (
			select explode(sequence(0,100, 20)) as commercial_discount, 
			1 as university_id
		  ) as base_commecial_discount
		) as base_discount
		),
		university_offers as (
		select
		  1000 as full_price,
		  null as commercial_discount
		),
		courses as (
		select false as digital_admission
		),

		base_final AS(
		select
		  round(university_offers.full_price
		  *(100-coalesce(university_offers.commercial_discount,offers.commercial_discount))
		  /100,2) AS offered_ies,
		offers.discount_percentage,
		offers.commercial_discount,

		{formula} AS pef     

		from offers, university_offers, courses
		)
		SELECT
		pef/offered_ies AS pef_offered,
		discount_percentage,
		commercial_discount

		FROM
		base_final
	""".format(formula=formula))
  
	return px.line(df.toPandas(),x='discount_percentage',y='pef_offered',color='commercial_discount')

def update_alternative_formula(experiment_id,alternative_id,formula):
	update_alternative_field(experiment_id,alternative_id, 'fee_formula', formula)

def update_alternative_formula_sql(experiment_id,alternative_id,formula):
	update_alternative_field(experiment_id,alternative_id, 'fee_formula_sql', formula)

def update_alternative_subsidy_formula(experiment_id,alternative_id,formula):
	update_alternative_field(experiment_id,alternative_id, 'fee_subsidy_formula', formula)

def update_alternative_subsidy_formula_sql(experiment_id,alternative_id,formula):
	update_alternative_field(experiment_id,alternative_id, 'fee_subsidy_formula_sql', formula)

def update_alternative_field(experiment_id,alternative_id,field,value):
	payload = str(json.dumps({ field: value }))
	headers = {'Content-type': 'application/json'}
	r=requests.put(\
	   url = os.environ["PRICING_API_HOST"] + 'api/pricing/experiments/'+str(experiment_id)+'/alternatives/' + str(alternative_id)\
	   ,auth = (os.environ['PRICING_API_USER'],os.environ['PRICING_API_PASSWORD']), data = payload, headers = headers)

def update_experiment(experiment_id):
	
	base_host = os.environ["PRICING_API_HOST"]
	print("Using as base_host: ", base_host)
	headers = {'Content-type': 'application/json'}
	r=requests.get(
		url = base_host + 'api/pricing/experiments/' + str(experiment_id),
		auth = (os.environ['PRICING_API_USER'],os.environ['PRICING_API_PASSWORD']),
		headers = headers
	)
	current_experiment = r.json()['experiment']
	new_experiment = get_experiment_by_name(current_experiment['name'])
	print("\nExperiment being updated: ", new_experiment['name'])

	#Update offers query
	#TODO Deveria ser aqui? Se nao tiver um sanity check, esse update pode deixar
	#offers fora de experimento

	#Update alternatives
	for current_alternative in current_experiment['fee_experiment_alternatives']:
		if current_alternative['name'] != 'dummy':
			new_alternative = get_alternative_from_experiment(new_experiment,current_alternative['name'])
				
			print("\nChecking alternative:", current_alternative['name'], " | id: ", current_alternative['id'] , " ...")
				
			change = False
			#Fee formula
			if new_alternative['fee_formula'].strip(' ') != current_alternative['fee_formula'].strip(' '):
				change = True
				print("[", datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"] Updating fee_formula ...")
				update_alternative_formula(
					user,
					pwd,
					experiment_id,
					current_alternative['id'],
					new_alternative['fee_formula']
				)
				
			#Fee subsidy formula	
			if new_alternative['fee_subsidy_formula'].strip(' ') != current_alternative['fee_subsidy_formula'].strip(' '):
				change = True
				print("[", datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"] Updating fee_subsidy_formula ...")
				update_alternative_subsidy_formula(
					user,
					pwd,
					experiment_id,
					current_alternative['id'],
					new_alternative['fee_subsidy_formula']
				)
				
			if not change:
				print("Nothing to change")
	
	print("\nFinished updating experiment")


