import json
import requests
from datetime import datetime
from pricing.operations.experiments import *
import os

def update_experiment_fields(experiment_id, fields_map):
  payload = str(json.dumps(fields_map))
  headers = {'Content-type': 'application/json'}
  return requests.put(\
     url = os.environ["PRICING_API_HOST"] + 'api/pricing/experiments/'+str(experiment_id)\
     ,auth = (os.environ['PRICING_API_USER'],os.environ['PRICING_API_PASSWORD']), data = payload, headers = headers)

def update_experiment_field(experiment_id,field,value):
  payload = str(json.dumps({ field: value }))
  headers = {'Content-type': 'application/json'}
  return requests.put(\
     url = os.environ["PRICING_API_HOST"] + 'api/pricing/experiments/'+str(experiment_id)\
     ,auth = (os.environ['PRICING_API_USER'],os.environ['PRICING_API_PASSWORD']), data = payload, headers = headers)

def recalculate_alternatives(experiment_id,reason,university_ids=None,course_ids=None,city_ids=None):

  if (reason == None):
    raise ValueError("You should input a reason")

  if (university_ids != None) and (course_ids != None):
    raise ValueError("You should input a list of university_ids or a list of course_ids")

  base_host = os.environ["PRICING_API_HOST"]

  filters = {}
  if university_ids is not None:
    filters.update({'university_id':university_ids})
  if course_ids is not None:
    filters.update({'course_id':course_ids})
  if city_ids is not None:
    filters.update({'city_id':city_ids})

  print("Recalculating using as base_host: ", base_host, "  -> with filters: ", filters)
  if 'PYSPARK_PYTHON' in os.environ:
    confirm = 'y'
  else:
    confirm = input("Are you sure [y,N]? ")

  if confirm == 'y':
    payload = str(json.dumps({'filters': filters, 'reason': reason}))

    headers = {'Content-type': 'application/json'}
    response = requests.post(
      url = os.environ['PRICING_API_HOST'] + 'api/pricing/experiments/'+str(experiment_id)+'/recalculate_alternatives_fees',
      auth =  (os.environ['PRICING_API_USER'],os.environ['PRICING_API_PASSWORD']), data = payload, headers = headers)

    if response.status_code == 200:
      print("[", datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"] Sucess.")
      print("Check job at https://app.datadoghq.com/logs?cols=host%2Cservice%2C%40timestamp&event&from_ts=1576593859294&index=main&live=true&messageDisplay=inline&query=%22Starting+experiment+setup+job%22+OR++%22Recalculating+alternative+fees+for+experiment%22+OR+%22Recalculating+alternative+fees+for+experiment%22+OR+%22End+of+recalculations+for%22+OR+%22Experiment+setup+job+finished%22&stream_sort=asc&to_ts=1576597459294")
    else:
      response.encoding = 'utf-8'
      print(">>>>> ERROR CALLING RECALCULATE <<<<< " + str(response.content))

    return response
  else:
    print("Job aborted")


def check_field_change(new_alternative, current_alternative, field):
  if field in current_alternative:
    if current_alternative[field] == None:
      if new_alternative[field] != None and new_alternative[field].strip(' ') != '':
        return True
    else:
      return new_alternative[field].strip(' ') != current_alternative[field].strip(' ')

def check_experiments_coverage(spark):
  select_fields = ''
  subselect_fields = ''
  separator = ''
  experiments_names = []

  sum_fields = ''
  separator_sum = ''
  for experiment_info in experiments_list:
    experiment = experiment_info.get_experiment()
    experiments_names.append(experiment['name'].replace(' ', '_'))
    subselect_fields = subselect_fields + separator + 'exp_' + experiment['name'].replace(' ', '_')
    select_fields = select_fields + separator + 'case when ' + experiment['target_offers_sql'] + ' then 1 end as exp_' + experiment['name'].replace(' ', '_')
    sum_fields = sum_fields + separator_sum + 'coalesce(exp_' + experiment['name'].replace(' ', '_') + ', 0)'
    separator = ',\n'
    separator_sum = ' + '

  sum_fields = sum_fields + ' as test_sum'

  select_query = """
  select
    *
  from (
    select
      offer_id,
      {subselect_fields},
      {sum_fields}
    from (
      select
        offers.id as offer_id,
        {select_fields}
      from
        querobolsa_production.offers
      inner join querobolsa_production.universities on (universities.id = offers.university_id)
      inner join querobolsa_production.courses on (courses.id = offers.course_id)
      inner join querobolsa_production.levels on (levels.name = courses.level and levels.parent_id is not null)
      inner join querobolsa_production.kinds on (kinds.name = courses.kind and kinds.parent_id is not null)
      where offers.enabled and not offers.restricted
    ) as d
  ) as d where test_sum > 1""".format(select_fields=select_fields, sum_fields=sum_fields, subselect_fields=subselect_fields)

  superposed_experiments = set()

  df = spark.sql(select_query)
  if df.count() > 0:
    print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>=<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<')
    print('>>>>>> WARNING: Offers selected for multiples experiments <<<<<')
    print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>=<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<')

    for row in df.collect():
      for experiment in experiments_names:
        if row['exp_' + experiment] == 1:
          # experiments_dict[experiment] = 1
          superposed_experiments.add(experiment)

    print('Experiments with superposition: ' + str(superposed_experiments))

    return False
  else:
    print('OK')

    return True

def dump_report(report_map):
  changes = False
  if 'target_offers_sql' in report_map:
    print('target_offers_sql: ' + report_map['target_offers_sql'])
    changes = True

  if report_map['fields'] != {}:
    print('Fields with modification: ' + str(report_map['fields']))
    changes = True

  if report_map['creation'] != []:
    print('Alternatives creation: ' + str(report_map['creation']))
    changes = True

  if not changes:
    print('No changes')

def check_experiments(update=False,spark=None,check_coverage=True):
  base_host = os.environ["PRICING_API_HOST"]
  print("Using as base_host: ", base_host)
  if check_coverage:
    if update and spark == None:
      print("Can't update without spark instance")
      return

    if update:
      if not check_experiments_coverage(spark):
        return

  else:
    if update:
      if 'PYSPARK_PYTHON' in os.environ:
        confirm = 'y'
      else:
        confirm = input("Running without experiments coverage check. Are you really sure [y,N]? ")

      if confirm != 'y':
        return

  if update:
    if 'PYSPARK_PYTHON' in os.environ:
      confirm = 'y'
    else:
      confirm = input("Will update all experiments with divergences. Are you sure [y,N]? ")

    if confirm != 'y':
      update = False
      print('Update skiped. Running check only')

  base_host = os.environ["PRICING_API_HOST"]
  # print("Using as base_host: ", base_host)
  headers = {'Content-type': 'application/json'}
  response = requests.get(
    url = base_host + 'api/pricing/experiments/',
    auth = (os.environ['PRICING_API_USER'],os.environ['PRICING_API_PASSWORD']),
    headers = headers
  )

  if response.status_code != 200:
    response.encoding = 'utf-8'
    print(">>>>> ERROR RETRIEVING EXPERIMENTS <<<<< " + str(response.content))
    return

  current_experiments = response.json()['experiments']

  for experiment_info in experiments_list:
    experiment = experiment_info.get_experiment()

    filtered = list(filter(lambda experiment_info: experiment_info['name'] == experiment['name'], current_experiments))

    if len(filtered) == 1:
      experiment_info = filtered[0]
      experiment_id = experiment_info['id']
      print('>>>> ' + experiment['name'] + ' ID: ' + str(experiment_id) + ' <<<<<<')

      report_map = validate_experiment_altenatives(experiment_info, experiment, update)

      if experiment_info['target_offers_sql'] != experiment['target_offers_sql']:
        report_map['target_offers_sql'] = 'OUTDATED'

        if update:
          update_experiment_field(experiment_id, 'target_offers_sql', experiment['target_offers_sql'])

      dump_report(report_map)

    else:
      if len(filtered) == 0:
        print("Experiment not found: " + experiment['name'] + " create!")

        if update:
          headers = {'Content-type': 'application/json'}

          experiment_request = {
            'name': experiment['name'],
            'target_offers_sql': experiment['target_offers_sql'],
            'start_date': datetime.now().strftime('%Y-%m-%d'),
            'fee_experiment_alternatives_attributes': list(map(lambda x: x.copy(), experiment['fee_experiment_alternatives']))
          }

          for alternative in experiment_request['fee_experiment_alternatives_attributes']:
            if alternative['name']  == 'dummy':
              alternative['baseline'] = True
              alternative['ratio'] = 0
            else:
              alternative['ratio'] = alternative['initial_ratio']
              alternative.pop('initial_ratio', None)
              alternative['baseline'] = False

          response = requests.post(
            url = base_host + 'api/pricing/experiments/',
            auth = (os.environ['PRICING_API_USER'],os.environ['PRICING_API_PASSWORD']),
            headers = headers,
            data = json.dumps(experiment_request)
          )

          print(response.content)

      else:
        print(">>>>> EXPERIMENT WITH DUPLICATED DEFINITION <<<<< " + experiment['name'])


def check_experiment_difference(experiment_id):

  base_host = os.environ["PRICING_API_HOST"]
  print("Using as base_host: ", base_host)
  headers = {'Content-type': 'application/json'}
  response = requests.get(
    url = base_host + 'api/pricing/experiments/' + str(experiment_id),
    auth = (os.environ['PRICING_API_USER'],os.environ['PRICING_API_PASSWORD']),
    headers = headers
  )
  current_experiment = response.json()['experiment']
  new_experiment = get_experiment_by_name(current_experiment['name'])
  print("\nExperiment being checked: ", new_experiment['name'])

  if current_experiment['target_offers_sql'] != new_experiment['target_offers_sql']:
    print('target_offers_sql OUTDATED')

  validate_experiment_altenatives(current_experiment, new_experiment)


def validate_experiment_altenatives(current_experiment, new_experiment, update=False):
  updates_vector = []
  report_map = { 'fields': {}, 'creation': []}
  for current_alternative in current_experiment['fee_experiment_alternatives']:
    updates_map = {}
    if current_alternative['name'] != 'dummy':
      new_alternative = get_alternative_from_experiment(new_experiment,current_alternative['name'])
      # print("\nChecking alternative:", current_alternative['name'], " | id: ", current_alternative['id'] , " ...")
      alternative_name = current_alternative['name']
      change = False

      for field in ['fee_formula_sql','fee_subsidy_formula_sql','fee_discount_formula_sql']:
        if check_field_change(new_alternative, current_alternative, field):
          change=True
          # print("    ", field + " is outdated ...")
          if not field in report_map['fields']:
            report_map['fields'][field] = []

          report_map['fields'][field].append(alternative_name)
          updates_map[field] = new_alternative[field]

      if updates_map != {}:
        updates_map['id'] = current_alternative['id']
        updates_vector.append(updates_map)

  if updates_vector != [] and update:
    fields = {
      'fee_experiment_alternatives_attributes': updates_vector
    }
    update_experiment_fields(current_experiment['id'], fields)

  # verifica criacao de alternativas!
  current_alternatives_names = list(map(lambda x: x['name'], current_experiment['fee_experiment_alternatives']))

  alternatives_creation = False
  for new_alternative in new_experiment['fee_experiment_alternatives']:
    if not new_alternative['name'] in current_alternatives_names:
      # print('Alternative not found: ' + new_alternative['name'])
      report_map['creation'].append({'name': new_alternative['name']})
      if update:
        alternatives_creation = True
        alternative_request = new_alternative.copy()
        if 'initial_ratio' in alternative_request:
          alternative_request['ratio'] = alternative_request['initial_ratio']
          alternative_request.pop('initial_ratio', None)
        else:
          alternative_request['ratio'] = 0
        alternative_request['baseline'] = False

        base_host = os.environ["PRICING_API_HOST"]
        headers = {'Content-type': 'application/json'}
        response = requests.post(
          url = base_host + 'api/pricing/experiments/' + str(current_experiment['id']) + '/alternatives',
          auth = (os.environ['PRICING_API_USER'],os.environ['PRICING_API_PASSWORD']),
          headers = headers,
          data = json.dumps(alternative_request)
        )

        if response.status_code != 200:
          response.encoding = 'utf-8'
          print(">>>>> ERROR CREATING ALTERNATIVE <<<<< " + str(response.content))

  return report_map
