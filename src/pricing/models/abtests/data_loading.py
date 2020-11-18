
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import os
import math
import random

from pprint import pprint
import scipy

from pyspark.sql import SparkSession
from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.sql.types import StringType
from unidecode import unidecode
py_round = round
# from pyspark.sql.functions import *
pd.set_option('display.max_columns', None)
from datetime import *
from pricing.utils import load_from_db_cache
import time
from pricing.utils import logger

from pricing.utils import db_connection
import scipy.stats

def get_experiment_test_data(df_all,selected_experiment,filters = {}):
  print('get_experiment_test_data experiment_id: ' + str(experiment_id))
  '''
  Filters the output of @load_raw_test_data for a specific experiment_id
  '''
  df = df_all[
    (df_all['fee_experiment_id']==experiment_id)&\
    (df_all['alternative']!='dummy')
  ]

  for filter_col,filter_value in filters.items():
    df = df[ df[filter_col]==filter_value]

  return df

def clean_date(input_date):
  if input_date is None:
    return None
  if "T" in input_date:
    return datetime.strptime(input_date, '%Y-%m-%dT00:00:00').strftime('%Y-%m-%d')
  return input_date

def _build_end_date_clause(end_date, date_field = 'date'):
  if not end_date is None:
    return "and {date_field} <= '{end_date}'".format(date_field=date_field,end_date=end_date)
  return ''

def get_alternatives_for_experiment(experiment_id, start_date = '2020-01-11', end_date = None):
  conn = db_connection.get_conn()
  start_date = clean_date(start_date)
  end_date = clean_date(end_date)
  df = pd.read_sql_query("""
  select
    max(alternative_ratio) as alternative_ratio,
    alternative,
    alternative_id
  from
    data_science.experiments_aggregate_base_per_city
  where
    fee_experiment_id = {selected_experiment}
    and date >= '{start_date}'
    {end_date_clause}
  group by alternative, alternative_id
  """.format(selected_experiment=experiment_id,start_date=start_date,end_date_clause=_build_end_date_clause(end_date)), conn)
  db_connection.put_conn(conn)
  return df

def get_experiment_agg_data_per_city(experiment_id, start_date, end_date=None):
  conn = db_connection.get_conn()
  start_date = clean_date(start_date)
  end_date = clean_date(end_date)

  aux = pd.read_sql_query("""
  select
    sum(orders)::integer as order_id,
    city,
    city_id
  from
    data_science.experiments_aggregate_base_per_city
  where
    fee_experiment_id = {selected_experiment}
    and date >= '{start_date}'
    {end_date_clause}
  group by
   city,
   city_id
  order by 1 desc
  """.format(selected_experiment=experiment_id,start_date=start_date,end_date_clause=_build_end_date_clause(end_date)), conn)
  db_connection.put_conn(conn)
  return aux

# METODO COMPLEXO?
def get_experiment_agg_data(experiment_id, start_date = '2020-01-11', end_date = None, filters = {}, ordering = None):
  '''
    Filters the input table for a specific experiment_id
  '''
  conn = db_connection.get_conn()
  print('get_experiment_agg_data experiment_id: ' + str(experiment_id) + ' start_date: ' + str(start_date) + ' end_date: ' + str(end_date))
  start_date = clean_date(start_date)
  end_date = clean_date(end_date)

  filters_clause = ""
  filters_array = []
  input_table = 'data_science.experiments_aggregate_base'
  if 'city' in filters:
    filters_array.append("city = '{selected_city}'".format(selected_city=filters['city']))
    # filters_clause = " and city = '{selected_city}'".format(selected_city=filters['city'])
    input_table = 'data_science.experiments_aggregate_base_per_city'
  if 'distinct_dates_7' in filters:
    filters_array.append("distinct_dates_7 >= 7")
  if 'distinct_dates_14' in filters:
    filters_array.append("distinct_dates_7 >= 14")
  if 'alternative_ratio' in filters:
    filters_array.append("alternative_ratio > 0")

  if filters_array != []:
    filters_clause = ' AND ' + ' and '.join(filters_array)


  order_clause = ''
  if not ordering is None:
    order_clause = 'order by {ordering}'.format(ordering=ordering)

  query = """
  select
    *
  from
    {input_table}
  where
    fee_experiment_id = {selected_experiment}
    and date >= '{start_date}'
    {end_date_clause}
    {filters_clause}
  {order_clause}
  """.format(selected_experiment=experiment_id,input_table=input_table,start_date=start_date,end_date_clause=_build_end_date_clause(end_date),filters_clause=filters_clause,order_clause=order_clause)

  # print(query)

  df_agg = pd.read_sql_query(query, conn)
  df_agg = adjust_zero_ratios(adjust_dataframe_fields(df_agg))
  db_connection.put_conn(conn)
  return df_agg

def get_alternative_agg_data(alternative_id, start_date, end_date, filters = {}):
  conn = db_connection.get_conn()
  baseline_alternative = sample_baseline_for_alternative(alternative_id, conn)

  df_dates = sample_valid_dates(alternative_id, baseline_alternative, conn, start_date, end_date)
  dates_str = ','.join(map(lambda x: "'{x}'".format(x=x.strftime('%Y-%m-%d')), df_dates['date'].values[:-1]))

  alternative_ids = [str(alternative_id), str(baseline_alternative)]

  location_filter = ""
  if 'city' in filters:
    location_filter = " AND city = '{city}'".format(city=filters['city'])

  base_selection = get_consolidated_orders_data_sql() + """
    and created_day in ({dates}) and alternative_id in ({alternatives}) {location_filter}
  """.format(alternatives=','.join(alternative_ids),dates=dates_str,location_filter=location_filter)

  df_cum_results = pd.read_sql_query(get_cumulative_agg_results_sql(base_selection, postgres=True, initial_date=df_dates['date'].values[0], final_date=df_dates['date'].values[-2]), conn)
  df_cum_results = adjust_dataframe_fields(df_cum_results)
  db_connection.put_conn(conn)
  return df_cum_results

def sample_baseline_for_alternative(alternative_id, conn):
  df = pd.read_sql_query("""
  select
    alternative_id
  from
    data_science.experiments_aggregate_base
  where
    alternative = 'baseline' and
    fee_experiment_id in (
      select fee_experiment_id from data_science.experiments_aggregate_base where alternative_id = {alternative_selection}
    )
  limit 1
  """.format(alternative_selection=alternative_id), conn)
  return df.iloc[0]['alternative_id']

def sample_valid_dates(alternative_id, baseline_id, conn, start_date, end_date):
  df_dates = pd.read_sql_query("""
  select
    experiments_aggregate_base.date
  from
    data_science.experiments_aggregate_base
  inner join
    data_science.experiments_aggregate_base baseline_data
      on (baseline_data.date = experiments_aggregate_base.date and baseline_data.alternative_id = {baseline_alternative} and baseline_data.alternative_ratio > 0)
  where
    experiments_aggregate_base.alternative_id = {alternative_selection} and experiments_aggregate_base.alternative_ratio > 0
    and experiments_aggregate_base.date >= '{start_date}'
    {end_date_clause}
  order by experiments_aggregate_base.date
  """.format(alternative_selection=alternative_id,baseline_alternative=baseline_id,start_date=start_date,end_date_clause=_build_end_date_clause(end_date, 'experiments_aggregate_base.date')), conn)
  return df_dates

def adjust_zero_ratios(df):
  def ratio_cleaning(x,x_ratio):
    return (x*(x_ratio >0).astype(int)).replace(0,np.nan)

  fields = ['arpu', 'arpu_7', 'arpu_14', 'registered_conversion', 'av_ticket', 'registered_conversion_7', 'av_ticket_7', 'registered_conversion_14', 'av_ticket_14']
  for field in fields:
    if field in df.columns:
      df[field] = ratio_cleaning(df[field], df['alternative_ratio'])
  return df

def adjust_dataframe_fields(df):
  if 'alternative_ratio' in df.columns:
    df['alternative_ratio']= df['alternative_ratio'].astype(float)
  if 'price' in df.columns:
    df['price']= df['price'].astype(float)
  if 'value' in df.columns:
    df['value']= df['value'].astype(float)
  if 'discount_value' in df.columns:
    df['discount_value']= df['discount_value'].astype(float)
  if 'baseline_value' in df.columns:
    df['baseline_value']= df['baseline_value'].astype(float)

  if not 'created_day' in df.columns:
    if 'created_at' in df.columns:
      df['created_day'] = pd.to_datetime(df['created_at'].dt.date )

    if not 'created_at' in df.columns and 'date' in df.columns:
      df['created_day'] = df['date']

  return df

def get_cumulative_agg_results_sql(consolidated_orders_data_sql, aditional_grouping = [], postgres=False, initial_date='2020-01-11', final_date = None):
  """
  Returns the query for consolidated data with daily cummulative results for input @consolidated_orders_data_sql
  Important!
    - final_date must be supplied

  Args:
      consolidated_orders_data_sql (string): query with base selection on data_science.base_ordens_experimentos
      aditional_grouping ([string]): list of fields to be additionally selected and grouped
      postgres (boolean): flag indicating if must be generated postgresql version of sql query

  Returns:
      query: sql string for consolidated data with daily cummulative results
      initial_date (string): string representing the output range initial date
      final_date (string): string representing the output range final date
  """
  interval_separator = ''
  dates_set = "select explode(sequence(date('{initial_date}'), date('{final_date}'))) as date"
  if postgres:
    interval_separator = "'"
    dates_set = "select date(generate_series(date('{initial_date}'), date('{final_date}'), interval '1 day')) as date"

  aditional_grouping_str = ''
  if aditional_grouping != []:
    aditional_grouping_str = ',' + ','.join(aditional_grouping)

  if final_date is None:
    final_date = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')

  query = """
  with base_orders as (
    {sql_input}
  )

  select
    *,
    customers as n_visits,
    case when customers > 0 then revenue / customers end as arpu,
    case when customers_7 > 0 then revenue_7 / customers_7 end as arpu_7,
    case when customers_14 > 0 then revenue_14 / customers_14 end as arpu_14,
    case when customers > 0 then n_paids / customers end as registered_conversion,
    case when customers_7 > 0 then n_paids_7 / customers_7 end as registered_conversion_7,
    case when customers_14 > 0 then n_paids_14 / customers_14 end as registered_conversion_14,
    case when n_paids > 0 then revenue / n_paids end as av_ticket,
    case when n_paids_7 > 0 then revenue_7 / n_paids_7 end as av_ticket_7,
    case when n_paids_14 > 0 then revenue_14 / n_paids_14 end as av_ticket_14,
    case when orders > 0 then cumsum_offered_price / orders end as av_offered_price,
    case when orders > 0 then cumsum_price / orders end as av_price,
    date as created_day
  from (
    select
      base_dates.date,
      max(case when date(base_orders.created_at) = base_dates.date then base_orders.alternative_ratio end) as alternative_ratio,
      count(distinct customer_id) as customers,
      count(distinct case when date(base_orders.created_at) = base_dates.date then customer_id end) as customers_today,
      sum(offered_price) as cumsum_offered_price,
      sum(price) as cumsum_price,
      count(order_id) as orders,
      count(case when checkout_step = 'paid' and not exchange then order_id end) as n_paids,
      sum(case when checkout_step = 'paid' and not exchange then price end) as revenue,

      count(case when date(registered_at) >= base_dates.date - interval {interval_separator}6 days{interval_separator} and checkout_step = 'paid' and not exchange then order_id end) as n_paids_7,
      count(case when date(registered_at) >= base_dates.date - interval {interval_separator}13 days{interval_separator} and checkout_step = 'paid' and not exchange then order_id end) as n_paids_14,

      sum(case when date(registered_at) >= base_dates.date - interval {interval_separator}6 days{interval_separator} and checkout_step = 'paid' and not exchange then price end) as revenue_7,
      sum(case when date(registered_at) >= base_dates.date - interval {interval_separator}13 days{interval_separator} and checkout_step = 'paid' and not exchange then price end) as revenue_14,

      count(distinct case when date(registered_at) >= base_dates.date - interval {interval_separator}6 days{interval_separator} then customer_id end) as customers_7,
      count(distinct case when date(registered_at) >= base_dates.date - interval {interval_separator}13 days{interval_separator} then customer_id end) as customers_14,

      count(distinct case when date(registered_at) <= base_dates.date and date(registered_at) >= base_dates.date - interval {interval_separator}6 days{interval_separator} then date(registered_at) end) as distinct_dates_7,
      count(distinct case when date(registered_at) <= base_dates.date and date(registered_at) >= base_dates.date - interval {interval_separator}13 days{interval_separator} then date(registered_at) end) as distinct_dates_14,

      alternative, fee_experiment_id, alternative_id
      {aditional_grouping_str}
    from (
      {dates_set}
    ) as base_dates
    left join base_orders on (date(base_orders.created_at) <= base_dates.date and base_orders.registered_at is not null)
    group by
      date, alternative, fee_experiment_id, alternative_id
      {aditional_grouping_str}
  ) as d
  order by date
  """.format(interval_separator=interval_separator,sql_input=consolidated_orders_data_sql,final_date=final_date,aditional_grouping_str=aditional_grouping_str,dates_set=dates_set.format(final_date=final_date,initial_date=initial_date))

  return query

def get_consolidated_orders_data_sql(aditional_join='', aditional_fields=''):
  """
  Returns the base query for selecting orders processed by experiment.

Important!
- This is the entry point for selecting query for analysis.
- This query assumes that table data_science.base_ordens_experimentos is up to date

Args:
  aditional_join (string): string interpolated in query join clause
  aditional_fields (string): string interpolated in query select clause

Returns:
  query: query for selecting order data for consolidation of data_science.base_ordens_experimentos
  """
  return """
    select
      orders_base.*
      {aditional_fields}
    from
      data_science.base_ordens_experimentos orders_base
    {aditional_join}
    WHERE
      orders_base.origin = 'Quero Bolsa'  -- excluding OPA, app, etc
    """.format(aditional_join=aditional_join, aditional_fields=aditional_fields)

def get_base_consolidated_orders_sql():
  """
Returns the base query for selecting orders with experiment data without filters for consolidation purpose

Important!
  - This query is intended to be used just for CONSOLIDATION PURPOSE!
  - There must be a daily job saving the result of this query to the tables data_science.base_ordens_experimentos as data_science.base_ordens_experimentos_per_city

Args:
    None

Returns:
    query: query for selecting order data for consolidation of data_science.base_ordens_experimentos and data_science.base_ordens_experimentos_per_city
  """

  query = '''
    SELECT
    orders.id AS order_id
    ,line_items.offer_id
    ,from_utc_timestamp(orders.registered_at, 'America/Sao_Paulo') as registered_at
    ,fee_experiment_alternatives.fee_experiment_id
    ,fee_experiment_alternatives.id as alternative_id
    ,fee_experiment_alternatives_history.ratio as alternative_ratio
    ,fee_experiment_alternatives.name as alternative
    ,base_users.customer_id
    ,orders.checkout_step
    ,orders.price
    ,pre_enrollment_fees.value
    ,pre_enrollment_fees.original_value
    ,pre_enrollment_fees.discount_value
    ,from_utc_timestamp(orders.created_at, 'America/Sao_Paulo') as created_at
    ,orders.created_at as base_order_created_at
    -- ,round(100-100*offers.offered_price/university_offers.full_price
    --   /(100-coalesce(university_offers.commercial_discount,offers.commercial_discount))*100,2) real_discount
    ,offers.university_id
    ,offers.offered_price
    ,CASE WHEN orders.checkout_step='paid' AND coupon_exchanges.id IS NOT NULL THEN true ELSE false END exchange
    ,coupons.id AS coupon_id
    ,order_origins.origin
    ,campuses.city_id
    ,campuses.city
    ,from_utc_timestamp(orders.paid_at, 'America/Sao_Paulo') as paid_at
    ,date(from_utc_timestamp(orders.created_at, 'America/Sao_Paulo')) as created_day
  FROM
    querobolsa_production.orders
  LEFT JOIN
    querobolsa_production.coupons ON coupons.order_id=orders.id
  LEFT JOIN
    querobolsa_production.coupon_exchanges ON coupon_exchanges.to_coupon_id=coupons.id
  LEFT JOIN
    querobolsa_production.order_origins ON (order_origins.order_id = orders.id)
  INNER JOIN
    querobolsa_production.base_users ON (base_users.id = orders.base_user_id)
  INNER JOIN
    querobolsa_production.line_items ON (line_items.order_id = orders.id AND orders.registered_at IS NOT NULL)
  INNER JOIN
    querobolsa_production.offers ON (line_items.offer_id = offers.id)
  INNER JOIN
    querobolsa_production.university_offers on (university_offers.id=offers.university_offer_id)
  LEFT JOIN
    querobolsa_production.courses on courses.id = offers.course_id
  LEFT JOIN
    querobolsa_production.campuses on campuses.id = courses.campus_id
  INNER JOIN
    querobolsa_production.pre_enrollment_fees ON (pre_enrollment_fees.id = line_items.pre_enrollment_fee_id)
  LEFT JOIN
    data_science.orders_experiment_alternatives ON orders_experiment_alternatives.order_id = orders.id
  LEFT JOIN
    querobolsa_production.fee_experiment_alternatives ON orders_experiment_alternatives.fee_experiment_alternative_id = fee_experiment_alternatives.id
  LEFT JOIN
  data_science.fee_experiment_alternatives_history ON (
    fee_experiment_alternatives_history.id = fee_experiment_alternatives.id
    AND fee_experiment_alternatives_history.date = date(from_utc_timestamp(orders.created_at, 'America/Sao_Paulo'))
  )

  '''
  return query







# from pricing.models.abtests.data_loading import get_cumulative_agg_results_sql
# from pricing.models.abtests.data_loading import get_consolidated_orders_data_sql


def elasticity_for_experiment(experiment_id, initial_window_date = None, base_date = None, field = None):
  print("elasticity_for_experiment: " + str(experiment_id) + " initial_window_date: " + str(initial_window_date) + " base_date: " + str(base_date))
  # qual eh a data base que vai olhar no db?
  if base_date is None:
    base_date = (datetime.datetime.now() - datetime.timedelta(days=3)).date().strftime('%Y-%m-%d')

  if initial_window_date is None:
    initial_window_date = base_date

  # partir dessa data_science.experiments_aggregate_base nao permite que faca o acumulado para um periodo arbitrario
  # mas acumular num periodo arbitrario leva um tempo consideravel!

  # TODO - nao pode aceitar field = 'cum'!
  if field == 'cum':
    field = 'window_7'

  conversion_field = 'alternative_conversion'
  arpu_field = 'arpu'
  ticket_field = 'av_ticket'

  days_where = ''
  if field == 'window_7':
    conversion_field = 'registered_conversion_7'
    arpu_field = 'arpu_7'
    ticket_field = 'av_ticket_7'
    days_where = ' and distinct_dates_7 >= 7'

  if field == 'window_14':
    conversion_field = 'alternative_conversion_14'
    arpu_field = 'arpu_14'
    ticket_field = 'av_ticket_14'
    days_where = ' and distinct_dates_14 >= 14'


  df = get_experiment_agg_data(experiment_id,start_date=base_date, end_date=base_date, filters={'distinct_dates_7': True, 'alternative_ratio': 0})

  conversion = []
  arpu = []
  price = []

  for index, row in df.iterrows():
    conversion.append(float(row[conversion_field]))
    arpu.append(float(row[arpu_field]))
    price.append(float(row[ticket_field]))



  slope, intercept, r_value, p_value, std_err = scipy.stats.linregress(conversion, price)
  calculated_price = list(map(lambda x: slope * x + intercept, conversion))

  max_aov = -intercept / 2 / slope

  r_squared = r_value * r_value

  print('slope: ' + str(slope) + ' intercept: ' + str(intercept) + ' r_value: ' + str(r_value) + ' p_value: ' + str(p_value) + ' std_err: ' + str(std_err) + " r_squared: " + str(r_squared) + ' max_aov: ' + str(max_aov))

  label = 'R2: ' + str(round(r_squared, 2))
  if slope > 0:
    print('>>>>>>>>>>>> INCONSISTENCIA na regressao da conversao pelo preco <<<<<<<<<<<')
    label = 'Regressão inconsistente'
  else:
    if r_squared < 0.8:
      print('>>>>>>>>>>>> BAIXA CONFIABILIDADE na regressao da conversao pelo preco R2: ' + str(r_squared) + ' <<<<<<<<<<<')
      label = 'Regressão de baixa confiabilidade - ' + label

  print(max_aov)
  if max_aov < 0:
    max_aov = 0

  min_conv = min(conversion)
  max_conv = max(conversion)

  # TODO -  pq esse step?
  max_out_step = 0.01

  if max_aov > max_conv:
    max_aov = max_conv + max_out_step

  if max_aov < min_conv:
    max_aov = min_conv - max_out_step

  y_max_aov = (slope * max_aov * max_aov) + ( intercept * max_aov ) # + p[2]

  if max_conv < max_aov:
    max_conv = max_aov + 0.01

  if min_conv > max_aov:
    min_conv = max_aov - 0.01

  interval_min = (round(min_conv, 2) - 0.01) * 1000
  interval_max = (round(max_conv, 2) + 0.01) * 1000

  p_reta = np.polyfit(conversion,price,1)

  x_range = list(map(lambda x: x/1000, range(int(interval_min), int(interval_max), 1)))
  y_range = list(map(lambda x: (slope * x * x) + ( intercept * x ), x_range))

  y_range_reta = list(map(lambda x: ( slope * x ) + intercept, x_range))

  return x_range, y_range_reta, y_range, conversion, price, arpu, label
