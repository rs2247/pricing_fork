import time
import plotly.express as px

import matplotlib.pyplot as plt
from matplotlib import cm
from mpl_toolkits.mplot3d import Axes3D
import numpy as np
from matplotlib.ticker import LinearLocator, FormatStrFormatter

def base_validation_select():
  return """with offers as (
    select
      *,
      {full_price} * (1 - (discount_percentage / 100)) as offered_price,
      {position} as position,
      {campaign} as campaign,
      {billing_integration} as billing_integration,
      {course_id} as course_id
    from (
      select
         *,
         explode(sequence(1,99)) as discount_percentage
       from (
        select
          explode(sequence({range_start},{range_end},{range_step})) as commercial_discount,
          {university_id} as university_id
      ) as base_commecial_discount
    ) as base_discount
  ),
  university_offers as (
    select
      {full_price} as full_price,
      {max_payments} as max_payments,
      null as commercial_discount
  ),
  courses as (
    select 
      {digital_admission} as digital_admission,
      {campus_id} as campus_id
  ),
  campuses as (
    select {city_id} as city_id
  ),
  kinds as (
    select {kind_id} as parent_id
  ),
  levels as (
    select {level_id} as parent_id
  ),
  states as (
    select {region} as region
  ),
  pricing_factors as (
    select {target_offered} as target_offered
  ),
  pricing_offers_regressive_discounts as (
    select 
      {weighted_discount} as weighted_discount,
      {weighted_offered_price} as weighted_offered_price,
      {commercial_weighted_discount} as commercial_weighted_discount,
      {commercial_weighted_offered_price} as commercial_weighted_offered_price
  ),
  base_final AS(
   select
     *,
     {discount_formula} as discount_value
   from (
     select
       *,
       {subsidy_formula} as subsidy_value
     from (
       select
          round(university_offers.full_price
          *(100-coalesce(university_offers.commercial_discount,offers.commercial_discount))
          /100,2) AS offered_ies,
        offers.discount_percentage,
        offers.commercial_discount,

        offers.university_id as university_id,
        offers.offered_price as offered_price,
        offers.campaign as campaign,
        offers.billing_integration as billing_integration,
        university_offers.full_price as full_price,
        kinds.parent_id as kind_id,
        levels.parent_id as level_id,
        states.region as region,
        offers.course_id,
        pricing_factors.target_offered,
        courses.campus_id,
        {formula} AS original_value

        from offers, university_offers, courses, kinds, levels, campuses, states, pricing_factors, pricing_offers_regressive_discounts

      ) as base_subsidy
    ) as base_discount
  )
  SELECT
  original_value as pef,
  subsidy_value as pef_subsidy,
  discount_value as pef_discount,
  -- original_value - coalesce(discount_value, 0) as pef_with_discount,
  (original_value - discount_value)/offered_ies as pef_with_discount,
  original_value/offered_ies AS pef_offered,
  discount_percentage,
  commercial_discount,
  offered_price

  FROM
  base_final"""

def validate_formula_3d_surface(spark, formula, subsidy_formula, discount_formula, range_step = 1, z_limit = None, azimuth = 75, z_field = 'pef_offered', university_id=1, full_price=1000, campaign='null', billing_integration='false',kind_id=1,level_id=1,city_id=10901,position="null",max_payments=32,region='sudeste',course_id=1,digital_admission=False,weighted_discount=50,weighted_offered_price=500,commercial_weighted_discount=40,commercial_weighted_offered_price=600): #10901 is São Paulo
  start_time = time.time()

  fig = plt.figure()
  fig.clf()
  ax = fig.gca(projection='3d')

  X = []
  Y = []
  Z = []

  range_start = 0
  range_end = 100

  if campaign != 'null':
    campaign = "'{campaign}'".format(campaign=campaign)

  if region != 'null':
    region = "'{region}'".format(region=region)

  validate_query = base_validation_select().format(formula=formula,discount_formula=discount_formula,subsidy_formula=subsidy_formula,range_start=range_start,range_end=range_end,range_step=range_step,university_id=university_id, full_price=full_price,campaign=campaign,billing_integration=billing_integration,level_id=level_id,kind_id=kind_id,city_id=city_id,position=position,max_payments=max_payments,region=region,course_id=course_id,digital_admission=digital_admission,weighted_discount=weighted_discount,weighted_offered_price=weighted_offered_price,commercial_weighted_discount=commercial_weighted_discount,commercial_weighted_offered_price=commercial_weighted_offered_price)

  df = spark.sql(validate_query)

  df_list = df.rdd.collect()

  for commercial in list(range(range_start, range_end, range_step)):
    df_serie = list(filter(lambda x: x['commercial_discount'] == commercial, df_list))

    z_field_data = list(map(lambda x: 0 if x[z_field] == None else float(x[z_field]), df_serie))
    discount_percentage = list(map(lambda x: float(x['discount_percentage']), df_serie))
    commercial_discount = list(map(lambda x: float(x['commercial_discount']), df_serie))

    X.append(commercial_discount)
    Y.append(discount_percentage)
    Z.append(z_field_data)

  Xnp = np.array(X)
  Ynp = np.array(Y)
  Znp = np.array(Z)

  ax.set_xlabel('Desconto Comercial', fontsize=16)
  ax.set_ylabel('Desconto Anunciado', fontsize=16)
  ax.set_zlabel(z_field, fontsize=16)

  surf = ax.plot_surface(Xnp, Ynp, Znp, cmap=cm.coolwarm, linewidth=0, antialiased=False)

  ax.view_init(30, azimuth)

  # Customize the z axis.
  if z_limit != None:
    ax.set_zlim(0, z_limit)

  # Add a color bar which maps values to colors.
  fig.colorbar(surf, shrink=0.5, aspect=5)

  elapsed_time = time.time() - start_time
  print("elapsed_time: " + str(elapsed_time))

  return fig

def validate_formula(spark,formula,subsidy_formula='original_value',discount_formula='null',level_id=1,kind_id=1,max_payments=32, city_id = 10901,university_id=1,position="null",range_start=0,range_end=100,range_step=20,full_price=1000,campaign='null',billing_integration='false',region='sudeste',y_field = 'pef_offered',course_id=1,digital_admission=False,target_offered='NULL',campus_id=1,weighted_discount=50,weighted_offered_price=500,commercial_weighted_discount=40,commercial_weighted_offered_price=600): #10901 is São Paulo
  '''
    Inputs:
      - y_field: value to be plotted. One of:
        original_value as pef,
        subsidy_value as pef_subsidy,
        discount_value as pef_discount,
        -- original_value - coalesce(discount_value, 0) as pef_with_discount,
        (original_value - discount_value)/offered_ies as pef_with_discount,
        original_value/offered_ies AS pef_offered,
        discount_percentage,
        commercial_discount,
        offered_price


  '''
  if campaign != 'null':
    campaign = "'{campaign}'".format(campaign=campaign)

  if region != 'null':
    region = "'{region}'".format(region=region)

  validate_query = base_validation_select().format(formula=formula,discount_formula=discount_formula,subsidy_formula=subsidy_formula,range_start=range_start,range_end=range_end,range_step=range_step,university_id=university_id, full_price=full_price,campaign=campaign,billing_integration=billing_integration,level_id=level_id,kind_id=kind_id,city_id=city_id,position=position,max_payments=max_payments,region=region,course_id=course_id,digital_admission=digital_admission,target_offered=target_offered,campus_id=campus_id,weighted_discount=weighted_discount,weighted_offered_price=weighted_offered_price,commercial_weighted_discount=commercial_weighted_discount,commercial_weighted_offered_price=commercial_weighted_offered_price)
  df = spark.sql(validate_query)

  return px.line(df.toPandas(),x='discount_percentage',y=y_field,color='commercial_discount')
