
# DATA_FOLDER = '/Users/renatosanabria/Desktop/code/inventory_analysis/'
# df_sales_pivot = pd.read_hdf(DATA_FOLDER + '/df_sales_pivot.h5', key='df')

import pandas as pd
import numpy as np
import os
from pricing.utils import load_from_db_cache
from pricing.utils import logger

def load_daily_pef (spark,from_cache=False):
  logger.debug("Loading consolidated_pef_per_day ... ")
  query = """SELECT date,
                    city,
                    case when name = 'Presencial' then 'Presencial' else 'EaD + Semi' end kind,
                    avg(value) as value,
                    avg(original_value) as original_value,
                    avg(pef_desconto) as pef_desconto


   FROM data_science.consolidated_pef_per_day
   GROUP BY 1,2,3""" #WHERE name ='Presencial'"

  df = load_from_db_cache(spark,query,'consolidated_pef_per_day', from_cache)
  df = df.sort_values(['city','date'])

  return df

# def load_share_rpu (spark,from_cache=False):
#   print("Loading consolidated_shares_rpu ... ")

#   # df = load_from_db_cache(spark,'data_science','consolidated_shares_rpu', from_cache)
#   query = "SELECT * FROM data_science.consolidated_shares_rpu"
#   df = load_from_db_cache(spark,query,'consolidated_shares_rpu', from_cache)
#   df['delta_rpu_20_19'] = (df['receita_20']/df['usuarios_20'])/(df['receita_19']/df['ordens_19']) - 1
#   df['delta_pagos_20_19'] = df['pagos_20']/df['pagos_19'] - 1
#   df['share_19'] = df['qb_19']/df['inep_19']

#   return df

def load_daily_share_rpu(spark,from_cache=False):
  logger.debug("Loading daily_share_rpu ... ")

  query = "SELECT * FROM data_science.consolidated_rpu_per_day WHERE level='Graduação'"
  df = load_from_db_cache(spark,query,'consolidated_rpu_per_day', from_cache)

  #Pricing log

  pricing_log = {
  "RECIFE":[-15,14],
  "CURITIBA":[-10,14],
  "RIO DE JANEIRO":[-10,14],
  "JOÃO PESSOA":[-20,14],
  "BELEM":[-5,14],
  "SALVADOR":[-10,14],
  "FORTALEZA":[-10,14],
  "MANAUS":[10,14],
  "PORTO VELHO":[20,14],
  "CARUARU":[-50,14],
  "FEIRA DE SANTANA":[-30,14],
  "TERESINA":[-10,14],
  "CAMPINAS":[-15,14],
  "OSASCO":[-20,14],
  "SÃO JOSÉ DOS CAMPOS":[-10,14],
  "SOROCABA":[-30,14],
  "SANTO ANDRE":[10,14],
  "JUAZEIRO DO NORTE":[-10,37],
  "CUIABA":[-20,42],
  "BOA VISTA":[-20,42]
  }

  if from_cache:
    #Simulating pricing logs
    df['pricing_change'] = np.nan
    df['pricing_log'] = np.nan

    for key in pricing_log:

      df.loc[(df['dia_ano']==pricing_log[key][1])&(df['city']==key),'pricing_change'] = pricing_log[key][0]
      df.loc[(df['dia_ano']==pricing_log[key][1])&(df['city']==key),'pricing_log'] = "Alteração de {}% para elevar ARPU e captação".format(pricing_log[key][0])


  df['delta_rpu_20_19'] = (df['receita_acumulada']/df['ordens_acumuladas'])/(df['receita_acumulada_18']/df['ordens_acumuladas_18']) - 1 
  df['delta_pagos_20_19'] = df['pagos_acumulados']/df['pagos_acumulados_18'] - 1

  #Excluding cities where the deltas are not defined
  df = df[(df['delta_rpu_20_19']!=np.inf)&(df['delta_pagos_20_19']!=np.inf)]
  df = df.sort_values(['city','dia_ano'])

  return df


def city2ies(spark,from_cache=False):
  logger.debug("Loading city2ies ... ")

  query  = """
  WITH city_sales as (
    select distinct
      campuses.city_id,
      sales.campus_city as city,
      sales.campus_state as state,
      kinds.parent_id as kind_id,
      round(sum(sales.total_revenue)) as sales
    from
      data_warehouse.sales
      left join querobolsa_production.coupons on coupons.id = sales.coupon_id
      left join querobolsa_production.offers on offers.id = coupons.offer_id
      left join querobolsa_production.university_offers  on university_offers.id = offers.university_offer_id
      left join (select * from querobolsa_production.kinds where parent_id is not null) kinds on kinds.name = sales.course_kind
      left join (select * from querobolsa_production.levels where parent_id is not null) levels on levels.name = sales.course_level
      left join querobolsa_production.campuses on campuses.id = sales.campus_id
    where
      university_offers.enrollment_semester in ('2019.1','2019.2','2020.1')
      and sales.campus_city is not null
      and sales.campus_city <> ''
    group by 1,2,3,4
    order by 5 desc
  ),
  top_in_state as (
  select
    city_sales.*
  from
    (select kind_id, state, max(sales) as max_sales from city_sales group by 1,2 ) as ref
    join city_sales on city_sales.state = ref.state and city_sales.sales = ref.max_sales and ref.kind_id = city_sales.kind_id
  ),
  top_cities as (
    select
      *
    from
      city_sales
    order by sales desc
    limit 40
  ),
  filter_cities as (
  select * from top_cities union select * from top_in_state order by sales desc
  )

  select distinct
    campuses.city_id,
    sales.campus_city as city,
    sales.campus_state as state,
    case when kinds.id in (3,8) then 'EaD + Semi' else 'Presencial' end kind, 
    levels.name AS level,
    offers.university_id,
    universities.name,
    sum(sales.total_revenue) as revenue
  from
    data_warehouse.sales
    left join querobolsa_production.coupons on coupons.id = sales.coupon_id
    left join querobolsa_production.offers on offers.id = coupons.offer_id
    left join querobolsa_production.universities on universities.id = offers.university_id
    left join querobolsa_production.university_offers  on university_offers.id = offers.university_offer_id
    left join querobolsa_production.courses on sales.course_id = courses.id
    left join querobolsa_production.kinds k ON k.name = sales.course_kind AND k.parent_id IS NOT NULL
    left join querobolsa_production.kinds ON k.parent_id = kinds.id
    left join querobolsa_production.levels l ON l.name = sales.course_level AND l.parent_id IS NOT NULL
    left join querobolsa_production.levels ON l.parent_id = levels.id
    join filter_cities on filter_cities.city = sales.campus_city and filter_cities.state = sales.campus_state and filter_cities.kind_id = kinds.id
    left join querobolsa_production.campuses on campuses.id = sales.campus_id
  where
    sales.payment_date BETWEEN '2019-10-01' AND '2020-04-01'
    and offers.university_id is not null
    and levels.id = 1
    and kinds.id IN (1,3,8)
  group by 1,2,3,4,5,6,7
  order by 8 desc
  """
  df = load_from_db_cache(spark,query,'city2ies', from_cache)
  df['university_id'] = df['university_id'].astype(int)

  # NUMBER OF IES PER RELEVANT CITIES
  df['revenue_city'] = df.groupby(['city','state','kind'])['revenue'].transform('sum')
  df['relevance_city'] = df['revenue']/df['revenue_city']
  df['cumrelevance_city'] = df.groupby(['city','state','kind'])['relevance_city'].transform(lambda x: x.cumsum())
  # df[df['city']=='Brasília'].sort_values('revenue',ascending=False)
  df = df.sort_values(['revenue_city','revenue'],ascending=False)

  #Getting only IES that are in 80% of relevance
  df = df[df['cumrelevance_city']<.8]

  return df

def log_recalculate(spark,from_cache=False):
  logger.debug("Loading log_recalculate ... ")

  query  = """
      with mudancas as
      (
        select *
        from parcerias.log_changes_pricing
      ),

      base as
      (
        select datas.dia,
          modalidade.kind,
          campus.university_id,
          campus.city,
          campus.state,
          campus.ies

        from (select distinct university_id, city, state, ies from parcerias.log_changes_pricing) as campus
        cross join (select explode(sequence(date('2019-12-01'), date(now()))) dia) as datas
        cross join (select distinct case when parent_id = 1 then 'Presencial' else 'EaD + Semi' end kind from querobolsa_production.kinds where kinds.parent_id is not null) as modalidade
      )

      select base.*,
        coalesce(mudancas.qtde,0.0) as qtde

      from base
      left join mudancas on base.dia = mudancas.dia
                        and base.kind = mudancas.kind
                        and base.university_id = mudancas.university_id
                        and base.city = mudancas.city
                        and base.state = mudancas.state

      order by base.dia, base.kind, mudancas.qtde




  
  """
  df = load_from_db_cache(spark,query,'log_recalculate', from_cache)
  #df['university_id'] = df['university_id'].astype(int)

  # NUMBER OF IES PER RELEVANT CITIES
  #df['revenue_city'] = df.groupby(['dia','city','state','kind'])['qtde'].transform('sum')
  


  return df

def daily_order_pef(spark,from_cache=False):
  logger.debug("Loading daily_order_pef ... ")

  query = """
   WITH city_sales AS (
    SELECT DISTINCT
      sales.campus_city AS city,
      sales.campus_state AS state,
      round(sum(sales.total_revenue)) AS sales
    FROM
      data_warehouse.sales
      LEFT JOIN querobolsa_production.coupons ON coupons.id = sales.coupon_id
      LEFT JOIN querobolsa_production.offers ON offers.id = coupons.offer_id
      LEFT JOIN querobolsa_production.university_offers ON university_offers.id = offers.university_offer_id
      LEFT JOIN (SELECT * FROM  querobolsa_production.kinds WHERE parent_id IS NOT NULL) kinds ON kinds.name = sales.course_kind
      LEFT JOIN (SELECT * FROM querobolsa_production.levels WHERE parent_id IS NOT NULL) levels ON levels.name = sales.course_level
    WHERE
      university_offers.enrollment_semester IN ('2019.1','2019.2','2020.1')
      AND sales.campus_city IS NOT NULL
      AND sales.campus_city <> ''
    GROUP BY 1,2
    ORDER BY 3 DESC
  ),
  top_in_state AS (
  SELECT
    city_sales.*
  FROM
    (SELECT state, max(sales) AS max_sales FROM city_sales GROUP BY 1 ) AS ref
    JOIN city_sales ON city_sales.state = ref.state AND city_sales.sales = ref.max_sales
  ),
  top_cities AS (
    SELECT
      *
    FROM
      city_sales
    ORDER BY sales DESC
    LIMIT 40
  ),
  cidades_alvo AS(
    SELECT
    *
    FROM
      top_cities
    UNION SELECT * FROM top_in_state ORDER BY sales DESC
  )

  SELECT
    DATE(orders.registered_at) AS date,
    campuses.city,
    case when k.parent_id = 1 then 'Presencial' else 'EaD + Semi' end kind,
    AVG(orders.price) AS value
  FROM
    querobolsa_production.orders
  JOIN
    querobolsa_production.line_items ON orders.id = line_items.order_id
  JOIN
    querobolsa_production.pre_enrollment_fees ON pre_enrollment_fees.id = line_items.pre_enrollment_fee_id
  JOIN
    querobolsa_production.offers ON line_items.offer_id = offers.id
  JOIN
    querobolsa_production.courses ON offers.course_id = courses.id
  JOIN
    querobolsa_production.campuses ON campuses.id = courses.campus_id
  JOIN
    querobolsa_production.levels l ON courses.level = l.name AND l.parent_id IS NOT NULL
  JOIN
    querobolsa_production.levels ON l.parent_id = levels.id
  JOIN
    querobolsa_production.kinds k ON courses.kind = k.name AND k.parent_id IS NOT NULL
  JOIN
    querobolsa_production.kinds ON k.parent_id = kinds.id
  JOIN
    cidades_alvo ON campuses.city = cidades_alvo.city 

  WHERE
    orders.checkout_step NOT IN ('initiated')
  AND l.parent_id = 1
  AND orders.registered_at BETWEEN '2019-12-12' AND '2020-04-01'
  GROUP BY 1,2,3
  ORDER BY 1
  """

  df = load_from_db_cache(spark,query,'daily_order_pef', from_cache)

  return df

#################################       ##########################################
#################################  RPU  ##########################################
################################# GOALS ##########################################
#################################       ##########################################


def rpu_goals_only_qb_time_series(spark,from_cache=False):
  print("Loading rpu_goals_only_qb_time_series ... ")

  query="""
   WITH 
  categorizacao_ies AS(
  SELECT DISTINCT
    university_id,
    levels.name AS level,
    kinds.name AS kind,
    'QAP' AS categoria

  FROM
  querobolsa_production.university_billing_configurations
  JOIN
  querobolsa_production.kinds ON university_billing_configurations.kind_id = kinds.id
  JOIN
  querobolsa_production.levels ON university_billing_configurations.level_id = levels.id

  WHERE
  (university_billing_configurations.enabled_until > '2019-04-01'
  AND
  university_billing_configurations.created_at <= '2019-10-01')
  OR
  (university_billing_configurations.enabled_until > '2020-04-01'
  AND
  university_billing_configurations.created_at <= '2020-10-01')
  OR
  university_billing_configurations.enabled_until IS NULL
  ),
  
  base_ordens AS(
    SELECT
      base_ordens_experimentos.university_id,
      offer_id,
      base_ordens_experimentos.order_id, 
      DATE(registered_at) AS registered_at, 
      customer_id,
      kinds.name AS kind, 
      levels.name AS level, 
      checkout_step,
      CASE WHEN checkout_step = 'paid' THEN base_ordens_experimentos.order_id ELSE null END AS paid_id,
      CASE WHEN checkout_step = 'paid' THEN price ELSE 0 END AS price,
      CASE WHEN checkout_step = 'paid' THEN original_value ELSE 0 END AS original_value,
      CASE WHEN checkout_step = 'paid' THEN value ELSE 0 END AS value,
      coalesce(sales.ltv_qp,0) AS ltv_qp,
      CASE WHEN (checkout_step = 'paid' AND offers.billing_integration AND base_ordens_experimentos.registered_at < '2019-10-01') THEN 
        CASE
          WHEN ((l.parent_id = 1) AND (k.parent_id = 1)) THEN 27.87
          WHEN ((l.parent_id = 1) AND (k.parent_id <> 1)) THEN 23.32
          WHEN (l.parent_id = 7) THEN (8.0)
        ELSE (0.0)
        END * offers.offered_price * 0.08
      ELSE 0
      END AS ltv_passado, 
      categorizacao_ies.categoria
      
    FROM 
      data_science.base_ordens_experimentos
    INNER JOIN querobolsa_production.offers ON base_ordens_experimentos.offer_id = offers.id
    INNER JOIN querobolsa_production.courses ON offers.course_id = courses.id
    INNER JOIN querobolsa_production.levels l ON courses.level = l.name AND l.parent_id IS NOT NULL 
    INNER JOIN querobolsa_production.levels ON l.parent_id = levels.id 
    INNER JOIN querobolsa_production.kinds k ON courses.kind = k.name AND k.parent_id IS NOT NULL
    INNER JOIN querobolsa_production.kinds ON k.parent_id = kinds.id
    LEFT JOIN data_warehouse.sales ON sales.order_id = base_ordens_experimentos.order_id 
    LEFT JOIN categorizacao_ies ON categorizacao_ies.university_id = base_ordens_experimentos.university_id AND categorizacao_ies.kind = kinds.name AND categorizacao_ies.level = levels.name
      
    WHERE 
      (registered_at BETWEEN '2019-04-01' AND '2019-10-01' OR registered_at BETWEEN '2020-04-01' AND '2020-10-01')
      AND l.parent_id IN (1, 7)
      AND categorizacao_ies.categoria IS NULL
  ), 

  base_rpu AS(
    SELECT 
      registered_at,
      CASE WHEN registered_at BETWEEN '2020-04-01' AND '2020-10-01'
      THEN '2020.1' 
      ELSE
        CASE WHEN registered_at BETWEEN '2019-04-01' AND '2019-10-01'
        THEN '2019.1' 
        ELSE '' 
        END
      END  AS semestre,      
      kind,
      level, 
      count(paid_id) AS pagos,
      SUM(price) + SUM(ltv_qp) + SUM(ltv_passado) AS price, 
      SUM(original_value) AS original_value,
      COUNT(DISTINCT customer_id) AS usuarios 
    FROM 
      base_ordens

    WHERE 
      registered_at BETWEEN '2019-04-01' AND '2019-10-01'
      OR registered_at BETWEEN '2020-04-01'  AND current_date() - interval 3 day 
    GROUP BY 
      1,2,3,4
    ORDER BY 
      3,2,1
  ), 
  base_inicial AS(
    SELECT
      dayofyear(registered_at) AS dia_ano,
      kind, 
      level, 
      SUM(
        CASE WHEN semestre = 2019.1 THEN price ELSE 0 END
      ) AS receita_19, 
      SUM(
        CASE WHEN semestre = 2019.1 THEN usuarios ELSE 0 END
      ) AS usuarios_19, 
      SUM(
        CASE WHEN semestre = 2019.1 THEN pagos ELSE 0 END
      ) AS pagos_19,
      SUM(
        CASE WHEN semestre = 2020.1 THEN price ELSE 0 END
      ) AS receita_20, 
      SUM(
        CASE WHEN semestre = 2020.1 THEN usuarios ELSE 0 END
      ) AS usuarios_20, 
      SUM(
        CASE WHEN semestre = 2020.1 THEN pagos ELSE 0 END
      ) AS pagos_20
      
    FROM 
      base_rpu 
    GROUP BY 
      1,2,3
  ),
  base_datas AS(
  SELECT
  explode(sequence(to_date('2020-04-01'),to_date('2020-10-01'),interval 1 day)) AS date
  ),
  base_datas_2 AS(
  SELECT
  date,
  dayofyear(date) AS dia_ano
  FROM
  base_datas
  ),
  base_evolutivo AS(
  SELECT
    base_datas_2.date,
    level, 
    kind,
    concat(level," ",kind) AS level_kind,
    SUM(receita_19) AS receita_19, 
    SUM(receita_20) AS receita_20, 
    SUM(pagos_19) AS pagos_19,
    SUM(pagos_20) AS pagos_20,
    SUM(usuarios_19) AS usuarios_19,
    SUM(usuarios_20) AS usuarios_20

  FROM 
    base_inicial
  JOIN
    base_datas_2 ON base_datas_2.dia_ano = base_inicial.dia_ano

  GROUP BY
    1,2,3,4
  ),
  base_final AS(
  SELECT
  date,
  level,
  kind,
  level_kind,
  SUM(receita_19) OVER (
          PARTITION BY
          kind,
          level
          ORDER BY
          date
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS receita_ac_19,
  SUM(receita_20) OVER (
          PARTITION BY
          kind,
          level
          ORDER BY
          date
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS receita_ac_20,
  SUM(usuarios_19) OVER (
          PARTITION BY
          kind,
          level
          ORDER BY
          date
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS usuarios_ac_19,
  SUM(usuarios_20) OVER (
          PARTITION BY
          kind,
          level
          ORDER BY
          date
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS usuarios_ac_20,
  SUM(receita_19) OVER (
          PARTITION BY
          kind,
          level
          ORDER BY
          date
          ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS receita_mm_19,
  SUM(receita_20) OVER (
          PARTITION BY
          kind,
          level
          ORDER BY
          date
          ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS receita_mm_20,
  SUM(usuarios_19) OVER (
          PARTITION BY
          kind,
          level
          ORDER BY
          date
          ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS usuarios_mm_19,
  SUM(usuarios_20) OVER (
          PARTITION BY
          kind,
          level
          ORDER BY
          date
          ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS usuarios_mm_20,
  SUM(pagos_19) OVER (
          PARTITION BY
          kind,
          level
          ORDER BY
          date
          ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS pagos_mm_19,
  SUM(pagos_20) OVER (
          PARTITION BY
          kind,
          level
          ORDER BY
          date
          ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS pagos_mm_20,
  SUM(pagos_19) OVER (
          PARTITION BY
          kind,
          level
          ORDER BY
          date
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS pagos_ac_19,
  SUM(pagos_20) OVER (
          PARTITION BY
          kind,
          level
          ORDER BY
          date
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS pagos_ac_20
         
  FROM
  base_evolutivo

  ORDER BY
  1 desc
  )

  SELECT
  date,
  level,
  kind,
  level_kind,
  receita_ac_19,
  CASE WHEN date < current_date() - interval 3 day THEN receita_ac_20 ELSE null END AS receita_ac_20,
  receita_mm_19,
  CASE WHEN date < current_date() - interval 3 day THEN receita_mm_20 ELSE null END AS receita_mm_20,
  usuarios_ac_19,
  CASE WHEN date < current_date() - interval 3 day THEN usuarios_ac_20 ELSE null END AS usuarios_ac_20,
  usuarios_mm_19,
  CASE WHEN date < current_date() - interval 3 day THEN usuarios_mm_20 ELSE null END AS usuarios_mm_20,
  pagos_mm_19,
  CASE WHEN date < current_date() - interval 3 day THEN pagos_mm_20 ELSE null END AS pagos_mm_20,
  pagos_ac_19,
  CASE WHEN date < current_date() - interval 3 day THEN pagos_ac_20 ELSE null END AS pagos_ac_20,
  1,
  CASE WHEN date < current_date() - interval 3 day THEN receita_ac_20/usuarios_ac_20 ELSE null END AS rpu_ac_20,
  receita_ac_19/usuarios_ac_19 AS rpu_ac_19,
  
  receita_ac_19/usuarios_ac_19 * 
  (CASE WHEN level = 'Graduação' AND kind = 'Presencial' THEN 0.95
         WHEN level = 'Graduação' AND kind <> 'Presencial' THEN 1.05
           WHEN level = 'Pós-graduação' AND kind = 'Presencial' THEN 1.20
             WHEN level = 'Pós-graduação' AND kind <> 'Presencial' THEN 1.10
   ELSE 1.1 END) AS rpu_ac_goal,
   
  CASE WHEN date < current_date() - interval 3 day THEN receita_mm_20/usuarios_mm_20 ELSE null END AS rpu_mm_20,
  receita_mm_19/usuarios_mm_19 AS rpu_mm_19,
  
  receita_mm_19/usuarios_mm_19 * 
  (CASE WHEN level = 'Graduação' AND kind = 'Presencial' THEN 0.95
         WHEN level = 'Graduação' AND kind <> 'Presencial' THEN 1.05 
           WHEN level = 'Pós-graduação' AND kind = 'Presencial' THEN 1.20
             WHEN level = 'Pós-graduação' AND kind <> 'Presencial' THEN 1.10
   ELSE 1.1 END) AS rpu_mm_goal

  FROM
  base_final
"""

  df = load_from_db_cache(spark,query,'rpu_goals_only_qb_time_series', from_cache)

  return df

def ies_categorization(spark,from_cache=False):
  print("Loading ies_categorization ... ")

  query="""
WITH 
categorizacao_ies AS(
SELECT DISTINCT
university_id,
levels.name AS level,
kinds.name AS kind,
'QAP' AS categoria

FROM
querobolsa_production.university_billing_configurations
JOIN
querobolsa_production.kinds ON university_billing_configurations.kind_id = kinds.id
JOIN
querobolsa_production.levels ON university_billing_configurations.level_id = levels.id

WHERE
(university_billing_configurations.enabled_until > '2019-04-01'
AND
university_billing_configurations.created_at <= '2019-10-01')
OR
(university_billing_configurations.enabled_until > '2020-04-01'
AND
university_billing_configurations.created_at <= '2020-10-01')
OR
university_billing_configurations.enabled_until IS NULL
),
base_ordens AS(
  SELECT
    base_ordens_experimentos.university_id,
    offer_id,
    order_id, 
    DATE(registered_at) AS registered_at, 
    customer_id,
    kinds.name AS kind, 
    levels.name AS level, 
    checkout_step,
    CASE WHEN checkout_step = 'paid' THEN order_id ELSE null END AS paid_id,
    CASE WHEN checkout_step = 'paid' THEN price ELSE 0 END AS price,
    CASE WHEN checkout_step = 'paid' THEN original_value ELSE 0 END AS original_value,
    CASE WHEN checkout_step = 'paid' THEN value ELSE 0 END AS value,
    categorizacao_ies.categoria
    
  FROM 
    data_science.base_ordens_experimentos
  INNER JOIN querobolsa_production.offers ON base_ordens_experimentos.offer_id = offers.id
  INNER JOIN querobolsa_production.courses ON offers.course_id = courses.id
  INNER JOIN querobolsa_production.levels l ON courses.level = l.name AND l.parent_id IS NOT NULL 
  INNER JOIN querobolsa_production.levels ON l.parent_id = levels.id 
  INNER JOIN querobolsa_production.kinds k ON courses.kind = k.name AND k.parent_id IS NOT NULL
  INNER JOIN querobolsa_production.kinds ON k.parent_id = kinds.id
  LEFT JOIN categorizacao_ies ON categorizacao_ies.university_id = base_ordens_experimentos.university_id AND categorizacao_ies.kind = kinds.name AND categorizacao_ies.level = levels.name
    
  WHERE 
    (registered_at BETWEEN '2019-04-01' AND '2019-10-01' OR registered_at BETWEEN '2020-04-01' AND '2020-10-01')
    AND l.parent_id IN (1, 7)
)

SELECT
universities.name AS ies,
level,
kind,
CASE WHEN categoria IS NULL THEN '' ELSE 'QAP' END AS categoria,
SUM(price) AS receita_total

FROM
base_ordens
JOIN
querobolsa_production.universities ON base_ordens.university_id = universities.id

GROUP BY
1,2,3,4

ORDER BY
5 desc

"""
  df = load_from_db_cache(spark,query,'ies_categorization', from_cache)

  return df

def campaigns(spark,from_cache=False):
  print("Loading campaigns ... ")

  query="""select * from parcerias.campaigns WHERE offer_campaign_flag IN ("black_friday","ead_week","high_campaign")"""

  df = load_from_db_cache(spark,query,'campaigns', from_cache)

  return df



def updates_per_day(spark,from_cache=False):
  logger.debug("Loading pricing updates ... ")


  query = """
  select * from
  (
    select dia,
      origem,
      ies,
      city,
      state,
      kind,
      qtde,
      row_number() over (partition by ies, city, kind order by qtde desc) as ranking

    from parcerias.log_changes_pricing
  ) dd
  """
  df = load_from_db_cache(spark,query,'pricing_updates', from_cache)
  return df

def rpu_goals_only_qb_time_series_summer(spark,from_cache=False):
  print("Loading rpu_goals_only_qb_time_series ... ")

  query="""
  WITH 
  categorizacao_ies AS(
  SELECT DISTINCT
    university_id,
    levels.name AS level,
    kinds.name AS kind,
    'QAP' AS categoria

  FROM
  querobolsa_production.university_billing_configurations
  JOIN
  querobolsa_production.kinds ON university_billing_configurations.kind_id = kinds.id
  JOIN
  querobolsa_production.levels ON university_billing_configurations.level_id = levels.id

  WHERE
  (university_billing_configurations.enabled_until > '2018-10-01'
  AND
  university_billing_configurations.created_at <= '2019-04-01')
  OR
  (university_billing_configurations.enabled_until > '2019-10-01'
  AND
  university_billing_configurations.created_at <= '2020-04-01')
  ),
  
  base_ordens AS(
    SELECT
      base_ordens_experimentos.university_id,
      offer_id,
      base_ordens_experimentos.order_id, 
      DATE(registered_at) AS registered_at, 
      customer_id,
      kinds.name AS kind, 
      levels.name AS level, 
      checkout_step,
      CASE WHEN checkout_step = 'paid' THEN base_ordens_experimentos.order_id ELSE null END AS paid_id,
      CASE WHEN checkout_step = 'paid' THEN price ELSE 0 END AS price,
      CASE WHEN checkout_step = 'paid' THEN original_value ELSE 0 END AS original_value,
      CASE WHEN checkout_step = 'paid' THEN value ELSE 0 END AS value,
      coalesce(sales.ltv_qp,0) AS ltv_qp,
      CASE WHEN (checkout_step = 'paid' AND offers.billing_integration AND base_ordens_experimentos.registered_at < '2019-10-01') THEN 
        CASE
          WHEN ((l.parent_id = 1) AND (k.parent_id = 1)) THEN 27.87
          WHEN ((l.parent_id = 1) AND (k.parent_id <> 1)) THEN 23.32
          WHEN (l.parent_id = 7) THEN (8.0)
        ELSE (0.0)
        END * offers.offered_price * 0.08
      ELSE 0
      END AS ltv_passado, 
      categorizacao_ies.categoria
      
    FROM 
      data_science.base_ordens_experimentos
    INNER JOIN querobolsa_production.offers ON base_ordens_experimentos.offer_id = offers.id
    INNER JOIN querobolsa_production.courses ON offers.course_id = courses.id
    INNER JOIN querobolsa_production.levels l ON courses.level = l.name AND l.parent_id IS NOT NULL 
    INNER JOIN querobolsa_production.levels ON l.parent_id = levels.id 
    INNER JOIN querobolsa_production.kinds k ON courses.kind = k.name AND k.parent_id IS NOT NULL
    INNER JOIN querobolsa_production.kinds ON k.parent_id = kinds.id
    LEFT JOIN data_warehouse.sales ON sales.order_id = base_ordens_experimentos.order_id 
    LEFT JOIN categorizacao_ies ON categorizacao_ies.university_id = base_ordens_experimentos.university_id AND categorizacao_ies.kind = kinds.name AND categorizacao_ies.level = levels.name
      
    WHERE 
      (registered_at BETWEEN '2018-10-01' AND '2019-04-01' OR registered_at BETWEEN '2019-10-01' AND '2020-04-01')
      AND l.parent_id IN (1, 7)
      AND categorizacao_ies.categoria IS NULL
  ), 

  base_rpu AS(
    SELECT 
      registered_at,
      CASE WHEN registered_at BETWEEN '2019-10-01' AND '2020-04-01'
      THEN '2020.1' 
      ELSE
        CASE WHEN registered_at BETWEEN '2018-10-01' AND '2019-04-01'
        THEN '2019.1' 
        ELSE '' 
        END
      END  AS semestre,      
      kind,
      level, 
      count(paid_id) AS pagos,
      SUM(price) + SUM(ltv_qp) + SUM(ltv_passado) AS price, 
      SUM(original_value) AS original_value,
      COUNT(DISTINCT customer_id) AS usuarios 
    FROM 
      base_ordens

    WHERE 
      registered_at BETWEEN '2018-10-01' AND '2019-04-01'
      OR registered_at BETWEEN '2019-10-01'  AND current_date() - interval 3 day 
    GROUP BY 
      1,2,3,4
    ORDER BY 
      3,2,1
  ), 
  base_inicial AS(
    SELECT
      dayofyear(registered_at) AS dia_ano,
      kind, 
      level, 
      SUM(
        CASE WHEN semestre = 2019.1 THEN price ELSE 0 END
      ) AS receita_19, 
      SUM(
        CASE WHEN semestre = 2019.1 THEN usuarios ELSE 0 END
      ) AS usuarios_19, 
      SUM(
        CASE WHEN semestre = 2019.1 THEN pagos ELSE 0 END
      ) AS pagos_19,
      SUM(
        CASE WHEN semestre = 2020.1 THEN price ELSE 0 END
      ) AS receita_20, 
      SUM(
        CASE WHEN semestre = 2020.1 THEN usuarios ELSE 0 END
      ) AS usuarios_20, 
      SUM(
        CASE WHEN semestre = 2020.1 THEN pagos ELSE 0 END
      ) AS pagos_20
      
    FROM 
      base_rpu 
    GROUP BY 
      1,2,3
  ),
  base_datas AS(
  SELECT
  explode(sequence(to_date('2019-10-01'),to_date('2020-04-01'),interval 1 day)) AS date
  ),
  base_datas_2 AS(
  SELECT
  date,
  dayofyear(date) AS dia_ano
  FROM
  base_datas
  ),
  base_evolutivo AS(
  SELECT
    base_datas_2.date,
    level, 
    kind,
    concat(level," ",kind) AS level_kind,
    SUM(receita_19) AS receita_19, 
    SUM(receita_20) AS receita_20, 
    SUM(pagos_19) AS pagos_19,
    SUM(pagos_20) AS pagos_20,
    SUM(usuarios_19) AS usuarios_19,
    SUM(usuarios_20) AS usuarios_20

  FROM 
    base_inicial
  JOIN
    base_datas_2 ON base_datas_2.dia_ano = base_inicial.dia_ano

  GROUP BY
    1,2,3,4
  ),
  base_final AS(
  SELECT
  date,
  level,
  kind,
  level_kind,
  SUM(receita_19) OVER (
          PARTITION BY
          kind,
          level
          ORDER BY
          date
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS receita_ac_19,
  SUM(receita_20) OVER (
          PARTITION BY
          kind,
          level
          ORDER BY
          date
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS receita_ac_20,
  SUM(usuarios_19) OVER (
          PARTITION BY
          kind,
          level
          ORDER BY
          date
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS usuarios_ac_19,
  SUM(usuarios_20) OVER (
          PARTITION BY
          kind,
          level
          ORDER BY
          date
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS usuarios_ac_20,
  SUM(receita_19) OVER (
          PARTITION BY
          kind,
          level
          ORDER BY
          date
          ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS receita_mm_19,
  SUM(receita_20) OVER (
          PARTITION BY
          kind,
          level
          ORDER BY
          date
          ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS receita_mm_20,
  SUM(usuarios_19) OVER (
          PARTITION BY
          kind,
          level
          ORDER BY
          date
          ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS usuarios_mm_19,
  SUM(usuarios_20) OVER (
          PARTITION BY
          kind,
          level
          ORDER BY
          date
          ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS usuarios_mm_20,
  SUM(pagos_19) OVER (
          PARTITION BY
          kind,
          level
          ORDER BY
          date
          ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS pagos_mm_19,
  SUM(pagos_20) OVER (
          PARTITION BY
          kind,
          level
          ORDER BY
          date
          ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS pagos_mm_20,
  SUM(pagos_19) OVER (
          PARTITION BY
          kind,
          level
          ORDER BY
          date
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS pagos_ac_19,
  SUM(pagos_20) OVER (
          PARTITION BY
          kind,
          level
          ORDER BY
          date
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS pagos_ac_20
         
  FROM
  base_evolutivo

  ORDER BY
  1 desc
  )

  SELECT
  date,
  level,
  kind,
  level_kind,
  receita_ac_19,
  CASE WHEN date < current_date() - interval 3 day THEN receita_ac_20 ELSE null END AS receita_ac_20,
  receita_mm_19,
  CASE WHEN date < current_date() - interval 3 day THEN receita_mm_20 ELSE null END AS receita_mm_20,
  usuarios_ac_19,
  CASE WHEN date < current_date() - interval 3 day THEN usuarios_ac_20 ELSE null END AS usuarios_ac_20,
  usuarios_mm_19,
  CASE WHEN date < current_date() - interval 3 day THEN usuarios_mm_20 ELSE null END AS usuarios_mm_20,
  pagos_mm_19,
  CASE WHEN date < current_date() - interval 3 day THEN pagos_mm_20 ELSE null END AS pagos_mm_20,
  pagos_ac_19,
  CASE WHEN date < current_date() - interval 3 day THEN pagos_ac_20 ELSE null END AS pagos_ac_20,
  1,
  CASE WHEN date < current_date() - interval 3 day THEN receita_ac_20/usuarios_ac_20 ELSE null END AS rpu_ac_20,
  receita_ac_19/usuarios_ac_19 AS rpu_ac_19,
  receita_ac_19/usuarios_ac_19 * (CASE WHEN level = 'Graduação' THEN 1.075 ELSE 1.50 END) AS rpu_ac_goal,
  CASE WHEN date < current_date() - interval 3 day THEN receita_mm_20/usuarios_mm_20 ELSE null END AS rpu_mm_20,
  receita_mm_19/usuarios_mm_19 AS rpu_mm_19,
  receita_mm_19/usuarios_mm_19 * (CASE WHEN level = 'Graduação' THEN 1.075 ELSE 1.50 END) AS rpu_mm_goal

  FROM
  base_final
"""

  df = load_from_db_cache(spark,query,'rpu_goals_only_qb_time_series_summer', from_cache)

  return df


def demand_data(spark,from_cache=False):
  logger.debug("Loading pricing updates ... ")


  query = """
  SELECT
  *
  FROM
  data_science.base_ordens_experimentos
  WHERE
  base_ordens_experimentos.registered_at BETWEEN '2020-01-01' AND '2020-10-01'
  AND
  offered_price <> 0
  AND
  origin IN ('Quero Bolsa')
  """
  
  df = load_from_db_cache(spark,query,'demand_data', from_cache)
  return df


def active_tests_results(spark,from_cache=False):
  logger.debug("Loading pricing updates ... ")


  query = """
WITH
alternatives_per_day AS(
  SELECT
    date,
    experiments_aggregate_base.fee_experiment_id,
    collect_set(alternative_id) AS alternative_ids
  FROM
    data_science.experiments_aggregate_base
  WHERE
    date >= '2020-04-01'
  AND
    alternative_ratio IS NOT NULL
  GROUP BY
  1,2
),

active_alternatives(
  SELECT
    fee_experiment_id,
    alternative_ids
  FROM
    alternatives_per_day
  WHERE
    date = date_sub(current_date(),3)
),

test_start AS(
  SELECT
    alternatives_per_day.fee_experiment_id,
    min(alternatives_per_day.date) AS date
  FROM
    alternatives_per_day 
  JOIN 
    active_alternatives ON active_alternatives.alternative_ids = alternatives_per_day.alternative_ids
  GROUP BY
    1
),
base_resultados AS(
SELECT
  test_start.date AS test_start_date,
  base_ordens_experimentos.fee_experiment_id,
  alternative,
  CASE WHEN alternative = 'seasonality minus 25' THEN 'c' ELSE
       CASE WHEN alternative = 'seasonality lowest' THEN 'd' ELSE 
       CASE WHEN alternative = 'seasonality minus 75' THEN 'e' ELSE alternative    
       END END END AS ordem,   
  SUM(CASE WHEN orders.checkout_step = 'paid' THEN 1 ELSE 0 END) AS paids,
  COUNT(DISTINCT customer_id) AS customers,
  SUM(CASE WHEN orders.checkout_step = 'paid' THEN 1 ELSE 0 END)/COUNT(DISTINCT customer_id) AS conversao,
  SUM(CASE WHEN orders.checkout_step = 'paid' THEN orders.price ELSE 0 END)/SUM(CASE WHEN orders.checkout_step = 'paid' THEN 1 ELSE 0 END) AS ticket_medio,
  (SUM(CASE WHEN orders.checkout_step = 'paid' THEN orders.price ELSE 0 END) + SUM(coalesce(sales.ltv_qp,0)))/SUM(CASE WHEN orders.checkout_step = 'paid' THEN 1 ELSE 0 END) AS ticket_medio_ltv,
  SUM(orders.price)/COUNT(DISTINCT customer_id) AS ticket_customer,
  SUM(CASE WHEN orders.checkout_step = 'paid' THEN orders.price ELSE 0 END) AS revenue,
  SUM(CASE WHEN orders.checkout_step = 'paid' THEN orders.price ELSE 0 END) + SUM(coalesce(sales.ltv_qp,0)) AS receita_com_ltv,
  (SUM(CASE WHEN orders.checkout_step = 'paid' THEN orders.price ELSE 0 END) + SUM(coalesce(sales.ltv_qp,0)))/COUNT(DISTINCT customer_id) AS rpu,
  SUM(CASE WHEN orders.checkout_step = 'paid' THEN orders.price ELSE 0 END)/COUNT(DISTINCT customer_id) AS rpu_sem_ltv,
  AVG(base_ordens_experimentos.offered_price) AS offered_price

FROM
  data_science.base_ordens_experimentos
LEFT JOIN
  data_warehouse.sales ON base_ordens_experimentos.order_id = sales.order_id
JOIN
  querobolsa_production.orders ON base_ordens_experimentos.order_id = orders.id
JOIN
  querobolsa_production.line_items ON orders.id = line_items.order_id
JOIN
  querobolsa_production.offers ON offers.id = line_items.offer_id
JOIN
  querobolsa_production.courses ON courses.id = offers.course_id
JOIN
  querobolsa_production.kinds k ON k.name = courses.kind
JOIN
  querobolsa_production.kinds ON k.parent_id = kinds.id
JOIN
  querobolsa_production.levels l ON l.name = courses.level
JOIN
  querobolsa_production.levels ON l.parent_id = levels.id
JOIN
  test_start ON test_start.fee_experiment_id = base_ordens_experimentos.fee_experiment_id AND base_ordens_experimentos.registered_at >= test_start.date

WHERE
  origin IN ('Quero Bolsa')

GROUP BY
  1,2,3

ORDER BY
  1,2
)

SELECT
  *
FROM
  base_resultados
WHERE
  customers > 50
ORDER BY
  fee_experiment_id
  """
  
  df = load_from_db_cache(spark,query,'active_tests_results', from_cache)
  return df

def demand_data(spark,from_cache=False):
  logger.debug("Loading pricing updates ... ")


  query = """
  SELECT
  *
  FROM
  data_science.base_ordens_experimentos
  WHERE
  base_ordens_experimentos.registered_at BETWEEN '2020-01-01' AND '2020-10-01'
  AND
  offered_price <> 0
  AND
  origin IN ('Quero Bolsa')
  """
  
  df = load_from_db_cache(spark,query,'demand_data', from_cache)
  return df


def baseline_versus_tests(spark,from_cache=False):
  logger.debug("Loading pricing updates ... ")

  query = """
  WITH base AS(
  SELECT
  DATE(base_ordens_experimentos.registered_at) AS date,
  fee_experiment_id,
  -- alternative,
  CASE WHEN alternative = 'baseline' THEN 'baseline' ELSE 'testes' END AS alternative_kind, 
  SUM(CASE WHEN orders.checkout_step = 'paid' THEN 1 ELSE 0 END) AS paids,
  COUNT(DISTINCT customer_id) AS customers,
  SUM(CASE WHEN orders.checkout_step = 'paid' THEN 1 ELSE 0 END)/COUNT(DISTINCT customer_id) AS conversao,
  SUM(CASE WHEN orders.checkout_step = 'paid' THEN orders.price ELSE 0 END)/SUM(CASE WHEN orders.checkout_step = 'paid' THEN 1 ELSE 0 END) AS ticket_medio,
  (SUM(CASE WHEN orders.checkout_step = 'paid' THEN orders.price ELSE 0 END) + SUM(coalesce(sales.ltv_qp,0)))/SUM(CASE WHEN orders.checkout_step = 'paid' THEN 1 ELSE 0 END) AS ticket_medio_ltv,
  SUM(orders.price)/COUNT(DISTINCT customer_id) AS ticket_customer,
  SUM(CASE WHEN orders.checkout_step = 'paid' THEN orders.price ELSE 0 END) AS revenue,
  SUM(CASE WHEN orders.checkout_step = 'paid' THEN orders.price ELSE 0 END) + SUM(coalesce(sales.ltv_qp,0)) AS receita_com_ltv,
  (SUM(CASE WHEN orders.checkout_step = 'paid' THEN orders.price ELSE 0 END) + SUM(coalesce(sales.ltv_qp,0)))/COUNT(DISTINCT customer_id) AS rpu,
  SUM(CASE WHEN orders.checkout_step = 'paid' THEN orders.price ELSE 0 END)/COUNT(DISTINCT customer_id) AS rpu_sem_ltv

  FROM
  data_science.base_ordens_experimentos
  LEFT JOIN
  data_warehouse.sales ON base_ordens_experimentos.order_id = sales.order_id
  JOIN
  querobolsa_production.orders ON base_ordens_experimentos.order_id = orders.id
  JOIN
  querobolsa_production.line_items ON orders.id = line_items.order_id
  JOIN
  querobolsa_production.offers ON offers.id = line_items.offer_id
  JOIN
  querobolsa_production.courses ON courses.id = offers.course_id
  JOIN
  querobolsa_production.kinds k ON k.name = courses.kind
  JOIN
  querobolsa_production.kinds ON k.parent_id = kinds.id
  JOIN
  querobolsa_production.levels l ON l.name = courses.level
  JOIN
  querobolsa_production.levels ON l.parent_id = levels.id

  WHERE
  base_ordens_experimentos.registered_at BETWEEN '2020-04-01' AND current_date - interval 2 days
  AND
  origin IN ('Quero Bolsa')

  GROUP BY
  1,2,3

  ORDER BY
  1,2
  ),
  base_limpa AS (
  SELECT
  date,
  fee_experiment_id,
  alternative_kind,
  paids,
  customers,
  receita_com_ltv

  FROM
  base

  ORDER BY
  1,alternative_kind
  ),
  base_evolutivo AS(
  SELECT
  date,
  fee_experiment_id,
  SUM(CASE WHEN alternative_kind = 'baseline' THEN paids ELSE 0 END) AS baseline_paids,
  SUM(CASE WHEN alternative_kind = 'baseline' THEN customers ELSE 0 END) AS baseline_customers,
  SUM(CASE WHEN alternative_kind = 'baseline' THEN receita_com_ltv ELSE 0 END) AS baseline_revenue,
  SUM(CASE WHEN alternative_kind = 'testes' THEN paids ELSE 0 END) AS testes_paids,
  SUM(CASE WHEN alternative_kind = 'testes' THEN customers ELSE 0 END) AS testes_customers,
  SUM(CASE WHEN alternative_kind = 'testes' THEN receita_com_ltv ELSE 0 END) AS testes_revenue

  FROM
  base_limpa

  GROUP BY
  1,2
  ),
  base_consolidada AS (
  SELECT
  date,
  fee_experiment_id,

  -- Acumulado

  SUM(baseline_customers) OVER (
          PARTITION BY
          fee_experiment_id    
          ORDER BY
          date
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS baseline_customers_ac,
  SUM(baseline_paids) OVER (
          PARTITION BY
          fee_experiment_id    
          ORDER BY
          date
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS baseline_paids_ac,
  SUM(baseline_revenue) OVER (
          PARTITION BY
          fee_experiment_id    
          ORDER BY
          date
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS baseline_revenue_ac,
  SUM(testes_customers) OVER (
          PARTITION BY
          fee_experiment_id    
          ORDER BY
          date
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS testes_customers_ac,
  SUM(testes_paids) OVER (
          PARTITION BY
          fee_experiment_id    
          ORDER BY
          date
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS testes_paids_ac,
  SUM(testes_revenue) OVER (
          PARTITION BY
          fee_experiment_id    
          ORDER BY
          date
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS testes_revenue_ac,
          
  -- Média Móvel        
          
  AVG(baseline_customers) OVER (
          PARTITION BY
          fee_experiment_id    
          ORDER BY
          date
          ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS baseline_customers_mm,
  AVG(baseline_paids) OVER (
          PARTITION BY
          fee_experiment_id    
          ORDER BY
          date
          ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS baseline_paids_mm,
  AVG(baseline_revenue) OVER (
          PARTITION BY
          fee_experiment_id    
          ORDER BY
          date
          ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS baseline_revenue_mm,
  AVG(testes_customers) OVER (
          PARTITION BY
          fee_experiment_id    
          ORDER BY
          date
          ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS testes_customers_mm,
  AVG(testes_paids) OVER (
          PARTITION BY
          fee_experiment_id    
          ORDER BY
          date
          ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS testes_paids_mm,
  AVG(testes_revenue) OVER (
          PARTITION BY
          fee_experiment_id    
          ORDER BY
          date
          ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS testes_revenue_mm
          
  FROM
  base_evolutivo
  ),
base_final AS(
  SELECT
  date,
  fee_experiment_id,
  baseline_revenue_ac,
  testes_revenue_ac,
  baseline_customers_ac,
  testes_customers_ac,
  baseline_paids_ac,
  testes_paids_ac,

  baseline_revenue_ac/baseline_customers_ac AS rpu_baseline_ac,
  testes_revenue_ac/testes_customers_ac AS rpu_testes_ac,

  baseline_paids_ac/baseline_customers_ac AS conversao_baseline_ac,
  testes_paids_ac/testes_customers_ac AS conversao_testes_ac,

  baseline_revenue_mm/baseline_customers_mm AS rpu_baseline_mm,
  testes_revenue_mm/testes_customers_mm AS rpu_testes_mm,

  baseline_paids_mm/baseline_customers_mm AS conversao_baseline_mm,
  testes_paids_mm/testes_customers_mm AS conversao_testes_mm

  FROM
  base_consolidada

  WHERE
  fee_experiment_id IN (56,57,58,59,60,61,62,63,65)
)
SELECT
*,
round(rpu_testes_ac/rpu_baseline_ac-1,2) AS rpu_gain,
round(conversao_testes_ac/conversao_baseline_ac-1,2) AS conversion_gain

FROM
base_final
  """
  
  df = load_from_db_cache(spark,query,'baseline_versus_tests', from_cache)
  return df