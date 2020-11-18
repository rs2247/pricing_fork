from pricing.operations.experiments.helper import *

from pricing.operations.alternative_formulas import real_discount_qap,\
   qap_subsidy,campaign_discount

def get_experiment():
  return {

  "name": "qap",
  "target_offers_sql": load_target_offers(__file__),
  "fee_experiment_alternatives":[
    {
      "name":'baseline',
      "fee_formula_sql":real_discount_qap.get_fee_formula_sql(k=1.),
      "fee_subsidy_formula_sql":qap_subsidy.get_fee_formula_sql(level_1_subsidy = 0.5),
      "fee_discount_formula_sql":campaign_discount.get_fee_formula_sql(),
    },
    {
      "name":'level_1_subsidy_40',
      "fee_formula_sql":real_discount_qap.get_fee_formula_sql(k=1.),
      "fee_subsidy_formula_sql":qap_subsidy.get_fee_formula_sql(level_1_subsidy = 0.4),
      "fee_discount_formula_sql":campaign_discount.get_fee_formula_sql(),
      "initial_ratio": 0,
    },
    {
      "name":'level_1_subsidy_60',
      "fee_formula_sql":real_discount_qap.get_fee_formula_sql(k=1.),
      "fee_subsidy_formula_sql":qap_subsidy.get_fee_formula_sql(level_1_subsidy = 0.6),
      "fee_discount_formula_sql":campaign_discount.get_fee_formula_sql(),
      "initial_ratio": 0,
    },
    {
      "name":'level_1_subsidy_75',
      "fee_formula_sql":real_discount_qap.get_fee_formula_sql(k=1.),
      "fee_subsidy_formula_sql":qap_subsidy.get_fee_formula_sql(level_1_subsidy = 0.50),
      "fee_discount_formula_sql":campaign_discount.get_fee_formula_sql(),
      "initial_ratio": 0,
    },
    {
      "name":'seasonality minus 50',
      "fee_formula_sql":real_discount_qap.get_fee_formula_sql(k=0.5),
      "fee_subsidy_formula_sql":qap_subsidy.get_fee_formula_sql(level_1_subsidy = 0.5),
      "fee_discount_formula_sql":campaign_discount.get_fee_formula_sql(),
      "initial_ratio": 0,
    },
    {
      "name":'dummy'
    }
]}
