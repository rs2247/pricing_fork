def experiment_current_average_value_for_city(city_id, experiment_id):

    query = """
    select
      avg(pre_enrollment_fees.value)
    from
      querobolsa_production.fee_experiment_offers
    inner join querobolsa_production.offers on (offers.id = fee_experiment_offers.offer_id and offers.enabled)
    inner join querobolsa_production.experiment_pre_enrollment_fees on (experiment_pre_enrollment_fees.offer_id = offers.id and experiment_pre_enrollment_fees.enabled)
    inner join querobolsa_production.fee_experiment_alternatives on (fee_experiment_alternatives.id = experiment_pre_enrollment_fees.fee_experiment_alternative_id and fee_experiment_alternatives.name = 'baseline')
    inner join querobolsa_production.pre_enrollment_fees on (pre_enrollment_fees.id = experiment_pre_enrollment_fees.pre_enrollment_fee_id)
    inner join querobolsa_production.courses on (courses.id = offers.course_id)
    inner join querobolsa_production.campuses on (campuses.id = courses.campus_id)
    where
      fee_experiment_offers.fee_experiment_id = {experiment_id}
      and campuses.city_id = {city_id}
    """.format(experiment_id=experiment_id, city_id=city_id)


    df = spark.sql(query)

    return df.toPandas()



def build_calculated_values_query(experiment_id, select_fields, subsidy_values, discount_select_fields, subsidy_select_fields, aditional_filter = ''):

    select_query = """
    select
    id,
    {subsidy_values},
    {discount_select_fields}
    from (
    select
     id,
     {subsidy_select_fields},
     university_id,
     offered_price,
     campaign,
     billing_integration,
     full_price,
     kind_id,
     level_id
    from (
      select
        offers.id,
        {select_fields},
        offers.university_id as university_id,
        offers.offered_price as offered_price,
        offers.campaign as campaign,
        offers.billing_integration as billing_integration,
        university_offers.full_price as full_price,
        kinds.parent_id as kind_id,
        levels.parent_id as level_id
      from
        querobolsa_production.fee_experiment_offers
      inner join
        querobolsa_production.offers on (offers.id = fee_experiment_offers.offer_id and offers.enabled)
      inner join querobolsa_production.university_offers on (university_offers.id = offers.university_offer_id)
      inner join querobolsa_production.courses on (courses.id = offers.course_id)
      inner join querobolsa_production.campuses on (campuses.id = courses.campus_id)
      inner join querobolsa_production.kinds on (kinds.name = courses.kind and kinds.parent_id is not null)
      inner join querobolsa_production.levels on (levels.name = courses.level and levels.parent_id is not null)
      where
        fee_experiment_offers.fee_experiment_id = {experiment_id}
        {aditional_filter}
    ) as base_subsidy
    ) as base_discount
    """.format(select_fields=select_fields, experiment_id=experiment_id, subsidy_select_fields=subsidy_select_fields, discount_select_fields=discount_select_fields, subsidy_values=subsidy_values, aditional_filter=aditional_filter)


    return select_query




from pricing.operations.alternative_formulas.alternatives import *

def simulate_k_value_for_city_formula(experiment_id, city_id, k_value, formula_sql):

    TEST_CITY_WEIGHTS = {}

    TEST_CITY_WEIGHTS[city_id] = k_value

    aditional_filter = 'and campuses.city_id = {city_id}'.format(city_id=city_id)


    alternative_object = CityAlternative(formula_sql, TEST_CITY_WEIGHTS)


    select_fields = alternative_object.get_fee_formula_sql() + " as original_value"
    subsidy_select_fields = 'original_value as subsidy_value'
    subsidy_values = 'subsidy_value'
    discount_select_fields = 'null as discount_value'

    validate_query = """
    select avg(subsidy_value) as avg_fee from (
        {inner_query}
    ) as base_values""".format(inner_query=build_calculated_values_query(experiment_id, select_fields, subsidy_values, discount_select_fields, subsidy_select_fields, aditional_filter))

    # print(validate_query)
    
    return spark.sql(validate_query)



def simulate_values_for_city_formula(experiment_id, city_id, formula_sql, k_range):
    for k in k_range:
        df = simulate_k_value_for_city_formula(experiment_id, city_id, k, 'real_discount_level_1_kind_1')
        avg_fee = df.first()['avg_fee']
        print(str(k) + ' -> ' + str(avg_fee))
