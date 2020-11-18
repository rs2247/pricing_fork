
from pricing.operations.utils import *

def _min_wrapper_sql(full_discount_formula):
    final_formula = '''  case when {full_discount_formula} > 0 then
        case when subsidy_value - ({full_discount_formula}) > {min} then
          {full_discount_formula}
        else
          subsidy_value - {min}
        end
    end'''

    return final_formula.format_map({
        'full_discount_formula': full_discount_formula,
        'min':'{min}'
    })

def get_fee_formula_sql():
    """
    Returns the amount of di
    scount to be applied to the subsidy_value (value of the pef after subsidy)

    Returns:
        str: sql query defining the amout of discount
    """
    # discounts -> dict que representa o valor do desconto para cada campanha
    discounts = {
      # 'high_campaign': 'case when university_id = 19 then 0 else subsidy_value - offered_price end',
      'without_enem': """
          case when university_id = 19 then
              0
          else
              case when region = 'nordeste' then
                  (subsidy_value - offered_price)
              else
                  least(subsidy_value - offered_price, 0.15*subsidy_value )
              end
          end
      """
    }

    discount_whens = ''.join(list(map(lambda key: '''
  when campaign = '{key}' then
    {value}'''.format(key=key, value=_min_wrapper_sql(discounts[key])), discounts)))

    final_formula = '''case when subsidy_value > {min} then
  case''' + str(discount_whens) + '''
  end
end'''.format(discount_whens=discount_whens)

    return format_sql_formula(final_formula)
