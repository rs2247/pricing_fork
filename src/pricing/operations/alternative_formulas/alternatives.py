from pricing.operations.utils import *
import os

class BaseAlternative:

  def __init__(self,filename = None):
    '''
      filename: name of the .sql file defining the alternative formula, if one exists
    '''
    if filename is not None:
      folderpath = os.path.dirname(__file__)
      with open(folderpath + '/' + filename + '.sql', 'r') as file:
        self.base_query = file.read().strip('\n')
    else:
        self.base_query = ''

  def get_fee_formula_sql(self,**params):
    return format_sql_formula(self.base_query,params)

class CityAlternative(BaseAlternative):

  def __init__(self,filename = None, weights = {}):
    super().__init__(filename)
    self.weights = weights

  def get_fee_formula_sql(self,k=1):

    if self.weights == {}:
      return format_sql_formula(self.base_query,{'k':k})
    else:
     city_query = ''.join(list(map (
        lambda key: '''
      WHEN campuses.city_id = {key} THEN
        {value}'''.format(
            key = key ,
            value = format_sql_formula(
                self.base_query,
                {'k':self.weights[key]*k}
            )
        ),
        self.weights
      )))

     return 'CASE' + city_query + '\nELSE \n' + format_sql_formula(self.base_query,{'k':k}) + '\nEND'
