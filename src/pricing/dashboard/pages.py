from pricing.dashboard.apps import app_cities,app_abtest,app_goals


#####################################################################
#                             PAGES
#####################################################################
#All pages should be in this dict and have a @load_page_data method
pages_definition = {
  'Metas': {
    'pathname':'/goals',
    'module': app_goals
  },
  'Cidades':{
    'pathname':'/cities',
    'module':app_cities
  },
  'Testes':{
    'pathname':'/abtest',
    'module':app_abtest
  }
}

def pathname2module(pathname):
  for page_name,page_info in pages_definition.items():
    if page_info['pathname'] == pathname:
      return page_info['module']

  return None
