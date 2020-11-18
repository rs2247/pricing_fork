
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import visdcc
from pricing.dashboard.app import app
# from pricing.dashboard.apps import app_cities,app_abtest,app_goals
from pricing.dashboard.pages import pages_definition,pathname2module
from datetime import datetime,timedelta
import dash_bootstrap_components as dbc
from pricing.utils import logger
import os



#####################################################################
#                             LAYOUT
#####################################################################
pages_list = []
for page_name,page_info in pages_definition.items():
  pages_list.append(
    html.Li([
      dcc.Link(page_name,href=page_info['pathname'])
    ], className='')
  )
nav_menu = html.Div([
  html.Div([
    html.Ul(pages_list, className='nav navbar-nav')
  ], className='navbar navbar-default'),
  visdcc.Run_js(id = 'javascript')
],id = 'topheader')

app.layout = html.Div([
  dcc.Location(id='url', refresh=False),
  html.Div([
    html.Div([
      html.H1(
        children=['Painel do Pricing'],
        id = 'main_title'
      )
    ],className= 'six columns'),
    html.Div([
      html.Label(
        'Last update: - ',
        id = 'index_timeupdate',
      )
    ],className = "six columns", style={'text-align':'right'}),
  ],className = 'row',style={'background':'#7D8C93'}),
  nav_menu,
  html.Div(id='page-content'),
])

#Makes the selected item in the navbar stay active while clicking in the page
@app.callback(
  Output('javascript', 'run'),
  [Input('url','pathname')]
)
def hold_navbar_active_state(x):
  return '''
$( '#topheader .navbar-nav a' ).on( 'click', function () {
  console.log("Click navbar")
  $( '#topheader .navbar-nav' ).find( 'li.active' ).removeClass( 'active' );
  $( this ).parent( 'li' ).addClass( 'active' );
});
  '''

@app.callback(
  Output('page-content','children'),
  [Input('url', 'pathname')]
)
def display_page(pathname):
    page_module = pathname2module(pathname)
    if page_module is not None:
      return page_module.make_layout()
    elif pathname == '/':
      return None
    else:
      return '404'

if __name__ == '__main__':

    #Instantiating the data in each page
    for _,page_info in pages_definition.items():
      page_info['module'].load_page_data(from_cache = True)

    if os.environ['DASHBOARD_MODE']=='DEBUG':
      debug = True
    else:
      debug = False
    logger.info("Starting dashboard in debug mode? " + str(debug))
    app.run_server(host='0.0.0.0',port=8050,debug=debug)

