
#GAMBI
import sys
sys.path.append('../..')
# DEBUG_MODE = True

import math
import time
import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output,State
import plotly.graph_objs as go
from plotly.subplots import make_subplots
import plotly.express as px

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
from pricing.dashboard.utils import *
from pricing.dashboard.data_loading import *
from pricing.dashboard.app import app
from pricing.models.abtests import *
from pricing.utils import logger
import os

import time

EXPERIMENTS_LIST =[
  {'label': '56 | Graduação | Presencial | Premium','value':56},
  {'label': '57 | Graduação | Presencial | Popular','value':57},
  {'label': '58 | Graduação | EaD        | Premium','value':58},
  {'label': '59 | Graduação | EaD        | Popular','value':59},
  {'label': '60 | Graduação | Semi','value':60},
  {'label': '61 | Pós-grad  | Presencial','value':61},
  {'label': '62 | Pós-grad  | EaD + Semi | Popular','value':62},
  {'label': '63 | Pós-grad  | EaD + Semi | Premium','value':63},
  {'label': '64 | Kroton','value':64},
  {'label': '65 | QAP','value':65},
]

BASE_FIELDS = {
  'cum': { 'label': 'Acumulado', 'fields': [ 'arpu', 'alternative_ratio', 'av_ticket', 'registered_conversion'] },
  'window_7': { 'label': 'Média móvel 7 dias', 'fields': [ 'arpu_7', 'alternative_ratio', 'av_ticket_7', 'registered_conversion_7'] },
  'window_14': { 'label': 'Média móvel 14 dias', 'fields': [ 'arpu_14', 'alternative_ratio', 'av_ticket_14', 'registered_conversion_14'] }
}

#####################################################################
#                          LOADING DATA
#####################################################################

def load_page_data(from_cache=True):
  logger.debug("Loading data for app_tests ...")
  global df_demand, df_active_alternatives, df_baseline_vs_tests

  df_demand = demand_data(spark,from_cache)
  df_active_alternatives = active_tests_results(spark,from_cache)
  df_baseline_vs_tests = baseline_versus_tests(spark,from_cache)

#####################################################################
#                             LAYOUT
#####################################################################
def format_figure(fig,showlegend=False,height = ROW_HEIGHTS[1]):
  fig.update_layout(
    showlegend=showlegend,
    height=height,
    paper_bgcolor=PLOT_BGCOLOR,
    plot_bgcolor=PLOT_BGCOLOR,
    xaxis = {'color':"#ffffff",'gridcolor':"#1F2426",'title': ''},
    yaxis = {'color':"#ffffff",'gridcolor':"#1F2426",'title': ''},
    margin= {'l': 10, 'r': 10, 't': 10, 'b': 1}
  )

  if showlegend:
    fig.update_layout(
      legend= {
        'x':-.1,
        'y':1.2,
        'font': {'color':'white'},
        'orientation':'h'
      },
      # margin= {'t': 100}
    )

def make_layout():
  return html.Div([

# Financial return strings

  html.Div([
      html.Label(financial_return()),
    ]
  ,className='six columns pretty_container'),

  html.Div([
      html.Label(volume_return()),
    ]
  ,className='six columns pretty_container'),

## Charts start here

  html.Div(children=[
    html.Div(children=[
        html.H6([
            "Comparativo RPU: Testes x Baseline",
        ], className="container_title"),
        dcc.Loading(
            dcc.Graph(
                id='rpu_baseline_vs_tests',
                figure=rpu_base_vs_tests()
                # config={'displayModeBar': False},
            ),
            className='svg-container',
            style={'height': ROW_HEIGHTS[1]}
        ),
    ], className='six columns pretty_container'),
    html.Div(children=[
        html.H6([
            "Comparativo Conversão: Testes x Baseline",
        ], className="container_title"),
        dcc.Loading(
            dcc.Graph(
                id='conversao_baseline_vs_tests',
                figure=conversion_base_vs_tests()
                # config={'displayModeBar': False},
            ),
            className='svg-container',
            style={'height': ROW_HEIGHTS[1]}
        )
    ], className='six columns pretty_container'),
  ], className = "row",style={'margin-top':'2%'}),


  html.Div([
    html.Label('Período'),
    html.Div([
      dcc.DatePickerRange(
          id='experiment-date-picker-range',
          min_date_allowed=datetime(2020, 1, 1),
          max_date_allowed=datetime.now().date(),
          initial_visible_month=(datetime.now() - timedelta(days=3)).date(),
          start_date=datetime(2020, 1, 1),
          end_date=(datetime.now() - timedelta(days=3)).date()
      ),
      html.Div(id='output-container-date-picker-range')
    ]),
  ]),
  html.Div([
      html.Label('Experimento'),
      dcc.Dropdown(
      id = 'experiment_selection',
      options=EXPERIMENTS_LIST,
      value=56,
      className = 'pretty_dropdown'
    )]
  ,style={'width':'25%'}),

  html.Div([
      html.Label('Cidade'),
      dcc.Dropdown(
      id = 'city_selection',
      options=[{'label':'All','value':-1}],
      value=-1,
      className = 'pretty_dropdown'
    )]
  ,style={'width':'25%'}),


  html.Div([
      html.Label('Visualização'),
      dcc.Dropdown(
      id = 'serie_type_selection',
      options=[{'label': 'Acumulado','value':'cum'},{'label': 'Media móvel 7 dias','value':'window_7'},{'label': 'Media móvel 14 dias','value':'window_14'}],
      value='window_7',
      className = 'pretty_dropdown'
    )]
  ,style={'width':'25%'}),

  html.Div([ #Experiment comparison
    html.Div(children=[
      #ARPU
      html.Div(children=[
          html.H6([
              html.Label('ARPU', id='label_arpu_experiment'),
          ], className="container_title"),
          dcc.Loading(
              dcc.Graph(
                  id='arpu_experiment',
                  figure=blank_fig(ROW_HEIGHTS[1]),
                  config={'displayModeBar': False},
              ),
              className='svg-container',
              style={'height': ROW_HEIGHTS[1]}
          ),
      ], className='six columns pretty_container'),
      #Ratio
      html.Div(children=[
          html.H6([
              html.Label("Ratio por alternativa"),
          ], className="container_title"),
          dcc.Loading(
              dcc.Graph(
                  id='ratio_experiment',
                  figure=blank_fig(ROW_HEIGHTS[1])
                  # config={'displayModeBar': False},
              ),
              className='svg-container',
              style={'height': ROW_HEIGHTS[1]}
          )
      ], className='six columns pretty_container'),
    ], className = "row"),

    html.Div(children=[
      #Ticket
      html.Div(children=[
          html.H6([
              html.Label('Ticket Médio', id='label_ticket_experiment'),
          ], className="container_title"),
          dcc.Loading(
              dcc.Graph(
                  id='ticket_experiment',
                  figure=blank_fig(ROW_HEIGHTS[1]),
                  config={'displayModeBar': False},
              ),
              className='svg-container',
              style={'height': ROW_HEIGHTS[1]}
          ),
      ], className='six columns pretty_container'),
      #Conversion
      html.Div(children=[
          html.H6([
              html.Label('Conversão (pagos/registered)', id='label_conversion_experiment'),
          ], className="container_title"),
          dcc.Loading(
              dcc.Graph(
                  id='conversion_experiment',
                  figure=blank_fig(ROW_HEIGHTS[1])
                  # config={'displayModeBar': False},
              ),
              className='svg-container',
              style={'height': ROW_HEIGHTS[1]}
          )
      ], className='six columns pretty_container'),
    ], className = "row"),

################# Nova Métrica: Comparativo Baseline x Testes ############################

# RPU

    html.Div(children=[
      # Acumulado
      html.Div(children=[
          html.H6([
              html.Label('Comparativo RPU Baseline x Testes (Acumulado)'),
          ], className="container_title"),
          dcc.Loading(
              dcc.Graph(
                  id='new_metric_rpu_ac',
                  figure=blank_fig(ROW_HEIGHTS[1]),
                  config={'displayModeBar': False},
              ),
              className='svg-container',
              style={'height': ROW_HEIGHTS[1]}
          ),
      ], className='six columns pretty_container'),
      # Média Móvel
      html.Div(children=[
          html.H6([
              html.Label('Comparativo RPU Baseline x Testes (Média Móvel)'),
          ], className="container_title"),
          dcc.Loading(
              dcc.Graph(
                  id='new_metric_rpu_mm',
                  figure=blank_fig(ROW_HEIGHTS[1])
                  # config={'displayModeBar': False},
              ),
              className='svg-container',
              style={'height': ROW_HEIGHTS[1]}
          )
      ], className='six columns pretty_container'),
    ], className = "row"),

# Conversao

    html.Div(children=[
      # Acumulado
      html.Div(children=[
          html.H6([
              html.Label('Comparativo Conversão Baseline x Testes (Acumulado)'),
          ], className="container_title"),
          dcc.Loading(
              dcc.Graph(
                  id='new_metric_conv_ac',
                  figure=blank_fig(ROW_HEIGHTS[1]),
                  config={'displayModeBar': False},
              ),
              className='svg-container',
              style={'height': ROW_HEIGHTS[1]}
          ),
      ], className='six columns pretty_container'),
      # Média Móvel
      html.Div(children=[
          html.H6([
              html.Label('Comparativo Conversão Baseline x Testes (Média Móvel)'),
          ], className="container_title"),
          dcc.Loading(
              dcc.Graph(
                  id='new_metric_conv_mm',
                  figure=blank_fig(ROW_HEIGHTS[1])
                  # config={'displayModeBar': False},
              ),
              className='svg-container',
              style={'height': ROW_HEIGHTS[1]}
          )
      ], className='six columns pretty_container'),
    ], className = "row"),

################# Comparativo de Alternativas Ativas por Experimento ############################

  html.Div([
      html.Label('Dados Acumulados para Alternativas Ativas'),
    ]
  ,style={'width':'25%'}),

  html.Div([
      html.Label('Experimento Selecionado: - ', id='label_selected_experiment_2'),
    ]
  ,style={'width':'25%'}),

  html.Div([
      html.Label('Data de início dos testes ativos: - ', id='label_selected_experiment_date'),
    ]
  ,style={'width':'25%'}),  
  
  html.Div([
      html.Label('Dias decorridos do início: - ', id='label_selected_experiment_days'),
    ]
  ,style={'width':'25%'}),

  html.Div([ #Alternativas Ativas
    html.Div(children=[
      #Customers
      html.Div(children=[
          html.H6([
              html.Label("Customers"),
          ], className="container_title"),
          dcc.Loading(
              dcc.Graph(
                  id='active_alternatives_customers',
                  figure=blank_fig(ROW_HEIGHTS[2]),
                  config={'displayModeBar': False},
              ),
              className='svg-container',
              style={'height': ROW_HEIGHTS[2]}
          ),
      ], className = 'four columns pretty_container'),
      #RPU
      html.Div(children=[
          html.H6([
              html.Label("Retorno Por Usuário (RPU)"),
          ], className="container_title"),
          dcc.Loading(
              dcc.Graph(
                  id='active_alternatives_rpu',
                  figure=blank_fig(ROW_HEIGHTS[2])
                  # config={'displayModeBar': False},
              ),
              className='svg-container',
              style={'height': ROW_HEIGHTS[2]}
          )
      ], className='four columns pretty_container'),
      # Conversão
      html.Div(children=[
          html.H6([
              html.Label("Conversão"),
          ], className="container_title"),
          dcc.Loading(
              dcc.Graph(
                  id='active_alternatives_conv',
                  figure=blank_fig(ROW_HEIGHTS[2]),
                  config={'displayModeBar': False},
              ),
              className='svg-container',
              style={'height': ROW_HEIGHTS[2]}
          ),
      ], className='four columns pretty_container'),
    ], className = "row"),

####################### Curva de Elasticidade ###############################

    html.Div(children=[
      # Elasticidade
      html.Div(children=[
          html.H6([
              html.Label('Ticket Médio das Alternativas Ativas'),
          ], className="container_title"),
          dcc.Loading(
              dcc.Graph(
                  id='active_alternatives_pef_offered',
                  figure=blank_fig(ROW_HEIGHTS[1]),
                  config={'displayModeBar': False},
              ),
              className='svg-container',
              style={'height': ROW_HEIGHTS[1]}
          ),
      ], className='six columns pretty_container'),
      # Elasticidade
      html.Div(children=[
          html.H6([
              html.Label('Elasticidade Estimada PEF', id='label_elasticity_pef'),
          ], className="container_title"),
          dcc.Loading(
              dcc.Graph(
                  id='demand_elasticity',
                  figure=blank_fig(ROW_HEIGHTS[1])
                  # config={'displayModeBar': False},
              ),
              className='svg-container',
              style={'height': ROW_HEIGHTS[1]}
          )
      ], className='six columns pretty_container'),
    ], className = "row"),
  ], className = "row"),

######################### BAEYESIAN ANALYSIS ##########################


  ]),
  html.Label('Experimento selecionado: - ', id='label_selected_experiment'),
  html.Div([
      dcc.Dropdown(
      id = 'alternative_selection',
      options=[],
      value=None,
      className = 'pretty_dropdown'
    )]
  ,style={'width':'25%'}),
  html.Div([ #Alternative Comparsion
    html.Div(children=[
      #ARPU distribution
      html.Div(children=[
          html.H6([
              "Distribuição de ARPU",
          ], className="container_title"),
          dcc.Loading(
              dcc.Graph(
                  id='arpu_alternative',
                  figure=blank_fig(ROW_HEIGHTS[2]),
                  config={'displayModeBar': False},
              ),
              className='svg-container',
              style={'height': ROW_HEIGHTS[2]}
          ),
      ], className = 'four columns pretty_container'),
      #Probability to beat alternative
      html.Div(children=[
          html.H6([
              "Probabilidade de vencer alternativa",
          ], className="container_title"),
          dcc.Loading(
              dcc.Graph(
                  id='probability_alternative',
                  figure=blank_fig(ROW_HEIGHTS[2])
                  # config={'displayModeBar': False},
              ),
              className='svg-container',
              style={'height': ROW_HEIGHTS[2]}
          )
      ], className='four columns pretty_container'),
      html.Div(children=[
          html.H6([
              "Perda esperada (%) caso perca",
          ], className="container_title"),
          dcc.Loading(
              dcc.Graph(
                  id='loss_alternative',
                  figure=blank_fig(ROW_HEIGHTS[2]),
                  config={'displayModeBar': False},
              ),
              className='svg-container',
              style={'height': ROW_HEIGHTS[2]}
          ),
      ], className='four columns pretty_container'),

    ], className = "row"),
    html.Div(children=[
      #Real discount for both alternatives
      html.Div(children=[
          html.H6([
              "Offered_price médio acumulado por alternativa",
          ], className="container_title"),
          dcc.Loading(
              dcc.Graph(
                  id='offered_price_alternative',
                  figure=blank_fig(ROW_HEIGHTS[2])
                  # config={'displayModeBar': False},
              ),
              className='svg-container',
              style={'height': ROW_HEIGHTS[2]}
          )
      ], className='six columns pretty_container'),
      #Drop in av ticket between alternatives
      html.Div(children=[
          html.H6([
              "Desconto teste x baseline acumulado",
          ], className="container_title"),
          dcc.Loading(
              dcc.Graph(
                  id='price_discount_alternative',
                  figure=blank_fig(ROW_HEIGHTS[2]),
                  config={'displayModeBar': False},
              ),
              className='svg-container',
              style={'height': ROW_HEIGHTS[2]}
          ),
      ], className='six columns pretty_container'),

    ], className = "row"),

    html.Div(children=[
      #% orders per IES
      html.Div(children=[
          html.H6([
              "Número de customers por dia/alternativa (não acumulado)",
          ], className="container_title"),
          dcc.Loading(
              dcc.Graph(
                  id='customer_count_alternative',
                  figure=blank_fig(ROW_HEIGHTS[2])
                  # config={'displayModeBar': False},
              ),
              className='svg-container',
              style={'height': ROW_HEIGHTS[2]}
          )
      ], className='six columns pretty_container'),
      #ExpLoss
      html.Div(children=[
          html.H6([
              "---",
          ], className="container_title"),
          dcc.Loading(
              dcc.Graph(
                  id='placeholder',
                  figure=blank_fig(ROW_HEIGHTS[2]),
                  config={'displayModeBar': False},
              ),
              className='svg-container',
              style={'height': ROW_HEIGHTS[2]}
          ),
      ], className='six columns pretty_container'),

    ], className = "row"),
  ])
],className = 'plots_body')


#####################################################################
#                      FIRST CHARTS, NO CALL BACKS                  #
#####################################################################

def financial_return():

  df = df_baseline_vs_tests
  current_date = pd.to_datetime(df['date']).max()

  # filter_date = date.today() - timedelta(days=4) 

  df = df[df['date']==current_date]

  df['financial_return'] = (df['rpu_testes_ac'] - df['rpu_baseline_ac']) * df['testes_customers_ac']
  total_financial_return = (df['financial_return'].sum()/1000).round(0).astype('int64')
  total_revenue = ((df['baseline_revenue_ac'].sum() + df['testes_revenue_ac'].sum())/1000000).round(2)
  percent_financial_return = ((total_financial_return*1000)/(total_revenue*1000000)*100).round(2)

  # String concatenation

  string = 'Retorno financeiro de R$ {}k, de um total de R$ {}M que representa  um retorno de {}%'.format(total_financial_return,total_revenue,percent_financial_return)

  return string

def volume_return():

  df = df_baseline_vs_tests
  current_date = pd.to_datetime(df['date']).max()

  df = df[df['date']==current_date]

  df['volume_return'] = (df['conversao_testes_ac'] - df['conversao_baseline_ac']) * df['testes_customers_ac']
  total_volume_return = (df['volume_return'].sum()).round(0).astype('int64')
  total_paids = (df['baseline_paids_ac'].sum() + df['testes_paids_ac'].sum()).round(0).astype('int64')
  percent_volume_return = (total_volume_return/total_paids*100).round(2)

  # String concatenation

  string = 'Retorno em volume de {} alunos, de um total de {} captados que representa  um retorno {}%'.format(total_volume_return,total_paids,percent_volume_return)

  return string

def rpu_base_vs_tests():

  df = df_baseline_vs_tests
  current_date = pd.to_datetime(df['date']).max()

  # filter_date = date.today() - timedelta(days=4) 

  df = df[df['date']==current_date]
  df['fee_experiment_id'] = df['fee_experiment_id'].astype(str)

  dict_exp = {'56':'56 Grad Presencial Premium',
              '57':'57 Grad Presencial Popular',
              '58':'58 Grad EaD Premium',
              '59':'59 Grad EaD Popular',
              '60':'60 Grad Semipresencial',
              '61':'61 Pós Presencial',
              '62':'62 Pós EaD Premium',
              '63':'63 Pós EaD Popular',
              '65':'65 QAP'}

  df['fee_experiment'] = df['fee_experiment_id'].map(dict_exp)

  df = df.sort_values(by='fee_experiment_id',ascending=True)

  # df['rpu_gain'] = str(round((df['rpu_testes_ac']/df['rpu_baseline_ac']-1),2))

  fig = go.Figure(
    data=[go.Bar(
    name = 'Baseline',
    x = df['fee_experiment'],
    y = df['rpu_baseline_ac']
    ),
    go.Bar(
    name = 'Testes',
    x = df['fee_experiment'],
    y = df['rpu_testes_ac'],
    text = df['rpu_gain'],
    textposition = 'inside'
    )
    ]
  )

  format_figure(fig,showlegend=True)

  return fig


def conversion_base_vs_tests():

  df = df_baseline_vs_tests
  current_date = pd.to_datetime(df['date']).max()

  df = df[df['date']==current_date]
  df['fee_experiment_id'] = df['fee_experiment_id'].astype(str)

  dict_exp = {'56':'56 Grad Presencial Premium',
              '57':'57 Grad Presencial Popular',
              '58':'58 Grad EaD Premium',
              '59':'59 Grad EaD Popular',
              '60':'60 Grad Semipresencial',
              '61':'61 Pós Presencial',
              '62':'62 Pós EaD Premium',
              '63':'63 Pós EaD Popular',
              '65':'65 QAP'}

  df['fee_experiment'] = df['fee_experiment_id'].map(dict_exp)

  df = df.sort_values(by='fee_experiment_id',ascending=True)

  # df['conversion_gain'] = str(round((df['conversao_testes_ac']/df['conversao_baseline_ac']-1),2))

  fig = go.Figure(
    data=[go.Bar(
    name = 'Baseline',
    x = df['fee_experiment'],
    y = df['conversao_baseline_ac']
    ),
    go.Bar(
    name = 'Testes',
    x = df['fee_experiment'],
    y = df['conversao_testes_ac'],
    text = df['conversion_gain'],
    textposition = 'inside'
    )
    ]
  )

  format_figure(fig,showlegend=True)

  return fig

#####################################################################
#                         EXPERIMENT CALLBACKS
#####################################################################
@app.callback(
    dash.dependencies.Output('output-container-date-picker-range', 'children'),
    [dash.dependencies.Input('experiment-date-picker-range', 'start_date'),
     dash.dependencies.Input('experiment-date-picker-range', 'end_date')])
def update_date_range(start_date, end_date):
  print('update_date_range# start_date: ' + str(start_date) + " end_date: " + str(end_date))

@app.callback(
  [Output('city_selection', 'options'),
   Output('city_selection', 'value')],
  [
   Input('experiment_selection','value'),
   Input('experiment-date-picker-range', 'start_date'),
   Input('experiment-date-picker-range', 'end_date')
  ])
def update_city_dropdown(selected_experiment,start_date,end_date):
  logger.debug("Updating city dropdown: " + str(selected_experiment))
  aux = get_experiment_agg_data_per_city(selected_experiment, start_date, end_date)
  aux = aux[aux['order_id']>MIN_N_POINTS]
  options = [{'label':'All','value':-1}]
  for city,n_orders in zip(aux['city'].tolist(),aux['order_id'].tolist()):
      options.append({
          'label':city + '  (' + str(n_orders) + ' orders)' ,
          'value':city
      })
  logger.debug("Updating city dropdown: " + str(selected_experiment) + " FINISHED")
  return options,-1

@app.callback(
  [
    Output('arpu_experiment', 'figure'),
    Output('ratio_experiment', 'figure'),
    Output('ticket_experiment', 'figure'),
    Output('conversion_experiment', 'figure'),
    Output('label_arpu_experiment', 'children'),
    Output('label_ticket_experiment', 'children'),
    Output('label_conversion_experiment', 'children'),
    # Output('pef_elasticity', 'figure'),
    # Output('label_elasticity_pef', 'children'),
  ],
  [
    Input('city_selection','value'),
    Input('serie_type_selection','value'),
  ],
  [
    State('experiment-date-picker-range', 'start_date'),
    State('experiment-date-picker-range', 'end_date'),
    State('experiment_selection', 'value')
  ]
)
def update_selected_experiment(selected_city,selected_series_type,start_date,end_date,selected_experiment):
  logger.debug("Updating selected experiment: " + str(selected_experiment) + " selected_city: " + str(selected_city) + " start_date: " + start_date + " end_date: " + end_date + " selected_series_type: " + selected_series_type)

  #Setting filters
  filters = {}
  if selected_city != -1:
    filters = {'city':selected_city}

  df_exp_agg = get_experiment_agg_data(selected_experiment, start_date, end_date, filters, 'date, alternative_id')

  # x_range, y_range_reta, y_range, conversion, price, arpu, elasticity_label = elasticity_for_experiment(selected_experiment, initial_window_date = start_date, base_date = end_date, field = selected_series_type)
  # elasticity_plot = plot_pef_elasticity(x_range, y_range_reta, y_range, conversion, price, arpu)

  fig_arpu,fig_ratio,fig_ticket,fig_conversion = plot_experiment_comparison(df_exp_agg, BASE_FIELDS[selected_series_type]['fields'])
  for fig in [fig_arpu]:
    format_figure(fig,showlegend=True)

  for fig in [fig_ratio,fig_ticket,fig_conversion]:
    format_figure(fig)

  print("Updating selected experiment: " + str(selected_experiment) + " FINISH")

  arpu_label = 'ARPU - ' + BASE_FIELDS[selected_series_type]['label']
  ticket_label = 'Ticket Médio - ' + BASE_FIELDS[selected_series_type]['label']
  conversion_label = 'Conversão (pagos/registered) - ' + BASE_FIELDS[selected_series_type]['label']

  # elasticity_label = 'Elasticidade PEF - ' + elasticity_label

  return fig_arpu,fig_ratio,fig_ticket,fig_conversion,arpu_label,ticket_label,conversion_label

@app.callback(
  Output('label_selected_experiment', 'children'),
  [
    Input('experiment_selection','value'),
    Input('city_selection','value')
  ])
def update_experiment_label(selected_experiment,selected_city):
  if selected_experiment is None:
    selected_experiment = '-'
  if (selected_city is None) or (selected_city is -1):
    selected_city = '-'

  experiment_label = ('Experimento Selecionado: ' + str(selected_experiment) + ' | Cidade Selecionada: '  + str(selected_city))
  
  return experiment_label

# Call back da curva de demanda

@app.callback(
  Output('demand_elasticity', 'figure'),
  [
    Input('experiment_selection','value'),
    Input('experiment-date-picker-range','start_date'),
    Input('experiment-date-picker-range','end_date')
  ])
def update_demand_elasticity(selected_experiment,start_date,end_date):

  df = df_demand

  # Fixing Types
  df['price'] = df['price'].astype('float64')
  df['offered_price'] = df['offered_price'].astype('float64')
  df['pef_over_offered'] = (df['price']/df['offered_price']).round(1).astype('float64')
  df['paid'] = df['checkout_step']=='paid'
  df['pagos'] = df['paid']

  # User Filters (Callback)

  # Experiment ID
  df = df[df['fee_experiment_id']==selected_experiment]
  # Date Range
  df = df[(df['registered_at']>start_date) & (df['registered_at']<end_date)] 

  # Aggregation
  plot_df = df.groupby('pef_over_offered').agg({'paid':'sum','pagos':'sem','price':'sum','order_id':'count'}).reset_index()
  plot_df = plot_df.rename(columns={'paid':'paids','order_id':'orders','price':'revenue','pagos':'sem'})
  plot_df['fatordem'] = plot_df['revenue']/plot_df['orders']
  plot_df['conversion'] = plot_df['paids']/plot_df['orders']
  plot_df['otimizacao'] = plot_df['conversion']*plot_df['pef_over_offered']
  plot_df['otimizacao_ma'] = plot_df['otimizacao'].rolling(window=7).mean()

  # Regular Filters
  plot_df = plot_df[plot_df['orders']>50]
  plot_df = plot_df[plot_df['pef_over_offered']<10.0]

  # Create Figure with secondary y-axis
  fig = make_subplots(specs=[[{"secondary_y": True}]])

  # Add Traces
  fig.add_trace(
    go.Bar(
          x=plot_df['pef_over_offered'],
          y=plot_df['conversion'],
          name='Conversion',
          error_y = dict(
            type='data',
            array=plot_df['sem'],
            visible=True,
            color='white')),
    secondary_y=False
  )

  fig.add_trace(
    go.Scatter(x=plot_df['pef_over_offered'],y=plot_df['otimizacao'],name='Optimization'),
    secondary_y=True
  )

  fig.add_trace(
    go.Scatter(x=plot_df['pef_over_offered'],y=plot_df['otimizacao_ma'],name='Moving_Average'),
    secondary_y=True
  )

  fig.update_yaxes(title_text="Conversion", secondary_y=False)
  fig.update_yaxes(title_text="Optimization", secondary_y=True)

  format_figure(fig)

  fig.update_layout(
      xaxis_title="PEF sobre Offered",
      font=dict(
        color='white'
        )
  )

  fig.update_layout(
    showlegend=True,
    legend=dict(
      bordercolor='white',
      borderwidth=0.5
    )
  )

  return fig

# Call Back das Alternativas Ativas pro Experimento Selecionado

@app.callback(
  [
    Output('active_alternatives_customers', 'figure'),
    Output('active_alternatives_rpu', 'figure'),
    Output('active_alternatives_conv', 'figure'),
    Output('active_alternatives_pef_offered', 'figure')
  ],
  [
    Input('experiment_selection','value')
  ])
def update_active_alternatives(selected_experiment):

  df = df_active_alternatives
  df = df[df['fee_experiment_id']==selected_experiment]
  df = df.sort_values(by='ticket_customer',ascending=False)

  df['pef_offered'] = df['ticket_customer']/df['offered_price']

  fig_cust = go.Figure(data=[go.Bar(
                x=df['alternative'],
                y=df['customers']
  )])

  fig_rpu = go.Figure(data=[go.Bar(
                x=df['alternative'],
                y=df['rpu']
  )])  

  fig_conv = go.Figure(data=[go.Bar(
                x=df['alternative'],
                y=df['conversao']
  )])  

  ### Ticket Médio ###

  fig_tickm = go.Figure(
            data=[
                go.Bar(
                    name='Ticket Vendido',
                    x=df['alternative'],
                    y=df['ticket_medio'],
                    yaxis='y',
                    offsetgroup=1),
                go.Bar(
                    name='Ticket Ordens',
                    x=df['alternative'],
                    y=df['ticket_customer'],
                    yaxis='y',
                    offsetgroup=2),
                go.Bar(
                    name='Ticket Ordens/Offered',
                    x=df['alternative'],
                    y=df['pef_offered'],
                    yaxis='y2',
                    offsetgroup=3)
            ],
            layout = {
                    'yaxis':{'title':'Ticket Médio'},
                    'yaxis2':{'title':'Pef/Offered','overlaying':'y','side':'right'}
            }
  )

  fig_tickm.update_layout(
    barmode='group',
    font=dict(
      color='white'
      )
  )

  format_figure(fig_cust)
  format_figure(fig_rpu)
  format_figure(fig_conv)
  format_figure(fig_tickm)

  fig_tickm.update_layout(
    showlegend=True,
    legend=dict(
      x=1.10,
      y=1.0,
      bordercolor='white',
      borderwidth=0.5
    )
  )

  return fig_cust,fig_rpu,fig_conv,fig_tickm

################## Labels for Active Experiments ####################

@app.callback(
  [
    Output('label_selected_experiment_2', 'children'),
    Output('label_selected_experiment_date', 'children'),
    Output('label_selected_experiment_days', 'children')
  ],
  [
    Input('experiment_selection','value')
  ])
def update_active_alternatives_labels(selected_experiment):
  
  experiment_start_date = '-'

  if selected_experiment is None:
    selected_experiment = '-'
    experiment_start_date = '-'

  df = df_active_alternatives
  df = df[df['fee_experiment_id']==selected_experiment]
  start_date = df['test_start_date'].iloc[0]

  days_since_start = str((date.today() - timedelta(days=3)) - start_date)
  days_since_start = days_since_start.split(',')[0]
  days_since_start = days_since_start.replace('days','dias')

  experiment_label = ('Experimento Selecionado: ' + str(selected_experiment))
  experiment_start_label = ('Data de início dos testes ativos: ' + str(start_date))
  experiment_days_label = ('Dias decorridos do início: ' + str(days_since_start))
  
  return experiment_label, experiment_start_label, experiment_days_label

################## Call Back New Metric: Baseline x Tests ##################

@app.callback(
  [
    Output('new_metric_rpu_ac', 'figure'),
    Output('new_metric_rpu_mm', 'figure'),
    Output('new_metric_conv_ac', 'figure'),
    Output('new_metric_conv_mm', 'figure')
  ],
  [
    Input('experiment_selection','value')
  ])

def baseline_versus_tests_chart(selected_experiment):

  df = df_baseline_vs_tests
  df = df[df['fee_experiment_id']==selected_experiment]

# RPU ACUMULADO
  fig_rpu_ac = go.Figure()
  fig_rpu_ac.add_trace(go.Scatter(
    x=df['date'],
    y=df['rpu_baseline_ac'],
    name='RPU Baseline',
    # line_color='dimgray',
    line_color = '#18ACC4',
    opacity=1
  ))
  fig_rpu_ac.add_trace(go.Scatter(
    x=df['date'],
    y=df['rpu_testes_ac'],
    name='RPU Testes',
    # line_color='dimgray',
    line_color = 'mediumseagreen',
    opacity=1
  )) 
# RPU MÉDIA MÓVEL
  fig_rpu_mm = go.Figure()
  fig_rpu_mm.add_trace(go.Scatter(
    x=df['date'],
    y=df['rpu_baseline_mm'],
    name='RPU Baseline',
    # line_color='dimgray',
    line_color = '#18ACC4',
    opacity=1
  ))
  fig_rpu_mm.add_trace(go.Scatter(
    x=df['date'],
    y=df['rpu_testes_mm'],
    name='RPU Testes',
    # line_color='dimgray',
    line_color = 'mediumseagreen',
    opacity=1
  )) 

# CONVERSÃO ACUMULADA 
  fig_conv_ac = go.Figure()
  fig_conv_ac.add_trace(go.Scatter(
    x=df['date'],
    y=df['conversao_baseline_ac'],
    name='Conversão Baseline',
    # line_color='dimgray',
    line_color = '#18ACC4',
    opacity=1
  ))
  fig_conv_ac.add_trace(go.Scatter(
    x=df['date'],
    y=df['conversao_testes_ac'],
    name='Conversão Testes',
    # line_color='dimgray',
    line_color = 'mediumseagreen',
    opacity=1
  )) 
# CONVERSÃO MÉDIA MÓVEL 
  fig_conv_mm = go.Figure()
  fig_conv_mm.add_trace(go.Scatter(
    x=df['date'],
    y=df['conversao_baseline_mm'],
    name='Conversão Baseline',
    # line_color='dimgray',
    line_color = '#18ACC4',
    opacity=1
  ))
  fig_conv_mm.add_trace(go.Scatter(
    x=df['date'],
    y=df['conversao_testes_mm'],
    name='Conversão Testes',
    # line_color='dimgray',
    line_color = 'mediumseagreen',
    opacity=1
  )) 

  format_figure(fig_rpu_ac,showlegend=True)
  format_figure(fig_rpu_mm,showlegend=True)
  format_figure(fig_conv_ac,showlegend=True)
  format_figure(fig_conv_mm,showlegend=True)

  fig_rpu_ac.update_yaxes(rangemode="tozero")
  fig_rpu_mm.update_yaxes(rangemode="tozero") 
  fig_conv_ac.update_yaxes(rangemode="tozero")
  fig_conv_mm.update_yaxes(rangemode="tozero")

  return fig_rpu_ac, fig_rpu_mm, fig_conv_ac, fig_conv_mm

#####################################################################
#                         ALTERNATIVE CALLBACKS
#####################################################################
@app.callback(
  Output('alternative_selection', 'options'),
  [
    Input('experiment_selection','value'),
    Input('experiment-date-picker-range', 'start_date'),
    Input('experiment-date-picker-range', 'end_date')
  ])
def update_alternative_dropdown(selected_experiment,start_date,end_date):
  print('update_alternative_dropdown: ' + str(selected_experiment))

  df = get_alternatives_for_experiment(selected_experiment, start_date, end_date)


  df = df[(df['alternative']!='baseline')&(df['alternative_ratio']>0)]
  df['label'] = df.apply(
      lambda x: {
          'label': x['alternative'] + ' (' + str(x['alternative_id']) + ')',
          'value': x['alternative_id']
      },axis=1)
  print('update_alternative_dropdown: ' + str(selected_experiment) + ' FINISHED')
  return df['label'].tolist()

@app.callback(
  [
    Output('arpu_alternative', 'figure'),
    Output('probability_alternative', 'figure'),
    Output('loss_alternative', 'figure'),
    Output('offered_price_alternative', 'figure'),
    Output('price_discount_alternative', 'figure'),
    Output('customer_count_alternative', 'figure')
  ],
  [
    Input('alternative_selection','value'),
    Input('city_selection','value'),
  ],
  [
    State('experiment-date-picker-range', 'start_date'),
    State('experiment-date-picker-range', 'end_date'),
  ]
)
def update_selected_alternative(alternative_selection,selected_city,start_date,end_date):
  print('update_selected_alternative: ' + str(alternative_selection))
  number_of_outputs = 6
  if alternative_selection is None:
    return number_of_outputs*(blank_fig(ROW_HEIGHTS[2]),)

  #Setting filters
  filters = {}
  if selected_city != -1:
    filters = {'city':selected_city}

  start = time.process_time()
  #Setting BayesianABTest
  df_cum_results = get_alternative_agg_data(alternative_selection, start_date, end_date, filters)


  # hist_data = {
  #     'conversion':{
  #         'mean': .07,
  #         'std': .2
  #     },
  #     'aov':{
  #         'mean': 500,
  #         'std':100
  #     }
  # }
  hist_data = None
  unique_alternatives = df_cum_results['alternative'].unique().tolist()
  abtest = BayesianABTest('arpu',unique_alternatives, hist_data = hist_data)

  #Feeding data to the test
  for alternative in unique_alternatives:
    last_data = df_cum_results.groupby('alternative').agg('last').reset_index()

    n_visits = last_data[last_data['alternative']==alternative]['n_visits'].values[0]
    n_paids = last_data[last_data['alternative']==alternative]['n_paids'].values[0]
    revenue = last_data[last_data['alternative']==alternative]['revenue'].values[0]
    arpu = last_data[last_data['alternative']==alternative]['arpu'].values[0]

    abtest.feed_alternative_data(
        alternative,
        n_visits=n_visits,
        n_paids=n_paids,
        revenue = revenue
    )

  fig_arpu = abtest.plot_results(plotly=True)
  fig_prob2beat,fig_expLoss = abtest.plot_cumulative_results(df_cum_results,plotly=True)

  legend_colors = {}
  for i in range(len(fig_arpu.data)):
    legend_colors[fig_arpu.data[i]['legendgroup']] = fig_arpu.data[i]['marker']['color']
  format_figure(fig_arpu,showlegend=True,height = ROW_HEIGHTS[2])

  #Formatting and setting the same legend colors
  for fig in [fig_prob2beat,fig_expLoss]:
    format_figure(fig,showlegend=False, height = ROW_HEIGHTS[2])

    for i in range(len(fig.data)):
      label = fig.data[i]['legendgroup'].replace('variable=','')
      fig.data[i]['line']['color'] = legend_colors[label]

  logger.debug("Time to run BayesianABTest: " + str(time.process_time() - start))

  start = time.process_time()
  #Comparative plots
  fig_offered_price = plot_offered_price_alternative(df_cum_results,legend_colors)
  fig_price = plot_price_discount_alternative(df_cum_results)
  fig_customer_count = plot_customer_count(df_cum_results,legend_colors)
  logger.debug("Time to BayesianABTest plots: " + str(time.process_time() - start))

  return fig_arpu,fig_prob2beat,fig_expLoss,fig_offered_price,fig_price,fig_customer_count


#####################################################################
#                         PAGE PLOTS
#####################################################################
def plot_offered_price_alternative(df_cum_results,legend_colors):
  fig = px.line(df_cum_results,x = 'created_day', y = 'av_offered_price',color = 'alternative')
  format_figure(fig,showlegend=False,height = ROW_HEIGHTS[2])
  for i in range(len(fig.data)):
    label = fig.data[i]['legendgroup'].replace('alternative=','')
    fig.data[i]['line']['color'] = legend_colors[label]
  fig.update_yaxes(rangemode="tozero")
  return fig

def plot_price_discount_alternative(df_cum_results):
  alternative_name = df_cum_results[df_cum_results['alternative']!='baseline']['alternative'].unique().tolist()[0]
  df_cum_results_price = df_cum_results.reset_index().pivot(index='created_day',columns = 'alternative',values='av_price')
  df_cum_results_price['price_discount'] = 1- df_cum_results_price[alternative_name]/df_cum_results_price['baseline']
  fig = px.line(df_cum_results_price.reset_index(),x='created_day',y='price_discount')
  format_figure(fig,showlegend=False,height = ROW_HEIGHTS[2])
  fig.update_yaxes(rangemode="tozero")
  return fig

def plot_customer_count(df,legend_colors):

  alternative_name = list(legend_colors.keys()-{'baseline'})[0]
  df_customers = df.pivot(index='created_day',columns = 'alternative',values = 'customers_today').reset_index()
  df_customers['ratio']= df_customers[alternative_name]/df_customers['baseline']
  legend_colors['ratio'] = 'lightgreen'

  # Create figure with secondary y-axis
  fig = make_subplots(specs=[[{"secondary_y": True}]])

  # Add traces
  for col in ['baseline',alternative_name,'ratio']:
    fig.add_trace(go.Scatter(
            x=df_customers['created_day'].tolist(),
            y=df_customers[col].tolist(),
            name=col,
            # marker_color ="blue"
            marker_color = legend_colors[col],
            yaxis = None if col=='baseline' else ('y2' if col==alternative_name else 'y3')
    ))

  fig.update_layout(
      xaxis={
        'domain':[0.1, 0.9]
      },
      yaxis={
          'titlefont':{
            'color':legend_colors['baseline']
          },
          'tickfont':{
            'color':legend_colors['baseline']
          }
      },
      yaxis2={
        'titlefont':{
          'color':legend_colors[alternative_name]
        },
        'tickfont':{
          'color':legend_colors[alternative_name]
        },
        'anchor':"free",
        'overlaying':"y",
        'side':"left",
        'position':0.01,
        'showgrid': False
      },
      yaxis3={
          'titlefont':{
            'color':legend_colors['ratio']
          },
          'tickfont':{
            'color':legend_colors['ratio']
          },
          'anchor':"x",
          'overlaying':"y",
          'side':"right",
          'showgrid': False
      }
  )
  fig.update_yaxes(rangemode="tozero")
  format_figure(fig,showlegend=True,height = ROW_HEIGHTS[2])
  return fig
