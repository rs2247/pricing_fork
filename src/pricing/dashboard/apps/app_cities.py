#GAMBI
import sys
sys.path.append('../..')

import math
import time
import pandas as pd
import numpy as np
from datetime import date, timedelta

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
from plotly.subplots import make_subplots
import plotly.express as px

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
from pricing.dashboard.utils import *
from pricing.dashboard.data_loading import *
from pricing.dashboard.app import app
from pricing.utils import logger

#####################################################################
#                          LOADING DATA
#####################################################################

def load_page_data(from_cache=True):
  logger.debug("Loading data for app_cities ... " + str(from_cache))
  global df_daily
  global df_pef
  global df_city2ies
  global df_daily_order_pef
  global df_log_recalculate
  global limit_date

  df_daily = load_daily_share_rpu(spark,from_cache)
  df_pef = load_daily_pef(spark,from_cache)
  df_city2ies = city2ies(spark,from_cache)
  df_daily_order_pef = daily_order_pef(spark,from_cache)
  df_log_recalculate = log_recalculate(spark,from_cache)
  limit_date = []

#####################################################################
#                             LAYOUT
#####################################################################
def make_layout():
  return html.Div([
  html.Label('Modalidade'),
  html.Div([
    dcc.Dropdown(
      id = 'kind_selection',
      options=[
        {'label': 'Presencial', 'value': 'Presencial'},
        {'label': 'EaD + Semi', 'value': 'EaD + Semi'}
      ],
      value=1,
      className = "pretty_dropdown"
    )],
  style = {'width':'15%'}),
  html.Div(children=[
    html.Div(children=[
        html.H4([
            "Share x RPU em todas as praças",
        ], className="container_title"),
        dcc.Loading(
            dcc.Graph(
                id='daily_share_rpu',
                figure=blank_fig(ROW_HEIGHTS[0]),
                config={'displayModeBar': False},
            ),
            className='svg-container',
            style={'height': ROW_HEIGHTS[0]}
        ),
    ], className='twelve columns pretty_container')
  ], className = "row"),

  html.Hr(style={'margin-right':'2%','margin-top':'2%'}),

  html.Div([
    html.Div([
      html.Label('Cidade selecionada: - ', id='label_selected_city' ),
    ],className= 'six columns'),
    html.Div([
      html.A(
        children=html.Button('Abrir cidade no ppa',style={'background':'#7D8C93','color':'#fff'},id='ppa_button'),
        href='http://ppa.querobolsa.space/dashboard',
        id='ppa_link',
        target="_blank"),
    ],className = "six columns", style={'text-align':'right'}),
  ],className = 'row',style={'margin-left': 0, 'margin-right': '2%'}),

  html.Div(children=[
    html.Div(children=[
        html.H6([
            "Evolução diária de Share x RPU",
        ], className="container_title"),
        dcc.Loading(
            dcc.Graph(
                id='share_rpu',
                figure=blank_fig(ROW_HEIGHTS[1]),
                config={'displayModeBar': False},
            ),
            className='svg-container',
            style={'height': ROW_HEIGHTS[1]}
        ),
    ], className='six columns pretty_container'),
    html.Div(children=[
        html.H6([
            "Evolução Diária do PEF",
        ], className="container_title"),
        dcc.Loading(
            dcc.Graph(
                id='daily_order_pef',
                figure=blank_fig(ROW_HEIGHTS[1])
                # config={'displayModeBar': False},
            ),
            className='svg-container',
            style={'height': ROW_HEIGHTS[1]}
        )
    ], className='six columns pretty_container'),
  ], className = "row",style={'margin-top':'2%'}),

  html.Div(children=[
    html.Div(children=[
        html.H6([
            "IES mais relevantes na praça",
        ], className="container_title"),
        dcc.Loading(
            dcc.Graph(
                id='city2ies',
                figure=blank_fig(ROW_HEIGHTS[1]),
                config={'displayModeBar': False},
            ),
            className='svg-container',
            style={'height': ROW_HEIGHTS[1]}
        )

    ], className='six columns pretty_container'),
    html.Div(children=[
        html.H6([
            "Log recalculates (# de skus com alterações)",
        ], className="container_title"),
        dcc.Loading(
            dcc.Graph(
                id='log_recalculate',
                figure=blank_fig(ROW_HEIGHTS[1]),
                config={'displayModeBar': False},
            ),
            className='svg-container',
            style={'height': ROW_HEIGHTS[1]}
        )

    ], className='six columns pretty_container'),
  ], className="row")

],className = 'plots_body')


#####################################################################
#                             CALLBACKS
#####################################################################
@app.callback(
  Output('label_selected_city', 'children'),
  [Input('daily_share_rpu','clickData')])
def update_selected_city_label(selected_city):
  logger.debug(selected_city)
  # print(selected_city)

  if selected_city is None:
    selected_city = '-'
  else:
    selected_city = selected_city['points'][0]['id']

  return 'Cidade selecionada: ' + selected_city

@app.callback(
  Output('daily_share_rpu', 'figure'),
  [Input('kind_selection', 'value')])

def update_daily_share_rpu(selected_kind):

  # logger.debug("Updating daily share ")
  # print(" >>>>>> WARNING !!! >>>>> SLEEPING FOREVER")
  # import time
  # time.sleep(1000)


  #Scaling by passing the revenue through a sigmoid function
  def sigmoid(x,scale):
      return 1 / (1 + math.exp(-(x/scale-2.5)) )

  k=4
  scale = 100000 #100000 is suitable for 'receita_ltv_acumulada'
  df_daily['city_revenue'] = df_daily.groupby('city')['receita_ltv_acumulada'].transform('mean')
  df_daily['size'] = df_daily['city_revenue'].apply(lambda x: sigmoid(x,k*scale))

  df_daily_lk = df_daily[(df_daily.level == 'Graduação')]
  df_daily_lk = df_daily_lk[(df_daily_lk.kind == selected_kind)]

  if selected_kind == 'Presencial' or selected_kind == 'EaD + Semi': 
    fig = px.scatter(
      df_daily_lk,
      x="delta_rpu_20_19",
      y="delta_pagos_20_19",
      animation_frame="dia_ano",
      animation_group="city",
      size="size",
      color="state",
      hover_name="city",
      hover_data=["state"]
      )#,

    fig.update_xaxes(
      zeroline=True,
      zerolinewidth=2,
      zerolinecolor='darkgray'
    )

    fig.update_yaxes(
      zeroline=True,
      zerolinewidth=2,
      zerolinecolor='darkgray'
    )

    new_updatemenus = fig['layout']['updatemenus']
    new_updatemenus[0]['bgcolor'] = 'white'

    new_sliders = fig['layout']['sliders']
    new_sliders[0]['font']['color'] = 'white'
    new_sliders[0]['active'] = 0

    fig.update_layout(
      height=ROW_HEIGHTS[0],
      paper_bgcolor=PLOT_BGCOLOR,
      plot_bgcolor=PLOT_BGCOLOR,
      xaxis = {'color':"#ffffff",'gridcolor':"#1F2426"},
      yaxis = {'color':"#ffffff",'gridcolor':"#1F2426"},
      margin=  {'l': 5, 'r': 5, 't': 5, 'b': 5},
      legend= {
        'font':{
          'color':"white"}},
      updatemenus= new_updatemenus,
      sliders = new_sliders
    )

    return fig
  else:
    return blank_fig(ROW_HEIGHTS[1])
      

@app.callback(
  Output('share_rpu', 'figure'),

  [Input('daily_share_rpu','clickData'),
   Input('kind_selection', 'value')])

def update_share_rpu(selected_city,selected_kind):

  if selected_city is None:
    return blank_fig(ROW_HEIGHTS[1])

  selected_city = selected_city['points'][0]['id']

  df_city = df_daily[df_daily['city']==selected_city]
  df_city_lk = df_city[df_city['level']=='Graduação']
  df_city_lk = df_city_lk[df_city_lk['kind']==selected_kind]

  fig = px.scatter(
      df_city_lk,
      x = 'delta_rpu_20_19',
      y = 'delta_pagos_20_19',
      # size = 'receita_20',
      color = 'dia_ano',
      color_continuous_scale='Greys',
      hover_data= [
        'dia'
      ],
      # template = 'ggplot2'
  )
  def include_axis(ax,df,column):
    '''
      ax: axis -> 'x' or 'y'
    '''
    ax_min = df[column].min()
    ax_max = df[column].max()

    if ax_min*ax_max >0:
      if ax_min < 0:
        ax_range = [1.2*ax_min,0 + abs(ax_max)*.2]
      else:
        ax_range = [0 - abs(ax_min)*.2 ,1.2*abs(ax_max)]

      if ax=='x':
        fig.update_xaxes(range=ax_range)
      elif ax=='y':
        fig.update_yaxes(range=ax_range)
  include_axis('x',df_city,'delta_rpu_20_19')
  include_axis('y',df_city,'delta_pagos_20_19')


  fig.update_xaxes(zeroline=True, zerolinewidth=2, zerolinecolor='darkgray')
  fig.update_yaxes(zeroline=True, zerolinewidth=2, zerolinecolor='darkgray')

    #Adding annotations
  annotations = []
  df_annon = df_city_lk[pd.isna(df_city_lk['pricing_log'])==False]
  if df_annon.shape[0]!=0:
    for i in range(df_annon.shape[0]):
      row = df_annon.iloc[i]
      annotations.append(go.layout.Annotation(
        x = row['delta_rpu_20_19'],
        y = row['delta_pagos_20_19'],
        yref = 'y',
        text = row['pricing_log'],
        showarrow=True,
        arrowhead=7,
        ax=0,
        ay=-40,
        font = {
          'color': "#ffffff"
        },
        arrowcolor = "#ffffff"
        # bordercolor = "#ffffff"

    ))

    fig.update_layout(annotations= annotations)


  # fig.update_layout(showlegend=False,height=ROW_HEIGHTS[1])
  fig.update_layout(
    showlegend=False,
    height=ROW_HEIGHTS[1],
    paper_bgcolor=PLOT_BGCOLOR,
    plot_bgcolor=PLOT_BGCOLOR,
    xaxis = {'color':"#ffffff",'gridcolor':"#1F2426"},
    yaxis = {'color':"#ffffff",'gridcolor':"#1F2426"},
    margin=  {'l': 10, 'r': 10, 't': 30, 'b': 10},
  )

  return fig

@app.callback(
  Output('daily_pef', 'figure'),
  [Input('daily_share_rpu','clickData'),
   Input('kind_selection', 'value')])



@app.callback(
    Output(component_id='city2ies',component_property='figure'),
    [Input(component_id='daily_share_rpu',component_property='clickData'),
     Input('kind_selection', 'value')])

def update_city2ies(selected_city,selected_kind):

  logger.debug("Updating city2ies")

  if selected_city is None:
    return blank_fig(ROW_HEIGHTS[1])

  selected_city = selected_city['points'][0]['id'].lower()
  df_city2ies['normalized_city'] = df_city2ies['city'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.lower()
  df_city = df_city2ies[df_city2ies['normalized_city']==selected_city]
  df_city = df_city[df_city['kind']==selected_kind]

  if df_city.shape[0]==0:
    logger.warning("Warninggg no city: ", selected_city)

  fig = go.Figure(
    data=[go.Bar(
      x=df_city['name'],
      y=df_city['relevance_city'])
    ]
  )

  fig.update_traces(
    marker_color='rgb(105,105,105)',
    marker_line_color='rgb(0,0,0)',
    marker_line_width=1.5, opacity=0.8
  )

  fig.update_layout(
    height=ROW_HEIGHTS[1],
    paper_bgcolor=PLOT_BGCOLOR,
    plot_bgcolor=PLOT_BGCOLOR,
    xaxis = {'color':"#ffffff",'gridcolor':"#1F2426"},
    yaxis = {'color':"#ffffff",'gridcolor':"#1F2426"},
    margin=  {'l': 10, 'r': 10, 't': 30, 'b': 10}
  )

  return fig

@app.callback(
    Output(component_id='daily_order_pef',component_property='figure'),
    [Input(component_id='daily_share_rpu',component_property='clickData'),
    Input('kind_selection', 'value')])
def update_daily_order_pef(selected_city,selected_kind):

  logger.debug("Updating daily_order_pef")

  if selected_city is None:
    return blank_fig(ROW_HEIGHTS[1])

  selected_city = selected_city['points'][0]['id'].lower()
  df_daily_order_pef['normalized_city'] = df_daily_order_pef['city'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.lower()
  df_city = df_daily_order_pef[df_daily_order_pef['normalized_city']==selected_city]
  df_city = df_city[df_city['kind']==selected_kind]

  df_pef['normalized_city'] = df_pef['city'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.lower()
  df_city_2 = df_pef[df_pef['normalized_city']==selected_city]
  df_city_2 = df_city_2[df_city_2['kind']==selected_kind]


  if df_city.shape[0]==0:
    logger.warning("Warninggg no city: ", selected_city)

  fig = go.Figure()
  fig.add_trace(go.Scatter(
    x=df_city['date'],
    y=df_city['value'],
    name='orders',
    # line_color='dimgray',
    line_color = 'darkorange',
    opacity=1
  ))

  fig.add_trace(go.Scatter(
    x=df_city_2['date'],
    y=df_city_2['pef_desconto'],
    name='desconto_campanha',
    # line_color='dimgray',
    line_color = 'mediumseagreen',
    opacity=1
  ))

  fig.add_trace(go.Scatter(
    x=df_city_2['date'],
    y=df_city_2['value'],
    name='oferecido',
    # line_color='dimgray',
    line_color = '#18ACC4',
    opacity=1
  ))


  fig.update_yaxes(rangemode="tozero")

  fig.update_layout(
    height=ROW_HEIGHTS[1],
    paper_bgcolor=PLOT_BGCOLOR,
    plot_bgcolor=PLOT_BGCOLOR,
    xaxis = {'color':"#ffffff",'gridcolor':"#1F2426"},
    yaxis = {'color':"#ffffff",'gridcolor':"#1F2426"},
    margin= {'l': 10, 'r': 10, 't': 30, 'b': 10},
    legend=dict(
      x=0.7,
      y=0.1,
      bgcolor=PLOT_BGCOLOR,
      bordercolor="white",
      borderwidth=2,
      font=dict(
        color = "white"
      )
    ),
  )

  return fig

@app.callback(
  Output(component_id='log_recalculate',component_property='figure'),
    [Input(component_id='daily_share_rpu',component_property='clickData'),
     Input(component_id='kind_selection', component_property='value')])
def update_log_recalculates(selected_city,selected_kind): #atualização de dados

  logger.debug("Updating log recalculates")

  if selected_city is None:
    return blank_fig(ROW_HEIGHTS[1])

  selected_city = selected_city['points'][0]['id'].lower()
  df_daily_order_pef['normalized_city'] = df_daily_order_pef['city'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.lower()
  df_city_aux = df_daily_order_pef[df_daily_order_pef['normalized_city']==selected_city]
  df_city_aux = df_city_aux[df_city_aux['kind']==selected_kind]

  df_log_recalculate['normalized_city'] = df_log_recalculate['city'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.lower()
  df_city = df_log_recalculate[df_log_recalculate['normalized_city']==selected_city]
  df_city = df_city[df_city['kind']==selected_kind]
  df_city = df_city[df_city['dia'] >= min(df_city_aux['date'])]
  df_city = df_city[df_city['dia'] <= max(df_city_aux['date'])]
  

  if df_city.shape[0]==0:
    logger.warning("Warninggg no city: ", selected_city)

  plot_data=[]
  ies_names=df_city['ies'].unique().tolist()
  for ies_name in ies_names:
    df_city_ies=df_city.loc[df_city['ies']==ies_name]
    plot_data.append(go.Bar(
     x=df_city_ies['dia'],
     y=df_city_ies['qtde'],
     name=ies_name))

  fig = go.Figure(
    data=plot_data
  )


  fig.update_traces(
    marker_line_width=1.5, opacity=0.8,
  )

  fig.update_layout(
    height=ROW_HEIGHTS[1],
    paper_bgcolor=PLOT_BGCOLOR,
    plot_bgcolor=PLOT_BGCOLOR,
    xaxis = {'color':"#ffffff",'gridcolor':"#1F2426"},
    yaxis = {'color':"#ffffff",'gridcolor':"#1F2426"},
    margin=  {'l': 10, 'r': 10, 't': 30, 'b': 10},
    barmode= 'stack',
    showlegend = False

  )

  return fig

@app.callback(
  Output(component_id='ppa_link',component_property='href'),
  [Input(component_id ='daily_share_rpu',component_property='clickData')])
def ppa_link_converter(selected_data):
  if selected_data is None:
    return 'http://ppa.querobolsa.space/dashboard'

  selected_city = selected_data['points'][0]['id']
  selected_state = selected_data['points'][0]['customdata'][0]

  return get_ppa_link(selected_city,selected_state)
