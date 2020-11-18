'''
TODO:
  - ABTests
  - Level/kind selection
  - Break the app in multiple "pages"

'''
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
# from pricing.dashboard import *
from pricing.dashboard.utils import *
from pricing.dashboard.data_loading import *
from pricing.dashboard.app import app
from pricing.utils import logger

#####################################################################
#                          LOADING DATA
#####################################################################
def load_page_data(from_cache=True):
  logger.debug("Loading data for app_goals ...")
  global df_goals_only_qb_time_series,df_ies_categorization,df_campaigns,df_goals_only_qb_time_series_summer

  df_goals_only_qb_time_series = rpu_goals_only_qb_time_series(spark,from_cache)
  df_ies_categorization = ies_categorization(spark,from_cache)
  df_campaigns = campaigns(spark,from_cache)
  df_goals_only_qb_time_series_summer = rpu_goals_only_qb_time_series_summer(spark,from_cache)

  #return df_goals,df_goals_only_qb,df_goals_only_qb_time_series,df_ies_categorization,df_campaigns

#df_goals,df_goals_only_qb,df_goals_only_qb_time_series,df_ies_categorization,df_campaigns = load_page_data()

###################### AUTOMATIC LAYOUTS ##############################

  # return df_goals

#####################################################################
#                             LAYOUT
#####################################################################


def bar_chart_layout(fig,height = ROW_HEIGHTS[1]):

  fig.update_traces(
  #marker_color='darkorange',
  marker_line_color='rgb(0,0,0)',
  marker_line_width=1.5, opacity=0.7
  )

  fig.update_layout(
  height=height,
  paper_bgcolor=PLOT_BGCOLOR,
  plot_bgcolor=PLOT_BGCOLOR,
  xaxis = {'color':"#ffffff",'gridcolor':"#1F2426"},
  yaxis = {'color':"#ffffff",'gridcolor':"#1F2426"},
  margin=  {'l': 10, 'r': 10, 't': 30, 'b': 10},
    legend=dict(
      x=0.85,
      y=1,
      bgcolor=PLOT_BGCOLOR,
      bordercolor="white",
      borderwidth=2,
      font=dict(
        color = "white"
      )
    )
  )
  fig.update_layout(barmode='group')

def time_series_layout(fig,height = ROW_HEIGHTS[1]): 
  
  fig.update_yaxes(rangemode="tozero")

  fig.update_layout(
    height=ROW_HEIGHTS[1],
    paper_bgcolor=PLOT_BGCOLOR,
    plot_bgcolor=PLOT_BGCOLOR,
    xaxis = {'color':"#ffffff",'gridcolor':"#1F2426"},
    yaxis = {'color':"#ffffff",'gridcolor':"#1F2426"},
    margin= {'l': 10, 'r': 10, 't': 30, 'b': 10},
    legend=dict(
      x=0.75,
      y=0.03,
      bgcolor=PLOT_BGCOLOR,
      bordercolor="white",
      borderwidth=2,
      font=dict(
        color = "white"
      )
    )
  )

######################### ADDING CAMPAIGN DATES ################################

def adding_campaigns(fig):

  max_values = []
  
  for i in range(len(fig['data'])):
    
    if len(fig['data'][i]['y']) == 0:
      max_values.append(0) 
    else:
      max_values.append(max(fig['data'][i]['y']))
  
  maximum = max(max_values)   

  campaigns_dict = df_campaigns.T.to_dict()

  colors = {0:'white',
            1:'darkorange',
            2:'yellow'}

  for key in colors:

    campaigns_dict[key]['color'] = colors[key]

  for key in campaigns_dict:
      fig.add_trace(go.Scatter(
        x=[campaigns_dict[key]['start'],campaigns_dict[key]['start']],
        y=[0,maximum],
        name=campaigns_dict[key]['offer_campaign_flag'],
        # line_color='dimgray',
        line_color = campaigns_dict[key]['color'],
        line=dict(
          dash='dot'),
        opacity=0.6
      ))

      fig.add_trace(go.Scatter(
        x=[campaigns_dict[key]['end'],campaigns_dict[key]['end']],
        y=[0,maximum],
        # line_color='dimgray',
        line_color = campaigns_dict[key]['color'],
        line=dict(
          dash='dot'),
        opacity=0.6,
        showlegend=False
      ))
######################### CONSOLIDATED CHARTS ##################################

def rpu_yoy_grad():

  filter_date = date.today() - timedelta(days=4)
  df_consolidated_rpu = df_goals_only_qb_time_series[df_goals_only_qb_time_series['date']==filter_date]
  df_rpu_grad = df_consolidated_rpu[df_consolidated_rpu['level']=='Graduação']

  df_rpu_grad = df_rpu_grad.sort_values(by='rpu_ac_20',ascending=False)
  yoy = round((df_rpu_grad['rpu_ac_20']/df_rpu_grad['rpu_ac_19']-1),2)

  fig = go.Figure(
    data=[go.Bar(
    name = 'RPU 19.2',
    x = df_rpu_grad['kind'],
    y = df_rpu_grad['rpu_ac_19']
    ),
    go.Bar(
    name = 'RPU 20.2',
    x = df_rpu_grad['kind'],
    y = df_rpu_grad['rpu_ac_20'],
    text = yoy,
    textposition = 'inside'
    )
    ]
  )

  bar_chart_layout(fig)

  return fig


def rpu_yoy_pos():

  filter_date = date.today() - timedelta(days=4)
  df_consolidated_rpu = df_goals_only_qb_time_series[df_goals_only_qb_time_series['date']==filter_date]
  df_rpu_pos = df_consolidated_rpu[df_consolidated_rpu['level']=='Pós-graduação']  

  df_rpu_pos = df_rpu_pos.sort_values(by='rpu_ac_20',ascending=False) 
  yoy = round((df_rpu_pos['rpu_ac_20']/df_rpu_pos['rpu_ac_19']-1),2) 

  fig = go.Figure(
    data=[go.Bar(
    name = 'RPU 19.2',
    x = df_rpu_pos['kind'],
    y = df_rpu_pos['rpu_ac_19']
    ),
    go.Bar(
    name = 'RPU 20.2',
    x = df_rpu_pos['kind'],
    y = df_rpu_pos['rpu_ac_20'],
    text = yoy,
    textposition = 'inside'
    )
    ]
  )

  bar_chart_layout(fig)

  return fig

###################### TIME SERIES CALL BACKS #####################################

@app.callback(
  [
  Output(component_id='fig_rpu_mm',component_property='figure'),
  Output(component_id='fig_rpu_ac',component_property='figure'),
  Output(component_id='fig_ss_ac',component_property='figure'),
  Output(component_id='fig_ss_mm',component_property='figure'),
  Output(component_id='fig_tm_mm',component_property='figure'),
  Output(component_id='fig_tm_ac',component_property='figure'),
  ],

  [Input(component_id='level_selection',component_property='value'),
   Input(component_id='kind_selection',component_property='value'),
   Input(component_id='period_selection',component_property='value')
  ])

def update_selected_level(selected_level,selected_kind,selected_period):

  fig_rpu_mm,fig_rpu_ac,fig_ss_ac,fig_ss_mm,fig_tm_mm,fig_tm_ac = plot_time_series(selected_level,selected_kind,selected_period)

  return fig_rpu_mm,fig_rpu_ac,fig_ss_ac,fig_ss_mm,fig_tm_mm,fig_tm_ac

def rpu_time_series_mm(selected_level,selected_kind,selected_period):
  
  if selected_level is None:
    return blank_fig(ROW_HEIGHTS[1])

  if selected_period is None:
    return blank_fig(ROW_HEIGHTS[1])

  if selected_period == '2020.1':
    df_goals_level_kind_time_series = df_goals_only_qb_time_series_summer[df_goals_only_qb_time_series_summer['level_kind'] == '{} {}'.format(selected_level,selected_kind)]
  elif selected_period == '2020.2':
    df_goals_level_kind_time_series = df_goals_only_qb_time_series[df_goals_only_qb_time_series['level_kind'] == '{} {}'.format(selected_level,selected_kind)]
  else:
    return blank_fig(ROW_HEIGHTS[1])

  fig = go.Figure()
  fig.add_trace(go.Scatter(
    x=df_goals_level_kind_time_series['date'],
    y=df_goals_level_kind_time_series['rpu_mm_19'],
    name='RPU 19',
    # line_color='dimgray',
    line_color = '#18ACC4',
    opacity=1
  ))

  fig.add_trace(go.Scatter(
    x=df_goals_level_kind_time_series['date'],
    y=df_goals_level_kind_time_series['rpu_mm_20'],
    name='RPU 20',
    # line_color='dimgray',
    line_color = 'mediumseagreen',
    opacity=1
  ))

  fig.add_trace(go.Scatter(
    x=df_goals_level_kind_time_series['date'],
    y=df_goals_level_kind_time_series['rpu_mm_goal'],
    name='Meta RPU',
    # line_color='dimgray',
    line_color = 'red',
    opacity=1
  )) 

  # adding_campaigns(fig)

  time_series_layout(fig)

  return fig

def rpu_time_series_ac(selected_level,selected_kind,selected_period):
  
  if selected_level is None:
    return blank_fig(ROW_HEIGHTS[1])

  if selected_period is None:
    return blank_fig(ROW_HEIGHTS[1])

  if selected_period == '2020.1':
    df_goals_level_kind_time_series = df_goals_only_qb_time_series_summer[df_goals_only_qb_time_series_summer['level_kind'] == '{} {}'.format(selected_level,selected_kind)]
  elif selected_period == '2020.2':
    df_goals_level_kind_time_series = df_goals_only_qb_time_series[df_goals_only_qb_time_series['level_kind'] == '{} {}'.format(selected_level,selected_kind)]
  else:
    return blank_fig(ROW_HEIGHTS[1])

  fig = go.Figure()
  fig.add_trace(go.Scatter(
    x=df_goals_level_kind_time_series['date'],
    y=df_goals_level_kind_time_series['rpu_ac_19'],
    name='RPU 19',
    # line_color='dimgray',
    line_color = '#18ACC4',
    opacity=1
  ))

  fig.add_trace(go.Scatter(
    x=df_goals_level_kind_time_series['date'],
    y=df_goals_level_kind_time_series['rpu_ac_20'],
    name='RPU 20',
    # line_color='dimgray',
    line_color = 'mediumseagreen',
    opacity=1
  ))

  fig.add_trace(go.Scatter(
    x=df_goals_level_kind_time_series['date'],
    y=df_goals_level_kind_time_series['rpu_ac_goal'],
    name='Meta RPU',
    # line_color='dimgray',
    line_color = 'red',
    opacity=1
  )) 

  # adding_campaigns(fig)

  time_series_layout(fig)

  return fig

def sucesso_time_series_mm(selected_level,selected_kind,selected_period):
  
  if selected_level is None:
    return blank_fig(ROW_HEIGHTS[1])

  if selected_period is None:
    return blank_fig(ROW_HEIGHTS[1])

  if selected_period == '2020.1':  
    df_goals_level_kind_time_series = df_goals_only_qb_time_series_summer[df_goals_only_qb_time_series_summer['level_kind'] == '{} {}'.format(selected_level,selected_kind)]
    df_goals_level_kind_time_series['ss_20_mm'] = df_goals_level_kind_time_series['pagos_mm_20']/df_goals_level_kind_time_series['usuarios_mm_20']
    df_goals_level_kind_time_series['ss_19_mm'] = df_goals_level_kind_time_series['pagos_mm_19']/df_goals_level_kind_time_series['usuarios_mm_19']
  elif selected_period == '2020.2':
    df_goals_level_kind_time_series = df_goals_only_qb_time_series[df_goals_only_qb_time_series['level_kind'] == '{} {}'.format(selected_level,selected_kind)]
    df_goals_level_kind_time_series['ss_20_mm'] = df_goals_level_kind_time_series['pagos_mm_20']/df_goals_level_kind_time_series['usuarios_mm_20']
    df_goals_level_kind_time_series['ss_19_mm'] = df_goals_level_kind_time_series['pagos_mm_19']/df_goals_level_kind_time_series['usuarios_mm_19'] 
  else:
    return blank_fig(ROW_HEIGHTS[1])

  fig = go.Figure()
  fig.add_trace(go.Scatter(
    x=df_goals_level_kind_time_series['date'],
    y=df_goals_level_kind_time_series['ss_19_mm'],
    name='Sucesso 19',
    # line_color='dimgray',
    line_color = '#18ACC4',
    opacity=1
  ))

  fig.add_trace(go.Scatter(
    x=df_goals_level_kind_time_series['date'],
    y=df_goals_level_kind_time_series['ss_20_mm'],
    name='Sucesso 20',
    # line_color='dimgray',
    line_color = 'mediumseagreen',
    opacity=1
  ))

  # adding_campaigns(fig)

  time_series_layout(fig)

  return fig

def sucesso_time_series_ac(selected_level,selected_kind,selected_period):
  
  if selected_level is None:
    return blank_fig(ROW_HEIGHTS[1])

  if selected_period is None:
    return blank_fig(ROW_HEIGHTS[1])

  if selected_period == '2020.1':
    df_goals_level_kind_time_series = df_goals_only_qb_time_series_summer[df_goals_only_qb_time_series_summer['level_kind'] == '{} {}'.format(selected_level,selected_kind)]
    df_goals_level_kind_time_series['ss_20_ac'] = df_goals_level_kind_time_series['pagos_ac_20']/df_goals_level_kind_time_series['usuarios_ac_20']
    df_goals_level_kind_time_series['ss_19_ac'] = df_goals_level_kind_time_series['pagos_ac_19']/df_goals_level_kind_time_series['usuarios_ac_19']
  elif selected_period == '2020.2':
    df_goals_level_kind_time_series = df_goals_only_qb_time_series[df_goals_only_qb_time_series['level_kind'] == '{} {}'.format(selected_level,selected_kind)]
    df_goals_level_kind_time_series['ss_20_ac'] = df_goals_level_kind_time_series['pagos_ac_20']/df_goals_level_kind_time_series['usuarios_ac_20']
    df_goals_level_kind_time_series['ss_19_ac'] = df_goals_level_kind_time_series['pagos_ac_19']/df_goals_level_kind_time_series['usuarios_ac_19']
  else:
    return blank_fig(ROW_HEIGHTS[1])  

  fig = go.Figure()
  fig.add_trace(go.Scatter(
    x=df_goals_level_kind_time_series['date'],
    y=df_goals_level_kind_time_series['ss_19_ac'],
    name='Sucesso 19',
    # line_color='dimgray',
    line_color = '#18ACC4',
    opacity=1
  ))

  fig.add_trace(go.Scatter(
    x=df_goals_level_kind_time_series['date'],
    y=df_goals_level_kind_time_series['ss_20_ac'],
    name='Sucesso 20',
    # line_color='dimgray',
    line_color = 'mediumseagreen',
    opacity=1
  ))

  # adding_campaigns(fig)

  time_series_layout(fig)

  return fig

def ticket_time_series_ac(selected_level,selected_kind,selected_period):
  
  if selected_level is None:
    return blank_fig(ROW_HEIGHTS[1])

  if selected_period is None:
    return blank_fig(ROW_HEIGHTS[1])

  if selected_period == '2020.1':
    df_goals_level_kind_time_series = df_goals_only_qb_time_series_summer[df_goals_only_qb_time_series_summer['level_kind'] == '{} {}'.format(selected_level,selected_kind)]
    df_goals_level_kind_time_series['tm_20_ac'] = df_goals_level_kind_time_series['receita_ac_20']/df_goals_level_kind_time_series['pagos_ac_20']
    df_goals_level_kind_time_series['tm_19_ac'] = df_goals_level_kind_time_series['receita_ac_19']/df_goals_level_kind_time_series['pagos_ac_19']
  elif selected_period == '2020.2':
    df_goals_level_kind_time_series = df_goals_only_qb_time_series[df_goals_only_qb_time_series['level_kind'] == '{} {}'.format(selected_level,selected_kind)]
    df_goals_level_kind_time_series['tm_20_ac'] = df_goals_level_kind_time_series['receita_ac_20']/df_goals_level_kind_time_series['pagos_ac_20']
    df_goals_level_kind_time_series['tm_19_ac'] = df_goals_level_kind_time_series['receita_ac_19']/df_goals_level_kind_time_series['pagos_ac_19']
  else:
    return blank_fig(ROW_HEIGHTS[1])       

  fig = go.Figure()
  fig.add_trace(go.Scatter(
    x=df_goals_level_kind_time_series['date'],
    y=df_goals_level_kind_time_series['tm_19_ac'],
    name='Ticket Médio 19',
    # line_color='dimgray',
    line_color = '#18ACC4',
    opacity=1
  ))

  fig.add_trace(go.Scatter(
    x=df_goals_level_kind_time_series['date'],
    y=df_goals_level_kind_time_series['tm_20_ac'],
    name='Ticket Médio 20',
    # line_color='dimgray',
    line_color = 'mediumseagreen',
    opacity=1
  ))

  # adding_campaigns(fig)

  time_series_layout(fig)

  return fig

def ticket_time_series_mm(selected_level,selected_kind,selected_period):
  
  if selected_level is None:
    return blank_fig(ROW_HEIGHTS[1])

  if selected_period is None:
    return blank_fig(ROW_HEIGHTS[1])  
  
  if selected_period == '2020.1':
    df_goals_level_kind_time_series = df_goals_only_qb_time_series_summer[df_goals_only_qb_time_series_summer['level_kind'] == '{} {}'.format(selected_level,selected_kind)]
    df_goals_level_kind_time_series['tm_20_mm'] = df_goals_level_kind_time_series['receita_mm_20']/df_goals_level_kind_time_series['pagos_mm_20']
    df_goals_level_kind_time_series['tm_19_mm'] = df_goals_level_kind_time_series['receita_mm_19']/df_goals_level_kind_time_series['pagos_mm_19']
  elif selected_period == '2020.2':
    df_goals_level_kind_time_series = df_goals_only_qb_time_series[df_goals_only_qb_time_series['level_kind'] == '{} {}'.format(selected_level,selected_kind)]
    df_goals_level_kind_time_series['tm_20_mm'] = df_goals_level_kind_time_series['receita_mm_20']/df_goals_level_kind_time_series['pagos_mm_20']
    df_goals_level_kind_time_series['tm_19_mm'] = df_goals_level_kind_time_series['receita_mm_19']/df_goals_level_kind_time_series['pagos_mm_19']    
  else:
    return blank_fig(ROW_HEIGHTS[1])

  fig = go.Figure()
  fig.add_trace(go.Scatter(
    x=df_goals_level_kind_time_series['date'],
    y=df_goals_level_kind_time_series['tm_19_mm'],
    name='Ticket Médio 19',
    # line_color='dimgray',
    line_color = '#18ACC4',
    opacity=1
  ))

  fig.add_trace(go.Scatter(
    x=df_goals_level_kind_time_series['date'],
    y=df_goals_level_kind_time_series['tm_20_mm'],
    name='Ticket Médio 20',
    # line_color='dimgray',
    line_color = 'mediumseagreen',
    opacity=1
  ))

  # adding_campaigns(fig)

  time_series_layout(fig)

  return fig


def rpu_goals_only_qb_time_series_chart_ac_grad(selected_level):

  df_goals_level_time_series = df_goals_only_qb_time_series[df_goals_only_qb_time_series['level'] == selected_level]
  df_goals_level_time_series = (df_goals_level_time_series.groupby(['date','level'])['receita_ac_19','receita_ac_20','usuarios_ac_19','usuarios_ac_20','receita_mm_19','receita_mm_20','usuarios_mm_19','usuarios_mm_20'].agg('sum')).reset_index()
  df_goals_level_time_series['rpu_ac_20'] = df_goals_level_time_series['receita_ac_20']/df_goals_level_time_series['usuarios_ac_20']
  df_goals_level_time_series['rpu_ac_19'] = df_goals_level_time_series['receita_ac_19']/df_goals_level_time_series['usuarios_ac_19']
  df_goals_level_time_series['rpu_ac_goal'] = df_goals_level_time_series['rpu_ac_19']*1.075

  fig = go.Figure()
  fig.add_trace(go.Scatter(
    x=df_goals_level_time_series['date'],
    y=df_goals_level_time_series['rpu_ac_19'],
    name='RPU 19',
    # line_color='dimgray',
    line_color = '#18ACC4',
    opacity=1
  ))

  fig.add_trace(go.Scatter(
    x=df_goals_level_time_series['date'],
    y=df_goals_level_time_series['rpu_ac_20'],
    name='RPU 20',
    # line_color='dimgray',
    line_color = 'mediumseagreen',
    opacity=1
  )) 

  fig.add_trace(go.Scatter(
    x=df_goals_level_time_series['date'],
    y=df_goals_level_time_series['rpu_ac_goal'],
    name='Meta RPU',
    # line_color='dimgray',
    line_color = 'red',
    opacity=1
  )) 

  # adding_campaigns(fig)

  time_series_layout(fig)

  return fig


def rpu_goals_only_qb_time_series_chart_mm_grad(selected_level):

  df_goals_level_time_series = df_goals_only_qb_time_series[df_goals_only_qb_time_series['level'] == selected_level]
  df_goals_level_time_series = (df_goals_level_time_series.groupby(['date','level'])['receita_ac_19','receita_ac_20','usuarios_ac_19','usuarios_ac_20','receita_mm_19','receita_mm_20','usuarios_mm_19','usuarios_mm_20'].agg('sum')).reset_index()
  df_goals_level_time_series['rpu_mm_20'] = df_goals_level_time_series['receita_mm_20']/df_goals_level_time_series['usuarios_mm_20']
  df_goals_level_time_series['rpu_mm_19'] = df_goals_level_time_series['receita_mm_19']/df_goals_level_time_series['usuarios_mm_19']
  df_goals_level_time_series['rpu_mm_goal'] = df_goals_level_time_series['rpu_mm_19']*1.075

  fig = go.Figure()
  fig.add_trace(go.Scatter(
    x=df_goals_level_time_series['date'],
    y=df_goals_level_time_series['rpu_mm_19'],
    name='RPU 19',
    # line_color='dimgray',
    line_color = '#18ACC4',
    opacity=1
  ))

  fig.add_trace(go.Scatter(
    x=df_goals_level_time_series['date'],
    y=df_goals_level_time_series['rpu_mm_20'],
    name='RPU 20',
    # line_color='dimgray',
    line_color = 'mediumseagreen',
    opacity=1
  )) 

  fig.add_trace(go.Scatter(
    x=df_goals_level_time_series['date'],
    y=df_goals_level_time_series['rpu_mm_goal'],
    name='Meta RPU',
    # line_color='dimgray',
    line_color = 'red',
    opacity=1
  )) 

  # adding_campaigns(fig)

  time_series_layout(fig)

  return fig

def ies_categorization_table():

  fig = go.Figure(data=[go.Table(
    header=dict(values=['ies',
                       'level',
                       'kind',
                       'categoria',
                       'receita_total'],
      fill_color='lightseagreen',
      align='left',
      font=dict(color='white', size=14)),
    cells=dict(values=[df_ies_categorization.ies,
                      df_ies_categorization.level,
                      df_ies_categorization.kind,
                      df_ies_categorization.categoria,
                      df_ies_categorization.receita_total],
                fill_color=PLOT_BGCOLOR,
                font=dict(color='white', size=14),
                align='left')
      )]
  )

  fig.update_layout(
    height=ROW_HEIGHTS[1],
    paper_bgcolor=PLOT_BGCOLOR,
    plot_bgcolor=PLOT_BGCOLOR,
    xaxis = {'color':"#ffffff",'gridcolor':"#1F2426"},
    yaxis = {'color':"#ffffff",'gridcolor':"#1F2426"},
    margin= {'l': 10, 'r': 10, 't': 30, 'b': 10},
  ) 
  return fig

def qb_share():

  df_qb_share = (df_ies_categorization.groupby(['level','kind','categoria'])['receita_total'].agg('sum')).round(0).reset_index()
  df_qb_share['soma_categoria'] = (df_qb_share.groupby(['level','kind'])['receita_total'].transform('sum')).round(0)
  df_qb_share['porcentagem'] = (df_qb_share['receita_total']/df_qb_share['soma_categoria']).round(2)
  df_qb_share.drop(['soma_categoria'],axis=1)

  data_list = []
  for i in df_qb_share.columns.tolist():
    data_list.append(df_qb_share[i])

  fig = go.Figure(data=[go.Table(
  header=dict(values=df_qb_share.columns.tolist(),
    fill_color='lightseagreen',
    align='left',
    font=dict(color='white', size=14)),
  cells=dict(values=data_list,
              fill_color=PLOT_BGCOLOR,
              font=dict(color='white', size=14),
              align='left')
    )]
  )

  fig.update_layout(
    height=ROW_HEIGHTS[1],
    paper_bgcolor=PLOT_BGCOLOR,
    plot_bgcolor=PLOT_BGCOLOR,
    xaxis = {'color':"#ffffff",'gridcolor':"#1F2426"},
    yaxis = {'color':"#ffffff",'gridcolor':"#1F2426"},
    margin= {'l': 10, 'r': 10, 't': 30, 'b': 10},
  ) 
  return fig

def plot_time_series(selected_level,selected_kind,selected_period):

  fig_rpu_mm = rpu_time_series_mm(selected_level,selected_kind,selected_period)
  fig_rpu_ac = rpu_time_series_ac(selected_level,selected_kind,selected_period)
  fig_ss_mm = sucesso_time_series_mm(selected_level,selected_kind,selected_period)
  fig_ss_ac = sucesso_time_series_ac(selected_level,selected_kind,selected_period)
  fig_tm_mm = ticket_time_series_mm(selected_level,selected_kind,selected_period)
  fig_tm_ac = ticket_time_series_ac(selected_level,selected_kind,selected_period)



  return fig_rpu_mm,fig_rpu_ac,fig_ss_ac,fig_ss_mm,fig_tm_mm,fig_tm_ac

#####################################################################
#                             LAYOUT
#####################################################################

def make_layout():
  return html.Div([
  html.Div([
  html.Label('Comparativo de RPU YoY (to date)'),
  ],style={'fontSize': 25}),

  html.Div(children=[
    html.Div(children=[
        html.H6([
            "Graduação",
        ], className="container_title"),
        dcc.Loading(
            dcc.Graph(
                id='rpu_yoy',
                figure=rpu_yoy_grad()
                # config={'displayModeBar': False},
            ),
            className='svg-container',
            style={'height': ROW_HEIGHTS[1]}
        ),
    ], className='six columns pretty_container'),
    html.Div(children=[
        html.H6([
            "Pós-graduação",
        ], className="container_title"),
        dcc.Loading(
            dcc.Graph(
                id='rpu_yoy_2',
                figure=rpu_yoy_pos()
                # config={'displayModeBar': False},
            ),
            className='svg-container',
            style={'height': ROW_HEIGHTS[1]}
        )
    ], className='six columns pretty_container'),

  ], className = "row",style={'margin-top':'2%'}),

  html.Hr(style={'margin-right':'2%','margin-top':'2%'}),

########################### FILTRO LEVEL KIND ################################

  html.Label('Nível'),
  html.Div([
    dcc.Dropdown(
      id = 'level_selection',
      options=[
        {'label': 'Graduação', 'value': 'Graduação'},
        {'label': 'Pós-graduação', 'value': 'Pós-graduação'}
      ],
      value=1,
      className = "pretty_dropdown"
    )],
  style = {'width':'15%'}),

    html.Label('Modalidade'),
  html.Div([
    dcc.Dropdown(
      id = 'kind_selection',
      options=[
        {'label': 'Presencial', 'value': 'Presencial'},
        {'label': 'A distância (EaD)', 'value': 'A distância (EaD)'},
        {'label': 'Semipresencial', 'value': 'Semipresencial'}
      ],
      value=1,
      className = "pretty_dropdown"
    )],
  style = {'width':'15%'}),

    html.Label('Período'),
  html.Div([
    dcc.Dropdown(
      id = 'period_selection',
      options=[
        {'label': '2020.1', 'value': '2020.1'},
        {'label': '2020.2', 'value': '2020.2'}
      ],
      value=1,
      className = "pretty_dropdown"
    )],
  style = {'width':'15%'}),  

  html.Br(style={'margin-right':'2%','margin-top':'2%'}),

  html.Div([
  html.Label('Evolutivo das Taxas (Considerando apenas IES que não foram QAP no período)'),
],style={'fontSize': 25}),

############################### RPU ########################################

  html.Div(children=[
    html.Div(children=[
        html.H6([
            "RPU Acumulado",
        ], className="container_title"),
        dcc.Loading(
            dcc.Graph(
                id='fig_rpu_ac',
                figure=blank_fig(ROW_HEIGHTS[1])
                # config={'displayModeBar': False},
            ),
            className='svg-container',
            style={'height': ROW_HEIGHTS[1]}
        ),
    ], className='six columns pretty_container'),
    html.Div(children=[
        html.H6([
            "RPU Média Móvel",
        ], className="container_title"),
        dcc.Loading(
            dcc.Graph(
                id='fig_rpu_mm',
                figure=blank_fig(ROW_HEIGHTS[1])
                # config={'displayModeBar': False},
            ),
            className='svg-container',
            style={'height': ROW_HEIGHTS[1]}
        )
    ], className='six columns pretty_container'),

  ], className = "row",style={'margin-top':'2%'}),

############################### SUCESSO #############################################

  html.Div(children=[
    html.Div(children=[
        html.H6([
            "Sucesso Acumulado - Ordens Pagas/Ordens Registradas",
        ], className="container_title"),
        dcc.Loading(
            dcc.Graph(
                id='fig_ss_ac',
                figure=blank_fig(ROW_HEIGHTS[1])
                # config={'displayModeBar': False},
            ),
            className='svg-container',
            style={'height': ROW_HEIGHTS[1]}
        ),
    ], className='six columns pretty_container'),
    html.Div(children=[
        html.H6([
            "Sucesso Média Móvel - Ordens Pagas/Ordens Registradas",
        ], className="container_title"),
        dcc.Loading(
            dcc.Graph(
                id='fig_ss_mm',
                figure=blank_fig(ROW_HEIGHTS[1])
                # config={'displayModeBar': False},
            ),
            className='svg-container',
            style={'height': ROW_HEIGHTS[1]}
        )
    ], className='six columns pretty_container'),

  ], className = "row",style={'margin-top':'2%'}),


############################## TICKET MÉDIO #########################################

  html.Div(children=[
    html.Div(children=[
        html.H6([
            "Ticket Médio Acumulado",
        ], className="container_title"),
        dcc.Loading(
            dcc.Graph(
                id='fig_tm_ac',
                figure=blank_fig(ROW_HEIGHTS[1])
                # config={'displayModeBar': False},
            ),
            className='svg-container',
            style={'height': ROW_HEIGHTS[1]}
        ),
    ], className='six columns pretty_container'),
    html.Div(children=[
        html.H6([
            "Ticket Médio Média Móvel",
        ], className="container_title"),
        dcc.Loading(
            dcc.Graph(
                id='fig_tm_mm',
                figure=blank_fig(ROW_HEIGHTS[1])
                # config={'displayModeBar': False},
            ),
            className='svg-container',
            style={'height': ROW_HEIGHTS[1]}
        )
    ], className='six columns pretty_container'),

  ], className = "row",style={'margin-top':'2%'}),

############################### CATEGORIZAÇÃO DAS IES #########################
  
  html.Div(children=[
      html.Div(children=[
          html.H4([
              "Share dos Clusters em Receita QB e QAP por Modalidade e Nível",
          ], className="container_title"),
          dcc.Loading(
              dcc.Graph(
                  id='qb_share_level_kind',
                  figure=qb_share(),
                  config={'displayModeBar': False},
              ),
              className='svg-container',
              style={'height': ROW_HEIGHTS[0]}
          ),
      ], className='twelve columns pretty_container')
    ], className = "row"),


  html.Div(children=[
      html.Div(children=[
          html.H4([
              "Categorização das IES em QB e QAP",
          ], className="container_title"),
          dcc.Loading(
              dcc.Graph(
                  id='ies_categorization',
                  figure=ies_categorization_table(),
                  config={'displayModeBar': False},
              ),
              className='svg-container',
              style={'height': ROW_HEIGHTS[0]}
          ),
      ], className='twelve columns pretty_container')
    ], className = "row"),

],className = 'plots_body')

