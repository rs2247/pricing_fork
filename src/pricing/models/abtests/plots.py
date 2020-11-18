import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import os
import math
import random
import plotly.graph_objs as go
from plotly.subplots import make_subplots
import plotly.express as px


from pprint import pprint
import scipy

from datetime import *
from pricing.models.abtests import *
import time

def plot_experiment_comparison(df_cum, fields):
  '''
  Args:
    df_exp: df with alternatives cummulative data for a given experiment
  '''

  [field_arpu, field_ratio, field_ticket, field_conversion] = fields

  start = time.process_time()

  fig_arpu = _experiment_arpu_comparison(df_cum, field_arpu)
  fig_ratio = _experiment_ratio_comparison(df_cum, field_ratio)
  fig_ticket = _experiment_ticket_comparison(df_cum, field_ticket)
  fig_conversion = _experiment_conversion_comparison(df_cum, field_conversion)

  return fig_arpu,fig_ratio,fig_ticket,fig_conversion

def _experiment_arpu_comparison(df, field='arpu'):
  '''
  Args:
    df : dataframe with cummulative results of all altenatives of one experiment
                 (see @data_loading.get_cumulative_agg_results)
  '''
  fig = px.line(df,x='created_day',y=field,color='alternative')
  fig.update_yaxes(rangemode="tozero")
  return fig

def _experiment_ratio_comparison(df, field='alternative_ratio'):
  return px.bar(df, x="created_day", y=field, color='alternative')

def _experiment_ticket_comparison(df, field='av_ticket'):
  '''
  Args:
    df : dataframe with cummulative results of all altenatives of one experiment
                 (see @data_loading.get_cumulative_agg_results)
  '''
  fig = px.line(df,x = 'created_day',y=field,color ='alternative')
  fig.update_yaxes(rangemode="tozero")
  return fig

def _experiment_conversion_comparison(df, field='registered_conversion'):
  fig = px.line(df,x = 'created_day',y=field,color ='alternative')
  fig.update_yaxes(rangemode="tozero")
  return fig


def plot_pef_elasticity(x_range, y_range_reta, y_range, conversion, price, arpu):
  plot_line_df = pd.DataFrame({'x_data':x_range, 'y_data':y_range_reta})
  fig = px.line(plot_line_df, x="x_data", y="y_data")

  fig = fig.add_scatter(x=conversion, y=price,mode="markers")

  plot_curve_df = pd.DataFrame({'x_data':x_range, 'y_data':y_range})
  fig = fig.add_scatter(x=plot_curve_df['x_data'], y=plot_curve_df['y_data'],mode="lines",yaxis='y2')

  fig = fig.add_scatter(x=conversion, y=arpu,mode="markers",yaxis='y2')

  fig.update_layout(
      yaxis2={
        'anchor':"free",
        'overlaying':"y",
        'side':"left",
        'position':0.01,
        'showgrid': False
      },
  )

  return fig
