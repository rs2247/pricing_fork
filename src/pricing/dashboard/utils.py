import math
import time
import pandas as pd
import numpy as np
from datetime import date, timedelta

####################################################################################
#                             CONSTANTS
####################################################################################
# DEBUG_MODE = True #using cached data files or not

# Colors
BGCOLOR = "#f3f3f1"  # mapbox light map land color
PLOT_BGCOLOR = "#3B4347"
# bar_bgcolor = "#b0bec5"  # material blue-gray 200
# bar_unselected_color = "#78909c"  # material blue-gray 400
# bar_color = "#546e7a"  # material blue-gray 600
# bar_selected_color = "#37474f" # material blue-gray 800
# bar_unselected_opacity = 0.8

# Figure template
template = {'layout': {'paper_bgcolor': BGCOLOR, 'plot_bgcolor': BGCOLOR}}
ROW_HEIGHTS = [500, 350, 270]
# plot_backround = '#3B4347'

#Min number of points for a filter be available in apps/abtest
MIN_N_POINTS = 1000

####################################################################################
#                             HELPER METHODS
####################################################################################

def blank_fig(height):
  """
  Build blank figure with the requested height
  """
  return {
    'data': [],
    'layout': {
        'height': height,
        # 'template': template,
        'paper_bgcolor':PLOT_BGCOLOR,
        'plot_bgcolor': PLOT_BGCOLOR,
        'xaxis': {'visible': False},
        'yaxis': {'visible': False},
    }
  }


def get_ppa_link(city,state):

  ppa_link = "https://ppa.querobolsa.space/dashboard?currentFilter={%22initialDate%22:%222019-10-01T00:00:00Z%22,%22finalDate%22:%222020-03-31T23:59:59Z%22,%22baseFilters%22:"
  ppa_link = ppa_link + "[{%22type%22:%22%22,%22name%22:%22Site%20Todo%22}],%22locationType%22:%22city%22,%22locationValue%22:{"
  ppa_link = ppa_link + "%22name%22:%22{}%22,".format(city) # cidade
  ppa_link = ppa_link + "%22state%22:%22{}%22".format(state) # estado
  ppa_link = ppa_link + "},%22productLineSelectionType%22:%22kind_and_level%22,"
  ppa_link = ppa_link + "%22kinds%22:[{%22name%22:%22"
  ppa_link = ppa_link + "Presencial%22,%22id%22:%221%" # kind
  ppa_link = ppa_link + "22}],"
  ppa_link = ppa_link + "%22levels%22:[{%22name%22:%22"
  ppa_link = ppa_link + "Gradua%C3%A7%C3%A3o%22,%22id%22:%221%" # level
  ppa_link = ppa_link + "22}],"
  ppa_link = ppa_link + "%22seriesSelection%22:%22higher%22,%22seriesSelectionField%22:%22income%22}&comparingMode=grouping&grouping={%22name%22:%22IES%22,%22id%22:%22university%22}&exibitionType=all_charts"

  return ppa_link

