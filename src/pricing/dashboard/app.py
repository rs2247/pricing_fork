
import dash
import dash_bootstrap_components as dbc

external_stylesheets = [
  'https://codepen.io/chriddyp/pen/bWLwgP.css',
  "https://codepen.io/chriddyp/pen/rzyyWo.css",
  "https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css"
]
external_scripts =[
  "http://code.jquery.com/jquery-1.7.1.min.js"
]

app = dash.Dash(__name__, external_stylesheets=external_stylesheets,external_scripts=external_scripts)
server = app.server
app.config.suppress_callback_exceptions = True