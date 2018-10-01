import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from parsl.monitoring.web_app.app import app
from parsl.monitoring.web_app.utils import generate_table1, dropdown

app.config['suppress_callback_exceptions']=True

layout = html.Div(children=[
    html.H1("Workflows"),
    dropdown("SELECT run_id FROM workflows")
])