import pandas as pd
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from parsl.monitoring.web_app.app import app, get_db, close_db
from parsl.monitoring.web_app.utils import dataframe_to_table


sql_conn = get_db()

layout = html.Div(children=[
    html.H1("Workflows"),
    dataframe_to_table(id='workflows_table', dataframe=pd.read_sql_query("SELECT workflow_name, run_id FROM workflows", sql_conn)),
    html.Div(id='workflow_details')
])

close_db()
