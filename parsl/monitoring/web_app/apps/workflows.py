import pandas as pd
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from parsl.monitoring.web_app.app import app, get_db, close_db
from parsl.monitoring.web_app.utils import dataframe_to_html_table


sql_conn = get_db()

layout = html.Div(children=[
    html.H1("Workflows"),
    dataframe_to_html_table(id='workflows_table',
                            dataframe=pd.read_sql_query("SELECT * FROM workflows", sql_conn)
                            .drop_duplicates(
                                subset=['workflow_name', 'workflow_version'],
                                keep='last')
                            .sort_values(
                                by=['time_began'],
                                ascending=[False]),
                            field='workflow_name'),
    html.Div(id='workflow_details')
])

close_db()
