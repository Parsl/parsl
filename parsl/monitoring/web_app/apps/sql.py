import pandas as pd
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

from parsl.monitoring.web_app.app import app, get_db, close_db
from parsl.monitoring.web_app.utils import dataframe_to_html_table


sql_conn = get_db()

# FIXME hrefs broken because url now displays workflow_name instead of run_id
layout = html.Div(children=[
    html.H1("Tasks"),
    dataframe_to_html_table(id="sql_task", dataframe=pd.read_sql_query("SELECT * FROM task", sql_conn), field='run_id'),
    html.H1("Task Resources"),
    dataframe_to_html_table(id="sql_task_resources", dataframe=pd.read_sql_query("SELECT * FROM task_resources", sql_conn), field='run_id'),
    html.H1("Task Status"),
    dataframe_to_html_table(id="sql_task_status", dataframe=pd.read_sql_query("SELECT * FROM task_status", sql_conn), field='run_id'),
    html.H1("Workflows"),
    dataframe_to_html_table(id="sql_workflows", dataframe=pd.read_sql_query("SELECT * FROM workflows", sql_conn), field='run_id')
])

close_db()
