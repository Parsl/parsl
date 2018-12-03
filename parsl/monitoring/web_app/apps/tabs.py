import pandas as pd
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from parsl.monitoring.web_app.app import app, get_db, close_db
from parsl.monitoring.web_app.utils import dropdown
from parsl.monitoring.web_app.apps import workflow_details, tasks_details


def display_workflow(workflow_name):
    sql_conn = get_db()
    df_workflows = pd.read_sql_query('SELECT workflow_name, time_began, rundir, run_id FROM workflows WHERE workflow_name=(?)',
                                     sql_conn, params=(workflow_name, ))
    close_db()
    return html.Div(children=[
        html.H2(id='workflow_name', children=df_workflows['workflow_name'][0]),
        dropdown(id='run_number_dropdown', dataframe=df_workflows.sort_values(by='time_began', ascending=False), field='rundir'),
        dcc.Tabs(id="tabs", value='workflow', children=[
            dcc.Tab(label='Workflow', value='workflow'),
            dcc.Tab(label='Tasks', value='tasks'),
        ]),
        html.Div(id='tabs-content')
    ])


@app.callback(Output('tabs-content', 'children'),
              [Input('tabs', 'value')])
def render_content(tab):
    if tab == 'workflow':
        return workflow_details.layout
    elif tab == 'tasks':
        return tasks_details.layout
