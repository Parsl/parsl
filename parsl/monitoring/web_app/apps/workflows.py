import pandas as pd
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import plotly.graph_objs as go
from parsl.monitoring.web_app.app import app, get_db
from parsl.monitoring.web_app.utils import dropdown


layout = html.Div(children=[
    html.H1("Workflows"),
    dropdown('dropdown'),
    html.Div(id='workflow')
])


@app.callback(Output('workflow', 'children'),
              [Input('dropdown', 'value')])
def load_workflow(task_id):
    return html.Div(children=[
        html.Div(children=[
            html.A(id="task_id", children=task_id, hidden=True)
        ]),
        load_radio_items(task_id),
        dcc.Graph(id='graph'),
        html.Div(id='tables')
    ])


@app.callback(Output('graph', 'figure'),
              [Input('radio', 'value')],
              [State('task_id', 'children')])
def load_task_graph(field, task_id):
    sql_conn = get_db()

    df_resources = pd.read_sql_query("SELECT * FROM task_resources WHERE run_id=(?)", sql_conn, params=(task_id, ))
    df_task = pd.read_sql_query("SELECT * FROM task WHERE run_id=(?)", sql_conn, params=(task_id, ))

    return go.Figure(
        data=[go.Scatter(x=df_resources[df_resources['task_id'] == task].timestamp,
                         y=df_resources.loc[df_resources['task_id'] == task][field],
                         name=df_task.loc[df_task['task_id'] == task].task_fn_hash.iat[0] + "_" + task)
              for task in df_task.task_id],
        layout=go.Layout(xaxis=dict(tickformat='%H:%M:%S', range=[min(df_resources.timestamp), max(df_resources.timestamp)]),
                         title="Workflow " + task_id)
    )


@app.callback(
    Output('tables', 'children'),
    [Input('graph', 'clickData')],
    [State('task_id', 'children')])
def load_task_table(clicked, task_id):
    if not clicked:
        return
    sql_conn = get_db()

    df_resources = pd.read_sql_query("SELECT * FROM task_resources WHERE run_id=(?)", sql_conn, params=(task_id, ))
    df_task = pd.read_sql_query("SELECT * FROM task WHERE run_id=(?)", sql_conn, params=(task_id, ))

    return [html.Table(
        [html.Tr([html.Th(col) for col in df_resources.columns])] + \
        [html.Tr([html.Td(html.A(df_resources.loc[df_resources['task_id'] == str(point['curveNumber'])].iloc[i][col])) for col in df_resources.loc[df_resources['task_id'] == str(point['curveNumber'])].columns]) for i in range(len(df_resources.loc[df_resources['task_id'] == str(point['curveNumber'])]))]) for point in clicked['points']]


def load_radio_items(task_id):
    return dcc.RadioItems(
        id='radio',
        options=[{'label': 'task_id', 'value': 'task_id'},
                 {'label': 'psutil_process_time_user', 'value': 'psutil_process_time_user'},
                 {'label': 'psutil_process_time_system', 'value': 'psutil_process_time_system'},
                 {'label': 'psutil_process_memory_percent', 'value': 'psutil_process_memory_percent'}],
        value='task_id',
    )
