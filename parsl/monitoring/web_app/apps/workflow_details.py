import pandas as pd
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import plotly.graph_objs as go
from parsl.monitoring.web_app.app import app, get_db, close_db


def display_workflow(run_id):
    sql_conn = get_db()
    df_workflows = pd.read_sql_query('SELECT workflow_name, rundir FROM workflows WHERE run_id=(?)', sql_conn, params=(run_id, ))
    return html.Div(children=[
        html.A(id='run_id', children=run_id, hidden=True),
        html.H2(children=df_workflows['workflow_name'] + '_' + df_workflows['rundir'][0].split('/').pop()),
        total_tasks_graph(run_id)
        # html.Div(id='tables')
    ])


def total_tasks_graph(run_id):
    sql_conn = get_db()
    df_status = pd.read_sql_query("SELECT run_id, task_id, task_status_name, timestamp FROM task_status WHERE run_id=(?)", sql_conn, params=(run_id, ))
    df_task = pd.read_sql_query("SELECT task_id, task_fn_hash FROM task WHERE run_id=(?)", sql_conn, params=(run_id,))
    apps = dict()

    def count_running(array):
        count = 0
        n = []
        for i in array:
            if i:
                count += 1
            elif count > 0:
                count -= 1
            n.append(count)

        return n

    for i in range(len(df_task)):
        row = df_task.ix[i]
        if row['task_fn_hash'] in apps:
            apps[row['task_fn_hash']].append(row['task_id'])
        else:
            apps[row['task_fn_hash']] = [row['task_id']]

    return dcc.Graph(id='graph',
                     figure=go.Figure(
                         data=[go.Scatter(x=df_status[df_status['task_id'].isin(tasks)]['timestamp'],
                                          y=count_running(df_status[df_status['task_id'].isin(tasks)]['task_status_name'] == 'running'),
                                          name=app)
                               for app, tasks in apps.items()] +
                              [go.Scatter(x=df_status['timestamp'],
                                          y=count_running(df_status['task_status_name'] == 'running'),
                                          name='all')],
                         layout=go.Layout(xaxis=dict(tickformat='%m-%d\n%H:%M:%S',
                                                     range=[min(df_status['timestamp']), max(df_status['timestamp'])]),
                                          title="Total tasks")
    ))

# TODO: task_resources is not created for all workflows. Throws error

# @app.callback(Output('graph', 'figure'),
#               [Input('radio', 'value')],
#               [State('run_id', 'children')])
# def load_task_graph(field, task_id):
#     sql_conn = get_db()
#
#     df_resources = pd.read_sql_query("SELECT * FROM task_resources WHERE run_id=(?)", sql_conn, params=(task_id, ))
#     df_task = pd.read_sql_query("SELECT * FROM task WHERE run_id=(?)", sql_conn, params=(task_id, ))
#
#     return go.Figure(
#         data=[go.Scatter(x=df_resources[df_resources['task_id'] == task].timestamp,
#                          y=df_resources.loc[df_resources['task_id'] == task][field],
#                          name=df_task.loc[df_task['task_id'] == task].task_fn_hash.iat[0] + "_" + task)
#               for task in df_task.task_id],
#         layout=go.Layout(xaxis=dict(tickformat='%H:%M:%S', range=[min(df_resources.timestamp), max(df_resources.timestamp)]),
#                          title="Workflow " + task_id)
#     )
#
#
# @app.callback(
#     Output('tables', 'children'),
#     [Input('graph', 'clickData')],
#     [State('task_id', 'children')])
# def load_task_table(clicked, task_id):
#     if not clicked:
#         return
#     sql_conn = get_db()
#
#     df_resources = pd.read_sql_query("SELECT * FROM task_resources WHERE run_id=(?)", sql_conn, params=(task_id, ))
#     df_task = pd.read_sql_query("SELECT * FROM task WHERE run_id=(?)", sql_conn, params=(task_id, ))
#
#     return [html.Table(
#         [html.Tr([html.Th(col) for col in df_resources.columns])] + \
#         [html.Tr([html.Td(html.A(df_resources.loc[df_resources['task_id'] == str(point['curveNumber'])].iloc[i][col])) for col in df_resources.loc[df_resources['task_id'] == str(point['curveNumber'])].columns]) for i in range(len(df_resources.loc[df_resources['task_id'] == str(point['curveNumber'])]))]) for point in clicked['points']]
#
#
# def load_radio_items(task_id):
#     return dcc.RadioItems(
#         id='radio',
#         options=[{'label': 'task_id', 'value': 'task_id'},
#                  {'label': 'psutil_process_time_user', 'value': 'psutil_process_time_user'},
#                  {'label': 'psutil_process_time_system', 'value': 'psutil_process_time_system'},
#                  {'label': 'psutil_process_memory_percent', 'value': 'psutil_process_memory_percent'}],
#         value='task_id',
#     )

close_db()
