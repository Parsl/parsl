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
        load_radio_items(),
        dcc.Graph(id='workflow_details'),
        total_tasks_graph(run_id)
        # html.Div(id='tables')
    ])


# TODO: task_resources is not created for all workflows. Throws error
@app.callback(Output('workflow_details', 'figure'),
              [Input('radio', 'value')],
              [State('run_id', 'children')])
def workflow_details(field, run_id):
    sql_conn = get_db()
    df_resources = pd.read_sql_query("SELECT * FROM task_resources WHERE run_id=(?)", sql_conn, params=(run_id, ))
    df_task = pd.read_sql_query("SELECT task_id, task_time_completed FROM task WHERE run_id=(?)", sql_conn, params=(run_id, ))
    close_db()

    def count_running():
        dic = dict()
        count = 0
        n = []

        for i in range(len(df_resources)):
            task_id = df_resources.iloc[i]['task_id']
            value = float(df_resources.iloc[i][field])

            if task_id in dic:
                count -= dic[task_id][0]

            dic[task_id] = (value, df_task[df_task['task_id'] == task_id]['task_time_completed'].iloc[0])
            count += value

            remove = []
            for k, v in dic.items():
                if v[1] < df_resources.iloc[i]['timestamp']:
                    count -= v[0]
                    remove.append(k)

            for k in remove: del dic[k]

            n.append(count)

        return n

    return go.Figure(
        data=[go.Scatter(x=df_resources['timestamp'],
                         y=count_running(),
                         name='tasks')],
        layout=go.Layout(xaxis=dict(tickformat='%H:%M:%S', range=[min(df_resources.timestamp), max(df_resources.timestamp)]),
                         title="Resource usage")
    )


def total_tasks_graph(run_id):
    sql_conn = get_db()
    df_status = pd.read_sql_query("SELECT run_id, task_id, task_status_name, timestamp FROM task_status WHERE run_id=(?)", sql_conn, params=(run_id, ))
    df_task = pd.read_sql_query("SELECT task_id, task_fn_hash FROM task WHERE run_id=(?)", sql_conn, params=(run_id,))
    close_db()

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

    # Fill up dict "apps" like: {app1: [#task1, #task2], app2: [#task4], app3: [#task3]}
    apps = dict()
    for i in range(len(df_task)):
        row = df_task.iloc[i]
        if row['task_fn_hash'] in apps:
            apps[row['task_fn_hash']].append(row['task_id'])
        else:
            apps[row['task_fn_hash']] = [row['task_id']]

    return dcc.Graph(id='total_tasks',
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

def load_radio_items():
    return dcc.RadioItems(
        id='radio',
        options=[{'label': 'psutil_process_time_user', 'value': 'psutil_process_time_user'},
                 {'label': 'psutil_process_time_system', 'value': 'psutil_process_time_system'},
                 {'label': 'psutil_process_memory_percent', 'value': 'psutil_process_memory_percent'}],
        value='psutil_process_memory_percent',
    )
