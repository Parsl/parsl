import pandas as pd
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import plotly.graph_objs as go
from parsl.monitoring.web_app.app import app, get_db, close_db
from parsl.monitoring.web_app.utils import timestamp_to_int, int_to_timestamp, DB_DATE_FORMAT


layout = html.Div(id='workflow_details')


@app.callback(Output('workflow_details', 'children'),
              [Input('run_number_dropdown', 'value')])
def workflow_details(run_id):
    return [html.A(id='run_id', children=run_id, hidden=True),
            dcc.RadioItems(
                id='resource_usage_radio_items',
                options=[{'label': 'User time', 'value': 'psutil_process_time_user'},
                         {'label': 'System time', 'value': 'psutil_process_time_system'},
                         {'label': 'Memory usage', 'value': 'psutil_process_memory_percent'}],
                value='psutil_process_time_user',
            ),
            dcc.Graph(id='resource_usage_plot'),
            tasks_per_app_plot(run_id),
            total_tasks_plot(run_id)]


@app.callback(Output('resource_usage_plot', 'figure'),
              [Input('resource_usage_radio_items', 'value')],
              [State('run_number_dropdown', 'value')])
def resource_usage_plot(field, run_id):
    sql_conn = get_db()
    df_resources = pd.read_sql_query('SELECT {field}, timestamp, task_id FROM task_resources WHERE run_id=(?)'.format(field=field),
                                     sql_conn, params=(run_id, ))
    df_task = pd.read_sql_query('SELECT task_id, task_time_completed FROM task WHERE run_id=(?)',
                                sql_conn, params=(run_id, ))
    close_db()

    def y_axis_setup():
        dic = dict()
        count = 0
        items = []

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

            items.append(count)

        return items

    if len(df_resources) == 0:
        return html.H3('No resource data found')
    else:
        return go.Figure(
            data=[go.Scatter(x=df_resources['timestamp'],
                             y=y_axis_setup(),
                             name='tasks')],
            layout=go.Layout(xaxis=dict(tickformat='%m-%d\n%H:%M:%S',
                                        range=[min(df_resources.timestamp), max(df_resources.timestamp)],
                                        title='Time'),
                             title="Resource usage")
        )


def tasks_per_app_plot(run_id):
    sql_conn = get_db()
    df_status = pd.read_sql_query('SELECT run_id, task_id, task_status_name, timestamp FROM task_status WHERE run_id=(?)',
                                  sql_conn, params=(run_id, ))
    df_task = pd.read_sql_query('SELECT task_id, task_fn_hash FROM task WHERE run_id=(?)',
                                sql_conn, params=(run_id,))
    close_db()

    def y_axis_setup(array):
        count = 0
        items = []
        for n in array:
            if n:
                count += 1
            elif count > 0:
                count -= 1
            items.append(count)

        return items

    # Fill up dict "apps" like: {app1: [#task1, #task2], app2: [#task4], app3: [#task3]}
    apps = dict()
    for i in range(len(df_task)):
        row = df_task.iloc[i]
        if row['task_fn_hash'] in apps:
            apps[row['task_fn_hash']].append(row['task_id'])
        else:
            apps[row['task_fn_hash']] = [row['task_id']]

    return dcc.Graph(id='tasks_per_app',
                     figure=go.Figure(
                         data=[go.Scatter(x=df_status[df_status['task_id'].isin(tasks)]['timestamp'],
                                          y=y_axis_setup(df_status[df_status['task_id'].isin(tasks)]['task_status_name'] == 'running'),
                                          name=app)
                               for app, tasks in apps.items()] +
                              [go.Scatter(x=df_status['timestamp'],
                                          y=y_axis_setup(df_status['task_status_name'] == 'running'),
                                          name='all')],
                         layout=go.Layout(xaxis=dict(tickformat='%m-%d\n%H:%M:%S',
                                                     range=[min(df_status['timestamp']), max(df_status['timestamp'])],
                                                     title='Time'),
                                          yaxis=dict(tickformat= ',d',
                                                     title='Tasks'),
                                          hovermode='closest',
                                          title="Tasks per app")
                     ))


def total_tasks_plot(run_id):
    sql_conn = get_db()
    df_status = pd.read_sql_query('SELECT run_id, task_id, task_status_name, timestamp FROM task_status WHERE run_id=(?)',
                                  sql_conn, params=(run_id, ))
    close_db()

    columns = 20

    min_time = timestamp_to_int(min(df_status['timestamp']))
    max_time = timestamp_to_int(max(df_status['timestamp']))
    time_step = int((max_time - min_time) / columns)

    x_axis = []
    for i in range(min_time, max_time, time_step):
        x_axis.append(int_to_timestamp(i).strftime(DB_DATE_FORMAT))

    def y_axis_setup(value):
        items = []
        for i in range(len(x_axis) - 1):
            x = df_status['timestamp'] >= x_axis[i]
            y = df_status['timestamp'] < x_axis[i+1]
            items.append(sum(df_status.loc[[a and b for a, b in zip(x, y)]]['task_status_name'] == value))

        return items


    return dcc.Graph(id='total_tasks',
                     figure=go.Figure(data=[go.Bar(x=x_axis,
                                                   y=y_axis_setup('done'),
                                                   name='done'),
                                            go.Bar(x=x_axis,
                                                   y=y_axis_setup('failed'),
                                                   name='failed')],
                                      layout=go.Layout(xaxis=dict(tickformat='%m-%d\n%H:%M:%S',
                                                                  range=[min(df_status['timestamp']), max(df_status['timestamp'])],
                                                                  title='Time. ' + ' Bin width: ' + int_to_timestamp(time_step).strftime('%Mm%Ss')),
                                                       yaxis=dict(tickformat= ',d',
                                                                  title='Tasks'),
                                                       barmode='stack',
                                                       title="Total tasks")))







# @app.callback(
#     Output('tables', 'children'),
#     [Input('tasks_per_app', 'clickData')],
#     [State('run_id', 'children')])
# def load_task_table(clicked, task_id):
#     print(clicked)
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