import pandas as pd
import numpy as np
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import plotly.graph_objs as go
from parsl.monitoring.web_app.app import app, get_db, close_db
from parsl.monitoring.web_app.utils import timestamp_to_int, num_to_timestamp, DB_DATE_FORMAT


layout = html.Div(id='tasks_details')


@app.callback(Output('tasks_details', 'children'),
              [Input('run_number_dropdown', 'value')])
def tasks_details(run_id):
    sql_conn = get_db()
    df_task = pd.read_sql_query('SELECT task_id, task_func_name FROM task WHERE run_id=(?)',
                                sql_conn, params=(run_id,))
    close_db()

    apps = []
    for _app in df_task['task_func_name'].unique():
        apps.append(dict(label=_app, value=_app))

    return [dcc.Dropdown(
        id='apps_dropdown',
        options=apps,
        value=apps[0],
        multi=True),
        tasks_per_app_plot(run_id),
        total_tasks_plot(run_id)]


def tasks_per_app_plot(run_id):
    return dcc.Graph(id='tasks_per_app_plot_tasks')


@app.callback(Output('tasks_per_app_plot_tasks', 'figure'),
              [Input('apps_dropdown', 'value')],
              [State('run_number_dropdown', 'value')])
def tasks_per_app_plot_callback(apps, run_id):
    if type(apps) is dict:
        apps = ['', apps['label']]
    elif len(apps) == 1:
        apps.append('')

    sql_conn = get_db()
    df_status = pd.read_sql_query('SELECT run_id, task_id, task_status_name, timestamp FROM task_status WHERE run_id=(?)',
                                  sql_conn, params=(run_id, ))
    df_task = pd.read_sql_query('SELECT task_id, task_func_name FROM task WHERE run_id=(?) AND task_fn_hash IN {apps}'.format(apps=tuple(apps)),
                                sql_conn, params=(run_id, ))
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
        if row['task_func_name'] in apps:
            apps[row['task_func_name']].append(row['task_id'])
        else:
            apps[row['task_func_name']] = [row['task_id']]

    return go.Figure(data=[go.Scatter(x=df_status[df_status['task_id'].isin(tasks)]['timestamp'],
                                      y=y_axis_setup(df_status[df_status['task_id'].isin(tasks)]['task_status_name'] == 'running'),
                                      name=_app)
                           for _app, tasks in apps.items()],
                     layout=go.Layout(xaxis=dict(tickformat='%m-%d\n%H:%M:%S',
                                                 range=[min(df_status['timestamp']), max(df_status['timestamp'])],
                                                 title='Time'),
                                      yaxis=dict(tickformat=',d',
                                                 title='Tasks'),
                                      hovermode='closest',
                                      title="Tasks per app")
                     )


# FIXME Duplicated code
def total_tasks_plot(run_id, columns=20):
    sql_conn = get_db()
    df_status = pd.read_sql_query('SELECT run_id, task_id, task_status_name, timestamp FROM task_status WHERE run_id=(?)',
                                  sql_conn, params=(run_id, ))
    close_db()

    min_time = timestamp_to_int(min(df_status['timestamp']))
    max_time = timestamp_to_int(max(df_status['timestamp']))

    time_step = int((max_time - min_time) / columns)
    minutes = time_step // 60
    seconds = time_step % 60

    return html.Div(id='total_tasks_container',
                    children=[html.P('Bin width'),
                              html.Label(htmlFor='bin_width_minutes', children='Minutes'),
                              dcc.Input(id='bin_width_minutes', type='number', min=0, value=minutes),
                              html.Label(htmlFor='bin_width_seconds', children='Seconds'),
                              dcc.Input(id='bin_width_seconds', type='number', min=0, value=seconds),
                              dcc.Graph(id='total_tasks_plot_tasks')])


# FIXME Almost duplicated code
@app.callback(Output('total_tasks_plot_tasks', 'figure'),
              [Input('apps_dropdown', 'value'),
               Input('bin_width_minutes', 'value'),
               Input('bin_width_seconds', 'value')],
              [State('run_number_dropdown', 'value')])
def total_tasks_plot_tasks(apps, minutes, seconds, run_id):
    # apps is sometimes a dict and sometimes a list.
    # This if statement is to make sure that tuple(apps) in pd.read_sql_query is formatted correctly
    if type(apps) is dict:
        apps = ['', apps['label']]
    elif len(apps) == 1:
        apps.append('')

    sql_conn = get_db()
    df_status = pd.read_sql_query('SELECT run_id, task_id, task_status_name, timestamp FROM task_status WHERE run_id=(?)',
                                  sql_conn, params=(run_id, ))
    df_task = pd.read_sql_query('SELECT task_id, task_func_name FROM task WHERE run_id=(?) AND task_fn_hash IN {apps}'.format(apps=tuple(apps)),
                                sql_conn, params=(run_id, ))
    close_db()

    min_time = timestamp_to_int(min(df_status['timestamp']))
    max_time = timestamp_to_int(max(df_status['timestamp']))
    time_step = 60 * minutes + seconds

    x_axis = []
    for i in range(min_time, max_time, time_step):
        x_axis.append(num_to_timestamp(i).strftime(DB_DATE_FORMAT))

    # Fill up dict "apps" like: {app1: [#task1, #task2], app2: [#task4], app3: [#task3]}
    apps_dict = dict()
    for i in range(len(df_task)):
        row = df_task.iloc[i]
        if row['task_func_name'] in apps_dict:
            apps_dict[row['task_func_name']].append(row['task_id'])
        else:
            apps_dict[row['task_func_name']] = [row['task_id']]

    def y_axis_setup(value):
        items = []
        for _app, tasks in apps_dict.items():
            tmp = []
            for i in range(len(x_axis) - 1):
                task = df_status[df_status['task_id'].isin(tasks)]
                x = task['timestamp'] >= x_axis[i]
                y = task['timestamp'] < x_axis[i + 1]
                tmp.append(sum(task.loc[[a and b for a, b in zip(x, y)]]['task_status_name'] == value))
            items = np.sum([items, tmp], axis=0)

        return items

    return go.Figure(data=[go.Bar(x=x_axis[:-1],
                                  y=y_axis_setup('done'),
                                  name='done'),
                           go.Bar(x=x_axis[:-1],
                                  y=y_axis_setup('failed'),
                                  name='failed')],
                     layout=go.Layout(xaxis=dict(tickformat='%m-%d\n%H:%M:%S',
                                                 autorange=True,
                                                 title='Time. ' + ' Bin width: ' + num_to_timestamp(time_step).strftime('%Mm%Ss')),
                                      yaxis=dict(tickformat=',d',
                                                 title='Tasks'),
                                      barmode='stack',
                                      title="Total tasks"))
