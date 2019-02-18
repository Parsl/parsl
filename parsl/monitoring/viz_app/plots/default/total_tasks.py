import numpy as np
import pandas as pd
import plotly.graph_objs as go
from plotly.offline import plot
from parsl.monitoring.viz_app.utils import timestamp_to_int, num_to_timestamp, DB_DATE_FORMAT
from parsl.monitoring.viz_app.views import get_db, close_db


def total_tasks_plot(run_id, columns=20):
    sql_conn = get_db()
    df_status = pd.read_sql_query('SELECT run_id, task_id, task_status_name, timestamp FROM status WHERE run_id=(?)',
                                  sql_conn, params=(run_id, ))
    df_task = pd.read_sql_query('SELECT task_id, task_func_name FROM task WHERE run_id=(?)',
                                sql_conn, params=(run_id, ))

    close_db()

    min_time = timestamp_to_int(min(df_status['timestamp']))
    max_time = timestamp_to_int(max(df_status['timestamp']))
    time_step = (max_time - min_time) / columns

    x_axis = []
    for i in np.arange(min_time, max_time + time_step, time_step):
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
        for app, tasks in apps_dict.items():
            tmp = []
            task = df_status[df_status['task_id'].isin(tasks)]
            for i in range(len(x_axis) - 1):
                x = task['timestamp'] >= x_axis[i]
                y = task['timestamp'] < x_axis[i + 1]
                tmp.append(sum(task.loc[x & y]['task_status_name'] == value))
            items = np.sum([items, tmp], axis=0)

        return items

    y_axis_done = y_axis_setup('done')
    y_axis_failed = y_axis_setup('failed')

    fig = go.Figure(data=[go.Bar(x=x_axis[:-1],
                                 y=y_axis_done,
                                 name='done'),
                           go.Bar(x=x_axis[:-1],
                                  y=y_axis_failed,
                                  name='failed')],
                    layout=go.Layout(xaxis=dict(tickformat='%m-%d\n%H:%M:%S',
                                                autorange=True,
                                                title='Time'),
                                     yaxis=dict(tickformat=',d',
                                                title='Running tasks.' ' Bin width: ' + num_to_timestamp(time_step).strftime('%Mm%Ss')),
                                     annotations=[
                                         dict(
                                             x=0,
                                             y=1.07,
                                             showarrow=False,
                                             text='Total Done: ' + str(sum(y_axis_done)),
                                             xref='paper',
                                             yref='paper'
                                         ),
                                         dict(
                                             x=0,
                                             y=1.05,
                                             showarrow=False,
                                             text='Total Failed: ' + str(sum(y_axis_failed)),
                                             xref='paper',
                                             yref='paper'
                                         ),
                                     ],
                                     barmode='stack',
                                     title="Total tasks"))

    return plot(fig, show_link=False, output_type="div", include_plotlyjs=False)


def total_tasks_multiple_plot(run_id, apps, columns=20):
    sql_conn = get_db()
    df_status = pd.read_sql_query('SELECT run_id, task_id, task_status_name, timestamp FROM status WHERE run_id=(?)',
                                  sql_conn, params=(run_id, ))

    if type(apps) is dict:
        apps = ['', apps['label']]
    elif len(apps) == 1:
        apps.append('')

    df_task = pd.read_sql_query('SELECT task_id, task_func_name FROM task WHERE run_id=(?) AND task_func_name IN {apps}'.format(apps=tuple(apps)),
                                sql_conn, params=(run_id, ))

    close_db()

    min_time = timestamp_to_int(min(df_status['timestamp']))
    max_time = timestamp_to_int(max(df_status['timestamp']))
    time_step = (max_time - min_time) / columns

    x_axis = []
    for i in np.arange(min_time, max_time + time_step, time_step):
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
        for app, tasks in apps_dict.items():
            tmp = []
            task = df_status[df_status['task_id'].isin(tasks)]
            for i in range(len(x_axis) - 1):
                x = task['timestamp'] >= x_axis[i]
                y = task['timestamp'] < x_axis[i + 1]
                tmp.append(sum(task.loc[x & y]['task_status_name'] == value))
            items = np.sum([items, tmp], axis=0)

        return items

    y_axis_done = y_axis_setup('done')
    y_axis_failed = y_axis_setup('failed')

    fig = go.Figure(
        data=[go.Bar(x=x_axis[:-1],
                     y=y_axis_done,
                     name='done' + ' (' + str(sum(y_axis_done)) + ')'),
              go.Bar(x=x_axis[:-1],
                     y=y_axis_failed,
                     name='failed' + ' (' + str(sum(y_axis_failed)) + ')')],
        layout=go.Layout(xaxis=dict(tickformat='%m-%d\n%H:%M:%S',
                                    autorange=True,
                                    title='Time'),
                         yaxis=dict(tickformat=',d',
                                    title='Running tasks.' ' Bin width: ' + num_to_timestamp(time_step).strftime('%Mm%Ss')),
                         barmode='stack',
                         title="Total tasks"))

    return plot(fig, show_link=False, output_type="div", include_plotlyjs=False)
