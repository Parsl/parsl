import pandas as pd
import plotly.graph_objs as go
from plotly.offline import plot
from parsl.monitoring.viz_app.views import get_db, close_db


def task_per_app_plot(run_id):
    sql_conn = get_db()
    df_status = pd.read_sql_query('SELECT run_id, task_id, task_status_name, timestamp FROM task_status WHERE run_id=(?)',
                                  sql_conn, params=(run_id, ))
    df_task = pd.read_sql_query('SELECT task_id, task_func_name FROM task WHERE run_id=(?)',
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
    apps_dict = dict()
    for i in range(len(df_task)):
        row = df_task.iloc[i]
        if row['task_func_name'] in apps_dict:
            apps_dict[row['task_func_name']].append(row['task_id'])
        else:
            apps_dict[row['task_func_name']] = [row['task_id']]

    fig = go.Figure(
        data=[go.Scatter(x=df_status[df_status['task_id'].isin(tasks)]['timestamp'],
                         y=y_axis_setup(df_status[df_status['task_id'].isin(tasks)]['task_status_name'] == 'running'),
                         name=app)
              for app, tasks in apps_dict.items()] +
             [go.Scatter(x=df_status['timestamp'],
                         y=y_axis_setup(df_status['task_status_name'] == 'running'),
                         name='all')],
        layout=go.Layout(xaxis=dict(tickformat='%m-%d\n%H:%M:%S',
                                    autorange=True,
                                    title='Time'),
                         yaxis=dict(tickformat=',d',
                                    title='Tasks'),
                         hovermode='closest',
                         title='Tasks per app'))

    return plot(fig, show_link=False, output_type="div", include_plotlyjs=False)


def task_per_app_multiple_plot(run_id, apps):
    sql_conn = get_db()
    df_status = pd.read_sql_query('SELECT run_id, task_id, task_status_name, timestamp FROM task_status WHERE run_id=(?)',
                                  sql_conn, params=(run_id, ))

    if type(apps) is dict:
        apps = ['', apps['label']]
    elif len(apps) == 1:
        apps.append('')

    df_task = pd.read_sql_query('SELECT task_id, task_func_name FROM task WHERE run_id=(?) AND task_func_name IN {apps}'.format(apps=tuple(apps)),
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
    apps_dict = dict()
    for i in range(len(df_task)):
        row = df_task.iloc[i]
        if row['task_func_name'] in apps_dict:
            apps_dict[row['task_func_name']].append(row['task_id'])
        else:
            apps_dict[row['task_func_name']] = [row['task_id']]

    fig = go.Figure(
        data=[go.Scatter(x=df_status[df_status['task_id'].isin(tasks)]['timestamp'],
                         y=y_axis_setup(df_status[df_status['task_id'].isin(tasks)]['task_status_name'] == 'running'),
                         name=app)
              for app, tasks in apps_dict.items()] +
             [go.Scatter(x=df_status['timestamp'],
                         y=y_axis_setup(df_status['task_status_name'] == 'running'),
                         name='all')],
        layout=go.Layout(xaxis=dict(tickformat='%m-%d\n%H:%M:%S',
                                    autorange=True,
                                    title='Time'),
                         yaxis=dict(tickformat=',d',
                                    title='Tasks'),
                         hovermode='closest',
                         title='Tasks per app'))

    return plot(fig, show_link=False, output_type="div", include_plotlyjs=False)
