import numpy as np
import pandas as pd
from plotly.offline import plot
import plotly.graph_objs as go
from parsl.monitoring.viz_app.views import get_db, close_db


def system_time_distribution_plot(run_id, option, columns=20):
    sql_conn = get_db()
    df_resources = pd.read_sql_query('SELECT psutil_process_time_system, timestamp, task_id FROM task_resources WHERE run_id=(?)',
                                     sql_conn, params=(run_id, ))
    df_task = pd.read_sql_query('SELECT task_id, task_time_returned FROM task WHERE run_id=(?)',
                                sql_conn, params=(run_id, ))
    close_db()

    min_range = min(df_resources['psutil_process_time_system'].astype('float'))
    max_range = max(df_resources['psutil_process_time_system'].astype('float'))
    time_step = (max_range - min_range) / columns

    x_axis = []
    for i in np.arange(min_range, max_range + time_step, time_step):
        x_axis.append(i)

    apps_dict = dict()
    for i in range(len(df_task)):
        row = df_task.iloc[i]
        apps_dict[row['task_id']] = []

    def y_axis_setup():
        items = []

        for app, tasks in apps_dict.items():
            tmp = []
            if option == 'avg':
                task = df_resources[df_resources['task_id'] == app]['psutil_process_time_system'].astype('float').mean()
            elif option == 'max':
                task = max(df_resources[df_resources['task_id'] == app]['psutil_process_time_system'].astype('float'))

            for i in range(len(x_axis) - 1):
                a = task >= x_axis[i]
                b = task < x_axis[i + 1]
                tmp.append(a & b)
            items = np.sum([items, tmp], axis=0)
        return items

    fig = go.Figure(
        data=[go.Bar(x=x_axis[:-1],
                     y=y_axis_setup(),
                     name='tasks')],
        layout=go.Layout(xaxis=dict(autorange=True,
                                    title='Duration (seconds)'),
                         yaxis=dict(title='Tasks'),
                         title='System Time Distribution ' + '(' + option + ')'))

    return plot(fig, show_link=False, output_type="div", include_plotlyjs=False)
