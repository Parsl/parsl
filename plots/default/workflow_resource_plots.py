import numpy as np
import pandas as pd
import plotly.graph_objs as go
from plotly.offline import plot


def resource_distribution_plot(df_resources, df_task, type='psutil_process_time_user', label='CPU Time Distribution', option='avg', columns=20,):
    # E.g., psutil_process_time_user or psutil_process_memory_percent

    min_range = min(df_resources[type].astype('float'))
    max_range = max(df_resources[type].astype('float'))
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
                task = df_resources[df_resources['task_id'] == app][type].astype('float').mean()
            elif option == 'max':
                task = max(df_resources[df_resources['task_id'] == app][type].astype('float'))

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
                         title=label + '(' + option + ')'))

    return plot(fig, show_link=False, output_type="div", include_plotlyjs=False)