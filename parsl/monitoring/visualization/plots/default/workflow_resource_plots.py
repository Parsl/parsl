import numpy as np
import pandas as pd
import plotly.graph_objs as go
from plotly.offline import plot


def resource_distribution_plot(df_resources, df_task, type='psutil_process_time_user', label='CPU Time Distribution', option='avg', columns=20,):
    # E.g., psutil_process_time_user or psutil_process_memory_percent

    min_range = min(df_resources[type].astype('float'))
    max_range = max(df_resources[type].astype('float'))
    time_step = (max_range - min_range) / columns

    if min_range == max_range:
        x_axis = [min_range]
    else:
        x_axis = []
        for i in np.arange(min_range, max_range + time_step, time_step):
            x_axis.append(i)

    apps_dict = dict()
    for i in range(len(df_task)):
        row = df_task.iloc[i]
        apps_dict[row['task_id']] = []

    def y_axis_setup():
        items = [0] * len(x_axis)

        for app, tasks in apps_dict.items():
            if option == 'avg':
                task = df_resources[df_resources['task_id'] ==
                                    app][type].astype('float').mean()
            elif option == 'max':
                task = max(
                    df_resources[df_resources['task_id'] == app][type].astype('float'))

            for i in range(len(x_axis) - 1):
                a = task >= x_axis[i]
                b = task < x_axis[i + 1]
                if a and b:
                    items[i] += 1
            if task >= x_axis[-1]:
                items[-1] += 1
        return items

    if "memory" not in type:
        xaxis = dict(autorange=True,
                     title='CPU user time (seconds)')
    else:
        xaxis = dict(autorange=True,
                     title='Memory usage (bytes)')
    fig = go.Figure(
        data=[go.Bar(x=x_axis,
                     y=y_axis_setup(),
                     name='tasks')],
        layout=go.Layout(xaxis=xaxis,
                         yaxis=dict(title='Tasks'),
                         title=label + '(' + option + ')'))

    return plot(fig, show_link=False, output_type="div", include_plotlyjs=False)


def resource_time_series(tasks, type='psutil_process_time_user', label='CPU user time'):
    tasks['epoch_time'] = (pd.to_datetime(
        tasks['timestamp']) - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')
    step = int(tasks['resource_monitoring_interval'][0])
    start = tasks['epoch_time'].min()
    end = tasks['epoch_time'].max()
    tasks['relative_time'] = tasks['epoch_time'] - start
    if end != start:
        bins = pd.cut(tasks['relative_time'],
                      range(0, end - start + 1, step),
                      include_lowest=True)
        df = tasks.groupby(bins, as_index=False)[type].mean()
        df['time'] = step * df.index
        fig = go.Figure(
            data=[go.Scatter(x=df['time'],
                             y=df[type],
                             )],
            layout=go.Layout(xaxis=dict(autorange=True,
                                        title='Time (seconds)'),
                             yaxis=dict(title=label),
                             title=label))
    else:
        fig = go.Figure(
            data=[go.Scatter(x=[0],
                             y=[tasks[type].mean()],
                             )],
            layout=go.Layout(xaxis=dict(autorange=True,
                                        title='Time (seconds)'),
                             yaxis=dict(title=label),
                             title=label))
    return plot(fig, show_link=False, output_type="div", include_plotlyjs=False)
