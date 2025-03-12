import math

import numpy as np
import pandas as pd
import plotly.graph_objs as go
from plotly.offline import plot


def resource_distribution_plot(df_resources, df_task, type, label, option, columns=20,):
    assert type == "psutil_process_time_user" or type == "psutil_process_memory_resident"
    assert option == "avg" or option == "max"

    min_range = min(df_resources[type].astype('float'))
    max_range = max(df_resources[type].astype('float'))
    time_step = (max_range - min_range) / columns

    if min_range == max_range:
        x_axis = [min_range]
    else:
        x_axis = []
        for i in np.arange(min_range, max_range + time_step, time_step):
            x_axis.append(i)

    tasks_dict = dict()
    for i in range(len(df_task)):
        row = df_task.iloc[i]
        tasks_dict[row['task_id']] = []

    def y_axis_setup():
        items = [0] * len(x_axis)

        for task_id, tasks in tasks_dict.items():
            if option == 'avg':
                task = df_resources[df_resources['task_id'] ==
                                    task_id][type].astype('float').mean()
            elif option == 'max':
                task = df_resources[df_resources['task_id'] == task_id][type].astype('float').max()
            else:
                raise ValueError(f"Cannot plot unknown aggregation option {option}")

            for i in range(len(x_axis) - 1):
                a = task >= x_axis[i]
                b = task < x_axis[i + 1]
                if a and b:
                    items[i] += 1
                    break
            if task >= x_axis[-1]:
                items[-1] += 1
        return items

    if type == "psutil_process_time_user":
        xaxis = dict(autorange=True,
                     title='CPU user time (seconds)')
    elif type == "psutil_process_memory_resident":
        xaxis = dict(autorange=True,
                     title='Memory usage (bytes)')
    else:
        raise ValueError(f"Cannot plot unknown type {type}")

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


def worker_efficiency(task, node):
    try:
        node['epoch_time'] = (pd.to_datetime(
            node['timestamp']) - pd.Timestamp("1970-01-01")) / pd.Timedelta('1s')
        task['epoch_time_start'] = (pd.to_datetime(
            task['task_try_time_launched']) - pd.Timestamp("1970-01-01")) / pd.Timedelta('1s')
        task['epoch_time_running'] = (pd.to_datetime(
            task['task_try_time_running']) - pd.Timestamp("1970-01-01")) / pd.Timedelta('1s')
        task['epoch_time_returned'] = (pd.to_datetime(
            task['task_try_time_returned']) - pd.Timestamp("1970-01-01")) / pd.Timedelta('1s')
        start = int(min(task['epoch_time_start'].min(), node['epoch_time'].min()))
        end = int(task['epoch_time_returned'].max())

        # worker_plot will be used to make a histogram of worker usage, one bucket per
        # second
        worker_plot = [0] * (end - start + 1)
        total_workers = node['worker_count'].sum()

        for i, row in task.iterrows():
            if math.isnan(row['epoch_time_running']):
                # skip tasks with no running start time
                continue
            if math.isnan(row['epoch_time_returned']):
                # there is no end time for this, so we should assume the "end" time
                etr = end
            else:
                etr = row['epoch_time_returned']

            # there's a two cases here:
            # The task starts and ends in same one-second bucket:
            #   populate one bucket with the fraction of second between
            #   the start and end
            #
            # or
            #
            # The task starts in one bucket and ends in another:
            #   populate middle buckets with 1, and start/end buckets with fraction

            start_bucket = int(row['epoch_time_running']) - start
            end_bucket = int(etr) - start

            if start_bucket == end_bucket:
                fraction = etr - row['epoch_time_running']
                worker_plot[start_bucket] += fraction

            else:
                fraction_before = row['epoch_time_running'] - int(row['epoch_time_running'])
                worker_plot[start_bucket] += (1 - fraction_before)

                for j in range(start_bucket + 1, end_bucket):
                    worker_plot[j] += 1

                fraction_after = etr - int(etr)
                worker_plot[end_bucket] += fraction_after

        fig = go.Figure(
            data=[go.Scatter(x=list(range(0, end - start + 1)),
                             y=worker_plot,
                             name='Total busy workers',
                             ),
                  go.Scatter(x=list(range(0, end - start + 1)),
                             y=[total_workers] * (end - start + 1),
                             name='Total of workers in whole run',
                             )
                  ],
            layout=go.Layout(xaxis=dict(autorange=True,
                                        title='Time (seconds)'),
                             yaxis=dict(title='Number of workers'),
                             title="Worker efficiency"))
        return plot(fig, show_link=False, output_type="div", include_plotlyjs=False)
    except Exception as e:
        print(e)
        return "The worker efficiency plot cannot be generated due to missing data."


def resource_efficiency(resource, node, label):
    try:
        resource['epoch_time'] = (pd.to_datetime(
            resource['timestamp']) - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')
        node['epoch_time'] = (pd.to_datetime(
            node['timestamp']) - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')
        resource = resource.sort_values(by='epoch_time')
        start = min(resource['epoch_time'].min(), node['epoch_time'].min())
        end = resource['epoch_time'].max()
        resource['relative_time'] = resource['epoch_time'] - start
        node['relative_time'] = node['epoch_time'] - start
        resource['key'] = resource['task_id'].astype(str) + "-" + resource['try_id'].astype(str)

        task_plot = [0] * (end - start + 1)
        if label == 'CPU':
            total = node['cpu_count'].sum()
        elif label == 'mem':
            total = node['total_memory'].sum() / 1024 / 1024 / 1024

        resource['total_cpu_time'] = resource['psutil_process_time_user'] + resource['psutil_process_time_system']
        for key in resource['key'].unique():
            tmp = resource[resource['key'] == key]
            tmp['last_timestamp'] = tmp['relative_time'].shift(1)
            if label == 'CPU':
                tmp['last_cputime'] = tmp['total_cpu_time'].shift(1)
            for index, row in tmp.iterrows():
                if np.isnan(row['last_timestamp']):
                    continue
                for i in range(int(row['last_timestamp']), int(row['relative_time'])):
                    if label == 'CPU':
                        diff = (row['total_cpu_time'] - row['last_cputime']) / (row['relative_time'] - row['last_timestamp'])
                    elif label == 'mem':
                        diff = row['psutil_process_memory_resident'] / 1024 / 1024 / 1024
                    task_plot[i] += diff

        if label == 'CPU':
            name1 = 'Used CPU cores'
            name2 = 'Total CPU cores'
            yaxis = 'Number of CPU cores'
            title = 'CPU usage'
        elif label == 'mem':
            name1 = 'Used memory'
            name2 = 'Total memory'
            yaxis = 'Memory (GB)'
            title = 'Memory usage'

        fig = go.Figure(
            data=[go.Scatter(x=list(range(0, end - start + 1)),
                             y=task_plot,
                             name=name1,
                             ),
                  go.Scatter(x=list(range(0, end - start + 1)),
                             y=[total] * (end - start + 1),
                             name=name2,
                             )
                  ],
            layout=go.Layout(xaxis=dict(autorange=True,
                                        title='Time (seconds)'),
                             yaxis=dict(title=yaxis),
                             title=title))
        return plot(fig, show_link=False, output_type="div", include_plotlyjs=False)
    except Exception as e:
        print(e)
        return "The resource efficiency plot cannot be generated because of exception {}.".format(e)
