import pandas as pd
import numpy as np
import plotly.graph_objs as go
import plotly.figure_factory as ff
from plotly.offline import plot
from utils import timestamp_to_int, num_to_timestamp, DB_DATE_FORMAT


def task_gantt_plot(df_task):

    df_task = df_task.sort_values(by=['task_time_submitted'], ascending=False)

    #df_task['task_time_submitted'] = pd.to_datetime(df_task['task_time_submitted'], unit='s')
    #df_task['task_time_returned'] = pd.to_datetime(df_task['task_time_returned'], unit='s')

    df_task = df_task.rename(index=str, columns={"task_id": "Task",
                                                 "task_time_submitted": "Start",
                                                 "task_time_returned": "Finish"})
    parsl_tasks = df_task.to_dict('records')

    fig = ff.create_gantt(parsl_tasks,
                          title="",
                          )

    return plot(fig, show_link=False, output_type="div", include_plotlyjs=False)


def task_per_app_plot(df_task, df_status):

    def y_axis_setup(array):
        count = 0
        items = []
        for n in array:
            if n:
                count += 1
            elif count > 0:
                count -= 1
            items.append(count)
        print(items)
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


def total_tasks_plot(df_task, df_status, columns=20):

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
