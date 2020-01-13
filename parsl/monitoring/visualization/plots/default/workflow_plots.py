import math
import numpy as np
import pandas as pd
import plotly.graph_objs as go
import plotly.figure_factory as ff
from plotly.offline import plot
import networkx as nx
import datetime

from parsl.monitoring.visualization.utils import timestamp_to_int, num_to_timestamp, DB_DATE_FORMAT


def task_gantt_plot(df_task, time_completed=None):

    df_task = df_task.sort_values(by=['task_time_submitted'], ascending=False)

    parsl_tasks = []
    for i, task in df_task.iterrows():
        time_running, time_returned = task['task_time_running'], task['task_time_returned']
        if task['task_time_returned'] is None:
            time_returned = datetime.datetime.now()
            if time_completed is not None:
                time_returned = time_completed
        if task['task_time_submitted'] is not None:
            time_submitted = task['task_time_submitted']
        else:
            time_submitted = time_returned
        if task['task_time_running'] is None:
            time_running = time_submitted
        description = "Task ID: {}, app: {}".format(task['task_id'], task['task_func_name'])
        dic1 = dict(Task=description, Start=time_submitted,
                    Finish=time_running, Resource="Pending")
        dic2 = dict(Task=description, Start=time_running,
                    Finish=time_returned, Resource="Running")
        parsl_tasks.extend([dic1, dic2])
    colors = {'Pending': 'rgb(168, 168, 168)', 'Running': 'rgb(0, 0, 255)'}
    fig = ff.create_gantt(parsl_tasks,
                          title="",
                          colors=colors,
                          group_tasks=True,
                          show_colorbar=True,
                          index_col='Resource',
                          )
    fig['layout']['yaxis']['title'] = 'Task'
    fig['layout']['yaxis']['showticklabels'] = False
    fig['layout']['xaxis']['title'] = 'Time'
    return plot(fig, show_link=False, output_type="div", include_plotlyjs=False)


def task_per_app_plot(task, status):

    try:
        task['epoch_time_running'] = (pd.to_datetime(
            task['task_time_running']) - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')
        task['epoch_time_returned'] = (pd.to_datetime(
            task['task_time_returned']) - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')
        start = int(task['epoch_time_running'].min())
        end = int(task['epoch_time_returned'].max())
        tasks_per_app = {}
        all_tasks = [0] * (end - start + 1)
        for i, row in task.iterrows():
            if math.isnan(row['epoch_time_running']):
                # Skip rows with no running start time.
                continue
            if row['task_func_name'] not in tasks_per_app:
                tasks_per_app[row['task_func_name']] = [0] * (end - start + 1)
            for j in range(int(row['epoch_time_running']) + 1, int(row['epoch_time_returned']) + 1):
                tasks_per_app[row['task_func_name']][j - start] += 1
                all_tasks[j - start] += 1
        fig = go.Figure(
            data=[go.Scatter(x=list(range(0, end - start + 1)),
                             y=tasks_per_app[app],
                             name=app,
                             ) for app in tasks_per_app] +
                 [go.Scatter(x=list(range(0, end - start + 1)),
                             y=all_tasks,
                             name='All',
                             )],
            layout=go.Layout(xaxis=dict(autorange=True,
                                        title='Time (seconds)'),
                             yaxis=dict(title='Number of tasks'),
                             title="Tasks per app"))
        return plot(fig, show_link=False, output_type="div", include_plotlyjs=False)
    except Exception as e:
        return "The tasks per app plot cannot be generated because of exception {}.".format(e)


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
                                             text='Total Done: ' +
                                             str(sum(y_axis_done)),
                                             xref='paper',
                                             yref='paper'
                                         ),
                                         dict(
                                             x=0,
                                             y=1.05,
                                             showarrow=False,
                                             text='Total Failed: ' +
                                             str(sum(y_axis_failed)),
                                             xref='paper',
                                             yref='paper'
                                         ),
                    ],
        barmode='stack',
        title="Total tasks"))

    return plot(fig, show_link=False, output_type="div", include_plotlyjs=False)


def workflow_dag_plot(df_tasks, group_by_apps=True):
    G = nx.DiGraph(directed=True)
    nodes = df_tasks['task_id'].unique()
    dic = df_tasks.set_index('task_id').to_dict()
    G.add_nodes_from(nodes)

    # Add edges or links between the nodes:
    edges = []
    for k, v in dic['task_depends'].items():
        if v:
            adj = v.split(",")
            for e in adj:
                edges.append((int(e), k))
    G.add_edges_from(edges)

    node_positions = nx.nx_pydot.pydot_layout(G, prog='dot')

    if group_by_apps:
        groups_list = {app: (i, None) for i, app in enumerate(
            df_tasks['task_func_name'].unique())}
    else:
        groups_list = {'Pending': (0, 'gray'), "Running": (1, 'blue'), 'Completed': (2, 'green'), 'Unknown': (3, 'red')}

    node_traces = [...] * len(groups_list)

    for k, (index, color) in groups_list.items():
        node_trace = go.Scatter(
            x=[],
            y=[],
            text=[],
            mode='markers',
            textposition='top center',
            textfont=dict(
                family='arial',
                size=18,
                color='rgb(0,0,0)'
            ),
            hoverinfo='text',
            name=k,          # legend app_name here
            marker=dict(
                showscale=False,
                color=color,
                size=11,
                line=dict(width=1, color='rgb(0,0,0)')))
        node_traces[index] = node_trace

    for node in node_positions:
        x, y = node_positions[node]
        if group_by_apps:
            name = dic['task_func_name'][node]
        else:
            if dic['task_time_returned'][node] is not None:
                name = 'Completed'
            elif dic['task_time_running'][node] is not None:
                name = "Running"
            elif dic['task_time_submitted'][node] is not None:
                name = "Pending"
            else:
                name = "Unknown"
        index, _ = groups_list[name]
        node_traces[index]['x'] += tuple([x])
        node_traces[index]['y'] += tuple([y])
        node_traces[index]['text'] += tuple(
            ["{}:{}".format(dic['task_func_name'][node], node)])

    # The edges will be drawn as lines:
    edge_trace = go.Scatter(
        x=[],
        y=[],
        line=dict(width=1, color='rgb(160,160,160)'),
        hoverinfo='none',
        # showlegend=False,
        name='Dependency',
        mode='lines')

    for edge in G.edges:
        x0, y0 = node_positions[edge[0]]
        x1, y1 = node_positions[edge[1]]
        edge_trace['x'] += tuple([x0, x1, None])
        edge_trace['y'] += tuple([y0, y1, None])

    # Create figure:
    fig = go.Figure(data=[edge_trace] + node_traces,
                    layout=go.Layout(
                    title='Workflow DAG',
                    titlefont=dict(size=16),
                    showlegend=True,
                    hovermode='closest',
                    margin=dict(b=20, l=5, r=5, t=40),   # noqa: E741
                    xaxis=dict(showgrid=False, zeroline=False,
                               showticklabels=False),
                    yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)))
    return plot(fig, show_link=False, output_type="div", include_plotlyjs=False)
