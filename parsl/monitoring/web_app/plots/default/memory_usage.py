import numpy as np
import pandas as pd
import plotly.graph_objs as go
import dash_html_components as html
import dash_core_components as dcc
from parsl.monitoring.web_app.app import get_db, close_db
from parsl.monitoring.web_app.plots.base_plot import BasePlot


class MemoryUsageDistributionPlot(BasePlot):
    def __init__(self, plot_id, plot_args):
        super().__init__(plot_id, plot_args)

    def setup(self, args):
        return [dcc.RadioItems(id='memory_usage_distribution_radio_items',
                               options=[{'label': 'Average', 'value': 'avg'},
                                        {'label': 'Max', 'value': 'max'}],
                               value='avg'),
                html.Div(children=[html.Label(htmlFor='memory_usage_distribution_columns', children='Columns'),
                                   dcc.Input(id='memory_usage_distribution_columns', type='number', min=1, value=20)])]

    def plot(self, option, columns, run_id):
        sql_conn = get_db()
        df_resources = pd.read_sql_query('SELECT psutil_process_memory_percent, timestamp, task_id FROM task_resources WHERE run_id=(?)',
                                         sql_conn, params=(run_id, ))
        df_task = pd.read_sql_query('SELECT task_id, task_time_returned FROM task WHERE run_id=(?)',
                                    sql_conn, params=(run_id, ))
        close_db()

        min_range = min(df_resources['psutil_process_memory_percent'].astype('float'))
        max_range = max(df_resources['psutil_process_memory_percent'].astype('float'))
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
                    task = df_resources[df_resources['task_id'] == app]['psutil_process_memory_percent'].astype('float').mean()
                elif option == 'max':
                    task = max(df_resources[df_resources['task_id'] == app]['psutil_process_memory_percent'].astype('float'))

                for i in range(len(x_axis) - 1):
                    a = task >= x_axis[i]
                    b = task < x_axis[i + 1]
                    tmp.append(a & b)
                items = np.sum([items, tmp], axis=0)
            return items

        return go.Figure(
            data=[go.Bar(x=x_axis[:-1],
                         y=y_axis_setup(),
                         name='tasks')],
            layout=go.Layout(xaxis=dict(autorange=True,
                                        title='Usage (%)'),
                             yaxis=dict(title='Tasks'),
                             title='Memory Usage Distribution'))


class VirtualMemoryUsageDistributionPlot(BasePlot):
    def __init__(self, plot_id, plot_args):
        super().__init__(plot_id, plot_args)

    def setup(self, args):
        return [dcc.RadioItems(id='virtual_memory_distribution_radio_items',
                               options=[{'label': 'Average', 'value': 'avg'},
                                        {'label': 'Max', 'value': 'max'}],
                               value='avg'),
                html.Div(children=[html.Label(htmlFor='virtual_memory_distribution_columns', children='Columns'),
                                   dcc.Input(id='virtual_memory_distribution_columns', type='number', min=1, value=20)])]

    def plot(self, option, columns, run_id):
        sql_conn = get_db()
        df_resources = pd.read_sql_query('SELECT psutil_process_memory_virtual, timestamp, task_id FROM task_resources WHERE run_id=(?)',
                                         sql_conn, params=(run_id, ))
        df_task = pd.read_sql_query('SELECT task_id, task_time_returned FROM task WHERE run_id=(?)',
                                    sql_conn, params=(run_id, ))
        close_db()

        min_range = min(df_resources['psutil_process_memory_virtual'].astype('float'))
        max_range = max(df_resources['psutil_process_memory_virtual'].astype('float'))
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
                    task = df_resources[df_resources['task_id'] == app]['psutil_process_memory_virtual'].astype('float').mean()
                elif option == 'max':
                    task = max(df_resources[df_resources['task_id'] == app]['psutil_process_memory_virtual'].astype('float'))

                for i in range(len(x_axis) - 1):
                    a = task >= x_axis[i]
                    b = task < x_axis[i + 1]
                    tmp.append(a & b)
                items = np.sum([items, tmp], axis=0)
            return items

        return go.Figure(
            data=[go.Bar(x=[num / 1000000000 for num in x_axis[:-1]],
                         y=y_axis_setup(),
                         name='tasks')],
            layout=go.Layout(xaxis=dict(autorange=True,
                                        title='Usage (GB)'),
                             yaxis=dict(title='Tasks'),
                             title='Virtual Memory Distribution'))


class ResidentMemoryUsageDistributionPlot(BasePlot):
    def __init__(self, plot_id, plot_args):
        super().__init__(plot_id, plot_args)

    def setup(self, args):
        return [dcc.RadioItems(id='resident_memory_distribution_radio_items',
                               options=[{'label': 'Average', 'value': 'avg'},
                                        {'label': 'Max', 'value': 'max'}],
                               value='avg'),
                html.Div(children=[html.Label(htmlFor='resident_memory_distribution_columns', children='Columns'),
                                   dcc.Input(id='resident_memory_distribution_columns', type='number', min=1, value=20)])]

    def plot(self, option, columns, run_id):
        sql_conn = get_db()
        df_resources = pd.read_sql_query('SELECT psutil_process_memory_resident, timestamp, task_id FROM task_resources WHERE run_id=(?)',
                                         sql_conn, params=(run_id, ))
        df_task = pd.read_sql_query('SELECT task_id, task_time_returned FROM task WHERE run_id=(?)',
                                    sql_conn, params=(run_id, ))
        close_db()

        min_range = min(df_resources['psutil_process_memory_resident'].astype('float'))
        max_range = max(df_resources['psutil_process_memory_resident'].astype('float'))
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
                    task = df_resources[df_resources['task_id'] == app]['psutil_process_memory_resident'].astype('float').mean()
                elif option == 'max':
                    task = max(df_resources[df_resources['task_id'] == app]['psutil_process_memory_resident'].astype('float'))

                for i in range(len(x_axis) - 1):
                    a = task >= x_axis[i]
                    b = task < x_axis[i + 1]
                    tmp.append(a & b)
                items = np.sum([items, tmp], axis=0)
            return items

        return go.Figure(
            data=[go.Bar(x=[num / 1000000000 for num in x_axis[:-1]],
                         y=y_axis_setup(),
                         name='tasks')],
            layout=go.Layout(xaxis=dict(autorange=True,
                                        title='Usage (GB)'),
                             yaxis=dict(title='Tasks'),
                             title='Resident Memory Distribution'))
