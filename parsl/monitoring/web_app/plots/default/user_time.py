import numpy as np
import pandas as pd
import plotly.graph_objs as go
import dash_html_components as html
import dash_core_components as dcc
from parsl.monitoring.web_app.utils import timestamp_to_float
from parsl.monitoring.web_app.app import get_db, close_db
from parsl.monitoring.web_app.plots.base_plot import BasePlot


class UserTimePlot(BasePlot):
    def __init__(self, plot_id, plot_args):
        super().__init__(plot_id, plot_args)

    def setup(self, args):
        return []

    def plot(self, run_id):
        sql_conn = get_db()
        df_resources = pd.read_sql_query('SELECT psutil_process_time_user, timestamp, task_id FROM task_resources WHERE run_id=(?)',
                                         sql_conn, params=(run_id, ))
        df_task = pd.read_sql_query('SELECT task_id, task_time_returned FROM task WHERE run_id=(?)',
                                    sql_conn, params=(run_id, ))
        close_db()

        def y_axis_setup():
            dic = dict()
            count = 0
            items = []

            for i in range(len(df_resources)):
                task_id = df_resources.iloc[i]['task_id']
                value = float(df_resources.iloc[i]['psutil_process_time_user'])

                if task_id in dic:
                    count -= dic[task_id][0]

                dic[task_id] = (value, float(df_task[df_task['task_id'] == task_id]['task_time_returned'].iloc[0]))
                count += value

                remove = []
                for k, v in dic.items():
                    if v[1] < timestamp_to_float(df_resources.iloc[i]['timestamp']):
                        count -= v[0]
                        remove.append(k)

                for k in remove:
                    del dic[k]

                items.append(count)

            return items

        return go.Figure(
            data=[go.Scatter(x=df_resources['timestamp'],
                             y=y_axis_setup(),
                             name='tasks')],
            layout=go.Layout(xaxis=dict(tickformat='%m-%d\n%H:%M:%S',
                                        autorange=True,
                                        title='Time'),
                             yaxis=dict(title='Duration (seconds)'),
                             title='User time'))


class UserTimeDistributionPlot(BasePlot):
    def __init__(self, plot_id, plot_args):
        super().__init__(plot_id, plot_args)

    def setup(self, args):
        return [dcc.RadioItems(id='user_time_distribution_radio_items',
                               options=[{'label': 'Average', 'value': 'avg'},
                                        {'label': 'Max', 'value': 'max'}],
                               value='avg'),
                html.Label(htmlFor='user_time_distribution_columns', children='Columns'),
                dcc.Input(id='user_time_distribution_columns', type='number', min=1, value=20)]

    def plot(self, option, columns, run_id):
        sql_conn = get_db()
        df_resources = pd.read_sql_query('SELECT psutil_process_time_user, timestamp, task_id FROM task_resources WHERE run_id=(?)',
                                         sql_conn, params=(run_id, ))
        df_task = pd.read_sql_query('SELECT task_id, task_func_name FROM task WHERE run_id=(?)',
                                    sql_conn, params=(run_id, ))
        close_db()

        min_range = min(df_resources['psutil_process_time_user'].astype('float'))
        max_range = max(df_resources['psutil_process_time_user'].astype('float'))
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
                    task = df_resources[df_resources['task_id'] == app]['psutil_process_time_user'].astype('float').mean()
                elif option == 'max':
                    task = max(df_resources[df_resources['task_id'] == app]['psutil_process_time_user'].astype('float'))

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
                                        title='Duration (seconds)'),
                             yaxis=dict(title='Tasks'),
                             title='User Time Distribution'))
