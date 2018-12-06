import pandas as pd
import plotly.graph_objs as go
from parsl.monitoring.web_app.app import get_db, close_db
from parsl.monitoring.web_app.plots.base_plot import BasePlot


class TaskPerAppPlot(BasePlot):
    def __init__(self, plot_id, setup_args, plot_args):
        super().__init__(plot_id, setup_args, plot_args)

    def setup(self, args):
        return []

    def plot(self, run_id, apps=None):
        sql_conn = get_db()
        df_status = pd.read_sql_query('SELECT run_id, task_id, task_status_name, timestamp FROM task_status WHERE run_id=(?)',
                                      sql_conn, params=(run_id, ))

        if apps:
            if type(apps) is dict:
                apps = ['', apps['label']]
            elif len(apps) == 1:
                apps.append('')
            df_task = pd.read_sql_query('SELECT task_id, task_func_name FROM task WHERE run_id=(?) AND task_func_name IN {apps}'.format(apps=tuple(apps)),
                                        sql_conn, params=(run_id, ))
        else:
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
        apps = dict()
        for i in range(len(df_task)):
            row = df_task.iloc[i]
            if row['task_func_name'] in apps:
                apps[row['task_func_name']].append(row['task_id'])
            else:
                apps[row['task_func_name']] = [row['task_id']]

        return go.Figure(data=[go.Scatter(x=df_status[df_status['task_id'].isin(tasks)]['timestamp'],
                                          y=y_axis_setup(df_status[df_status['task_id'].isin(tasks)]['task_status_name'] == 'running'),
                                          name=app)
                               for app, tasks in apps.items()] +
                              [go.Scatter(x=df_status['timestamp'],
                                          y=y_axis_setup(df_status['task_status_name'] == 'running'),
                                          name='all')],
                         layout=go.Layout(xaxis=dict(tickformat='%m-%d\n%H:%M:%S',
                                                     autorange=True,
                                                     title='Time'),
                                          yaxis=dict(tickformat=',d',
                                                     title='Tasks'),
                                          hovermode='closest',
                                          title='Tasks per app')
                         )
