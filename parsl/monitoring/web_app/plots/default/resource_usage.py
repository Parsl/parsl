import pandas as pd
import plotly.graph_objs as go
import dash_core_components as dcc
from parsl.monitoring.web_app.utils import timestamp_to_float
from parsl.monitoring.web_app.app import get_db, close_db
from parsl.monitoring.web_app.plots.base_plot import BasePlot


# TODO y_axis labels can't be set with callback from radioitems. Might have to split up the plots
# TODO return html.H3('No resource data found')
class ResourceUsagePlot(BasePlot):
    def __init__(self, plot_id, setup_args, plot_args):
        super().__init__(plot_id, setup_args, plot_args)

    def setup(self, args):
        return [dcc.RadioItems(id='resource_usage_radio_items',
                               options=[{'label': 'User time', 'value': 'psutil_process_time_user'},
                                        {'label': 'System time', 'value': 'psutil_process_time_system'},
                                        {'label': 'Memory usage', 'value': 'psutil_process_memory_percent'}],
                               value='psutil_process_time_user')]

    def plot(self, field, run_id):
        sql_conn = get_db()
        df_resources = pd.read_sql_query('SELECT {field}, timestamp, task_id FROM task_resources WHERE run_id=(?)'.format(field=field),
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
                value = float(df_resources.iloc[i][field])

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
                             title='Resource usage'))
