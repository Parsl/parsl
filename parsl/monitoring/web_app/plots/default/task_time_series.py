import pandas as pd
import plotly.graph_objs as go
from parsl.monitoring.web_app.app import get_db, close_db
from parsl.monitoring.web_app.plots.base_plot import BasePlot


class TimeSeriesUserTimePerTaskPlot(BasePlot):
    def __init__(self, plot_id, plot_args):
        super().__init__(plot_id, plot_args)

    def setup(self, run_id):
        return []

    def plot(self, task_id, run_id):
        sql_conn = get_db()
        df_resources = pd.read_sql_query('SELECT psutil_process_time_user, timestamp FROM task_resources WHERE run_id=(?) AND task_id=(?)',
                                         sql_conn, params=(run_id, task_id))

        close_db()

        return go.Figure(data=[go.Scatter(x=df_resources['timestamp'],
                                          y=df_resources['psutil_process_time_user'])],
                         layout=go.Layout(xaxis=dict(tickformat='%m-%d\n%H:%M:%S',
                                                     autorange=True,
                                                     title='Time'),
                                          yaxis=dict(title='User time (seconds)'),
                                          title="Time series - User Time"))


class TimeSeriesSystemTimePerTaskPlot(BasePlot):
    def __init__(self, plot_id, plot_args):
        super().__init__(plot_id, plot_args)

    def setup(self, run_id):
        return []

    def plot(self, task_id, run_id):
        sql_conn = get_db()
        df_resources = pd.read_sql_query('SELECT psutil_process_time_system, timestamp FROM task_resources WHERE run_id=(?) AND task_id=(?)',
                                         sql_conn, params=(run_id, task_id))

        close_db()

        return go.Figure(data=[go.Scatter(x=df_resources['timestamp'],
                                          y=df_resources['psutil_process_time_system'])],
                         layout=go.Layout(xaxis=dict(tickformat='%m-%d\n%H:%M:%S',
                                                     autorange=True,
                                                     title='Time'),
                                          yaxis=dict(title='System time (seconds)'),
                                          title="Time series - System Time"))


class TimeSeriesResidentMemoryUsagePerTaskPlot(BasePlot):
    def __init__(self, plot_id, plot_args):
        super().__init__(plot_id, plot_args)

    def setup(self, run_id):
        return []

    def plot(self, task_id, run_id):
        sql_conn = get_db()
        df_resources = pd.read_sql_query('SELECT psutil_process_memory_resident, timestamp FROM task_resources WHERE run_id=(?) AND task_id=(?)',
                                         sql_conn, params=(run_id, task_id))

        close_db()

        return go.Figure(data=[go.Scatter(x=df_resources['timestamp'],
                                          y=[num / 1000000000 for num in df_resources['psutil_process_memory_resident'].astype(float)])],
                         layout=go.Layout(xaxis=dict(tickformat='%m-%d\n%H:%M:%S',
                                                     autorange=True,
                                                     title='Time'),
                                          yaxis=dict(title='Resident memory usage (GB)'),
                                          title="Time series - Resident memory"))


class TimeSeriesVirtualMemoryUsagePerTaskPlot(BasePlot):
    def __init__(self, plot_id, plot_args):
        super().__init__(plot_id, plot_args)

    def setup(self, run_id):
        return []

    def plot(self, task_id, run_id):
        sql_conn = get_db()
        df_resources = pd.read_sql_query('SELECT psutil_process_memory_virtual, timestamp FROM task_resources WHERE run_id=(?) AND task_id=(?)',
                                         sql_conn, params=(run_id, task_id))

        close_db()

        return go.Figure(data=[go.Scatter(x=df_resources['timestamp'],
                                          y=[num / 1000000000 for num in df_resources['psutil_process_memory_virtual'].astype(float)])],
                         layout=go.Layout(xaxis=dict(tickformat='%m-%d\n%H:%M:%S',
                                                     autorange=True,
                                                     title='Time'),
                                          yaxis=dict(title='Virtual memory usage (GB)'),
                                          title="Time series - Virtual memory"))
