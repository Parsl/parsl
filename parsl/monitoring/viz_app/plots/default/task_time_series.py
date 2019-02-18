import pandas as pd
import plotly.graph_objs as go
from plotly.offline import plot
from parsl.monitoring.viz_app.views import get_db, close_db


def time_series_user_time_per_task_plot(run_id, task_id):
    sql_conn = get_db()
    df_resources = pd.read_sql_query('SELECT psutil_process_time_user, timestamp FROM task_resources WHERE run_id=(?) AND task_id=(?)',
                                     sql_conn, params=(run_id, task_id))

    close_db()

    fig = go.Figure(data=[go.Scatter(x=df_resources['timestamp'],
                                     y=df_resources['psutil_process_time_user'])],
                    layout=go.Layout(xaxis=dict(tickformat='%m-%d\n%H:%M:%S',
                                                autorange=True,
                                                title='Time'),
                                     yaxis=dict(title='User time (seconds)'),
                                     title="Time series - User Time"))

    return plot(fig, show_link=False, output_type="div", include_plotlyjs=False)


def time_series_system_time_per_task_plot(run_id, task_id):
    sql_conn = get_db()
    df_resources = pd.read_sql_query('SELECT psutil_process_time_system, timestamp FROM task_resources WHERE run_id=(?) AND task_id=(?)',
                                     sql_conn, params=(run_id, task_id))

    close_db()

    fig = go.Figure(data=[go.Scatter(x=df_resources['timestamp'],
                                      y=df_resources['psutil_process_time_system'])],
                    layout=go.Layout(xaxis=dict(tickformat='%m-%d\n%H:%M:%S',
                                                 autorange=True,
                                                 title='Time'),
                                      yaxis=dict(title='System time (seconds)'),
                                      title="Time series - System Time"))

    return plot(fig, show_link=False, output_type="div", include_plotlyjs=False)


def time_series_resident_memory_usage_per_task_plot(run_id, task_id):
    sql_conn = get_db()
    df_resources = pd.read_sql_query('SELECT psutil_process_memory_resident, timestamp FROM task_resources WHERE run_id=(?) AND task_id=(?)',
                                     sql_conn, params=(run_id, task_id))

    close_db()

    fig = go.Figure(data=[go.Scatter(x=df_resources['timestamp'],
                                      y=[num / 1000000000 for num in df_resources['psutil_process_memory_resident'].astype(float)])],
                    layout=go.Layout(xaxis=dict(tickformat='%m-%d\n%H:%M:%S',
                                                 autorange=True,
                                                 title='Time'),
                                      yaxis=dict(title='Resident memory usage (GB)'),
                                      title="Time series - Resident memory"))
    return plot(fig, show_link=False, output_type="div", include_plotlyjs=False)


def time_series_virtual_memory_usage_per_task_plot(run_id, task_id):
    sql_conn = get_db()
    df_resources = pd.read_sql_query('SELECT psutil_process_memory_virtual, timestamp FROM task_resources WHERE run_id=(?) AND task_id=(?)',
                                     sql_conn, params=(run_id, task_id))

    close_db()

    fig = go.Figure(data=[go.Scatter(x=df_resources['timestamp'],
                                      y=[num / 1000000000 for num in df_resources['psutil_process_memory_virtual'].astype(float)])],
                    layout=go.Layout(xaxis=dict(tickformat='%m-%d\n%H:%M:%S',
                                                 autorange=True,
                                                 title='Time'),
                                      yaxis=dict(title='Virtual memory usage (GB)'),
                                      title="Time series - Virtual memory"))

    return plot(fig, show_link=False, output_type="div", include_plotlyjs=False)
