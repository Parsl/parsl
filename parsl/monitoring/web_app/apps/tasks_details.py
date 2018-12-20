import pandas as pd
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from parsl.monitoring.web_app.plots.default.task_time_series import \
    TimeSeriesUserTimePerTaskPlot, \
    TimeSeriesSystemTimePerTaskPlot, \
    TimeSeriesResidentMemoryUsagePerTaskPlot, \
    TimeSeriesVirtualMemoryUsagePerTaskPlot
from parsl.monitoring.web_app.app import app, get_db, close_db

layout = html.Div(id='tasks_details')

user_time_time_series_per_task = \
    TimeSeriesUserTimePerTaskPlot('user_time_time_series_per_task',
                                  plot_args=([Input('tasks_dropdown', 'value')],
                                             [State('run_id', 'children')]))

system_time_time_series_per_task = \
    TimeSeriesSystemTimePerTaskPlot('system_time_time_series_per_task',
                                    plot_args=([Input('tasks_dropdown', 'value')],
                                               [State('run_id', 'children')]))

resident_memory_usage_time_series_per_task = \
    TimeSeriesResidentMemoryUsagePerTaskPlot('resident_memory_usage_time_series_per_task',
                                             plot_args=([Input('tasks_dropdown', 'value')],
                                                        [State('run_id', 'children')]))

virtual_memory_usage_time_series_per_task = \
    TimeSeriesVirtualMemoryUsagePerTaskPlot('virtual_memory_usage_time_series_per_task',
                                            plot_args=([Input('tasks_dropdown', 'value')],
                                                       [State('run_id', 'children')]))

plots = [user_time_time_series_per_task,
         system_time_time_series_per_task,
         resident_memory_usage_time_series_per_task,
         virtual_memory_usage_time_series_per_task]


@app.callback(Output('tasks_details', 'children'),
              [Input('run_id', 'children')])
def tasks_details(run_id):
    sql_conn = get_db()
    df_task = pd.read_sql_query('SELECT task_id, task_func_name FROM task WHERE run_id=(?)',
                                sql_conn, params=(run_id,))
    close_db()

    tasks = []
    for task_id in df_task['task_id']:
        tasks.append({'label': task_id, 'value': task_id})

    return [dcc.Dropdown(
        id='tasks_dropdown',
        options=tasks,
        value=df_task['task_id'][0],
        style=dict(width='200px', display='inline-block'))] + [plot.html(run_id) for plot in plots]
