import dash_html_components as html
from dash.dependencies import Input, Output, State
from parsl.monitoring.web_app.app import app
from parsl.monitoring.web_app.plots.default.memory_usage import MemoryUsageDistributionPlot, \
    VirtualMemoryUsageDistributionPlot, ResidentMemoryUsageDistributionPlot
from parsl.monitoring.web_app.plots.default.system_time import SystemTimeDistributionPlot
from parsl.monitoring.web_app.plots.default.task_per_app import TaskPerAppPlot
from parsl.monitoring.web_app.plots.default.total_tasks import TotalTasksPlot
from parsl.monitoring.web_app.plots.default.user_time import UserTimeDistributionPlot

layout = html.Div(id='workflow_details')


tasks_per_app_plot = \
    TaskPerAppPlot('task_per_app_plot_workflow',
                   plot_args=([Input('setup', 'children')],
                              [State('run_id', 'children')]))

total_tasks_plot = \
    TotalTasksPlot('total_tasks_plot_workflow',
                   plot_args=([Input('bin_width_minutes', 'value'),
                               Input('bin_width_seconds', 'value')],
                              [State('run_id', 'children')]))

user_time_distribution_plot = \
    UserTimeDistributionPlot('user_time_distribution_plot_workflow',
                             plot_args=([Input('user_time_distribution_radio_items', 'value'),
                                         Input('user_time_distribution_columns', 'value')],
                                        [State('run_id', 'children')]))

system_time_distribution_plot = \
    SystemTimeDistributionPlot('system_time_distribution_plot_workflow',
                               plot_args=([Input('system_time_distribution_radio_items', 'value'),
                                           Input('system_time_distribution_columns', 'value')],
                                          [State('run_id', 'children')]))

memory_usage_distribution_plot = \
    MemoryUsageDistributionPlot('memory_usage_distribution_plot_workflow',
                                plot_args=([Input('memory_usage_distribution_radio_items', 'value'),
                                            Input('memory_usage_distribution_columns', 'value')],
                                           [State('run_id', 'children')]))

virtual_usage_distribution_plot = \
    VirtualMemoryUsageDistributionPlot('virtual_memory_distribution_plot_workflow',
                                       plot_args=([Input('virtual_memory_distribution_radio_items', 'value'),
                                                   Input('virtual_memory_distribution_columns', 'value')],
                                                  [State('run_id', 'children')]))

resident_usage_distribution_plot = \
    ResidentMemoryUsageDistributionPlot('resident_memory_distribution_plot_workflow',
                                        plot_args=([Input('resident_memory_distribution_radio_items', 'value'),
                                                    Input('resident_memory_distribution_columns', 'value')],
                                                   [State('run_id', 'children')]))

plots = [tasks_per_app_plot,
         total_tasks_plot,
         user_time_distribution_plot,
         system_time_distribution_plot,
         memory_usage_distribution_plot,
         virtual_usage_distribution_plot,
         resident_usage_distribution_plot]


@app.callback(Output('workflow_details', 'children'),
              [Input('run_id', 'children')])
def workflow_details(run_id):
    return [html.P(className='view_title', children='Workflow view')] + [plot.html(run_id) for plot in plots]
