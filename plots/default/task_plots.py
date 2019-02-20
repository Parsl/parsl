import plotly.graph_objs as go
from plotly.offline import plot


def time_series_cpu_per_task_plot(df_resources, resource_type, label):

    fig = go.Figure(data=[go.Scatter(x=df_resources['timestamp'],
                                     y=df_resources[resource_type])],
                    layout=go.Layout(xaxis=dict(tickformat='%m-%d\n%H:%M:%S',
                                                autorange=True,
                                                title='Time'),
                                     yaxis=dict(title='CPU time (seconds)'),
                                     title=label))

    return plot(fig, show_link=False, output_type="div", include_plotlyjs=False)


def time_series_memory_per_task_plot(df_resources, resource_type, label):

    fig = go.Figure(data=[go.Scatter(x=df_resources['timestamp'],
                                      y=[num / 1000000000 for num in df_resources[resource_type].astype(float)])],
                    layout=go.Layout(xaxis=dict(tickformat='%m-%d\n%H:%M:%S',
                                                 autorange=True,
                                                 title='Time'),
                                      yaxis=dict(title='Memory usage (GB)'),
                                      title=label))
    return plot(fig, show_link=False, output_type="div", include_plotlyjs=False)