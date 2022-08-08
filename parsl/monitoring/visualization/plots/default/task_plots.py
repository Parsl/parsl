import plotly.graph_objs as go
from plotly.offline import plot


def time_series_cpu_per_task_plot(df_resources, resource_type, label):
    yaxis = dict(title='Accumulated CPU user time (seconds)')
    fig = go.Figure(data=[go.Scatter(x=df_resources['timestamp'],
                                     y=df_resources[resource_type])],
                    layout=go.Layout(xaxis=dict(tickformat='%m-%d\n%H:%M:%S',
                                                autorange=True,
                                                title='Time'),
                                     yaxis=yaxis,
                                     title=label))

    return plot(fig, show_link=False, output_type="div", include_plotlyjs=False)


def time_series_memory_per_task_plot(df_resources, resource_type, label):
    if resource_type == "psutil_process_memory_percent":
        yaxis = dict(title="Memory utilization")
        data = [go.Scatter(x=df_resources['timestamp'],
                           y=df_resources[resource_type])]
    else:
        yaxis = dict(title='Memory usage (GB)')
        data = [go.Scatter(x=df_resources['timestamp'],
                           y=[num / 1000000000 for num in df_resources[resource_type].astype(float)])]
    fig = go.Figure(data=data,
                    layout=go.Layout(xaxis=dict(tickformat='%m-%d\n%H:%M:%S',
                                                autorange=True,
                                                title='Time'),
                                     yaxis=yaxis,
                                     title=label))
    return plot(fig, show_link=False, output_type="div", include_plotlyjs=False)
