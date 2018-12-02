import dash_html_components as html
import dash_core_components as dcc
from datetime import datetime


DB_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


def timestamp_to_float(time, format=DB_DATE_FORMAT):
    return datetime.strptime(time, format).timestamp()


def timestamp_to_int(time, format=DB_DATE_FORMAT):
    return int(timestamp_to_float(time))


def num_to_timestamp(n):
    return datetime.fromtimestamp(n)


def dropdown(id, dataframe, field):
    options = []

    latest = dataframe['run_id'].iloc[0]
    options.append({'label': dataframe[field].iloc[0].split('/').pop() + ' (Latest)', 'value': latest})

    for i in range(1, len(dataframe)):
        run_id = dataframe['run_id'].iloc[i]
        options.append({'label': dataframe[field].iloc[i].split('/').pop(), 'value': run_id})

    return dcc.Dropdown(
        id=id,
        options=options,
        value=latest,
        style=dict(width='200px', display='inline-block')
    )
