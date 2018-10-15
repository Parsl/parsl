import dash_html_components as html
import dash_core_components as dcc
import pandas as pd
from parsl.monitoring.web_app.app import app, get_db


# TODO Add parameter to choose column in dataframe instead of hardcoded /workflows/'run_id'
def dataframe_to_html_table(id, dataframe, field):
    return html.Table(id=id, children=(
        [html.Tr([html.Th(col) for col in dataframe.columns])] +

        # Body
        [html.Tr([
            html.Td(html.A(children=dataframe.iloc[i][col], href='/workflows/' + dataframe[field].iloc[i])) for col in dataframe.columns
        ]) for i in range(len(dataframe))])
    )


def dropdown(id, dataframe, field):
    options = []

    latest = dataframe['run_id'][0]
    options.append({'label': dataframe[field].iloc[0].split('/').pop() + ' (Latest)', 'value': latest})

    for i in range(1, len(dataframe)):
        run_id = dataframe['run_id'][i]
        options.append({'label': dataframe[field].iloc[i].split('/').pop(), 'value': run_id})

    return dcc.Dropdown(
        id=id,
        options=options,
        value=latest,
        style=dict(width='200px', display='inline-block')
    )