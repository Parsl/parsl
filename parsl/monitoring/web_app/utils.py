import dash_html_components as html
import dash_core_components as dcc
import pandas as pd
from parsl.monitoring.web_app.app import app, get_db


# TODO Add parameter to choose column in dataframe instead of hardcoded /workflows/'run_id'
def dataframe_to_table(id, dataframe):
    return html.Table(id=id, children=(
        [html.Tr([html.Th(col) for col in dataframe.columns])] +

        # Body
        [html.Tr([
            html.Td(html.A(children=dataframe.iloc[i][col], href='/workflows/' + dataframe['run_id'].iloc[i])) for col in dataframe.columns
        ]) for i in range(len(dataframe))])
    )


def dropdown(id):
    sql_conn = get_db()

    dataframe = pd.read_sql_query("SELECT run_id FROM workflows", sql_conn)

    options = [{'label': 'Select Workflow', 'value': ''}]

    for i in range(len(dataframe)):
        run_id = dataframe['run_id'].iloc[i]
        options.append({'label': run_id, 'value': run_id})

    return dcc.Dropdown(
        id=id,
        options=options,
        value=''
    )