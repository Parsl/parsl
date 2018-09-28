import sqlite3
import dash_html_components as html
import dash_core_components as dcc
import pandas as pd


def generate_table1(sql, max_rows=100):
    sql_conn = sqlite3.connect('parsl.db')
    dataframe = pd.read_sql_query(sql, sql_conn)

    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in dataframe.columns])] +

        # Body
        [html.Tr([
            html.Td(html.A(dataframe.iloc[i][col])) for col in dataframe.columns
        ]) for i in range(min(len(dataframe), max_rows))]
    )

def dropdown(sql, max_rows=100):
    sql_conn = sqlite3.connect('parsl.db')
    dataframe = pd.read_sql_query(sql, sql_conn)

    for row in dataframe:
        print(row)

    dcc.Dropdown(
        id='my-dropdown',
        options=[
            {'label': 'New York City', 'value': 'NYC'},
            {'label': 'Montreal', 'value': 'MTL'},
            {'label': 'San Francisco', 'value': 'SF'}
        ],
        value='NYC'
    ),

    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in dataframe.columns])] +

        # Body
        [html.Tr([
            html.Td(html.A(dataframe.iloc[i][col])) for col in dataframe.columns
        ]) for i in range(min(len(dataframe), max_rows))]
    )