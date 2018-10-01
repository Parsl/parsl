import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

from parsl.monitoring.web_app.app import app
from parsl.monitoring.web_app.apps import workflows
from parsl.monitoring.web_app.apps import sql


app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Nav(children=[
        dcc.Link('Workflows', href='/workflows'),
        dcc.Link('SQL', href='/sql')]),
    html.Div(id='page-content')
])


@app.callback(Output('page-content', 'children'),
              [Input('url', 'pathname')])
def display_page(pathname):
    if pathname == "/":
        pass
    elif pathname == "/workflows":
        return workflows.layout
    elif pathname == "/sql":
        return sql.layout
    else:
        return '404'


def run(handler):
    print('db')
    print(handler)
    app.run_server()


if __name__ == '__main__':
    app.run_server()
