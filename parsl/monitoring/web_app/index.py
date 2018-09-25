import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

from parsl.monitoring.web_app.app import app
from parsl.monitoring.web_app.apps import app1

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])


@app.callback(Output('page-content', 'children'),
              [Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/apps/app1':
        return app1.layout
    else:
        return '404'


if __name__ == '__main__':
    app.run_server()
