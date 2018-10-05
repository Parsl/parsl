import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from parsl.monitoring.web_app.app import app, init_db

import click


def web_app(db, port):
    if not init_db(db):
        return

    from parsl.monitoring.web_app.apps import sql, workflows

    app.layout = html.Div([
        html.Link(href='/assets/styles.css', rel='stylesheet'),
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
            dcc.Location(id='url', refresh=True, pathname="/workflows"),
        elif pathname == "/workflows":
            return workflows.layout
        elif pathname == "/sql":
            return sql.layout
        else:
            return '404'

    app.run_server(port=port, debug=True)


# TODO CSS is not being imported when ran through cli
@click.command()
@click.option('--db_dir', default='./', help='Database location directory')
@click.option('--db_name', default='parsl.db', help='Database name')
@click.option('--port', default=8050)
def cli_run(db_dir, db_name, port):
    web_app(db_dir + db_name, port)


def run(monitoring_config):
    db = monitoring_config.eng_link.split('/').pop()
    port = monitoring_config.web_app_port + 1
    web_app(db, port)


if __name__ == '__main__':
    cli_run()