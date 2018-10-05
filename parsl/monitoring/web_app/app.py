import dash
from flask import request, g, current_app
import requests
import sqlite3
import os
import time

# TODO: Remove hardcoded database. Load dynamically if possible
# TODO: When there is no database, don't load it

app = dash.Dash(__name__)
app.config['suppress_callback_exceptions']=True

if os.path.isfile('parsl.db'):
    app.server.config.update(dict(DATABASE='parsl.db'))
else:
    app.server.config.update(dict(DATABASE=None))


def config_server(monitoring_config):
    db = monitoring_config.eng_link.split('/').pop()
    if os.path.isfile(db) or get_db() is not None:
        app.server.config.update(dict(DATABASE=db))
        return True
    else:
        return False


@app.server.route('/shutdown', methods=['POST'])
def shutdown():
    shutdown_server()
    return 'Server shutting down...'


def shutdown_web_app(host, port):
    print(host + ':' + str(port) + '/shutdown')
    print(requests.post(host + ':' + str(port) + '/shutdown', data=''))


def shutdown_server():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()


def get_db():
    with app.server.app_context():
        if 'db' not in g:
            if app.server.config['DATABASE'] is not None:
                g.db = sqlite3.connect(
                    app.server.config['DATABASE'],
                    detect_types=sqlite3.PARSE_DECLTYPES
                )

                g.db.row_factory = sqlite3.Row
            else:
                return None
        return g.db


def close_db(e=None):
    db = g.pop('db', None)

    if db is not None:
        db.close()


