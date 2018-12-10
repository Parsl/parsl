import dash
from flask import request, g
import requests
import sqlite3
import os


app = dash.Dash(__name__)
app.config['suppress_callback_exceptions'] = True


def init_db(db):
    if os.path.isfile(db):
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
            g.db = sqlite3.connect(
                app.server.config['DATABASE'],
                detect_types=sqlite3.PARSE_DECLTYPES
            )

            g.db.row_factory = sqlite3.Row

        return g.db


def close_db():
    with app.server.app_context():
        db = g.pop('db', None)

        if db is not None:
            db.close()
