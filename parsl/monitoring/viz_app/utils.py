import os
import sqlite3
from flask import g


def init_db(app, db):
    if os.path.isfile(db):
        app.config.update(dict(DATABASE=db))
        return True
    else:
        return False


def get_db(app):
    with app.app_context():
        if 'db' not in g:
            g.db = sqlite3.connect(
                app.config['DATABASE'],
                detect_types=sqlite3.PARSE_DECLTYPES
            )

            g.db.row_factory = sqlite3.Row

        return g.db


def close_db(app):
    with app.app_context():
        db = g.pop('db', None)

        if db is not None:
            db.close()
