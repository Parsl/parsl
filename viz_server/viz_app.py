#!flask/bin/python
import os
from flask import Flask
from flask_sqlalchemy import SQLAlchemy


"""
We need to make this a function that takes the monitoring db path
and does whatever logging it needs to do
"""

def initialize_db_app(db_path="sqlite:///monitoring.db", track_mods=False):

    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = db_path
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = track_mods

    global db = SQLAlchemy(app)

    return app, db


