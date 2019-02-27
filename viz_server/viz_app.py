#!flask/bin/python
import os
from flask import Flask
from flask_sqlalchemy import SQLAlchemy


"""
We need to make this a function that takes the monitoring db path
and does whatever logging it needs to do
"""

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///monitoring.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)
