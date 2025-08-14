from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField
from wtforms.validators import DataRequired


class FileForm(FlaskForm):
    file_name = StringField('File Name', validators=[DataRequired()])
    submit = SubmitField('Submit')
