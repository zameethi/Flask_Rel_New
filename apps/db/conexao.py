from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import os

port = 5000
app = Flask(__name__, template_folder='../../templates/',  static_folder=os.path.abspath('static'), static_url_path='/static')
app.config['SECRET_KEY'] = 'a really really really really long secret key'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///sortimento.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ECHO'] = False
app.config['SQLALCHEMY_POOL_RECYCLE'] = 10
app.jinja_env.add_extension('jinja2.ext.loopcontrols')
db = SQLAlchemy(app)

db.Model.metadata.reflect(db.engine)

