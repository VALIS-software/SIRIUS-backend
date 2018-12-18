from flask import Flask
from flask_cors import CORS
# need to define app before importing other modules
app = Flask(__name__)
CORS(app)