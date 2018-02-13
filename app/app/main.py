#!/usr/bin/env python

from flask import Flask
from flask_cors import CORS
# need to define app before importing other modules
app = Flask(__name__, static_folder='valis-dist/static')
CORS(app)

from .core import views
from .mongo import mongo_util

if __name__ == "__main__":
    # Only for debugging while developing
    app.run(debug=True, use_reloader=False, host="0.0.0.0", port=5000)
