#!/usr/bin/env python

from flask import Flask
from flask_cors import CORS
# need to define app before importing other modules
app = Flask(__name__, static_folder='valis-dist/static')
CORS(app)

from sirius.core import views, auth0

if __name__ == "__main__":
    # Only for debugging while developing
    app.run(debug=True, use_reloader=False)
