#!/usr/bin/env python

from flask import Flask
from flask_cors import CORS
# need to define app before importing other modules
app = Flask(__name__, static_folder='valis-static/dist')
CORS(app)

from .prepare import update_valis_webfront, copy_pyensembl_cache
from .core import views
from .mongo import mongo_util

def init():
    try:
        update_valis_webfront.update_valis_webfront()
        copy_pyensembl_cache.copy_pyensembl_cache()
    except Exception as e:
        print(str(e))

init()

if __name__ == "__main__":
    # Only for debugging while developing
    app.run(debug=True, use_reloader=False, host="0.0.0.0", port=5000)
