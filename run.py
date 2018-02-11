#!/usr/bin/env python

from app import app
from app.prepare import update_valis_webfront, copy_pyensembl_cache

def init():
    try:
        update_valis_webfront.update_valis_webfront()
        copy_pyensembl_cache.copy_pyensembl_cache()
    except Exception as e:
        print(str(e))

if __name__ == "__main__":
    init()
    # Only for debugging while developing
    app.run(debug=True, use_reloader=False, host="0.0.0.0", port=5000)
