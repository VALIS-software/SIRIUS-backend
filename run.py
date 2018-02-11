#!/usr/bin/env python

from app import app
from app.prepare.update_valis_webfront import update_valis_webfront

update_valis_webfront()

if __name__ == "__main__":
    # Only for debugging while developing
    app.run(debug=True, use_reloader=False, host="0.0.0.0", port=5000)
