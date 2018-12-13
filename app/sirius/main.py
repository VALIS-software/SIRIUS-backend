#!/usr/bin/env python

from sirius import app
from sirius.core import views, auth0

if __name__ == "__main__":
    # Only for debugging while developing
    app.run(debug=True, use_reloader=False)
