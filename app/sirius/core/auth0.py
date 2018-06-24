from functools import wraps
import os, json
from flask import Flask, jsonify, redirect, session, url_for
from six.moves.urllib.parse import urlencode
from authlib.flask.client import OAuth

from sirius.main import app

app.secret_key = 'qydsecretkey'

oauth = OAuth(app)

auth0 = oauth.register(
    'auth0',
    client_id='UHugP5v627feBCWA6h4bLP3g__VCNGyL',
    client_secret='CzVyAa9Kk3Tt-ogjxYeHwYqbKAub1Rk5OISryB4krOBh4AXXr4r9GY136CqILyTk',
    api_base_url='https://valis-dev.auth0.com',
    access_token_url='https://valis-dev.auth0.com/oauth/token',
    authorize_url='https://valis-dev.auth0.com/authorize',
    client_kwargs={
        'scope': 'openid profile',
    },
)

@app.route('/callback')
def callback_handling():
    # Handles response from token endpoint
    auth0.authorize_access_token()
    resp = auth0.get('userinfo')
    userinfo = resp.json()

    # Store the user information in flask session.
    session['jwt_payload'] = userinfo
    session['profile'] = {
        'user_id': userinfo['sub'],
        'name': userinfo['name'],
        'picture': userinfo['picture']
    }
    return redirect('/')

@app.route('/login')
def login():
    return auth0.authorize_redirect(redirect_uri='http://localhost:5000/callback', audience='https://valis-dev.auth0.com/userinfo')

def requires_auth(f):
    # skip auth if in dev mode
    if os.environ.get('VALIS_DEV_MODE', None): return f
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'profile' not in session:
            # Redirect to Login page here
            return redirect('/login')
        return f(*args, **kwargs)
    return decorated

@app.route('/dashboard')
@requires_auth
def dashboard():
    return json.dumps(session['profile'])

@app.route('/logout')
def logout():
    # Clear session stored data
    session.clear()
    # Redirect user to logout endpoint
    # params = {'returnTo': url_for('login', _external=True), 'client_id': 'UHugP5v627feBCWA6h4bLP3g__VCNGyL'}
    return redirect(auth0.api_base_url + '/v2/logout')#?' + urlencode(params))



