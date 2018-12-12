# auth0.py
# reference: https://auth0.com/docs/quickstart/backend/python/01-authorization
import os
import json
from six.moves.urllib.request import urlopen
from functools import wraps
import requests
from flask import request, jsonify, _request_ctx_stack
from flask_cors import cross_origin
from jose import jwt

from sirius.main import app
from sirius.core.utilities import threadsafe_ttl_cache

AUTH0_DOMAIN = "valis-dev.auth0.com"
API_AUDIENCE = 'https://api.valis.bio/'
ALGORITHMS = ["RS256"]


# Error handler
class AuthError(Exception):
    def __init__(self, error, status_code):
        self.error = error
        self.status_code = status_code

@app.errorhandler(AuthError)
def handle_auth_error(ex):
    response = jsonify(ex.error)
    response.status_code = ex.status_code
    return response

def get_token_auth_header():
    """Obtains the Access Token from the Authorization Header
    """
    auth = request.headers.get("Authorization", None)
    if not auth:
        raise AuthError({"code": "authorization_header_missing",
                        "description":
                            "Authorization header is expected"}, 401)
    parts = auth.split()
    if parts[0].lower() != "bearer":
        raise AuthError({"code": "invalid_header",
                        "description":
                            "Authorization header must start with"
                            " Bearer"}, 401)
    elif len(parts) == 1:
        raise AuthError({"code": "invalid_header",
                        "description": "Token not found"}, 401)
    elif len(parts) > 2:
        raise AuthError({"code": "invalid_header",
                        "description":
                            "Authorization header must be"
                            " Bearer token"}, 401)
    token = parts[1]
    return token

def requires_auth(f):
    """Determines if the Access Token is valid
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        token = get_token_auth_header()
        print(auth_token_payload.cache_info())
        payload = auth_token_payload(token)
        _request_ctx_stack.top.current_user = payload
        return f(*args, **kwargs)
    return decorated

# Note: here we use a time-to-live cache, and the timeout value 86400 is consistent with the token expiration
@threadsafe_ttl_cache(maxsize=10000, ttl=86400)
def auth_token_payload(token):
    """ cached function to reduce number of calls to the auth0 server """
    jsonurl = urlopen("https://"+AUTH0_DOMAIN+"/.well-known/jwks.json")
    jwks = json.loads(jsonurl.read())
    unverified_header = jwt.get_unverified_header(token)
    rsa_key = {}
    for key in jwks["keys"]:
        if key["kid"] == unverified_header["kid"]:
            rsa_key = {
                "kty": key["kty"],
                "kid": key["kid"],
                "use": key["use"],
                "n": key["n"],
                "e": key["e"]
            }
    if rsa_key:
        try:
            payload = jwt.decode(
                token,
                rsa_key,
                algorithms=ALGORITHMS,
                audience=API_AUDIENCE,
                issuer="https://"+AUTH0_DOMAIN+"/"
            )
        except jwt.ExpiredSignatureError:
            raise AuthError({"code": "token_expired",
                            "description": "token is expired"}, 401)
        except jwt.JWTClaimsError:
            raise AuthError({"code": "invalid_claims",
                            "description":
                                "incorrect claims,"
                                "please check the audience and issuer"}, 401)
        except Exception:
            raise AuthError({"code": "invalid_header",
                            "description":
                                "Unable to parse authentication"
                                " token."}, 401)
        # If we're on a dev server, only developers are given access
        # A rule is created on auth0.com to add this custom field
        # ref: https://auth0.com/docs/api-auth/tutorials/adoption/scope-custom-claims#custom-claims
        if os.environ.get('VALIS_DEV_MODE'):
            if payload.get('https://valis.bio/role') != 'developer':
                raise AuthError({
                    'code': 'insufficient_permission',
                    'discription': 'Dev server is only accessible to developers',
                }, 401)
        # use the access token to get the user profile
        if "name" not in payload:
            payload = request_user_profile(payload, token)
        _request_ctx_stack.top.current_user = payload
    else:
        raise AuthError({"code": "invalid_header",
            "description": "Unable to find appropriate key"}, 401)
    return payload

def request_user_profile(payload, token):
    """ Request user profile from auth0 server when token do not have profile """
    if 'https://valis-dev.auth0.com/userinfo' not in payload['aud']:
        raise AuthError({"code": "no_user_profile", "description": f"Token do not"
            " contain user profile and token audience is not able to get profile"}, 401)
    headers = {'content-type': 'application/json', 'Authorization': f'Bearer {token}'}
    response = requests.get('https://valis-dev.auth0.com/userinfo', headers=headers)
    payload.update(response.json())
    return payload

def get_user_profile():
    return _request_ctx_stack.top.current_user