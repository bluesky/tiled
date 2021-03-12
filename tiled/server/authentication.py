import secrets

from fastapi.security.api_key import APIKeyQuery, APIKeyHeader, APIKey
from fastapi import Depends, HTTPException, Security

from ..utils import SpecialUsers
from .settings import get_settings


# Placeholder for a "database" of API tokens.
API_TOKENS = {"secret": SpecialUsers.admin}  # Maps secret API key to username

api_key_query = APIKeyQuery(name="access_token", auto_error=False)
api_key_header = APIKeyHeader(name="X-Access-Token", auto_error=False)


async def get_api_key(
    api_key_query: APIKey = Security(api_key_query),
    api_key_header: APIKey = Security(api_key_header),
    # TODO Accept cookie as well.
):

    if api_key_query:
        return api_key_query
    elif api_key_header:
        return api_key_header
    else:
        return None


async def get_current_user(api_key: APIKey = Depends(get_api_key)):
    if api_key is None:
        if get_settings().allow_anonymous_access:
            return SpecialUsers.public
        else:
            raise HTTPException(status_code=403, detail="Credentials are required")
    try:
        return API_TOKENS[api_key]
    except KeyError:
        raise HTTPException(status_code=403, detail="Could not validate credentials")


def new_token(username):
    token = secrets.token_hex(32)
    API_TOKENS[token] = username
    return token


def revoke_token(token):
    API_TOKENS.pop(token, None)


def get_user_for_token(token):
    return API_TOKENS.get(token)
