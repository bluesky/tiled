from datetime import datetime, timedelta
import secrets
from typing import Any, Optional
import uuid
import warnings

from fastapi import (
    Depends,
    APIRouter,
    HTTPException,
    Security,
    Request,
    Response,
)
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.security.api_key import APIKeyCookie, APIKeyQuery, APIKeyHeader

# To hide third-party warning
# .../jose/backends/cryptography_backend.py:18: CryptographyDeprecationWarning:
#     int_from_bytes is deprecated, use int.from_bytes instead
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    from jose import ExpiredSignatureError, JWTError, jwt
from pydantic import BaseModel, BaseSettings

from .models import AccessAndRefreshTokens, RefreshToken
from .settings import get_settings
from ..utils import SpecialUsers

ALGORITHM = "HS256"
UNIT_SECOND = timedelta(seconds=1)
ACCESS_TOKEN_COOKIE_NAME = "tiled_access_token"
REFRESH_TOKEN_COOKIE_NAME = "tiled_refresh_token"
API_KEY_COOKIE_NAME = "tiled_api_key"
API_KEY_HEADER_NAME = "x-tiled-api-key"
API_KEY_QUERY_PARAMETER = "api_key"
CSRF_COOKIE_NAME = "tiled_csrf"


def get_authenticator():
    raise NotImplementedError(
        "This should be overridden via dependency_overrides. "
        "See tiled.server.app.serve_tree()."
    )


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: Optional[str] = None


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token", auto_error=False)
api_key_query = APIKeyQuery(name="api_key", auto_error=False)
api_key_header = APIKeyHeader(name=API_KEY_HEADER_NAME, auto_error=False)
api_key_cookie = APIKeyCookie(name="tiled_api_key", auto_error=False)
password_authentication_router = APIRouter()
external_authentication_router = APIRouter()


def create_access_token(data: dict, expires_delta, secret_key):
    to_encode = data.copy()
    expire = datetime.utcnow() + expires_delta
    to_encode.update({"exp": expire, "type": "access"})
    encoded_jwt = jwt.encode(to_encode, secret_key, algorithm=ALGORITHM)
    return encoded_jwt


def create_refresh_token(
    data: dict, secret_key, session_id=None, session_creation_time=None
):
    to_encode = data.copy()
    issued_at_time = datetime.utcnow()
    session_id = session_id or uuid.uuid4().int
    session_creation_time = session_creation_time or issued_at_time
    to_encode.update(
        {
            "type": "refresh",
            # This is used to compute expiry.
            # We do not use "exp" in refresh tokens because we want the freedom
            # to adjust the max age and have that respected immediately.
            "iat": issued_at_time.timestamp(),
            # The session ID is the same for a whole chain of refresh tokens,
            # and it can be potentially used to revoke all of them if
            # we believe the session is compromised.
            "sid": session_id,
            # This is used to enforce a maximum session age.
            "sct": session_creation_time.timestamp(),  # nonstandard claim
        }
    )
    encoded_jwt = jwt.encode(to_encode, secret_key, algorithm=ALGORITHM)
    return encoded_jwt


def decode_token(token, secret_keys, expected_type):
    credentials_exception = HTTPException(
        status_code=401,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    # The first key in settings.secret_keys is used for *encoding*.
    # All keys are tried for *decoding* until one works or they all
    # fail. They supports key rotation.
    for secret_key in secret_keys:
        try:
            payload = jwt.decode(token, secret_key, algorithms=[ALGORITHM])
            break
        except ExpiredSignatureError:
            # Do not let this be caught below with the other JWTError types.
            raise
        except JWTError:
            # Try the next key in the key rotation.
            continue
    else:
        raise credentials_exception
    if payload.get("type") != expected_type:
        raise credentials_exception
    return payload


async def check_single_user_api_key(
    api_key_query: str = Security(api_key_query),
    api_key_header: str = Security(api_key_header),
    api_key_cookie: str = Security(api_key_cookie),
    settings: BaseSettings = Depends(get_settings),
):
    for api_key in [api_key_query, api_key_header, api_key_cookie]:
        if api_key is not None:
            if secrets.compare_digest(api_key, settings.single_user_api_key):
                return True
            raise HTTPException(status_code=401, detail="Invalid API key")
    return False


async def get_current_user(
    request: Request,
    access_token: str = Depends(oauth2_scheme),
    has_single_user_api_key: str = Depends(check_single_user_api_key),
    settings: BaseSettings = Depends(get_settings),
    authenticator=Depends(get_authenticator),
):
    if (authenticator is None) and has_single_user_api_key:
        if request.cookies.get(API_KEY_COOKIE_NAME) != settings.single_user_api_key:
            request.state.cookies_to_set.append(
                {"key": API_KEY_COOKIE_NAME, "value": settings.single_user_api_key}
            )
        return SpecialUsers.admin
    # Check cookies and then the Authorization header.
    access_token_from_either_location = request.cookies.get(
        ACCESS_TOKEN_COOKIE_NAME, access_token
    )
    if access_token_from_either_location is None:
        # No access token anywhere. Is anonymous public access permitted?
        if settings.allow_anonymous_access:
            # Any user who can see the server can make unauthenticated requests.
            # This is a sentinel that has special meaning to the authorization
            # code (the access control policies).
            return SpecialUsers.public
        else:
            # In this mode, there may still be entries that are visible to all,
            # but users have to authenticate as *someone* to see anything.
            raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        payload = decode_token(
            access_token_from_either_location, settings.secret_keys, "access"
        )
    except ExpiredSignatureError:
        raise HTTPException(
            status_code=401, detail="Access token has expired. Refresh token."
        )
    username: str = payload.get("sub")
    return username


@external_authentication_router.get("/auth/code")
async def auth_code(
    request: Request,
    authenticator: Any = Depends(get_authenticator),
    settings: BaseSettings = Depends(get_settings),
):
    request.state.endpoint = "auth"
    username = await authenticator.authenticate(request)
    if not username:
        raise HTTPException(
            status_code=401,
            detail="Authentication failure",
        )
    refresh_token = create_refresh_token(
        data={"sub": username},
        secret_key=settings.secret_keys[0],  # Use the *first* secret key to encode.
    )
    return refresh_token


@password_authentication_router.post(
    "/token", response_model=AccessAndRefreshTokens, include_in_schema=False
)  # back-compat alias
@password_authentication_router.post(
    "/auth/token", response_model=AccessAndRefreshTokens
)
async def login_for_access_token(
    request: Request,
    form_data: OAuth2PasswordRequestForm = Depends(),
    authenticator: Any = Depends(get_authenticator),
    settings: BaseSettings = Depends(get_settings),
):
    request.state.endpoint = "auth"
    if authenticator is None:
        if settings.allow_anonymous_access:
            msg = "This is a public Tiled server with no login."
        else:
            msg = (
                "This is a single-user Tiled server. "
                "To authenticate, use the API key logged at server startup."
            )
        raise HTTPException(status_code=404, detail=msg)
    username = await authenticator.authenticate(
        username=form_data.username, password=form_data.password
    )
    if not username:
        raise HTTPException(
            status_code=401,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token = create_access_token(
        data={"sub": username},
        expires_delta=settings.access_token_max_age,
        secret_key=settings.secret_keys[0],  # Use the *first* secret key to encode.
    )
    refresh_token = create_refresh_token(
        data={"sub": username},
        secret_key=settings.secret_keys[0],  # Use the *first* secret key to encode.
    )
    return {
        "access_token": access_token,
        "expires_in": settings.access_token_max_age / UNIT_SECOND,
        "refresh_token": refresh_token,
        "refresh_token_expires_in": settings.refresh_token_max_age / UNIT_SECOND,
        "token_type": "bearer",
    }


@password_authentication_router.post(
    "/token/refresh", response_model=AccessAndRefreshTokens
)
@external_authentication_router.post(
    "/token/refresh", response_model=AccessAndRefreshTokens
)
@password_authentication_router.post(
    "/auth/token/refresh",
    response_model=AccessAndRefreshTokens,
    include_in_schema=False,
)  # back-compat alias
@external_authentication_router.post(
    "/auth/token/refresh",
    response_model=AccessAndRefreshTokens,
    include_in_schema=False,
)  # back-compat alias
async def post_token_refresh(
    request: Request,
    refresh_token: RefreshToken,
    settings: BaseSettings = Depends(get_settings),
):
    "Obtain a new access token and refresh token."
    request.state.endpoint = "auth"
    new_tokens = slide_session(refresh_token.refresh_token, settings)
    return new_tokens


def slide_session(refresh_token, settings):
    try:
        payload = decode_token(refresh_token, settings.secret_keys, "refresh")
    except ExpiredSignatureError:
        raise HTTPException(
            status_code=401, detail="Session has expired. Please re-authenticate."
        )
    now = datetime.utcnow().timestamp()
    # Enforce refresh token max age.
    # We do this here rather than with an "exp" claim in the token so that we can
    # change the configuration and have that change respected.
    if timedelta(seconds=(now - payload["iat"])) > settings.refresh_token_max_age:
        raise HTTPException(
            status_code=401, detail="Session has expired. Please re-authenticate."
        )
    # Enforce maximum session age, if set.
    if settings.session_max_age is not None:
        if timedelta(seconds=(now - payload["sct"])) > settings.session_max_age:
            raise HTTPException(
                status_code=401, detail="Session has expired. Please re-authenticate."
            )
    new_refresh_token = create_refresh_token(
        data={"sub": payload["sub"]},
        session_id=payload["sid"],
        session_creation_time=datetime.fromtimestamp(payload["sct"]),
        secret_key=settings.secret_keys[0],  # Use the *first* secret key to encode.
    )
    access_token = create_access_token(
        data={"sub": payload["sub"]},
        expires_delta=settings.access_token_max_age,
        secret_key=settings.secret_keys[0],  # Use the *first* secret key to encode.
    )
    return {
        "access_token": access_token,
        "expires_in": settings.access_token_max_age / UNIT_SECOND,
        "refresh_token": new_refresh_token,
        "refresh_token_expires_in": settings.refresh_token_max_age / UNIT_SECOND,
        "token_type": "bearer",
    }


@external_authentication_router.get("/auth/whoami")
@password_authentication_router.get("/auth/whoami")
async def whoami(request: Request, current_user: str = Depends(get_current_user)):
    request.state.endpoint = "auth"
    return {"username": current_user}


@external_authentication_router.post("/auth/logout")
@password_authentication_router.post("/auth/logout")
@external_authentication_router.post(
    "/logout", include_in_schema=False
)  # back-compat alias
@password_authentication_router.post(
    "/logout", include_in_schema=False
)  # back-compat alias
async def logout(
    request: Request,
    response: Response,
):
    request.state.endpoint = "auth"
    response.delete_cookie(API_KEY_COOKIE_NAME)
    response.delete_cookie(CSRF_COOKIE_NAME)
    return {}
