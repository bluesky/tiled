from datetime import datetime, timedelta
from typing import Any, Optional
import warnings

from fastapi import (
    Cookie,
    Depends,
    APIRouter,
    HTTPException,
    Query,
    Security,
    status,
    Response,
    Request,
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

from .settings import get_settings
from ..utils import SpecialUsers

ALGORITHM = "HS256"
ACCESS_TOKEN_LIFETIME_MINUTES = 15


def get_authenticator():
    raise NotImplementedError(
        "This should be overridden via dependency_overrides. "
        "See tiled.server.app.serve_catalog()."
    )


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: Optional[str] = None


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token", auto_error=False)
api_key_query = APIKeyQuery(name="api_key", auto_error=False)
api_key_header = APIKeyHeader(name="X-TILED-API-KEY", auto_error=False)
api_key_cookie = APIKeyCookie(name="TILED_API_KEY", auto_error=False)
authentication_router = APIRouter()


def create_access_token(data: dict, expires_delta, secret_key):
    print(secret_key)
    to_encode = data.copy()
    expire = datetime.utcnow() + expires_delta
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, secret_key, algorithm=ALGORITHM)
    return encoded_jwt


async def check_single_user_api_key(
    api_key_query: str = Security(api_key_query),
    api_key_header: str = Security(api_key_header),
    api_key_cookie: str = Security(api_key_cookie),
    settings: BaseSettings = Depends(get_settings),
):
    for api_key in [api_key_query, api_key_header, api_key_cookie]:
        if api_key is not None:
            if api_key == settings.single_user_api_key:
                return True
            raise HTTPException(status_code=401, detail="Invalid API key")
    return False


async def get_current_user(
    token: str = Depends(oauth2_scheme),
    has_single_user_api_key: str = Depends(check_single_user_api_key),
    settings: BaseSettings = Depends(get_settings),
    authenticator=Depends(get_authenticator),
):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    if (authenticator is None) and has_single_user_api_key:
        return SpecialUsers.admin
    if token is None:
        if settings.allow_anonymous_access:
            # Any user who can see the server can make unauthenticated requests.
            # This is a sentinel that has special meaning to the authorization
            # code (the access control policies).
            return SpecialUsers.public
        else:
            # In this mode, there may still be entries that are visible to all,
            # but users have to authenticate as *someone* to see anything.
            raise HTTPException(status_code=401, detail="Not authenticated")
    # The first key in settings.secret_keys is used for *encoding*.
    # All keys are tried for *decoding* until one works or they all
    # fail. They supports key rotation.
    for secret_key in settings.secret_keys:
        try:
            payload = jwt.decode(token, secret_key, algorithms=[ALGORITHM])
            username: str = payload.get("sub")
            if username is None:
                raise credentials_exception
            break
        except ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="Access token has expired.")

        except JWTError:
            # Try the next key in the key rotation.
            continue
    else:
        raise credentials_exception
    # The user has a valid token for 'username' so we know that
    # within the expiration time they successfully validated.
    # Do we want to re-verify that the user still exists and is
    # authorized at this point? This only makes sense if we grow
    # some server-side database.
    return username


@authentication_router.post("/token", response_model=Token)
async def login_for_access_token(
    form_data: OAuth2PasswordRequestForm = Depends(),
    authenticator: Any = Depends(get_authenticator),
    settings: BaseSettings = Depends(get_settings),
):
    username = authenticator.authenticate(
        username=form_data.username, password=form_data.password
    )
    if not username:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_LIFETIME_MINUTES)
    access_token = create_access_token(
        data={"sub": username},
        expires_delta=access_token_expires,
        secret_key=settings.secret_keys[0],  # Use the *first* secret key to encode.
    )
    return {"access_token": access_token, "token_type": "bearer"}


@authentication_router.post("/logout")
async def logout(
    request: Request,
    response: Response,
    csrf_token: str = Query(...),
    TILED_CSRF_TOKEN=Cookie(...),
):
    if csrf_token != TILED_CSRF_TOKEN:
        raise HTTPException(status_code=401, detail="invalid csrf_token")
    domain = request.url.hostname
    response.delete_cookie("TILED_API_KEY", domain=domain)
    response.delete_cookie("TILED_CSRF_TOKEN", domain=domain)
    return {}
