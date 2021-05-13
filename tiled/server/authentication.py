from datetime import datetime, timedelta
import os
from secrets import token_hex
from typing import Any, Optional
import warnings

from fastapi import Depends, APIRouter, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm

# To hide third-party warning
# .../jose/backends/cryptography_backend.py:18: CryptographyDeprecationWarning:
#     int_from_bytes is deprecated, use int.from_bytes instead
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    from jose import ExpiredSignatureError, JWTError, jwt
from pydantic import BaseModel, BaseSettings

from .settings import get_settings
from ..utils import SpecialUsers

# The TILED_SERVER_SECRET_KEYS may be a single key or a ;-separated list of
# keys to support key rotation. The first key will be used for encryption. Each
# key will be tried in turn for decryption.
SECRET_KEYS = os.environ.get("TILED_SERVER_SECRET_KEYS", token_hex(32)).split(";")
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
authentication_router = APIRouter()


def create_access_token(data: dict, expires_delta):
    to_encode = data.copy()
    expire = datetime.utcnow() + expires_delta
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEYS[0], algorithm=ALGORITHM)
    return encoded_jwt


async def get_current_user(
    token: str = Depends(oauth2_scheme),
    settings: BaseSettings = Depends(get_settings),
):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    if token is None:
        if settings.allow_anonymous_access:
            # Any user who can see the server can make unauthenticated requests.
            # This is a sentinel that has special meaning to the authorization
            # code (the access control policies).
            return SpecialUsers.public
        else:
            # In this mode, there may still be entries that are visible to all,
            # but users have to authenticate as *someone* to see anything.
            raise HTTPException(status_code=403, detail="Not authenticated")
    for secret_key in SECRET_KEYS:
        try:
            payload = jwt.decode(token, secret_key, algorithms=[ALGORITHM])
            username: str = payload.get("sub")
            if username is None:
                raise credentials_exception
            break
        except ExpiredSignatureError:
            raise HTTPException(status_code=403, detail="Access token has expired.")

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
        data={"sub": username}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}
