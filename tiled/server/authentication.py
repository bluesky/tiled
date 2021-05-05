from datetime import datetime, timedelta
import os
from secrets import token_hex
from typing import Any, Optional

from fastapi import Depends, APIRouter, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.param_functions import Form
from jose import ExpiredSignatureError, JWTError, jwt
from pydantic import BaseModel, BaseSettings

from .settings import get_settings
from ..utils import SpecialUsers

# The TILED_SERVER_SECRET_KEYS may be a single key or a ;-separated list of
# keys to support key rotation. The first key will be used for encryption. Each
# key will be tried in turn for decryption.
SECRET_KEYS = os.environ.get("TILED_SERVER_SECRET_KEYS", token_hex(32)).split(";")
MAX_TOKEN_LIFETIME = int(
    os.environ.get("TILED_MAX_TOKEN_LIFETIME", 60 * 60 * 24)
)  # seconds
DEFAULT_TOKEN_LIFETIME = int(
    os.environ.get("TILED_DEFAULT_TOKEN_LIFETIME", 60 * 15)
)  # seconds
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30


def get_authenticator():
    raise NotImplementedError(
        "This should be overridden via dependency_overrides. "
        "See tiled.server.app.serve_catalogs()."
    )


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: Optional[str] = None


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token", auto_error=False)
authentication_router = APIRouter()


def create_access_token(data: dict, lifetime: int):
    to_encode = data.copy()
    expire = datetime.utcnow() + lifetime * timedelta(seconds=1)
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
    # within ACCESS_TOKEN_EXPIRE_MINUTES they successfully validated.
    # Do we want to re-verify that the user still exists and is
    # authorized at this point? This only makes sense if we grow
    # some server-side database.
    return username


class CustomOAuth2PasswordRequestForm(OAuth2PasswordRequestForm):
    "Add a lifetime parameter for setting the JWT expiration (within limits)."

    def __init__(
        self,
        grant_type: str = Form(None, regex="password"),
        username: str = Form(...),
        password: str = Form(...),
        scope: str = Form(""),
        client_id: Optional[str] = Form(None),
        client_secret: Optional[str] = Form(None),
        lifetime: Optional[int] = Form(DEFAULT_TOKEN_LIFETIME),
    ):
        super().__init__(
            grant_type=grant_type,
            username=username,
            password=password,
            scope=scope,
            client_id=client_id,
            client_secret=client_secret,
        )
        self.lifetime = lifetime


@authentication_router.post("/token", response_model=Token)
async def login_for_access_token(
    form_data: CustomOAuth2PasswordRequestForm = Depends(),
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
    if form_data.lifetime > MAX_TOKEN_LIFETIME:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Requested token lifetime {form_data.lifetime} seconds is "
                f"greater than the maximmum {MAX_TOKEN_LIFETIME} seconds."
            ),
        )
    lifetime = min(form_data.lifetime, MAX_TOKEN_LIFETIME)
    access_token = create_access_token(data={"sub": username}, lifetime=lifetime)
    return {"access_token": access_token, "token_type": "bearer"}
