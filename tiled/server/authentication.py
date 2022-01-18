import enum
import secrets
import uuid
import warnings
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request, Response, Security
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.security.api_key import APIKeyCookie, APIKeyHeader, APIKeyQuery

# To hide third-party warning
# .../jose/backends/cryptography_backend.py:18: CryptographyDeprecationWarning:
#     int_from_bytes is deprecated, use int.from_bytes instead
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    from jose import ExpiredSignatureError, JWTError, jwt

from pydantic import BaseModel, BaseSettings

from ..utils import SpecialUsers
from . import orm
from .database import get_db
from .models import AccessAndRefreshTokens, Identity, Principal, RefreshToken, Session
from .settings import get_settings
from .utils import get_base_url

ALGORITHM = "HS256"
UNIT_SECOND = timedelta(seconds=1)
ACCESS_TOKEN_COOKIE_NAME = "tiled_access_token"
REFRESH_TOKEN_COOKIE_NAME = "tiled_refresh_token"
API_KEY_COOKIE_NAME = "tiled_api_key"
API_KEY_HEADER_NAME = "x-tiled-api-key"
API_KEY_QUERY_PARAMETER = "api_key"
CSRF_COOKIE_NAME = "tiled_csrf"


def get_authenticators():
    raise NotImplementedError(
        "This should be overridden via dependency_overrides. "
        "See tiled.server.app.build_app()."
    )


class Mode(enum.Enum):
    password = "password"
    external = "external"


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: Optional[str] = None


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token", auto_error=False)
api_key_query = APIKeyQuery(name="api_key", auto_error=False)
api_key_header = APIKeyHeader(name=API_KEY_HEADER_NAME, auto_error=False)
api_key_cookie = APIKeyCookie(name="tiled_api_key", auto_error=False)


def create_access_token(data, secret_key, expires_delta):
    to_encode = data.copy()
    expire = datetime.utcnow() + expires_delta
    to_encode.update({"exp": expire, "type": "access"})
    encoded_jwt = jwt.encode(to_encode, secret_key, algorithm=ALGORITHM)
    return encoded_jwt


def create_refresh_token(data, session_id, session_creation_time, secret_key):
    to_encode = data.copy()
    issued_at_time = datetime.utcnow()
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


async def get_current_principal(
    request: Request,
    access_token: str = Depends(oauth2_scheme),
    has_single_user_api_key: str = Depends(check_single_user_api_key),
    settings: BaseSettings = Depends(get_settings),
    authenticators=Depends(get_authenticators),
):
    if (not authenticators) and has_single_user_api_key:
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

            # Include a link to the root page which provides a list of
            # authenticators. The use case here is:
            # 1. User is emailed a link like https://example.com/subpath/node/metadata/a/b/c
            # 2. Tiled Client tries to connect to that and gets 401.
            # 3. Client can use this header to find its way to
            #    https://examples.com/subpath/ and obtain a list of
            #    authentication providers and endpoints.
            raise HTTPException(
                status_code=401,
                detail="Not authenticated",
                headers={"X-Tiled-Root": get_base_url(request)},
            )
    try:
        payload = decode_token(
            access_token_from_either_location, settings.secret_keys, "access"
        )
    except ExpiredSignatureError:
        raise HTTPException(
            status_code=401,
            detail="Access token has expired. Refresh token.",
        )
    return Principal(
        id=payload["sub"],
        display_name=payload["dis"],
        identities=[
            Identity(external_id=identity["eid"], provider=identity["idp"])
            for identity in payload["ids"]
        ],
    )


def build_auth_code_route(authenticator, provider):
    "Build an auth_code route function for this Authenticator."

    async def auth_code(
        request: Request,
        settings: BaseSettings = Depends(get_settings),
    ):
        request.state.endpoint = "auth"
        username = await authenticator.authenticate(request)
        if not username:
            raise HTTPException(status_code=401, detail="Authentication failure")
        refresh_token = create_refresh_token(
            data={"sub": username},
            secret_key=settings.secret_keys[0],  # Use the *first* secret key to encode.
        )
        return refresh_token

    return auth_code


def build_handle_credentials_route(authenticator, provider):
    "Register a handle_credentials route function for this Authenticator."

    async def handle_credentials(
        request: Request,
        form_data: OAuth2PasswordRequestForm = Depends(),
        settings: BaseSettings = Depends(get_settings),
        db=Depends(get_db),
    ):
        request.state.endpoint = "auth"
        username = await authenticator.authenticate(
            username=form_data.username, password=form_data.password
        )
        if not username:
            raise HTTPException(
                status_code=401,
                detail="Incorrect username or password",
                headers={"WWW-Authenticate": "Bearer"},
            )
        # Have we seen this Identity before?
        identity = (
            db.query(orm.Identity)
            .filter(orm.Identity.external_id == username)
            .filter(orm.Identity.provider == provider)
            .first()
        )
        if identity is None:
            # We have not. Make a new Principal and link this new Identity to it.
            # TODO Confirm that the user intends to create a new Principal here.
            # Give them the opportunity to link an existing Principal instead.
            principal = orm.Principal(type="user", display_name=username)
            db.add(principal)
            db.commit()
            db.refresh(
                principal
            )  # Refresh to sync back the auto-generated principal.id.
            identity = orm.Identity(
                provider=provider, external_id=username, principal_id=principal.id
            )
            db.add(identity)
            db.commit()
        else:
            principal = identity.principal
        session = orm.Session(
            principal_id=principal.id,
            expiration_time=datetime.now() + settings.session_max_age,
        )
        db.add(session)
        db.commit()
        db.refresh(session)  # Refresh to sync back the auto-generated session.id.
        session_model = Session.from_orm(session)
        # Provide enough information to reconstruct Principal and its
        # Identities sufficient for access policy enforcement without a
        # database hit.
        data = {
            "sub": principal.id,
            "dis": principal.display_name,
            "typ": principal.type.value,
            "ids": [
                {"eid": identity.external_id, "idp": identity.provider}
                for identity in principal.identities
            ],
        }
        access_token = create_access_token(
            data=data,
            expires_delta=settings.access_token_max_age,
            secret_key=settings.secret_keys[0],  # Use the *first* secret key to encode.
        )
        refresh_token = create_refresh_token(
            data=data,
            session_id=session_model.id.hex,
            session_creation_time=datetime.now(),
            secret_key=settings.secret_keys[0],  # Use the *first* secret key to encode.
        )
        return {
            "access_token": access_token,
            "expires_in": settings.access_token_max_age / UNIT_SECOND,
            "refresh_token": refresh_token,
            "refresh_token_expires_in": settings.refresh_token_max_age / UNIT_SECOND,
            "token_type": "bearer",
        }

    return handle_credentials


async def post_token_refresh(
    request: Request,
    refresh_token: RefreshToken,
    settings: BaseSettings = Depends(get_settings),
    db=Depends(get_db),
):
    "Obtain a new access token and refresh token."
    request.state.endpoint = "auth"
    new_tokens = slide_session(refresh_token.refresh_token, settings, db)
    return new_tokens


def slide_session(refresh_token, settings, db):
    try:
        payload = decode_token(refresh_token, settings.secret_keys, "refresh")
    except ExpiredSignatureError:
        raise HTTPException(
            status_code=401, detail="Session has expired. Please re-authenticate."
        )
    now = datetime.utcnow().timestamp()
    # Enforce refresh token max age.
    # We do this here as well as with an "exp" claim in the token so that we can
    # change the configuration to *decrease* the lifetime and have that change
    # respected on existing sessions.
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
    session_id_hex = payload["sid"]
    session_id = uuid.UUID(bytearray.fromhex(session_id_hex))
    session = db.query(orm.Session).filter(orm.Session.id == session_id).first()
    if (session is None) or session.revoked:
        # Do not leak (to a potential attacker) that this has been *revoked* specifically.
        # Give the same error as if it had expired.
        raise HTTPException(
            status_code=401, detail="Session has expired. Please re-authenticate."
        )
    # Provide enough information to reconstruct Principal and its
    # Identities sufficient for access policy enforcement without a
    # database hit.
    principal = Principal.from_orm(session.principal)
    data = {
        "sub": principal.id,
        "dis": principal.display_name,
        "typ": principal.type.value,
        "identities": [
            {"eid": identity.external_id, "idp": identity.provider}
            for identity in principal.identities
        ],
    }
    access_token = create_access_token(
        data=data,
        expires_delta=settings.access_token_max_age,
        secret_key=settings.secret_keys[0],  # Use the *first* secret key to encode.
    )
    new_refresh_token = create_refresh_token(
        data=data,
        session_id=payload["sid"],
        session_creation_time=datetime.fromtimestamp(payload["sct"]),
        secret_key=settings.secret_keys[0],  # Use the *first* secret key to encode.
    )
    return {
        "access_token": access_token,
        "expires_in": settings.access_token_max_age / UNIT_SECOND,
        "refresh_token": new_refresh_token,
        "refresh_token_expires_in": settings.refresh_token_max_age / UNIT_SECOND,
        "token_type": "bearer",
    }


async def whoami(
    request: Request,
    principal: str = Depends(get_current_principal),
):
    request.state.endpoint = "auth"
    return principal.dict()


async def logout(request: Request, response: Response):
    request.state.endpoint = "auth"
    response.delete_cookie(API_KEY_COOKIE_NAME)
    response.delete_cookie(CSRF_COOKIE_NAME)
    return {}


def build_authentication_router():
    router = APIRouter()
    router.post(
        "/token/refresh",
        response_model=AccessAndRefreshTokens,
    )(post_token_refresh)
    router.get("/whoami")(whoami)
    router.post("/logout")(logout)
    return router
