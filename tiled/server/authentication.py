import asyncio
import enum
import hashlib
import secrets
import uuid as uuid_module
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

from ..database import orm
from ..utils import SpecialUsers
from .models import (
    AccessAndRefreshTokens,
    APIKey,
    APIKeyParams,
    APIKeyWithSecret,
    Identity,
    Principal,
    RefreshToken,
    Session,
    WhoAmI,
)
from .settings import get_sessionmaker, get_settings
from .utils import (
    API_KEY_COOKIE_NAME,
    API_KEY_HEADER_NAME,
    CSRF_COOKIE_NAME,
    get_authenticators,
    get_base_url,
)

ALGORITHM = "HS256"
UNIT_SECOND = timedelta(seconds=1)


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


def create_refresh_token(session_id, secret_key, expires_delta):
    expire = datetime.utcnow() + expires_delta
    to_encode = {
        "type": "refresh",
        "sid": session_id,
        "exp": expire,
    }
    encoded_jwt = jwt.encode(to_encode, secret_key, algorithm=ALGORITHM)
    return encoded_jwt


def decode_token(token, secret_keys):
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


def get_current_principal(
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
    if access_token is None:
        # Is anonymous public access permitted?
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
        payload = decode_token(access_token, settings.secret_keys)
    except ExpiredSignatureError:
        raise HTTPException(
            status_code=401,
            detail="Access token has expired. Refresh token.",
        )
    return Principal(
        uuid=uuid_module.UUID(hex=payload["sub"]),
        type=payload["sub_typ"],
        identities=[
            Identity(id=identity["id"], provider=identity["idp"])
            for identity in payload["ids"]
        ],
    )


def create_session(db, settings, identity_provider, id):
    # Have we seen this Identity before?
    identity = (
        db.query(orm.Identity)
        .filter(orm.Identity.id == id)
        .filter(orm.Identity.provider == identity_provider)
        .first()
    )
    if identity is None:
        # We have not. Make a new Principal and link this new Identity to it.
        # TODO Confirm that the user intends to create a new Principal here.
        # Give them the opportunity to link an existing Principal instead.
        principal = orm.Principal(type="user")
        db.add(principal)
        db.commit()
        db.refresh(principal)  # Refresh to sync back the auto-generated uuid.
        identity = orm.Identity(
            provider=identity_provider,
            id=id,
            principal_id=principal.id,
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
    db.refresh(session)  # Refresh to sync back the auto-generated session.uuid.
    session_model = Session.from_orm(session)
    principal_model = Principal.from_orm(principal)
    # Provide enough information in the access token to reconstruct Principal
    # and its Identities sufficient for access policy enforcement without a
    # database hit.
    data = {
        "sub": principal_model.uuid.hex,
        "sub_typ": principal_model.type.value,
        "ids": [
            {"id": identity.id, "idp": identity.provider}
            for identity in principal_model.identities
        ],
    }
    access_token = create_access_token(
        data=data,
        expires_delta=settings.access_token_max_age,
        secret_key=settings.secret_keys[0],  # Use the *first* secret key to encode.
    )
    refresh_token = create_refresh_token(
        session_id=session_model.uuid.hex,
        expires_delta=settings.refresh_token_max_age,
        secret_key=settings.secret_keys[0],  # Use the *first* secret key to encode.
    )
    return {
        "access_token": access_token,
        "expires_in": settings.access_token_max_age / UNIT_SECOND,
        "refresh_token": refresh_token,
        "refresh_token_expires_in": settings.refresh_token_max_age / UNIT_SECOND,
        "token_type": "bearer",
    }


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
        db = get_sessionmaker(settings.database_uri)()
        return await asyncio.get_running_loop().run_in_executor(
            None, create_session, db, settings, provider, username
        )

    return auth_code


def build_handle_credentials_route(authenticator, provider):
    "Register a handle_credentials route function for this Authenticator."

    async def handle_credentials(
        request: Request,
        form_data: OAuth2PasswordRequestForm = Depends(),
        settings: BaseSettings = Depends(get_settings),
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
        db = get_sessionmaker(settings.database_uri)()
        return await asyncio.get_running_loop().run_in_executor(
            None, create_session, db, settings, provider, username
        )

    return handle_credentials


base_authentication_router = APIRouter()


@base_authentication_router.get("/principals")
def principals(
    request: Request,
    settings: BaseSettings = Depends(get_settings),
):
    "List Principals (users and services)."
    # TODO Pagination
    request.state.endpoint = "auth"
    db = get_sessionmaker(settings.database_uri)()
    principals = db.query(orm.Principal).all()
    models = []
    for principal in principals:
        principal_model = Principal.from_orm(principal)
        models.append(principal_model)
    return {"data": [models]}


@base_authentication_router.get("/principal/{uuid}")
def principal(
    request: Request,
    uuid: str,
    settings: BaseSettings = Depends(get_settings),
):
    "Get information about one Principal (user or service)."
    request.state.endpoint = "auth"
    db = get_sessionmaker(settings.database_uri)()
    try:
        parsed_uuid = uuid_module.UUID(hex=uuid)
    except Exception:
        raise HTTPException(400, f"Malformed UUID {uuid}")
    principal = (
        db.query(orm.Principal).filter(orm.Principal.uuid == parsed_uuid).first()
    )
    if principal is None:
        raise HTTPException(
            404, f"Principal {uuid} does not exist or insufficient permissions."
        )
    principal_model = Principal.from_orm(principal)
    return {"data": principal_model.dict()}


@base_authentication_router.post("/principal/{uuid}/apikey")
def apikey_for_principal(
    request: Request,
    uuid: str,
    apikey_params: APIKeyParams,
    settings: BaseSettings = Depends(get_settings),
):
    "Generate an API key for a Principal."
    request.state.endpoint = "auth"
    db = get_sessionmaker(settings.database_uri)()
    try:
        parsed_uuid = uuid_module.UUID(hex=uuid)
    except Exception:
        raise HTTPException(400, f"Malformed UUID {uuid}")
    principal = (
        db.query(orm.Principal).filter(orm.Principal.uuid == parsed_uuid).first()
    )
    if principal is None:
        raise HTTPException(
            404, f"Principal {uuid} does not exist or insufficient permissions."
        )
    if apikey_params.lifetime is not None:
        expiration_time = datetime.now() + timedelta(seconds=apikey_params.lifetime)
    else:
        expiration_time = None
    secret = secrets.token_bytes(32)
    hashed_secret = hashlib.sha256(secret).digest()
    new_key = orm.APIKey(
        principal_id=principal.id,
        expiration_time=expiration_time,
        note=apikey_params.note,
        scopes=apikey_params.scopes,
        hashed_secret=hashed_secret,
    )
    db.add(new_key)
    db.commit()
    db.refresh(new_key)
    return {
        "data": APIKeyWithSecret(secret=secret.hex(), **APIKey.from_orm(new_key).dict())
    }


@base_authentication_router.post(
    "/session/refresh", response_model=AccessAndRefreshTokens
)
def refresh_session(
    request: Request,
    refresh_token: RefreshToken,
    settings: BaseSettings = Depends(get_settings),
):
    "Obtain a new access token and refresh token."
    request.state.endpoint = "auth"
    db = get_sessionmaker(settings.database_uri)()
    new_tokens = slide_session(refresh_token.refresh_token, settings, db)
    return new_tokens


@base_authentication_router.delete("/session/revoke/{session_id}")
def revoke_session(
    session_id: str,  # from path parameter
    request: Request,
    principal: Principal = Depends(get_current_principal),
    settings: BaseSettings = Depends(get_settings),
):
    "Mark a Session as revoked so it cannot be refreshed again."
    request.state.endpoint = "auth"
    db = get_sessionmaker(settings.database_uri)()
    # Find this session in the database.
    session = (
        db.query(orm.Session)
        .filter(orm.Session.uuid == uuid_module.UUID(hex=session_id))
        .first()
    )
    if principal.uuid != session.principal.uuid:
        # TODO Add a scope for doing this for other users.
        raise HTTPException(
            404,
            detail="Sessions does not exist or requester has insufficient permissions",
        )
    session.revoked = True
    db.commit()
    return Response(status_code=204)


def slide_session(refresh_token, settings, db):
    try:
        payload = decode_token(refresh_token, settings.secret_keys)
    except ExpiredSignatureError:
        raise HTTPException(
            status_code=401, detail="Session has expired. Please re-authenticate."
        )
    # Find this session in the database.
    session = (
        db.query(orm.Session)
        .filter(orm.Session.uuid == uuid_module.UUID(hex=payload["sid"]))
        .first()
    )
    now = datetime.now()
    # This token is *signed* so we know that the information came from us.
    # If the Session is forgotten or revoked or expired, do not allow refresh.
    if (session is None) or session.revoked or (session.expiration_time < now):
        # Do not leak (to a potential attacker) whether this has been *revoked*
        # specifically. Give the same error as if it had expired.
        raise HTTPException(
            status_code=401, detail="Session has expired. Please re-authenticate."
        )
    # Update Session info.
    session.time_last_refreshed = now
    # This increments in a way that avoids a race condition.
    session.refresh_count = orm.Session.refresh_count + 1
    # Provide enough information in the access token to reconstruct Principal
    # and its Identities sufficient for access policy enforcement without a
    # database hit.
    principal = Principal.from_orm(session.principal)
    data = {
        "sub": principal.uuid.hex,
        "sub_typ": principal.type.value,
        "ids": [
            {"id": identity.id, "idp": identity.provider}
            for identity in principal.identities
        ],
    }
    access_token = create_access_token(
        data=data,
        expires_delta=settings.access_token_max_age,
        secret_key=settings.secret_keys[0],  # Use the *first* secret key to encode.
    )
    new_refresh_token = create_refresh_token(
        session_id=payload["sid"],
        expires_delta=settings.refresh_token_max_age,
        secret_key=settings.secret_keys[0],  # Use the *first* secret key to encode.
    )
    return {
        "access_token": access_token,
        "expires_in": settings.access_token_max_age / UNIT_SECOND,
        "refresh_token": new_refresh_token,
        "refresh_token_expires_in": settings.refresh_token_max_age / UNIT_SECOND,
        "token_type": "bearer",
    }


@base_authentication_router.get("/whoami")
def whoami(
    request: Request,
    principal: str = Depends(get_current_principal),
    settings: BaseSettings = Depends(get_settings),
):
    # This is here to avoid circular import. TODO Reorganize modules.
    from .core import json_or_msgpack, resolve_media_type

    # TODO Permit filtering the fields of the response.
    request.state.endpoint = "auth"
    if principal is None:
        return None
    db = get_sessionmaker(settings.database_uri)()
    # The principal from get_current_principal tells us everything that the
    # access_token carries around, but the database knows more than that.
    principal_orm = (
        db.query(orm.Principal).filter(orm.Principal.uuid == principal.uuid).first()
    )
    principal_model = Principal.from_orm(principal_orm)
    return json_or_msgpack(
        request,
        WhoAmI(data=principal_model),
        resolve_media_type(request),
    )


@base_authentication_router.post("/logout")
async def logout(request: Request, response: Response):
    request.state.endpoint = "auth"
    response.delete_cookie(API_KEY_COOKIE_NAME)
    response.delete_cookie(CSRF_COOKIE_NAME)
    return {}
