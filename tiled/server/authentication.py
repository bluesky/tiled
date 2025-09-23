import hashlib
import secrets
import uuid as uuid_module
import warnings
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Annotated, Any, Optional, Sequence, Set

from fastapi import (
    APIRouter,
    Depends,
    Form,
    Header,
    HTTPException,
    Query,
    Request,
    Response,
    Security,
    WebSocket,
)
from fastapi.security import (
    OAuth2PasswordBearer,
    OAuth2PasswordRequestForm,
    SecurityScopes,
)
from fastapi.security.api_key import APIKeyCookie, APIKeyHeader, APIKeyQuery
from fastapi.security.utils import get_authorization_scheme_param
from fastapi.templating import Jinja2Templates
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import func
from starlette.status import (
    HTTP_204_NO_CONTENT,
    HTTP_400_BAD_REQUEST,
    HTTP_401_UNAUTHORIZED,
    HTTP_404_NOT_FOUND,
    HTTP_409_CONFLICT,
)

from tiled.access_control.scopes import NO_SCOPES, PUBLIC_SCOPES, USER_SCOPES

# To hide third-party warning
# .../jose/backends/cryptography_backend.py:18: CryptographyDeprecationWarning:
#     int_from_bytes is deprecated, use int.from_bytes instead
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    from jose import ExpiredSignatureError, JWTError, jwt

from pydantic import BaseModel

from ..authn_database import orm
from ..authn_database.connection_pool import get_database_session
from ..authn_database.core import (
    create_service,
    create_user,
    latest_principal_activity,
    lookup_valid_api_key,
    lookup_valid_pending_session_by_device_code,
    lookup_valid_pending_session_by_user_code,
    lookup_valid_session,
)
from ..utils import SHARE_TILED_PATH, SingleUserPrincipal
from . import schemas
from .core import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE, json_or_msgpack
from .protocols import ExternalAuthenticator, InternalAuthenticator, UserSessionState
from .settings import Settings, get_settings
from .utils import API_KEY_COOKIE_NAME, get_base_url

ALGORITHM = "HS256"

# Max API keys and Sessions allowed to Principal.
# This is here for at least two reasons:
# 1. Ensure that the routes which list API keys and sessions, which are
#    not paginated, returns in a reasonable time.
# 2. Avoid unintentional or intentional abuse.
API_KEY_LIMIT = 100
SESSION_LIMIT = 200

DEVICE_CODE_MAX_AGE = timedelta(minutes=15)
DEVICE_CODE_POLLING_INTERVAL = 5  # seconds


def utcnow():
    "UTC now with second resolution"
    return datetime.now(timezone.utc).replace(microsecond=0)


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: Optional[str] = None


class StrictAPIKeyHeader(APIKeyHeader):
    # TODO: remove custom subclass https://github.com/bluesky/tiled/issues/921
    """
    APIKeyHeader does not enforce that the scheme matches the expected, potentially
    leading to the case where Bearer tokens are treated as API keys.
    Additionally strips the scheme, as expected by current handling.
    """

    async def __call__(self, request: Request) -> Optional[str]:
        api_key: Optional[str] = request.headers.get(self.model.name)
        scheme, param = get_authorization_scheme_param(api_key)
        if not scheme or scheme.lower() == "bearer":
            return self.check_api_key(None, self.auto_error)
        if scheme.lower() != self.scheme_name.lower():
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail=(
                    "Authorization header must include the authorization type "
                    "followed by a space and then the secret, as in "
                    "'Bearer SECRET' or 'Apikey SECRET'. "
                ),
            )
        return self.check_api_key(param, self.auto_error)


# The tokenUrl below is patched at app startup when we know it.
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="PLACEHOLDER", auto_error=False)


def create_access_token(data, secret_key, expires_delta):
    to_encode = data.copy()
    expire = utcnow() + expires_delta
    to_encode.update({"exp": expire, "type": "access"})
    encoded_jwt = jwt.encode(to_encode, secret_key, algorithm=ALGORITHM)
    return encoded_jwt


def create_refresh_token(session_id, secret_key, expires_delta):
    expire = utcnow() + expires_delta
    to_encode = {
        "type": "refresh",
        "sid": session_id,
        "exp": expire,
    }
    encoded_jwt = jwt.encode(to_encode, secret_key, algorithm=ALGORITHM)
    return encoded_jwt


def decode_token(token, secret_keys):
    credentials_exception = HTTPException(
        status_code=HTTP_401_UNAUTHORIZED,
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


async def get_api_key(
    api_key_query: Optional[str] = Depends(
        APIKeyQuery(name="api_key", auto_error=False)
    ),
    api_key_header: Optional[str] = Depends(
        StrictAPIKeyHeader(
            name="Authorization",
            description="Prefix value with 'Apikey ' as in, 'Apikey SECRET'",
            scheme_name="Apikey",
            auto_error=False,
        )
    ),
    api_key_cookie: Optional[str] = Depends(
        APIKeyCookie(name=API_KEY_COOKIE_NAME, auto_error=False)
    ),
) -> Optional[str]:
    for api_key in [api_key_query, api_key_header, api_key_cookie]:
        if api_key is not None:
            return api_key
    return None


def headers_for_401(request: Request, security_scopes: SecurityScopes):
    # call directly from methods, rather than as a dependency, to avoid calling
    # when not needed.
    if security_scopes.scopes:
        authenticate_value = f'Bearer scope="{security_scopes.scope_str}"'
    else:
        authenticate_value = "Bearer"
    headers_for_401 = {
        "WWW-Authenticate": authenticate_value,
        "X-Tiled-Root": get_base_url(request),
    }
    return headers_for_401


async def get_decoded_access_token(
    request: Request,
    security_scopes: SecurityScopes,
    access_token: str = Depends(oauth2_scheme),
    settings: Settings = Depends(get_settings),
):
    if not access_token:
        return None
    try:
        payload = decode_token(access_token, settings.secret_keys)
    except ExpiredSignatureError:
        raise HTTPException(
            status_code=HTTP_401_UNAUTHORIZED,
            detail="Access token has expired. Refresh token.",
            headers=headers_for_401(request, security_scopes),
        )
    return payload


async def get_session_state(decoded_access_token=Depends(get_decoded_access_token)):
    if decoded_access_token:
        return decoded_access_token.get("state")


async def get_access_tags_from_api_key(
    api_key: str, authenticated: bool, db: Optional[AsyncSession]
) -> Optional[Set[str]]:
    if not authenticated:
        # Tiled is in a "single user" mode with only one API key.
        # In this mode, there is no meaningful access tag limit.
        return None
    # Tiled is in a multi-user configuration with authentication providers.
    # We store the hashed value of the API key secret.
    try:
        secret = bytes.fromhex(api_key)
    except Exception:
        # access tag limit cannot be enforced without key information
        return None
    api_key_orm = await lookup_valid_api_key(db, secret)
    if api_key_orm is None:
        # access tag limit cannot be enforced without key information
        return None
    else:
        if (access_tags := api_key_orm.access_tags) is not None:
            access_tags = set(access_tags)
        return access_tags


async def get_current_access_tags(
    request: Request,
    api_key: Optional[str] = Depends(get_api_key),
    db: Optional[AsyncSession] = Depends(get_database_session),
) -> Optional[Set[str]]:
    if api_key is not None:
        return await get_access_tags_from_api_key(
            api_key, request.app.state.authenticated, db
        )
    else:
        # Limits on access tags only available via API key auth
        return None


def get_api_key_websocket(
    authorization: Annotated[Optional[str], Header()] = None,
):
    if authorization is None:
        raise HTTPException(
            status_code=HTTP_401_UNAUTHORIZED,
            detail="An API key must be passed in the Authorization header",
        )
    scheme, api_key = get_authorization_scheme_param(authorization)
    if scheme.lower() != "apikey":
        raise HTTPException(
            status_code=HTTP_400_BAD_REQUEST,
            detail="Authorization header must be formatted like 'Apikey SECRET'",
        )
    return api_key


async def get_current_access_tags_websocket(
    websocket: WebSocket,
    api_key: Optional[str] = Depends(get_api_key_websocket),
    db: Optional[AsyncSession] = Depends(get_database_session),
) -> Optional[Set[str]]:
    if api_key is not None:
        return await get_access_tags_from_api_key(
            api_key, websocket.app.state.authenticated, db
        )
    else:
        # Limits on access tags only available via API key auth
        return None


async def move_api_key(request: Request, api_key: Optional[str] = Depends(get_api_key)):
    """
    Move API key from query parameter to cookie.

    When a URL with an API key in the query parameter is opened in a browser,
    the API key is set as a cookie so that subsequent requests from the browser
    are authenticated. (This approach was inspired by Jupyter notebook.)
    """
    if ("api_key" in request.query_params) and (
        request.cookies.get(API_KEY_COOKIE_NAME) != api_key
    ):
        request.state.cookies_to_set.append(
            {"key": API_KEY_COOKIE_NAME, "value": api_key}
        )


async def get_scopes_from_api_key(
    api_key: str, settings: Settings, authenticated: bool, db: Optional[AsyncSession]
) -> Sequence[str]:
    if not authenticated:
        # Tiled is in a "single user" mode with only one API key.
        return (
            USER_SCOPES
            if secrets.compare_digest(api_key, settings.single_user_api_key)
            else set()
        )
    # Tiled is in a multi-user configuration with authentication providers.
    # We store the hashed value of the API key secret.
    # By comparing hashes we protect against timing attacks.
    # By storing only the hash of the (high-entropy) secret
    # we reduce the value of that an attacker can extracted from a
    # stolen database backup.
    try:
        secret = bytes.fromhex(api_key)
    except Exception:
        return NO_SCOPES
    api_key_orm = await lookup_valid_api_key(db, secret)
    if api_key_orm is None:
        return NO_SCOPES
    else:
        principal = api_key_orm.principal
        principal_scopes = set().union(*[role.scopes for role in principal.roles])
        # This intersection addresses the case where the Principal has
        # lost a scope that they had when this key was created.
        scopes = set(api_key_orm.scopes).intersection(principal_scopes | {"inherit"})
        if "inherit" in scopes:
            # The scope "inherit" is a metascope that confers all the
            # scopes for the Principal associated with this API,
            # resolved at access time.
            scopes.update(principal_scopes)
        return scopes


async def get_current_scopes(
    request: Request,
    decoded_access_token: Optional[dict[str, Any]] = Depends(get_decoded_access_token),
    api_key: Optional[str] = Depends(get_api_key),
    settings: Settings = Depends(get_settings),
    db: Optional[AsyncSession] = Depends(get_database_session),
) -> set[str]:
    if api_key is not None:
        return await get_scopes_from_api_key(
            api_key, settings, request.app.state.authenticated, db
        )
    elif decoded_access_token is not None:
        return decoded_access_token["scp"]
    else:
        return PUBLIC_SCOPES if settings.allow_anonymous_access else NO_SCOPES


async def get_current_scopes_websocket(
    websocket: WebSocket,
    api_key: Optional[str] = Depends(get_api_key_websocket),
    settings: Settings = Depends(get_settings),
    db: Optional[AsyncSession] = Depends(get_database_session),
) -> set[str]:
    if api_key is not None:
        return await get_scopes_from_api_key(
            api_key, settings, websocket.app.state.authenticated, db
        )
    else:
        return PUBLIC_SCOPES if settings.allow_anonymous_access else NO_SCOPES


async def check_scopes(
    request: Request,
    security_scopes: SecurityScopes,
    scopes: set[str] = Depends(get_current_scopes),
) -> None:
    if not set(security_scopes.scopes).issubset(scopes):
        raise HTTPException(
            status_code=HTTP_401_UNAUTHORIZED,
            detail=(
                "Not enough permissions. "
                f"Requires scopes {security_scopes.scopes}. "
                f"Request had scopes {list(scopes)}"
            ),
            headers=headers_for_401(request, security_scopes),
        )


async def get_current_principal_from_api_key(
    api_key: str, authenticated: bool, db: AsyncSession, settings: Settings
):
    if authenticated:
        # Tiled is in a multi-user configuration with authentication providers.
        # We store the hashed value of the API key secret.
        # By comparing hashes we protect against timing attacks.
        # By storing only the hash of the (high-entropy) secret
        # we reduce the value of that an attacker can extracted from a
        # stolen database backup.
        try:
            secret = bytes.fromhex(api_key)
        except Exception:
            # Not valid hex, therefore not a valid API key
            return None

        api_key_orm = await lookup_valid_api_key(db, secret)
        if api_key_orm is not None:
            principal = api_key_orm.principal
            principal_scopes = set().union(*[role.scopes for role in principal.roles])
            # This intersection addresses the case where the Principal has
            # lost a scope that they had when this key was created.
            scopes = set(api_key_orm.scopes).intersection(
                principal_scopes | {"inherit"}
            )
            if "inherit" in scopes:
                # The scope "inherit" is a metascope that confers all the
                # scopes for the Principal associated with this API,
                # resolved at access time.
                scopes.update(principal_scopes)
            api_key_orm.latest_activity = utcnow()
            await db.commit()
            return principal
        else:
            return None
    else:
        # Tiled is in a "single user" mode with only one API key.
        if secrets.compare_digest(api_key, settings.single_user_api_key):
            # Valid single-user API key - return None to indicate valid single user
            return None
        else:
            # Invalid single-user API key - raise exception directly
            raise HTTPException(
                status_code=HTTP_401_UNAUTHORIZED, detail="Invalid API key"
            )


async def get_current_principal_websocket(
    websocket: WebSocket,
    api_key: str = Depends(get_api_key_websocket),
    settings: Settings = Depends(get_settings),
    db: Optional[AsyncSession] = Depends(get_database_session),
):
    principal = await get_current_principal_from_api_key(
        api_key, websocket.app.state.authenticated, db, settings
    )
    if principal is None and websocket.app.state.authenticated:
        raise HTTPException(status_code=HTTP_401_UNAUTHORIZED, detail="Invalid API key")
    return principal


async def get_current_principal(
    request: Request,
    security_scopes: SecurityScopes,
    decoded_access_token: str = Depends(get_decoded_access_token),
    api_key: str = Depends(get_api_key),
    settings: Settings = Depends(get_settings),
    db: Optional[AsyncSession] = Depends(get_database_session),
    _=Depends(move_api_key),
) -> Optional[schemas.Principal]:
    """
    Get current Principal from:
    - API key in 'api_key' query parameter
    - API key in header 'Authorization: Apikey ...'
    - API key in cookie 'tiled_api_key'
    - OAuth2 JWT access token in header 'Authorization: Bearer ...'

    If anonymous access is allowed, Principal will be `None`.
    If the server is configured with a "single-user API key", then
    the Principal will also be `None` - but is differentiated for
    logging with a SingleUserPrincipal sentinel
    """

    if api_key is not None:
        principal = await get_current_principal_from_api_key(
            api_key,
            request.app.state.authenticated,
            db,
            settings,
        )
        if principal is None and request.app.state.authenticated:
            raise HTTPException(
                status_code=HTTP_401_UNAUTHORIZED,
                detail="Invalid API key",
                headers=headers_for_401(request, security_scopes),
            )
    elif decoded_access_token is not None:
        principal = schemas.Principal(
            uuid=uuid_module.UUID(hex=decoded_access_token["sub"]),
            type=decoded_access_token["sub_typ"],
            identities=[
                schemas.Identity(id=identity["id"], provider=identity["idp"])
                for identity in decoded_access_token["ids"]
            ],
        )
    else:
        # No form of authentication is present.
        principal = None
    # This is used to pass the currently-authenticated principal into the logger.
    is_apikey_single_user = api_key is not None and not request.app.state.authenticated
    request.state.principal = (
        principal if not is_apikey_single_user else SingleUserPrincipal
    )
    return principal


async def create_pending_session(db: AsyncSession):
    device_code = secrets.token_bytes(32)
    hashed_device_code = hashlib.sha256(device_code).digest()
    for _ in range(3):
        user_code = secrets.token_hex(4).upper()  # 8 digit code
        pending_session = orm.PendingSession(
            user_code=user_code,
            hashed_device_code=hashed_device_code,
            expiration_time=utcnow() + DEVICE_CODE_MAX_AGE,
        )
        db.add(pending_session)
        try:
            await db.commit()
        except IntegrityError:
            # Since the user_code is short, we cannot completely dismiss the
            # possibility of a collission. Retry.
            continue
        break
    formatted_user_code = f"{user_code[:4]}-{user_code[4:]}"
    return {
        "user_code": formatted_user_code,
        "device_code": device_code.hex(),
    }


async def create_session(
    settings: Settings,
    db: AsyncSession,
    identity_provider,
    id,
    state: UserSessionState = None,
):
    # Have we seen this Identity before?
    identity = (
        await db.execute(
            select(orm.Identity)
            .options(selectinload(orm.Identity.principal))
            .filter(orm.Identity.id == id)
            .filter(orm.Identity.provider == identity_provider)
        )
    ).scalar()
    now = utcnow()
    if identity is None:
        # We have not. Make a new Principal and link this new Identity to it.
        # TODO Confirm that the user intends to create a new Principal here.
        # Give them the opportunity to link an existing Principal instead.
        principal = await create_user(db, identity_provider, id)
        (new_identity,) = principal.identities
        new_identity.latest_login = now
    else:
        identity.latest_login = now
        principal = identity.principal
    session_count = (
        await db.execute(
            select(func.count())
            .select_from(orm.Session)
            .join(orm.Principal)
            .filter(orm.Principal.id == principal.id)
        )
    ).scalar()
    if session_count >= SESSION_LIMIT:
        raise HTTPException(
            400,
            f"This Principal already has {session_count} sessions which is greater "
            f"than or equal to the maximum number allowed, {SESSION_LIMIT}. "
            "Some Sessions must be closed before creating new ones.",
        )
    session = orm.Session(
        principal_id=principal.id,
        expiration_time=utcnow() + settings.session_max_age,
        state=state or {},
    )
    db.add(session)
    await db.commit()
    # Reload to select Principal and Identities.
    fully_loaded_session = (
        await db.execute(
            select(orm.Session)
            .options(
                selectinload(orm.Session.principal).selectinload(
                    orm.Principal.identities
                ),
            )
            .filter(orm.Session.id == session.id)
        )
    ).scalar()
    return fully_loaded_session


async def create_tokens_from_session(
    settings: Settings, db: AsyncSession, session, provider
):
    # Provide enough information in the access token to reconstruct Principal
    # and its Identities sufficient for access policy enforcement without a
    # database hit.
    principal = session.principal
    data = {
        "sub": principal.uuid.hex,
        "sub_typ": principal.type,  # Why is this str and not Enum?
        "scp": list(set().union(*[role.scopes for role in principal.roles])),
        "state": session.state,
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
    refresh_token = create_refresh_token(
        session_id=session.uuid.hex,
        expires_delta=settings.refresh_token_max_age,
        secret_key=settings.secret_keys[0],  # Use the *first* secret key to encode.
    )
    # Include the identity. This is not stored as part of the session.
    # Once you are logged in, it does not matter *how* you logged in.
    # But in order to enable UIs to display a sensible username we provide
    # this information alongside the tokens only when the session is first created.
    identity = (
        await db.execute(
            select(orm.Identity)
            .filter(orm.Identity.principal == principal)
            .filter(orm.Identity.provider == provider)
        )
    ).scalar()
    return {
        "access_token": access_token,
        "expires_in": settings.access_token_max_age.total_seconds(),
        "refresh_token": refresh_token,
        "refresh_token_expires_in": settings.refresh_token_max_age.total_seconds(),
        "token_type": "bearer",
        "identity": {"id": identity.id, "provider": provider},
        "principal": principal.uuid.hex,
    }


def add_external_routes(
    router: APIRouter, provider: str, authenticator: ExternalAuthenticator
):
    if not SHARE_TILED_PATH:
        raise Exception(
            "Static assets could not be found and are required for "
            "setting up external OAuth authentication."
        )
    templates = Jinja2Templates(Path(SHARE_TILED_PATH, "templates"))

    "Build an auth_code route function for this Authenticator."

    @router.get(f"/provider/{provider}/code")
    async def auth_code_route(
        request: Request,
        settings: Settings = Depends(get_settings),
        db: Optional[AsyncSession] = Depends(get_database_session),
    ):
        request.state.endpoint = "auth"
        user_session_state: Optional[
            UserSessionState
        ] = await authenticator.authenticate(request)
        if not user_session_state:
            raise HTTPException(
                status_code=HTTP_401_UNAUTHORIZED, detail="Authentication failure"
            )
        session = await create_session(
            settings,
            db,
            provider,
            user_session_state.user_name,
            user_session_state.state,
        )
        tokens = await create_tokens_from_session(settings, db, session, provider)
        return tokens

    "Build an /authorize route function for this Authenticator."

    @router.post(f"/provider/{provider}/authorize")
    async def device_code_authorize_route(
        request: Request,
        db: Optional[AsyncSession] = Depends(get_database_session),
    ):
        request.state.endpoint = "auth"
        pending_session = await create_pending_session(db)
        verification_uri = f"{get_base_url(request)}/auth/provider/{provider}/token"
        authorization_uri = authenticator.authorization_endpoint.copy_with(
            params={
                "client_id": authenticator.client_id,
                "response_type": "code",
                "scope": "openid",
                "redirect_uri": f"{get_base_url(request)}/auth/provider/{provider}/device_code",
            }
        )
        return {
            "authorization_uri": str(
                authorization_uri
            ),  # URL that user should visit in browser
            "verification_uri": str(
                verification_uri
            ),  # URL that terminal client will poll
            "interval": DEVICE_CODE_POLLING_INTERVAL,  # suggested polling interval
            "device_code": pending_session["device_code"],
            "expires_in": DEVICE_CODE_MAX_AGE,  # seconds
            "user_code": pending_session["user_code"],
        }

    @router.get(f"/provider/{provider}/device_code")
    async def device_code_user_code_form_route(
        request: Request,
        code: str,
    ):
        action = (
            f"{get_base_url(request)}/auth/provider/{provider}/device_code?code={code}"
        )
        return templates.TemplateResponse(
            request,
            "device_code_form.html",
            {
                "code": code,
                "action": action,
            },
        )

    "Build an /authorize route function for this Authenticator."

    @router.post(f"/provider/{provider}/device_code")
    async def device_code_user_code_submit_route(
        request: Request,
        code: str = Form(),
        user_code: str = Form(),
        settings: Settings = Depends(get_settings),
        db: Optional[AsyncSession] = Depends(get_database_session),
    ):
        request.state.endpoint = "auth"
        action = (
            f"{get_base_url(request)}/auth/provider/{provider}/device_code?code={code}"
        )
        normalized_user_code = user_code.upper().replace("-", "").strip()
        pending_session = await lookup_valid_pending_session_by_user_code(
            db, normalized_user_code
        )
        if pending_session is None:
            message = "Invalid user code. It may have been mistyped, or the pending request may have expired."
            return templates.TemplateResponse(
                request,
                "device_code_form.html",
                {
                    "code": code,
                    "action": action,
                    "message": message,
                },
                status_code=HTTP_401_UNAUTHORIZED,
            )
        user_session_state: Optional[
            UserSessionState
        ] = await authenticator.authenticate(request)
        if not user_session_state:
            return templates.TemplateResponse(
                request,
                "device_code_failure.html",
                {
                    "message": (
                        "User code was correct but authentication with third party failed. "
                        "Ask administrator to see logs for details."
                    ),
                },
                status_code=HTTP_401_UNAUTHORIZED,
            )
        session = await create_session(
            settings,
            db,
            provider,
            user_session_state.user_name,
            user_session_state.state,
        )
        pending_session.session_id = session.id
        db.add(pending_session)
        await db.commit()
        return templates.TemplateResponse(
            request,
            "device_code_success.html",
            {
                "interval": DEVICE_CODE_POLLING_INTERVAL,
            },
        )

    "Build an /authorize route function for this Authenticator."

    @router.post(f"/provider/{provider}/token")
    async def device_code_token_route(
        request: Request,
        body: schemas.DeviceCode,
        settings: Settings = Depends(get_settings),
        db: Optional[AsyncSession] = Depends(get_database_session),
    ):
        request.state.endpoint = "auth"
        device_code_hex = body.device_code
        try:
            device_code = bytes.fromhex(device_code_hex)
        except Exception:
            # Not valid hex, therefore not a valid device_code
            raise HTTPException(
                status_code=HTTP_401_UNAUTHORIZED, detail="Invalid device code"
            )
        pending_session = await lookup_valid_pending_session_by_device_code(
            db, device_code
        )
        if pending_session is None:
            raise HTTPException(
                404,
                detail="No such device_code. The pending request may have expired.",
            )
        if pending_session.session_id is None:
            raise HTTPException(
                HTTP_400_BAD_REQUEST, {"error": "authorization_pending"}
            )
        session = pending_session.session
        # The pending session can only be used once.
        await db.delete(pending_session)
        await db.commit()
        tokens = await create_tokens_from_session(settings, db, session, provider)
        return tokens


def add_internal_routes(
    router: APIRouter, provider: str, authenticator: InternalAuthenticator
):
    "Register a handle_credentials route function for this Authenticator."

    @router.post(f"/provider/{provider}/token")
    async def handle_credentials_route(
        request: Request,
        form_data: OAuth2PasswordRequestForm = Depends(),
        settings: Settings = Depends(get_settings),
        db: Optional[AsyncSession] = Depends(get_database_session),
    ):
        request.state.endpoint = "auth"
        user_session_state: Optional[
            UserSessionState
        ] = await authenticator.authenticate(
            username=form_data.username, password=form_data.password
        )
        if not user_session_state or not user_session_state.user_name:
            raise HTTPException(
                status_code=HTTP_401_UNAUTHORIZED,
                detail="Incorrect username or password",
                headers={"WWW-Authenticate": "Bearer"},
            )
        session = await create_session(
            settings,
            db,
            provider,
            user_session_state.user_name,
            state=user_session_state.state,
        )
        tokens = await create_tokens_from_session(settings, db, session, provider)
        return tokens


async def generate_apikey(db: AsyncSession, principal, apikey_params, request):
    if apikey_params.scopes is None:
        scopes = ["inherit"]
    else:
        scopes = apikey_params.scopes
    principal_scopes = set().union(*[role.scopes for role in principal.roles])
    if not set(scopes).issubset(principal_scopes | {"inherit"}):
        raise HTTPException(
            403,
            (
                f"Requested scopes {apikey_params.scopes} must be a subset of the "
                f"principal's scopes {list(principal_scopes)}."
            ),
        )
    admin_scopes = ["admin:apikeys"]
    if (access_tags := apikey_params.access_tags) is not None:
        if all(scope in scopes for scope in admin_scopes):
            raise HTTPException(
                403,
                (
                    f"Requested scopes {scopes} contain scopes {admin_scopes}, "
                    f"which cannot be combined with access tag restrictions."
                ),
            )
    if apikey_params.expires_in is not None:
        expiration_time = utcnow() + timedelta(seconds=apikey_params.expires_in)
    else:
        expiration_time = None
    # The standard 32 byes of entropy,
    # plus 4 more for extra safety since we store the first eight HEX chars.
    secret = secrets.token_bytes(4 + 32)
    hashed_secret = hashlib.sha256(secret).digest()
    keys_count = (
        await db.execute(
            select(func.count())
            .select_from(orm.APIKey)
            .join(orm.Principal)
            .filter(orm.Principal.id == principal.id)
        )
    ).scalar()
    if keys_count >= API_KEY_LIMIT:
        raise HTTPException(
            400,
            f"This Principal already has {keys_count} API keys which is greater "
            f"than or equal to the maximum number allowed, {API_KEY_LIMIT}. "
            "Some API keys must be deleted before creating new ones.",
        )
    new_key = orm.APIKey(
        principal_id=principal.id,
        expiration_time=expiration_time,
        note=apikey_params.note,
        scopes=scopes,
        access_tags=access_tags,
        first_eight=secret.hex()[:8],
        hashed_secret=hashed_secret,
    )
    db.add(new_key)
    await db.commit()
    # db.refresh(new_key)
    return json_or_msgpack(
        request,
        schemas.APIKeyWithSecret.from_orm(new_key, secret=secret.hex()).model_dump(),
    )


def authentication_router() -> APIRouter:
    router = APIRouter()

    @router.get(
        "/principal",
        response_model=schemas.Principal,
    )
    async def principal_list(
        request: Request,
        offset: Optional[int] = Query(0, alias="page[offset]", ge=0),
        limit: Optional[int] = Query(
            DEFAULT_PAGE_SIZE, alias="page[limit]", ge=0, le=MAX_PAGE_SIZE
        ),
        principal: Optional[schemas.Principal] = Depends(get_current_principal),
        _=Security(check_scopes, scopes=["read:principals"]),
        db: Optional[AsyncSession] = Depends(get_database_session),
    ):
        "List Principals (users and services)."
        request.state.endpoint = "auth"
        principal_orms = (
            (
                await db.execute(
                    select(orm.Principal)
                    .offset(offset)
                    .limit(limit)
                    .options(
                        selectinload(orm.Principal.identities),
                        selectinload(orm.Principal.roles),
                        selectinload(orm.Principal.api_keys),
                        selectinload(orm.Principal.sessions),
                    )
                )
            )
            .unique()
            .all()
        )
        principals = []
        for (principal_orm,) in principal_orms:
            latest_activity = await latest_principal_activity(db, principal_orm)
            principal = schemas.Principal.from_orm(
                principal_orm, latest_activity
            ).model_dump()
            principals.append(principal)
        return json_or_msgpack(request, principals)

    @router.post(
        "/principal",
        response_model=schemas.Principal,
    )
    async def create_service_principal(
        request: Request,
        principal: Optional[schemas.Principal] = Depends(get_current_principal),
        _=Security(check_scopes, scopes=["write:principals"]),
        db: Optional[AsyncSession] = Depends(get_database_session),
        role: str = Query(...),
    ):
        "Create a principal for a service account."

        principal_orm = await create_service(db, role)

        # Reload to select Principal and Identities.
        fully_loaded_principal_orm = (
            await db.execute(
                select(orm.Principal)
                .options(
                    selectinload(orm.Principal.identities),
                    selectinload(orm.Principal.roles),
                    selectinload(orm.Principal.api_keys),
                    selectinload(orm.Principal.sessions),
                )
                .filter(orm.Principal.id == principal_orm.id)
            )
        ).scalar()

        principal = schemas.Principal.from_orm(fully_loaded_principal_orm).model_dump()
        request.state.endpoint = "auth"

        return json_or_msgpack(request, principal)

    @router.get(
        "/principal/{uuid}",
        response_model=schemas.Principal,
    )
    async def principal(
        request: Request,
        uuid: uuid_module.UUID,
        _=Security(check_scopes, scopes=["read:principals"]),
        db: Optional[AsyncSession] = Depends(get_database_session),
    ):
        "Get information about one Principal (user or service)."
        request.state.endpoint = "auth"
        principal_orm = (
            await db.execute(
                select(orm.Principal)
                .filter(orm.Principal.uuid == uuid)
                .options(
                    selectinload(orm.Principal.identities),
                    selectinload(orm.Principal.roles),
                    selectinload(orm.Principal.api_keys),
                    selectinload(orm.Principal.sessions),
                )
            )
        ).scalar()
        if principal_orm is None:
            raise HTTPException(
                status_code=HTTP_404_NOT_FOUND, detail=f"No such Principal {uuid}"
            )
        latest_activity = await latest_principal_activity(db, principal_orm)

        return json_or_msgpack(
            request,
            schemas.Principal.from_orm(principal_orm, latest_activity).model_dump(),
        )

    @router.delete(
        "/principal/{uuid}/apikey",
        response_model=schemas.Principal,
    )
    async def revoke_apikey_for_principal(
        request: Request,
        uuid: uuid_module.UUID,
        first_eight: str,
        _=Security(check_scopes, scopes=["admin:apikeys"]),
        db: Optional[AsyncSession] = Depends(get_database_session),
    ):
        "Allow Tiled Admins to delete any user's apikeys e.g."
        request.state.endpoint = "auth"
        api_key_orm = (
            await db.execute(
                select(orm.APIKey).filter(orm.APIKey.first_eight == first_eight[:8])
            )
        ).scalar()
        if (api_key_orm is None) or (api_key_orm.principal.uuid != uuid):
            raise HTTPException(
                404,
                f"The principal {uuid} has no such API key.",
            )
        await db.delete(api_key_orm)
        await db.commit()

        return Response(status_code=HTTP_204_NO_CONTENT)

    @router.post(
        "/principal/{uuid}/apikey",
        response_model=schemas.APIKeyWithSecret,
    )
    async def apikey_for_principal(
        request: Request,
        uuid: uuid_module.UUID,
        apikey_params: schemas.APIKeyRequestParams,
        principal: Optional[schemas.Principal] = Depends(get_current_principal),
        _=Security(check_scopes, scopes=["admin:apikeys"]),
        db: Optional[AsyncSession] = Depends(get_database_session),
    ):
        "Generate an API key for a Principal."
        request.state.endpoint = "auth"
        principal = (
            await db.execute(select(orm.Principal).filter(orm.Principal.uuid == uuid))
        ).scalar()
        if principal is None:
            raise HTTPException(
                404, f"Principal {uuid} does not exist or insufficient permissions."
            )
        return await generate_apikey(db, principal, apikey_params, request)

    @router.post("/session/refresh", response_model=schemas.AccessAndRefreshTokens)
    async def refresh_session(
        request: Request,
        refresh_token: schemas.RefreshToken,
        settings: Settings = Depends(get_settings),
        db: Optional[AsyncSession] = Depends(get_database_session),
    ):
        "Obtain a new access token and refresh token."
        request.state.endpoint = "auth"
        new_tokens = await slide_session(refresh_token.refresh_token, settings, db)
        return new_tokens

    @router.post("/session/revoke")
    async def revoke_session(
        request: Request,
        refresh_token: schemas.RefreshToken,
        settings: Settings = Depends(get_settings),
        db: Optional[AsyncSession] = Depends(get_database_session),
    ):
        "Mark a Session as revoked so it cannot be refreshed again."
        request.state.endpoint = "auth"
        payload = decode_token(refresh_token.refresh_token, settings.secret_keys)
        session_id = payload["sid"]
        # Find this session in the database.
        session = await lookup_valid_session(db, session_id)
        if session is None:
            raise HTTPException(HTTP_409_CONFLICT, detail=f"No session {session_id}")
        session.revoked = True
        db.add(session)
        await db.commit()
        return Response(status_code=HTTP_204_NO_CONTENT)

    @router.delete("/session/revoke/{session_id}")
    async def revoke_session_by_id(
        session_id: str,  # from path parameter
        request: Request,
        principal: Optional[schemas.Principal] = Depends(get_current_principal),
        db: Optional[AsyncSession] = Depends(get_database_session),
    ):
        "Mark a Session as revoked so it cannot be refreshed again."
        request.state.endpoint = "auth"
        # Find this session in the database.
        session = await lookup_valid_session(db, session_id)
        if session is None:
            raise HTTPException(404, detail=f"No session {session_id}")
        if principal.uuid != session.principal.uuid:
            # TODO Add a scope for doing this for other users.
            raise HTTPException(
                HTTP_404_NOT_FOUND,
                detail="Sessions does not exist or requester has insufficient permissions",
            )
        session.revoked = True
        db.add(session)
        await db.commit()
        return Response(status_code=HTTP_204_NO_CONTENT)

    async def slide_session(refresh_token, settings, db):
        try:
            payload = decode_token(refresh_token, settings.secret_keys)
        except ExpiredSignatureError:
            raise HTTPException(
                status_code=HTTP_401_UNAUTHORIZED,
                detail="Session has expired. Please re-authenticate.",
            )
        # Find this session in the database.
        session = await lookup_valid_session(db, payload["sid"])
        now = utcnow()
        # This token is *signed* so we know that the information came from us.
        # If the Session is forgotten or revoked or expired, do not allow refresh.
        if (
            (session is None)
            or session.revoked
            or (session.expiration_time.replace(tzinfo=timezone.utc) < now)
        ):
            # Do not leak (to a potential attacker) whether this has been *revoked*
            # specifically. Give the same error as if it had expired.
            raise HTTPException(
                status_code=HTTP_401_UNAUTHORIZED,
                detail="Session has expired. Please re-authenticate.",
            )
        # Update Session info.
        session.time_last_refreshed = now
        # This increments in a way that avoids a race condition.
        session.refresh_count = orm.Session.refresh_count + 1
        # Update the database.
        db.add(session)
        await db.commit()
        # Provide enough information in the access token to reconstruct Principal
        # and its Identities sufficient for access policy enforcement without a
        # database hit.
        data = {
            "sub": session.principal.uuid.hex,
            "sub_typ": session.principal.type,  # Why is this str and not Enum?
            "scp": list(
                set().union(*[role.scopes for role in session.principal.roles])
            ),
            "state": session.state,
            "ids": [
                {"id": identity.id, "idp": identity.provider}
                for identity in session.principal.identities
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
            "expires_in": settings.access_token_max_age.total_seconds(),
            "refresh_token": new_refresh_token,
            "refresh_token_expires_in": settings.refresh_token_max_age.total_seconds(),
            "token_type": "bearer",
        }

    @router.post(
        "/apikey",
        response_model=schemas.APIKeyWithSecret,
    )
    async def new_apikey(
        request: Request,
        apikey_params: schemas.APIKeyRequestParams,
        principal: Optional[schemas.Principal] = Depends(get_current_principal),
        _=Security(check_scopes, scopes=["apikeys"]),
        db: Optional[AsyncSession] = Depends(get_database_session),
    ):
        """
        Generate an API for the currently-authenticated user or service.
        """
        # TODO Permit filtering the fields of the response.
        request.state.endpoint = "auth"
        if principal is None:
            return None
        # The principal from get_current_principal tells us everything that the
        # access_token carries around, but the database knows more than that.
        principal_orm = (
            await db.execute(
                select(orm.Principal).filter(orm.Principal.uuid == principal.uuid)
            )
        ).scalar()
        apikey = await generate_apikey(db, principal_orm, apikey_params, request)
        return apikey

    @router.get("/apikey", response_model=schemas.APIKey)
    async def current_apikey_info(
        request: Request,
        api_key: str = Depends(get_api_key),
        db: Optional[AsyncSession] = Depends(get_database_session),
    ):
        """
        Give info about the API key used to authentication the current request.

        This provides a way to look up the API uuid, given the API secret.
        """
        # TODO Permit filtering the fields of the response.
        request.state.endpoint = "auth"
        if api_key is None:
            raise HTTPException(
                status_code=HTTP_401_UNAUTHORIZED,
                detail="No API key was provided with this request.",
            )
        try:
            secret = bytes.fromhex(api_key)
        except Exception:
            # Not valid hex, therefore not a valid API key
            raise HTTPException(
                status_code=HTTP_401_UNAUTHORIZED, detail="Invalid API key"
            )
        api_key_orm = await lookup_valid_api_key(db, secret)
        if api_key_orm is None:
            raise HTTPException(
                status_code=HTTP_401_UNAUTHORIZED, detail="Invalid API key"
            )
        return json_or_msgpack(
            request, schemas.APIKey.from_orm(api_key_orm).model_dump()
        )

    @router.delete("/apikey")
    async def revoke_apikey(
        request: Request,
        first_eight: str,
        principal: Optional[schemas.Principal] = Depends(get_current_principal),
        _=Security(check_scopes, scopes=["apikeys"]),
        db: Optional[AsyncSession] = Depends(get_database_session),
    ):
        """
        Revoke an API belonging to the currently-authenticated user or service."""
        # TODO Permit filtering the fields of the response.
        request.state.endpoint = "auth"
        if principal is None:
            return None
        api_key_orm = (
            await db.execute(
                select(orm.APIKey).filter(orm.APIKey.first_eight == first_eight[:8])
            )
        ).scalar()
        if (api_key_orm is None) or (api_key_orm.principal.uuid != principal.uuid):
            raise HTTPException(
                404,
                f"The currently-authenticated {principal.type} has no such API key.",
            )
        await db.delete(api_key_orm)
        await db.commit()
        return Response(status_code=HTTP_204_NO_CONTENT)

    @router.get(
        "/whoami",
        response_model=schemas.Principal,
    )
    async def whoami(
        request: Request,
        principal: Optional[schemas.Principal] = Depends(get_current_principal),
        db: Optional[AsyncSession] = Depends(get_database_session),
    ):
        # TODO Permit filtering the fields of the response.
        request.state.endpoint = "auth"
        if principal is None:
            return json_or_msgpack(request, None)
        # The principal from get_current_principal tells us everything that the
        # access_token carries around, but the database knows more than that.
        principal_orm = (
            await db.execute(
                select(orm.Principal)
                .options(
                    selectinload(orm.Principal.identities),
                    selectinload(orm.Principal.roles),
                    selectinload(orm.Principal.api_keys),
                    selectinload(orm.Principal.sessions),
                )
                .filter(orm.Principal.uuid == principal.uuid)
            )
        ).scalar()
        if principal_orm is None:
            raise HTTPException(
                status_code=HTTP_401_UNAUTHORIZED, detail="Principal no longer exists."
            )
        latest_activity = await latest_principal_activity(db, principal_orm)
        return json_or_msgpack(
            request,
            schemas.Principal.from_orm(principal_orm, latest_activity).model_dump(),
        )

    @router.post("/logout", include_in_schema=False, deprecated=True)
    async def logout(
        request: Request,
        response: Response,
    ):
        "Deprecated. See revoke_session: POST /session/revoke."
        request.state.endpoint = "auth"
        response.delete_cookie(API_KEY_COOKIE_NAME)
        return {}

    return router
