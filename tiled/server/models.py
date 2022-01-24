import enum
import uuid
from datetime import datetime
from typing import Any, Dict, Generic, List, Optional, Tuple, TypeVar

import pydantic
import pydantic.dataclasses
import pydantic.generics

DataT = TypeVar("DataT")


class Error(pydantic.BaseModel):
    code: int
    message: str


class Response(pydantic.generics.GenericModel, Generic[DataT]):
    data: Optional[DataT]
    error: Optional[Error]
    meta: Optional[dict]
    links: Optional[dict]

    @pydantic.validator("error", always=True)
    def check_consistency(cls, v, values):
        if v is not None and values["data"] is not None:
            raise ValueError("must not provide both data and error")
        if v is None and values.get("data") is None:
            raise ValueError("must provide data or error")
        return v


class EntryFields(str, enum.Enum):
    metadata = "metadata"
    structure_family = "structure_family"
    microstructure = "structure.micro"
    macrostructure = "structure.macro"
    count = "count"
    sorting = "sorting"
    specs = "specs"
    none = ""


class StructureFamilies(str, enum.Enum):
    node = "node"
    array = "array"
    dataframe = "dataframe"
    xarray_data_array = "xarray_data_array"
    xarray_dataset = "xarray_dataset"


class NodeAttributes(pydantic.BaseModel):
    structure_family: Optional[StructureFamilies]
    specs: Optional[List[str]]
    metadata: Optional[dict]  # free-form, user-specified dict
    structure: Optional[Any]  # TODO Figure out how to deal with dataclasses in FastAPI
    count: Optional[int]
    sorting: Optional[List[Tuple[str, int]]]


class Resource(pydantic.BaseModel):
    "A JSON API Resource"
    id: str
    meta: Optional[dict]
    links: Optional[dict]
    attributes: NodeAttributes


class AccessAndRefreshTokens(pydantic.BaseModel):
    access_token: str
    expires_in: int
    refresh_token: str
    refresh_token_expires_in: int
    token_type: str


class RefreshToken(pydantic.BaseModel):
    refresh_token: str


class TokenData(pydantic.BaseModel):
    username: Optional[str] = None


class AuthenticationMode(str, enum.Enum):
    password = "password"
    external = "external"


class AboutAuthenticationProvider(pydantic.BaseModel):
    provider: str
    mode: AuthenticationMode
    links: Dict[str, str]
    confirmation_message: Optional[str]


class AboutAuthenticationLinks(pydantic.BaseModel):
    whoami: str
    new_apikey: str
    revoke_apikey: str
    refresh_session: str
    revoke_session: str
    logout: str


class AboutAuthentication(pydantic.BaseModel):
    required: bool
    providers: List[AboutAuthenticationProvider]
    links: Optional[AboutAuthenticationLinks]


class About(pydantic.BaseModel):
    api_version: int
    library_version: str
    formats: Dict[str, List[str]]
    aliases: Dict[str, Dict[str, List[str]]]
    queries: List[str]
    authentication: AboutAuthentication
    links: Dict[str, str]
    meta: dict


class PrincipalType(str, enum.Enum):
    user = "user"
    service = "service"


class Identity(pydantic.BaseModel, orm_mode=True):
    id: pydantic.constr(max_length=255)
    provider: pydantic.constr(max_length=255)


class Role(pydantic.BaseModel, orm_mode=True):
    name: str
    scopes: List[str]
    # principals


class APIKey(pydantic.BaseModel, orm_mode=True):
    uuid: uuid.UUID
    expiration_time: Optional[datetime]
    note: Optional[pydantic.constr(max_length=255)]
    scopes: List[str]


class APIKeyWithSecret(pydantic.BaseModel):
    uuid: uuid.UUID
    principal: uuid.UUID
    expiration_time: Optional[datetime]
    note: Optional[pydantic.constr(max_length=255)]
    scopes: List[str]
    secret: str  # hex-encoded bytes


class APIKeyResponse(pydantic.BaseModel):
    data: APIKey


class APIKeyWithSecretResponse(pydantic.BaseModel):
    data: APIKeyWithSecret


class Session(pydantic.BaseModel, orm_mode=True):
    """
    This related to refresh tokens, which have a session uuid ("sid") claim.

    When the client attempts to use a refresh token, we first check
    here to ensure that the "session", which is associated with a chain
    of refresh tokens that came from a single authentication, are still valid.
    """

    # The id field (primary key) is intentionally not exposed to the application.
    # It is left as an internal database concern.
    uuid: uuid.UUID
    expiration_time: datetime
    revoked: bool


class Principal(pydantic.BaseModel, orm_mode=True):
    "Represents a User or Service"
    # The id field (primary key) is intentionally not exposed to the application.
    # It is left as an internal database concern.
    uuid: uuid.UUID
    type: PrincipalType
    identities: List[Identity] = []
    roles: List[Role] = []
    api_keys: List[APIKey] = []
    sessions: List[Session] = []


class WhoAmI(pydantic.BaseModel):
    data: Principal


class APIKeyParams(pydantic.BaseModel):
    lifetime: Optional[int]  # seconds
    scopes: Optional[List[str]]
    note: Optional[str]
