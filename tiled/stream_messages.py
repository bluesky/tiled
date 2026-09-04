from datetime import datetime
from typing import Annotated, Generic, Literal, Optional, TypeVar, Union, Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, TypeAdapter

from tiled.server.schemas import EnvelopeFormat

from .structures.array import ArrayStructure, BuiltinDtype, StructDtype
from .structures.core import Spec, StructureFamily
from .structures.data_source import Management


class Asset(BaseModel):
    id: Optional[int]  # TODO This should be required, needs work on the SQL
    data_uri: str
    is_directory: bool
    parameter: str
    num: int


StructureT = TypeVar("StructureT")


class DataSource(BaseModel, Generic[StructureT]):
    id: Optional[int]  # TODO This should be required, needs work on the SQL
    structure_family: StructureFamily
    structure: StructureT
    mimetype: str
    parameters: dict
    # TODO: make `properties` required in a future release. See Issue #1300
    properties: dict = Field(default_factory=dict)
    assets: list[Asset]
    management: Management


class Schema(BaseModel):
    version: int

    def content(self):
        return self.model_dump(exclude={"type", "version"})


class ArraySchema(Schema):
    type: Literal["array-schema"]
    data_type: Union[BuiltinDtype, StructDtype]


class ContainerSchema(Schema):
    type: Literal["container-schema"]
    pass


class TableSchema(Schema):
    type: Literal["table-schema"]
    arrow_schema: str


class Update(BaseModel):
    sequence: int = Field(gt=0)
    timestamp: datetime


class ChildCreated(Update):
    type: Literal["container-child-created"] = "container-child-created"
    key: str
    structure_family: StructureFamily
    specs: list[Spec]
    metadata: dict
    data_sources: list[DataSource]
    access_blob: dict


class ChildMetadataUpdated(Update):
    type: Literal[
        "container-child-metadata-updated"
    ] = "container-child-metadata-updated"
    key: str
    specs: list[Spec]
    metadata: dict


class ArrayData(Update):
    type: Literal["array-data"] = "array-data"
    mimetype: str
    shape: tuple[int, ...]
    offset: Optional[tuple[int, ...]]
    block: Optional[tuple[int, ...]]
    payload: bytes
    data_type: Union[BuiltinDtype, StructDtype]


class ArrayPatch(BaseModel):
    offset: tuple[int, ...]
    shape: tuple[int, ...]


class ArrayRef(Update):
    type: Literal["array-ref"] = "array-ref"
    data_source: DataSource[ArrayStructure]
    patch: Optional[ArrayPatch]
    uri: Optional[str]
    shape: tuple[int, ...]
    data_type: Union[BuiltinDtype, StructDtype]


class TableData(Update):
    type: Literal["table-data"] = "table-data"
    mimetype: str
    # partition=None means a write to the entire table, an old design choice
    # that may need revisiting.
    partition: Optional[int]
    append: bool
    payload: bytes
    arrow_schema: str


## Definition of a bi-directional websocket protocol. Based off of https://github.com/enisdenjo/graphql-ws/blob/master/PROTOCOL.md

# Client -> Server. Authenticate client via websocket connection
class ConnectionAuthMsg(BaseModel):
    type: Literal["connection_auth"] = "connection_auth"
    envelope_format: EnvelopeFormat = EnvelopeFormat.json
    api_key: Optional[str] = None
    access_token: Optional[str] = None

# Server -> Client. Auth successfully, server acknowledges client
class ConnectionAckMsg(BaseModel):
    type: Literal["connection_ack"] = "connection_ack"

# Client -> Server. Subscribe to tiled node
class SubscribeMsg(BaseModel):
    type: Literal["subscribe"] = "subscribe"
    path: str
    start: int = 0


class SubscribeAckMsg(BaseModel):
    type: Literal["subscribe_ack"]
    path: str
    node_id: UUID

# Server -> Client. Packet of data, use the same UUID that was used to subscribe
class NextMsg(BaseModel):
    id: UUID
    type: Literal["next"] = "next"
    metadata: Any
    # sequence: int

# Server -> Client. Error message
class ErrorMsg(BaseModel):
    # path: Optional[str] = None
    id: Optional[UUID] = None
    type: Literal["error"] = "error"
    error: str

# Bidirectional. Either client or server stops a node from streaming
class CompleteMsg(BaseModel):
    path: Optional[UUID] = None
    type: Literal["complete"] = "complete"

class Ping(BaseModel):
    type: Literal["ping"] = "ping"
    payload: Optional[str] = None

class Pong(BaseModel):
    type: Literal["pong"] = "pong"
    payload: Optional[str] = None

WebsocketMsg = Annotated[Union[ConnectionAuthMsg, ConnectionAckMsg, SubscribeMsg, NextMsg, ErrorMsg, CompleteMsg, Ping, Pong], Field(discriminator="type"),]

websocket_msg_adapter = TypeAdapter(WebsocketMsg)
