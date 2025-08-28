import collections
import dataclasses
import inspect
import os
import re
import warnings
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from functools import partial
from pathlib import Path
from typing import Callable, List, Optional, Set, TypeVar, Union

import anyio
import packaging
import pydantic_settings
from fastapi import (
    APIRouter,
    Body,
    Depends,
    HTTPException,
    Query,
    Request,
    Security,
    WebSocket,
)
from jmespath.exceptions import JMESPathError
from json_merge_patch import merge as apply_merge_patch
from jsonpatch import apply_patch as apply_json_patch
from starlette.requests import URL
from starlette.status import (
    HTTP_200_OK,
    HTTP_206_PARTIAL_CONTENT,
    HTTP_400_BAD_REQUEST,
    HTTP_403_FORBIDDEN,
    HTTP_404_NOT_FOUND,
    HTTP_405_METHOD_NOT_ALLOWED,
    HTTP_406_NOT_ACCEPTABLE,
    HTTP_410_GONE,
    HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE,
    HTTP_422_UNPROCESSABLE_ENTITY,
)

from tiled.adapters.protocols import AnyAdapter
from tiled.media_type_registration import SerializationRegistry
from tiled.query_registration import QueryRegistry
from tiled.schemas import About
from tiled.server.protocols import ExternalAuthenticator, InternalAuthenticator
from tiled.server.schemas import Principal

from .. import __version__
from ..ndslice import NDSlice
from ..structures.core import Spec, StructureFamily
from ..type_aliases import Scopes
from ..utils import BrokenLink, ensure_awaitable, patch_mimetypes, path_from_uri
from ..validation_registration import ValidationError, ValidationRegistry
from . import schemas
from .authentication import (
    check_scopes,
    get_current_access_tags,
    get_current_access_tags_websocket,
    get_current_principal,
    get_current_principal_websocket,
    get_current_scopes,
    get_current_scopes_websocket,
    get_session_state,
)
from .core import (
    DEFAULT_PAGE_SIZE,
    DEPTH_LIMIT,
    MAX_PAGE_SIZE,
    NoEntry,
    UnsupportedMediaTypes,
    WrongTypeForRoute,
    apply_search,
    construct_data_response,
    construct_entries_response,
    construct_resource,
    construct_revisions_response,
    get_websocket_envelope_formatter,
    json_or_msgpack,
    resolve_media_type,
)
from .dependencies import (
    block,
    expected_shape,
    get_entry,
    get_root_tree,
    offset_param,
    shape_param,
)
from .file_response_with_range import FileResponseWithRange
from .links import links_for_node
from .settings import Settings, get_settings
from .utils import (
    filter_for_access,
    get_base_url,
    get_base_url_websocket,
    record_timing,
)

T = TypeVar("T")


def _patch_route_signature(
    query_registry: QueryRegistry,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    This is done dynamically at router startup.

    We check the registry of known search query types, which is user
    configurable, and use that to define the allowed HTTP query parameters for
    this route.

    Take a route that accept unspecified search queries as **filters.
    Return a wrapped version of the route that has the supported
    search queries explicitly spelled out in the function signature.

    This has no change in the actual behavior of the function,
    but it enables FastAPI to generate good OpenAPI documentation
    showing the supported search queries.

    """

    def inner(route: Callable[..., T]) -> Callable[..., T]:
        # Build a wrapper so that we can modify the signature
        # without mutating the wrapped original.

        async def route_with_sig(*args, **kwargs):
            return await route(*args, **kwargs)

        # Black magic here! FastAPI bases its validation and auto-generated swagger
        # documentation on the signature of the route function. We do not know what
        # that signature should be at compile-time. We only know it once we have a
        # chance to check the user-configurable registry of query types. Therefore,
        # we modify the signature here, at runtime, just before handing it to
        # FastAPI in the usual way.

        # When FastAPI calls the function with these added parameters, they will be
        # accepted via **filters.

        # Make a copy of the original parameters.
        signature = inspect.signature(route)
        parameters = list(signature.parameters.values())
        # Drop the **filters parameter from the signature.
        del parameters[-1]
        # Add a parameter for each field in each type of query.
        for name, query in query_registry.name_to_query_type.items():
            for field in dataclasses.fields(query):
                # The structured "alias" here is based on
                # https://mglaman.dev/blog/using-json-router-query-your-search-router-indexes
                if getattr(field.type, "__origin__", None) is list:
                    field_type = str
                else:
                    field_type = field.type
                injected_parameter = inspect.Parameter(
                    name=f"filter___{name}___{field.name}",
                    kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    default=Query(
                        None, alias=f"filter[{name}][condition][{field.name}]"
                    ),
                    annotation=Optional[List[field_type]],
                )
                parameters.append(injected_parameter)
        route_with_sig.__signature__ = signature.replace(parameters=parameters)
        # End black magic

        return route_with_sig

    return inner


def get_router(
    query_registry: QueryRegistry,
    serialization_registry: SerializationRegistry,
    deserialization_registry: SerializationRegistry,
    validation_registry: ValidationRegistry,
    authenticators: dict[str, Union[ExternalAuthenticator, InternalAuthenticator]],
) -> APIRouter:
    router = APIRouter()

    @router.get("/", response_model=About)
    async def about(
        request: Request,
        settings: Settings = Depends(get_settings),
    ):
        # TODO The lazy import of entry modules and serializers means that the
        # lists of formats are not populated until they are first used. Not very
        # helpful for discovery! The registration can be made non-lazy, while the
        # imports of the underlying I/O libraries themselves (openpyxl, pillow,
        # etc.) can remain lazy.
        request.state.endpoint = "about"
        base_url = get_base_url(request)
        authentication = {
            "required": not settings.allow_anonymous_access,
        }
        provider_specs = []
        user_agent = request.headers.get("user-agent", "")
        # The name of the "internal" mode used to be "password".
        # This ensures back-compat with older Python clients.
        internal_mode_name = "internal"
        MINIMUM_INTERNAL_PYTHON_CLIENT_VERSION = packaging.version.parse("0.1.0b17")
        if user_agent.startswith("python-tiled/"):
            agent, _, raw_version = user_agent.partition("/")
            try:
                parsed_version = packaging.version.parse(raw_version)
            except Exception:
                pass
            else:
                if parsed_version < MINIMUM_INTERNAL_PYTHON_CLIENT_VERSION:
                    internal_mode_name = "password"
        for provider, authenticator in authenticators.items():
            if isinstance(authenticator, InternalAuthenticator):
                spec = {
                    "provider": provider,
                    "mode": internal_mode_name,
                    "links": {
                        "auth_endpoint": f"{base_url}/auth/provider/{provider}/token"
                    },
                    "confirmation_message": getattr(
                        authenticator, "confirmation_message", None
                    ),
                }
            elif isinstance(authenticator, ExternalAuthenticator):
                spec = {
                    "provider": provider,
                    "mode": "external",
                    "links": {
                        "auth_endpoint": f"{base_url}/auth/provider/{provider}/authorize"
                    },
                    "confirmation_message": getattr(
                        authenticator, "confirmation_message", None
                    ),
                }
            else:
                # It should be impossible to reach here.
                assert False
            provider_specs.append(spec)
        if provider_specs:
            # If there are *any* authenticaiton providers, these
            # endpoints will be added.
            authentication["links"] = {
                "whoami": f"{base_url}/auth/whoami",
                "apikey": f"{base_url}/auth/apikey",
                "refresh_session": f"{base_url}/auth/session/refresh",
                "revoke_session": f"{base_url}/auth/session/revoke/{{session_id}}",
                "logout": f"{base_url}/auth/logout",
            }
        authentication["providers"] = provider_specs

        return json_or_msgpack(
            request,
            About(
                library_version=__version__,
                api_version=0,
                formats={
                    structure_family: list(
                        serialization_registry.media_types(structure_family)
                    )
                    for structure_family in serialization_registry.structure_families
                },
                aliases={
                    structure_family: serialization_registry.aliases(structure_family)
                    for structure_family in serialization_registry.structure_families
                },
                queries=list(query_registry.name_to_query_type),
                authentication=authentication,
                links={
                    "self": base_url,
                    "documentation": f"{base_url}/docs",
                },
                meta={"root_path": request.scope.get("root_path") or "" + "/api"},
            ).model_dump(),
            expires=datetime.now(timezone.utc) + timedelta(seconds=600),
        )

    @router.get(
        "/search/{path:path}",
        response_model=schemas.Response[
            List[schemas.Resource[schemas.NodeAttributes, dict, dict]],
            schemas.PaginationLinks,
            dict,
        ],
    )
    @_patch_route_signature(query_registry)
    async def search(
        request: Request,
        path: str,
        fields: Optional[List[schemas.EntryFields]] = Query(list(schemas.EntryFields)),
        select_metadata: Optional[str] = Query(None),
        offset: Optional[int] = Query(0, alias="page[offset]", ge=0),
        limit: Optional[int] = Query(
            DEFAULT_PAGE_SIZE, alias="page[limit]", ge=0, le=MAX_PAGE_SIZE
        ),
        sort: Optional[str] = Query(None),
        max_depth: Optional[int] = Query(None, ge=0, le=DEPTH_LIMIT),
        omit_links: bool = Query(False),
        include_data_sources: bool = Query(False),
        principal: Optional[Principal] = Depends(get_current_principal),
        root_tree=Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        settings: Settings = Depends(get_settings),
        _=Security(check_scopes, scopes=["read:metadata"]),
        **filters,
    ):
        entry = await get_entry(
            path,
            ["read:metadata"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            request.state.metrics,
            {StructureFamily.container},
            getattr(request.app.state, "access_policy", None),
        )
        request.state.endpoint = "search"
        try:
            (
                resource,
                metadata_stale_at,
                must_revalidate,
            ) = await construct_entries_response(
                query_registry,
                entry,
                "/search",
                path,
                offset,
                limit,
                fields,
                select_metadata,
                omit_links,
                include_data_sources,
                filters,
                sort,
                get_base_url(request),
                resolve_media_type(request),
                max_depth=max_depth,
                exact_count_limit=settings.exact_count_limit,
            )
            # We only get one Expires header, so if different parts
            # of this response become stale at different times, we
            # cite the earliest one.
            entries_stale_at = getattr(entry, "entries_stale_at", None)
            headers = {}
            if (metadata_stale_at is None) or (entries_stale_at is None):
                expires = None
            else:
                expires = min(metadata_stale_at, entries_stale_at)
            if must_revalidate:
                headers["Cache-Control"] = "must-revalidate"
            return json_or_msgpack(
                request,
                resource.model_dump(),
                expires=expires,
                headers=headers,
            )
        except BrokenLink as err:
            raise HTTPException(status_code=HTTP_410_GONE, detail=err.args[0])
        except NoEntry:
            raise HTTPException(status_code=HTTP_404_NOT_FOUND, detail="No such entry.")
        except WrongTypeForRoute as err:
            raise HTTPException(status_code=HTTP_404_NOT_FOUND, detail=err.args[0])
        except JMESPathError as err:
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail=f"Malformed 'select_metadata' parameter raised JMESPathError: {err}",
            )

    @router.get(
        "/distinct/{path:path}",
        response_model=schemas.GetDistinctResponse,
    )
    @_patch_route_signature(query_registry)
    async def distinct(
        path: str,
        request: Request,
        structure_families: bool = False,
        specs: bool = False,
        metadata: Optional[List[str]] = Query(default=[]),
        counts: bool = False,
        principal: Optional[Principal] = Depends(get_current_principal),
        root_tree=Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        _=Security(check_scopes, scopes=["read:metadata"]),
        **filters,
    ):
        entry = await get_entry(
            path,
            ["read:metadata"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            request.state.metrics,
            None,
            getattr(request.app.state, "access_policy", None),
        )
        if hasattr(entry, "get_distinct"):
            filtered = await apply_search(entry, filters, query_registry)
            distinct = await ensure_awaitable(
                filtered.get_distinct, metadata, structure_families, specs, counts
            )

            return json_or_msgpack(
                request,
                schemas.GetDistinctResponse.model_validate(distinct).model_dump(),
            )
        else:
            raise HTTPException(
                status_code=HTTP_405_METHOD_NOT_ALLOWED,
                detail="This node does not support distinct.",
            )

    @router.get(
        "/metadata/{path:path}",
        response_model=schemas.Response[
            schemas.Resource[schemas.NodeAttributes, dict, dict], dict, dict
        ],
    )
    async def metadata(
        request: Request,
        path: str,
        fields: Optional[List[schemas.EntryFields]] = Query(list(schemas.EntryFields)),
        select_metadata: Optional[str] = Query(None),
        max_depth: Optional[int] = Query(None, ge=0, le=DEPTH_LIMIT),
        omit_links: bool = Query(False),
        include_data_sources: bool = Query(False),
        root_path: bool = Query(False),
        principal: Optional[Principal] = Depends(get_current_principal),
        root_tree=Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        settings: Settings = Depends(get_settings),
        _=Security(check_scopes, scopes=["read:metadata"]),
    ):
        """Fetch the metadata and structure information for one entry"""
        entry = await get_entry(
            path,
            ["read:metadata"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            request.state.metrics,
            None,
            getattr(request.app.state, "access_policy", None),
        )
        request.state.endpoint = "metadata"
        base_url = get_base_url(request)
        path_parts = [segment for segment in path.split("/") if segment]
        try:
            resource = await construct_resource(
                base_url,
                path_parts,
                entry,
                fields,
                select_metadata,
                omit_links,
                include_data_sources,
                resolve_media_type(request),
                max_depth=max_depth,
                exact_count_limit=settings.exact_count_limit,
            )
        except BrokenLink as err:
            raise HTTPException(status_code=HTTP_410_GONE, detail=err.args[0])
        except JMESPathError as err:
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail=f"Malformed 'select_metadata' parameter raised JMESPathError: {err}",
            )
        meta = {"root_path": request.scope.get("root_path") or "/"} if root_path else {}

        return json_or_msgpack(
            request,
            schemas.Response(data=resource, meta=meta).model_dump(),
            expires=getattr(entry, "metadata_stale_at", None),
        )

    @router.get(
        "/array/block/{path:path}", response_model=schemas.Response, name="array block"
    )
    async def array_block(
        request: Request,
        path: str,
        block=Depends(block),
        slice=Depends(NDSlice.from_query),
        expected_shape=Depends(expected_shape),
        format: Optional[str] = None,
        filename: Optional[str] = None,
        settings: Settings = Depends(get_settings),
        principal: Optional[Principal] = Depends(get_current_principal),
        root_tree=Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        _=Security(check_scopes, scopes=["read:data"]),
    ):
        """
        Fetch a chunk of array-like data.
        """
        entry = await get_entry(
            path,
            ["read:data"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            request.state.metrics,
            {StructureFamily.array, StructureFamily.sparse},
            getattr(request.app.state, "access_policy", None),
        )
        shape = entry.structure().shape
        # Check that block dimensionality matches array dimensionality.
        ndim = len(shape)
        if len(block) != ndim:
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail=(
                    f"Block parameter must have {ndim} comma-separated parameters, "
                    f"corresponding to the dimensions of this {ndim}-dimensional array."
                ),
            )
        if block == ():
            # Handle special case of numpy scalar.
            if shape != ():
                raise HTTPException(
                    status_code=HTTP_400_BAD_REQUEST,
                    detail=f"Requested scalar but shape is {entry.structure().shape}",
                )
            with record_timing(request.state.metrics, "read"):
                array = await ensure_awaitable(entry.read)
        else:
            try:
                with record_timing(request.state.metrics, "read"):
                    array = await ensure_awaitable(entry.read_block, block, slice)
            except IndexError:
                raise HTTPException(
                    status_code=HTTP_400_BAD_REQUEST, detail="Block index out of range"
                )
            if (expected_shape is not None) and (expected_shape != array.shape):
                raise HTTPException(
                    status_code=HTTP_400_BAD_REQUEST,
                    detail=f"The expected_shape {expected_shape} does not match the actual shape {array.shape}",
                )
        if array.nbytes > settings.response_bytesize_limit:
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail=(
                    f"Response would exceed {settings.response_bytesize_limit}. "
                    "Use slicing ('?slice=...') to request smaller chunks."
                ),
            )
        try:
            with record_timing(request.state.metrics, "pack"):
                return await construct_data_response(
                    entry.structure_family,
                    serialization_registry,
                    array,
                    entry.metadata(),
                    request,
                    format,
                    specs=getattr(entry, "specs", []),
                    expires=getattr(entry, "content_stale_at", None),
                    filename=filename,
                )
        except UnsupportedMediaTypes as err:
            # raise HTTPException(status_code=406, detail=", ".join(err.supported))
            raise HTTPException(status_code=HTTP_406_NOT_ACCEPTABLE, detail=err.args[0])

    @router.get(
        "/array/full/{path:path}", response_model=schemas.Response, name="full array"
    )
    async def array_full(
        path: str,
        request: Request,
        slice=Depends(NDSlice.from_query),
        expected_shape=Depends(expected_shape),
        format: Optional[str] = None,
        filename: Optional[str] = None,
        settings: Settings = Depends(get_settings),
        principal: Optional[Principal] = Depends(get_current_principal),
        root_tree=Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        _=Security(check_scopes, scopes=["read:data"]),
    ):
        """
        Fetch a slice of array-like data.
        """
        entry = await get_entry(
            path,
            ["read:data"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            request.state.metrics,
            {StructureFamily.array, StructureFamily.sparse},
            getattr(request.app.state, "access_policy", None),
        )
        structure_family = entry.structure_family
        # Deferred import because this is not a required dependency of the server
        # for some use cases.
        import numpy

        try:
            with record_timing(request.state.metrics, "read"):
                array = await ensure_awaitable(entry.read, slice)
            if structure_family == StructureFamily.array:
                array = numpy.asarray(array)  # Force dask or PIMS or ... to do I/O.
        except IndexError:
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST, detail="Block index out of range"
            )
        if (expected_shape is not None) and (expected_shape != array.shape):
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail=f"The expected_shape {expected_shape} does not match the actual shape {array.shape}",
            )
        if array.nbytes > settings.response_bytesize_limit:
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail=(
                    f"Response would exceed {settings.response_bytesize_limit}. "
                    "Use slicing ('?slice=...') to request smaller chunks."
                ),
            )
        try:
            with record_timing(request.state.metrics, "pack"):
                return await construct_data_response(
                    structure_family,
                    serialization_registry,
                    array,
                    entry.metadata(),
                    request,
                    format,
                    specs=getattr(entry, "specs", []),
                    expires=getattr(entry, "content_stale_at", None),
                    filename=filename,
                )
        except UnsupportedMediaTypes as err:
            raise HTTPException(status_code=HTTP_406_NOT_ACCEPTABLE, detail=err.args[0])

    @router.delete("/stream/close/{path:path}")
    async def close_stream(
        request: Request,
        path: str,
        principal: Optional[schemas.Principal] = Depends(get_current_principal),
        root_tree: pydantic_settings.BaseSettings = Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        _=Security(check_scopes, scopes=["write:data"]),
    ):
        entry = await get_entry(
            path,
            ["write:data"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            request.state.metrics,
            {StructureFamily.array, StructureFamily.sparse},
            getattr(request.app.state, "access_policy", None),
        )
        await entry.close_stream()

    @router.websocket("/stream/single/{path:path}")
    async def websocket_endpoint(
        websocket: WebSocket,
        path: str,
        envelope_format: schemas.EnvelopeFormat = schemas.EnvelopeFormat.json,
        start: Optional[int] = None,
        principal: Optional[schemas.Principal] = Depends(
            get_current_principal_websocket
        ),
        authn_access_tags: Optional[Set[str]] = Depends(
            get_current_access_tags_websocket
        ),
        authn_scopes: Scopes = Depends(get_current_scopes_websocket),
    ):
        root_tree = websocket.app.state.root_tree
        websocket.state.metrics = collections.defaultdict(
            lambda: collections.defaultdict(lambda: 0)
        )
        entry = await get_entry(
            path,
            ["read:data", "read:metadata"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            {},  # session_state,
            websocket.state.metrics,
            {
                StructureFamily.array,
                StructureFamily.container,
                StructureFamily.sparse,
            },
            getattr(websocket.app.state, "access_policy", None),
        )
        formatter = get_websocket_envelope_formatter(
            envelope_format, entry, deserialization_registry
        )
        base_websocket_url = URL(get_base_url_websocket(websocket))
        scheme = "https" if base_websocket_url.scheme == "wss" else "http"
        path_parts = [segment for segment in path.split("/") if segment]
        path_str = "/".join(path_parts)
        uri = f"{base_websocket_url.replace(scheme=scheme)}/array/full/{path_str}"
        handler = entry.make_ws_handler(websocket, formatter, uri)
        await handler(start)

    @router.get(
        "/table/partition/{path:path}",
        response_model=schemas.Response,
        name="table partition",
    )
    async def get_table_partition(
        path: str,
        request: Request,
        partition: int,
        column: Optional[List[str]] = Query(None, min_length=1),
        field: Optional[List[str]] = Query(None, min_length=1, deprecated=True),
        format: Optional[str] = None,
        filename: Optional[str] = None,
        settings: Settings = Depends(get_settings),
        principal: Optional[Principal] = Depends(get_current_principal),
        root_tree=Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        _=Security(check_scopes, scopes=["read:data"]),
    ):
        """
        Fetch a partition (continuous block of rows) from a DataFrame [GET route].
        """
        entry = await get_entry(
            path,
            ["read:data"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            request.state.metrics,
            {StructureFamily.table},
            getattr(request.app.state, "access_policy", None),
        )
        if (field is not None) and (column is not None):
            redundant_field_and_column = " ".join(
                (
                    "Cannot accept both 'column' and 'field' query parameters",
                    "in the same /table/partition request.",
                    "Include these query values using only the 'column' parameter.",
                )
            )
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST, detail=redundant_field_and_column
            )
        elif field is not None:
            field_is_deprecated = " ".join(
                (
                    "Query parameter 'field' is deprecated for the /table/partition route.",
                    "Instead use the query parameter 'column'.",
                )
            )
            warnings.warn(field_is_deprecated, DeprecationWarning)
        return await table_partition(
            request=request,
            partition=partition,
            entry=entry,
            column=(column or field),
            format=format,
            filename=filename,
            settings=settings,
        )

    @router.post(
        "/table/partition/{path:path}",
        response_model=schemas.Response,
        name="table partition",
    )
    async def post_table_partition(
        path: str,
        request: Request,
        partition: int,
        column: Optional[List[str]] = Body(None, min_length=1),
        format: Optional[str] = None,
        filename: Optional[str] = None,
        settings: Settings = Depends(get_settings),
        principal: Optional[Principal] = Depends(get_current_principal),
        root_tree=Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        _=Security(check_scopes, scopes=["read:data"]),
    ):
        """
        Fetch a partition (continuous block of rows) from a DataFrame [POST route].
        """
        entry = await get_entry(
            path,
            ["read:data"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            request.state.metrics,
            {StructureFamily.table},
            getattr(request.app.state, "access_policy", None),
        )
        return await table_partition(
            request=request,
            partition=partition,
            entry=entry,
            column=column,
            format=format,
            filename=filename,
            settings=settings,
        )

    async def table_partition(
        request: Request,
        partition: int,
        entry: AnyAdapter,
        column: Optional[List[str]],
        format: Optional[str],
        filename: Optional[str],
        settings: Settings,
    ):
        """
        Fetch a partition (continuous block of rows) from a DataFrame.
        """
        try:
            # The singular/plural mismatch here of "fields" and "field" is
            # due to the ?field=A&field=B&field=C... encodes in a URL.
            with record_timing(request.state.metrics, "read"):
                df = await ensure_awaitable(entry.read_partition, partition, column)
        except IndexError:
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST, detail="Partition out of range"
            )
        except KeyError as err:
            (key,) = err.args
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST, detail=f"No such field {key}."
            )
        if df.memory_usage().sum() > settings.response_bytesize_limit:
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail=(
                    f"Response would exceed {settings.response_bytesize_limit}. "
                    "Select a subset of the columns ('?field=...') to "
                    "request a smaller chunks."
                ),
            )
        try:
            with record_timing(request.state.metrics, "pack"):
                return await construct_data_response(
                    StructureFamily.table,
                    serialization_registry,
                    df,
                    entry.metadata(),
                    request,
                    format,
                    specs=getattr(entry, "specs", []),
                    expires=getattr(entry, "content_stale_at", None),
                    filename=filename,
                )
        except UnsupportedMediaTypes as err:
            raise HTTPException(status_code=HTTP_406_NOT_ACCEPTABLE, detail=err.args[0])

    @router.get(
        "/table/full/{path:path}",
        response_model=schemas.Response,
        name="full 'table' data",
    )
    async def get_table_full(
        request: Request,
        path: str,
        column: Optional[List[str]] = Query(None, min_length=1),
        format: Optional[str] = None,
        filename: Optional[str] = None,
        settings: Settings = Depends(get_settings),
        principal: Optional[Principal] = Depends(get_current_principal),
        root_tree=Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        _=Security(check_scopes, scopes=["read:data"]),
    ):
        """
        Fetch the data for the given table [GET route].
        """
        entry = await get_entry(
            path,
            ["read:data"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            request.state.metrics,
            {StructureFamily.table},
            getattr(request.app.state, "access_policy", None),
        )
        return await table_full(
            request=request,
            entry=entry,
            column=column,
            format=format,
            filename=filename,
            settings=settings,
        )

    @router.post(
        "/table/full/{path:path}",
        response_model=schemas.Response,
        name="full 'table' data",
    )
    async def post_table_full(
        request: Request,
        path: str,
        column: Optional[List[str]] = Body(None, min_length=1),
        format: Optional[str] = None,
        filename: Optional[str] = None,
        settings: Settings = Depends(get_settings),
        principal: Optional[Principal] = Depends(get_current_principal),
        root_tree=Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        _=Security(check_scopes, scopes=["read:data"]),
    ):
        """
        Fetch the data for the given table [POST route].
        """
        entry = await get_entry(
            path,
            ["read:data"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            request.state.metrics,
            {StructureFamily.table},
            getattr(request.app.state, "access_policy", None),
        )
        return await table_full(
            request=request,
            entry=entry,
            column=column,
            format=format,
            filename=filename,
            settings=settings,
        )

    async def table_full(
        request: Request,
        entry: AnyAdapter,
        column: Optional[List[str]],
        format: Optional[str],
        filename: Optional[str],
        settings: Settings,
    ):
        """
        Fetch the data for the given table.
        """
        try:
            with record_timing(request.state.metrics, "read"):
                data = await ensure_awaitable(entry.read, column)
        except KeyError as err:
            (key,) = err.args
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST, detail=f"No such field {key}."
            )
        if data.memory_usage().sum() > settings.response_bytesize_limit:
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail=(
                    f"Response would exceed {settings.response_bytesize_limit}. "
                    "Select a subset of the columns to "
                    "request a smaller chunks."
                ),
            )
        try:
            with record_timing(request.state.metrics, "pack"):
                return await construct_data_response(
                    entry.structure_family,
                    serialization_registry,
                    data,
                    entry.metadata(),
                    request,
                    format,
                    specs=getattr(entry, "specs", []),
                    expires=getattr(entry, "content_stale_at", None),
                    filename=filename,
                    filter_for_access=None,
                )
        except UnsupportedMediaTypes as err:
            raise HTTPException(status_code=HTTP_406_NOT_ACCEPTABLE, detail=err.args[0])

    @router.get(
        "/container/full/{path:path}",
        response_model=schemas.Response,
        name="full 'container' metadata and data",
    )
    async def get_container_full(
        request: Request,
        path: str,
        principal: Optional[Principal] = Depends(get_current_principal),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        field: Optional[List[str]] = Query(None, min_length=1),
        format: Optional[str] = None,
        filename: Optional[str] = None,
        root_tree=Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        _=Security(check_scopes, scopes=["read:data"]),
    ):
        """
        Fetch the data for the given container via a GET request.
        """
        entry = await get_entry(
            path,
            ["read:data"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            request.state.metrics,
            {StructureFamily.container},
            getattr(request.app.state, "access_policy", None),
        )
        return await container_full(
            request=request,
            entry=entry,
            principal=principal,
            authn_access_tags=authn_access_tags,
            authn_scopes=authn_scopes,
            field=field,
            format=format,
            filename=filename,
        )

    @router.post(
        "/container/full/{path:path}",
        response_model=schemas.Response,
        name="full 'container' metadata and data",
    )
    async def post_container_full(
        request: Request,
        path: str,
        principal: Optional[Principal] = Depends(get_current_principal),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        field: Optional[List[str]] = Body(None, min_length=1),
        format: Optional[str] = None,
        filename: Optional[str] = None,
        root_tree=Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        _=Security(check_scopes, scopes=["read:data"]),
    ):
        """
        Fetch the data for the given container via a POST request.
        """
        entry = await get_entry(
            path,
            ["read:data"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            request.state.metrics,
            {StructureFamily.container},
            getattr(request.app.state, "access_policy", None),
        )
        return await container_full(
            request=request,
            entry=entry,
            principal=principal,
            authn_access_tags=authn_access_tags,
            authn_scopes=authn_scopes,
            field=field,
            format=format,
            filename=filename,
        )

    async def container_full(
        request: Request,
        entry,
        principal: Optional[Principal],
        authn_access_tags: Optional[Set[str]],
        authn_scopes: Scopes,
        field: Optional[List[str]],
        format: Optional[str],
        filename: Optional[str],
    ):
        """
        Fetch the data for the given container.
        """
        try:
            with record_timing(request.state.metrics, "read"):
                data = await ensure_awaitable(entry.read, fields=field)
        except KeyError as err:
            (key,) = err.args
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST, detail=f"No such field {key}."
            )
        curried_filter = partial(
            filter_for_access,
            access_policy=request.app.state.access_policy,
            principal=principal,
            authn_access_tags=authn_access_tags,
            authn_scopes=authn_scopes,
            scopes=["read:data"],
            metrics=request.state.metrics,
        )
        # TODO Walk node to determine size before handing off to serializer.
        try:
            with record_timing(request.state.metrics, "pack"):
                return await construct_data_response(
                    entry.structure_family,
                    serialization_registry,
                    data,
                    entry.metadata(),
                    request,
                    format,
                    specs=getattr(entry, "specs", []),
                    expires=getattr(entry, "content_stale_at", None),
                    filename=filename,
                    filter_for_access=curried_filter,
                )
        except UnsupportedMediaTypes as err:
            raise HTTPException(status_code=HTTP_406_NOT_ACCEPTABLE, detail=err.args[0])

    @router.get(
        "/node/full/{path:path}",
        response_model=schemas.Response,
        name="full 'container' or 'table'",
        deprecated=True,
    )
    async def node_full(
        request: Request,
        path: str,
        principal: Optional[Principal] = Depends(get_current_principal),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        field: Optional[List[str]] = Query(None, min_length=1),
        format: Optional[str] = None,
        filename: Optional[str] = None,
        settings: Settings = Depends(get_settings),
        root_tree=Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        _=Security(check_scopes, scopes=["read:data"]),
    ):
        """
        Fetch the data below the given node.
        """
        entry = await get_entry(
            path,
            ["read:data"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            request.state.metrics,
            {StructureFamily.table, StructureFamily.container},
            getattr(request.app.state, "access_policy", None),
        )
        try:
            with record_timing(request.state.metrics, "read"):
                data = await ensure_awaitable(entry.read, field)
        except KeyError as err:
            (key,) = err.args
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST, detail=f"No such field {key}."
            )
        if (entry.structure_family == StructureFamily.table) and (
            data.memory_usage().sum() > settings.response_bytesize_limit
        ):
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail=(
                    f"Response would exceed {settings.response_bytesize_limit}. "
                    "Select a subset of the columns ('?field=...') to "
                    "request a smaller chunks."
                ),
            )
        if entry.structure_family == StructureFamily.container:
            curried_filter = partial(
                filter_for_access,
                access_policy=request.app.state.access_policy,
                principal=principal,
                authn_access_tags=authn_access_tags,
                authn_scopes=authn_scopes,
                scopes=["read:data"],
                metrics=request.state.metrics,
            )
        else:
            curried_filter = None
            # TODO Walk node to determine size before handing off to serializer.
        try:
            with record_timing(request.state.metrics, "pack"):
                return await construct_data_response(
                    entry.structure_family,
                    serialization_registry,
                    data,
                    entry.metadata(),
                    request,
                    format,
                    specs=getattr(entry, "specs", []),
                    expires=getattr(entry, "content_stale_at", None),
                    filename=filename,
                    filter_for_access=curried_filter,
                )
        except UnsupportedMediaTypes as err:
            raise HTTPException(status_code=HTTP_406_NOT_ACCEPTABLE, detail=err.args[0])

    @router.get(
        "/awkward/buffers/{path:path}",
        response_model=schemas.Response,
        name="AwkwardArray buffers",
    )
    async def get_awkward_buffers(
        request: Request,
        path: str,
        form_key: Optional[List[str]] = Query(None, min_length=1),
        format: Optional[str] = None,
        filename: Optional[str] = None,
        settings: Settings = Depends(get_settings),
        principal: Optional[Principal] = Depends(get_current_principal),
        root_tree=Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        _=Security(check_scopes, scopes=["read:data"]),
    ):
        """
        Fetch a slice of AwkwardArray data.

        Note that there is a POST route on this same path with equivalent functionality.
        HTTP caches tends to engage with GET but not POST, so that GET route may be
        preferred for that reason. However, HTTP clients, servers, and proxies
        typically impose a length limit on URLs. (The HTTP spec does not specify
        one, but this is a pragmatic measure.) For requests with large numbers of
        form_key parameters, POST may be the only option.
        """
        entry = await get_entry(
            path,
            ["read:data"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            request.state.metrics,
            {StructureFamily.awkward},
            getattr(request.app.state, "access_policy", None),
        )
        return await _awkward_buffers(
            request=request,
            entry=entry,
            form_key=form_key,
            format=format,
            filename=filename,
            settings=settings,
        )

    @router.post(
        "/awkward/buffers/{path:path}",
        response_model=schemas.Response,
        name="AwkwardArray buffers",
    )
    async def post_awkward_buffers(
        request: Request,
        path: str,
        body: List[str],
        format: Optional[str] = None,
        filename: Optional[str] = None,
        settings: Settings = Depends(get_settings),
        principal: Optional[Principal] = Depends(get_current_principal),
        root_tree=Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        _=Security(check_scopes, scopes=["read:data"]),
    ):
        """
        Fetch a slice of AwkwardArray data.

        Note that there is a GET route on this same path with equivalent functionality.
        HTTP caches tends to engage with GET but not POST, so that GET route may be
        preferred for that reason. However, HTTP clients, servers, and proxies
        typically impose a length limit on URLs. (The HTTP spec does not specify
        one, but this is a pragmatic measure.) For requests with large numbers of
        form_key parameters, POST may be the only option.
        """
        entry = await get_entry(
            path,
            ["read:data"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            request.state.metrics,
            {StructureFamily.awkward},
            getattr(request.app.state, "access_policy", None),
        )
        return await _awkward_buffers(
            request=request,
            entry=entry,
            form_key=body,
            format=format,
            filename=filename,
            settings=settings,
        )

    async def _awkward_buffers(
        request: Request,
        entry,
        form_key: Optional[List[str]],
        format: Optional[str],
        filename: Optional[str],
        settings: Settings,
    ):
        structure_family = entry.structure_family
        structure = entry.structure()
        with record_timing(request.state.metrics, "read"):
            # The plural vs. singular mismatch is due to the way query parameters
            # are given as ?form_key=A&form_key=B&form_key=C.
            container = await ensure_awaitable(entry.read_buffers, form_key)
        if (
            sum(len(buffer) for buffer in container.values())
            > settings.response_bytesize_limit
        ):
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail=(
                    f"Response would exceed {settings.response_bytesize_limit}. "
                    "Use slicing ('?slice=...') to request smaller chunks."
                ),
            )
        components = (structure.form, structure.length, container)
        try:
            with record_timing(request.state.metrics, "pack"):
                return await construct_data_response(
                    structure_family,
                    serialization_registry,
                    components,
                    entry.metadata(),
                    request,
                    format,
                    specs=getattr(entry, "specs", []),
                    expires=getattr(entry, "content_stale_at", None),
                    filename=filename,
                )
        except UnsupportedMediaTypes as err:
            raise HTTPException(status_code=HTTP_406_NOT_ACCEPTABLE, detail=err.args[0])

    @router.get(
        "/awkward/full/{path:path}",
        response_model=schemas.Response,
        name="Full AwkwardArray",
    )
    async def awkward_full(
        request: Request,
        path: str,
        format: Optional[str] = None,
        filename: Optional[str] = None,
        settings: Settings = Depends(get_settings),
        principal: Optional[Principal] = Depends(get_current_principal),
        root_tree=Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        _=Security(check_scopes, scopes=["read:data"]),
    ):
        """
        Fetch a slice of AwkwardArray data.
        """
        entry = await get_entry(
            path,
            ["read:data"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            request.state.metrics,
            {StructureFamily.awkward},
            getattr(request.app.state, "access_policy", None),
        )
        structure_family = entry.structure_family
        # Deferred import because this is not a required dependency of the server
        # for some use cases.
        import awkward

        with record_timing(request.state.metrics, "read"):
            container = await ensure_awaitable(entry.read)
        structure = entry.structure()
        components = (structure.form, structure.length, container)
        array = awkward.from_buffers(*components)
        if array.nbytes > settings.response_bytesize_limit:
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail=(
                    f"Response would exceed {settings.response_bytesize_limit}. "
                    "Use slicing ('?slice=...') to request smaller chunks."
                ),
            )
        try:
            with record_timing(request.state.metrics, "pack"):
                return await construct_data_response(
                    structure_family,
                    serialization_registry,
                    components,
                    entry.metadata(),
                    request,
                    format,
                    specs=getattr(entry, "specs", []),
                    expires=getattr(entry, "content_stale_at", None),
                    filename=filename,
                )
        except UnsupportedMediaTypes as err:
            raise HTTPException(status_code=HTTP_406_NOT_ACCEPTABLE, detail=err.args[0])

    @router.post("/metadata/{path:path}", response_model=schemas.PostMetadataResponse)
    async def post_metadata(
        request: Request,
        path: str,
        body: schemas.PostMetadataRequest,
        settings: Settings = Depends(get_settings),
        principal: Optional[Principal] = Depends(get_current_principal),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        root_tree=Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        _=Security(check_scopes, scopes=["write:metadata", "create"]),
    ):
        entry = await get_entry(
            path,
            ["write:metadata", "create"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            request.state.metrics,
            None,
            getattr(request.app.state, "access_policy", None),
        )
        for data_source in body.data_sources:
            if data_source.assets:
                raise HTTPException(
                    "Externally-managed assets cannot be registered "
                    "using POST /metadata/{path}. Use POST /register/{path} instead."
                )
        if body.data_sources and not getattr(entry, "writable", False):
            raise HTTPException(
                status_code=HTTP_405_METHOD_NOT_ALLOWED,
                detail=f"Data cannot be written at the path {path}",
            )
        return await _create_node(
            request=request,
            path=path,
            body=body,
            settings=settings,
            entry=entry,
            principal=principal,
            authn_access_tags=authn_access_tags,
            authn_scopes=authn_scopes,
        )

    @router.post("/register/{path:path}", response_model=schemas.PostMetadataResponse)
    async def post_register(
        request: Request,
        path: str,
        body: schemas.PostMetadataRequest,
        settings: Settings = Depends(get_settings),
        principal: Optional[Principal] = Depends(get_current_principal),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        root_tree=Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        _=Security(check_scopes, scopes=["write:metadata", "create", "register"]),
    ):
        entry = await get_entry(
            path,
            ["write:metadata", "create", "register"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            request.state.metrics,
            None,
            getattr(request.app.state, "access_policy", None),
        )
        return await _create_node(
            request=request,
            path=path,
            body=body,
            settings=settings,
            entry=entry,
            principal=principal,
            authn_access_tags=authn_access_tags,
            authn_scopes=authn_scopes,
        )

    async def _create_node(
        request: Request,
        path: str,
        body: schemas.PostMetadataRequest,
        settings: Settings,
        entry,
        principal: Optional[Principal],
        authn_access_tags: Optional[Set[str]],
        authn_scopes: Scopes,
    ):
        metadata, structure_family, specs, access_blob = (
            body.metadata,
            body.structure_family,
            body.specs,
            body.access_blob,
        )
        if structure_family == StructureFamily.container:
            structure = None
        else:
            if len(body.data_sources) != 1:
                raise NotImplementedError
            structure = body.data_sources[0].structure

        key = body.id or entry.context.key_maker()
        metadata_modified, metadata = await validate_specs(
            specs=specs,
            metadata=metadata,
            entry=None,  # the node doesn't exist yet
            structure_family=structure_family,
            structure=structure,
            settings=settings,
        )

        if request.app.state.access_policy is not None and hasattr(
            request.app.state.access_policy, "init_node"
        ):
            try:
                (
                    access_blob_modified,
                    access_blob,
                ) = await request.app.state.access_policy.init_node(
                    principal, authn_access_tags, authn_scopes, access_blob=access_blob
                )
            except ValueError as e:
                raise HTTPException(
                    status_code=HTTP_403_FORBIDDEN,
                    detail=f"Access policy rejects the provided access blob.\n{e}",
                )
        else:
            access_blob_modified = access_blob != {}
            access_blob = {}

        node = await entry.create_node(
            metadata=body.metadata,
            structure_family=body.structure_family,
            key=key,
            specs=body.specs,
            data_sources=body.data_sources,
            access_blob=access_blob,
        )
        links = links_for_node(
            structure_family, structure, get_base_url(request), path + f"/{node.key}"
        )
        response_data = {
            "id": node.key,
            "links": links,
            "data_sources": [ds.model_dump() for ds in node.data_sources],
        }
        if metadata_modified:
            response_data["metadata"] = metadata
        if access_blob_modified:
            response_data["access_blob"] = access_blob

        return json_or_msgpack(request, response_data)

    @router.put("/data_source/{path:path}")
    async def put_data_source(
        request: Request,
        path: str,
        body: schemas.PutDataSourceRequest,
        settings: Settings = Depends(get_settings),
        principal: Optional[Principal] = Depends(get_current_principal),
        root_tree=Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        _=Security(check_scopes, scopes=["write:metadata", "register"]),
    ):
        entry = await get_entry(
            path,
            ["write:metadata", "register"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            request.state.metrics,
            None,
            getattr(request.app.state, "access_policy", None),
        )
        await entry.put_data_source(data_source=body.data_source, patch=body.patch)

    @router.delete("/metadata/{path:path}")
    async def delete(
        request: Request,
        path: str,
        recursive: Optional[bool] = Query(
            False, description="Delete children recursively"
        ),
        external_only: Optional[bool] = Query(
            True,
            description=(
                "Delete the node, but only if this would not "
                "affect any internally-managed data sources"
            ),
        ),
        principal: Optional[Principal] = Depends(get_current_principal),
        root_tree=Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        _=Security(check_scopes, scopes=["write:data", "write:metadata"]),
    ):
        entry = await get_entry(
            path,
            ["write:data", "write:metadata"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            request.state.metrics,
            None,
            getattr(request.app.state, "access_policy", None),
        )
        if hasattr(entry, "delete"):
            await entry.delete(recursive=recursive, external_only=external_only)
        else:
            raise HTTPException(
                status_code=HTTP_405_METHOD_NOT_ALLOWED,
                detail="This node does not support deletion.",
            )
        return json_or_msgpack(request, None)

    @router.put("/array/full/{path:path}")
    async def put_array_full(
        request: Request,
        path: str,
        principal: Optional[Principal] = Depends(get_current_principal),
        root_tree=Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        _=Security(check_scopes, scopes=["write:data"]),
    ):
        entry = await get_entry(
            path,
            ["write:data"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            request.state.metrics,
            {StructureFamily.array, StructureFamily.sparse},
            getattr(request.app.state, "access_policy", None),
        )
        body = await request.body()
        if not hasattr(entry, "write"):
            raise HTTPException(
                status_code=HTTP_405_METHOD_NOT_ALLOWED,
                detail="This node cannot accept array data.",
            )
        media_type = request.headers["content-type"]
        if entry.structure_family == "array":
            deserializer = deserialization_registry.dispatch("array", media_type)
        elif entry.structure_family == "sparse":
            deserializer = deserialization_registry.dispatch("sparse", media_type)
        else:
            raise NotImplementedError(entry.structure_family)
        await ensure_awaitable(entry.write, media_type, deserializer, entry, body)
        return json_or_msgpack(request, None)

    @router.put("/array/block/{path:path}")
    async def put_array_block(
        request: Request,
        path: str,
        block=Depends(block),
        principal: Optional[Principal] = Depends(get_current_principal),
        root_tree=Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        _=Security(check_scopes, scopes=["write:data"]),
    ):
        entry = await get_entry(
            path,
            ["write:data"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            request.state.metrics,
            {StructureFamily.array, StructureFamily.sparse},
            getattr(request.app.state, "access_policy", None),
        )
        if not hasattr(entry, "write_block"):
            raise HTTPException(
                status_code=HTTP_405_METHOD_NOT_ALLOWED,
                detail="This node cannot accept array data.",
            )

        body = await request.body()
        media_type = request.headers["content-type"]
        deserializer = deserialization_registry.dispatch(
            entry.structure_family, media_type
        )
        await ensure_awaitable(
            entry.write_block, block, media_type, deserializer, entry, body
        )
        return json_or_msgpack(request, None)

    @router.patch("/array/full/{path:path}")
    async def patch_array_full(
        request: Request,
        path: str,
        offset=Depends(offset_param),
        shape=Depends(shape_param),
        extend: bool = False,
        principal: Optional[Principal] = Depends(get_current_principal),
        root_tree=Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        _=Security(check_scopes, scopes=["write:data"]),
    ):
        entry = await get_entry(
            path,
            ["write:data"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            request.state.metrics,
            {StructureFamily.array},
            getattr(request.app.state, "access_policy", None),
        )
        if not hasattr(entry, "patch"):
            raise HTTPException(
                status_code=HTTP_405_METHOD_NOT_ALLOWED,
                detail="This node cannot accept array data.",
            )

        body = await request.body()
        media_type = request.headers["content-type"]
        deserializer = deserialization_registry.dispatch("array", media_type)
        structure = await ensure_awaitable(
            entry.patch, shape, offset, extend, media_type, deserializer, entry, body
        )
        return json_or_msgpack(request, structure)

    @router.put("/table/full/{path:path}")
    @router.put("/node/full/{path:path}", deprecated=True)
    async def put_node_full(
        request: Request,
        path: str,
        principal: Optional[Principal] = Depends(get_current_principal),
        root_tree=Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        _=Security(check_scopes, scopes=["write:data"]),
    ):
        entry = await get_entry(
            path,
            ["write:data"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            request.state.metrics,
            {StructureFamily.table},
            getattr(request.app.state, "access_policy", None),
        )
        if not hasattr(entry, "write"):
            raise HTTPException(
                status_code=HTTP_405_METHOD_NOT_ALLOWED,
                detail="This node does not support writing.",
            )
        body = await request.body()
        media_type = request.headers["content-type"]
        deserializer = deserialization_registry.dispatch(
            StructureFamily.table, media_type
        )
        data = await ensure_awaitable(deserializer, body)
        await ensure_awaitable(entry.write, data)
        return json_or_msgpack(request, None)

    @router.put("/table/partition/{path:path}")
    async def put_table_partition(
        partition: int,
        path: str,
        request: Request,
        principal: Optional[Principal] = Depends(get_current_principal),
        root_tree=Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        _=Security(check_scopes, scopes=["write:data"]),
    ):
        entry = await get_entry(
            path,
            ["write:data"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            request.state.metrics,
            None,
            getattr(request.app.state, "access_policy", None),
        )
        if not hasattr(entry, "write_partition"):
            raise HTTPException(
                status_code=HTTP_405_METHOD_NOT_ALLOWED,
                detail="This node does not supporting writing a partition.",
            )
        body = await request.body()
        media_type = request.headers["content-type"]
        deserializer = deserialization_registry.dispatch(
            StructureFamily.table, media_type
        )
        data = await ensure_awaitable(deserializer, body)
        await ensure_awaitable(entry.write_partition, data, partition)
        return json_or_msgpack(request, None)

    @router.patch("/table/partition/{path:path}")
    async def patch_table_partition(
        partition: int,
        path: str,
        request: Request,
        principal: Optional[Principal] = Depends(get_current_principal),
        root_tree=Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        _=Security(check_scopes, scopes=["write:data"]),
    ):
        entry = await get_entry(
            path,
            ["write:data"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            request.state.metrics,
            None,
            getattr(request.app.state, "access_policy", None),
        )
        if not hasattr(entry, "write_partition"):
            raise HTTPException(
                status_code=HTTP_405_METHOD_NOT_ALLOWED,
                detail="This node does not supporting writing a partition.",
            )
        body = await request.body()
        media_type = request.headers["content-type"]
        deserializer = deserialization_registry.dispatch(
            StructureFamily.table, media_type
        )
        data = await ensure_awaitable(deserializer, body)
        await ensure_awaitable(entry.append_partition, data, partition)
        return json_or_msgpack(request, None)

    @router.put("/awkward/full/{path:path}")
    async def put_awkward_full(
        request: Request,
        path: str,
        principal: Optional[Principal] = Depends(get_current_principal),
        root_tree=Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        _=Security(check_scopes, scopes=["write:data"]),
    ):
        entry = await get_entry(
            path,
            ["write:data"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            request.state.metrics,
            {StructureFamily.awkward},
            getattr(request.app.state, "access_policy", None),
        )
        body = await request.body()
        if not hasattr(entry, "write"):
            raise HTTPException(
                status_code=HTTP_405_METHOD_NOT_ALLOWED,
                detail="This node cannot be written to.",
            )
        media_type = request.headers["content-type"]
        deserializer = deserialization_registry.dispatch(
            StructureFamily.awkward, media_type
        )
        structure = entry.structure()
        data = await ensure_awaitable(
            deserializer, body, structure.form, structure.length
        )
        await ensure_awaitable(entry.write, data)
        return json_or_msgpack(request, None)

    @router.patch("/metadata/{path:path}", response_model=schemas.PatchMetadataResponse)
    async def patch_metadata(
        request: Request,
        path: str,
        body: schemas.PatchMetadataRequest,
        settings: Settings = Depends(get_settings),
        principal: Optional[Principal] = Depends(get_current_principal),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        drop_revision: bool = False,
        root_tree=Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        _=Security(check_scopes, scopes=["write:metadata"]),
    ):
        entry = await get_entry(
            path,
            ["write:metadata"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            request.state.metrics,
            None,
            getattr(request.app.state, "access_policy", None),
        )
        if not hasattr(entry, "replace_metadata"):
            raise HTTPException(
                status_code=HTTP_405_METHOD_NOT_ALLOWED,
                detail="This node does not support update of metadata.",
            )

        if body.content_type == patch_mimetypes.JSON_PATCH:
            metadata = apply_json_patch(entry.metadata(), (body.metadata or []))
            specs = apply_json_patch((entry.specs or []), (body.specs or []))
            access_blob = apply_json_patch(entry.access_blob, (body.access_blob or []))
        elif body.content_type == patch_mimetypes.MERGE_PATCH:
            metadata = apply_merge_patch(entry.metadata(), (body.metadata or {}))
            # body.specs = [] clears specs, as per json merge patch specification
            # but we treat body.specs = None as "no modifications"
            current_specs = entry.specs or []
            target_specs = current_specs if body.specs is None else body.specs
            specs = apply_merge_patch(current_specs, target_specs)
            # json_merge_patch applies merge in-place, which would
            # otherwise modify the in-memory node and prevent the
            # access policy from sanity checking the access blob.
            # make a copy so we can compare the node against the
            # proposed new access blob.
            entry_access_blob_copy = deepcopy(entry.access_blob)
            access_blob = apply_merge_patch(
                entry_access_blob_copy, (body.access_blob or [])
            )
        else:
            raise HTTPException(
                status_code=HTTP_406_NOT_ACCEPTABLE,
                detail=f"valid content types: {', '.join(patch_mimetypes)}",
            )

        # Manually validate limits that bypass pydantic validation via patch
        if len(specs) > schemas.MAX_ALLOWED_SPECS:
            raise HTTPException(
                status_code=HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Update cannot result in more than {schemas.MAX_ALLOWED_SPECS} specs",
            )
        if len(specs) != len(set(specs)):
            raise HTTPException(
                status_code=HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Update cannot result in non-unique specs",
            )

        metadata_modified, metadata = await validate_specs(
            specs=specs,
            metadata=metadata,
            entry=entry,
            settings=settings,
        )

        if request.app.state.access_policy is not None and hasattr(
            request.app.state.access_policy, "modify_node"
        ):
            try:
                (
                    access_blob_modified,
                    access_blob,
                ) = await request.app.state.access_policy.modify_node(
                    entry, principal, authn_access_tags, authn_scopes, access_blob
                )
            except ValueError as e:
                raise HTTPException(
                    status_code=HTTP_403_FORBIDDEN,
                    detail=f"Access policy rejects the provided access blob.\n{e}",
                )
        else:
            # Cannot modify the access blob if there is no access policy
            access_blob_modified = access_blob != entry.access_blob
            access_blob = entry.access_blob

        await entry.replace_metadata(
            metadata=metadata,
            specs=specs,
            access_blob=access_blob,
            drop_revision=drop_revision,
        )

        response_data = {"id": entry.node.key}
        if metadata_modified:
            response_data["metadata"] = metadata
        if access_blob_modified:
            response_data["access_blob"] = access_blob
        return json_or_msgpack(request, response_data)

    @router.put("/metadata/{path:path}", response_model=schemas.PutMetadataResponse)
    async def put_metadata(
        request: Request,
        path: str,
        body: schemas.PutMetadataRequest,
        settings: Settings = Depends(get_settings),
        principal: Optional[Principal] = Depends(get_current_principal),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        drop_revision: bool = False,
        root_tree=Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        _=Security(check_scopes, scopes=["write:metadata"]),
    ):
        entry = await get_entry(
            path,
            ["write:metadata"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            request.state.metrics,
            None,
            getattr(request.app.state, "access_policy", None),
        )
        if not hasattr(entry, "replace_metadata"):
            raise HTTPException(
                status_code=HTTP_405_METHOD_NOT_ALLOWED,
                detail="This node does not support update of metadata.",
            )

        metadata, specs, access_blob = (
            body.metadata if body.metadata is not None else entry.metadata(),
            body.specs if body.specs is not None else entry.specs,
            body.access_blob if body.access_blob is not None else entry.access_blob,
        )

        metadata_modified, metadata = await validate_specs(
            specs=specs,
            metadata=metadata,
            entry=entry,
            settings=settings,
        )

        if request.app.state.access_policy is not None and hasattr(
            request.app.state.access_policy, "modify_node"
        ):
            try:
                (
                    access_blob_modified,
                    access_blob,
                ) = await request.app.state.access_policy.modify_node(
                    entry, principal, authn_access_tags, authn_scopes, access_blob
                )
            except ValueError as e:
                raise HTTPException(
                    status_code=HTTP_403_FORBIDDEN,
                    detail=f"Access policy rejects the provided access blob.\n{e}",
                )
        else:
            # Cannot modify the access blob if there is no access policy
            access_blob_modified = access_blob != entry.access_blob
            access_blob = entry.access_blob

        await entry.replace_metadata(
            metadata=metadata,
            specs=specs,
            access_blob=access_blob,
            drop_revision=drop_revision,
        )

        response_data = {"id": entry.node.key}
        if metadata_modified:
            response_data["metadata"] = metadata
        if access_blob_modified:
            response_data["access_blob"] = access_blob
        return json_or_msgpack(request, response_data)

    @router.get("/revisions/{path:path}")
    async def get_revisions(
        request: Request,
        path: str,
        offset: Optional[int] = Query(0, alias="page[offset]", ge=0),
        limit: Optional[int] = Query(
            DEFAULT_PAGE_SIZE, alias="page[limit]", ge=0, le=MAX_PAGE_SIZE
        ),
        principal: Optional[Principal] = Depends(get_current_principal),
        root_tree=Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        _=Security(check_scopes, scopes=["read:metadata"]),
    ):
        entry = await get_entry(
            path,
            ["read:metadata"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            request.state.metrics,
            None,
            getattr(request.app.state, "access_policy", None),
        )
        if not hasattr(entry, "revisions"):
            raise HTTPException(
                status_code=HTTP_405_METHOD_NOT_ALLOWED,
                detail="This node does not support revisions.",
            )

        base_url = get_base_url(request)
        resource = await construct_revisions_response(
            entry,
            base_url,
            "/revisions",
            path,
            offset,
            limit,
            resolve_media_type(request),
        )
        return json_or_msgpack(request, resource.model_dump())

    @router.delete("/revisions/{path:path}")
    async def delete_revision(
        request: Request,
        path: str,
        number: int,
        principal: Optional[Principal] = Depends(get_current_principal),
        root_tree=Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        _=Security(check_scopes, scopes=["write:metadata"]),
    ):
        entry = await get_entry(
            path,
            ["write:metadata"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            request.state.metrics,
            None,
            getattr(request.app.state, "access_policy", None),
        )
        if not hasattr(entry, "revisions"):
            raise HTTPException(
                status_code=HTTP_405_METHOD_NOT_ALLOWED,
                detail="This node does not support a del request for revisions.",
            )

        await entry.delete_revision(number)
        return json_or_msgpack(request, None)

    # For simplicity of implementation, we support a restricted subset of the full
    # Range spec. This could be extended if the need arises.
    # https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Range
    RANGE_HEADER_PATTERN = re.compile(r"^bytes=(\d+)-(\d+)$")

    @router.get("/asset/bytes/{path:path}")
    async def get_asset(
        request: Request,
        path: str,
        id: int,
        relative_path: Optional[Path] = None,
        settings: Settings = Depends(get_settings),
        principal: Optional[Principal] = Depends(get_current_principal),
        root_tree=Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        _=Security(check_scopes, scopes=["read:data"]),
    ):
        entry = await get_entry(
            path,
            ["read:data"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            request.state.metrics,
            None,
            getattr(request.app.state, "access_policy", None),
        )  # TODO: Separate scope for assets?
        if not settings.expose_raw_assets:
            raise HTTPException(
                status_code=HTTP_403_FORBIDDEN,
                detail=(
                    "This Tiled server is configured not to allow "
                    "downloading raw assets."
                ),
            )
        if not hasattr(entry, "asset_by_id"):
            raise HTTPException(
                status_code=HTTP_405_METHOD_NOT_ALLOWED,
                detail="This node does not support downloading assets.",
            )

        asset = await entry.asset_by_id(id)
        if asset is None:
            raise HTTPException(
                status_code=HTTP_404_NOT_FOUND,
                detail=f"This node exists but it does not have an Asset with id {id}",
            )
        if asset.is_directory:
            if relative_path is None:
                raise HTTPException(
                    status_code=HTTP_400_BAD_REQUEST,
                    detail=(
                        "This asset is a directory. Must specify relative path, "
                        f"from manifest provided by /asset/manifest/...?id={id}"
                    ),
                )
            if relative_path.is_absolute():
                raise HTTPException(
                    status_code=HTTP_400_BAD_REQUEST,
                    detail="relative_path query parameter must be a *relative* path",
                )
        else:
            if relative_path is not None:
                raise HTTPException(
                    status_code=HTTP_400_BAD_REQUEST,
                    detail="This asset is not a directory. The relative_path query parameter must not be set.",
                )
        if not asset.data_uri.startswith("file:"):
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail="Only download assets stored as file:// is currently supported.",
            )
        path = path_from_uri(asset.data_uri)
        if relative_path is not None:
            # Be doubly sure that this is under the Asset's data_uri directory
            # and not sneakily escaping it.
            if not os.path.commonpath([path, path / relative_path]) != path:
                # This should not be possible.
                raise RuntimeError(
                    f"Refusing to serve {path / relative_path} because it is outside "
                    "of the Asset's directory"
                )
            full_path = path / relative_path
        else:
            full_path = path
        stat_result = await anyio.to_thread.run_sync(os.stat, full_path)
        filename = full_path.name
        if "range" in request.headers:
            range_header = request.headers["range"]
            match = RANGE_HEADER_PATTERN.match(range_header)
            if match is None:
                raise HTTPException(
                    status_code=HTTP_400_BAD_REQUEST,
                    detail=(
                        "Only a Range headers of the form 'bytes=start-end' are supported. "
                        f"Could not parse Range header: {range_header}",
                    ),
                )
            range = start, _ = (int(match.group(1)), int(match.group(2)))
            if start > stat_result.st_size:
                raise HTTPException(
                    status_code=HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE,
                    headers={"content-range": f"bytes */{stat_result.st_size}"},
                )
            status_code = HTTP_206_PARTIAL_CONTENT
        else:
            range = None
            status_code = HTTP_200_OK
        return FileResponseWithRange(
            full_path,
            stat_result=stat_result,
            status_code=status_code,
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
            range=range,
        )

    @router.get("/asset/manifest/{path:path}")
    async def get_asset_manifest(
        request: Request,
        path: str,
        id: int,
        settings: Settings = Depends(get_settings),
        principal: Optional[Principal] = Depends(get_current_principal),
        root_tree=Depends(get_root_tree),
        session_state: dict = Depends(get_session_state),
        authn_access_tags: Optional[Set[str]] = Depends(get_current_access_tags),
        authn_scopes: Scopes = Depends(get_current_scopes),
        _=Security(check_scopes, scopes=["read:data"]),
    ):
        entry = await get_entry(
            path,
            ["read:data"],
            principal,
            authn_access_tags,
            authn_scopes,
            root_tree,
            session_state,
            request.state.metrics,
            None,
            getattr(request.app.state, "access_policy", None),
        )  # TODO: Separate scope for assets?
        if not settings.expose_raw_assets:
            raise HTTPException(
                status_code=HTTP_403_FORBIDDEN,
                detail=(
                    "This Tiled server is configured not to allow "
                    "downloading raw assets."
                ),
            )
        if not hasattr(entry, "asset_by_id"):
            raise HTTPException(
                status_code=HTTP_405_METHOD_NOT_ALLOWED,
                detail="This node does not support downloading assets.",
            )

        asset = await entry.asset_by_id(id)
        if asset is None:
            raise HTTPException(
                status_code=HTTP_404_NOT_FOUND,
                detail=f"This node exists but it does not have an Asset with id {id}",
            )
        if not asset.is_directory:
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail="This asset is not a directory. There is no manifest.",
            )
        if not asset.data_uri.startswith("file:"):
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail="Only download assets stored as file:// is currently supported.",
            )
        path = path_from_uri(asset.data_uri)
        manifest = []
        # Walk the directory and any subdirectories. Aggregate a list of all the
        # files, given as paths relative to the directory root.
        for root, _directories, files in os.walk(path):
            manifest.extend(Path(root, file) for file in files)
        return json_or_msgpack(request, {"manifest": manifest})

    async def validate_specs(
        specs: List[Spec],
        metadata: dict,
        entry: Optional[AnyAdapter] = None,
        structure_family: Optional[StructureFamily] = None,
        structure: Optional[dict] = None,
        settings: Settings = Depends(get_settings),
    ):
        metadata_modified = False

        # Specs should be ordered from most specific/constrained to least.
        # Validate them in reverse order, with the least constrained spec first,
        # because it may do normalization that helps pass the more constrained one.
        # Known Issue:
        # When there is more than one spec, it's possible for the validator for
        # Spec 2 to make a modification that breaks the validation for Spec 1.
        # For now we leave it to the server maintainer to ensure that validators
        # won't step on each other in this way, but this may need revisiting.
        for spec in reversed(specs):
            if spec not in validation_registry:
                if settings.reject_undeclared_specs:
                    raise HTTPException(
                        status_code=HTTP_400_BAD_REQUEST,
                        detail=f"Unrecognized spec: {spec.name}",
                    )
            else:
                validator = validation_registry(spec)
                try:
                    result = await ensure_awaitable(
                        validator, spec, metadata, entry, structure_family, structure
                    )
                except ValidationError as e:
                    raise HTTPException(
                        status_code=HTTP_400_BAD_REQUEST,
                        detail=f"failed validation for the {spec.name} spec:\n{e}",
                    )
                if result is not None:
                    metadata_modified = True
                    metadata = result

        return metadata_modified, metadata

    return router
