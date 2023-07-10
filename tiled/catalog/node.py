import copy
import shutil
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pydantic
from fastapi import HTTPException
from sqlalchemy import delete, func, select, update

from ..server.schemas import (
    DataSource,
    Management,
    NodeAttributes,
    Revision,
    SortingItem,
)
from ..utils import NoteToClient
from . import orm
from .utils import safe_path


class Node(NodeAttributes):
    # In the HTTP response, we place the key *outside* the other attributes,
    # as "id". This was inspired by JSON API, and for now we are sticking
    # with it.
    #
    # But for passing the Node around internally, it is useful to have the
    # key included in the model.
    key: str
    access_policy: Any
    _node: Any = pydantic.PrivateAttr()
    _context: Any = pydantic.PrivateAttr()

    def __init__(self, node, context, **data):
        super().__init__(**data)
        self._node = node
        self._context = context

    @classmethod
    def from_orm(cls, orm, context, *, access_policy, sorting=None):
        sorting = sorting or []
        # In the Python API we encode sorting as (key, direction).
        # This order-based "record" notion does not play well with OpenAPI.
        # In the HTTP API, therefore, we use {"key": key, "direction": direction}.
        if sorting and isinstance(sorting[0], tuple):
            sorting = [SortingItem(key=item[0], direction=item[1]) for item in sorting]
        if len(orm.data_sources) > 1:
            # TODO Handle multiple data sources
            raise NotImplementedError
        if orm.data_sources:
            structure = copy.deepcopy(
                DataSource.from_orm(orm.data_sources[0]).structure
            )
        else:
            structure = None
        return cls(
            key=orm.key,
            ancestors=orm.ancestors,
            # Trailing underscore in 'metadata_' avoids collision with
            # SQLAlchemy reserved word 'metadata'.
            metadata=orm.metadata_,
            structure_family=orm.structure_family,
            structure=structure,
            specs=orm.specs,
            sorting=sorting or [],
            data_sources=[DataSource.from_orm(ds) for ds in orm.data_sources],
            time_created=orm.time_created,
            time_updated=orm.time_updated,
            node=orm,
            context=context,
            access_policy=access_policy,
        )

    def microstructure(self):
        return getattr(self.structure, "micro", None)

    def macrostructure(self):
        return getattr(self.structure, "macro", None)

    async def revisions(self, offset, limit):
        async with self._context.session() as db:
            revision_orms = (
                await db.execute(
                    select(orm.Revision)
                    .where(orm.Revision.node_id == self._node.id)
                    .offset(offset)
                    .limit(limit)
                )
            ).all()
            return [Revision.from_orm(o[0]) for o in revision_orms]

    async def delete(self):
        async with self._context.session() as db:
            is_child = orm.Node.ancestors == self.ancestors + [self.key]
            num_children = (
                await db.execute(select(func.count(orm.Node.key)).where(is_child))
            ).scalar()
            if num_children:
                raise NoteToClient("Cannot delete node that has children")
            for data_source in self.data_sources:
                if data_source.management != Management.external:
                    # TODO Handle case where the same Asset is associated
                    # with multiple DataSources. This is not possible yet
                    # but it is expected to become possible in the future.
                    for asset in data_source.assets:
                        delete_asset(asset)
                        await db.execute(
                            delete(orm.Asset).where(orm.Asset.id == asset.id)
                        )
            result = await db.execute(
                delete(orm.Node).where(orm.Node.id == self._node.id)
            )
            if result.rowcount == 0:
                # TODO Abstract this from FastAPI?
                raise HTTPException(
                    status_code=404,
                    detail=f"No node {self._node.id}",
                )
            assert (
                result.rowcount == 1
            ), f"Deletion would affect {result.rowcount} rows; rolling back"
            await db.commit()

    async def delete_revision(self, number):
        async with self._context.session() as db:
            result = await db.execute(
                delete(orm.Revision)
                .where(orm.Revision.node_id == self._node.id)
                .where(orm.Revision.revision_number == number)
            )
            if result.rowcount == 0:
                # TODO Abstract this from FastAPI?
                raise HTTPException(
                    status_code=404,
                    detail=f"No revision {number} for node {self._node.id}",
                )
            assert (
                result.rowcount == 1
            ), f"Deletion would affect {result.rowcount} rows; rolling back"
            await db.commit()

    async def update_metadata(self, metadata=None, specs=None):
        values = {}
        if metadata is not None:
            # Trailing underscore in 'metadata_' avoids collision with
            # SQLAlchemy reserved word 'metadata'.
            values["metadata_"] = metadata
        if specs is not None:
            values["specs"] = [s.dict() for s in specs]
        async with self._context.session() as db:
            current = (
                await db.execute(select(orm.Node).where(orm.Node.id == self._node.id))
            ).scalar_one()
            next_revision_number = 1 + (
                (
                    await db.execute(
                        select(func.max(orm.Revision.revision_number)).where(
                            orm.Revision.node_id == self._node.id
                        )
                    )
                ).scalar_one()
                or 0
            )
            revision = orm.Revision(
                # Trailing underscore in 'metadata_' avoids collision with
                # SQLAlchemy reserved word 'metadata'.
                metadata_=current.metadata_,
                specs=current.specs,
                node_id=current.id,
                revision_number=next_revision_number,
            )
            db.add(revision)
            await db.execute(
                update(orm.Node).where(orm.Node.id == self._node.id).values(**values)
            )
            await db.commit()


def delete_asset(asset):
    url = urlparse(asset.data_uri)
    if url.scheme == "file":
        path = safe_path(asset.data_uri)
        if asset.is_directory:
            shutil.rmtree(path)
        else:
            Path(path).unlink()
    else:
        raise NotImplementedError(url.scheme)
