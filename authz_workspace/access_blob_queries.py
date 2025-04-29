import json
from dataclasses import dataclass
from typing import List

from sqlalchemy import false, func, or_, select
from sqlalchemy.dialects.postgresql import ARRAY, TEXT
from sqlalchemy.sql.expression import cast

from tiled.catalog import orm
from tiled.catalog.adapter import CatalogNodeAdapter
from tiled.queries import JSONSerializable
from tiled.query_registration import register


@register(name="access_blob_filter")
@dataclass
class AccessBlobFilter:
    """
    Perform a query against the access_blob with two conditions.
    1. Query for a value match under a given key, i.e. a username
    2. Query for if any value in a list of values is present in the
       list of values under a given key
    The keys and values for these conditions are independent.

    Parameters
    ----------
    key_id : str
        e.g. "user", "user.id"
    value_id : JSONSerializable
        e.g. "bill", "amanda"
    key_tags : str
        e.g. "tags", "tags.users"
    value_tags : List[JSONSerializable]
        e.g. ["tag_for_bill", "amanda_only"]


    Examples
    --------

    Search for user "bill", as well as tags in ["tag_for_bill", "useful_data"]

    >>> c.search(AccessBlobFilter("user", "bill", "tags", ["tag_for_bill", "useful_data"]))
    """

    key_id: str
    value_id: JSONSerializable
    key_tags: str
    value_tags: List[JSONSerializable]

    def __post_init__(self):
        self.value_tags = list(self.value_tags)

    def encode(self):
        return {
            "key_id": self.key_id,
            "value_id": json.dumps(self.value_id),
            "key_tags": self.key_tags,
            "value_tags": json.dumps(self.value_tags),
        }

    @classmethod
    def decode(cls, *, key_id, value_id, key_tags, value_tags):
        return cls(
            key_id=key_id,
            value_id=json.loads(value_id),
            key_tags=key_tags,
            value_tags=json.loads(value_tags),
        )


def access_blob_filter(query, tree):
    dialect_name = tree.engine.url.get_dialect().name
    key_id = query.key_id.split(".")
    attr_id = orm.Node.access_blob[key_id]
    key_tags = query.key_tags.split(".")
    attr_tags = orm.Node.access_blob[key_tags]
    if len(query.value_id) == 0 and len(query.value_tags) == 0:
        # Results cannot possibly match an empty value or list,
        # so put a False condition in the list ensuring that
        # there are no rows returned.
        condition = false()
    if dialect_name == "sqlite":
        access_blob_json = func.json_each(attr_tags).table_valued("value")
        condition = or_(
            (
                select(1)
                .select_from(access_blob_json)
                .where(access_blob_json.c.value.in_(query.value_tags))
            ).exists(),
            func.json_extract(func.json_quote(attr_id), "$") == query.value_id,
        )
    elif dialect_name == "postgresql":
        condition = or_(
            (attr_tags.op("?|")(cast(query.value_tags, ARRAY(TEXT)))),
            attr_id.astext == query.value_id,
        )
    else:
        raise UnsupportedQueryType("access_blob_filter")
    # import logging
    # logging.basicConfig()
    # logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
    return tree.new_variation(conditions=tree.conditions + [condition])


CatalogNodeAdapter.register_query(AccessBlobFilter, access_blob_filter)
