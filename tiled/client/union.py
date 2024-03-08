import copy

from .base import STRUCTURE_TYPES, BaseClient
from .utils import MSGPACK_MIME_TYPE, ClientError, client_for_item, handle_error


class UnionClient(BaseClient):
    def __repr__(self):
        return (
            f"<{type(self).__name__} {{"
            + ", ".join(f"'{key}'" for key in self.structure().all_keys)
            + "}>"
        )

    @property
    def parts(self):
        return UnionContents(self)

    def __getitem__(self, key):
        if key not in self.structure().all_keys:
            raise KeyError(key)
        try:
            self_link = self.item["links"]["self"]
            if self_link.endswith("/"):
                self_link = self_link[:-1]
            params = {}
            if self._include_data_sources:
                params["include_data_sources"] = True
            content = handle_error(
                self.context.http_client.get(
                    f"{self_link}/{key}",
                    headers={"Accept": MSGPACK_MIME_TYPE},
                    params=params,
                )
            ).json()
        except ClientError as err:
            if err.response.status_code == 404:
                raise KeyError(key)
            raise
        item = content["data"]
        return client_for_item(
            self.context,
            self.structure_clients,
            item,
            include_data_sources=self._include_data_sources,
        )


class UnionContents:
    def __init__(self, node):
        self.node = node

    def __repr__(self):
        return (
            f"<{type(self).__name__} {{"
            + ", ".join(f"'{item.name}'" for item in self.node.structure().parts)
            + "}>"
        )

    def __getitem__(self, name):
        for index, union_item in enumerate(self.node.structure().parts):
            if union_item.name == name:
                structure_family = union_item.structure_family
                structure_dict = union_item.structure
                break
        else:
            raise KeyError(name)
        item = copy.deepcopy(self.node.item)
        item["attributes"]["structure_family"] = structure_family
        item["attributes"]["structure"] = structure_dict
        item["links"] = item["links"]["parts"][index]
        structure_type = STRUCTURE_TYPES[structure_family]
        structure = structure_type.from_json(structure_dict)
        return client_for_item(
            self.node.context,
            self.node.structure_clients,
            item,
            structure=structure,
            include_data_sources=self.node._include_data_sources,
        )
