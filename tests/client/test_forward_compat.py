"""Client-side tolerance for unknown fields in server JSON payloads."""

import logging

from tiled.structures.awkward import AwkwardStructure
from tiled.structures.container import ContainerStructure
from tiled.structures.core import Spec
from tiled.structures.data_source import Asset, DataSource
from tiled.structures.table import TableStructure


def test_asset_from_json_tolerates_unknown_fields(caplog):
    payload = {
        "data_uri": "file:///tmp/x",
        "is_directory": False,
        "parameter": None,
        "num": 0,
        "id": 1,
        "size": 42,
        "future_field": "from a newer server",
    }
    with caplog.at_level(logging.DEBUG, logger="tiled.utils"):
        asset = Asset.from_json(payload)
    assert asset.data_uri == "file:///tmp/x"
    assert asset.size == 42
    assert "future_field" in caplog.text


def test_data_source_from_json_tolerates_unknown_fields():
    payload = {
        "structure_family": "container",
        "structure": None,
        "id": 1,
        "mimetype": "application/x-tiled-container",
        "parameters": {},
        "properties": {},
        "assets": [
            {
                "data_uri": "file:///tmp/x",
                "is_directory": False,
                "parameter": None,
                "size": 7,
                "future_asset_field": "unknown",
            }
        ],
        "management": "writable",
        "future_ds_field": "unknown",
    }
    ds = DataSource.from_json(payload)
    assert ds.mimetype == "application/x-tiled-container"
    assert ds.assets[0].size == 7


def test_spec_from_json_tolerates_unknown_fields():
    spec = Spec.from_json({"name": "foo", "version": "1", "future_field": "unknown"})
    assert spec.name == "foo"
    assert spec.version == "1"


def test_awkward_structure_from_json_tolerates_unknown_fields():
    payload = {"length": 10, "form": {}, "future_field": "unknown"}
    structure = AwkwardStructure.from_json(payload)
    assert structure.length == 10


def test_table_structure_from_json_tolerates_unknown_fields():
    payload = {
        "arrow_schema": "",
        "npartitions": 1,
        "columns": [],
        "resizable": False,
        "future_field": "unknown",
    }
    structure = TableStructure.from_json(payload)
    assert structure.npartitions == 1


def test_container_structure_from_json_tolerates_unknown_fields():
    payload = {"keys": ["a", "b"], "future_field": "unknown"}
    structure = ContainerStructure.from_json(payload)
    assert list(structure.keys) == ["a", "b"]
