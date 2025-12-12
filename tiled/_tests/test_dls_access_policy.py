from pydantic import TypeAdapter

from tiled.access_control.dls import DiamondAccessBlob


def test_blob_deserialised():
    # The access blob stored by the "permissionables" bundle and returned by OPA is in one form
    # but the query in the OPA is in another. We ensure that we can store the access blob lazily
    tag = '{"proposal": 12345, "visit": 1, "beamline": "adsim"}'
    blob = TypeAdapter(DiamondAccessBlob).validate_json(tag)
    assert blob.proposal == 12345
    assert blob.visit == 1
    assert blob.beamline == "adsim"
    assert blob.model_dump() == {"proposal": 12345, "visit": 1, "beamline": "adsim"}
