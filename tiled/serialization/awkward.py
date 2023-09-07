import io
import sys

import awkward

if sys.version_info < (3, 9):
    # Python 3.8 has a bug in zipfile that is not easily matched. Import
    # zipfile from a copy of the Python 3.9.17 version, vendored in the tiled
    # source.
    from . import _zipfile_py39 as zipfile
else:
    import zipfile

from ..media_type_registration import deserialization_registry, serialization_registry


@serialization_registry.register("awkward", "application/zip")
def to_zipped_buffers(components, metadata):
    (form, length, container) = components
    file = io.BytesIO()
    # Pack multiple buffers into a zipfile, uncompressed. This enables
    # multiple buffers in a single response, with random access. The
    # entire payload *may* be compressed using Tiled's normal compression
    # mechanisms.
    with zipfile.ZipFile(file, "w", compresslevel=zipfile.ZIP_STORED) as zip:
        for form_key, buffer in container.items():
            zip.writestr(form_key, buffer)
    return file.getbuffer()


@deserialization_registry.register("awkward", "application/zip")
def from_zipped_buffers(buffer, form, length):
    file = io.BytesIO(buffer)
    with zipfile.ZipFile(file, "r") as zip:
        form_keys = zip.namelist()
        container = {}
        for form_key in form_keys:
            container[form_key] = zip.read(form_key)
    return container


@serialization_registry.register("awkward", "application/json")
def to_json(components, metadata):
    (form, length, container) = components
    file = io.StringIO()
    array = awkward.from_buffers(form, length, container)
    awkward.to_json(array, file)
    return file.getvalue()
