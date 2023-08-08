import io
import zipfile

from ..media_type_registration import deserialization_registry, serialization_registry


@serialization_registry.register("awkward", "application/zip")
def to_zipped_buffers(container, metadata):
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
def from_zipped_buffers(buffer):
    file = io.BytesIO(buffer)
    with zipfile.ZipFile(file, "r") as zip:
        form_keys = zip.namelist()
        buffers = {}
        for form_key in form_keys:
            buffers[form_key] = zip.read(form_key)
    return buffers
