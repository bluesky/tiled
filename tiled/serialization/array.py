import base64
import io

import numpy

from ..media_type_registration import deserialization_registry, serialization_registry
from ..utils import (
    SerializationError,
    UnsupportedShape,
    modules_available,
    safe_json_dump,
)


def as_buffer(array, metadata):
    "Give back a zero-copy memoryview of the array if possible. Otherwise, copy to bytes."
    # The memoryview path fails for datetime type (and possibly some others?)
    # but it generally works for standard types like int, float, bool, str.
    try:
        return memoryview(numpy.ascontiguousarray(array))
    except ValueError:
        return numpy.asarray(array).tobytes()


serialization_registry.register(
    "array",
    "application/octet-stream",
    as_buffer,
)
if modules_available("orjson"):
    serialization_registry.register(
        "array",
        "application/json",
        lambda array, metadata: safe_json_dump(array),
    )


def serialize_csv(array, metadata):
    if array.ndim > 2:
        raise UnsupportedShape(array.shape)
    file = io.StringIO()
    numpy.savetxt(file, array, fmt="%s", delimiter=",")
    return file.getvalue().encode()


serialization_registry.register("array", "text/csv", serialize_csv)
serialization_registry.register("array", "text/x-comma-separated-values", serialize_csv)
serialization_registry.register("array", "text/plain", serialize_csv)
deserialization_registry.register(
    "array",
    "application/octet-stream",
    lambda buffer, dtype, shape: numpy.frombuffer(buffer, dtype=dtype).reshape(shape),
)
if modules_available("PIL"):

    def save_to_buffer_PIL(array, format):
        # The logic of which shapes are support is subtle, and we'll leave the details
        # PIL ("beg forgiveness rather than ask permission"). But we can rule out
        # anything above 3 dimensions as definitely not supported.
        if array.ndim > 3:
            raise UnsupportedShape(array.ndim)
        from PIL import Image

        from .image_serializer_helpers import img_as_ubyte

        # Handle too *few* dimensions here, and let PIL raise if there are too
        # *many* because it depends on the shape (RGB, RGBA, etc.)
        array = numpy.atleast_2d(array).astype(numpy.float32)
        # Auto-scale. TODO Use percentile.
        low = numpy.percentile(array.ravel(), 1)
        high = numpy.percentile(array.ravel(), 99)
        scaled_array = numpy.clip((array - low) / (high - low), 0, 1)
        file = io.BytesIO()
        try:
            prepared_array = img_as_ubyte(scaled_array)
            image = Image.fromarray(prepared_array)
            image.save(file, format=format)
        except (TypeError, ValueError):
            raise SerializationError(
                f"Failed to serialize this array as {format}. "
                f"Shape is {array.shape}, dtype is {array.dtype}."
            )

        return file.getbuffer()

    def array_from_buffer_PIL(buffer, format, dtype, shape):
        from PIL import Image

        file = io.BytesIO(buffer)
        image = Image.open(file, format=format)
        return numpy.asarray(image).asdtype(dtype).reshape(shape)

    serialization_registry.register(
        "array", "image/png", lambda array, metadata: save_to_buffer_PIL(array, "png")
    )
    deserialization_registry.register(
        "array",
        "image/png",
        lambda buffer, dtype, shape: array_from_buffer_PIL(buffer, "png", dtype, shape),
    )
if modules_available("tifffile"):

    def array_from_buffer_tifffile(buffer, dtype, shape):
        from tifffile import imread

        return imread(buffer).astype(dtype).reshape(shape)

    def save_to_buffer_tifffile(array, metadata):
        from tifffile import imwrite

        # Handle too *few* dimensions here, and let tifffile raise if there are too
        # *many* because it depends on the shape (RGB, RGBA, etc.)
        normalized_array = numpy.atleast_2d(array)
        # The logic of which shapes are support is subtle, and we'll leave the details
        # tifffile ("beg forgiveness rather than ask permission"). But we can rule out
        # anything above 4 dimensions as definitely not supported.
        if normalized_array.ndim > 4:
            raise UnsupportedShape(array.ndim)
        file = io.BytesIO()
        imwrite(file, normalized_array)
        return file.getbuffer()

    serialization_registry.register("array", "image/tiff", save_to_buffer_tifffile)
    deserialization_registry.register("array", "image/tiff", array_from_buffer_tifffile)


def serialize_html(array, metadata):
    "Try to display as image. Fall back to CSV."
    try:
        png_data = serialization_registry("array", "image/png", array, metadata)
    except Exception:
        csv_data = serialization_registry("array", "text/csv", array, metadata)
        return "<html>" "<body>" f"{csv_data.decode()!s}" "</body>" "</html>"
    else:
        return (
            "<html>"
            "<body>"
            '<img src="data:image/png;base64,'
            f'{base64.b64encode(png_data).decode()!s}"'
            "/>"
            "</body>"
            "</html>"
        )


serialization_registry.register("array", "text/html", serialize_html)
