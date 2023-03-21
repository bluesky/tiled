# This file will be (temporarily) included in the Python sys.path
# when config.yml is loaded by the Tiled server.
import io

from PIL import Image

from tiled.serialization.image_serializer_helpers import img_as_ubyte


def smiley_separated_variables(array, metadata):
    return "\n".join("🙂".join(str(number) for number in row) for row in array)


def to_jpeg(array, metadata):
    file = io.BytesIO()
    # PIL detail: ensure array has compatible data type before handing to PIL.
    prepared_array = img_as_ubyte(array)
    image = Image.fromarray(prepared_array)
    image.save(file, format="jpeg")
    return file.getbuffer()
