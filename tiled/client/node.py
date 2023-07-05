import warnings

warnings.warn(
    """The module 'tiled.client.node' has been moved to 'tiled.client.container' and
the object 'Node' has been renamed 'Container'.""",
    DeprecationWarning,
)
from .container import *  # noqa

Node = Container  # noqa
