import os

from bluesky_catalog.mongo_normalized import Catalog


try:
    uri = os.environ["MONGO_URI"]
except KeyError:
    raise Exception("Must set environment variable MONGO_URI to use this module")
catalog = Catalog.from_uri(uri)
