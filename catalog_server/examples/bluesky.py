"""
Populate MongoDB with example Bluesky documents like:

pip install databroker databroker-pack
python -c "from databroker.tutorial_utils import fetch_BMM_example; fetch_BMM_example()"
databroker-unpack mongo_normalized /home/dallan/.local/share/bluesky_tutorial_data/bluesky-tutorial-BMM bmm_example
"""
from catalog_server.catalogs.bluesky_mongo_normalized import Catalog


catalog = Catalog.from_uri("mongodb://localhost:27017/databroker_bmm_example")
