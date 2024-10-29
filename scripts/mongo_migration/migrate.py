# tiled serve catalog --temp --api-key=secret -w /tmp -r /Users/eugene/code/demo_stream_documents
# tiled serve catalog --temp --api-key=secret -w /tmp -r /nsls2/data

import json
from pathlib import Path
import shutil
from tqdm import tqdm
import traceback

from tiled.client import from_uri
from deepdiff import DeepDiff

from bluesky.callbacks.tiled_writer import TiledWriter
from tiled.client import from_uri

documents = json.loads(Path("data/docs_smi.json").read_text())

client = from_uri("http://localhost:8000", api_key="secret", include_data_sources=True)
tw = TiledWriter(client)

start_doc_uid = None
for item in tqdm(documents[:30]):
    name = item["name"]
    doc = item["doc"]
    if name == "start":
        start_doc_uid = doc['uid']
    try:
        tw(name, doc)
    except Exception as e:
        print(f"Error while processing Bluesky run with {start_doc_uid=}\n{traceback.format_exception(e)}")
