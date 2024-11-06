import json
import importlib
from datetime import datetime
import dateutil
import yaml
import event_model
import os
import re
import sys
import tqdm
from bson import json_util
from databroker.mongo_normalized import MongoAdapter

def import_from_yaml(spec):
    module_name, func_name =  spec.split(':')
    return getattr(importlib.import_module(module_name), func_name)


def unbatch_documents(docs):
    for item in docs:
        if item['name'] == 'event_page':
            for _doc in event_model.unpack_event_page(item['doc']):
                yield {"name": "event", "doc": _doc}
        elif item['name'] == 'datum_page':
            for _doc in event_model.unpack_datum_page(item['doc']):
                yield {"name":"datum", "doc": _doc}
        else:
            yield item


beamline = 'iss'
mongo_user = f"{beamline.lower()}_read"
mongo_pass = os.environ.get("MONGO_PASSWORD", MONGO_PASSWORD)

# Load the beamline-specific patches and handlers
tiled_config_dir = f"{os.getenv('HOME')}/.config/tiled"
sys.path.append(tiled_config_dir)
with open(tiled_config_dir + "/profiles/profiles.yml", 'r') as f:
    profiles = yaml.safe_load(f)
args = profiles[beamline]['direct']['trees'][0]['args']
transforms = args.get('transforms', {})
handlers = args.get('handler_registry', {})
uri = args['uri']
uri = re.sub(r'\$\{(?:MONGO_USER_)\w{3,}\}', mongo_user, uri)
uri = re.sub(r'\$\{(?:MONGO_PASSWORD_)\w{3,}\}', mongo_pass, uri)

print(f"Connecting to {uri}")
ma = MongoAdapter.from_uri(uri, transforms={key:import_from_yaml(val) for key, val in transforms.items()},
      handler_registry={key:import_from_yaml(val) for key, val in handlers.items()})
coll = ma._run_start_collection



# docs = []
# start_doc_cursor = coll.find()
# for i in range(100):
#     bs_run = ma._get_run(start_doc_cursor.next())
#     g = bs_run.documents(fill=False, size=25)
#     g = map(lambda item : {"name":item[0], "doc": (item[1], item[1].pop("_id", None))[0]}, g)
#     g = unbatch_documents(g)
#     docs += list(g)


cur = coll.find(filter={'time':{'$gt':dateutil.parser.parse('2024-10-22 00:00').timestamp(),
'$lt':dateutil.parser.parse('2024-10-23 00:00').timestamp()}}
, projection={'_id':False})
docs = []
for doc0 in cur:
    print(doc0["uid"])
    bs_run = ma._get_run(doc0)
    g = bs_run.documents(fill=False, size=25)
    g = map(lambda item : {"name":item[0], "doc": (item[1], item[1].pop("_id", None))[0]}, g)
    g = unbatch_documents(g)
    docs += list(g)

with open(f'data/docs_{beamline.lower()}.json', 'w') as f:
    f.write(json_util.dumps(docs))
