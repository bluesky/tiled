# tiled serve catalog --temp --api-key=secret -w /tmp -r /Users/eugene/code/demo_stream_documents

import json
from pathlib import Path
import shutil
from tqdm import tqdm
import numpy as np

from tiled.client import from_uri
from deepdiff import DeepDiff

from bluesky.callbacks.tiled_writer import TiledWriter
from tiled.client import from_uri

class ValidationException(Exception):

    def __init__(self, message, uid=None):
        super().__init__(message)
        self.uid = uid

class RunValidationException(ValidationException):
    pass

class MetadataValidationException(ValidationException):
    pass

class TableValidationException(ValidationException):
    pass

class ContainerValidationException(ValidationException):
    pass

class DataValidationException(ValidationException):
    pass


def validate(c0, c1, uid=None):
    run0 = c0[uid]
    run1 = c1[uid]

    # Check the Run metadata. Ignore formatting of summary.datetime
    meta0 = dict(run0.metadata)
    meta1 = dict(run1.metadata)
    diff = DeepDiff(meta0, meta1, exclude_paths="root['summary']['datetime']")
    if diff:
        raise MetadataValidationException(diff, uid)

    # Check the data stream names
    stream_names = set(run0.keys())
    if stream_names != set(run1.keys()):
        raise ValidationException("Inconsistent stream names", uid)

    # Check (descriptor) metadata for each stream
    for name in stream_names:
        md0 = dict(run0[name].metadata)
        md1 = dict(run1[name].metadata)
        conf_dict, time_dict = {}, {}
        for desc in md0['descriptors']:
            diff = DeepDiff(desc, md1, exclude_paths=["root['configuration']", "root['time']", "root['uid']"])
            if diff:
                raise MetadataValidationException(diff, uid)
            conf_dict[desc['uid']] = desc.get('configuration')
            time_dict[desc['uid']] = desc.get('time')
        diff = DeepDiff(conf_dict, md1['configuration'])
        if diff:
            raise MetadataValidationException(diff, uid)
        diff = DeepDiff(time_dict, md1['time'])
        if diff:
            raise MetadataValidationException(diff, uid)

    # Check structure for each stream
    for name in stream_names:
        # Check internal data
        external_data_cols = set()
        if ("data" in run0[name].keys()) and ("internal" not in run1[name].keys()):
            raise TableValidationException(f"Missing internal data table in stream {name}", uid)
        else:
            ds1 = run1[name]['internal']
        for old_key, prefix in {"data" : '', "timestams": "ts_"}.items():
            if old_key in run0[name].keys():
                ds0 = run0[name][old_key]
                external_data_cols = external_data_cols.union([prefix+k for k in ds0.keys()]).difference(ds1.columns+['time'])
        
        # Check external data
        if external_data_cols and ("external" not in run1[name].keys()):
            raise ContainerValidationException(f"Missing external data in stream {name}", uid)
        if external_data_cols != set(run1[name]['external'].keys()):
            raise ContainerValidationException(f"Inconsistent external data in stream {name}", uid)
        for key in external_data_cols:
            dat0 = run0[name]['data'][key].read()
            dat1 = run1[name]['external'][key].read()
            if not np.array_equal(dat0, dat1):
                raise DataValidationException(f"External data mismatch for {key=} in stream {name}", uid)

    # Check config for each stream
    for name in stream_names:
        if ("config" in run0[name].keys()) and ("config" not in run1[name].keys()):
            raise TableValidationException(f"Missing config container in stream {name}", uid)
        else:
            config_keys = set(run0[name]['config'].keys())
            if set(config_keys) != set(run1[name]['config'].keys()):
                raise ContainerValidationException(f"Inconsistent config keys in stream {name}", uid)
            
            for ckey in config_keys:
                ds1 = run1[name]['config'][ckey]
                ds0 = run0[name]['config'][ckey]
                missing_conf_cols = set(ds0.keys()).difference(ds1.columns)
                if missing_conf_cols:
                    raise TableValidationException(f"Missing config dataset columns {missing_conf_cols} in {name}/config/{ckey}", uid)

