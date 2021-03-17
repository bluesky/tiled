from dataclasses import dataclass
import importlib
from typing import Any, List

import dask.dataframe.utils
import pandas
import pyarrow

from ..media_type_registration import serialization_registry, deserialization_registry


@dataclass
class DataFrameStructure:
    meta: pandas.DataFrame
    divisions: List[Any]

    @classmethod
    def from_dask_dataframe(cls, ddf):
        # Make an *empty* DataFrame with the same structure as ddf.
        meta = dask.dataframe.utils.make_meta(ddf)
        return cls(meta=meta, divisions=ddf.divisions)


# The MIME type vnd.apache.arrow.file is provisional. See:
# https://lists.apache.org/thread.html/r9b462400a15296576858b52ae22e73f13c3e66f031757b2c9522f247%40%3Cdev.arrow.apache.org%3E  # noqa
# TODO Should we actually use vnd.apache.arrow.stream? I think 'file' is right
# for this use case but I have not read deeply into the details yet.
APACHE_ARROW_FILE_MIME_TYPE = "vnd.apache.arrow.file"
serialization_registry.register(
    "dataframe", APACHE_ARROW_FILE_MIME_TYPE, pyarrow.serialize_pandas
)
deserialization_registry.register(
    "dataframe", APACHE_ARROW_FILE_MIME_TYPE, lambda buffer: pyarrow.deserlialize_pandas
)
if importlib.util.find_spec("openpyxl"):
    # TODO Excel reading and writng seems like a nifty application of this.
    ...
