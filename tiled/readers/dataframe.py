import dask.dataframe

from ..containers.dataframe import DataFrameMacroStructure, DataFrameMicroStructure
from ..utils import DictView


class DataFrameReader:
    """
    Wrap a dataframe-like

    Such as:

    - pandas.DataFrame
    - dask.DataFrame
    """

    container = "dataframe"

    def __init__(self, data, metadata=None):
        self._metadata = metadata or {}
        if not isinstance(data, dask.dataframe.DataFrame):
            DEFAULT_CHUNK_SIZE = 100  # wild guess, needs more thought
            data = dask.dataframe.from_pandas(data, chunksize=DEFAULT_CHUNK_SIZE)
        self._data = data

    def __repr__(self):
        return f"{type(self).__name__}({self._data!r})"

    @property
    def metadata(self):
        return DictView(self._metadata)

    def macrostructure(self):
        return DataFrameMacroStructure.from_dask_dataframe(self._data)

    def microstructure(self):
        return DataFrameMicroStructure.from_dask_dataframe(self._data)

    def read(self):
        return self._data

    def read_partition(self, partition, columns=None):
        partition = self._data.partitions[partition]
        if columns is not None:
            # Sub-select columns.
            partition = partition[columns]
        return partition.compute()

    def close(self):
        # Allow the garbage collector to reclaim this memory.
        self._data = None

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()
