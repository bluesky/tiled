import copy
from collections.abc import Set
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Tuple, Union
from urllib.parse import quote_plus

import dask.dataframe
import pandas

from tiled.adapters.core import Adapter

from ..catalog.orm import Node
from ..storage import FileStorage, Storage
from ..structures.array import ArrayStructure, StructDtype
from ..structures.core import Spec, StructureFamily
from ..structures.data_source import Asset, DataSource, Management
from ..structures.table import TableStructure
from ..type_aliases import JSON
from ..utils import ensure_uri, path_from_uri
from .array import ArrayAdapter
from .utils import init_adapter_from_catalog


class CSVAdapter(Adapter[TableStructure]):
    """Adapter for tabular data stored as partitioned text (csv) files"""

    structure_family = StructureFamily.table

    def __init__(
        self,
        data_uris: Iterable[str],
        structure: Optional[TableStructure] = None,
        *,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        **kwargs: Optional[Any],
    ) -> None:
        """Adapter for partitioned tabular data stored as a sequence of text (csv) files

        Parameters
        ----------
        data_uris : list of uris to csv files
        structure :
        metadata :
        specs :
        kwargs : dict
            any keyword arguments that can be passed to the pandas.read_csv function, e.g. names, sep, dtype, etc.
        """
        self._file_paths = [path_from_uri(uri) for uri in data_uris]
        self._read_csv_kwargs = kwargs
        if structure is None:
            ddf = dask.dataframe.read_csv(self._file_paths, **self._read_csv_kwargs)
            structure = TableStructure.from_dask_dataframe(ddf)
        super().__init__(structure, metadata=metadata, specs=specs)

    @classmethod
    def supported_storage(cls) -> Set[type[Storage]]:
        return {FileStorage}

    @classmethod
    def from_catalog(
        cls,
        data_source: DataSource[TableStructure],
        node: Node,
        /,
        **kwargs: Optional[Any],
    ) -> "CSVAdapter":
        return init_adapter_from_catalog(cls, data_source, node, **kwargs)

    @classmethod
    def from_uris(
        cls,
        *data_uris: str,
        **kwargs: Optional[Any],
    ) -> "CSVAdapter":
        return cls(data_uris, **kwargs)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._structure.columns!r})"

    @classmethod
    def init_storage(
        cls,
        storage: Storage,
        data_source: DataSource[TableStructure],
        path_parts: List[str],
    ) -> DataSource[TableStructure]:
        """Initialize partitioned CSV storage

        Parameters
        ----------
        data_uri : str
            location of the dataset, should point to a folder in which partitioned csv files will be created
        structure : TableStructure
            description of the data structure

        Returns
        -------
            list of assets with each element corresponding to individual partition files
        """
        data_source = copy.deepcopy(data_source)  # Do not mutate caller input.
        data_uri = storage.uri + "".join(
            f"/{quote_plus(segment)}" for segment in path_parts
        )
        directory = path_from_uri(data_uri)
        directory.mkdir(parents=True, exist_ok=True)
        assets = [
            Asset(
                data_uri=f"{data_uri}/partition-{i}.csv",
                is_directory=False,
                parameter="data_uris",
                num=i,
            )
            for i in range(data_source.structure.npartitions)
        ]
        data_source.assets.extend(assets)
        return data_source

    def append_partition(
        self, partition: int, data: Union[dask.dataframe.DataFrame, pandas.DataFrame]
    ) -> None:
        """Append data to an existing partition

        Parameters
        ----------
        partition : int
            index of the partition to be appended to
        data : dask.dataframe.DataFrame or pandas.DataFrame
            data to be appended
        """

        uri = self._file_paths[partition]
        data.to_csv(uri, index=False, mode="a", header=False)

    def write_partition(
        self, partition: int, data: Union[dask.dataframe.DataFrame, pandas.DataFrame]
    ) -> None:
        """Write data to a new partition or overwrite an existing one

        Parameters
        ----------
        partition : int
            index of the partition to be appended to
        data : dask.dataframe.DataFrame or pandas.DataFrame
            data to be appended
        """

        uri = self._file_paths[partition]
        data.to_csv(uri, index=False)

    def write(self, data: Union[dask.dataframe.DataFrame, pandas.DataFrame]) -> None:
        """Default writing function to a dataset with a single partition

        Parameters
        ----------
        data : dask.dataframe.DataFrame or pandas.DataFrame
            data to be written
        """

        if self.structure().npartitions != 1:
            raise NotImplementedError
        uri = self._file_paths[0]
        data.to_csv(uri, index=False)

    def read(self, fields: Optional[List[str]] = None) -> dask.dataframe.DataFrame:
        """

        Parameters
        ----------
        fields :

        Returns
        -------

        """
        dfs = [
            self.read_partition(i, fields=fields) for i in range(len(self._file_paths))
        ]

        return dask.dataframe.concat(dfs, axis=0)

    def read_partition(
        self,
        indx: int,
        fields: Optional[List[str]] = None,
    ) -> dask.dataframe.DataFrame:
        """Read a single partition

        Parameters
        ----------
        indx : int
            index of the partition to read
        fields :

        Returns
        -------

        """

        df = dask.dataframe.read_csv(self._file_paths[indx], **self._read_csv_kwargs)

        if fields is not None:
            df = df[fields]

        return df.compute()

    def get(self, key: str) -> Union[ArrayAdapter, None]:
        """

        Parameters
        ----------
        key :

        Returns
        -------

        """
        if key not in self.structure().columns:
            return None
        return ArrayAdapter.from_array(self.read([key])[key].values)

    def generate_data_sources(
        self,
        mimetype: str,
        dict_or_none: Callable[[TableStructure], Dict[str, str]],
        item: Union[str, Path],
        is_directory: bool,
    ) -> List[DataSource[TableStructure]]:
        """

        Parameters
        ----------
        mimetype :
        dict_or_none :
        item :
        is_directory :

        Returns
        -------

        """
        return [
            DataSource(
                structure_family=StructureFamily.table,
                mimetype=mimetype,
                structure=self.structure(),
                parameters={},
                management=Management.external,
                assets=[
                    Asset(
                        data_uri=ensure_uri(item),
                        is_directory=is_directory,
                        parameter="data_uris",  # <-- PLURAL!
                        num=0,  # <-- denoting that the Adapter expects a list, and this is the first element
                    )
                ],
            )
        ]

    def __getitem__(self, key: str) -> ArrayAdapter:
        """Get an ArrayAdapter for a single column

        Parameters
        ----------
        key : str
            column name to get

        Returns
        -------
        An array adapter corresponding to a single column in the table.
        """
        # Must compute to determine shape.
        return ArrayAdapter.from_array(self.read([key])[key].values)

    def items(self) -> Iterator[Tuple[str, ArrayAdapter]]:
        """Iterator over table columns

        Returns
        -------
        Tuples of column names and corresponding ArrayAdapters
        """
        yield from (
            (key, ArrayAdapter.from_array(self.read([key])[key].values))
            for key in self._structure.columns
        )


class CSVArrayAdapter(ArrayAdapter):
    """Adapter for array-type data stored as partitioned csv files"""

    @classmethod
    def from_catalog(
        cls,
        data_source: DataSource[ArrayStructure],
        node: Node,
        /,
        **kwargs: Optional[Any],
    ) -> "CSVArrayAdapter":
        """Adapter for partitioned array data stored as a sequence of csv files

        Parameters
        ----------
        data_source :
        node :
        kwargs : dict
            any keyword arguments that can be passed to the pandas.read_csv function, e.g. names, sep, dtype, etc.
        """

        # Load the array lazily with Dask
        file_paths = [path_from_uri(ast.data_uri) for ast in data_source.assets]
        structure = data_source.structure
        nrows = kwargs.pop("nrows", None)  # dask doesn't accept nrows
        kwargs = {"header": None, **kwargs}  # no header for arrays by default
        ddf = dask.dataframe.read_csv(file_paths, **kwargs).rename(columns=str)

        # Ensure columns are in the same order as in the usecols parameter
        if usecols := kwargs.get("usecols"):
            ddf = ddf[usecols]

        chunks_0: tuple[int, ...] = structure.chunks[0]  # rows chunking, if not stacked

        # Read as a structural array if needed; ensure the correct dtype
        if isinstance(structure.data_type, StructDtype):
            array = ddf.to_records(lengths=chunks_0)[list(ddf.columns)].reshape(-1, 1)
        else:
            array = ddf.to_dask_array(lengths=chunks_0)
        array = array.astype(structure.data_type.to_numpy_dtype())

        # Possibly extend or cut the table according the nrows parameter
        if nrows is not None:
            # TODO: this pulls all the data and can take long to compute. Instead, we can open the files and
            #       iterate over the rows directly, which is about 4-5 times faster for 50K rows.
            #       Can also just .compute() and return a np array instead
            nrows_actual = len(ddf)
            if nrows > nrows_actual:
                padding = dask.array.zeros_like(
                    array, shape=(nrows - nrows_actual, *array.shape[1:])
                )
                array = dask.array.append(array[:nrows_actual, ...], padding, axis=0)
            else:
                array = array[:nrows, ...]

            array = array.reshape(structure.shape).rechunk(structure.chunks)

        return cls(
            array,
            structure,
            metadata=node.metadata_,
            specs=node.specs,
        )

    @classmethod
    def from_uris(
        cls,
        *data_uris: str,
        **kwargs: Optional[Any],
    ) -> "CSVArrayAdapter":
        file_paths = [path_from_uri(uri) for uri in data_uris]
        ddf = dask.dataframe.read_csv(file_paths, **{"header": None, **kwargs})
        if usecols := kwargs.get("usecols"):
            ddf = ddf[usecols]  # Ensure the order of columns is preserved
        array = ddf.to_dask_array()
        structure = ArrayStructure.from_array(array)

        return cls(array, structure)
