from typing import Any, Iterator, List, Optional, Set, Tuple, Union

import dask.base
import dask.dataframe
import pandas

from ..storage import Storage
from ..structures.core import Spec, StructureFamily
from ..structures.table import TableStructure
from ..type_aliases import JSON
from .array import ArrayAdapter


class TableAdapter:
    """
    Wrap a dataframe-like object in an interface that Tiled can serve.

    Examples
    --------

    >>> df = pandas.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    >>> DataFrameAdapter.from_pandas(df, npartitions=1)

    """

    structure_family = StructureFamily.table
    supported_storage: Set[type[Storage]] = set()

    def __init__(
        self,
        partitions: Union[dask.dataframe.DataFrame, pandas.DataFrame],
        structure: TableStructure,
        *,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
    ) -> None:
        """

        Parameters
        ----------
        partitions :
        structure :
        metadata :
        specs :
        """
        self._metadata = metadata or {}
        self._partitions = list(partitions)
        self._structure = structure
        self.specs = specs or []

    @classmethod
    def from_pandas(
        cls,
        *args: Any,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        npartitions: int = 1,
        **kwargs: Any,
    ) -> "TableAdapter":
        """

        Parameters
        ----------
        args :
        metadata :
        specs :
        npartitions :
        kwargs :

        Returns
        -------

        """
        ddf = dask.dataframe.from_pandas(*args, npartitions=npartitions, **kwargs)
        if specs is None:
            specs = [Spec("dataframe")]
        return cls.from_dask_dataframe(ddf, metadata=metadata, specs=specs)

    @classmethod
    def from_dict(
        cls,
        *args: Any,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        npartitions: int = 1,
        **kwargs: Any,
    ) -> "TableAdapter":
        """

        Parameters
        ----------
        args :
        metadata :
        specs :
        npartitions :
        kwargs :

        Returns
        -------

        """
        ddf = dask.dataframe.from_dict(*args, npartitions=npartitions, **kwargs)
        if specs is None:
            specs = [Spec("dataframe")]
        return cls.from_dask_dataframe(ddf, metadata=metadata, specs=specs)

    @classmethod
    def from_dask_dataframe(
        cls,
        ddf: dask.dataframe.DataFrame,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
    ) -> "TableAdapter":
        """

        Parameters
        ----------
        ddf :
        metadata :
        specs :

        Returns
        -------

        """
        structure = TableStructure.from_dask_dataframe(ddf)
        if specs is None:
            specs = [Spec("dataframe")]
        return cls(
            ddf.partitions,
            structure,
            metadata=metadata,
            specs=specs,
        )

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._structure.columns!r})"

    def __getitem__(self, key: str) -> ArrayAdapter:
        # Must compute to determine shape
        array = self.read([key])[key].values

        return ArrayAdapter.from_array(array)

    def get(self, key: str) -> Union[ArrayAdapter, None]:
        if key not in self.structure().columns:
            return None
        return self[key]

    def items(self) -> Iterator[Tuple[str, ArrayAdapter]]:
        yield from ((key, self[key]) for key in self._structure.columns)

    def metadata(self) -> JSON:
        return self._metadata

    def structure(self) -> TableStructure:
        return self._structure

    def read(
        self, fields: Optional[List[str]] = None
    ) -> Union[dask.dataframe.DataFrame, pandas.DataFrame]:
        """

        Parameters
        ----------
        fields :

        Returns
        -------

        """
        if any(p is None for p in self._partitions):
            raise ValueError("Not all partitions have been stored.")
        if isinstance(self._partitions[0], dask.dataframe.DataFrame):
            if fields is not None:
                ddf = dask.dataframe.concat(
                    [p[fields] for p in self._partitions], axis=0
                )
            else:
                ddf = dask.dataframe.concat(self._partitions, axis=0)
            return ddf.compute()
        df = pandas.concat(self._partitions, axis=0)
        if fields is not None:
            df = df[fields]
        return df

    def read_partition(
        self,
        partition: int,
        fields: Optional[List[str]] = None,
    ) -> Union[pandas.DataFrame, dask.dataframe.DataFrame]:
        """

        Parameters
        ----------
        partition :
        fields :

        Returns
        -------

        """
        df = self._partitions[partition]
        if df is None:
            raise RuntimeError(f"Partition {partition} has not been stored yet.")
        if fields is not None:
            df = df[fields]
        if isinstance(df, dask.dataframe.DataFrame):
            return df.compute()
        return partition


DataFrameAdapter = TableAdapter
