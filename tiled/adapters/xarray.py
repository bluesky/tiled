import itertools
from collections.abc import Mapping
from typing import Any, Iterator, List, Optional

import xarray

from ..structures.core import Spec
from .array import ArrayAdapter
from .mapping import MapAdapter


class DatasetAdapter(MapAdapter):
    """
    Wrap an xarray.Dataset
    """

    @classmethod
    def from_dataset(
        cls,
        dataset: Any,
        *,
        specs: Optional[List[Spec]] = None,
    ) -> "DatasetAdapter":
        """

        Parameters
        ----------
        dataset :
        specs :

        Returns
        -------

        """
        mapping = _DatasetMap(dataset)
        specs = specs or []
        if "xarray_dataset" not in [spec.name for spec in specs]:
            specs.append(Spec("xarray_dataset"))
        return cls(
            mapping,
            metadata={"attrs": dataset.attrs},
            specs=specs,
        )

    def __init__(
        self,
        mapping: Any,
        *args: Any,
        specs: Optional[List[Spec]] = None,
        **kwargs: Any,
    ) -> None:
        """

        Parameters
        ----------
        mapping :
        args :
        specs :
        kwargs :
        """
        if isinstance(mapping, xarray.Dataset):
            raise TypeError(
                "Use DatasetAdapter.from_dataset(...), not DatasetAdapter(...)."
            )
        super().__init__(mapping, *args, specs=specs, **kwargs)

    def inlined_contents_enabled(self, depth: int) -> bool:
        """

        Parameters
        ----------
        depth :

        Returns
        -------

        """
        # Tell the server to in-line the description of each array
        # (i.e. data_vars and coords) to avoid latency of a second
        # request.
        return True


class _DatasetMap(Mapping[str, Any]):
    def __init__(self, dataset: Any) -> None:
        """

        Parameters
        ----------
        dataset :
        """
        self._dataset = dataset

    def __len__(self) -> int:
        """

        Returns
        -------

        """
        return len(self._dataset.data_vars) + len(self._dataset.coords)

    def __iter__(self) -> Iterator[Any]:
        """

        Returns
        -------

        """
        yield from itertools.chain(self._dataset.data_vars, self._dataset.coords)

    def __getitem__(self, key: str) -> ArrayAdapter:
        """

        Parameters
        ----------
        key :

        Returns
        -------

        """
        data_array = self._dataset[key]
        if key in self._dataset.coords:
            spec = Spec("xarray_coord")
        else:
            spec = Spec("xarray_data_var")
        return ArrayAdapter.from_array(
            data_array.data,
            metadata={"attrs": data_array.attrs},
            dims=data_array.dims,
            specs=[spec],
        )
