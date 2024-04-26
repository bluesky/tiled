import collections.abc
import itertools
import sys
from typing import Any, Iterator, List, Optional

import xarray

from ..structures.core import Spec
from .array import ArrayAdapter
from .mapping import MapAdapter
from .protocols import AccessPolicy


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
        access_policy: Optional[AccessPolicy] = None,
    ) -> "DatasetAdapter":
        """

        Parameters
        ----------
        dataset :
        specs :
        access_policy :

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
            access_policy=access_policy,
        )

    def __init__(
        self,
        mapping: Any,
        *args: Any,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
        **kwargs: Any,
    ) -> None:
        """

        Parameters
        ----------
        mapping :
        args :
        specs :
        access_policy :
        kwargs :
        """
        if isinstance(mapping, xarray.Dataset):
            raise TypeError(
                "Use DatasetAdapter.from_dataset(...), not DatasetAdapter(...)."
            )
        super().__init__(
            mapping, *args, specs=specs, access_policy=access_policy, **kwargs
        )

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


if sys.version_info < (3, 9):
    from typing_extensions import Mapping

    MappingType = Mapping
else:
    import collections

    MappingType = collections.abc.Mapping


class _DatasetMap(MappingType[str, Any]):
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
