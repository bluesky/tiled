import dask.dataframe
import pandas

from ..adapters.mapping import MapAdapter
from ..server.object_cache import NO_CACHE, get_object_cache, with_object_cache
from .dataframe import DataFrameAdapter


class ExcelAdapter(MapAdapter):
    @classmethod
    def from_file(cls, filepath, **kwargs):
        """
        Read the sheets in an Excel file.

        This maps the Excel file, which may contain one of more spreadsheets,
        onto a tree of tabular structures.

        Examples
        --------

        >>> ExcelAdapter.from_file("path/to/excel_file.xlsx")
        """
        excel_file = pandas.ExcelFile(filepath)
        # If an instance has previously been created using the same parameters,
        # then we are here because the caller wants a *fresh* view on this data.
        # Therefore, we should clear any cached data.
        cache = get_object_cache()
        mapping = {}
        for sheet_name in excel_file.sheet_names:
            cache_key = (cls.__module__, cls.__qualname__, filepath, sheet_name)
            ddf = dask.dataframe.from_pandas(
                with_object_cache(cache_key, excel_file.parse, sheet_name),
                npartitions=1,  # TODO Be smarter about this.
            )
            if cache is not NO_CACHE:
                cache.discard(cache_key)  # parsed sheet content
                cache.discard_dask(ddf.__dask_keys__())  # dask tasks
            mapping[sheet_name] = DataFrameAdapter.from_dask_dataframe(ddf)
        return cls(mapping, **kwargs)
