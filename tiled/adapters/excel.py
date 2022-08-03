import dask.dataframe
import pandas

from ..adapters.mapping import MapAdapter
from ..server.object_cache import NO_CACHE, get_object_cache, with_object_cache
from .dataframe import DataFrameAdapter


class ExcelAdapter(MapAdapter):
    """
    Read the sheets in an Excel file.

    This maps the Excel file, which may contain one of more spreadsheets,
    onto a tree of tabular structures.

    Examples
    --------

    Given a file path

    >>> ExcelAdapter.from_file("path/to/excel_file.xlsx")

    Given a file object

    >>> file = open("path/to/excel_file.xlsx")
    >>> ExcelAdapter.from_file(file)

    Given a pandas.ExcelFile object

    >>> import pandas
    >>> ef = pandas.ExcelFile(file)
    >>> ExcelAdapter.from_file(ef)
    """

    @classmethod
    def from_file(cls, file, specs=None):

        if isinstance(file, pandas.ExcelFile):
            excel_file = file
        else:
            excel_file = pandas.ExcelFile(file)
        # If an instance has previously been created using the same parameters,
        # then we are here because the caller wants a *fresh* view on this data.
        # Therefore, we should clear any cached data.
        cache = get_object_cache()
        mapping = {}
        for sheet_name in excel_file.sheet_names:
            cache_key = (cls.__module__, cls.__qualname__, file, sheet_name)
            ddf = dask.dataframe.from_pandas(
                with_object_cache(cache_key, excel_file.parse, sheet_name),
                npartitions=1,  # TODO Be smarter about this.
            )
            if cache is not NO_CACHE:
                cache.discard(cache_key)  # parsed sheet content
                cache.discard_dask(ddf.__dask_keys__())  # dask tasks
            mapping[sheet_name] = DataFrameAdapter.from_dask_dataframe(ddf)
        return cls(mapping, specs=specs)
