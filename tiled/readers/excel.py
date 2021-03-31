from ..catalogs.in_memory import Catalog
from .dataframe import DataFrameAdapter

import dask.dataframe
import pandas


class ExcelReader(Catalog):
    @classmethod
    def from_file(cls, file):

        if isinstance(file, pandas.ExcelFile):
            excel_file = file
        else:
            excel_file = pandas.ExcelFile(file)
        mapping = {
            sheet_name: DataFrameAdapter(
                dask.dataframe.from_pandas(
                    excel_file.parse(sheet_name),
                    npartitions=1,  # TODO Be smarter about this.
                )
            )
            for sheet_name in excel_file.sheet_names
        }
        return cls(mapping)
