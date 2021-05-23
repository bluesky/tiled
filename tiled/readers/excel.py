from ..catalogs.in_memory import Catalog
from .dataframe import DataFrameAdapter

import dask.dataframe
import pandas


class ExcelReader(Catalog):
    """
    Read the sheets in an Excel file.

    This maps the Excel file, which may contain one of more spreadsheets,
    onto a "Catalog" of tabular structures.

    Examples
    --------

    Given a file path

    >>> ExcelReader.from_file("path/to/excel_file.xlsx")

    Given a file object

    >>> file = open("path/to/excel_file.xlsx")
    >>> ExcelReader.from_file(file)

    Given a pandas.ExcelFile object

    >>> import pandas
    >>> ef = pandas.ExcelFile(file)
    >>> ExcelReader.from_file(ef)
    """

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
