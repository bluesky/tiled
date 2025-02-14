from typing import Any, Optional

import dask.dataframe
import pandas

from ..catalog.orm import Node
from ..structures.data_source import DataSource
from .dataframe import DataFrameAdapter
from .mapping import MapAdapter


class ExcelAdapter(MapAdapter):
    @classmethod
    def from_file(cls, file: Any, **kwargs: Any) -> "ExcelAdapter":
        """
        Read the sheets in an Excel file.

        This maps the Excel file, which may contain one of more spreadsheets,
        onto a tree of tabular structures.

        Examples
        --------

        Given a file object

        >>> file = open("path/to/excel_file.xlsx")
        >>> ExcelAdapter.from_file(file)

        Given a pandas.ExcelFile object

        >>> import pandas
        >>> filepath = "path/to/excel_file.xlsx"
        >>> ef = pandas.ExcelFile(filepath)
        >>> ExcelAdapter.from_file(ef)

        Parameters
        ----------
        file :
        kwargs :

        Returns
        -------

        """
        if isinstance(file, pandas.ExcelFile):
            excel_file = file
        else:
            excel_file = pandas.ExcelFile(file)
        mapping = {}
        for sheet_name in excel_file.sheet_names:
            ddf = dask.dataframe.from_pandas(
                excel_file.parse(sheet_name),
                npartitions=1,  # TODO Be smarter about this.
            )
            mapping[sheet_name] = DataFrameAdapter.from_dask_dataframe(ddf)
        return cls(mapping, **kwargs)

    @classmethod
    def from_uris(cls, data_uri: str, **kwargs: Any) -> "ExcelAdapter":
        """
        Read the sheets in an Excel file.

        This maps the Excel file, which may contain one of more spreadsheets,
        onto a tree of tabular structures.

        Examples
        --------

        Given a file path

        >>> ExcelAdapter.from_file("path/to/excel_file.xlsx")

        Parameters
        ----------
        data_uri :
        kwargs :

        Returns
        -------

        """
        file = pandas.ExcelFile(data_uri)
        return cls.from_file(file)

    @classmethod
    def from_catalog(
        cls,
        # An Excel file is a container of tables, hence
        # DataSource[None].
        data_source: DataSource[None],
        node: Node,
        /,
        **kwargs: Optional[Any],
    ) -> "ExcelAdapter":
        data_uri = data_source.assets[0].data_uri
        return cls.from_uris(
            data_uri,
            structure=data_source.structure,
            metadata=node.metadata_,
            specs=node.specs,
            **kwargs,
        )
