from typing import Any, Dict, List, Optional, Union

import dask.dataframe
import pandas

from ..structures.core import Spec
from ..structures.data_source import Asset
from ..structures.table import TableStructure
from ..type_aliases import JSON
from .dataframe import DataFrameAdapter
from .mapping import MapAdapter
from .protocols import AccessPolicy


class ExcelAdapter(MapAdapter):
    """ """

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
    def from_uri(cls, data_uri: str, **kwargs: Any) -> "ExcelAdapter":
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
    def from_assets(
        cls,
        assets: List[Asset],
        structure: TableStructure,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
        **kwargs: Optional[Union[str, List[str], Dict[str, str]]],
    ) -> "ExcelAdapter":
        data_uri = assets[0].data_uri
        return cls.from_uri(
            data_uri,
            structure=structure,
            metadata=metadata,
            specs=specs,
            access_policy=access_policy,
            **kwargs,
        )
