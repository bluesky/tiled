"""
Example code for importing and exporting xdi using tiled

Source: https://github.com/XraySpectroscopy/XAS-Data-Interchange
"""

import collections
import io
import pathlib
import re
from typing import Any, Optional

import dask.dataframe
import pandas as pd

from tiled.adapters.dataframe import DataFrameAdapter
from tiled.adapters.table import TableAdapter
from tiled.adapters.utils import init_adapter_from_catalog
from tiled.catalog.orm import Node
from tiled.structures.core import Spec, StructureFamily
from tiled.structures.data_source import DataSource
from tiled.structures.table import TableStructure
from tiled.type_aliases import JSON
from tiled.utils import path_from_uri


class XDIAdapter(TableAdapter):
    structure_family = StructureFamily.table

    def __init__(
        self,
        data_uri: str,
        structure: Optional[TableStructure] = None,
        metadata: Optional[JSON] = None,
        specs: Optional[list[Spec]] = None,
        **kwargs: Optional[Any],
    ) -> None:
        """Adapter for XDI data"""

        filepath = path_from_uri(data_uri)
        with open(filepath, "r") as file:
            metadata = {}
            fields = collections.defaultdict(dict)

            # if isinstance(f, pathlib.PosixPath):
            #    line = f.read_text().split('\n')[0]
            # else:
            line = file.readline()
            m = re.match(r"#\s*XDI/(\S*)\s*(\S*)?", line)
            if not m:
                raise ValueError(
                    f"not an XDI file, no XDI versioning information in first line\n{line}"
                )

            metadata["xdi_version"] = m[1]
            metadata["extra_version"] = m[2]

            field_end_re = re.compile(r"#\s*/{3,}")
            header_end_re = re.compile(r"#\s*-{3,}")

            has_comments = False

            # read header
            for line in file:
                if line[0] != "#":
                    raise ValueError(f"reached invalid line in header\n{line}")
                if re.match(field_end_re, line):
                    has_comments = True
                    break
                elif re.match(header_end_re, line):
                    break

                try:
                    key, val = line[1:].strip().split(":", 1)
                    val = val.strip()
                    namespace, tag = key.split(".")
                    # TODO coerce to lower case?
                except ValueError:
                    print(f"error processing line\n{line}")
                    raise

                fields[namespace][tag] = val

            if has_comments:
                comments = ""
                for line in file:
                    if re.match(header_end_re, line):
                        break
                    comments += line

                metadata["comments"] = comments

            metadata["fields"] = fields

            line = file.readline()
            if line[0] != "#":
                raise ValueError(f"expected column labels. got\n{line}")
            col_labels = line[1:].split()

            # TODO validate

            df = pd.read_table(file, sep=r"\s+", names=col_labels)

        ddf = dask.dataframe.from_pandas(df, npartitions=1)
        structure = TableStructure.from_dask_dataframe(ddf)

        super().__init__(
            partitions=ddf.partitions,
            structure=structure,
            metadata=metadata,
            specs=(specs or []) + [Spec("xdi", version="1.0")],
        )

    @classmethod
    def from_catalog(
        cls,
        data_source: DataSource,
        node: Node,
        /,
        **kwargs: Optional[Any],
    ) -> "XDIAdapter":
        return init_adapter_from_catalog(cls, data_source, node, **kwargs)

    @classmethod
    def from_uris(
        cls,
        data_uri: str,
        **kwargs: Optional[Any],
    ) -> "XDIAdapter":
        return cls(data_uri, **kwargs)


def read_xdi(data_uri, structure=None, metadata=None, specs=None, access_policy=None):
    "Read XDI-formatted file."
    filepath = path_from_uri(data_uri)
    with open(filepath, "r") as file:
        metadata = {}
        fields = collections.defaultdict(dict)

        # if isinstance(f, pathlib.PosixPath):
        #    line = f.read_text().split('\n')[0]
        # else:
        line = file.readline()
        m = re.match(r"#\s*XDI/(\S*)\s*(\S*)?", line)
        if not m:
            raise ValueError(
                f"not an XDI file, no XDI versioning information in first line\n{line}"
            )

        metadata["xdi_version"] = m[1]
        metadata["extra_version"] = m[2]

        field_end_re = re.compile(r"#\s*/{3,}")
        header_end_re = re.compile(r"#\s*-{3,}")

        has_comments = False

        # read header
        for line in file:
            if line[0] != "#":
                raise ValueError(f"reached invalid line in header\n{line}")
            if re.match(field_end_re, line):
                has_comments = True
                break
            elif re.match(header_end_re, line):
                break

            try:
                key, val = line[1:].strip().split(":", 1)
                val = val.strip()
                namespace, tag = key.split(".")
                # TODO coerce to lower case?
            except ValueError:
                print(f"error processing line\n{line}")
                raise

            fields[namespace][tag] = val

        if has_comments:
            comments = ""
            for line in file:
                if re.match(header_end_re, line):
                    break
                comments += line

            metadata["comments"] = comments

        metadata["fields"] = fields

        line = file.readline()
        if line[0] != "#":
            raise ValueError(f"expected column labels. got\n{line}")
        col_labels = line[1:].split()

        # TODO validate

        df = pd.read_table(file, sep=r"\s+", names=col_labels)

    return DataFrameAdapter.from_pandas(
        df,
        metadata=metadata,
        specs=(specs or []) + [Spec("xdi", version="1.0")],
    )


def write_xdi(mimetype, df, metadata):
    output = io.StringIO()

    xdi_version = metadata.get("xdi_version")
    extra_version = metadata.get("extra_version")

    output.write(f"# XDI/{xdi_version} {extra_version}\n")

    fields = metadata["fields"]
    for namespace, namespace_dict in fields.items():
        for tag, value in namespace_dict.items():
            output.write(f"# {namespace}.{tag}: {value}\n")

    # write column labels
    columns = list(df.columns)
    output.write("# ")
    output.write(" ".join(columns))
    output.write("\n")

    # write data
    df.to_csv(output, header=False, index=False, sep=" ")
    return output.getvalue()


data = """# XDI/1.0 GSE/1.0
# Column.1: energy eV
# Column.2: i0
# Column.3: itrans
# Column.4: mutrans
# Element.edge: K
# Element.symbol: Cu
# Scan.edge_energy: 8980.0
# Mono.name: Si 111
# Mono.d_spacing: 3.13553
# Beamline.name: 13ID
# Beamline.collimation: none
# Beamline.focusing: yes
# Beamline.harmonic_rejection: rhodium-coated mirror
# Facility.name: APS
# Facility.energy: 7.00 GeV
# Facility.xray_source: APS Undulator A
# Scan.start_time: 2001-06-26T22:27:31
# Detector.I0: 10cm  N2
# Detector.I1: 10cm  N2
# Sample.name: Cu
# Sample.prep: Cu metal foil
# GSE.EXTRA:  config 1
# ///
# Cu foil Room Temperature
# measured at beamline 13-ID
#----
# energy i0 itrans mutrans
  8779.0  149013.7  550643.089065  -1.3070486
  8789.0  144864.7  531876.119084  -1.3006104
  8799.0  132978.7  489591.10592  -1.3033816
  8809.0  125444.7  463051.104096  -1.3059724
  8819.0  121324.7  449969.103983  -1.3107085
  8829.0  119447.7  444386.117562  -1.3138152
  8839.0  119100.7  440176.091039  -1.3072055
  8849.0  117707.7  440448.106567  -1.3195882
  8859.0  117754.7  442302.10637  -1.3233895
  8869.0  117428.7  441944.116528  -1.3253521
  8879.0  117383.7  442810.120466  -1.327693
  8889.0  117185.7  443658.11566  -1.3312944"""


def main():
    pathlib.Path("data").mkdir()
    with open("data/example.xdi", "w") as f:
        f.write(data)


if __name__ == "__main__":
    main()
