"""
Example data suitable for export to XDI

Source: https://github.com/XraySpectroscopy/XAS-Data-Interchange/blob/master/specification/spec.md#example-xdi-file
"""
import pandas
from tiled.trees.in_memory import Tree
from tiled.readers.dataframe import DataFrameAdapter

metadata = {
    "Column.1": "energy eV",
    "Column.2": "i0",
    "Column.3": "itrans",
    "Column.4": "mutrans",
    "Element.edge": "K",
    "Element.symbol": "Cu",
    "Scan.edge_energy": 8980.0,
    "Mono.name": "Si 111",
    "Mono.d_spacing": 3.13553,
    "Beamline.name": "13ID",
    "Beamline.collimation": "none",
    "Beamline.focusing": "yes",
    "Beamline.harmonic_rejection": "rhodium-coated mirror",
    "Facility.name": "APS",
    "Facility.energy": 7.00,  # GeV
    "Facility.xray_source": "APS Undulator A",
    "Scan.start_time": "2001-06-26T22:27:31",
    "Detector.I0": "10cm  N2",
    "Detector.I1": "10cm  N2",
    "Sample.name": "Cu",
    "Sample.prep": "Cu metal foil",
    "GSE.EXTRA": "config 1",
}
df = pandas.DataFrame.from_records(
    {
        "energy": {
            0: 8779.0,
            1: 8789.0,
            2: 8799.0,
            3: 8809.0,
            4: 8819.0,
            5: 8829.0,
            6: 8839.0,
            7: 8849.0,
            8: 8859.0,
            9: 8869.0,
            10: 8879.0,
            11: 8889.0,
        },
        "i0": {
            0: 149013.7,
            1: 144864.7,
            2: 132978.7,
            3: 125444.7,
            4: 121324.7,
            5: 119447.7,
            6: 119100.7,
            7: 117707.7,
            8: 117754.7,
            9: 117428.7,
            10: 117383.7,
            11: 117185.7,
        },
        "itrans": {
            0: 550643.089065,
            1: 531876.119084,
            2: 489591.10592,
            3: 463051.104096,
            4: 449969.103983,
            5: 444386.117562,
            6: 440176.091039,
            7: 440448.106567,
            8: 442302.10637,
            9: 441944.116528,
            10: 442810.120466,
            11: 443658.11566,
        },
        "mutrans": {
            0: -1.3070486,
            1: -1.3006104,
            2: -1.3033816,
            3: -1.3059724,
            4: -1.3107085,
            5: -1.3138152,
            6: -1.3072055,
            7: -1.3195882,
            8: -1.3233895,
            9: -1.3253521,
            10: -1.327693,
            11: -1.3312944,
        },
    }
)

tree = Tree(
    {"example": DataFrameAdapter.from_pandas(df, metadata=metadata, npartitions=1)}
)
