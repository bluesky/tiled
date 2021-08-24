from pathlib import Path

import numpy
import pandas
import tifffile

from ..utils import modules_available

TIFF_PATHS = [
    ("a.tif",),
    ("b.tif",),
    ("c.tif",),
    (
        "more",
        "d.tif",
    ),
    (
        "more",
        "even_more",
        "e.tif",
    ),
    (
        "more",
        "even_more",
        "f.tif",
    ),
]

# TODO Generate more interesting images.
data = numpy.ones((100, 100))

df1 = pandas.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
df2 = pandas.DataFrame({"C": [10, 20, 30], "D": [40, 50, 60]})


def generate_files(root_path):
    for path in TIFF_PATHS:
        full_path = Path(root_path, *path)
        full_path.parent.mkdir(parents=True, exist_ok=True)
        tifffile.imsave(str(full_path), data)
    with pandas.ExcelWriter(Path(root_path, "tables.xlsx")) as writer:
        df1.to_excel(writer, sheet_name="Sheet 1", index=False)
        df2.to_excel(writer, sheet_name="Sheet 2", index=False)
    df1.to_csv(Path(root_path, "another_table.csv"))


if __name__ == "__main__":
    import sys

    if not modules_available("tifffile", "openpyxl"):
        print(
            """This example requires tifffile and openpyxl, which can be installed by

pip install tifffile openpyxl
""",
            file=sys.stderr,
        )
    generate_files(*sys.argv[1:])
