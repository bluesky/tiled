from pathlib import Path

import tifffile
import numpy

PATHS = [
    ("a.tif",),
    ("b.tif",),
    ("c.tif",),
    ("more", "d.tif",),
    ("even_more", "e.tif",),
    ("even_more", "f.tif",),
]

# TODO Generate more interesting images.
data = numpy.ones((100, 100))


def main(root_path):
    for path in PATHS:
        full_path = Path(root_path, *path)
        full_path.parent.mkdir(parents=True, exist_ok=True)
        tifffile.imsave(str(full_path), data)

if __name__ == "__main__":
    import sys

    main(*sys.argv[1:])