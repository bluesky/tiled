import os

import h5py
from tiled.catalogs.hdf5 import Catalog


catalog = Catalog(h5py.File(os.environ["NEXUS_FILE_PATH"], "r"))
