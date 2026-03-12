"""
This example demonstrates serving a structured NeXus file with Tiled.
By default, the file is generated locally with the `simulated_nexus`
function, which mimics the structure of a real Nexus file found online
at EXAMPLE_URL but fills it with random data.
This is useful for tests or demos when external files are not available.

Use this examples like:

tiled serve pyobject --public tiled.examples.nexus:tree

To serve a different URL from the example hard-coded here, use the config:

```
# config.yml
authentication:
    allow_anonymous_access: true
trees:
    - path: /
      tree: tiled.examples.nexus:build_tree
      args:
          url: YOUR_URL_HERE
```

tiled serve config config.yml
"""

import io

import h5py
import httpx
import numpy as np

from tiled.adapters.hdf5 import HDF5Adapter

EXAMPLE_URL = "https://github.com/nexusformat/exampledata/blob/master/APS/EPICSareaDetector/hdf5/AgBehenate_228.hdf5?raw=true"  # noqa

rng = np.random.default_rng(42)


def rand_bytes(length):
    """Generate random fixed-length byte string."""
    letters = np.frombuffer(
        rng.choice(list(b"ABCDEFGHIJKLMNOPQRSTUVWXYZ"), length).tobytes(),
        dtype=f"|S{length}",
    )
    return letters


def scalar_dataset(group, name, dtype):
    """Create a dataset with shape (1,) and given dtype."""
    dtype = np.dtype(dtype)  # Ensure dtype is a numpy dtype object
    if dtype.kind in {"i", "u"}:
        data = rng.integers(0, 10, size=(1,), dtype=dtype)
    elif dtype.kind in "f":
        data = rng.random(size=(1,)).astype(dtype)
    elif dtype.kind == "S":
        data = np.array([rand_bytes(dtype.itemsize)], dtype=dtype)
    else:
        raise ValueError(dtype)

    group.create_dataset(name, data=data)


def simulated_nexus(file_handle):
    """Create a simulated Nexus file structure using the given file handle

    This example mimics the structure of the Nexus file available online
    at EXAMPLE_URL. It creates groups and datasets with the same names and dtypes,
    but fills the datasets with random data.
    """
    entry = file_handle.create_group("entry")

    # root datasets
    scalar_dataset(entry, "AD_template_ID", np.dtype("|S58"))
    scalar_dataset(entry, "definition", np.dtype("|S5"))
    scalar_dataset(entry, "end_time", np.dtype("|S1"))
    scalar_dataset(entry, "program_name", np.dtype("|S18"))
    scalar_dataset(entry, "run_cycle", np.dtype("|S8"))
    scalar_dataset(entry, "start_time", np.dtype("|S1"))
    scalar_dataset(entry, "title", np.dtype("|S23"))

    # control
    control = entry.create_group("control")
    scalar_dataset(control, "integral", np.float64)
    scalar_dataset(control, "mode", np.dtype("|S5"))
    scalar_dataset(control, "preset", np.float64)

    # data
    data_grp = entry.create_group("data")
    data_grp.create_dataset(
        "data",
        data=rng.integers(0, 1000, size=(195, 487), dtype=np.int32),
    )
    scalar_dataset(data_grp, "description", np.dtype("|S8"))
    scalar_dataset(data_grp, "local_name", np.dtype("|S12"))
    scalar_dataset(data_grp, "make", np.dtype("|S8"))
    scalar_dataset(data_grp, "model", np.dtype("|S8"))

    # instrument
    inst = entry.create_group("instrument")
    meta = inst.create_group("15ID-D metadata")
    meta_fields = [
        ("APS_run_cycle", "|S8"),
        ("EmptyFileName", "|S13"),
        ("EndTime", "|S1"),
        ("GUPNumber", "|S9"),
        ("GuardslitHap", np.int8),
        ("GuardslitHpos", np.int8),
        ("GuardslitVap", np.int8),
        ("GuardslitVpos", np.int8),
        ("I000_cts", np.float64),
        ("I00_V", np.float64),
        ("I00_cts", np.float64),
        ("I00_gain", np.float64),
        ("I0_V", np.float64),
        ("I0_cts", np.float64),
        ("I0_cts_gated", np.float64),
        ("I0_gain", np.float64),
        ("PIN_Y", np.float64),
        ("PIN_Z", np.float64),
        ("PresetTime", np.float64),
        ("SDD", np.float64),
        ("SRcurrent", np.float64),
        ("SampleTitle", "|S23"),
        ("ScanMacro", "|S1"),
        ("StartTime", "|S1"),
        ("USAXSslitHap", np.float64),
        ("USAXSslitHpos", np.float64),
        ("USAXSslitVap", np.float64),
        ("USAXSslitVpos", np.float64),
        ("UserName", "|S14"),
        ("ccdProtection", np.int16),
        ("filterAl", np.float64),
        ("filterGlass", np.float64),
        ("filterTi", np.float64),
        ("idE", np.int8),
        ("m2rp", np.int8),
        ("monoE", np.float64),
        ("mr_enc", np.float64),
        ("msrp", np.int8),
        ("mx", np.float64),
        ("my", np.float64),
        ("pin_ccd_center_x", np.float64),
        ("pin_ccd_center_x_pixel", np.float64),
        ("pin_ccd_center_y", np.float64),
        ("pin_ccd_center_y_pixel", np.float64),
        ("pin_ccd_pixel_size_x", np.float64),
        ("pin_ccd_pixel_size_y", np.float64),
        ("sa", np.float64),
        ("scaler_freq", np.float64),
        ("sthick", np.float64),
        ("sx", np.float64),
        ("sy", np.float64),
        ("wavelength", np.float64),
        ("wavelength_spread", np.int8),
    ]

    for name, dtype in meta_fields:
        scalar_dataset(meta, name, np.dtype(dtype))

    # aperture
    aperture = inst.create_group("aperture")
    scalar_dataset(aperture, "description", np.dtype("|S9"))
    scalar_dataset(aperture, "hcenter", np.float64)
    scalar_dataset(aperture, "hsize", np.float64)
    scalar_dataset(aperture, "vcenter", np.float64)
    scalar_dataset(aperture, "vsize", np.float64)

    # collimator
    collimator = inst.create_group("collimator")
    scalar_dataset(collimator, "absorbing_material", np.dtype("|S8"))

    geometry = collimator.create_group("geometry")
    shape = geometry.create_group("shape")
    scalar_dataset(shape, "shape", np.dtype("|S5"))
    scalar_dataset(shape, "size", np.dtype("|S19"))
    scalar_dataset(shape, "xcenter", np.float64)
    scalar_dataset(shape, "xsize", np.float64)
    scalar_dataset(shape, "ycenter", np.float64)
    scalar_dataset(shape, "ysize", np.float64)

    # detector
    detector = inst.create_group("detector")
    scalar_dataset(detector, "beam_center_x", np.float64)
    scalar_dataset(detector, "beam_center_y", np.float64)
    scalar_dataset(detector, "distance", np.float64)
    scalar_dataset(detector, "x_pixel_size", np.float64)
    scalar_dataset(detector, "y_pixel_size", np.float64)

    # monochromator
    mono = inst.create_group("monochromator")
    scalar_dataset(mono, "energy", np.float64)
    scalar_dataset(mono, "wavelength", np.float64)
    scalar_dataset(mono, "wavelength_spread", np.int8)

    scalar_dataset(inst, "name", np.dtype("|S5"))

    # source
    source = inst.create_group("source")
    scalar_dataset(source, "facility_beamline", np.dtype("|S4"))
    scalar_dataset(source, "facility_name", np.dtype("|S3"))
    scalar_dataset(source, "facility_sector", np.dtype("|S15"))
    scalar_dataset(source, "facility_station", np.dtype("|S1"))
    scalar_dataset(source, "name", np.dtype("|S49"))
    scalar_dataset(source, "probe", np.dtype("|S5"))
    scalar_dataset(source, "type", np.dtype("|S24"))

    # link_rules
    link_rules = entry.create_group("link_rules")
    scalar_dataset(link_rules, "link", np.dtype("|S40"))

    # sample
    sample = entry.create_group("sample")
    scalar_dataset(sample, "aequatorial_angle", np.float64)
    scalar_dataset(sample, "name", np.dtype("|S23"))
    scalar_dataset(sample, "thickness", np.float64)

    # user1
    user1 = entry.create_group("user1")
    scalar_dataset(user1, "name", np.dtype("|S14"))
    scalar_dataset(user1, "proposal_number", np.dtype("|S9"))

    return file_handle


def build_tree(url=None):
    if not url:
        # Create an in-memory Nexus file with the same structure as the example file.
        buffer = io.BytesIO()
        with h5py.File(buffer, "w") as f:
            simulated_nexus(f)
        buffer.seek(0)  # Reset buffer position to the beginning
        file_handle = h5py.File(buffer, "r")
    else:
        # Download a Nexus file into a memory buffer.
        buffer = io.BytesIO(httpx.get(url, follow_redirects=True).content)
        # Access the buffer with h5py, which can treat it like a "file".
        file_handle = h5py.File(buffer, "r")

    # Wrap the h5py.File in a MapAdapter to serve it with Tiled.
    return HDF5Adapter(file_handle)


tree = build_tree()
