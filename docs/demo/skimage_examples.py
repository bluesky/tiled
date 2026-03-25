import skimage

from tiled.client import from_uri

c = from_uri("https://tiled-demo.nsls2.bnl.gov")
if "examples" in c:
    c["examples"].delete(recursive=True, external_only=False)
c.create_container("examples")
c["examples"].create_container("images")
SKIMAGE_EXAMPLES = [
    "astronaut",
    "binary_blobs",
    "brain",
    "brick",
    "camera",
    "cat",
    "cell",
    "cells3d",
    "checkerboard",
    "chelsea",
    "clock",
    "coffee",
    "coins",
    "colorwheel",
    "eagle",
    "grass",
    "gravel",
    "horse",
    "hubble_deep_field",
    "human_mitosis",
    "immunohistochemistry",
    "moon",
    "nickel_solidification",
    "page",
    "protein_transport",
    "retina",
    "rocket",
    "shepp_logan_phantom",
    "vortex",
]
for name in SKIMAGE_EXAMPLES:
    c["examples/images"].write_array(getattr(skimage.data, name)(), key=name)
