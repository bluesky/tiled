============
Installation
============

This tutorial covers

* Installation using pip
* Installation from source

Standard Installation with pip
------------------------------

First, we strongly recommend creating a fresh software environment using venv,
conda, or similar.

.. code:: bash

   # with venv...
   python3 -m venv try-tiled
   source try-tiled/bin/activate

   # with conda...
   conda create -n try-tiled python pip
   conda activate try-tiled

Install Tiled from PyPI using pip.

.. code:: bash

   python3 -m pip install "tiled[all]"

If you are connecting to an existing to a tiled server as a client, there
is not need to install all the server-related dependencies.

.. code:: bash

   python3 -m pip install "tiled[client]"  # client only

Likewise, if you are deploying a tiled server but not using the client, you can
skip a couple client-related dependencies.

.. code:: bash

   python3 -m pip install "tiled[server]"  # server only

Minimal Installation (fewer dependencies)
-----------------------------------------

To be even more selective about dependencies, you can install `minimal-client`
and/or `minimal-server`. These do not install numpy, pandas, xarray,
and other dependencies related to transporting them between server and client.
This can be useful for a maximally-lean workflow that is only interested in
exploring metadata.

.. code:: bash

   python3 -m pip install "tiled[minimal-client]"
   python3 -m pip install "tiled[minimal-server]"

See lists of dependencies in `pyproject.toml` in the repository root for
details.

Source
------

To install an editable installation for local development:

.. code:: bash

   git clone https://github.com/bluesky/tiled
   cd tiled
   pip install -e ".[all]"

Web UI
------

Tiled includes a web front-end, based in React. A standard pip installation
(i.e. installing from the published wheel) includes the web front-end
pre-built---no further action required.

An installation from source will attempt to build the web front-end if an
`npm` executable is found in the `PATH`. To opt out of this step,
set `TILED_BUILD_SKIP_UI=1`. For details, see `hatch_build.py`, at the root of
the `tiled` source tree.
