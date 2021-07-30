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

   python3 -m pip install 'tiled[all]'

If you are connecting to an existing to a tiled server as a client, there
is not need to install all the server-related dependencies.

.. code:: bash

   python3 -m pip install 'tiled[client]'  # client only

Likewise, if you are deploying a tiled server but not using the client, you can
skip a couple client-related dependencies.

.. code:: bash

   python3 -m pip install 'tiled[server]'  # server only

.. warning::

   We use single quotes in the installation commands, as in:

   .. code:: bash

      python3 -m pip install 'tiled[all]'

   On MacOS, you MUST include the single quotes.
   On Windows, you MUST NOT include them; you should instead do:

   .. code:: bash

      python3 -m pip install tiled[all]

   On Linux, you may include them or omit them; it works the same either way.

   (This difference between Mac and Linux is not due to the operating system *per
   se*. It just happens that modern MacOS systems ship with zsh as the default
   shell, whereas Linux typically ships with bash.)

Minimal Installation (fewer dependencies)
-----------------------------------------

To be even more selective about dependencies, you can install `minimal-client`
and/or `minimal-server`. These do not install numpy, pandas, xarray,
and other dependencies related to transporting them between server and client.
This can be useful for a maximally-lean workflow that is only interested in
exploring metadata.

.. code:: bash

   python3 -m pip install 'tiled[minimal-client]'
   python3 -m pip install 'tiled[minimal-server]'

See the files named `requirements-*.txt` in the repository root for details.

Source
------

To install an editable installation for local development:

.. code:: bash

   git clone https://github.com/bluesky/tiled
   cd tiled
   pip install -e '.[all]'
