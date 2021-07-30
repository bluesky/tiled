============
Installation
============

This tutorial covers

* Installation using pip
* Installation from source

Pip
---

We strongly recommend creating a fresh environment.

.. code:: bash

   python3 -m venv try-tiled
   source try-tiled/bin/activate

Install Tiled from PyPI. If you want to just grab everything you might need
without worrying about keeping dependencies low:

.. code:: bash

   python3 -m pip install 'tiled[complete]'

.. warning::

   We use single quotes in the installation commands, as in:

   .. code:: bash

      python3 -m pip install 'tiled[complete]'

   On MacOS, you MUST include the single quotes.
   On Windows, you MUST NOT include them; you should instead do:

   .. code:: bash

      python3 -m pip install tiled[complete]

   On Linux, you may include them or omit them; it works the same either way.

   (This difference between Mac and Linux is not due to the operating system *per
   se*. It just happens that modern MacOS systems ship with zsh as the default
   shell, whereas Linux typically ships with bash.)

To install dependencies more selectively, decide whether you are
publishing data as a ``server``, reading data as a ``client`` or both.

.. code:: bash

   python3 -m pip install 'tiled[client]'         # client only
   python3 -m pip install 'tiled[server]'         # server only
   python3 -m pip install 'tiled[client,server]'  # both

To be even more selective about dependencies, you can install `minimal-client`
and/or `minimal-server`. These do not install numpy, pandas, xarray,
and other dependencies related to transporting them between server and client.
This can be useful for a maximally-lean deployment that perhaps does not
need all of these.

.. code:: bash

   python3 -m pip install 'tiled[minimal-client]'
   python3 -m pip install 'tiled[minimal-server]'

Source
------

To install an editable installation for local development:

.. code:: bash

   git clone https://github.com/bluesky/tiled
   cd tiled
   pip install -e '.[complete]'
