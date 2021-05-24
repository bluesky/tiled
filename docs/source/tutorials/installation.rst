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

To install dependencies more selectively, decide whether you are
publishing data as a ``server``, reading data as a ``client`` or both.

.. code:: bash

   python3 -m pip install 'tiled[client]'         # client only
   python3 -m pip install 'tiled[client,cache]'   # add support for keeping local copies
   python3 -m pip install 'tiled[server]'         # server only
   python3 -m pip install 'tiled[client,server]'  # both

If you may be publishing or reading ``xarray``, pandas ``dataframe``, and numpy
``array`` data, install them all, or a subset. With none of the above you can
still publish and/or read *metadata* but cannot pull any actual data.

.. code:: bash

   python3 -m pip install 'tiled[array,dataframe,xarray]'

Source
------

To install an editable installation for local development:

.. code:: bash

   git clone https://github.com/bluesky/tiled
   cd tiled
   pip install -e '.[complete]'
