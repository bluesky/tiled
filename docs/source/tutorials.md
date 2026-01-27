# Getting Started

## Installation

<!-- prettier-ignore-start -->

::::{tab-set}
:sync-group: install

:::{tab-item} pip
:sync: pip

You will need Python 3.10 or later. You can check your version of Python by
typing into a terminal:

```sh
python3 --version
```

It is recommended that you work in an isolated “virtual environment”, so this
installation will not interfere with any existing Python software:

```sh
python3 -m venv ./venv
source ./venv/bin/activate
```

You can now use `pip` to install the library and its dependencies:

```sh
python3 -m pip install "tiled[all]"
```

:::

:::{tab-item} conda
:sync: conda

Create a conda environment.

```sh
conda create -n try-tiled
conda activate try-tiled
```

Install the package.

```sh
conda install -c conda-forge tiled
```

:::

:::{tab-item} uv
:sync: uv

Create a project.

```sh
uv init
```

Add `tiled` to it.

```sh
uv add "tiled[all]"
```

:::

:::{tab-item} pixi
:sync: pixi

Create a workspace.

```sh
pixi init
```

Add `tiled` to it.

```sh
pixi add tiled
```

:::

::::

<!-- prettier-ignore-end -->

## Tutorials

```{toctree}
:maxdepth: 1
tutorials/navigation
tutorials/slicing
tutorials/export
tutorials/login
tutorials/serving-files
tutorials/search
tutorials/writing
tutorials/streaming
tutorials/simple-server
tutorials/plotly-integration
tutorials/zarr-integration
```
