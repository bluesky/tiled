# Register content in Tiled
This page details a process for registering files with Tiled. Consider this when starting up with `tiled serve catalog`.

## Background
While Tiled reads files from a file system, it also uses a database to efficiently serve information about the nodes in the tree that it serves. Without a database, tiled must walk the file tree every time it starts up to know about the files that it is serving. 

Tiled has several startup modes, described in [configuration](./configuration) documentation.

### Startup mode: tiled serve directory
 When Tiled is started up with `tiled serve directory <directory name>`, it takes some time at startup to read the filesystem and store information in its database. For smaller implementations of Tiled, this is very convenient. However, if the file system is large, this process can take too long every time Tiled starts up. 

In this mode, tiled also watches the folder for changes, and reindexes the entire tree when it encounters them. Again, this is fine for smaller instances, but can be too slow if the tree is large. Additionally, watching directories for changes can be inconsistent in cases where the directories are network mounted, as with `NFS` mounted file systems.

### Startup mode: tiled serve catalog 
`tiled serve catalog` is the preferred startup mode for larger deployments of Tiled. It is also recommended when the underlying files are being served in network-mounted filesystems. However, in this mode, Tiled does not have any automatic way to know about new data introduced to the file system. Some coding must be done.

## Example
We describe a Tiled registration python function for indexing new files. This code must have access to the same database instance and the same file system that the Tiled service have.

### Read Tiled's configuraiton file
First, let's read the same configuration file that your tiled instance reads:

``` python
config = tiled.config.parse_configs(config_path)
```

### Find the tree in the configuraion file that you want to register the new file with
The configuraition file may specficify multiple trees for tiled to serve. We want to find a particular one to register a file with

``` python
# find the tree in tiled configuration that matches the provided tiled_tree_path
matching_tree = next(
    (tree for tree in config["trees"] if tree["path"] == tiled_config_tree_path), None
)
```

### Create the Tiled catalog adapter
Before we can register the file, we need to create a catalog adapter, using information from desired tree in the configuraiton file.

``` python
 # using thre tree in the configuration, generate a catalog(adapter)
catalog_adapter = from_uri(
    matching_tree["args"]["uri"],
    readable_storage=matching_tree["args"]["readable_storage"],
    adapters_by_mimetype=matching_tree["args"].get("adapters_by_mimetype")
)
```

### Register the new file
Now we can register the file. Note that what we register can be a data file (like a `CSV` or `HDF5` file) or it can be a directory that Tiled knows how to index (like a directory containning a `TIFF` sequence or `ZARR` data set.)



``` python
 # Register with tiled. This writes entries into the database for all of the nodes down to the data node
    await register(
        catalog=catalog_adapter,
        key_from_filename=identity,
        path=file_path,
        prefix=path_prefix,
        overwrite=False)

```
Note that the `prefix` argument defines the level of the `path` to omit. Suppose the the `path` you sent in was `/a/b/c/d`, but you want it to be registered in Tiled with in a tree starting at `c`, then the prefix you send would be `/a/b`.


### Putting it all together
Wrapping this code in a function:

``` python
import asyncio
import logging
import sys

from tiled.catalog.register import identity, register
from tiled.catalog import from_uri
import tiled.config

logger = logging.getLogger(__name__)

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


async def process_file(
    file_path: str,
    tiled_config_tree_path: str = "/",
    config_path: str = "/deploy/config",
    path_prefix: str = "/"
):
    """
    Process a file that already exists and register it with tiled as a catalog. 
    We looks for a match in the tiled config file based on tiled_config_tree_path. This will be
    the tree that we import to. Should work with folders of TIFF sequence as well as single filed like 
    hdf5 or datasets like zarr. But honestly, on tiff sequence is tested.

    Args:
        file_path (str): The path of the file to be processed.
        tiled_config_tree_path (str, optional): The path of the tiled tree configuration. Defaults to "/".
        config_path (str, optional): The path of the configuration file. Defaults to "/deploy/config".
        path_prefix (str, optional): The prefix to be added to the registered path. Defaults to "/".

    Raises:
        AssertionError: If no tiled tree is configured for the provided tree path.
        AssertionError: If the matching tiled tree is not a catalog.

    Returns:
        None
    """
    config = tiled.config.parse_configs(config_path)
    # find the tree in tiled configuration that matches the provided tiled_tree_path
    matching_tree = next(
        (tree for tree in config["trees"] if tree["path"] == tiled_config_tree_path), None
    )
    assert matching_tree, f"No tiled tree configured for tree path {tiled_config_tree_path}"
    assert (
        matching_tree["tree"] == "catalog"
    ), f"Matching tiled tree {tiled_config_tree_path} is not a catalog"

    # using thre tree in the configuration, generate a catalog(adapter)
    catalog_adapter = from_uri(
        matching_tree["args"]["uri"],
        readable_storage=matching_tree["args"]["readable_storage"],
        adapters_by_mimetype=matching_tree["args"].get("adapters_by_mimetype")
    )

    # Register with tiled. This writes entries into the database for all of the nodes down to the data node
    await register(
        catalog=catalog_adapter,
        key_from_filename=identity,
        path=file_path,
        prefix=path_prefix,
        overwrite=False)


if __name__ == "__main__":
    asyncio.run(
        process_file(
            "/tiled_storage/a/b/c",
            path_prefix="/a"
        )
    )


```
