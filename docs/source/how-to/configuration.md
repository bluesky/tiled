# Serve Trees using Configuration Files

For all but the simplest deployments, a configuration file is needed to spell
out all the options.

## Where?

The path to the configuration file (or directory of multiple configuration files)
should be specified when the server is started, as in:

```
tiled serve config my_config_file.yml
```

or

```
tiled serve config my_config_directory/
```

Alternatively, if not specified in the commandline, the configuration path may
be passed in via the environment.

```
TILED_CONFIG=my_config_file.yml tiled serve config
TILED_CONFIG=my_config_directory/ tiled serve config
```

Finally, if the environment variable is not yet, a defeault location
`config.yml` is set. But the explicitly specifying the configuration location is
recommended for any important use.

```
tiled serve config  # uses config.yml if environment variable TILED_CONFIG is unset
```

For use with containers, this:

```
TILED_CONFIG=my_config_file.yml uvicorn tiled.server.app:app
```

is equivalent to this:

```
tiled serve config my_config_file.yml
```

## Simple examples
The simple deployment

```
tiled serve directory path/to/files
```

is equivalent to

```yaml
trees:
    - path: /
      tree: tiled.trees.files:Tree.from_directory
      args:
          directory: "path/to/files"
```

And the simple deployment

```
tiled serve pyobject tiled.examples.generated_minimal:tree
```

is equivalent to

```yaml
trees:
    - path: /
      tree: tiled.examples.generated_minimal:tree
```

## Less simple examples

### Serve two different directories at different sub-paths

```yaml
trees:
    - path: /a
      tree: tiled.trees.files:Tree.from_directory
      args:
          directory: "path/to/files"
    - path: /b
      tree: tiled.trees.files:Tree.from_directory
      args:
          directory: "path/to/other/files"
```

### Serve two different tree objects are different sub-paths

```yaml
trees:
    - path: /a
      tree: tiled.examples.generated_minimal:tree
    - path: /b
      tree: tiled.examples.generated:demo
```

See {doc}`../explanations/security` and {doc}`../explanations/access-control` for examples addressing
authentication and authorization.

## Reference

See {doc}`../reference/service-configuration` for a comprehensive reference.
