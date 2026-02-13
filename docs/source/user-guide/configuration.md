# Serve Data using Configuration Files

## Do I need a config file?

Simple Tiled servers can be run without a configuration file. Examples:

```
# A read-only public server of existing data
tiled serve directory --public files/
```

```
# A temporary writable catalog
tiled serve catalog --temp
```

```
# A persistent writable catalog
tiled catalog init catalog.db
tiled serve catalog catalog.db --write data/
```

A configuration file is necessary to specify more advanced deployments, such as:

* With multi-user authentication
* Serving multiple differently-configured sources of data at various path
  prefixes like `/raw_data` and `/processed_data`

## Where does the config file go?

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

Finally, if the environment variable is not yet, a default location
`./config.yml` is set.

```
tiled serve config  # uses ./config.yml if environment variable TILED_CONFIG is unset
```

But explicitly specifying the configuration location is recommended, for
clarity.


See {doc}`../explanations/security` and {doc}`../explanations/access-control` for examples addressing
authentication and authorization.

## Examples

See `example_configs/` in the Tiled repository root.

See {doc}`../explanations/security` and {doc}`../explanations/access-control`
for discussion and examples addressing authentication and authorization.

## Reference

See {doc}`../reference/service-configuration` for a comprehensive reference
of the server configuration options.
