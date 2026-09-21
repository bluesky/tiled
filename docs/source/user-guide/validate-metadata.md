(validate-metadata)=
# Specs and Metadata Validation

A *spec* is a label, attached to a node, that declares that the node follows some
convention -- for example, that its metadata contains certain fields. See
{doc}`../explanations/metadata` and {doc}`../explanations/standards` for
background.

The Tiled server can *validate* node's metadata against a spec when the data is written or
its metadata is edited. For this, the server configuration can declare which specs
are accepted and -- optionally -- a validator function that checks (and, if desired,
normalizes) the metadata for each of them.

## Write a validator function

A validator is an ordinary Python function with the following interface:

```py
from typing import Any, Optional

from tiled.adapters.protocols import AnyAdapter
from tiled.structures.core import Spec, StructureFamily
from tiled.validation_registration import ValidationError


def validate_my_spec(
    spec: Spec,
    metadata: dict[str, Any],
    entry: Optional[AnyAdapter],
    structure_family: Optional[StructureFamily],
    structure: Optional[dict[str, Any]],
) -> Optional[dict[str, Any]]:
    ...
```

The function should either:

- **raise** `tiled.validation_registration.ValidationError` to reject the write
  (the server responds with HTTP `400` and the error message), or
- **return** `None` to accept the metadata unchanged, or
- **return** a (possibly modified) metadata `dict` to accept it *and* replace
  it. The returned metadata is what gets stored and is echoed back to the client
  in the response.

The arguments are:

- `spec` -- the `Spec` (`tiled.structures.core.Spec`) being validated.
- `metadata` -- the metadata dict submitted by the client.
- `entry` -- carries the existing node when metadata is being edited, or `None`
  when a new node is being created.
- `structure_family` -- e.g. `"array"`, `"table"`, `"container"`; may be
  `None`.
- `structure` -- the structure description of the new node; may be `None`.

The function may be synchronous (as above) or an `async def`; the server
supports both.

Here is a small example that requires a `sample_id` field and, if it exists,
normalizes it to a string:

```py
# validators.py

from tiled.validation_registration import ValidationError


def validate_my_spec(spec, metadata, entry, structure_family, structure):
    if "sample_id" not in metadata:
        raise ValidationError("The 'my-validated-spec' spec requires a 'sample_id' field.")

    # Optionally normalize and return the modified metadata.
    normalized = dict(metadata)
    normalized["sample_id"] = str(normalized["sample_id"])
    return normalized
```

Notably, Tiled ships one built-in validator, for the `composite` container spec, in
`tiled/validation_registration.py`.

## Register spec validators in the server configuration

Reference the validator from the server configuration file under `specs`,
using an importable `module:function` path:

```yaml
# config.yml
specs:
  - spec: my-validated-spec
    validator: validators:validate_my_spec
  - spec: another-spec  # accepted, but not validated (no validator is specified)

trees:
  - path: /
    tree: catalog
    args:
      uri: ./catalog.db
```

The directory containing the configuration file is placed on the Python path
when the config is loaded, so a `validators.py` sitting next to `config.yml`
is importable as shown. Alternatively, use any importable package, e.g.
`my_package.validators:validate_my_spec`.

The `validator` key is optional. Listing a spec with no validator simply
declares it as accepted. This is useful together with the `reject_undeclared_specs` option:

```yaml
specs:
  ...

# Reject any write whose spec is not listed above.
reject_undeclared_specs: true
```

With `reject_undeclared_specs: true`, a client that tries to write a node
tagged with a spec not present in this list receives an HTTP `400`. By
default (`false`) unknown specs are allowed through unvalidated. Please
see {doc}`../reference/service-configuration` for more detail.

## When validation runs

Validators run when a node is created and when its metadata is replaced or
patched. When a node carries multiple specs they are validated starting from
least specific first (the reverse of the list order), so that a broad spec
can normalize the metadata before a narrower one inspects it. When declaring multiple
specs, please ensure that their validators do not make conflicting modifications;
the server does not currently prevent that.
