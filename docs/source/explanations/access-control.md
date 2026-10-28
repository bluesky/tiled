# Access Control

Tiled uses a programmable Access Control Policy to manage which
users can access and perform actions on (read, write, delete) which entries.

For use background on how to think about access control generally, we
recommend the talk
[Why Authorization Is Hard](https://www.youtube.com/watch?v=2BN96ON48U8),
(Sam Scott, PyCon 2022).

A key point is that if a user cannot access something, it should be as if it
does not exist. In a user interface built on Tiled, whether graphical or
terminal-based, we don't want to even _show_ the user buttons or options that
they are not allowed to use.

In support of this, a Tiled Access Control Policy answers two questions:

1. Given one specific dataset, what is the user allowed to do with it (read,
   write, etc.)?
2. Given a hierarchical dataset, which of the "children" is the user allowed to
   {read, read and write, ...}?

The first one lets us efficiently ask which "buttons" to show for a given data
set. The second one lets us efficiently ask which items to show (or hide...) in
a list of contents.

Rephrasing these two items now using the jargon of entities in Tiled:

1. Given a Principal (user or service) and a Node, return a list of the scopes
   (actions) the Principal is allowed to perform on that Node.

2. Given a Principal (user or service), a Node, and a list of scopes (actions),
   return a list of Query objects that, when applied to the Node, filters its
   children such that the Principal can do all of those actions on the remaining
   children (if any).

This determination can be backed by a call to an external service or by a
static configuration file. We demonstrate the static file case here.

Consider this simple tree of data:

```
/
├── A  -> array=(10 * numpy.ones((10, 10))), access_tags=["data_A"]
├── B  -> array=(10 * numpy.ones((10, 10))), access_tags=["data_B"]
├── C  -> array=(10 * numpy.ones((10, 10))), access_tags=["data_C"]
└── D  -> array=(10 * numpy.ones((10, 10))), access_tags=["data_D"]
```

which will be protected by Tiled's "Tag Based" Access Control Policy. Note the
"access tags" associated with each node. The tag-based access policy uses ACLs,
which are compiled from provided access-tag definitions (example below), to make
decisions based on these access tags.

```{eval-rst}
.. literalinclude:: ../../../example_configs/access_tags/tag_definitions.yml
   :caption: example_configs/access_tags/tag_definitions.yml
```

Under `tags`, usernames and groupnames are mapped to either a role or list of scopes.
Roles are pre-defined lists of scopes, and are also defined in this file. This mapping
confers these scopes to these users for data which is tagged with the corresponding tagname.

Tags can also inherit the ACLs of other tags, using the `auto_tags` field. There is also a
`public` tag which is a special tag used to mark data as public (all users can read).

Lastly, only "owners" of a tag can apply that tag to a node. Tag owners are defined in
this same tag definitions file, under the `tag_owners` key.

## Inherited access tags

A node does not need access tags of its own. A node with no tags is governed by
the tags of its nearest ancestor that has tags. If no ancestor has tags, it is
governed by the catalog's `top_level_access_tags`. This lets you control access
to a whole subtree by tagging only the container at its top. To change who can
see the subtree later, retag that one container; you don't have to touch every
node inside it.

(This is separate from `auto_tags`, described above, which lets one *tag*
include the ACLs of another tag. Inheritance passes tags from a *node* to its
descendants.)

Consider this tree:

```
/
└── experiment   -> access_tags=["data_A"]
    ├── scan_1   -> access_tags=[]          (inherits ["data_A"])
    │   └── image -> access_tags=[]         (inherits ["data_A"])
    └── shared   -> access_tags=["data_B"]
        └── image -> access_tags=[]         (inherits ["data_B"])
```

- Anyone `data_A` grants access to can see and use `scan_1` and `scan_1/image`,
  with the same scopes `data_A` gives them on `experiment`.
- `shared/image` inherits from its *nearest* tagged ancestor, `shared`. So it
  is governed by `data_B`, not `data_A`. A node with its own tags never
  inherits anything.
- The usual rule still applies: a user can only reach a node if they can see
  each container on the way to it. A user with `data_B` but not `data_A` cannot
  see `experiment`, so they cannot reach `shared/image` either, even though
  `data_B` would grant access to it. The same applies to deep search.

Inheritance applies everywhere access is checked: listing a container's
contents, search, deep search, and working out which scopes a user has on a
node.

### New nodes

By default, a node created without access tags is stored without tags, so it
inherits from its parent. To keep the older behavior, in which such a node is
tagged with its creator's user tag (e.g. `user:alice`) and is private to them,
set `tag_new_nodes_with_owner` in the policy's arguments:

```yaml
access_control:
  access_policy: "tiled.access_control.access_policies:TagBasedAccessPolicy"
  args:
    provider: "pam"
    access_tags_parser: "tiled.access_control.access_tags:AccessTagsParser"
    tag_new_nodes_with_owner: true
```

Only Tiled admins can remove every tag from an existing node, so only they can
make a tagged node go back to inheriting.

Graph entities created through the GraphQL interface do not inherit tags. When
`tag_new_nodes_with_owner` is off, a graph entity created without tags is
visible only to admins until someone tags it.

The compiler writes the compiled tag definitions into the catalog database itself,
where the tiled server reads them. Compile the tags before writing tagged data:
tags must be defined before they can be applied to nodes.

To try out this access control configuration, an example server can be prepped and launched:
```
# compile the access tags into the catalog database, then populate the catalog
python example_configs/access_tags/compile_tags.py
python example_configs/catalog/create_catalog.py
# launch the example server, which loads this database
ALICE_PASSWORD=secret1 BOB_PASSWORD=secret2 CARA_PASSWORD=secret3 tiled serve config example_configs/toy_authentication.yml
```
