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

To try out this access control configuration, an example server can be prepped and launched:
```
# prep the access tags and catalog databases
python example_configs/access_tags/compile_tags.py
python example_configs/catalog/create_catalog.py
# launch the example server, which loads these databases
ALICE_PASSWORD=secret1 BOB_PASSWORD=secret2 CARA_PASSWORD=secret3 tiled serve config example_configs/toy_authentication.yml
```
