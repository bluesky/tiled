The script in `main.sql` is designed to migrate a catalog of Bluesky runs created with TiledWriter of versions
prior to 1.14.5 to the more recent structure. Specifically, the dedicated 'streams' namespace (container) has been
abolished in favor of placing the individual streams containers (e.g. 'primary') directly under the BlueskyRun
root node.

The scripts identifies all nodes named 'streams' that are direct descendant of a node with `BlueskyRun` spec and
that have children of `BlueskyEventStream` specs (or no children at all). The children of the 'streams' node are
moved in the hierarchy one level up and the emptied 'streams' node are deleted.

One can test the script as follows.

1. Start local PG instance with podman.

```sh
podman run --rm --name tiled-test-postgres -p 5432:5432 -e POSTGRES_PASSWORD=secret -d docker.io/postgres:16
```

2. Initialize and empty catalog and populate it with example data.

```sh
./setup.sh
```

3. Run the processing script, grafting the children of 'streams' nodes onto their parents.

```sh
psql postgresql://postgres:secret@localhost:5432/dst -f main.sql
```

Given the original database:

```
catalog=# select id, key, parent,specs from nodes;
 id |   key   | parent |                                           specs
----+---------+--------+--------------------------------------------------------------------------------------------
  0 |         |        | []
  1 | runs    |      0 | [{"name": "CatalogOfBlueskyRuns", "version": "3.0"}]
  2 | run_1   |      1 | [{"name": "BlueskyRun", "version": "3.0"}]
  3 | streams |      2 | []
  4 | primary |      3 | [{"name": "BlueskyEventStream", "version": "3.0"}, {"name": "composite", "version": null}]
  5 | arr1    |      4 | []
  6 | arr2    |      4 | []
(7 rows)

catalog=# select * from nodes_closure;
 ancestor | descendant | depth
----------+------------+-------
        0 |          0 |     0
        1 |          1 |     0
        0 |          1 |     1
        2 |          2 |     0
        1 |          2 |     1
        0 |          2 |     2
        3 |          3 |     0
        2 |          3 |     1
        1 |          3 |     2
        0 |          3 |     3
        4 |          4 |     0
        3 |          4 |     1
        2 |          4 |     2
        1 |          4 |     3
        0 |          4 |     4
        5 |          5 |     0
        4 |          5 |     1
        3 |          5 |     2
        2 |          5 |     3
        1 |          5 |     4
        0 |          5 |     5
        6 |          6 |     0
        4 |          6 |     1
        3 |          6 |     2
        2 |          6 |     3
        1 |          6 |     4
        0 |          6 |     5
(27 rows)
```

The resulting nodes and closure table should look like this:
```
catalog=# select id, key, parent,specs from nodes;
 id |   key   | parent |                                           specs
----+---------+--------+--------------------------------------------------------------------------------------------
  0 |         |        | []
  1 | runs    |      0 | [{"name": "CatalogOfBlueskyRuns", "version": "3.0"}]
  2 | run_1   |      1 | [{"name": "BlueskyRun", "version": "3.0"}]
  5 | arr1    |      4 | []
  6 | arr2    |      4 | []
  4 | primary |      2 | [{"name": "BlueskyEventStream", "version": "3.0"}, {"name": "composite", "version": null}]
(6 rows)

catalog=# select * from nodes_closure;
 ancestor | descendant | depth
----------+------------+-------
        0 |          0 |     0
        1 |          1 |     0
        0 |          1 |     1
        2 |          2 |     0
        1 |          2 |     1
        0 |          2 |     2
        4 |          4 |     0
        5 |          5 |     0
        4 |          5 |     1
        6 |          6 |     0
        4 |          6 |     1
        0 |          5 |     4
        1 |          5 |     3
        2 |          5 |     2
        0 |          4 |     3
        1 |          4 |     2
        2 |          4 |     1
        0 |          6 |     4
        1 |          6 |     3
        2 |          6 |     2
(20 rows)
```

4. Review; try to read and write new data.

```bash
pixi run python review.py
```

5. Clean up.

```sh
./clean.sh
```
