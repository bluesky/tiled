# Access-tag inheritance benchmarks

Scripts for reproducing the query slowdown reported on the access-tag
inheritance PR, and for checking fixes against it. They compare the base
commit (before inheritance) with the current checkout on the same PostgreSQL
database.

## Quick start

```sh
performance/postgres.sh start
.pixi/envs/py313/bin/python performance/build_dataset.py --reset
performance/run_ab.sh
```

`run_ab.sh` prints a table like:

```
scenario                               A ms       B ms      B/A  A stmts  B stmts  A rows  B rows
inherit_true.count                      ...        ...      ...x      ...      ...     ...     ...
...
```

A is the base commit and B the current checkout. Plans for every statement
are in `performance/results/<label>/plans/<scenario>.txt`; diff the base and
branch plans of a slow scenario to see what changed.

## Files

| File | Purpose |
|---|---|
| `postgres.sh` | Start/stop a throwaway PostgreSQL 17 container on port 5433 (podman or docker). |
| `build_dataset.py` | Create the catalog schema with tiled, then bulk-insert the test tree with SQL. |
| `bench_sql.py` | Time each scenario and capture `EXPLAIN (ANALYZE, BUFFERS)` for every query it issues. |
| `compare.py` | Side-by-side table of two `bench_sql.py` runs. |
| `run_ab.sh` | Check the base out into a sibling worktree and run both versions, then compare. |

## Dataset

All containers sit under the root. The benchmark user holds the tags
`bench_user` and `grp_0`; children are tagged `grp_<i % 100>`, so 1% of the
tagged children match.

| Container | Its tag | Children | What it tests |
|---|---|---|---|
| `inherit_true/` | `bench_user` | all tagged | Inheritable tags match the user: the branch adds `OR untagged`. |
| `inherit_false/` | `other` | all tagged | Inheritable tags do not match: should be unchanged from base. |
| `half_untagged/` | `bench_user` | even ones untagged | Inheritance makes half the children visible. |
| `deep/` | `bench_user` | 4 levels, mixed | Deep search, including the hidden-ancestor check. |
| `chain/c/c/c/c` | `bench_user` | tagged throughout | A path both versions can walk. |
| `filler/` | `other` | `--filler` nodes | Grows the tables towards production size. |

Sizes are flags on `build_dataset.py` (`--children`, `--deep-fanout`,
`--deep-levels`, `--filler`). The defaults build about 800k nodes in a few
minutes. For production-like statistics, try `--children 1000000
--filler 5000000`.

## Scenarios

- `<container>.count`, `.keys_page`, `.items_page`, `.metadata_search`: a
  filtered listing of one container (`exact_len`, the first page of 100 keys or
  items, and a `Key("color") == "blue"` search).
- `deep.*`: the same through `search_deep()`.
- `walk.*`: resolving a path through the real `get_entry`, which applies the
  access filter at every segment. This shows extra per-request queries rather
  than slower ones; compare the statement counts.

Result counts that differ between the two versions are flagged with `*`.
That is expected for `half_untagged` and `deep`, where inheritance makes
untagged nodes visible. A difference anywhere else is a bug.

## How the access filter is applied

`bench_sql.py` calls the real `filter_for_access` and `get_entry` with a stub
policy granting the user's tags, so each version runs its own request-path
code without needing authentication or tag grants. The stub's
`allowed_scopes` makes the same database call as `TagBasedAccessPolicy`:
`effective_access_tags()` on the branch, and the node's own tags on the base.

## Tips

- Run one group with `--only`, e.g. `performance/run_ab.sh --only inherit_true`.
- `--no-explain` skips plan capture when you only want timings.
- Point at another database with `--uri` on both scripts.
- Rebuild with `--reset` after changing dataset flags. The schema comes from
  the checkout that runs `build_dataset.py`. The branch adds no migrations, so
  one database serves both versions.
