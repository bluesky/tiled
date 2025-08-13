# Queries

## Built in query types

These can be used in searches, as in

```python
c.search(FullText("hello"))
```

Follow the links in the table below for examples specific to each query.

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.queries.Comparison
   tiled.queries.Contains
   tiled.queries.Eq
   tiled.queries.NotEq
   tiled.queries.FullText
   tiled.queries.KeyPresent
   tiled.queries.In
   tiled.queries.Like
   tiled.queries.NotIn
   tiled.queries.Regex
   tiled.queries.SpecQuery
   tiled.queries.SpecsQuery
   tiled.queries.StructureFamilyQuery
```

(Some have the word `Query` at the end of their name to avoid confusion with
other objects in the Tiled codebase.)

## Query expressions

The `Key` object can be used to construct queries in a readable way using
standard Python comparison operators, as in

```python
Key("color") == "red"
Key("shape") != "circle"
Key("temperature") > 300
```

used in searches like

```python
c.search(Key("color") == "red").search(Key("shape") != "circle").search(Key("temperature") > 300)
```

Notice that, to progressively narrow results, you may use repeated calls to
`.search()` as in

```python
c.search(...).search(...).search(...)
```

The above achieves logical `AND`. To achieve logical `OR`, you must perform
separate queries and combine them yourself. It may be convenient to combine them
as shown in a `dict`, as long as the results are not too numerous to fit in
memory.

```python
a = c.search(...)
b = c.search(...)
c = c.search(...)
combined_results = {**a, **b, **c}  # to handle repeats, must use {...} syntax not dict(...)
```

````{warning}

**You cannot use queries with the Python keywords `not`, `in`, `and`, or `or`.**

In Python, `and` and `or` have a particular behavior:

```python
>>> 3 or 5
3

>>> 3 and 5
5
```

which would result in the first or last query being used, respectively,
ignoring all others. This is an unavoidable consequence of Python semantics.
Likewise, `in X` and ``not X` must return `True` or `False`; they cannot return
a query.

To avoid confusion, Tiled raises a `TypeError` if you attempt to use
a query with `not`, `in`, `and`, or `or`.

````


```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.queries.Key
```

## Convenience Functions

We recommend building convenience functions to provide for succinct usage for
common searches. For example:

```py
def Sample(sample_name):
    return Key("sample_name") == sample_name
```

This reduces

```py
c.search(Key("sample_name") == "stuff")
```

to

```py
c.search(Sample("stuff"))
```

##  Custom queries

Not all queries can be expressed as combinations of the built in ones.
External libraries (like databroker) may register custom query types
in addition to those built in to Tiled.
