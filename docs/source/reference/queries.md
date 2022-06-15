# Queries

## Built in query types

These can be used in searches, as in

```python
c.search(FullText("hello"))
```

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.queries.Comparison
   tiled.queries.Contains
   tiled.queries.Eq
   tiled.queries.NotEq
   tiled.queries.FullText
   tiled.queries.In
   tiled.queries.NotIn
   tiled.queries.Regex
```

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

Notice that, to compound searches, you may use repeated calls to `.search()` as in

```python
c.search(...).search(...).search(...)
```

````{warning}

**You cannot use queries with the Python keywords `not`, `and`, or `or`.**

In Python, `and` and `or` have a particular behavior:

```python
>>> 3 or 5
3

>>> 3 and 5
5
```

which would result in the first or last query being used, respectively,
ignoring all others. This is an unavoidable consequence of Python semantics.
Likewise, `not X` must return `True` or `False`; it cannot return a query.

To avoid confusion, Tiled raises a `TypeError` if you attempt to use
a query with `not`, `and`, or `or`.

````


```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.queries.Key
```
