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
   tiled.queries.FullText
   tiled.queries.Regex
```

## Query expressions

The `Key` object can be used to construct queries in a readable way using
standard Python comparison operators, as in

```python
Key("color") == "red"
Key("temperature") > 300
```

used in searches like

```python
c.search(Key("color") == "red").search(Key("temperature") > 300)
```

Notice that, to compound searches, you may use repeated calls to `.search()` as in

```python
c.search(...).search(...).search(...)
```

````{warning}

**You cannot use queries with the Python keywords `and` or `or`.**

In Python, `and` and `or` have a particular behavior:

```python
>>> 3 or 5
3

>>> 3 and 5
5
```

which would result in the first or last query being used, respectively,
ignoring all others. This is an unavoidable consequence of Python semantics.
To avoid confusion, Tiled raises a `TypeError` if you attempt to use
a query with `and` or `or`.

````


```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.queries.Key
```
