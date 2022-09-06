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
   tiled.queries.In
   tiled.queries.NotIn
   tiled.queries.Regex
```

## Query expressions

```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.queries.Key
```

The `Key` object can be used to construct queries in a readable way using
standard Python comparison operators, as in

```python
Key("color") == "red"
Key("shape") != "circle"
Key("temperature") > 300
```

used in searches like

```python
c.search(Key("color") == "red", Key("shape") != "circle", Key("temperature") > 300)
```

Passing _multiple_ queries to `search(...)`, as shown above, compounds them
(logical AND).  Alternatively, you may progressively narrow results and store
them in separate variables.

```python
results1 = c.search(...)
results2 = results1.search(...)
results3 = results2.search(...)
```

There is no support for logical OR. You must perform separate queries and
combine them yourself. It may convenient to combine them like so in a `dict`,
as long as the results are not too numerous to fit in memory.

```python
a = c.search(...)
b = c.search(...)
c = c.search(...)
combined_results = dict(**a, **b, **c)
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
