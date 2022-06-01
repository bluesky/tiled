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


```{eval-rst}
.. autosummary::
   :toctree: generated

   tiled.queries.Key
```
