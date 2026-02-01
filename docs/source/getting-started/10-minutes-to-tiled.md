---
jupytext:
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.17.1
kernelspec:
  display_name: Python 3 (ipykernel)
  language: python
  name: python3
---

# 10 minutes to Tiled

This is a short, tutorial-style introduction to Tiled, for new users.

## Connect

To begin, will use a public demo instance of Tiled. If you are reading
this tutorial from an airplane, see the section below on running your
own Tiled server.


```{code-cell} ipython3
from tiled.client import from_uri
client = from_uri("https://tiled-demo.nsls2.bnl.gov")
client
```
