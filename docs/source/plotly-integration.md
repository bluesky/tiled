# Plot Data in Plotly Chart Studio

Start the server in a way that accepts requests from the chart-studio frontend.

```
TILED_ALLOW_ORIGINS="https://chart-studio.plotly.com " tiled serve pyobject tiled.examples.generic:demo
```

Navigate your browser to https://chart-studio.plotly.com. Use the "Import"
feature to import data by URL. Enter a URL such as

```
http://localhost:8000/dataframe/full/dataframes/df?format=text/csv
```

or, to load only certain columns,

```
http://localhost:8000/dataframe/full/dataframes/df?format=text/csv&column=A&column=B
```
