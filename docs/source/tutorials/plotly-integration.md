# Plot Data in Plotly Chart Studio

1. Start the server in a way that accepts requests from the chart-studio frontend.

   ```
   TILED_ALLOW_ORIGINS="https://chart-studio.plotly.com" tiled serve pyobject tiled.examples.generic:demo
   ```

2. Navigate your browser to https://chart-studio.plotly.com.

3. Click the menu "Create" and the option "Chart".

4. Use the "Import" menu to import data by URL. Enter a URL such as

   ```
   http://localhost:8000/dataframe/full/dataframes/df?format=text/csv
   ```

   or, to load only certain columns,

   ```
   http://localhost:8000/dataframe/full/dataframes/df?format=text/csv&column=A&column=B
   ```
