# Plot Data in Plotly Chart Studio

This tutorial illustrates how Tiled can be used to get data into a browser-based
data visualization tool.

1. Start the server in a way that accepts requests from the chart-studio frontend.

   ```
   TILED_ALLOW_ORIGINS='["https://chart-studio.plotly.com"]' tiled serve pyobject --public tiled.examples.generated:tree
   ```

2. Navigate your browser to
   [https://chart-studio.plotly.com](https://chart-studio.plotly.com).

3. Log in. You can log in with your GitHub account or your Google account, among
   others. (It's free.)

4. Click the menu "Create" and the option "Chart".

5. Use the "Import" menu to import data by URL. Enter a URL such as

   ```
   http://localhost:8000/api/v1/table/full/tables/short_table?format=text/csv
   ```

   or, to load only certain columns,

   ```
   http://localhost:8000/api/v1/table/full/tables/short_table?format=text/csv&field=A&field=B
   ```
