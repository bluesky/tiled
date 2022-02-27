import "@fontsource/roboto/300.css";
import "@fontsource/roboto/400.css";
import "@fontsource/roboto/500.css";
import "@fontsource/roboto/700.css";

import { BrowserRouter, Route, Routes } from "react-router-dom";

import App from "./App";
import CssBaseline from "@mui/material/CssBaseline";
import Node from "./routes/node";
import { ThemeProvider } from "@mui/material/styles";
import { render } from "react-dom";
import reportWebVitals from "./reportWebVitals";
import theme from "./theme";
import yaml from 'js-yaml';

const root_element = document.getElementById("root");
const basename = process.env.PUBLIC_URL;

// Load UI configuration. This includes UI-specific information
// that we cannot get from the API.
// The configuration is specified as YAML.
// "Why not JSON?" I hear you ask.
// JSON is not a good configuration format because it does not support *comments*.
// Therefore, we parse the YAML into JSON here and stash it in sessionStorage.
fetch(`${basename}/config.yml`).then(response => response.text()).then(data => sessionStorage.setItem("config", JSON.stringify(yaml.load(data))));
render(
  <ThemeProvider theme={theme}>
    {/* CssBaseline kickstart an elegant, consistent, and simple baseline to build upon. */}
    <CssBaseline />

    <BrowserRouter basename={basename}>
      <Routes>
        <Route path="/" element={<App />}>
          <Route path="/browse/*" element={<Node />} />
        </Route>
        <Route
          path="*"
          element={
            <main style={{ padding: "1rem" }}>
              <p>There's nothing here!</p>
            </main>
          }
        />
      </Routes>
    </BrowserRouter>
  </ThemeProvider>,
  root_element
);

// If you want to start measuring performance in your app, pass a function
// to log results (for example: reportWebVitals(console.log))
// or send to an analytics endpoint. Learn more: https://bit.ly/CRA-vitals
reportWebVitals();
