import "@fontsource/roboto/300.css";
import "@fontsource/roboto/400.css";
import "@fontsource/roboto/500.css";
import "@fontsource/roboto/700.css";

import { BrowserRouter, Route, Routes } from "react-router-dom";
import { Suspense, lazy } from "react";

import App from "./App";
import CssBaseline from "@mui/material/CssBaseline";
import ErrorBoundary from "./components/error-boundary";
import Skeleton from "@mui/material/Skeleton";
import { ThemeProvider } from "@mui/material/styles";
import { render } from "react-dom";
import theme from "./theme";

const Browse = lazy(() => import("./routes/browse"));

const root_element = document.getElementById("root");
const basename = import.meta.env.BASE_URL;

render(
  <ThemeProvider theme={theme}>
    {/* CssBaseline kickstart an elegant, consistent, and simple baseline to build upon. */}
    <CssBaseline />

    <BrowserRouter basename={basename}>
      <ErrorBoundary>
        <Suspense fallback={<Skeleton variant="rectangular" />}>
          <Routes>
            <Route path="/" element={<App />}>
              <Route path="/browse/*" element={<Browse />} />
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
        </Suspense>
      </ErrorBoundary>
    </BrowserRouter>
  </ThemeProvider>,
  root_element
);
