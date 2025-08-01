import Container from "@mui/material/Container";
import ErrorBoundary from "./components/error-boundary/error-boundary";
import { Outlet } from "react-router-dom";
import TiledAppBar from "./components/tiled-app-bar/tiled-app-bar";
import { useEffect, useState } from "react";
import { fetchSettings } from "./settings";
import { SettingsContext, emptySettings } from "./context/settings";
import { BrowserRouter, Route, Routes } from "react-router-dom";
import { Suspense, lazy } from "react";
import Skeleton from "@mui/material/Skeleton";

const Browse = lazy(() => import("./routes/browse"));

function MainContainer() {
  return (
    <Container>
      <TiledAppBar />
      <ErrorBoundary>
        <Outlet />
      </ErrorBoundary>
    </Container>
  );
}

// This is set in vite.config.js. It is the base path of the ui.
const basename = import.meta.env.BASE_URL;

function App() {
  const [settings, setSettings] = useState(emptySettings);
  useEffect(() => {
    const controller = new AbortController();
    async function initSettingsContext() {
      const data = await fetchSettings(controller.signal);
      setSettings(data);
    }
    initSettingsContext();
  }, []);
  return (
    <SettingsContext.Provider value={settings}>
      <BrowserRouter basename={basename}>
        <ErrorBoundary>
          <Suspense fallback={<Skeleton variant="rectangular" />}>
            <Routes>
              <Route path="/" element={<MainContainer />}>
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
    </SettingsContext.Provider>
  );
}

export default App;
