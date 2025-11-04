import Container from "@mui/material/Container";
import ErrorBoundary from "./components/error-boundary/error-boundary";
import { Outlet } from "react-router-dom";
import { TiledAppBar } from "./components/tiled-app-bar/tiled-app-bar";
import { useEffect, useState } from "react";
import { fetchSettings, getApiBaseUrl } from "./settings";
import { SettingsContext, emptySettings } from "./context/settings";
import { BrowserRouter, Route, Routes } from "react-router-dom";
import { Suspense, lazy } from "react";
import Skeleton from "@mui/material/Skeleton";
import { LoginPage } from "./components/login-page/login-page";
import { ProtectedRoute } from "./components/protected-route";
import { Navigate } from "react-router-dom";
import { AuthProvider } from "./auth/auth-provider";


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
  const [settings, setSettings] = useState({
    ...emptySettings,
    api_url: `${getApiBaseUrl()}/api/v1`,

  });

  useEffect(() => {
    const controller = new AbortController();
    fetchSettings(controller.signal).then(setSettings);
    return () => controller.abort();
  }, []);
  return (
    <BrowserRouter basename={basename}>
      <ErrorBoundary>
        <AuthProvider>
          <SettingsContext.Provider value={settings}>
            <Suspense fallback={<Skeleton variant="rectangular" />}>
              <Routes>
                <Route path="/login" element={<LoginPage />} />
                <Route
                  path="/"
                  element={
                    <ProtectedRoute>
                      <MainContainer />
                    </ProtectedRoute>
                  }
                >
                  <Route index element={<Navigate to="/browse" replace />} />
                  <Route path="browse/*" element={<Browse />} />
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
          </SettingsContext.Provider>
        </AuthProvider>
      </ErrorBoundary>
    </BrowserRouter>
  );
}

export default App;
