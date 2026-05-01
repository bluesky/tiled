import Container from "@mui/material/Container";
import ErrorBoundary from "./components/error-boundary/error-boundary";
import { Outlet, Navigate, BrowserRouter, Route, Routes } from "react-router-dom";
import TiledAppBar from "./components/tiled-app-bar/tiled-app-bar";
import React, { useEffect, useState } from "react";
import * as ReactDOM from "react-dom";
import { fetchSettings } from "./settings";
import { SettingsContext, emptySettings } from "./context/settings";
import { AuthProvider } from "./auth/auth-provider";
import { useAuth } from "./auth/auth-context";
import { about } from "./client";
import { components } from "./openapi_schemas";
import { Suspense, lazy } from "react";
import Skeleton from "@mui/material/Skeleton";

// Expose React globals so external spec_view plugins (IIFE bundles)
// can use React without bundling their own copy.
(window as any).React = React;
(window as any).ReactDOM = ReactDOM;

const Browse = lazy(() => import("./routes/browse"));
const LoginPage = lazy(() => import("./routes/login"));
const AuthCallback = lazy(() => import("./routes/auth-callback"));

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

function RequireAuth({ children }: { children: React.ReactElement }) {
  const { authRequired, isAuthenticated, initialized } = useAuth();
  if (!initialized) return <Skeleton variant="rectangular" />;
  if (authRequired && !isAuthenticated) return <Navigate to="/login" replace />;
  return children;
}

// This is set in vite.config.js. It is the base path of the ui.
const basename = import.meta.env.BASE_URL;

function App() {
  const [settings, setSettings] = useState(emptySettings);
  const [authentication, setAuthentication] =
    useState<components["schemas"]["AboutAuthentication"] | null>(null);

  useEffect(() => {
    const controller = new AbortController();

    // Fetch settings independently — failure should not block auth init.
    fetchSettings(controller.signal)
      .then(setSettings)
      .catch((err) => {
        if (!controller.signal.aborted) {
          console.error("Failed to fetch UI settings:", err);
        }
      });

    // Fetch server info to determine auth requirements.
    // On failure, keep authentication=null (initialized=false) so RequireAuth
    // shows a loading skeleton rather than incorrectly bypassing login.
    about()
      .then((data) => setAuthentication(data.authentication))
      .catch((err) => {
        if (!controller.signal.aborted) {
          console.error("Failed to fetch server info:", err);
        }
      });

    return () => controller.abort();
  }, []);

  return (
    <SettingsContext.Provider value={settings}>
      <AuthProvider authentication={authentication}>
        <BrowserRouter basename={basename}>
          <ErrorBoundary>
            <Suspense fallback={<Skeleton variant="rectangular" />}>
              <Routes>
                <Route path="/" element={<MainContainer />}>
                  <Route
                    index
                    element={<Navigate to="/browse/" replace />}
                  />
                  <Route
                    path="/browse/*"
                    element={
                      <RequireAuth>
                        <Browse />
                      </RequireAuth>
                    }
                  />
                </Route>
                <Route path="/login" element={<LoginPage />} />
                <Route path="/auth/callback" element={<AuthCallback />} />
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
      </AuthProvider>
    </SettingsContext.Provider>
  );
}

export default App;
