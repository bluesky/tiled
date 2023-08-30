import React from 'react';
import {AxiosInterceptor} from './client';
import Container from "@mui/material/Container";
import ErrorBoundary from "./components/error-boundary";
import Login from "./routes/login";
import { Outlet } from "react-router-dom";
import TiledAppBar from "./components/tiled-app-bar";
import { useEffect, useState } from "react";
import { fetchSettings } from "./settings"
import { SettingsContext, emptySettings } from "./context/settings"
import { BrowserRouter, Route, Routes } from "react-router-dom";
import { Suspense, lazy } from "react";
import Skeleton from "@mui/material/Skeleton";
import UserContext, {userObjectContext} from './context/user';
import { fetchServerInfo, ServerInfo } from './server-info';
const Browse = lazy(() => import("./routes/browse"));


function MainContainer() {
  return (
    <Container>
      <TiledAppBar />
      <ErrorBoundary>
        <Outlet />
      </ErrorBoundary>
    </Container>
  )
}


// This is set in vite.config.js. It is the base path of the ui.
const basename = import.meta.env.BASE_URL;

function App() {
  const [userContext, setUserContext] = React.useState(userObjectContext)
  const [user, setUser] = React.useState({user: ""});
  const value = { user, setUser };
  // Create function to update the current context, prioritizing new over old changed properties
  const updateContext = (contextUpdates = {}) =>
    setUserContext(currentContext => ({ ...currentContext, ...contextUpdates }))

  // useEffect prevents the entire context for rerendering when component rerenders
  React.useEffect(() => {

      updateContext({
        updateStatus: (value: string) => updateContext({ user: value }),
      })

  }, [])


  const [settings, setSettings] = useState(emptySettings);
  const [serverInfo, setServerInfo] = useState(ServerInfo);

  useEffect( () => {
    const controller = new AbortController();
    async function initSettingsContext() {
      var data = await fetchSettings(controller.signal);
      setSettings(data);
    }

    async function initServerInfoContext(){
      var info = await fetchServerInfo(controller.signal, settings.api_url);
      setServerInfo(info);
    }

    initSettingsContext();
  }, []);
  return (
    <UserContext.Provider value={userContext} >
      <SettingsContext.Provider value={settings}>
        <BrowserRouter basename={basename}>
          <ErrorBoundary>
            <Suspense fallback={<Skeleton variant="rectangular" />}>
              <AxiosInterceptor>
                <Routes>
                  <Route path="/" element={<MainContainer />}>
                    <Route path="/browse/*" element={<Browse />} />
                    <Route path="/login" element={<Login/>} />
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
                </AxiosInterceptor>
            </Suspense>
          </ErrorBoundary>
        </BrowserRouter>
      </SettingsContext.Provider>
  </UserContext.Provider>
  )
}

export default App;