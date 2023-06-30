import Container from "@mui/material/Container";
import ErrorBoundary from "./components/error-boundary";
import { Outlet } from "react-router-dom";
import TiledAppBar from "./components/tiled-app-bar";
import { useEffect, useState } from "react";
import { fetchSettings } from "./settings"
import { SettingsContext, emptySettings } from "./context/settings"

function App() {
  const [settings, setSettings] = useState(emptySettings)
  useEffect( () => {
    const controller = new AbortController()
    async function initSettingsContext() {
      var data = await fetchSettings(controller.signal)
      setSettings(data)
    }
    initSettingsContext()
  } , []);
  return (
    <SettingsContext.Provider value={settings}>
      <Container>
        <TiledAppBar />
        <ErrorBoundary>
          <Outlet />
        </ErrorBoundary>
      </Container>
    </SettingsContext.Provider>
  );
}

export default App;
