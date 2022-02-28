import Container from "@mui/material/Container";
import ErrorBoundary from "./components/error-boundary";
import { Outlet } from "react-router-dom";
import TiledAppBar from "./components/tiled-app-bar";

function App() {
  return (
    <Container>
      <TiledAppBar />
        <ErrorBoundary>
          <Outlet />
        </ErrorBoundary>
    </Container>
  );
}

export default App;
