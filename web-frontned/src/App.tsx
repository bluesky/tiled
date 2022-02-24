import Container from "@mui/material/Container";
import { Outlet } from "react-router-dom";
import TiledAppBar from "./components/tiled-app-bar";

function App() {
  return (
    <Container>
      <TiledAppBar />
      <Outlet />
    </Container>
  );
}

export default App;
