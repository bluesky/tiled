import AppBar from "@mui/material/AppBar";
import Button from "@mui/material/Button";
import Container from "@mui/material/Container";
import { Link } from "react-router-dom";
import Toolbar from "@mui/material/Toolbar";
import Typography from "@mui/material/Typography";

const TiledAppBar = () => {
  return (
    <AppBar position="static">
      <Container maxWidth="xl">
        <Toolbar disableGutters>
          <Typography
            variant="h6"
            noWrap
            component="div"
            sx={{ mr: 2, display: { xs: "none", md: "flex" } }}
          >
            TILED
          </Typography>
          <Typography
            variant="h6"
            noWrap
            component="div"
            sx={{ mr: 2, display: { xs: "none", md: "flex" } }}
          >
            <Button component={Link} color="inherit" to="/node/">
              Browse
            </Button>
            <Button component={Link} color="inherit" to="/apikeys/">
              Manage API Keys
            </Button>
          </Typography>
        </Toolbar>
      </Container>
    </AppBar>
  );
};
export default TiledAppBar;
