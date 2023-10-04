import React from 'react';
import AppBar from "@mui/material/AppBar";
import Button from "@mui/material/Button";
import Container from "@mui/material/Container";
import { Link } from "react-router-dom";
import Toolbar from "@mui/material/Toolbar";
import Typography from "@mui/material/Typography";
import UserContext from '../context/user';
const TiledAppBar = () => {
  const user = React.useContext(UserContext);

  return (
    <AppBar position="static">
      <Container maxWidth="xl">
        <Toolbar disableGutters>
          <Typography
            variant="h6"
            noWrap
            component="div"
            sx={{ mr: 3, display: "flex" }}
          >
            TILED
          </Typography>
          <Typography
            variant="h6"
            noWrap
            component="div"
            sx={{ mr: 2, display: "flex" }}
          >
            <Button component={Link} color="inherit" to="/browse/">
              Browse
            </Button>
          </Typography>
          <Typography>
            {user.user}
          </Typography>
        </Toolbar>
      </Container>
    </AppBar>
  );
};
export default TiledAppBar;
