import React from "react";
import { useNavigate, useLocation } from "react-router-dom";
import { useAuth } from "../../auth/auth-context";
import {
  AppBar,
  Box,
  Toolbar,
  Typography,
  Button,
  IconButton,
} from "@mui/material";
import MenuIcon from "@mui/icons-material/Menu";
import LogoutIcon from "@mui/icons-material/Logout";
import LoginIcon from "@mui/icons-material/Login";

export const TiledAppBar: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const { isAuthenticated, logout } = useAuth();

  const handleLogout = async () => {
    await logout();
    navigate("/login");
  };

  return (
    <Box sx={{ flexGrow: 1 }}>
      <AppBar position="static">
        <Toolbar>
          <IconButton
            size="large"
            edge="start"
            color="inherit"
            aria-label="menu"
            sx={{ mr: 2 }}
          >
            <MenuIcon />
          </IconButton>

          <Typography variant="h5" component="div">
            TILED
          </Typography>

          {isAuthenticated && location.pathname !== "/browse" && (
            <Button color="inherit" onClick={() => navigate("/browse")} sx={{ ml: 2, minWidth: 120 }}>
              Browse
            </Button>
          )}

          <Box sx={{ flexGrow: 12 }} />

          {isAuthenticated ? (
            <Button
              color="inherit"
              onClick={handleLogout}
              startIcon={<LogoutIcon />}
              sx={{ minWidth: 120 }}
            >
              Logout
            </Button>
          ) : (
            <Button
              color="inherit"
              onClick={() => navigate("/login")}
              startIcon={<LoginIcon />}
            >
              Login
            </Button>
          )}
        </Toolbar>
      </AppBar>
    </Box>
  );
};
