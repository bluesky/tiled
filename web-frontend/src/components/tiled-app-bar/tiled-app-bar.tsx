import { useState } from "react";
import AppBar from "@mui/material/AppBar";
import Button from "@mui/material/Button";
import Container from "@mui/material/Container";
import Menu from "@mui/material/Menu";
import MenuItem from "@mui/material/MenuItem";
import { Link, useNavigate } from "react-router-dom";
import Toolbar from "@mui/material/Toolbar";
import Typography from "@mui/material/Typography";
import Box from "@mui/material/Box";
import { useAuth } from "../../auth/auth-context";
import { tokenManager } from "../../auth/token-manager";
import { axiosInstance } from "../../client";

const TiledAppBar = () => {
  const { isAuthenticated, identity, authRequired, onLogout } = useAuth();
  const navigate = useNavigate();
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);

  const handleLogout = async () => {
    setAnchorEl(null);
    const refreshToken = tokenManager.getRefreshToken();
    if (refreshToken) {
      try {
        await axiosInstance.post("/api/v1/auth/session/revoke", {
          refresh_token: refreshToken,
        });
      } catch {
        // Best-effort server-side revocation.
      }
    }
    onLogout();
    if (authRequired) {
      navigate("/login", { replace: true });
    }
  };

  return (
    <AppBar position="static">
      <Container maxWidth="xl">
        <Toolbar disableGutters>
          <Box
            component={Link}
            to="/browse/"
            sx={{
              display: "flex",
              alignItems: "center",
              textDecoration: "none",
              color: "inherit",
              mr: 3,
            }}
          >
            <img
              src={`${import.meta.env.BASE_URL}tiled-logo.svg`}
              alt="Tiled logo"
              style={{ height: 28, marginRight: 8 }}
            />
            <Typography variant="h6" noWrap>
              TILED
            </Typography>
          </Box>
          <Box sx={{ flexGrow: 1 }} />
          {isAuthenticated && identity && (
            <>
              <Button
                color="inherit"
                size="small"
                onClick={(e) => setAnchorEl(e.currentTarget)}
                sx={{ textTransform: "none" }}
              >
                {identity.id}
              </Button>
              <Menu
                anchorEl={anchorEl}
                open={Boolean(anchorEl)}
                onClose={() => setAnchorEl(null)}
              >
                <MenuItem onClick={handleLogout}>Log out</MenuItem>
              </Menu>
            </>
          )}
          {!isAuthenticated && authRequired && (
            <Button
              component={Link}
              color="inherit"
              to="/login"
              size="small"
            >
              Log in
            </Button>
          )}
        </Toolbar>
      </Container>
    </AppBar>
  );
};
export default TiledAppBar;
