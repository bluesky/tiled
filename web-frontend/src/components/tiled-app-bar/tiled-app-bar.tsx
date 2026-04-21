import AppBar from "@mui/material/AppBar";
import Button from "@mui/material/Button";
import Container from "@mui/material/Container";
import { Link, useNavigate } from "react-router-dom";
import Toolbar from "@mui/material/Toolbar";
import Typography from "@mui/material/Typography";
import Box from "@mui/material/Box";
import { useAuth, getStoredRefreshToken } from "../../context/auth";
import { axiosInstance } from "../../client";

const TiledAppBar = () => {
  const { isAuthenticated, identity, authRequired, onLogout } = useAuth();
  const navigate = useNavigate();

  const handleLogout = async () => {
    const refreshToken = getStoredRefreshToken();
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
          <Box sx={{ flexGrow: 1 }} />
          {isAuthenticated && (
            <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
              {identity && (
                <Typography variant="body2" sx={{ mr: 1 }}>
                  {identity.id}
                </Typography>
              )}
              <Button color="inherit" onClick={handleLogout} size="small">
                Log out
              </Button>
            </Box>
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
