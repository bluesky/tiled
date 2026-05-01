import React, { useState } from "react";
import { useAuth } from "../auth/auth-context";
import { useNavigate, Navigate } from "react-router-dom";
import { axiosInstance } from "../client";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import TextField from "@mui/material/TextField";
import Typography from "@mui/material/Typography";
import Alert from "@mui/material/Alert";
import Divider from "@mui/material/Divider";
import Stack from "@mui/material/Stack";
import { components } from "../openapi_schemas";

type Provider = components["schemas"]["AboutAuthenticationProvider"];

function PasswordLogin({
  provider,
  onSuccess,
}: {
  provider: Provider;
  onSuccess: () => void;
}) {
  const { onLogin } = useAuth();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    setLoading(true);
    try {
      const body = new URLSearchParams({
        username,
        password,
        grant_type: "password",
      });
      const response = await axiosInstance.post(
        provider.links.auth_endpoint,
        body.toString(),
        { headers: { "Content-Type": "application/x-www-form-urlencoded" } },
      );
      const data = response.data;
      onLogin(data.access_token, data.refresh_token, data.identity);
      onSuccess();
    } catch (err: any) {
      const detail =
        err?.response?.data?.detail || "Login failed. Check your credentials.";
      setError(typeof detail === "string" ? detail : JSON.stringify(detail));
    } finally {
      setLoading(false);
    }
  };

  return (
    <Card variant="outlined" sx={{ maxWidth: 400, mx: "auto", mt: 2 }}>
      <CardContent>
        {provider.confirmation_message && (
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            {provider.confirmation_message}
          </Typography>
        )}
        <form onSubmit={handleSubmit}>
          <Stack spacing={2}>
            {error && <Alert severity="error">{error}</Alert>}
            <TextField
              label="Username"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              required
              fullWidth
              size="small"
              autoFocus
              InputLabelProps={{ required: false }}
            />
            <TextField
              label="Password"
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
              fullWidth
              size="small"
              InputLabelProps={{ required: false }}
            />
            <Button
              type="submit"
              variant="contained"
              disabled={loading}
              fullWidth
            >
              {loading ? "Logging in..." : "Log In"}
            </Button>
          </Stack>
        </form>
      </CardContent>
    </Card>
  );
}

function ExternalLogin({ provider }: { provider: Provider }) {
  const handleClick = () => {
    const state = encodeURIComponent(window.location.href);
    window.location.href = `${provider.links.auth_endpoint}?state=${state}`;
  };

  return (
    <Card variant="outlined" sx={{ maxWidth: 400, mx: "auto", mt: 2 }}>
      <CardContent>
        <Button variant="contained" onClick={handleClick} fullWidth>
          Log in with {provider.provider.charAt(0).toUpperCase() + provider.provider.slice(1)}
        </Button>
      </CardContent>
    </Card>
  );
}

export default function LoginPage() {
  const { providers, isAuthenticated } = useAuth();
  const navigate = useNavigate();

  if (isAuthenticated) {
    return <Navigate to="/browse/" replace />;
  }

  const handleLoginSuccess = () => navigate("/browse/", { replace: true });

  const internalProviders = providers.filter(
    (p) => p.mode === "internal" || (p.mode as string) === "password",
  );
  const externalProviders = providers.filter((p) => p.mode === "external");

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "center",
        minHeight: "60vh",
        p: 3,
      }}
    >
      <Typography variant="h4" gutterBottom>
        Log in to Tiled
      </Typography>
      {providers.length === 0 && (
        <Typography color="text.secondary">
          No authentication providers configured.
        </Typography>
      )}
      {internalProviders.map((p) => (
        <PasswordLogin
          key={p.provider}
          provider={p}
          onSuccess={handleLoginSuccess}
        />
      ))}
      {internalProviders.length > 0 && externalProviders.length > 0 && (
        <Divider sx={{ width: "100%", maxWidth: 400, my: 2 }}>or</Divider>
      )}
      {externalProviders.map((p) => (
        <ExternalLogin key={p.provider} provider={p} />
      ))}
    </Box>
  );
}
