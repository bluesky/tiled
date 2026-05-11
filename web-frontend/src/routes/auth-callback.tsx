import { useEffect } from "react";
import { useNavigate } from "react-router-dom";
import { useAuth } from "../auth/auth-context";

/**
 * OAuth callback page. Captures tokens from query params
 * (set by the server's redirect_on_success) and redirects to browse.
 */
export default function AuthCallback() {
  const { onLogin } = useAuth();
  const navigate = useNavigate();

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const accessToken = params.get("access_token");
    const refreshToken = params.get("refresh_token");
    const identityId = params.get("identity.id");
    const identityProvider = params.get("identity.provider");
    const state = params.get("state");

    if (accessToken && refreshToken) {
      const identity =
        identityId && identityProvider
          ? { id: identityId, provider: identityProvider }
          : undefined;
      onLogin(accessToken, refreshToken, identity);

      if (state) {
        try {
          const url = new URL(decodeURIComponent(state));
          // Strip the router basename to avoid double-prefix (e.g. /ui/ui/browse/)
          const base = import.meta.env.BASE_URL.replace(/\/$/, "");
          const path = url.pathname.startsWith(base)
            ? url.pathname.slice(base.length) || "/"
            : url.pathname;
          navigate(path + url.search, { replace: true });
          return;
        } catch {
          // invalid URL in state, fall through
        }
      }
      navigate("/browse/", { replace: true });
    } else {
      navigate("/login", { replace: true });
    }
  }, [onLogin, navigate]);

  return <div style={{ padding: "1rem" }}>Completing login...</div>;
}
