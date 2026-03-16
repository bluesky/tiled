import React from "react";
import { Navigate, useLocation } from "react-router-dom";
import { useAuth } from "../auth/auth-context";
import { Box, CircularProgress, Typography } from "@mui/material";

interface ProtectedRouteProps {
  children: React.ReactNode;
}

export const ProtectedRoute: React.FC<ProtectedRouteProps> = ({ children }:{ children: React.ReactNode}) => {
  const { isAuthenticated, isLoading, authConfig } = useAuth();
  const location = useLocation();

  if (isLoading) {
    return (
      <Box
        sx={{
          display: "flex",
          flexDirection: "column",
          justifyContent: "center",
          alignItems: "center",
          height: "100vh",
          gap: 2,
        }}
      >
        <CircularProgress size={60} />
        <Typography variant="body1" color="text.secondary">
          Loading...
        </Typography>
      </Box>
    );
  }

  //scenario 2, if no providers, allow public access
  const hasProviders = authConfig && Array.isArray(authConfig.providers) && authConfig.providers.length>0;
  const isPublicAccess = authConfig?.required === false; 
  if (!hasProviders && isPublicAccess) {
    return <>{children}</>;
  }

  if (!isAuthenticated) {
    return <Navigate to="/login" state={{ from: location }} replace />;
  }

  return <>{children}</>;
};
