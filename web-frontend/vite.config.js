import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";
import { viteRequire } from "vite-require";
import { webcrypto as crypto } from "crypto";

// vite.config.js
if (!global.crypto) {
  global.crypto = require("crypto");
  global.crypto.getRandomValues = (arr) =>
    require("crypto").randomFillSync(arr);
}

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const target = env.VITE_API_TARGET || "http://127.0.0.1:8000";
  const redirectToLogin= env.VITE_LOGIN_REDIRECT || true;
  return {
    base: "/ui/",
    redirectToLogin: redirectToLogin,
    server: {
      proxy: {
        "/api": {
          target,
          ws: true,
        },
        "/custom": {
          target,
        },
        "/tiled-ui-settings": {
          target,
        },
      },
    },
    plugins: [
      viteRequire(),
      react({
        jsxRuntime: "automatic",
        babel: {
          plugins: [],
        },
      }),
    ],
    test: {
      globals: true,
      environment: "jsdom",
      setupFiles: "./test/setup.ts",
      include: ["src/components/**/*.test.tsx", "src/**/*.test.tsx"],
    },
  };
});
