import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { viteRequire } from "vite-require";
import { webcrypto as crypto } from "crypto";
import dotenv from "dotenv";
dotenv.config();

if (!global.crypto) {
  global.crypto = require("crypto");
  global.crypto.getRandomValues = (arr) =>
    require("crypto").randomFillSync(arr);
}

export default defineConfig({
  base: "/ui/",
  server: {
    //https: true,
    proxy: {
      "/api": {
        target: "http://127.0.0.1:8000",
        secure: false,
        changeOrigin: true,
        rewrite: (path) => path,
      },
      "/tiled-ui-settings": {
        target: "http://127.0.0.1:8000",
        secure: false,
        changeOrigin: true,
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
});
