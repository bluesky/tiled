import { defineConfig } from "vite"
import react from "@vitejs/plugin-react"
import { viteRequire } from "vite-require"

export default defineConfig({
    base: "/ui/",
    server: {
        proxy: {
            "/api": {
                target: "http://127.0.0.1:8000",
            },
            "/tiled-ui-settings": {
                target: "http://127.0.0.1:8000",
            }
        }
    },
    plugins: [
        viteRequire(),
        react({
            jsxRuntime: "classic",
            babel: {
                plugins: []
            }
        })
    ]
})
