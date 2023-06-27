import { defineConfig } from "vite"
import react from "@vitejs/plugin-react"
import { viteRequire } from "vite-require"

export default defineConfig({
    server: {
        proxy: {
            "/api": {
                target: "http://127.0.0.1:8000",
                changeOrigin: true,
		// rewrite: (path) => path.replace(/^\/api/, "")
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
