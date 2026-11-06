import { defineConfig } from "vite";
import { fileURLToPath } from "node:url";

export default defineConfig({
  root: "apps/demo-web",
  resolve: {
    alias: {
      "@client-logging-lab/client-logger": fileURLToPath(new URL("./packages/client-logger/src/logger.ts", import.meta.url)),
    },
  },
  build: {
    outDir: "../../dist",
    emptyOutDir: true,
    rolldownOptions: {
      input: {
        app: fileURLToPath(new URL("./apps/demo-web/index.html", import.meta.url)),
        inspector: fileURLToPath(new URL("./apps/demo-web/inspector.html", import.meta.url)),
      },
    },
  },
  server: {
    port: 5173,
    strictPort: true,
    proxy: { "/v1": "http://127.0.0.1:3001", "/health": "http://127.0.0.1:3001" },
  },
});
