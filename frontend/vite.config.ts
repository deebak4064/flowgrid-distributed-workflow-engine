import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: { alias: { "@": "/src" } },
  server: {
    port: 5173,
    proxy: { "/api": { target: process.env.FLOWGRID_API ?? "http://127.0.0.1:8080", changeOrigin: true } },
  },
  build: {
    chunkSizeWarningLimit: 1500,
    rollupOptions: {
      output: {
        manualChunks: (id) =>
          id.includes("node_modules/recharts") || id.includes("node_modules/d3") ? "charts"
          : id.includes("node_modules/@xyflow") ? "flow" : undefined,
      },
    },
  },
});
