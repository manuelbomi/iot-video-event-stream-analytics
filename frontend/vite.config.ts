import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    // Proxy API/WebSocket calls to the FastAPI dashboard backend
    // (src/dashboard/api.py, normally run with `uvicorn` on :8000) during
    // local `npm run dev`, so the frontend can use plain relative URLs
    // (`/ws`, `/api/snapshot`) in both dev and the built/served production
    // bundle (where nginx does the equivalent proxying -- see
    // frontend/nginx.conf).
    proxy: {
      '/ws': {
        target: 'ws://localhost:8000',
        ws: true,
      },
      '/api': {
        target: 'http://localhost:8000',
      },
    },
  },
})
