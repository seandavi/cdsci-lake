import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    host: true,          // bind 0.0.0.0 so the tailnet can reach it
    allowedHosts: true,  // accept the MagicDNS Host header (e.g. onclappc02)
    proxy: {
      // Same-origin: browser hits :5173, Vite forwards API calls to the backend.
      '/api': 'http://localhost:8010',
      '/health': 'http://localhost:8010',
    },
  },
})
