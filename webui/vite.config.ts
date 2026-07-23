import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  optimizeDeps: {
    exclude: ['monaco-editor'],
  },
  server: {
    proxy: {
      '/api': {
        target: process.env.API_ROOT || 'http://localhost:5115',
      },
    },
  },
});
