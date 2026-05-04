import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const frontendRestartTriggerPath = path.resolve(__dirname, '..', 'frontend-restart-trigger.json');

function frontendRestartTriggerPlugin() {
  return {
    name: 'frontend-restart-trigger',
    configureServer(server) {
      fs.closeSync(fs.openSync(frontendRestartTriggerPath, 'a'));
      server.watcher.add(frontendRestartTriggerPath);

      server.watcher.on('change', (changedPath) => {
        if (path.resolve(changedPath) !== frontendRestartTriggerPath) {
          return;
        }

        console.info('[FRONTEND_RESTART_TRIGGERED]', {
          triggerPath: frontendRestartTriggerPath,
        });

        void server.restart();
      });
    },
  };
}

export default defineConfig({
  plugins: [react(), frontendRestartTriggerPlugin()],
});
