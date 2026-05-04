import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectRoot = path.resolve(__dirname, '..');

const targetArg = String(process.argv[2] || 'all').toLowerCase();
const dryRun = process.argv.includes('--dry-run');
const validTargets = new Set(['backend', 'frontend', 'all']);

if (!validTargets.has(targetArg)) {
  console.error('[RESTART_TRIGGER_FAILED]', {
    reason: 'invalid_target',
    target: targetArg,
    validTargets: [...validTargets],
  });
  process.exit(1);
}

const triggerFiles = {
  backend: path.join(projectRoot, 'backend', 'local-restart-trigger.json'),
  frontend: path.join(projectRoot, 'frontend-restart-trigger.json'),
};

const targets = targetArg === 'all' ? ['backend', 'frontend'] : [targetArg];

function writeTrigger(target) {
  const triggerPath = triggerFiles[target];
  const payload = {
    target,
    source: 'restart_dev_service_script',
    requestedAt: new Date().toISOString(),
  };

  if (dryRun) {
    console.log('[RESTART_TRIGGER_DRY_RUN]', { target, triggerPath, payload });
    return;
  }

  fs.mkdirSync(path.dirname(triggerPath), { recursive: true });
  fs.writeFileSync(triggerPath, `${JSON.stringify(payload, null, 2)}\n`, 'utf8');

  console.log('[RESTART_TRIGGER_WRITTEN]', { target, triggerPath });
}

for (const target of targets) {
  writeTrigger(target);
}

console.log('[RESTART_TRIGGER_DONE]', {
  targets,
  mode: dryRun ? 'dry_run' : 'live',
});
