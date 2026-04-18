import { Router } from 'express';
import { enqueueGeneratorPost } from '../services/generatorService.js';
import {
  buildGeneratorDebugPayload,
  getGeneratorValidationError,
  logGeneratorDebug,
  normalizeGeneratorInput
} from '../services/generatorFlowService.js';
import {
  getWorkerStatus,
  listPublishingLogs,
  listPublishingQueue,
  retryPublishingQueue,
  runPublishingWorkers,
  saveFacebookWorkerSettings
} from '../services/publisherService.js';

const router = Router();

function getRequesterRole(req) {
  return String(req.headers['x-user-role'] || '').trim().toLowerCase();
}

function requireAdmin(req, res, next) {
  if (getRequesterRole(req) !== 'admin') {
    return res.status(403).json({ error: 'Nur Admin darf diesen Bereich verwalten.' });
  }

  return next();
}

router.get('/queue', (req, res) => {
  res.json({ items: listPublishingQueue() });
});

router.get('/logs', (req, res) => {
  res.json({ items: listPublishingLogs() });
});

router.get('/workers/status', (req, res) => {
  res.json(getWorkerStatus());
});

router.post('/generator', (req, res) => {
  const normalizedInput = normalizeGeneratorInput(req.body ?? {});
  const debugPayload = buildGeneratorDebugPayload(normalizedInput);
  logGeneratorDebug('api.publishing.generator.request', debugPayload);

  const validationError = getGeneratorValidationError(normalizedInput, { mode: 'queue' });
  if (validationError) {
    logGeneratorDebug('api.publishing.generator.rejected', {
      error: validationError,
      ...debugPayload
    });
    return res.status(400).json({ error: validationError });
  }

  try {
    const item = enqueueGeneratorPost(normalizedInput);
    logGeneratorDebug('api.publishing.generator.success', {
      queueId: item?.id ?? null,
      ...debugPayload
    });
    res.status(201).json({ item });
  } catch (error) {
    logGeneratorDebug('api.publishing.generator.error', {
      error: error instanceof Error ? error.message : 'Generator-Queue konnte nicht erstellt werden.',
      ...debugPayload
    });
    res.status(400).json({ error: error instanceof Error ? error.message : 'Generator-Queue konnte nicht erstellt werden.' });
  }
});

router.post('/workers/run', requireAdmin, async (req, res) => {
  try {
    const results = await runPublishingWorkers(req.body?.channelType || null);
    res.json({ items: results });
  } catch (error) {
    res.status(500).json({ error: error instanceof Error ? error.message : 'Worker konnten nicht gestartet werden.' });
  }
});

router.post('/queue/:id/retry', requireAdmin, (req, res) => {
  res.json({ item: retryPublishingQueue(Number(req.params.id)) });
});

router.put('/facebook-worker', requireAdmin, (req, res) => {
  res.json({ item: saveFacebookWorkerSettings(req.body ?? {}) });
});

export default router;
