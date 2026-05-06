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
import { getTelegramBotClientConfig, saveTelegramBotClientConfig } from '../services/telegramBotClientService.js';

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
  const limit = Number(req.query?.limit);
  res.json({
    items: listPublishingQueue({
      limit: Number.isFinite(limit) && limit > 0 ? limit : undefined
    })
  });
});

router.get('/logs', (req, res) => {
  const limit = Number(req.query?.limit);
  res.json({
    items: listPublishingLogs({
      limit: Number.isFinite(limit) && limit > 0 ? limit : undefined
    })
  });
});

router.get('/workers/status', (req, res) => {
  res.json(getWorkerStatus());
});

router.get('/telegram-bot-client', requireAdmin, (req, res) => {
  res.json({ item: getTelegramBotClientConfig() });
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
    const statusCode =
      error instanceof Error && typeof error.code === 'string' && error.code.startsWith('DEAL_LOCK_') ? 409 : 400;
    res.status(statusCode).json({
      error: error instanceof Error ? error.message : 'Generator-Queue konnte nicht erstellt werden.',
      dealLock: error instanceof Error && error.dealLock ? error.dealLock : null
    });
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

router.put('/telegram-bot-client', requireAdmin, (req, res) => {
  res.json({ item: saveTelegramBotClientConfig(req.body ?? {}) });
});

export default router;
