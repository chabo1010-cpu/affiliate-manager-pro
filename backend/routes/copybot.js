import { Router } from 'express';
import { saveRepostSettings, getRepostSettings } from '../services/dealHistoryService.js';
import {
  deleteSource,
  getCopybotOverview,
  listCopybotLogs,
  listPricingRules,
  listReviewQueue,
  listSamplingRules,
  listSources,
  processImportedDeal,
  savePricingRule,
  saveSamplingRule,
  saveSource,
  setSourceActiveState,
  testSource,
  updateReviewDecision
} from '../services/copybotService.js';

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

router.get('/overview', (req, res) => {
  res.json(getCopybotOverview());
});

router.get('/sources', (req, res) => {
  const platform = typeof req.query.platform === 'string' ? req.query.platform : null;
  res.json({ items: listSources(platform) });
});

router.post('/sources', requireAdmin, (req, res) => {
  try {
    res.status(201).json({ items: saveSource(req.body ?? {}) });
  } catch (error) {
    res.status(400).json({ error: error instanceof Error ? error.message : 'Quelle konnte nicht gespeichert werden.' });
  }
});

router.put('/sources/:id', requireAdmin, (req, res) => {
  try {
    res.json({ items: saveSource(req.body ?? {}, Number(req.params.id)) });
  } catch (error) {
    res.status(400).json({ error: error instanceof Error ? error.message : 'Quelle konnte nicht aktualisiert werden.' });
  }
});

router.patch('/sources/:id/active', requireAdmin, (req, res) => {
  res.json({ item: setSourceActiveState(Number(req.params.id), Boolean(req.body?.isActive)) });
});

router.delete('/sources/:id', requireAdmin, (req, res) => {
  res.json({ item: deleteSource(Number(req.params.id)) });
});

router.post('/sources/:id/test', requireAdmin, (req, res) => {
  try {
    res.json({ item: testSource(Number(req.params.id), req.body ?? {}) });
  } catch (error) {
    res.status(400).json({ error: error instanceof Error ? error.message : 'Quellen-Test fehlgeschlagen.' });
  }
});

router.get('/pricing-rules', (req, res) => {
  res.json({ items: listPricingRules() });
});

router.post('/pricing-rules', requireAdmin, (req, res) => {
  try {
    res.status(201).json({ items: savePricingRule(req.body ?? {}) });
  } catch (error) {
    res.status(400).json({ error: error instanceof Error ? error.message : 'Preispruef-Logik konnte nicht gespeichert werden.' });
  }
});

router.put('/pricing-rules/:id', requireAdmin, (req, res) => {
  try {
    res.json({ items: savePricingRule(req.body ?? {}, Number(req.params.id)) });
  } catch (error) {
    res.status(400).json({ error: error instanceof Error ? error.message : 'Preispruef-Logik konnte nicht aktualisiert werden.' });
  }
});

router.get('/sampling-rules', (req, res) => {
  res.json({ items: listSamplingRules() });
});

router.post('/sampling-rules', requireAdmin, (req, res) => {
  try {
    res.status(201).json({ items: saveSamplingRule(req.body ?? {}) });
  } catch (error) {
    res.status(400).json({ error: error instanceof Error ? error.message : 'Sampling-Regel konnte nicht gespeichert werden.' });
  }
});

router.put('/sampling-rules/:id', requireAdmin, (req, res) => {
  try {
    res.json({ items: saveSamplingRule(req.body ?? {}, Number(req.params.id)) });
  } catch (error) {
    res.status(400).json({ error: error instanceof Error ? error.message : 'Sampling-Regel konnte nicht aktualisiert werden.' });
  }
});

router.get('/review-queue', (req, res) => {
  res.json({ items: listReviewQueue() });
});

router.post('/review-queue/:id/approve', requireAdmin, (req, res) => {
  try {
    res.json({ item: updateReviewDecision(Number(req.params.id), 'approve') });
  } catch (error) {
    res.status(400).json({ error: error instanceof Error ? error.message : 'Review-Freigabe fehlgeschlagen.' });
  }
});

router.post('/review-queue/:id/reject', requireAdmin, (req, res) => {
  try {
    res.json({ item: updateReviewDecision(Number(req.params.id), 'reject') });
  } catch (error) {
    res.status(400).json({ error: error instanceof Error ? error.message : 'Review-Verwerfen fehlgeschlagen.' });
  }
});

router.get('/logs', (req, res) => {
  res.json({ items: listCopybotLogs() });
});

router.post('/imports/process/:sourceId', requireAdmin, (req, res) => {
  try {
    res.json({ item: processImportedDeal(Number(req.params.sourceId), req.body ?? {}) });
  } catch (error) {
    res.status(400).json({ error: error instanceof Error ? error.message : 'Deal-Verarbeitung fehlgeschlagen.' });
  }
});

router.get('/settings', (req, res) => {
  const settings = getRepostSettings();
  res.json({
    copybotEnabled: settings.copybotEnabled,
    telegramCopyButtonText: settings.telegramCopyButtonText
  });
});

router.put('/settings', requireAdmin, (req, res) => {
  const saved = saveRepostSettings({
    copybotEnabled: req.body?.copybotEnabled
  });
  res.json({
    copybotEnabled: saved.copybotEnabled,
    telegramCopyButtonText: saved.telegramCopyButtonText
  });
});

export default router;
