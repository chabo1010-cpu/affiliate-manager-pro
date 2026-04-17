import { Router } from 'express';
import {
  createKeepaRule,
  getKeepaSettingsView,
  getKeepaStatus,
  getKeepaUsageHistory,
  getKeepaUsageSummary,
  listKeepaAlerts,
  listKeepaResults,
  listKeepaRules,
  listKeepaUsageLogs,
  runKeepaManualSearch,
  saveKeepaSettings,
  sendKeepaTestAlert,
  testKeepaConnection,
  updateKeepaResult,
  updateKeepaRule
} from '../services/keepaService.js';
import {
  getFakeDropHistory,
  getFakeDropSettingsView,
  getFakeDropSummary,
  listFakeDropExamples,
  listFakeDropReviewQueue,
  recalculateFakeDropScores,
  saveFakeDropSettings,
  submitFakeDropReview
} from '../services/keepaFakeDropService.js';

const router = Router();

function getRequesterRole(req) {
  return String(req.headers['x-user-role'] || '').trim().toLowerCase();
}

function requireAdmin(req, res, next) {
  if (getRequesterRole(req) !== 'admin') {
    return res.status(403).json({ error: 'Nur Admin darf Keepa-Regeln und Keepa-Einstellungen aendern.' });
  }

  return next();
}

router.get('/status', async (req, res) => {
  try {
    res.json(await getKeepaStatus());
  } catch (error) {
    res.status(500).json({
      error: error instanceof Error ? error.message : 'Keepa-Status konnte nicht geladen werden.'
    });
  }
});

router.get('/settings', (req, res) => {
  res.json(getKeepaSettingsView());
});

router.put('/settings', requireAdmin, (req, res) => {
  try {
    res.json(saveKeepaSettings(req.body ?? {}));
  } catch (error) {
    res.status(400).json({
      error: error instanceof Error ? error.message : 'Keepa-Einstellungen konnten nicht gespeichert werden.'
    });
  }
});

router.post('/test-connection', requireAdmin, async (req, res) => {
  try {
    res.json(await testKeepaConnection());
  } catch (error) {
    res.status(400).json({
      error: error instanceof Error ? error.message : 'Keepa-Verbindungstest fehlgeschlagen.'
    });
  }
});

router.post('/manual-search', async (req, res) => {
  try {
    res.json(await runKeepaManualSearch(req.body ?? {}));
  } catch (error) {
    res.status(400).json({
      error: error instanceof Error ? error.message : 'Manuelle Keepa-Suche fehlgeschlagen.'
    });
  }
});

router.get('/results', (req, res) => {
  try {
    res.json(
      listKeepaResults({
        workflowStatus: req.query.workflowStatus,
        categoryId: req.query.categoryId,
        minDiscount: req.query.minDiscount,
        minDealScore: req.query.minDealScore,
        page: req.query.page,
        limit: req.query.limit
      })
    );
  } catch (error) {
    res.status(400).json({
      error: error instanceof Error ? error.message : 'Keepa-Ergebnisse konnten nicht geladen werden.'
    });
  }
});

router.get('/usage/summary', (req, res) => {
  try {
    res.json(getKeepaUsageSummary());
  } catch (error) {
    res.status(400).json({
      error: error instanceof Error ? error.message : 'Keepa-Usage-Zusammenfassung konnte nicht geladen werden.'
    });
  }
});

router.get('/usage/history', (req, res) => {
  try {
    res.json(
      getKeepaUsageHistory({
        days: req.query.days,
        module: req.query.module
      })
    );
  } catch (error) {
    res.status(400).json({
      error: error instanceof Error ? error.message : 'Keepa-Usage-Verlauf konnte nicht geladen werden.'
    });
  }
});

router.get('/usage/logs', (req, res) => {
  try {
    res.json(
      listKeepaUsageLogs({
        range: req.query.range,
        module: req.query.module,
        limit: req.query.limit
      })
    );
  } catch (error) {
    res.status(400).json({
      error: error instanceof Error ? error.message : 'Keepa-Usage-Logs konnten nicht geladen werden.'
    });
  }
});

router.get('/fake-drop/summary', (req, res) => {
  try {
    res.json(getFakeDropSummary());
  } catch (error) {
    res.status(400).json({
      error: error instanceof Error ? error.message : 'Fake-Drop-Zusammenfassung konnte nicht geladen werden.'
    });
  }
});

router.get('/fake-drop/history', (req, res) => {
  try {
    res.json(
      getFakeDropHistory({
        days: req.query.days
      })
    );
  } catch (error) {
    res.status(400).json({
      error: error instanceof Error ? error.message : 'Fake-Drop-Verlauf konnte nicht geladen werden.'
    });
  }
});

router.get('/fake-drop/review-queue', (req, res) => {
  try {
    res.json(
      listFakeDropReviewQueue({
        page: req.query.page,
        limit: req.query.limit,
        sellerType: req.query.sellerType,
        classification: req.query.classification,
        onlyUnlabeled: req.query.onlyUnlabeled,
        onlyOpen: req.query.onlyOpen
      })
    );
  } catch (error) {
    res.status(400).json({
      error: error instanceof Error ? error.message : 'Review Queue konnte nicht geladen werden.'
    });
  }
});

router.post('/fake-drop/review/:id', (req, res) => {
  try {
    res.json(submitFakeDropReview(Number(req.params.id), req.body ?? {}));
  } catch (error) {
    res.status(400).json({
      error: error instanceof Error ? error.message : 'Review konnte nicht gespeichert werden.'
    });
  }
});

router.get('/fake-drop/examples', (req, res) => {
  try {
    res.json(
      listFakeDropExamples({
        page: req.query.page,
        limit: req.query.limit,
        bucket: req.query.bucket,
        label: req.query.label,
        sellerType: req.query.sellerType,
        search: req.query.search
      })
    );
  } catch (error) {
    res.status(400).json({
      error: error instanceof Error ? error.message : 'Lern-Datenbank konnte nicht geladen werden.'
    });
  }
});

router.post('/fake-drop/recalculate', requireAdmin, (req, res) => {
  try {
    res.json(recalculateFakeDropScores());
  } catch (error) {
    res.status(400).json({
      error: error instanceof Error ? error.message : 'Fake-Drop-Neuberechnung fehlgeschlagen.'
    });
  }
});

router.get('/fake-drop/settings', (req, res) => {
  try {
    res.json(getFakeDropSettingsView());
  } catch (error) {
    res.status(400).json({
      error: error instanceof Error ? error.message : 'Fake-Drop-Einstellungen konnten nicht geladen werden.'
    });
  }
});

router.patch('/fake-drop/settings', requireAdmin, (req, res) => {
  try {
    res.json(saveFakeDropSettings(req.body ?? {}));
  } catch (error) {
    res.status(400).json({
      error: error instanceof Error ? error.message : 'Fake-Drop-Einstellungen konnten nicht gespeichert werden.'
    });
  }
});

router.patch('/results/:id', (req, res) => {
  try {
    res.json(updateKeepaResult(Number(req.params.id), req.body ?? {}));
  } catch (error) {
    res.status(400).json({
      error: error instanceof Error ? error.message : 'Keepa-Treffer konnte nicht aktualisiert werden.'
    });
  }
});

router.get('/rules', (req, res) => {
  res.json({ items: listKeepaRules() });
});

router.post('/rules', requireAdmin, (req, res) => {
  try {
    res.status(201).json(createKeepaRule(req.body ?? {}));
  } catch (error) {
    res.status(400).json({
      error: error instanceof Error ? error.message : 'Keepa-Regel konnte nicht erstellt werden.'
    });
  }
});

router.patch('/rules/:id', requireAdmin, (req, res) => {
  try {
    res.json(updateKeepaRule(Number(req.params.id), req.body ?? {}));
  } catch (error) {
    res.status(400).json({
      error: error instanceof Error ? error.message : 'Keepa-Regel konnte nicht aktualisiert werden.'
    });
  }
});

router.get('/alerts', (req, res) => {
  res.json(listKeepaAlerts({ limit: req.query.limit }));
});

router.post('/alerts/test', requireAdmin, async (req, res) => {
  try {
    res.json(await sendKeepaTestAlert());
  } catch (error) {
    res.status(400).json({
      error: error instanceof Error ? error.message : 'Keepa-Test-Alert fehlgeschlagen.'
    });
  }
});

export default router;
