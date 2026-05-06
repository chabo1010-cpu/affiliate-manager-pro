import { Router } from 'express';
import { getDealEngineDashboard } from '../services/dealEngine/dashboardService.js';
import { getDealEngineSettings, saveDealEngineSettings } from '../services/dealEngine/configService.js';
import { analyzeDealWithEngine, getDealEngineSamplePayload } from '../services/dealEngine/service.js';
import { listDealEngineRuns } from '../services/dealEngine/repositoryService.js';
import { deleteProductRule, listProductRules, saveProductRule } from '../services/productRulesService.js';

const router = Router();

function logUiRouteDone(route, startedAt, extra = {}) {
  const durationMs = Date.now() - startedAt;
  console.info('[UI_ROUTE_DONE]', { route, durationMs, ...extra });
  if (durationMs >= 800) {
    console.warn('[UI_ROUTE_SLOW]', { route, durationMs, ...extra });
  }
}

function respondWithTimedJson(route, getter, errorMessage) {
  return (req, res) => {
    const startedAt = Date.now();
    console.info('[UI_ROUTE_START]', { route });

    try {
      const payload = getter(req, res);
      if (!res.headersSent) {
        res.json(payload);
      }
      logUiRouteDone(route, startedAt);
    } catch (error) {
      const durationMs = Date.now() - startedAt;
      console.error('[UI_ROUTE_ERROR]', {
        route,
        durationMs,
        errorMessage: error instanceof Error ? error.message : errorMessage
      });
      if (!res.headersSent) {
        res.status(500).json({
          error: error instanceof Error ? error.message : errorMessage
        });
      }
    }
  };
}

function getRequesterRole(req) {
  return String(req.headers['x-user-role'] || '').trim().toLowerCase();
}

function requireAdmin(req, res, next) {
  if (getRequesterRole(req) !== 'admin') {
    return res.status(403).json({ error: 'Nur Admin darf die Deal-Engine Regler speichern.' });
  }

  return next();
}

router.get('/dashboard', respondWithTimedJson('/api/deal-engine/dashboard', () => getDealEngineDashboard(), 'Deal-Engine Dashboard konnte nicht geladen werden.'));

router.get('/settings', respondWithTimedJson('/api/deal-engine/settings', () => ({ item: getDealEngineSettings() }), 'Deal-Engine Settings konnten nicht geladen werden.'));

router.put('/settings', requireAdmin, (req, res) => {
  try {
    res.json({ item: saveDealEngineSettings(req.body ?? {}) });
  } catch (error) {
    res.status(400).json({
      error: error instanceof Error ? error.message : 'Deal-Engine Settings konnten nicht gespeichert werden.'
    });
  }
});

router.get('/product-rules', respondWithTimedJson('/api/deal-engine/product-rules', () => ({ items: listProductRules() }), 'Produkt-Regeln konnten nicht geladen werden.'));

router.post('/product-rules', requireAdmin, (req, res) => {
  try {
    res.status(201).json({ item: saveProductRule(req.body ?? {}) });
  } catch (error) {
    res.status(400).json({
      error: error instanceof Error ? error.message : 'Produkt-Regel konnte nicht gespeichert werden.'
    });
  }
});

router.put('/product-rules/:id', requireAdmin, (req, res) => {
  try {
    res.json({ item: saveProductRule(req.body ?? {}, Number(req.params.id)) });
  } catch (error) {
    res.status(400).json({
      error: error instanceof Error ? error.message : 'Produkt-Regel konnte nicht aktualisiert werden.'
    });
  }
});

router.patch('/product-rules/:id/active', requireAdmin, (req, res) => {
  try {
    res.json({
      item: saveProductRule(
        {
          active: Boolean(req.body?.active ?? req.body?.isActive)
        },
        Number(req.params.id)
      )
    });
  } catch (error) {
    res.status(400).json({
      error: error instanceof Error ? error.message : 'Produkt-Regel konnte nicht umgeschaltet werden.'
    });
  }
});

router.delete('/product-rules/:id', requireAdmin, (req, res) => {
  try {
    res.json({ item: deleteProductRule(Number(req.params.id)) });
  } catch (error) {
    res.status(400).json({
      error: error instanceof Error ? error.message : 'Produkt-Regel konnte nicht geloescht werden.'
    });
  }
});

router.get(
  '/runs',
  respondWithTimedJson(
    '/api/deal-engine/runs',
    (req) => listDealEngineRuns({ limit: req.query?.limit, decision: req.query?.decision }),
    'Deal-Engine Runs konnten nicht geladen werden.'
  )
);

router.get('/sample', respondWithTimedJson('/api/deal-engine/sample', () => ({ item: getDealEngineSamplePayload() }), 'Deal-Engine Sample konnte nicht geladen werden.'));

router.post('/analyze', async (req, res) => {
  try {
    const result = await analyzeDealWithEngine(req.body ?? {});
    res.status(201).json(result);
  } catch (error) {
    res.status(400).json({
      error: error instanceof Error ? error.message : 'Deal konnte nicht analysiert werden.'
    });
  }
});

export default router;
