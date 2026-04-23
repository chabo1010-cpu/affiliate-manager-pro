import { Router } from 'express';
import { getDealEngineDashboard } from '../services/dealEngine/dashboardService.js';
import { getDealEngineSettings, saveDealEngineSettings } from '../services/dealEngine/configService.js';
import { analyzeDealWithEngine, getDealEngineSamplePayload } from '../services/dealEngine/service.js';
import { listDealEngineRuns } from '../services/dealEngine/repositoryService.js';

const router = Router();

function getRequesterRole(req) {
  return String(req.headers['x-user-role'] || '').trim().toLowerCase();
}

function requireAdmin(req, res, next) {
  if (getRequesterRole(req) !== 'admin') {
    return res.status(403).json({ error: 'Nur Admin darf die Deal-Engine Regler speichern.' });
  }

  return next();
}

router.get('/dashboard', (req, res) => {
  res.json(getDealEngineDashboard());
});

router.get('/settings', (req, res) => {
  res.json({ item: getDealEngineSettings() });
});

router.put('/settings', requireAdmin, (req, res) => {
  try {
    res.json({ item: saveDealEngineSettings(req.body ?? {}) });
  } catch (error) {
    res.status(400).json({
      error: error instanceof Error ? error.message : 'Deal-Engine Settings konnten nicht gespeichert werden.'
    });
  }
});

router.get('/runs', (req, res) => {
  res.json(listDealEngineRuns({ limit: req.query?.limit, decision: req.query?.decision }));
});

router.get('/sample', (req, res) => {
  res.json({ item: getDealEngineSamplePayload() });
});

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
