import { Router } from 'express';
import {
  getAdvertisingDashboard,
  getAdvertisingModules,
  listAdvertisingHistory,
  pauseAdvertisingModule,
  runAdvertisingAdminSync,
  saveAdvertisingModule,
  triggerAdvertisingModuleTest
} from '../services/advertisingService.js';

const router = Router();

function getRequesterRole(req) {
  return String(req.headers['x-user-role'] || '').trim().toLowerCase();
}

function requireAdmin(req, res, next) {
  if (getRequesterRole(req) !== 'admin') {
    return res.status(403).json({ error: 'Nur Admin darf Werbung verwalten.' });
  }

  return next();
}

router.get('/dashboard', (req, res) => {
  res.json(getAdvertisingDashboard());
});

router.get('/modules', (req, res) => {
  res.json({ items: getAdvertisingModules() });
});

router.get('/history', (req, res) => {
  res.json(listAdvertisingHistory(Number(req.query?.limit || 30)));
});

router.put('/modules/:id', requireAdmin, (req, res) => {
  try {
    res.json({ item: saveAdvertisingModule(req.body ?? {}, Number(req.params.id)) });
  } catch (error) {
    res.status(400).json({
      error: error instanceof Error ? error.message : 'Werbemodul konnte nicht gespeichert werden.'
    });
  }
});

router.post('/modules/:id/test', requireAdmin, async (req, res) => {
  try {
    res.json({ item: await triggerAdvertisingModuleTest(Number(req.params.id)) });
  } catch (error) {
    res.status(400).json({
      error: error instanceof Error ? error.message : 'Testsendung konnte nicht erzeugt werden.'
    });
  }
});

router.post('/modules/:id/pause', requireAdmin, (req, res) => {
  try {
    res.json({
      item: pauseAdvertisingModule(Number(req.params.id), req.body?.paused !== false)
    });
  } catch (error) {
    res.status(400).json({
      error: error instanceof Error ? error.message : 'Status konnte nicht geaendert werden.'
    });
  }
});

router.post('/sync', requireAdmin, (req, res) => {
  try {
    res.json(runAdvertisingAdminSync());
  } catch (error) {
    res.status(400).json({
      error: error instanceof Error ? error.message : 'Werbung konnte nicht synchronisiert werden.'
    });
  }
});

export default router;
