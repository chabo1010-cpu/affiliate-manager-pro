import { Router } from 'express';
import { getConfigManagerEnvStructure, getConfigManagerSnapshot } from '../services/configManagerService.js';
import { getRepostSettings, saveRepostSettings } from '../services/dealHistoryService.js';

const router = Router();

function getRequesterRole(req) {
  return String(req.headers['x-user-role'] || '').trim().toLowerCase();
}

function requireAdmin(req, res, next) {
  if (getRequesterRole(req) !== 'admin') {
    return res.status(403).json({ error: 'Nur Admin darf den Config Manager einsehen.' });
  }

  return next();
}

router.get('/', (req, res) => {
  try {
    const settings = getRepostSettings();
    return res.json({
      success: true,
      repostCooldownEnabled: settings.repostCooldownEnabled,
      repostCooldownHours: settings.repostCooldownHours,
      telegramCopyButtonText: settings.telegramCopyButtonText,
      copybotEnabled: settings.copybotEnabled
    });
  } catch (error) {
    return res.status(500).json({
      error: error instanceof Error ? error.message : 'Failed to load settings'
    });
  }
});

router.post('/repost-cooldown', (req, res) => {
  try {
    const { enabled, hours } = req.body ?? {};
    const saved = saveRepostSettings({
      repostCooldownEnabled: enabled,
      repostCooldownHours: hours
    });

    return res.json({
      success: true,
      enabled: saved.repostCooldownEnabled,
      hours: saved.repostCooldownHours
    });
  } catch (error) {
    return res.status(500).json({
      error: error instanceof Error ? error.message : 'Failed to save settings'
    });
  }
});

router.get('/config-manager', requireAdmin, (req, res) => {
  try {
    return res.json({
      success: true,
      item: getConfigManagerSnapshot()
    });
  } catch (error) {
    return res.status(500).json({
      error: error instanceof Error ? error.message : 'Config Manager konnte nicht geladen werden.'
    });
  }
});

router.get('/config-manager/env-structure', requireAdmin, (req, res) => {
  try {
    return res.json({
      success: true,
      sections: getConfigManagerEnvStructure()
    });
  } catch (error) {
    return res.status(500).json({
      error: error instanceof Error ? error.message : 'ENV Struktur konnte nicht geladen werden.'
    });
  }
});

export default router;
