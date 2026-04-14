import { Router } from 'express';
import { getRepostSettings, saveRepostSettings } from '../services/dealHistoryService.js';

const router = Router();

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

export default router;
