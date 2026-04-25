import { Router } from 'express';
import { forceScanTelegramReader, resetTelegramReaderLastSeen } from '../services/telegramUserClientService.js';

const router = Router();

function getRequesterRole(req) {
  return String(req.headers['x-user-role'] || '').trim().toLowerCase();
}

function requireAdmin(req, res, next) {
  if (getRequesterRole(req) !== 'admin') {
    return res.status(403).json({ error: 'Nur Admin darf den Telegram Reader debuggen.' });
  }

  return next();
}

router.post('/reset-last-seen', requireAdmin, (req, res) => {
  try {
    res.json(resetTelegramReaderLastSeen(req.body ?? {}));
  } catch (error) {
    res.status(400).json({
      error: error instanceof Error ? error.message : 'lastSeen konnte nicht zurueckgesetzt werden.'
    });
  }
});

router.post('/force-scan', requireAdmin, async (req, res) => {
  try {
    res.json(await forceScanTelegramReader(req.body ?? {}));
  } catch (error) {
    res.status(400).json({
      error: error instanceof Error ? error.message : 'Force-Scan konnte nicht gestartet werden.'
    });
  }
});

export default router;
