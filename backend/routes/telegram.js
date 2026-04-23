import { Router } from 'express';
import { sendTelegramPost } from '../services/telegramSenderService.js';
import {
  completeTelegramPhoneLogin,
  disconnectTelegramUserSession,
  getTelegramReaderGroupConfig,
  getTelegramUserClientStatus,
  listTelegramUserDialogs,
  saveTelegramReaderGroupConfig,
  startTelegramPhoneLogin,
  startTelegramQrLogin,
  submitTelegramQrPassword,
  syncTelegramWatchedMessages,
  unwatchTelegramDialog,
  watchTelegramDialog
} from '../services/telegramUserClientService.js';

const router = Router();

function getRequesterRole(req) {
  return String(req.headers['x-user-role'] || '').trim().toLowerCase();
}

function requireAdmin(req, res, next) {
  if (getRequesterRole(req) !== 'admin') {
    return res.status(403).json({ error: 'Nur Admin darf den Telegram User Client verwalten.' });
  }

  return next();
}

router.get('/send', (req, res) => {
  res.status(405).json({
    success: false,
    error: 'Diese Route akzeptiert nur POST /api/telegram/send',
    code: 'METHOD_NOT_ALLOWED'
  });
});

router.post('/send', async (req, res) => {
  try {
    const { text, imageUrl, rabattgutscheinCode, chatId } = req.body ?? {};

    if (!req.body || typeof req.body !== 'object') {
      return res.status(400).json({
        success: false,
        error: 'JSON-Body fehlt oder konnte nicht geparst werden',
        code: 'INVALID_JSON_BODY'
      });
    }

    const result = await sendTelegramPost({
      text,
      imageUrl,
      rabattgutscheinCode,
      chatId
    });

    return res.status(200).json({
      success: true,
      message: result.imageUrl
        ? 'Post erfolgreich mit Bild zu Telegram gesendet'
        : 'Post erfolgreich zu Telegram gesendet',
      data: result
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      error:
        error instanceof Error
          ? `Fehler beim Telegram-Versand: ${error.message}`
          : 'Fehler beim Versenden. Bitte versuchen Sie es spaeter erneut.',
      code: 'SEND_ERROR'
    });
  }
});

router.get('/user-client/status', requireAdmin, async (req, res) => {
  try {
    res.json(await getTelegramUserClientStatus());
  } catch (error) {
    res.status(400).json({ error: error instanceof Error ? error.message : 'Status konnte nicht geladen werden.' });
  }
});

router.get('/user-client/groups', requireAdmin, (req, res) => {
  try {
    res.json(
      getTelegramReaderGroupConfig({
        sessionName: req.query.sessionName
      })
    );
  } catch (error) {
    res.status(400).json({ error: error instanceof Error ? error.message : 'Gruppen konnten nicht geladen werden.' });
  }
});

router.put('/user-client/groups', requireAdmin, (req, res) => {
  try {
    res.json(
      saveTelegramReaderGroupConfig({
        sessionName: req.body?.sessionName,
        slotCount: req.body?.slotCount,
        items: req.body?.items
      })
    );
  } catch (error) {
    res.status(400).json({ error: error instanceof Error ? error.message : 'Gruppen konnten nicht gespeichert werden.' });
  }
});

router.post('/user-client/login/phone/start', requireAdmin, async (req, res) => {
  try {
    res.json(await startTelegramPhoneLogin(req.body ?? {}));
  } catch (error) {
    res.status(400).json({ error: error instanceof Error ? error.message : 'Telefon-Login konnte nicht gestartet werden.' });
  }
});

router.post('/user-client/login/phone/complete', requireAdmin, async (req, res) => {
  try {
    res.json(await completeTelegramPhoneLogin(req.body ?? {}));
  } catch (error) {
    res.status(400).json({ error: error instanceof Error ? error.message : 'Telefon-Login konnte nicht abgeschlossen werden.' });
  }
});

router.post('/user-client/login/qr/start', requireAdmin, async (req, res) => {
  try {
    res.json(await startTelegramQrLogin(req.body ?? {}));
  } catch (error) {
    res.status(400).json({ error: error instanceof Error ? error.message : 'QR-Login konnte nicht gestartet werden.' });
  }
});

router.post('/user-client/login/qr/password', requireAdmin, async (req, res) => {
  try {
    res.json(await submitTelegramQrPassword(req.body ?? {}));
  } catch (error) {
    res.status(400).json({ error: error instanceof Error ? error.message : 'QR-2FA konnte nicht uebergeben werden.' });
  }
});

router.post('/user-client/disconnect', requireAdmin, async (req, res) => {
  try {
    res.json(await disconnectTelegramUserSession(req.body ?? {}));
  } catch (error) {
    res.status(400).json({ error: error instanceof Error ? error.message : 'Session konnte nicht getrennt werden.' });
  }
});

router.get('/user-client/dialogs', requireAdmin, async (req, res) => {
  try {
    res.json({
      items: await listTelegramUserDialogs({
        sessionName: req.query.sessionName,
        limit: req.query.limit
      })
    });
  } catch (error) {
    res.status(400).json({ error: error instanceof Error ? error.message : 'Dialoge konnten nicht geladen werden.' });
  }
});

router.post('/user-client/channels/watch', requireAdmin, async (req, res) => {
  try {
    res.json({
      item: await watchTelegramDialog(req.body ?? {})
    });
  } catch (error) {
    res.status(400).json({ error: error instanceof Error ? error.message : 'Channel konnte nicht uebernommen werden.' });
  }
});

router.delete('/user-client/channels/:id', requireAdmin, (req, res) => {
  try {
    res.json(unwatchTelegramDialog({ channelId: req.params.id }));
  } catch (error) {
    res.status(400).json({ error: error instanceof Error ? error.message : 'Channel konnte nicht entfernt werden.' });
  }
});

router.get('/user-client/messages/sync', requireAdmin, async (req, res) => {
  try {
    res.json(
      await syncTelegramWatchedMessages({
        sessionName: req.query.sessionName,
        limit: req.query.limit
      })
    );
  } catch (error) {
    res.status(400).json({ error: error instanceof Error ? error.message : 'Neue Nachrichten konnten nicht gelesen werden.' });
  }
});

export default router;
