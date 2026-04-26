import { Router } from 'express';
import { getDb } from '../db.js';
import { getTelegramUserReaderConfig } from '../env.js';
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
const db = getDb();

function maskPhoneNumber(value = '') {
  const raw = String(value || '').trim();
  if (!raw) {
    return '';
  }

  if (raw.length <= 4) {
    return raw;
  }

  return `${raw.slice(0, 3)}***${raw.slice(-2)}`;
}

function listSessionsFromDb() {
  return db
    .prepare(
      `
        SELECT *
        FROM telegram_reader_sessions
        ORDER BY updated_at DESC, id DESC
      `
    )
    .all()
    .map((row) => ({
      id: Number(row.id),
      name: row.name || '',
      status: row.status || 'disconnected',
      loginMode: row.login_mode || 'phone',
      phoneNumberMasked: maskPhoneNumber(row.phone_number),
      lastConnectedAt: row.last_connected_at || null,
      lastMessageAt: row.last_message_at || null,
      lastError: row.last_error || '',
      listenerActive: ['connected', 'active', 'watching'].includes(String(row.status || '').toLowerCase()),
      sessionPath: row.session_path || ''
    }));
}

function listWatchlistsFromDb() {
  return db
    .prepare(
      `
        SELECT
          c.*,
          s.name AS session_name
        FROM telegram_reader_channels c
        LEFT JOIN telegram_reader_sessions s ON s.id = c.session_id
        ORDER BY
          CASE WHEN c.slot_index IS NULL THEN 1 ELSE 0 END,
          c.slot_index ASC,
          c.channel_title COLLATE NOCASE ASC,
          c.id ASC
      `
    )
    .all()
    .map((row) => ({
      id: Number(row.id),
      sessionName: row.session_name || '',
      channelRef: row.channel_ref || '',
      channelTitle: row.channel_title || '',
      channelType: row.channel_type || 'group',
      isActive: row.is_active === 1,
      lastSeenMessageId: row.last_seen_message_id || '',
      lastSeenMessageAt: row.last_seen_message_at || null,
      lastCheckedAt: row.last_checked_at || null,
      slotIndex: row.slot_index ?? null
    }));
}

function buildUiStatusFallbackPayload() {
  const readerConfig = getTelegramUserReaderConfig();
  const sessions = listSessionsFromDb();
  const watchlists = listWatchlistsFromDb();
  const listenerSessions = sessions.filter((item) => item.listenerActive).length;
  const configured = Boolean(readerConfig.apiId && readerConfig.apiHash);

  return {
    configured,
    enabled: readerConfig.enabled === true,
    listenerActive: listenerSessions > 0,
    listenerSessions,
    sessionsCount: sessions.length,
    watchlistCount: watchlists.length,
    sessions,
    watchlists,
    channels: watchlists,
    sessionCount: sessions.length,
    activeSourceCount: watchlists.filter((item) => item.isActive).length,
    pendingLogins: [],
    lastPollAt: null,
    lastFoundMessageAt: null
  };
}

async function resolveUiStatusPayload() {
  const fallbackPayload = buildUiStatusFallbackPayload();

  try {
    const runtimePayload = await Promise.race([
      getTelegramUserClientStatus(),
      new Promise((_, reject) => {
        setTimeout(() => reject(new Error('status_timeout')), 1500);
      })
    ]);

    const runtimeSessions = Array.isArray(runtimePayload?.sessions) && runtimePayload.sessions.length
      ? runtimePayload.sessions
      : fallbackPayload.sessions;
    const runtimeWatchlists = Array.isArray(runtimePayload?.watchlists) && runtimePayload.watchlists.length
      ? runtimePayload.watchlists
      : Array.isArray(runtimePayload?.channels) && runtimePayload.channels.length
        ? runtimePayload.channels
      : fallbackPayload.channels;
    const runtimeListenerSessions = Number(runtimePayload?.listenerSessions || 0);

    return {
      ...fallbackPayload,
      ...(runtimePayload || {}),
      configured: fallbackPayload.configured || runtimePayload?.configured === true,
      listenerActive:
        typeof runtimePayload?.listenerActive === 'boolean'
          ? runtimePayload.listenerActive
          : runtimeListenerSessions > 0 || fallbackPayload.listenerActive === true,
      sessions: runtimeSessions,
      sessionsCount: runtimeSessions.length,
      sessionCount: runtimeSessions.length,
      watchlists: runtimeWatchlists,
      channels: runtimeWatchlists,
      watchlistCount: runtimeWatchlists.length,
      listenerSessions: runtimeListenerSessions > 0 ? runtimeListenerSessions : fallbackPayload.listenerSessions,
      activeSourceCount:
        typeof runtimePayload?.activeSourceCount === 'number'
          ? runtimePayload.activeSourceCount
          : fallbackPayload.activeSourceCount,
      pendingLogins: Array.isArray(runtimePayload?.pendingLogins) ? runtimePayload.pendingLogins : []
    };
  } catch {
    return fallbackPayload;
  }
}

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

router.get('/user-client/status', async (req, res) => {
  try {
    console.info('[UI_STATUS_ROUTE_HIT]', {
      route: '/api/telegram/user-client/status',
      requesterRole: getRequesterRole(req)
    });
    const sessions = listSessionsFromDb();
    const channels = listWatchlistsFromDb();
    console.info('[UI_SESSIONS_DB_COUNT]', {
      count: sessions.length
    });
    console.info('[UI_WATCHLIST_DB_COUNT]', {
      count: channels.length
    });
    const payload = await resolveUiStatusPayload();
    console.info('[UI_RESPONSE_PAYLOAD]', {
      configured: payload.configured === true,
      sessionsCount: Number(payload.sessionsCount || payload.sessionCount || 0),
      watchlistCount: Number(payload.watchlistCount || 0),
      listenerActive: payload.listenerActive === true,
      listenerSessions: Number(payload.listenerSessions || 0)
    });
    res.json(payload);
  } catch (error) {
    res.status(400).json({ error: error instanceof Error ? error.message : 'Status konnte nicht geladen werden.' });
  }
});

router.get('/user-client/sessions', (req, res) => {
  try {
    const sessions = listSessionsFromDb();
    res.json({
      sessions,
      sessionsCount: sessions.length
    });
  } catch (error) {
    res.status(400).json({ error: error instanceof Error ? error.message : 'Sessions konnten nicht geladen werden.' });
  }
});

router.get('/user-client/watchlists', (req, res) => {
  try {
    const watchlists = listWatchlistsFromDb();
    res.json({
      watchlists,
      watchlistCount: watchlists.length
    });
  } catch (error) {
    res.status(400).json({ error: error instanceof Error ? error.message : 'Watchlists konnten nicht geladen werden.' });
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
