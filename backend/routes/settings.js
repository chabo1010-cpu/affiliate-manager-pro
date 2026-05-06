import { Router } from 'express';
import { getDb } from '../db.js';
import { getConfigManagerEnvStructure, getConfigManagerSnapshot } from '../services/configManagerService.js';
import { getRepostSettings, saveRepostSettings } from '../services/dealHistoryService.js';
import { getAmazonAffiliateStatus } from '../services/amazonAffiliateService.js';
import {
  getCopybotRuntimeState,
  getCopybotStatusAudit
} from '../services/copybotControlService.js';
import { getTelegramUserReaderConfig } from '../env.js';
import { getTelegramBotClientConfig } from '../services/telegramBotClientService.js';
import { getWhatsappClientConfig } from '../services/whatsappClientService.js';
import { getWorkerStatus, getPublishingWorkerRuntimeStatus } from '../services/publisherService.js';
import { getAdvertisingSchedulerRuntimeStatus } from '../services/advertisingService.js';
import { getKeepaSchedulerRuntimeStatus } from '../services/keepaService.js';

const router = Router();
const db = getDb();

function nowIso() {
  return new Date().toISOString();
}

function cleanText(value = '') {
  return typeof value === 'string' ? value.trim() : '';
}

function maskValue(value = '', visibleStart = 3, visibleEnd = 2) {
  const trimmed = cleanText(value);
  if (!trimmed) {
    return '';
  }

  if (trimmed.length <= visibleStart + visibleEnd) {
    return `${trimmed.slice(0, 1)}***`;
  }

  return `${trimmed.slice(0, visibleStart)}***${trimmed.slice(-visibleEnd)}`;
}

async function resolveTelegramStatusSnapshot() {
  const readerConfig = getTelegramUserReaderConfig();
  const activeSessionRow =
    db
      .prepare(
        `
          SELECT
            name,
            status,
            last_connected_at,
            last_message_at
          FROM telegram_reader_sessions
          ORDER BY
            CASE WHEN status IN ('connected', 'active', 'watching') THEN 0 ELSE 1 END,
            updated_at DESC,
            id DESC
          LIMIT 1
        `
      )
      .get() || null;
  const sessionsRow =
    db
      .prepare(
        `
          SELECT
            COUNT(*) AS total_sessions,
            SUM(CASE WHEN status IN ('connected', 'active', 'watching') THEN 1 ELSE 0 END) AS active_sessions,
            MAX(last_connected_at) AS last_connected_at,
            MAX(last_message_at) AS last_message_at
          FROM telegram_reader_sessions
        `
      )
      .get() || {};
  const channelRow =
    db
      .prepare(
        `
          SELECT
            COUNT(*) AS total_channels,
            SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) AS active_channels
          FROM telegram_reader_channels
        `
      )
      .get() || {};
  const activeSessions = Number(sessionsRow.active_sessions || 0);
  const watchlistCount = Number(channelRow.active_channels || 0);
  const botConfig = getTelegramBotClientConfig();

  return {
    readerStatus: activeSessions > 0 ? 'aktiv' : readerConfig.enabled ? 'pausiert' : 'deaktiviert',
    readerEnabled: readerConfig.enabled === true,
    activeSessionName: cleanText(activeSessionRow?.name) || '',
    activeInputGroups: watchlistCount,
    totalInputGroups: Number(channelRow.total_channels || 0),
    activeSessions,
    savedSessions: Number(sessionsRow.total_sessions || 0),
    lastConnectedAt: activeSessionRow?.last_connected_at || sessionsRow.last_connected_at || null,
    lastMessageAt: activeSessionRow?.last_message_at || sessionsRow.last_message_at || null,
    botStatus: botConfig.enabled && botConfig.tokenConfigured ? 'aktiv' : botConfig.tokenConfigured ? 'bereit' : 'pruefen',
    botEnabled: botConfig.enabled,
    botTargets: Number(botConfig.effectiveTargets.length || 0),
    outputGroupsStatus: botConfig.effectiveTargets.length > 0 ? 'aktiv' : 'leer',
    userApiConfigured: Boolean(readerConfig.apiId && readerConfig.apiHash),
    loginMode: readerConfig.loginMode,
    phoneMasked: maskValue(readerConfig.phoneNumber, 3, 2)
  };
}

async function buildLiveStatusPayload() {
  const configSnapshot = getConfigManagerSnapshot();
  const amazonStatus = getAmazonAffiliateStatus();
  const workerStatus = getWorkerStatus();
  const publishingRuntime = getPublishingWorkerRuntimeStatus();
  const advertisingRuntime = getAdvertisingSchedulerRuntimeStatus();
  const keepaRuntime = getKeepaSchedulerRuntimeStatus();
  const whatsappConfig = getWhatsappClientConfig();
  const copybotRuntime = getCopybotRuntimeState();
  const copybotAudit = getCopybotStatusAudit();
  const repostSettings = getRepostSettings();
  const telegramStatus = await resolveTelegramStatusSnapshot();
  const lastPublishingError =
    db
      .prepare(
        `
          SELECT *
          FROM publishing_logs
          WHERE level IN ('warning', 'error')
          ORDER BY created_at DESC
          LIMIT 1
        `
      )
      .get() || null;
  const systemCounts =
    db
      .prepare(
        `
          SELECT
            (SELECT COUNT(*) FROM publishing_queue WHERE status IN ('pending', 'queued', 'retry', 'sending', 'processing')) AS open_queue_count,
            (SELECT COUNT(*) FROM telegram_reader_channels WHERE is_active = 1) AS active_reader_groups,
            (SELECT COUNT(*) FROM telegram_bot_targets WHERE is_active = 1 AND use_for_publishing = 1) AS active_bot_targets
        `
      )
      .get() || {};

  return {
    generatedAt: nowIso(),
    config: configSnapshot,
    telegram: telegramStatus,
    amazon: {
      creatorApiStatus:
        amazonStatus?.settings?.creatorApi?.configured === true
          ? amazonStatus?.latest?.success?.operation === 'CreatorGetItems' || amazonStatus?.latest?.success?.operation === 'CreatorToken'
            ? 'aktiv'
            : amazonStatus?.latest?.error?.operation === 'CreatorGetItems' || amazonStatus?.latest?.error?.operation === 'CreatorToken'
              ? 'fehler'
              : 'bereit'
          : 'pruefen',
      paapiStatus: amazonStatus?.connection?.apiStatus || 'unbekannt',
      creatorApiConfigured: amazonStatus?.settings?.creatorApi?.configured === true,
      paapiConfigured: amazonStatus?.settings?.configured === true,
      partnerTagMasked: amazonStatus?.settings?.partnerTagMasked || '',
      creatorPartnerTagMasked: amazonStatus?.settings?.creatorApi?.partnerTagMasked || '',
      lastSuccessfulRequest: amazonStatus?.overview?.lastSuccessfulFetch || amazonStatus?.latest?.success?.createdAt || null,
      lastErrorMessage: amazonStatus?.overview?.lastErrorMessage || amazonStatus?.latest?.error?.message || '',
      lastErrorAt: amazonStatus?.overview?.lastErrorAt || amazonStatus?.latest?.error?.createdAt || null
    },
    whatsapp: {
      clientStatus: whatsappConfig.enabled ? 'aktiv' : 'deaktiviert',
      endpointStatus: whatsappConfig.endpointConfigured ? 'vorhanden' : 'fehlt',
      senderStatus: whatsappConfig.senderConfigured ? 'gesetzt' : 'offen',
      retryLimit: whatsappConfig.retryLimit,
      sender: cleanText(whatsappConfig.sender),
      endpointMasked: maskValue(whatsappConfig.endpoint, 12, 0)
    },
    facebook: {
      workerStatus: workerStatus?.facebook?.enabled ? 'aktiv' : 'deaktiviert',
      sessionMode: workerStatus?.facebook?.sessionMode || 'persistent',
      retryLimit: Number(workerStatus?.facebook?.retryLimit || 0),
      defaultTargetMasked: maskValue(workerStatus?.facebook?.defaultTarget || '', 4, 0)
    },
    system: {
      backendOnline: true,
      backendStartedAt: new Date(Date.now() - Math.round(process.uptime() * 1000)).toISOString(),
      uptimeSeconds: Math.round(process.uptime()),
      queueWorkerActive: publishingRuntime.started === true,
      queueWorkerRunning: publishingRuntime.running === true,
      schedulerActive: keepaRuntime.started === true || advertisingRuntime.started === true,
      keepaSchedulerActive: keepaRuntime.started === true,
      advertisingSchedulerActive: advertisingRuntime.started === true,
      copybotStatus: copybotRuntime.enabled === true ? 'aktiv' : 'pausiert',
      openQueueCount: Number(systemCounts.open_queue_count || 0),
      activeReaderGroups: Number(systemCounts.active_reader_groups || 0),
      activeBotTargets: Number(systemCounts.active_bot_targets || 0),
      lastRestartAt: new Date(Date.now() - Math.round(process.uptime() * 1000)).toISOString(),
      lastError: cleanText(lastPublishingError?.message) || cleanText(amazonStatus?.overview?.lastErrorMessage),
      lastErrorAt: lastPublishingError?.created_at || amazonStatus?.overview?.lastErrorAt || null
    },
    copybot: {
      enabled: copybotRuntime.enabled === true,
      inputProcessing: copybotRuntime.enabled === true ? 'aktiv' : 'pausiert',
      queueProcessing: copybotRuntime.enabled === true ? 'aktiv' : 'pausiert',
      lastStatusChange: copybotAudit.lastChangedAt,
      changedBy: copybotAudit.changedBy,
      changedFrom: copybotAudit.source,
      reason: copybotRuntime.reason,
      cooldownEnabled: repostSettings.repostCooldownEnabled === true,
      cooldownHours: repostSettings.repostCooldownHours
    },
    security: {
      copyButtonTextMasked: maskValue(repostSettings.telegramCopyButtonText, 6, 0),
      envPathMasked: maskValue(configSnapshot?.envPath || '', 10, 0),
      dbPathMasked: maskValue(configSnapshot?.modules?.database?.dbPath || '', 12, 0)
    }
  };
}

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

router.get('/live-status', requireAdmin, async (req, res) => {
  try {
    const payload = await buildLiveStatusPayload();
    return res.json({
      success: true,
      item: payload
    });
  } catch (error) {
    return res.status(500).json({
      error: error instanceof Error ? error.message : 'Live Status konnte nicht geladen werden.'
    });
  }
});

export default router;
