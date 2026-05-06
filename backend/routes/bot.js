import { Router } from 'express';
import { getDb } from '../db.js';
import {
  getStorageConfig,
  getTelegramUserReaderConfig
} from '../env.js';
import { getCopybotOverview } from '../services/copybotService.js';
import { getRepostSettings } from '../services/dealHistoryService.js';
import { getDealEngineSettings } from '../services/dealEngine/configService.js';
import { getAdvertisingSchedulerRuntimeStatus } from '../services/advertisingService.js';
import { getKeepaSchedulerRuntimeStatus, getKeepaSettings } from '../services/keepaService.js';
import {
  getPublishingQueueCounts,
  getPublishingWorkerRuntimeStatus,
  getWorkerStatus,
  listPublishingLogs
} from '../services/publisherService.js';
import { getTelegramBotClientConfig } from '../services/telegramBotClientService.js';
import { getWhatsappClientConfig } from '../services/whatsappClientService.js';

const router = Router();
const db = getDb();

function buildOperationalModule(status, label, detail, extra = {}) {
  return {
    status,
    label,
    detail,
    ...extra
  };
}

function logUiRouteDone(route, startedAt, extra = {}) {
  const durationMs = Date.now() - startedAt;
  console.info('[UI_ROUTE_DONE]', { route, durationMs, ...extra });
  if (durationMs >= 800) {
    console.warn('[UI_ROUTE_SLOW]', { route, durationMs, ...extra });
  }
}

router.get('/', (req, res) => {
  const route = '/api/bot';
  const startedAt = Date.now();
  console.info('[UI_ROUTE_START]', { route });

  try {
    const publishingLogs = listPublishingLogs({ limit: 6 });
    const publishingQueueCounts = getPublishingQueueCounts();
    const workerStatus = getWorkerStatus();
    const copybotOverview = getCopybotOverview();
    const repostSettings = getRepostSettings();
    const keepaSettings = getKeepaSettings();
    const dealEngineSettings = getDealEngineSettings();
    const keepaSchedulerRuntime = getKeepaSchedulerRuntimeStatus();
    const publishingWorkerRuntime = getPublishingWorkerRuntimeStatus();
    const advertisingSchedulerRuntime = getAdvertisingSchedulerRuntimeStatus();
    const storageConfig = getStorageConfig();
    const telegramBotClient = getTelegramBotClientConfig();
    const telegramUserReaderConfig = getTelegramUserReaderConfig();
    const whatsappConfig = getWhatsappClientConfig();
    const telegramTargetStatus = workerStatus.channels.find((item) => item.channel_type === 'telegram') || null;
    const whatsappTargetStatus = workerStatus.channels.find((item) => item.channel_type === 'whatsapp') || null;
    const sessionStats =
      db.prepare(
        `
          SELECT
            COUNT(*) AS total_sessions,
            SUM(CASE WHEN status IN ('connected', 'active', 'watching') THEN 1 ELSE 0 END) AS active_sessions,
            MAX(last_message_at) AS last_message_at
          FROM telegram_reader_sessions
        `
      ).get() || {};
    const channelStats =
      db.prepare(
        `
          SELECT
            COUNT(*) AS total_channels,
            SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) AS active_channels
          FROM telegram_reader_channels
        `
      ).get() || {};
    const openQueueCount = publishingQueueCounts.openCount;
    const configuredTelegramBot = Boolean(telegramBotClient.tokenConfigured && telegramBotClient.effectiveTargets.length);
    const configuredTelegramReader = Boolean(telegramUserReaderConfig.apiId && telegramUserReaderConfig.apiHash);
    const readerActiveSessions = Number(sessionStats.active_sessions || 0);
    const readerSavedSessions = Number(sessionStats.total_sessions || 0);
    const readerWatchedChannels = Number(channelStats.active_channels || 0);
    const whatsappPrepared =
      whatsappConfig.enabled ||
      whatsappConfig.endpointConfigured ||
      whatsappConfig.senderConfigured ||
      Number(copybotOverview.activeWhatsappSources || 0) > 0;
    const whatsappLive = Boolean(whatsappConfig.enabled && whatsappConfig.endpointConfigured && whatsappConfig.senderConfigured);
    const telegramReaderStatus =
      readerActiveSessions > 0 ? 'active' : configuredTelegramReader && telegramUserReaderConfig.enabled ? 'session_missing' : 'prepared';
    const telegramReaderLabel =
      telegramReaderStatus === 'active'
        ? 'aktiv'
        : telegramReaderStatus === 'session_missing'
          ? 'Session fehlt'
          : 'vorbereitet';
    const telegramBotStatus = configuredTelegramBot && telegramBotClient.enabled ? 'active' : 'prepared';
    const telegramBotLabel = telegramBotStatus === 'active' ? 'aktiv' : 'vorbereitet';
    const whatsappStatus = whatsappLive ? 'active' : whatsappPrepared ? 'prepared' : 'not_configured';
    const whatsappLabel = whatsappStatus === 'active' ? 'aktiv' : whatsappStatus === 'prepared' ? 'vorbereitet' : 'nicht konfiguriert';
    const aiResolverStatus = dealEngineSettings.ai.resolverEnabled ? 'active' : 'disabled';
    const aiResolverLabel = aiResolverStatus === 'active' ? 'aktiv' : 'deaktiviert';
    const schedulerFullyActive =
      keepaSettings.schedulerEnabled &&
      keepaSchedulerRuntime.started &&
      publishingWorkerRuntime.started &&
      advertisingSchedulerRuntime.started;
    const schedulerLabel = schedulerFullyActive ? 'aktiv' : 'deaktiviert';

  const operationalStatus = {
    telegramReader: buildOperationalModule(
      telegramReaderStatus,
      telegramReaderLabel,
      readerActiveSessions > 0
        ? `${readerActiveSessions} aktive Arbeits-Session${readerActiveSessions === 1 ? '' : 's'} | ${readerWatchedChannels} Watches aktiv`
        : configuredTelegramReader && telegramUserReaderConfig.enabled
          ? 'Reader ist vorbereitet, aber ohne aktive Telegram User Session noch nicht live.'
          : 'Reader ist technisch vorbereitet, benoetigt aber API-Daten und eine echte Arbeits-Session.',
      {
        prepared: true,
        live: readerActiveSessions > 0,
        requiresSession: true,
        enabled: telegramUserReaderConfig.enabled,
        configured: configuredTelegramReader,
        loginMode: telegramUserReaderConfig.loginMode,
        phoneNumberConfigured: Boolean(telegramUserReaderConfig.phoneNumber),
        savedSessions: readerSavedSessions,
        activeSessions: readerActiveSessions,
        watchedChannels: readerWatchedChannels
      }
    ),
    telegramBot: buildOperationalModule(
      telegramBotStatus,
      telegramBotLabel,
      configuredTelegramBot && telegramBotClient.enabled
        ? `${telegramBotClient.effectiveTargets.length} Ziel${telegramBotClient.effectiveTargets.length === 1 ? '' : 'e'} aktiv fuer Publishing`
        : 'Bot ist vorbereitet und wird live, sobald Token und mindestens ein Publishing-Ziel aktiv sind.',
      {
        prepared: true,
        live: configuredTelegramBot && telegramBotClient.enabled,
        enabled: telegramBotClient.enabled,
        configured: configuredTelegramBot,
        targetCount: telegramBotClient.effectiveTargets.length
      }
    ),
    whatsapp: buildOperationalModule(
      whatsappStatus,
      whatsappLabel,
      whatsappLive
        ? 'WhatsApp ist technisch angebunden, bleibt aber als optionale Output-Strecke markiert.'
        : whatsappPrepared
          ? 'WhatsApp ist vorbereitet und optional, aber ohne belastbare Produktiv-Anbindung noch nicht voll live.'
          : 'WhatsApp ist noch nicht produktiv angebunden und benoetigt Endpoint, Sender und echte Zugangsdaten.',
      {
        prepared: true,
        optional: true,
        live: whatsappLive,
        enabled: whatsappConfig.enabled,
        endpointConfigured: whatsappConfig.endpointConfigured,
        senderConfigured: whatsappConfig.senderConfigured,
        configured: whatsappConfig.endpointConfigured && whatsappConfig.senderConfigured,
        activeSources: Number(copybotOverview.activeWhatsappSources || 0)
      }
    ),
    aiResolver: buildOperationalModule(
      aiResolverStatus,
      aiResolverLabel,
      dealEngineSettings.ai.resolverEnabled
        ? 'KI bleibt optional und greift nur bei Unsicherheit nach Marktvergleich, Keepa und Fake-Pattern ein.'
        : 'KI ist vorbereitet, aber deaktiviert. Das System bleibt ohne KI voll lauffaehig.',
      {
        prepared: true,
        optional: true,
        live: dealEngineSettings.ai.resolverEnabled,
        enabled: dealEngineSettings.ai.resolverEnabled
      }
    ),
    scheduler: buildOperationalModule(
      schedulerFullyActive ? 'active' : 'disabled',
      schedulerLabel,
      `Deals ${keepaSettings.schedulerEnabled && keepaSchedulerRuntime.started ? 'aktiv' : 'deaktiviert'} | Queue Worker ${
        publishingWorkerRuntime.started ? 'aktiv' : 'deaktiviert'
      } | Werbung ${advertisingSchedulerRuntime.started ? 'aktiv' : 'deaktiviert'}`,
      {
        live: schedulerFullyActive,
        enabled: keepaSettings.schedulerEnabled,
        keepaScheduler: keepaSchedulerRuntime,
        queueWorker: publishingWorkerRuntime,
        advertisingScheduler: advertisingSchedulerRuntime
      }
    )
  };

  const productionReality = {
    live: [
      'Internetvergleich als Hauptentscheidung',
      'Keepa nur als Fallback',
      'Fake-Pattern-Erkennung im Hauptpfad',
      publishingWorkerRuntime.started ? 'Queue Worker mit Retry und Recovery' : null,
      repostSettings.repostCooldownEnabled ? 'Sperrmodul fuer manuelle und automatische Posts' : null
    ].filter(Boolean),
    prepared: [
      'Telegram Reader ist vorbereitet und braucht fuer Live-Betrieb eine echte User Session',
      'WhatsApp bleibt optional und vorbereitet, bis eine belastbare Produktiv-Anbindung vorhanden ist',
      'AI Resolver ist vorbereitet und nur fuer Unsicherheitsfaelle vorgesehen'
    ],
    blocked: [
      telegramReaderStatus !== 'active' ? 'Telegram Reader hat noch keine aktive Arbeits-Session' : null,
      whatsappLive ? null : 'WhatsApp ist noch nicht als echte Produktiv-Strecke bestaetigt'
    ].filter(Boolean)
  };

  const finalFlow = [
    {
      id: 'reader',
      label: 'Reader und Quellen',
      status: telegramReaderLabel,
      detail: 'Telegram Reader bleibt ohne aktive User Session vorbereitet. WhatsApp bleibt optional vorbereitet.'
    },
    {
      id: 'market',
      label: 'Internetvergleich',
      status: 'aktiv',
      detail: 'Der guenstigste echte Marktpreis entscheidet immer zuerst.'
    },
    {
      id: 'keepa',
      label: 'Keepa Fallback',
      status: keepaSettings.keepaEnabled ? 'aktiv' : 'deaktiviert',
      detail: 'Keepa greift nur, wenn kein brauchbarer Marktpreis vorhanden ist.'
    },
    {
      id: 'fake-pattern',
      label: 'Fake-Pattern',
      status: 'aktiv',
      detail: 'Spikes, Coupon-Effekte und Sparse-History werden vor der finalen Entscheidung geprueft.'
    },
    {
      id: 'ai',
      label: 'AI Resolver',
      status: aiResolverLabel,
      detail: 'Optional und nur bei Unsicherheitsfaellen nach Marktvergleich, Keepa und Fake-Pattern.'
    },
    {
      id: 'decision',
      label: 'Finale Entscheidung',
      status: 'aktiv',
      detail: 'APPROVE, QUEUE oder REJECT werden erst nach allen Pflichtpruefungen gesetzt.'
    },
    {
      id: 'outbox',
      label: 'Queue und Publisher',
      status: publishingWorkerRuntime.started ? 'aktiv' : 'deaktiviert',
      detail: 'APPROVE geht in die Queue, danach ueber Publisher, Retry, Recovery und Sperrmodul weiter.'
    }
  ];

    res.json({
      status: openQueueCount > 0 ? 'aktiv' : configuredTelegramBot || configuredTelegramReader ? 'bereit' : 'konfiguration_noetig',
      queue: openQueueCount,
      lastCheck: new Date().toISOString(),
      activities: publishingLogs.map((entry) => ({
        id: entry.id,
        action: entry.message || entry.event_type || 'Publishing Event',
        user: entry.worker_type || 'system',
        time: entry.created_at
      })),
      modules: {
      telegramUserApi: {
        enabled: telegramUserReaderConfig.enabled,
        apiConfigured: configuredTelegramReader,
        loginMode: telegramUserReaderConfig.loginMode,
        sessionDir: telegramUserReaderConfig.sessionDir,
        savedSessions: readerSavedSessions,
        activeSessions: readerActiveSessions,
        watchedChannels: readerWatchedChannels,
        sourceCount: Number(copybotOverview.activeTelegramSources || 0),
        lastMessageAt: sessionStats.last_message_at || null,
        runtimeStatus: operationalStatus.telegramReader
      },
      telegramBotApi: {
        configured: configuredTelegramBot,
        queuePending: Number(telegramTargetStatus?.pending || 0),
        queueSending: Number(telegramTargetStatus?.sending || 0),
        queueWaiting: Number(telegramTargetStatus?.waiting || 0),
        queueProcessing: Number(telegramTargetStatus?.processing || 0),
        queueFailed: Number(telegramTargetStatus?.failed || 0),
        queueSent: Number(telegramTargetStatus?.sent || 0),
        targetChatConfigured: Boolean(telegramBotClient.effectiveTargets.length),
        configuredTargets: telegramBotClient.targets.length,
        publishTargets: telegramBotClient.effectiveTargets.length,
        retryLimit: telegramBotClient.defaultRetryLimit,
        runtimeStatus: operationalStatus.telegramBot
      },
      keepaFallback: {
        enabled: Boolean(keepaSettings.keepaEnabled),
        fallbackOnly: true,
        minMarketGapPct: Number(keepaSettings.strongDealMinComparisonGapPct || 0)
      },
      whatsapp: {
        configured: Boolean((whatsappConfig.enabled && whatsappConfig.endpointConfigured) || copybotOverview.activeWhatsappSources),
        retryLimit: Number(whatsappConfig.retryLimit || 0),
        activeSources: Number(copybotOverview.activeWhatsappSources || 0),
        endpointConfigured: Boolean(whatsappConfig.endpointConfigured),
        senderConfigured: Boolean(whatsappConfig.senderConfigured),
        queuePending: Number(whatsappTargetStatus?.pending || 0),
        queueSending: Number(whatsappTargetStatus?.sending || 0),
        queueWaiting: Number(whatsappTargetStatus?.waiting || 0),
        queueFailed: Number(whatsappTargetStatus?.failed || 0),
        queueSent: Number(whatsappTargetStatus?.sent || 0),
        runtimeStatus: operationalStatus.whatsapp
      },
        persistence: {
          dbPath: storageConfig.dbPath,
          queueEntries: publishingQueueCounts.totalCount,
          repostCooldownEnabled: Boolean(repostSettings.repostCooldownEnabled),
          repostCooldownHours: Number(repostSettings.repostCooldownHours || 0)
        }
      },
      operationalStatus,
      productionReality,
      finalFlow
    });

    logUiRouteDone(route, startedAt, {
      queueOpenCount: openQueueCount,
      queueEntries: publishingQueueCounts.totalCount
    });
  } catch (error) {
    const durationMs = Date.now() - startedAt;
    console.error('[UI_ROUTE_ERROR]', {
      route,
      durationMs,
      errorMessage: error instanceof Error ? error.message : 'Bot-Status konnte nicht geladen werden.'
    });
    res.status(500).json({
      error: error instanceof Error ? error.message : 'Bot-Status konnte nicht geladen werden.'
    });
  }
});

export default router;
