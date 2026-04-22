import { Router } from 'express';
import { getDb } from '../db.js';
import {
  getStorageConfig,
  getTelegramUserReaderConfig
} from '../env.js';
import { getCopybotOverview } from '../services/copybotService.js';
import { getRepostSettings } from '../services/dealHistoryService.js';
import { getKeepaSettings } from '../services/keepaService.js';
import { getWorkerStatus, listPublishingLogs, listPublishingQueue } from '../services/publisherService.js';
import { getTelegramBotClientConfig } from '../services/telegramBotClientService.js';
import { getWhatsappClientConfig } from '../services/whatsappClientService.js';

const router = Router();
const db = getDb();

router.get('/', (req, res) => {
  const publishingQueue = listPublishingQueue();
  const publishingLogs = listPublishingLogs().slice(0, 6);
  const workerStatus = getWorkerStatus();
  const copybotOverview = getCopybotOverview();
  const repostSettings = getRepostSettings();
  const keepaSettings = getKeepaSettings();
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
  const openQueueCount = publishingQueue.filter((item) => ['pending', 'sending', 'retry'].includes(item.status)).length;
  const configuredTelegramBot = Boolean(telegramBotClient.tokenConfigured && telegramBotClient.effectiveTargets.length);
  const configuredTelegramReader = Boolean(telegramUserReaderConfig.apiId && telegramUserReaderConfig.apiHash);

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
        savedSessions: Number(sessionStats.total_sessions || 0),
        activeSessions: Number(sessionStats.active_sessions || 0),
        watchedChannels: Number(channelStats.active_channels || 0),
        sourceCount: Number(copybotOverview.activeTelegramSources || 0),
        lastMessageAt: sessionStats.last_message_at || null
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
        retryLimit: telegramBotClient.defaultRetryLimit
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
        queueSent: Number(whatsappTargetStatus?.sent || 0)
      },
      persistence: {
        dbPath: storageConfig.dbPath,
        queueEntries: publishingQueue.length,
        repostCooldownEnabled: Boolean(repostSettings.repostCooldownEnabled),
        repostCooldownHours: Number(repostSettings.repostCooldownHours || 0)
      }
    }
  });
});

export default router;
