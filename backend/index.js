import './env.js';
import fs from 'fs';
import path from 'path';
import express from 'express';
import cors from 'cors';
import { getApiPort, getReaderRuntimeConfig, getTelegramTestGroupConfig, getTelegramUserReaderConfig } from './env.js';
import { attachAuthenticatedUser, requireAuthenticatedUser } from './middleware/auth.js';
import authRoutes from './routes/auth.js';
import botRoutes from './routes/bot.js';
import copybotRoutes from './routes/copybot.js';
import databaseRoutes from './routes/database.js';
import dealsRoutes from './routes/deals.js';
import postsRoutes from './routes/posts.js';
import logsRoutes from './routes/logs.js';
import amazonRoutes from './routes/amazon.js';
import telegramRoutes from './routes/telegram.js';
import telegramReaderDebugRoutes from './routes/telegramReaderDebug.js';
import testTelegramRoutes from './routes/testTelegram.js';
import publishingRoutes from './routes/publishing.js';
import settingsRoutes from './routes/settings.js';
import historyRoutes from './routes/history.js';
import keepaRoutes from './routes/keepa.js';
import learningRoutes from './routes/learning.js';
import dealEngineRoutes from './routes/dealEngine.js';
import advertisingRoutes from './routes/advertising.js';
import debugRoutes from './routes/debug.js';
import createSystemRoutes from './routes/system.js';
import { startKeepaScheduler } from './services/keepaService.js';
import { startPublishingWorkerLoop } from './services/publisherService.js';
import { startAdvertisingScheduler } from './services/advertisingService.js';
import { startTelegramUserReaderRuntime } from './services/telegramUserClientService.js';
import { startWhatsappHealthMonitor } from './services/whatsappRuntimeService.js';

const backendStartedAt = new Date().toISOString();
const restartManager = String(process.env.BACKEND_RESTART_MANAGER || '').trim().toLowerCase();
const nodemonRestartTriggerPath = path.join(process.cwd(), 'nodemon-restart-trigger.json');
let restartPending = false;
let server = null;

console.info('[BOOT_START]', {
  startedAt: backendStartedAt
});
console.info('[SERVER_RESTARTED]', {
  startedAt: backendStartedAt
});

function getRestartStatus() {
  return {
    enabled: restartManager === 'nodemon',
    pending: restartPending === true,
    manager: restartManager || 'unavailable'
  };
}

function getHealthPayload() {
  return {
    ok: true,
    restartManager: restartManager || 'unavailable',
    restartPending: restartPending === true,
    startedAt: backendStartedAt,
    uptimeSeconds: Math.round(process.uptime())
  };
}

function scheduleBackendRestart(meta = {}) {
  const restartStatus = getRestartStatus();

  if (restartStatus.enabled !== true) {
    return {
      accepted: false,
      reason: 'restart_manager_unavailable',
      manager: restartStatus.manager
    };
  }

  if (restartPending === true) {
    return {
      accepted: true,
      alreadyPending: true,
      manager: restartStatus.manager,
      reloadAfterMs: 3000
    };
  }

  restartPending = true;

  console.info('[SERVER_SHUTDOWN_INIT]', {
    requestedAt: meta.requestedAt || new Date().toISOString(),
    requesterRole: meta.requesterRole || 'unknown',
    source: meta.source || 'manual',
    restartManager: restartStatus.manager,
    shutdownDelayMs: 200,
    restartTriggerFile: nodemonRestartTriggerPath
  });

  const shutdownDelay = setTimeout(() => {
    try {
      fs.writeFileSync(
        nodemonRestartTriggerPath,
        JSON.stringify(
          {
            requestedAt: meta.requestedAt || new Date().toISOString(),
            source: meta.source || 'manual',
            requesterRole: meta.requesterRole || 'unknown',
            triggeredAt: new Date().toISOString()
          },
          null,
          2
        ),
        'utf8'
      );
    } catch (error) {
      restartPending = false;
      console.warn('[NO_POST_REASON]', {
        reason: 'Backend Restart Trigger Fehler',
        detail: error instanceof Error ? error.message : 'Nodemon Restart-Datei konnte nicht geschrieben werden.'
      });
      return;
    }

    console.info('[SERVER_RESTART_TRIGGERED]', {
      triggeredAt: new Date().toISOString(),
      triggerPath: nodemonRestartTriggerPath,
      restartManager: restartStatus.manager
    });
  }, 200);

  shutdownDelay.unref?.();

  return {
    accepted: true,
    manager: restartStatus.manager,
    reloadAfterMs: 3000
  };
}

const app = express();
const port = getApiPort();
const readerRuntimeConfig = getReaderRuntimeConfig();
const telegramUserReaderConfig = getTelegramUserReaderConfig();
const telegramTestGroupConfig = getTelegramTestGroupConfig();

console.info('[BOOT_ENV_FLAGS]', {
  port,
  readerTestMode: readerRuntimeConfig.readerTestMode === true,
  readerDebugMode: readerRuntimeConfig.readerDebugMode === true,
  allowRawReaderFallback: readerRuntimeConfig.allowRawReaderFallback === true,
  dealLockBypass: readerRuntimeConfig.dealLockBypass === true,
  telegramUserReaderEnabled: telegramUserReaderConfig.enabled === true,
  telegramUserApiConfigured: Boolean(telegramUserReaderConfig.apiId && telegramUserReaderConfig.apiHash),
  telegramSessionDirConfigured: Boolean(telegramUserReaderConfig.sessionDir),
  telegramTestChatConfigured: Boolean(telegramTestGroupConfig.chatId),
  telegramBotTokenConfigured: Boolean(telegramTestGroupConfig.token)
});

const allowedOrigins = new Set([
  'http://localhost:5173',
  'http://127.0.0.1:5173',
  'http://localhost:5174',
  'http://127.0.0.1:5174',
  'http://localhost:4173',
  'http://127.0.0.1:4173'
]);

app.use(
  cors({
    credentials: true,
    origin(origin, callback) {
      if (!origin || allowedOrigins.has(origin)) {
        callback(null, true);
        return;
      }

      callback(new Error(`CORS blockiert Origin: ${origin}`));
    }
  })
);
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ limit: '10mb', extended: true }));
app.use(attachAuthenticatedUser);

app.get('/api/health', (req, res) => {
  res.json(getHealthPayload());
});

app.use('/api/auth', authRoutes);
app.use('/api', requireAuthenticatedUser);
app.use('/api/bot', botRoutes);
app.use('/api/copybot', copybotRoutes);
app.use('/api/database', databaseRoutes);
app.use('/api/deals', dealsRoutes);
app.use('/api/settings', settingsRoutes);
app.use('/api/history', historyRoutes);
app.use('/api/posts', postsRoutes);
app.use('/api/logs', logsRoutes);
app.use('/api', amazonRoutes);
app.use('/api/amazon', amazonRoutes);
app.use('/api/telegram', telegramRoutes);
app.use('/api/telegram-reader', telegramReaderDebugRoutes);
app.use('/api', testTelegramRoutes);
app.use('/api/publishing', publishingRoutes);
app.use('/api/keepa', keepaRoutes);
app.use('/api/learning', learningRoutes);
app.use('/api/deal-engine', dealEngineRoutes);
app.use('/api/advertising', advertisingRoutes);
app.use('/api/debug', debugRoutes);
app.use(
  '/api/system',
  createSystemRoutes({
    getHealthPayload,
    getRestartStatus,
    scheduleBackendRestart
  })
);

console.info('[ROUTES_MOUNTED]', {
  mounted: [
    '/api/health',
    '/api/auth',
    '/api/bot',
    '/api/copybot',
    '/api/database',
    '/api/deals',
    '/api/settings',
    '/api/history',
    '/api/posts',
    '/api/logs',
    '/api',
    '/api/amazon',
    '/api/telegram',
    '/api/telegram-reader',
    '/api/publishing',
    '/api/keepa',
    '/api/learning',
    '/api/deal-engine',
    '/api/advertising',
    '/api/debug',
    '/api/system'
  ]
});

startKeepaScheduler();
startPublishingWorkerLoop();
startWhatsappHealthMonitor();
startAdvertisingScheduler();
void startTelegramUserReaderRuntime().catch((error) => {
  console.warn('[NO_POST_REASON]', {
    reason: 'Reader nicht aktiv',
    detail: error instanceof Error ? error.message : 'Telegram runtime bootstrap failed.'
  });
  console.error('Telegram runtime bootstrap failed', error);
});

app.get('/', (req, res) => {
  res.json({ status: 'Affiliate Manager Pro API laeuft', version: '0.1.0' });
});

console.info('[PORT_BIND_ATTEMPT]', {
  port
});

server = app.listen(port, () => {
  if (String(port) === '4000') {
    console.info('[PORT_BOUND_4000_OK]', {
      port: 4000
    });
  }
  console.info('[SERVER_READY]', {
    url: `http://localhost:${port}`,
    port
  });
  console.log(`Affiliate Manager Pro Backend laeuft auf http://localhost:${port}`);
});

server.on('error', (error) => {
  console.warn('[NO_POST_REASON]', {
    reason: 'Backend läuft nicht',
    detail: error instanceof Error ? error.message : 'Backend konnte den Port nicht binden.'
  });
  console.error('[BOOT_FATAL_ERROR]', {
    port,
    reason: error instanceof Error ? error.message : 'Backend konnte den Port nicht binden.'
  });
});
