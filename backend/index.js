import './env.js';
import express from 'express';
import cors from 'cors';
import { getApiPort } from './env.js';
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
import { startKeepaScheduler } from './services/keepaService.js';
import { startPublishingWorkerLoop } from './services/publisherService.js';
import { startAdvertisingScheduler } from './services/advertisingService.js';
import { startTelegramUserReaderRuntime } from './services/telegramUserClientService.js';

console.info('[BOOT_START]', {
  startedAt: new Date().toISOString()
});

const app = express();
const port = getApiPort();

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

app.use('/api/auth', authRoutes);
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

console.info('[ROUTES_MOUNTED]', {
  mounted: [
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
    '/api/debug'
  ]
});

startKeepaScheduler();
startPublishingWorkerLoop();
startAdvertisingScheduler();
void startTelegramUserReaderRuntime().catch((error) => {
  console.error('Telegram runtime bootstrap failed', error);
});

app.get('/', (req, res) => {
  res.json({ status: 'Affiliate Manager Pro API laeuft', version: '0.1.0' });
});

console.info('[PORT_BIND_ATTEMPT]', {
  port
});

const server = app.listen(port, () => {
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
  console.error('[BOOT_FATAL_ERROR]', {
    port,
    reason: error instanceof Error ? error.message : 'Backend konnte den Port nicht binden.'
  });
});
