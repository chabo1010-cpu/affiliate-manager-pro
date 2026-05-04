import fs from 'fs';
import path from 'path';
import crypto from 'node:crypto';
import { Api, TelegramClient } from 'telegram';
import { NewMessage } from 'telegram/events/index.js';
import { StringSession } from 'telegram/sessions/index.js';
import QRCode from 'qrcode';
import { getDb } from '../db.js';
import { getReaderRuntimeConfig, getTelegramTestGroupConfig, getTelegramUserReaderConfig } from '../env.js';
import { scrapeAmazonProduct } from '../routes/amazon.js';
import {
  enrichAmazonAffiliateProductsWithOfferData,
  loadAmazonAffiliateVariations,
  loadAmazonAffiliateContext,
  searchAmazonAffiliateProducts
} from './amazonAffiliateService.js';
import { buildAmazonAffiliateLinkRecord, extractAsin, isAmazonShortLink, normalizeSellerType } from './dealHistoryService.js';
import { upsertAppSession } from './databaseService.js';
import { publishGeneratorPostDirect } from './directPublisher.js';
import { buildGeneratorDealContext } from './generatorDealScoringService.js';
import { loadKeepaClientByAsin } from './keepaClientService.js';
import { createPublishingEntry, processPublishingQueueEntry } from './publisherService.js';
import { extractSellerSignalsFromText, formatSellerBoolean, resolveSellerIdentity } from './sellerClassificationService.js';
import { sendTelegramPost } from './telegramSenderService.js';
import { COUPON_OPTION_LABEL, formatPrice, generatePostText, resolveDealImageUrlFromScrape } from '../../frontend/src/lib/postGenerator.js';

const db = getDb();
const activeClients = new Map();
const pendingAuthStates = new Map();
const DEFAULT_SESSION_NAME = 'default-user';
const QR_READY_WAIT_MS = 5000;
const QR_READY_POLL_MS = 100;
const MAX_DIALOGS = 80;
const MAX_SYNC_PER_CHANNEL = 15;
const MIN_READER_GROUP_SLOTS = 10;
const MAX_READER_GROUP_SLOTS = 100;
const RECENT_MESSAGE_LIMIT = 50;
const TELEGRAM_READER_SOURCE_TYPE = 'telegram_reader';
const TELEGRAM_READER_SOURCE_PRIORITY = 50;
const TELEGRAM_READER_SOURCE_NOTE_PREFIX = 'telegram-reader|';
const TELEGRAM_RAW_EVENT_TEXT_LIMIT = 100;
const TELEGRAM_DIAGNOSTIC_DISABLE_WATCHLIST = true;
const TELEGRAM_POLL_INTERVAL_MS = 30 * 1000;
const TELEGRAM_POLL_MESSAGE_LIMIT = 5;
const TELEGRAM_DEBUG_SCAN_MESSAGE_LIMIT = 10;
const DEBUG_QUEUE_ID_PLACEHOLDER = '__QUEUE_ID__';
const OPTIMIZED_CHANNEL_ENABLED = process.env.TELEGRAM_OPTIMIZED_CHANNEL_ENABLED === '1';
const OPTIMIZED_CHANNEL_ID = cleanText(process.env.TELEGRAM_OPTIMIZED_CHANNEL_ID);
const OPTIMIZED_CHANNEL_USERNAME = normalizeConfiguredChannelRef(process.env.TELEGRAM_OPTIMIZED_CHANNEL_USERNAME);
const SIMILAR_PRODUCT_ALLOW_AMAZON_DIRECT = process.env.SIMILAR_PRODUCT_ALLOW_AMAZON_DIRECT === '1';
const optimizedChannelCache = {
  chatId: OPTIMIZED_CHANNEL_ID,
  resolvedFromUsername: false,
  resolveAttempted: false,
  error: '',
  lastSkipReason: '',
  lastTestAt: '',
  lastTestStatus: '',
  lastTestMessageId: null,
  lastOptimizedDeal: null,
  lastOriginalSourceGroup: '',
  lastComparisonPrice: ''
};
const SIMILAR_PRODUCT_QUERY_STOPWORDS = new Set([
  'amazon',
  'deal',
  'angebot',
  'angebote',
  'coupon',
  'gutschein',
  'rabatt',
  'sparabo',
  'anzeige',
  'partnerlink',
  'preis',
  'statt',
  'nur',
  'heute',
  'top',
  'mega',
  'hot',
  'sale',
  'prime'
]);
const SIMILAR_PRODUCT_MIN_SCORE = 70;
const SIMILAR_PRODUCT_TEST_MIN_SCORE = 60;
const SIMILAR_PRODUCT_TEST_SOFT_MIN_SCORE = 50;
const PRODUCT_INTELLIGENCE_CATEGORIES = [
  {
    category: 'Powerbank',
    keywords: ['powerbank', 'power bank', 'mah'],
    attributes: ['mAh', 'Watt', 'USB-C', 'PD', 'Schnellladen']
  },
  {
    category: 'Kopfhoerer',
    keywords: ['kopfhoerer', 'kopfhörer', 'earbuds', 'in ear', 'bluetooth', 'anc', 'noise cancelling'],
    attributes: ['ANC', 'Bluetooth', 'In Ear']
  },
  { category: 'USB-C Hub', keywords: ['usb-c hub', 'usb c hub', 'hub docking'], attributes: ['USB-C', 'HDMI', 'PD'] },
  { category: 'Ladegeraet', keywords: ['ladegeraet', 'ladegerät', 'charger', 'netzteil'], attributes: ['Watt', 'USB-C', 'PD'] },
  { category: 'Kabel', keywords: ['kabel', 'usb-c kabel', 'ladekabel'], attributes: ['USB-C', 'Laenge', 'Watt'] },
  { category: 'Smartwatch', keywords: ['smartwatch', 'fitness tracker', 'uhr'], attributes: ['Groesse', 'GPS', 'Bluetooth'] },
  { category: 'Staubsauger', keywords: ['staubsauger', 'akkusauger', 'saugroboter'], attributes: ['Watt', 'Akku', 'Zubehoer'] },
  { category: 'Kuechenzubehoer', keywords: ['kueche', 'küche', 'pfanne', 'messer', 'air fryer'], attributes: ['Groesse', 'Material'] },
  { category: 'Kleidung', keywords: ['shirt', 'hose', 'jacke', 'kleid', 'sneaker'], attributes: ['Groesse', 'Farbe'] }
];

console.info('[OPTIMIZED_CHANNEL_CONFIG_LOADED]', {
  enabled: process.env.OPTIMIZED_DEALS_ENABLED === '1' && process.env.TELEGRAM_OPTIMIZED_CHANNEL_ENABLED === '1',
  optimizedDealsEnabled: process.env.OPTIMIZED_DEALS_ENABLED === '1',
  telegramChannelEnabled: process.env.TELEGRAM_OPTIMIZED_CHANNEL_ENABLED === '1',
  chatIdConfigured: Boolean(OPTIMIZED_CHANNEL_ID),
  usernameConfigured: Boolean(OPTIMIZED_CHANNEL_USERNAME),
  ready:
    process.env.OPTIMIZED_DEALS_ENABLED === '1' &&
    process.env.TELEGRAM_OPTIMIZED_CHANNEL_ENABLED === '1' &&
    Boolean(OPTIMIZED_CHANNEL_ID || OPTIMIZED_CHANNEL_USERNAME)
});
console.info('[OPTIMIZED_CHANNEL_ENV_LOADED]', {
  enabled: process.env.OPTIMIZED_DEALS_ENABLED === '1' && process.env.TELEGRAM_OPTIMIZED_CHANNEL_ENABLED === '1',
  optimizedDealsEnabled: process.env.OPTIMIZED_DEALS_ENABLED === '1',
  telegramChannelEnabled: process.env.TELEGRAM_OPTIMIZED_CHANNEL_ENABLED === '1',
  channelIdConfigured: Boolean(OPTIMIZED_CHANNEL_ID),
  usernameConfigured: Boolean(OPTIMIZED_CHANNEL_USERNAME),
  finalTarget: OPTIMIZED_CHANNEL_ID || OPTIMIZED_CHANNEL_USERNAME || ''
});
try {
  db.exec(`
    CREATE TABLE IF NOT EXISTS product_intelligence_baselines (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      category TEXT NOT NULL,
      attributeKey TEXT NOT NULL,
      brand TEXT,
      title TEXT,
      asin TEXT,
      price REAL,
      sellerClass TEXT,
      similarityScore REAL,
      source TEXT,
      firstSeenAt TEXT NOT NULL,
      lastSeenAt TEXT NOT NULL,
      timesSeen INTEGER NOT NULL DEFAULT 1,
      isMasterBaseline INTEGER NOT NULL DEFAULT 0,
      ignoredAsPriceError INTEGER NOT NULL DEFAULT 0
    );

    CREATE INDEX IF NOT EXISTS idx_product_intelligence_baselines_lookup
      ON product_intelligence_baselines (category, attributeKey, brand, isMasterBaseline);
    CREATE INDEX IF NOT EXISTS idx_product_intelligence_baselines_seen
      ON product_intelligence_baselines (lastSeenAt DESC);
  `);
} catch (error) {
  console.warn('[PRODUCT_INTELLIGENCE_STORAGE_INIT_FAILED]', {
    error: error instanceof Error ? error.message : 'product_intelligence_baselines konnte nicht initialisiert werden.'
  });
}
const AMAZON_SEARCH_FETCH_HEADERS = {
  'User-Agent':
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36',
  'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8'
};
const PROTECTED_SOURCE_PATTERNS = [
  { key: 'just_a_moment', regex: /just a moment/i },
  { key: 'checking_your_browser', regex: /checking your browser/i },
  { key: 'cloudflare', regex: /cloudflare/i },
  { key: 'access_denied', regex: /access denied/i },
  { key: 'cf_ray', regex: /cf-ray/i }
];
const READER_SHORTLINK_HOSTS = new Set(['amzn.to', 'bit.ly', 'tidd.ly']);
const BLOCKED_MAIN_POST_SOURCE_IMAGE_KEYS = new Set([
  '@codeundcoupondeals',
  'codeundcoupondeals',
  'codecoupon',
  'codecoupondeals',
  '@piratdeals',
  '@piratedeals',
  'piratdeals',
  'piratedeals'
]);
const PROTECTED_DEAL_SOURCE_RULES = [
  {
    sourceKey: 'code_coupon',
    sourceLabel: 'Code&Coupon',
    sourceGroupKeys: new Set(['@codeundcoupondeals', 'codeundcoupondeals', 'codecoupon', 'codecoupondeals']),
    hostPatterns: [/(?:^|\.)code-coupon\.de$/i],
    textPatterns: [/\bcode\s*(?:&|und)?\s*coupon\b/i, /\bcodeundcoupon\b/i, /@codeundcoupondeals/i]
  },
  {
    sourceKey: 'pirate_deals',
    sourceLabel: 'Pirate Deals',
    sourceGroupKeys: new Set(['@piratdeals', '@piratedeals', 'piratdeals', 'piratedeals']),
    hostPatterns: [/(?:^|\.)s\.pirat\.deals$/i, /(?:^|\.)pirat\.deals$/i],
    textPatterns: [/\bpirate\s+deals\b/i, /\bpirat\s+deals\b/i, /\bs\.pirat\.deals\b/i]
  }
];
const PROTECTED_SOURCE_HOST_PATTERNS = [
  /(?:^|\.)amzn\.to$/i,
  /(?:^|\.)amzlink\.to$/i,
  /(?:^|\.)bit\.ly$/i,
  /(?:^|\.)tidd\.ly$/i,
  /(?:^|\.)code-coupon\.de$/i,
  /(?:^|\.)s\.pirat\.deals$/i,
  /(?:^|\.)pirat\.deals$/i,
  /(?:^|\.)awin(?:1)?\.[a-z.]+$/i,
  /(?:^|\.)adcell\.[a-z.]+$/i
];
const ALLOWED_MAIN_POST_TITLE_SOURCES = new Set(['paapi', 'amazon', 'keepa_verified']);
const ALLOWED_MAIN_POST_IMAGE_SOURCES = new Set(['paapi', 'amazon', 'keepa_verified', 'manual_upload']);
const ALLOWED_MAIN_POST_LINK_SOURCES = new Set(['own_affiliate']);
const BLOCKED_MAIN_POST_SOURCE_VALUES = new Set([
  'telegram',
  'source',
  'originaltext',
  'originalimage',
  'preview',
  'fallback_source',
  'unknown',
  'missing',
  'og',
  'scraped'
]);
const SOURCE_BRAND_STOPWORDS = new Set([
  'amazon',
  'angebot',
  'anzeige',
  'deal',
  'fuer',
  'für',
  'gratis',
  'gutschein',
  'heute',
  'inkl',
  'jetzt',
  'mit',
  'nur',
  'oder',
  'ohne',
  'rabatt',
  'sale',
  'sparabo',
  'sparen',
  'und'
]);

function cleanText(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function safeUrl(value = '') {
  try {
    return new URL(value);
  } catch {
    return null;
  }
}

function normalizeUrlHost(value = '') {
  return safeUrl(cleanText(value))?.hostname?.toLowerCase().replace(/^www\./, '') || '';
}

function clampReaderGroupSlotCount(value) {
  const parsed = Number(value);

  if (!Number.isFinite(parsed)) {
    return MIN_READER_GROUP_SLOTS;
  }

  return Math.min(MAX_READER_GROUP_SLOTS, Math.max(MIN_READER_GROUP_SLOTS, Math.round(parsed)));
}

function nowIso() {
  return new Date().toISOString();
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeSessionName(value) {
  const normalized = cleanText(value)
    .toLowerCase()
    .replace(/[^a-z0-9_-]/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');

  return normalized || DEFAULT_SESSION_NAME;
}

function getReaderConfig() {
  const config = getTelegramUserReaderConfig();
  const apiId = Number.parseInt(String(config.apiId || '').trim(), 10);

  return {
    ...config,
    apiId: Number.isFinite(apiId) ? apiId : 0,
    apiHash: cleanText(config.apiHash),
    sessionDir: cleanText(config.sessionDir)
  };
}

function buildReaderRuntimeFlagSnapshot() {
  const runtimeConfig = getReaderRuntimeConfig();

  return {
    READER_TEST_MODE: runtimeConfig.readerTestMode === true ? 1 : 0,
    READER_DEBUG_MODE: runtimeConfig.readerDebugMode === true ? 1 : 0,
    ALLOW_RAW_READER_FALLBACK: runtimeConfig.allowRawReaderFallback === true ? 1 : 0,
    dealLockBypass: runtimeConfig.dealLockBypass === true
  };
}

function logNoPostReason(reason = '', details = {}) {
  console.warn('[NO_POST_REASON]', {
    reason: cleanText(reason) || 'Unbekannt',
    ...details
  });
}

function ensureReaderConfigured() {
  const config = getReaderConfig();

  if (!config.apiId || !config.apiHash) {
    throw new Error('TELEGRAM_USER_API_ID oder TELEGRAM_USER_API_HASH fehlen im Backend.');
  }

  if (!config.sessionDir) {
    throw new Error('TELEGRAM_USER_SESSION_DIR ist nicht konfiguriert.');
  }

  if (!fs.existsSync(config.sessionDir)) {
    fs.mkdirSync(config.sessionDir, { recursive: true });
  }

  return config;
}

function getSessionFilePath(sessionName) {
  const config = ensureReaderConfigured();
  return path.join(config.sessionDir, `${normalizeSessionName(sessionName)}.session`);
}

function readStoredSession(sessionName) {
  const sessionPath = getSessionFilePath(sessionName);
  if (!fs.existsSync(sessionPath)) {
    return '';
  }

  return fs.readFileSync(sessionPath, 'utf8').trim();
}

function saveStoredSession(sessionName, sessionString) {
  const sessionPath = getSessionFilePath(sessionName);
  fs.writeFileSync(sessionPath, cleanText(sessionString), 'utf8');
  return sessionPath;
}

function maskPhoneNumber(phoneNumber) {
  const normalized = cleanText(phoneNumber);
  if (!normalized) {
    return '';
  }

  if (normalized.length <= 4) {
    return `${normalized.slice(0, 1)}***`;
  }

  return `${normalized.slice(0, 3)}***${normalized.slice(-2)}`;
}

function toBase64Url(buffer) {
  return Buffer.from(buffer)
    .toString('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/g, '');
}

function getSessionRowByName(sessionName) {
  return (
    db.prepare(`SELECT * FROM telegram_reader_sessions WHERE name = ? LIMIT 1`).get(normalizeSessionName(sessionName)) || null
  );
}

function mapSessionRow(row) {
  if (!row) {
    return null;
  }

  return {
    id: row.id,
    name: row.name,
    loginMode: row.login_mode,
    phoneNumberMasked: maskPhoneNumber(row.phone_number),
    sessionPath: row.session_path,
    status: row.status,
    reuseEnabled: row.reuse_enabled === 1,
    lastConnectedAt: row.last_connected_at || null,
    lastMessageAt: row.last_message_at || null,
    lastError: row.last_error || '',
    qrLoginRequestedAt: row.qr_login_requested_at || null,
    createdAt: row.created_at,
    updatedAt: row.updated_at
  };
}

function upsertSessionRow(input = {}) {
  const sessionName = normalizeSessionName(input.name);
  const timestamp = nowIso();
  const sessionPath = cleanText(input.sessionPath) || getSessionFilePath(sessionName);
  const existing = getSessionRowByName(sessionName);

  if (existing) {
    db.prepare(
      `
        UPDATE telegram_reader_sessions
        SET login_mode = @loginMode,
            phone_number = @phoneNumber,
            session_path = @sessionPath,
            status = @status,
            reuse_enabled = @reuseEnabled,
            last_connected_at = COALESCE(@lastConnectedAt, last_connected_at),
            last_message_at = COALESCE(@lastMessageAt, last_message_at),
            last_error = @lastError,
            qr_login_requested_at = COALESCE(@qrLoginRequestedAt, qr_login_requested_at),
            updated_at = @updatedAt
        WHERE name = @name
      `
    ).run({
      name: sessionName,
      loginMode: cleanText(input.loginMode) || existing.login_mode || 'phone',
      phoneNumber: cleanText(input.phoneNumber) || existing.phone_number || null,
      sessionPath,
      status: cleanText(input.status) || existing.status || 'disconnected',
      reuseEnabled: input.reuseEnabled === false ? 0 : 1,
      lastConnectedAt: input.lastConnectedAt || null,
      lastMessageAt: input.lastMessageAt || null,
      lastError: cleanText(input.lastError),
      qrLoginRequestedAt: input.qrLoginRequestedAt || null,
      updatedAt: timestamp
    });
  } else {
    db.prepare(
      `
        INSERT INTO telegram_reader_sessions (
          name,
          login_mode,
          phone_number,
          session_path,
          status,
          reuse_enabled,
          last_connected_at,
          last_message_at,
          last_error,
          qr_login_requested_at,
          created_at,
          updated_at
        ) VALUES (
          @name,
          @loginMode,
          @phoneNumber,
          @sessionPath,
          @status,
          @reuseEnabled,
          @lastConnectedAt,
          @lastMessageAt,
          @lastError,
          @qrLoginRequestedAt,
          @createdAt,
          @updatedAt
        )
      `
    ).run({
      name: sessionName,
      loginMode: cleanText(input.loginMode) || 'phone',
      phoneNumber: cleanText(input.phoneNumber) || null,
      sessionPath,
      status: cleanText(input.status) || 'disconnected',
      reuseEnabled: input.reuseEnabled === false ? 0 : 1,
      lastConnectedAt: input.lastConnectedAt || null,
      lastMessageAt: input.lastMessageAt || null,
      lastError: cleanText(input.lastError),
      qrLoginRequestedAt: input.qrLoginRequestedAt || null,
      createdAt: timestamp,
      updatedAt: timestamp
    });
  }

  upsertAppSession({
    sessionKey: `telegram-user:${sessionName}`,
    module: 'telegram-user-client',
    sessionType: 'reader',
    status: cleanText(input.status) || existing?.status || 'disconnected',
    storagePath: sessionPath,
    externalRef: cleanText(input.phoneNumber) || existing?.phone_number || sessionName,
    lastSeenAt: timestamp,
    meta: {
      sessionName,
      loginMode: cleanText(input.loginMode) || existing?.login_mode || 'phone',
      reuseEnabled: input.reuseEnabled === false ? false : true
    }
  });

  return mapSessionRow(getSessionRowByName(sessionName));
}

function listSessionRows() {
  return db.prepare(`SELECT * FROM telegram_reader_sessions ORDER BY updated_at DESC, id DESC`).all().map(mapSessionRow);
}

function getPreferredReaderSessionRow() {
  return (
    db
      .prepare(
        `
          SELECT *
          FROM telegram_reader_sessions
          ORDER BY
            CASE WHEN status IN ('connected', 'active', 'watching') THEN 0 ELSE 1 END,
            updated_at DESC,
            id DESC
          LIMIT 1
        `
      )
      .get() || null
  );
}

function resolveReaderSessionName(sessionName = '') {
  const explicitSessionName = cleanText(sessionName);
  if (explicitSessionName) {
    return normalizeSessionName(explicitSessionName);
  }

  const preferredSession = getPreferredReaderSessionRow();
  return preferredSession?.name || DEFAULT_SESSION_NAME;
}

function getReaderGroupSlotCount() {
  const row = db.prepare(`SELECT telegramReaderGroupSlotCount FROM app_settings WHERE id = 1`).get() || {};
  return clampReaderGroupSlotCount(row.telegramReaderGroupSlotCount);
}

function saveReaderGroupSlotCount(slotCount) {
  const nextSlotCount = clampReaderGroupSlotCount(slotCount);
  db.prepare(`UPDATE app_settings SET telegramReaderGroupSlotCount = ? WHERE id = 1`).run(nextSlotCount);
  return nextSlotCount;
}

function listWatchedChannels(sessionName = '') {
  const rows = sessionName
    ? db
        .prepare(
          `
            SELECT c.*, s.name AS session_name
            FROM telegram_reader_channels c
            LEFT JOIN telegram_reader_sessions s ON s.id = c.session_id
            WHERE s.name = ?
            ORDER BY
              CASE WHEN c.slot_index IS NULL THEN 1 ELSE 0 END,
              c.slot_index ASC,
              c.channel_title COLLATE NOCASE ASC,
              c.id ASC
          `
        )
        .all(normalizeSessionName(sessionName))
    : db
        .prepare(
          `
            SELECT c.*, s.name AS session_name
            FROM telegram_reader_channels c
            LEFT JOIN telegram_reader_sessions s ON s.id = c.session_id
            ORDER BY
              CASE WHEN c.slot_index IS NULL THEN 1 ELSE 0 END,
              c.slot_index ASC,
              c.channel_title COLLATE NOCASE ASC,
              c.id ASC
          `
        )
        .all();

  return rows.map((row) => ({
    id: row.id,
    sessionName: row.session_name || '',
    slotIndex: row.slot_index ?? null,
    channelRef: row.channel_ref,
    channelTitle: row.channel_title || '',
    channelType: row.channel_type || 'group',
    isActive: row.is_active === 1,
    lastSeenMessageId: row.last_seen_message_id || '',
    lastSeenMessageAt: row.last_seen_message_at || null,
    lastCheckedAt: row.last_checked_at || null,
    createdAt: row.created_at,
    updatedAt: row.updated_at
  }));
}

function buildClient(sessionName) {
  const config = ensureReaderConfigured();
  const storedSession = readStoredSession(sessionName);
  const client = new TelegramClient(new StringSession(storedSession || ''), config.apiId, config.apiHash, {
    connectionRetries: 5,
    useWSS: false
  });

  return {
    client,
    config
  };
}

async function createConnectedClient(sessionName) {
  const { client, config } = buildClient(sessionName);
  await client.connect();
  return {
    client,
    config
  };
}

function clearPendingAuthState(sessionName) {
  const pendingState = pendingAuthStates.get(sessionName);
  if (pendingState?.passwordRejecter) {
    try {
      pendingState.passwordRejecter(new Error('AUTH_REPLACED'));
    } catch {}
  }
  pendingAuthStates.delete(sessionName);
}

async function releaseClient(sessionName) {
  const active = activeClients.get(sessionName);
  if (!active?.client) {
    return;
  }

  if (active.pollingIntervalId) {
    try {
      clearInterval(active.pollingIntervalId);
    } catch {}
  }

  if (active.rawListenerHandler) {
    try {
      active.client.removeEventHandler(active.rawListenerHandler);
    } catch {}
  }

  if (active.listenerHandler && active.listenerEvent) {
    try {
      active.client.removeEventHandler(active.listenerHandler, active.listenerEvent);
    } catch {}
  }

  try {
    await active.client.disconnect();
  } catch {}

  activeClients.delete(sessionName);
}

function buildPendingAuthSummary(sessionName) {
  const state = pendingAuthStates.get(sessionName);
  if (!state) {
    return null;
  }

  return {
    sessionName,
    type: state.type,
    status: state.status,
    phoneNumberMasked: maskPhoneNumber(state.phoneNumber),
    isCodeViaApp: state.isCodeViaApp === true,
    qrUrl: state.qrUrl || '',
    qrDataUrl: state.qrDataUrl || '',
    qrExpiresAt: state.qrExpiresAt || null,
    passwordHint: state.passwordHint || '',
    lastError: state.lastError || '',
    createdAt: state.createdAt
  };
}

async function finalizeAuthorizedSession(sessionName, client, meta = {}) {
  const me = await client.getMe();
  const sessionString = client.session.save();
  const sessionPath = saveStoredSession(sessionName, sessionString);
  upsertSessionRow({
    name: sessionName,
    loginMode: meta.loginMode || 'phone',
    phoneNumber: meta.phoneNumber || '',
    sessionPath,
    status: 'connected',
    lastConnectedAt: nowIso(),
    lastError: ''
  });

  activeClients.set(sessionName, {
    client,
    me,
    connectedAt: nowIso(),
    recentMessages: [],
    processedMessageKeys: [],
    pollingIntervalId: null,
    pollingInFlight: false,
    pollingActive: false,
    pollingIntervalMs: TELEGRAM_POLL_INTERVAL_MS,
    lastPollAt: null,
    lastPolledDialogs: [],
    lastFoundMessageAt: null,
    lastFoundMessagePreview: '',
    rawListenerAttached: false,
    rawListenerHandler: null,
    listenerAttached: false,
    listenerHandler: null,
    listenerEvent: null,
    listenerStartedAt: null,
    listenerStatus: 'starting',
    listenerWatchCount: 0,
    newMessageHandlerRegistered: false,
    lastNewMessageAt: null,
    lastNewMessageChatId: '',
    lastNewMessageTextPreview: ''
  });
  syncTelegramSourcesForSession(sessionName);
  await ensureSessionListener(sessionName, client);
  await ensureSessionPolling(sessionName, client);
  clearPendingAuthState(sessionName);

  return {
    session: mapSessionRow(getSessionRowByName(sessionName)),
    user: {
      id: me?.id ? String(me.id) : '',
      username: cleanText(me?.username),
      name: [cleanText(me?.firstName), cleanText(me?.lastName)].filter(Boolean).join(' ') || cleanText(me?.username) || 'Telegram User'
    }
  };
}

async function ensureAuthorizedClient(sessionName) {
  const normalizedSessionName = normalizeSessionName(sessionName);
  const active = activeClients.get(normalizedSessionName);

  if (active?.client) {
    try {
      if (await active.client.checkAuthorization()) {
        await ensureSessionListener(normalizedSessionName, active.client);
        await ensureSessionPolling(normalizedSessionName, active.client);
        return active.client;
      }
    } catch {}

    await releaseClient(normalizedSessionName);
  }

  const { client } = await createConnectedClient(normalizedSessionName);
  const authorized = await client.checkAuthorization();

  if (!authorized) {
    await client.disconnect();
    throw new Error('Telegram User Session ist noch nicht autorisiert.');
  }

  await finalizeAuthorizedSession(normalizedSessionName, client, {
    loginMode: getSessionRowByName(normalizedSessionName)?.login_mode || 'phone',
    phoneNumber: getSessionRowByName(normalizedSessionName)?.phone_number || ''
  });

  return activeClients.get(normalizedSessionName)?.client || client;
}

function normalizeConfiguredChannelRef(value = '') {
  const trimmed = cleanText(value);

  if (!trimmed) {
    return '';
  }

  const normalizedTelegramUrl = trimmed.match(
    /^(?:https?:\/\/)?(?:t\.me|telegram\.me)\/(?:joinchat\/|\+)?([A-Za-z0-9_+-]+)(?:\/.*)?$/i
  );

  if (normalizedTelegramUrl?.[1]) {
    const token = cleanText(normalizedTelegramUrl[1]);
    return token.startsWith('+') ? `https://t.me/${token}` : `@${token.replace(/^@+/, '')}`;
  }

  if (trimmed.startsWith('@')) {
    return `@${trimmed.replace(/^@+/, '')}`;
  }

  return trimmed;
}

function normalizeChannelMatchKey(value = '') {
  const normalizedRef = normalizeConfiguredChannelRef(value);

  if (!normalizedRef) {
    return '';
  }

  if (normalizedRef.startsWith('@')) {
    return `@${normalizedRef.slice(1).toLowerCase()}`;
  }

  const parsed = Number(normalizedRef);
  return Number.isFinite(parsed) ? String(parsed) : normalizedRef.toLowerCase();
}

function getOptimizedChannelEnvSnapshot() {
  const optimizedDealsEnabled = process.env.OPTIMIZED_DEALS_ENABLED === '1';
  const telegramChannelEnabled = process.env.TELEGRAM_OPTIMIZED_CHANNEL_ENABLED === '1';

  return {
    enabled: optimizedDealsEnabled && telegramChannelEnabled,
    optimizedDealsEnabled,
    telegramChannelEnabled,
    channelIdConfigured: Boolean(OPTIMIZED_CHANNEL_ID),
    usernameConfigured: Boolean(OPTIMIZED_CHANNEL_USERNAME),
    configuredChatId: OPTIMIZED_CHANNEL_ID,
    configuredUsername: OPTIMIZED_CHANNEL_USERNAME,
    finalTarget: OPTIMIZED_CHANNEL_ID || OPTIMIZED_CHANNEL_USERNAME || '',
    resolvedChatId: optimizedChannelCache.chatId || '',
    resolvedFromUsername: optimizedChannelCache.resolvedFromUsername === true,
    lastSkipReason: optimizedChannelCache.lastSkipReason || '',
    lastTestAt: optimizedChannelCache.lastTestAt || '',
    lastTestStatus: optimizedChannelCache.lastTestStatus || '',
    lastTestMessageId: optimizedChannelCache.lastTestMessageId || null,
    lastOptimizedDeal: optimizedChannelCache.lastOptimizedDeal || null,
    lastOriginalSourceGroup: optimizedChannelCache.lastOriginalSourceGroup || '',
    lastComparisonPrice: optimizedChannelCache.lastComparisonPrice || '',
    error: optimizedChannelCache.error || ''
  };
}

function getOptimizedDealsDisabledReason() {
  if (process.env.OPTIMIZED_DEALS_ENABLED !== '1') {
    return 'optimized_deals_disabled';
  }

  if (process.env.TELEGRAM_OPTIMIZED_CHANNEL_ENABLED !== '1') {
    return 'optimized_channel_disabled';
  }

  return '';
}

function logOptimizedDealsDisabledSendBlocked(extra = {}) {
  const reason = getOptimizedDealsDisabledReason() || 'optimized_deals_disabled';
  optimizedChannelCache.lastSkipReason = reason;
  console.info('[OPTIMIZED_DEALS_DISABLED_SEND_BLOCKED]', {
    reason,
    optimizedDealsEnabled: process.env.OPTIMIZED_DEALS_ENABLED === '1',
    telegramOptimizedChannelEnabled: process.env.TELEGRAM_OPTIMIZED_CHANNEL_ENABLED === '1',
    ...extra
  });
  return reason;
}

function logOptimizedDealsDisabledCheckSkipped(extra = {}) {
  const reason = getOptimizedDealsDisabledReason() || 'optimized_deals_disabled';
  optimizedChannelCache.lastSkipReason = reason;
  console.info('[OPTIMIZED_DEALS_DISABLED_CHECK_SKIPPED]', {
    reason,
    optimizedDealsEnabled: process.env.OPTIMIZED_DEALS_ENABLED === '1',
    telegramOptimizedChannelEnabled: process.env.TELEGRAM_OPTIMIZED_CHANNEL_ENABLED === '1',
    ...extra
  });
  return reason;
}

async function resolveOptimizedDealsChannelId() {
  const disabledReason = getOptimizedDealsDisabledReason();
  if (disabledReason) {
    optimizedChannelCache.lastSkipReason = disabledReason;
    return '';
  }

  if (OPTIMIZED_CHANNEL_ID) {
    optimizedChannelCache.chatId = OPTIMIZED_CHANNEL_ID;
    optimizedChannelCache.error = '';
    optimizedChannelCache.lastSkipReason = '';
    return OPTIMIZED_CHANNEL_ID;
  }

  if (optimizedChannelCache.chatId) {
    return optimizedChannelCache.chatId;
  }

  if (!OPTIMIZED_CHANNEL_USERNAME) {
    optimizedChannelCache.lastSkipReason = 'optimized_channel_missing_target';
    return '';
  }

  if (optimizedChannelCache.resolveAttempted) {
    return optimizedChannelCache.chatId || '';
  }

  optimizedChannelCache.resolveAttempted = true;

  try {
    const { token } = getTelegramTestGroupConfig();
    if (!cleanText(token)) {
      throw new Error('TELEGRAM_BOT_TOKEN fehlt.');
    }

    const response = await fetch(
      `https://api.telegram.org/bot${cleanText(token)}/getChat?chat_id=${encodeURIComponent(OPTIMIZED_CHANNEL_USERNAME)}`
    );
    const data = await response.json().catch(() => null);

    if (!response.ok || data?.ok !== true) {
      throw new Error(data?.description || `Telegram getChat fehlgeschlagen (${response.status}).`);
    }

    const chatId = data?.result?.id === undefined || data?.result?.id === null ? '' : String(data.result.id);
    if (!chatId) {
      throw new Error('Telegram getChat hat keine chat.id geliefert.');
    }

    optimizedChannelCache.chatId = chatId;
    optimizedChannelCache.resolvedFromUsername = true;
    optimizedChannelCache.error = '';
    optimizedChannelCache.lastSkipReason = '';
    console.info('[OPTIMIZED_CHANNEL_CONFIG_LOADED]', {
      enabled: OPTIMIZED_CHANNEL_ENABLED,
      username: OPTIMIZED_CHANNEL_USERNAME,
      chatId,
      resolvedFromUsername: true
    });
    return chatId;
  } catch (error) {
    optimizedChannelCache.error = error instanceof Error ? error.message : 'Optimized Channel konnte nicht aufgeloest werden.';
    optimizedChannelCache.lastSkipReason = 'optimized_channel_missing_target';
    console.warn('[SIMILAR_PRODUCT_OPTIMIZED_CHANNEL_SKIPPED]', {
      reason: 'optimized_channel_missing_target',
      username: OPTIMIZED_CHANNEL_USERNAME,
      error: optimizedChannelCache.error
    });
    return '';
  }
}

export async function getOptimizedDealsChannelStatus({ resolve = false } = {}) {
  if (resolve === true) {
    await resolveOptimizedDealsChannelId();
  }

  return getOptimizedChannelEnvSnapshot();
}

export async function sendOptimizedDealsChannelTestMessage() {
  const envSnapshot = getOptimizedChannelEnvSnapshot();
  console.info('[OPTIMIZED_CHANNEL_TEST_SEND_START]', envSnapshot);

  const disabledReason = getOptimizedDealsDisabledReason();
  if (disabledReason) {
    logOptimizedDealsDisabledSendBlocked({
      context: 'optimized_channel_test',
      finalTarget: envSnapshot.finalTarget
    });
    optimizedChannelCache.lastTestAt = nowIso();
    optimizedChannelCache.lastTestStatus = 'failed';
    console.error('[OPTIMIZED_CHANNEL_TEST_SEND_FAILED]', {
      reason: optimizedChannelCache.lastSkipReason,
      finalTarget: envSnapshot.finalTarget
    });
    return {
      success: false,
      reason: optimizedChannelCache.lastSkipReason,
      status: getOptimizedChannelEnvSnapshot()
    };
  }

  if (!envSnapshot.finalTarget) {
    optimizedChannelCache.lastSkipReason = 'optimized_channel_missing_target';
    optimizedChannelCache.lastTestAt = nowIso();
    optimizedChannelCache.lastTestStatus = 'failed';
    console.error('[OPTIMIZED_CHANNEL_TEST_SEND_FAILED]', {
      reason: optimizedChannelCache.lastSkipReason,
      finalTarget: ''
    });
    return {
      success: false,
      reason: optimizedChannelCache.lastSkipReason,
      status: getOptimizedChannelEnvSnapshot()
    };
  }

  try {
    const preSendDisabledReason = getOptimizedDealsDisabledReason();
    if (preSendDisabledReason) {
      throw new Error(logOptimizedDealsDisabledSendBlocked({
        context: 'optimized_channel_test_presend',
        finalTarget: envSnapshot.finalTarget
      }));
    }

    const chatId = await resolveOptimizedDealsChannelId();
    if (!chatId) {
      throw new Error(optimizedChannelCache.lastSkipReason || 'optimized_channel_missing_target');
    }

    const finalSendDisabledReason = getOptimizedDealsDisabledReason();
    if (finalSendDisabledReason) {
      throw new Error(logOptimizedDealsDisabledSendBlocked({
        context: 'optimized_channel_test_final_send',
        finalTarget: chatId
      }));
    }

    const result = await sendTelegramPost({
      text: ['\u{1F9EA} Optimierte Deals Test', '\u2705 Verbindung funktioniert', '\u{1F4E6} Produkt-Intelligenz ist verbunden'].join('\n'),
      chatId,
      disableWebPagePreview: true,
      titlePreview: 'Optimierte Deals Test',
      postContext: 'product_intelligence_optimized_channel_test'
    });

    optimizedChannelCache.lastSkipReason = '';
    optimizedChannelCache.lastTestAt = nowIso();
    optimizedChannelCache.lastTestStatus = 'success';
    optimizedChannelCache.lastTestMessageId = result?.messageId || null;
    console.info('[OPTIMIZED_CHANNEL_TEST_SEND_SUCCESS]', {
      chatId,
      messageId: result?.messageId || null,
      method: result?.method || ''
    });

    return {
      success: true,
      chatId,
      messageId: result?.messageId || null,
      method: result?.method || '',
      status: getOptimizedChannelEnvSnapshot()
    };
  } catch (error) {
    const reason = error instanceof Error ? error.message : 'optimized_channel_test_failed';
    optimizedChannelCache.lastSkipReason = reason;
    optimizedChannelCache.lastTestAt = nowIso();
    optimizedChannelCache.lastTestStatus = 'failed';
    console.error('[OPTIMIZED_CHANNEL_TEST_SEND_FAILED]', {
      reason,
      finalTarget: envSnapshot.finalTarget
    });
    return {
      success: false,
      reason,
      status: getOptimizedChannelEnvSnapshot()
    };
  }
}

function normalizeReaderImagePolicySourceKey(value = '') {
  return cleanText(value)
    .toLowerCase()
    .replace(/[^a-z0-9@]+/g, '');
}

function resolveProtectedDealSourceContext({
  source = {},
  structuredMessage = {},
  originalLink = '',
  originalText = ''
} = {}) {
  const channelRef = cleanText(source?.channelRef) || cleanText(structuredMessage?.channelRef);
  const channelTitle = cleanText(source?.channelTitle) || cleanText(structuredMessage?.channelTitle);
  const group = cleanText(structuredMessage?.group);
  const originalLinkHost = normalizeUrlHost(
    originalLink || structuredMessage?.externalLink || structuredMessage?.previewUrl || structuredMessage?.link
  );
  const originalTextValue = cleanText(originalText || structuredMessage?.text);
  const groupedCandidates = [
    {
      field: 'channelRef',
      value: channelRef,
      normalizedKeys: [normalizeChannelMatchKey(channelRef), normalizeReaderImagePolicySourceKey(channelRef)].filter(Boolean)
    },
    {
      field: 'channelTitle',
      value: channelTitle,
      normalizedKeys: [normalizeReaderImagePolicySourceKey(channelTitle)].filter(Boolean)
    },
    {
      field: 'group',
      value: group,
      normalizedKeys: [normalizeReaderImagePolicySourceKey(group)].filter(Boolean)
    }
  ];

  for (const rule of PROTECTED_DEAL_SOURCE_RULES) {
    for (const candidate of groupedCandidates) {
      if (candidate.normalizedKeys.some((value) => rule.sourceGroupKeys.has(value))) {
        return {
          matched: true,
          sourceKey: rule.sourceKey,
          sourceLabel: rule.sourceLabel,
          matchedField: candidate.field,
          matchedValue: candidate.value,
          channelRef,
          channelTitle,
          group,
          originalLinkHost
        };
      }

      if (candidate.value && rule.textPatterns.some((pattern) => pattern.test(candidate.value))) {
        return {
          matched: true,
          sourceKey: rule.sourceKey,
          sourceLabel: rule.sourceLabel,
          matchedField: candidate.field,
          matchedValue: candidate.value,
          channelRef,
          channelTitle,
          group,
          originalLinkHost
        };
      }
    }

    if (originalLinkHost && rule.hostPatterns.some((pattern) => pattern.test(originalLinkHost))) {
      return {
        matched: true,
        sourceKey: rule.sourceKey,
        sourceLabel: rule.sourceLabel,
        matchedField: 'originalLinkHost',
        matchedValue: originalLinkHost,
        channelRef,
        channelTitle,
        group,
        originalLinkHost
      };
    }

    if (originalTextValue && rule.textPatterns.some((pattern) => pattern.test(originalTextValue))) {
      return {
        matched: true,
        sourceKey: rule.sourceKey,
        sourceLabel: rule.sourceLabel,
        matchedField: 'originalText',
        matchedValue: originalTextValue.slice(0, 180),
        channelRef,
        channelTitle,
        group,
        originalLinkHost
      };
    }
  }

  return {
    matched: false,
    sourceKey: '',
    sourceLabel: '',
    matchedField: '',
    matchedValue: '',
    channelRef,
    channelTitle,
    group,
    originalLinkHost
  };
}

function resolveBlockedMainPostSourceImageContext(structuredMessage = {}) {
  const sourceGroupRef = cleanText(structuredMessage?.channelRef);
  const sourceGroupTitle = cleanText(structuredMessage?.channelTitle || structuredMessage?.group) || 'Unbekannt';
  const protectedSourceContext = resolveProtectedDealSourceContext({
    structuredMessage,
    originalLink: cleanText(structuredMessage?.externalLink) || cleanText(structuredMessage?.previewUrl) || cleanText(structuredMessage?.link),
    originalText: cleanText(structuredMessage?.text)
  });
  const candidateKeys = [
    normalizeChannelMatchKey(sourceGroupRef),
    normalizeReaderImagePolicySourceKey(sourceGroupRef),
    normalizeReaderImagePolicySourceKey(sourceGroupTitle)
  ].filter(Boolean);
  const matchedPolicyKey =
    protectedSourceContext.matched === true
      ? protectedSourceContext.sourceKey
      : candidateKeys.find((value) => BLOCKED_MAIN_POST_SOURCE_IMAGE_KEYS.has(value)) || '';

  if (!matchedPolicyKey) {
    return null;
  }

  return {
    sourceGroupRef,
    sourceGroupTitle,
    matchedPolicyKey
  };
}

function normalizeTelegramDate(value) {
  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : value;
  }

  if (typeof value === 'bigint') {
    const numeric = Number(value);
    return Number.isFinite(numeric) ? new Date(numeric > 10_000_000_000 ? numeric : numeric * 1000) : null;
  }

  if (typeof value === 'number' && Number.isFinite(value)) {
    return new Date(value > 10_000_000_000 ? value : value * 1000);
  }

  if (typeof value === 'string') {
    const trimmed = cleanText(value);

    if (!trimmed) {
      return null;
    }

    const numeric = Number(trimmed);
    if (Number.isFinite(numeric)) {
      return new Date(numeric > 10_000_000_000 ? numeric : numeric * 1000);
    }

    const parsed = new Date(trimmed);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }

  return null;
}

function safeStringifyJson(value) {
  return JSON.stringify(value ?? null);
}

function normalizeTelegramPeerIdForLog(peerId) {
  if (!peerId || typeof peerId !== 'object') {
    return peerId ?? null;
  }

  return {
    className: cleanText(peerId.className),
    channelId:
      peerId.channelId === undefined || peerId.channelId === null ? null : String(peerId.channelId),
    chatId: peerId.chatId === undefined || peerId.chatId === null ? null : String(peerId.chatId),
    userId: peerId.userId === undefined || peerId.userId === null ? null : String(peerId.userId)
  };
}

function extractTelegramPeerIdCandidates(peerId) {
  const normalizedPeerId = normalizeTelegramPeerIdForLog(peerId);
  const values = new Set();

  if (normalizedPeerId?.channelId) {
    values.add(String(normalizedPeerId.channelId));
    values.add(`-100${normalizedPeerId.channelId}`);
  }

  if (normalizedPeerId?.chatId) {
    values.add(String(normalizedPeerId.chatId));
  }

  if (normalizedPeerId?.userId) {
    values.add(String(normalizedPeerId.userId));
  }

  return Array.from(values).filter(Boolean);
}

function buildTelegramDialogIdCandidates(dialog = {}) {
  const values = new Set();
  const dialogId = cleanText(dialog?.id ? String(dialog.id) : '');
  const entityId = cleanText(dialog?.entity?.id ? String(dialog.entity.id) : '');
  const isChannelLike = Boolean(dialog?.isChannel || dialog?.entity?.broadcast || dialog?.entity?.megagroup);

  if (dialogId) {
    values.add(dialogId);
  }

  if (entityId) {
    values.add(entityId);

    if (isChannelLike && !entityId.startsWith('-100')) {
      values.add(`-100${entityId}`);
    }
  }

  return Array.from(values).filter(Boolean);
}

function isTelegramChannelChat(chat, message) {
  return Boolean(message?.isChannel || chat?.broadcast === true || chat?.className === 'Channel');
}

function isTelegramGroupChat(chat, message) {
  return Boolean(message?.isGroup || chat?.megagroup === true || chat?.className === 'Chat');
}

function getTelegramClassName(value) {
  return cleanText(value?.className || value?.constructor?.name);
}

function summarizeTelegramMessageCandidate(message) {
  if (!message || typeof message !== 'object') {
    return null;
  }

  return {
    className: getTelegramClassName(message),
    id: message?.id === undefined || message?.id === null ? null : String(message.id),
    textPreview: String(message?.message || message?.text || '').slice(0, TELEGRAM_RAW_EVENT_TEXT_LIMIT),
    peerId: normalizeTelegramPeerIdForLog(message?.peerId),
    senderId: normalizeTelegramPeerIdForLog(message?.senderId || message?.fromId),
    date: normalizeTelegramDate(message?.date)?.toISOString() || message?.date || null
  };
}

function summarizeTelegramMessages(messages) {
  if (!Array.isArray(messages)) {
    return null;
  }

  return messages.slice(0, 3).map((message) => summarizeTelegramMessageCandidate(message));
}

function summarizeTelegramUpdates(updates) {
  if (!Array.isArray(updates)) {
    return null;
  }

  return updates.slice(0, 5).map((update, index) => ({
    index,
    className: getTelegramClassName(update),
    message: summarizeTelegramMessageCandidate(update?.message),
    messages: summarizeTelegramMessages(update?.messages)
  }));
}

function pushTelegramMessageCandidate(candidates, message, sourceType) {
  if (!message || typeof message !== 'object') {
    return;
  }

  candidates.push({
    message,
    sourceType
  });
}

function collectTelegramMessageCandidates(candidates, container, sourcePrefix) {
  if (!container || typeof container !== 'object') {
    return;
  }

  pushTelegramMessageCandidate(candidates, container.message, `${sourcePrefix}.message`);

  if (Array.isArray(container.messages) && container.messages[0]) {
    pushTelegramMessageCandidate(candidates, container.messages[0], `${sourcePrefix}.messages[0]`);
  }

  if (!Array.isArray(container.updates)) {
    return;
  }

  container.updates.forEach((update, index) => {
    pushTelegramMessageCandidate(candidates, update?.message, `${sourcePrefix}.updates[${index}].message`);

    if (Array.isArray(update?.messages) && update.messages[0]) {
      pushTelegramMessageCandidate(candidates, update.messages[0], `${sourcePrefix}.updates[${index}].messages[0]`);
    }
  });
}

function extractTelegramMessageFromUpdate(event) {
  const candidates = [];

  collectTelegramMessageCandidates(candidates, event, 'event');
  collectTelegramMessageCandidates(candidates, event?.originalUpdate, 'event.originalUpdate');
  collectTelegramMessageCandidates(candidates, event?.update, 'event.update');

  const selectedCandidate =
    candidates.find((candidate) => cleanText(candidate.message?.message || candidate.message?.text)) ||
    candidates.find(
      (candidate) =>
        candidate.message?.id !== undefined ||
        candidate.message?.peerId !== undefined ||
        candidate.message?.date !== undefined
    ) ||
    null;

  if (!selectedCandidate) {
    return {
      hasMessage: false,
      text: '',
      chatId: '',
      peerId: null,
      senderId: null,
      date: null,
      sourceType: '',
      dialogIdCandidates: [],
      messageId: '',
      message: null
    };
  }

  const message = selectedCandidate.message;
  const peerId = normalizeTelegramPeerIdForLog(message?.peerId);
  const senderId = normalizeTelegramPeerIdForLog(message?.senderId || message?.fromId);
  const dialogIdCandidates = Array.from(
    new Set([
      cleanText(message?.chatId ? String(message.chatId) : ''),
      ...extractTelegramPeerIdCandidates(message?.peerId)
    ])
  ).filter(Boolean);
  const text = String(message?.message || message?.text || '');
  const date = normalizeTelegramDate(message?.date)?.toISOString() || message?.date || null;

  return {
    hasMessage: true,
    text,
    chatId: dialogIdCandidates[0] || '',
    peerId,
    senderId,
    date,
    sourceType: selectedCandidate.sourceType,
    dialogIdCandidates,
    messageId: message?.id === undefined || message?.id === null ? '' : String(message.id),
    message
  };
}

function buildTelegramRawEventPayload(event) {
  const update = event?.originalUpdate || event?.update || event;
  const extracted = extractTelegramMessageFromUpdate(event);

  return {
    eventClassName: getTelegramClassName(event),
    eventKeys: event && typeof event === 'object' ? Object.keys(event) : [],
    originalUpdateClassName: getTelegramClassName(event?.originalUpdate),
    updateClassName: getTelegramClassName(update),
    updateMessage: summarizeTelegramMessageCandidate(update?.message),
    updateMessages: summarizeTelegramMessages(update?.messages),
    updateUpdates: summarizeTelegramUpdates(update?.updates),
    hasMessage: extracted.hasMessage,
    text: extracted.text.slice(0, TELEGRAM_RAW_EVENT_TEXT_LIMIT),
    chatId: extracted.chatId || normalizeTelegramPeerIdForLog(extracted.peerId),
    date: extracted.date,
    sourceType: extracted.sourceType
  };
}

function logTelegramRawEvent(event) {
  console.log('[TELEGRAM_RAW_EVENT]', safeStringifyJson(buildTelegramRawEventPayload(event)));
}

function updateNewMessageRuntime(sessionName, payload = {}) {
  const active = activeClients.get(normalizeSessionName(sessionName));

  if (!active) {
    return;
  }

  active.lastNewMessageAt = nowIso();
  active.lastNewMessageChatId = cleanText(payload.chatId) || payload.dialogIdCandidates?.[0] || '';
  active.lastNewMessageTextPreview = cleanText(payload.textPreview);
}

function prepareExtractedTelegramMessage(message, event, client) {
  if (!message || typeof message !== 'object' || !client) {
    return message;
  }

  try {
    if (typeof message._finishInit === 'function') {
      const entities =
        event?.originalUpdate?._entities || event?._entities || event?.update?._entities || message?._entities || new Map();
      message._finishInit(client, entities, undefined);
    }
  } catch {}

  try {
    message._client = client;
  } catch {}

  return message;
}

function buildProcessedTelegramMessageKey(payload = {}) {
  return [payload.chatId || '', payload.messageId || '', payload.date || '', payload.textPreview || ''].join(':');
}

function shouldProcessTelegramMessage(sessionName, payload = {}) {
  const active = activeClients.get(normalizeSessionName(sessionName));

  if (!active) {
    return true;
  }

  const key = buildProcessedTelegramMessageKey(payload);
  if (!key || key === ':::') {
    return true;
  }

  if (!Array.isArray(active.processedMessageKeys)) {
    active.processedMessageKeys = [];
  }

  if (active.processedMessageKeys.includes(key)) {
    return false;
  }

  active.processedMessageKeys.push(key);
  if (active.processedMessageKeys.length > RECENT_MESSAGE_LIMIT * 2) {
    active.processedMessageKeys.splice(0, active.processedMessageKeys.length - RECENT_MESSAGE_LIMIT * 2);
  }

  return true;
}

async function buildTelegramNewMessageRawPayload(event, client = null) {
  const extracted = extractTelegramMessageFromUpdate(event);
  const message = prepareExtractedTelegramMessage(extracted.message, event, client);
  let chat = null;

  try {
    if (message?.getChat) {
      chat = await message.getChat();
    }
  } catch {
    chat = null;
  }

  const peerId = extracted.peerId || normalizeTelegramPeerIdForLog(message?.peerId);
  const senderId = extracted.senderId || normalizeTelegramPeerIdForLog(message?.senderId || message?.fromId);
  const dialogIdCandidates = Array.from(
    new Set([
      cleanText(chat?.id ? String(chat.id) : ''),
      cleanText(extracted.chatId),
      cleanText(message?.chatId ? String(message.chatId) : ''),
      ...extractTelegramPeerIdCandidates(message?.peerId)
    ])
  ).filter(Boolean);

  return {
    hasMessage: extracted.hasMessage,
    messageId: extracted.messageId,
    chatId: dialogIdCandidates[0] || '',
    peerId,
    senderId,
    textPreview: extracted.text.slice(0, TELEGRAM_RAW_EVENT_TEXT_LIMIT),
    isChannel:
      isTelegramChannelChat(chat, message) ||
      cleanText(extracted.sourceType).toLowerCase().includes('channel'),
    isGroup: isTelegramGroupChat(chat, message),
    dialogIdCandidates,
    chatTitle: cleanText(chat?.title),
    username: cleanText(chat?.username),
    date: extracted.date,
    sourceType: extracted.sourceType,
    message
  };
}

async function extractAndProcessTelegramMessage(sessionName, event, client, options = {}) {
  const payload = await buildTelegramNewMessageRawPayload(event, client);
  const logPrefix = cleanText(options.logPrefix) || 'TELEGRAM_NEW_MESSAGE_RAW';

  console.log(`[${logPrefix}]`, payload);
  console.log('[TELEGRAM_NEW_MESSAGE]', payload.textPreview || '');

  if (payload.hasMessage && payload.textPreview) {
    console.log('[TELEGRAM_MESSAGE_EXTRACTED]', {
      sourceType: payload.sourceType,
      chatId: payload.chatId,
      peerId: payload.peerId,
      senderId: payload.senderId,
      textPreview: payload.textPreview,
      date: payload.date
    });
  }

  if (payload.hasMessage && payload.message) {
    updateNewMessageRuntime(sessionName, payload);
    console.log('[MESSAGE OK]', {
      text: payload.textPreview,
      chatId: payload.chatId,
      peerId: payload.peerId,
      senderId: payload.senderId,
      isChannel: payload.isChannel,
      isGroup: payload.isGroup
    });

    if (shouldProcessTelegramMessage(sessionName, payload)) {
      await handleWatchedTelegramMessage(sessionName, payload.message);
    }

    return payload;
  }

  console.log('[TELEGRAM_NEW_MESSAGE]', safeStringifyJson({ hasMessage: false }));
  return payload;
}

function logTelegramRuntime(message, payload = null) {
  if (payload && Object.keys(payload).length) {
    console.info(message, payload);
    return;
  }

  console.info(message);
}

function logTelegramReaderEvent({ level = 'info', eventType, sourceId = null, message, payload = null }) {
  db.prepare(
    `
      INSERT INTO copybot_logs (
        level,
        event_type,
        source_id,
        imported_deal_id,
        message,
        payload_json,
        created_at
      ) VALUES (?, ?, ?, NULL, ?, ?, ?)
    `
  ).run(level, eventType, sourceId, cleanText(message), payload ? safeStringifyJson(payload) : null, nowIso());
}

function listActiveWatchedChannels(sessionName) {
  return listWatchedChannels(sessionName).filter((item) => item.isActive && cleanText(item.channelRef));
}

function getSessionRuntimeStatus(sessionName) {
  return listActiveWatchedChannels(sessionName).length > 0 ? 'watching' : 'connected';
}

function getDefaultPricingRuleId() {
  const preferred = db.prepare(`SELECT id FROM pricing_rules WHERE is_active = 1 ORDER BY id ASC LIMIT 1`).get();
  const fallback = preferred || db.prepare(`SELECT id FROM pricing_rules ORDER BY id ASC LIMIT 1`).get();
  return Number(fallback?.id || 1);
}

function getDefaultSamplingRuleId() {
  const preferred = db.prepare(`SELECT id FROM sampling_rules WHERE is_active = 1 ORDER BY id ASC LIMIT 1`).get();
  const fallback = preferred || db.prepare(`SELECT id FROM sampling_rules ORDER BY id ASC LIMIT 1`).get();
  return fallback?.id ? Number(fallback.id) : null;
}

function buildTelegramReaderSourceTag(sessionName, channelRef = '') {
  return `${TELEGRAM_READER_SOURCE_NOTE_PREFIX}${normalizeSessionName(sessionName)}|${normalizeChannelMatchKey(channelRef)}`;
}

function findTelegramReaderSource(sessionName, channelRef = '') {
  const tag = buildTelegramReaderSourceTag(sessionName, channelRef);

  return (
    db
      .prepare(
        `
          SELECT *
          FROM sources
          WHERE platform = 'telegram'
            AND source_type = ?
            AND notes = ?
          ORDER BY id ASC
          LIMIT 1
        `
      )
      .get(TELEGRAM_READER_SOURCE_TYPE, tag) || null
  );
}

function upsertTelegramReaderSource(sessionName, channel = {}) {
  const channelRef = cleanText(channel.channelRef);

  if (!channelRef) {
    return null;
  }

  const existing = findTelegramReaderSource(sessionName, channelRef);
  const timestamp = nowIso();
  const pricingRuleId = existing?.pricing_rule_id || getDefaultPricingRuleId();
  const samplingRuleId =
    existing?.sampling_rule_id !== undefined && existing?.sampling_rule_id !== null
      ? existing.sampling_rule_id
      : getDefaultSamplingRuleId();
  const name = cleanText(channel.channelTitle) || channelRef;
  const tag = buildTelegramReaderSourceTag(sessionName, channelRef);
  const isActive = channel.isActive === true ? 1 : 0;

  if (existing) {
    db.prepare(
      `
        UPDATE sources
        SET name = @name,
            platform = 'telegram',
            source_type = @sourceType,
            is_active = @isActive,
            priority = @priority,
            pricing_rule_id = @pricingRuleId,
            sampling_rule_id = @samplingRuleId,
            notes = @notes,
            updated_at = @updatedAt
        WHERE id = @id
      `
    ).run({
      id: existing.id,
      name,
      sourceType: TELEGRAM_READER_SOURCE_TYPE,
      isActive,
      priority: TELEGRAM_READER_SOURCE_PRIORITY,
      pricingRuleId,
      samplingRuleId,
      notes: tag,
      updatedAt: timestamp
    });

    return db.prepare(`SELECT * FROM sources WHERE id = ?`).get(existing.id) || null;
  }

  const result = db
    .prepare(
      `
        INSERT INTO sources (
          name,
          platform,
          source_type,
          is_active,
          priority,
          pricing_rule_id,
          sampling_rule_id,
          success_rate,
          notes,
          created_at,
          updated_at
        ) VALUES (
          @name,
          'telegram',
          @sourceType,
          @isActive,
          @priority,
          @pricingRuleId,
          @samplingRuleId,
          NULL,
          @notes,
          @createdAt,
          @updatedAt
        )
      `
    )
    .run({
      name,
      sourceType: TELEGRAM_READER_SOURCE_TYPE,
      isActive,
      priority: TELEGRAM_READER_SOURCE_PRIORITY,
      pricingRuleId,
      samplingRuleId,
      notes: tag,
      createdAt: timestamp,
      updatedAt: timestamp
    });

  return db.prepare(`SELECT * FROM sources WHERE id = ?`).get(result.lastInsertRowid) || null;
}

function syncTelegramSourcesForSession(sessionName) {
  const resolvedSessionName = normalizeSessionName(sessionName);
  const activeTags = new Set();

  for (const channel of listWatchedChannels(resolvedSessionName)) {
    const channelRef = cleanText(channel.channelRef);

    if (!channelRef) {
      continue;
    }

    const tag = buildTelegramReaderSourceTag(resolvedSessionName, channelRef);
    activeTags.add(tag);
    upsertTelegramReaderSource(resolvedSessionName, channel);
  }

  const managedSources = db
    .prepare(
      `
        SELECT id, notes
        FROM sources
        WHERE platform = 'telegram'
          AND source_type = ?
          AND notes LIKE ?
      `
    )
    .all(TELEGRAM_READER_SOURCE_TYPE, `${TELEGRAM_READER_SOURCE_NOTE_PREFIX}${resolvedSessionName}|%`);

  for (const source of managedSources) {
    if (activeTags.has(source.notes)) {
      continue;
    }

    db.prepare(`UPDATE sources SET is_active = 0, updated_at = ? WHERE id = ?`).run(nowIso(), source.id);
  }

  return managedSources.length;
}

function appendRecentMessage(sessionName, item) {
  const active = activeClients.get(normalizeSessionName(sessionName));

  if (!active) {
    return;
  }

  active.recentMessages.push(item);
  if (active.recentMessages.length > RECENT_MESSAGE_LIMIT) {
    active.recentMessages.splice(0, active.recentMessages.length - RECENT_MESSAGE_LIMIT);
  }
}

function consumeRecentMessages(sessionName) {
  const active = activeClients.get(normalizeSessionName(sessionName));

  if (!active) {
    return [];
  }

  const items = Array.isArray(active.recentMessages) ? [...active.recentMessages] : [];
  active.recentMessages = [];
  return items;
}

function buildWatchedChannelLookup(sessionName) {
  const lookup = new Map();

  for (const channel of listActiveWatchedChannels(sessionName)) {
    const key = normalizeChannelMatchKey(channel.channelRef);

    if (key) {
      lookup.set(key, channel);
    }
  }

  return lookup;
}

function buildMessageMatchKeys(message, chat) {
  const keys = new Set();
  const username = cleanText(chat?.username);
  const chatId = cleanText(chat?.id ? String(chat.id) : message?.chatId ? String(message.chatId) : '');

  if (username) {
    keys.add(`@${username.toLowerCase()}`);
  }

  if (chatId) {
    keys.add(chatId);
  }

  return Array.from(keys);
}

function resolveDialogRef(ref) {
  const normalizedRef = normalizeConfiguredChannelRef(ref);
  if (!normalizedRef) {
    throw new Error('Dialog-Referenz fehlt.');
  }

  if (normalizedRef.startsWith('@')) {
    return normalizedRef;
  }

  const parsed = Number(normalizedRef);
  return Number.isFinite(parsed) ? parsed : normalizedRef;
}

function extractFirstLink(text) {
  const match = String(text || '').match(/https?:\/\/\S+/i);
  return match ? match[0] : '';
}

function extractAllLinks(text = '') {
  return Array.from(new Set(String(text || '').match(/https?:\/\/\S+/gi) || []));
}

function isReaderResolvableShortlink(value = '') {
  return READER_SHORTLINK_HOSTS.has(normalizeUrlHost(value));
}

function resolveReaderShortlinkCandidate({ explicitAmazonLink = '', structuredMessage = {}, originalLink = '' } = {}) {
  const candidates = [
    cleanText(explicitAmazonLink),
    ...(Array.isArray(structuredMessage?.allLinks) ? structuredMessage.allLinks : []),
    cleanText(structuredMessage?.externalLink),
    cleanText(structuredMessage?.previewUrl),
    cleanText(originalLink)
  ]
    .map((candidate) => cleanText(candidate))
    .filter(Boolean);

  return candidates.find((candidate) => isReaderResolvableShortlink(candidate)) || '';
}

async function resolveReaderShortlink(value = '') {
  const trimmed = cleanText(value);
  if (!isReaderResolvableShortlink(trimmed)) {
    return {
      attempted: false,
      resolved: false,
      finalUrl: trimmed,
      asin: '',
      method: '',
      errorMessage: ''
    };
  }

  let lastErrorMessage = '';
  let lastResolvedUrl = '';

  for (const method of ['HEAD', 'GET']) {
    try {
      const response = await fetch(trimmed, {
        method,
        redirect: 'follow',
        headers: AMAZON_SEARCH_FETCH_HEADERS
      });
      const finalUrl = cleanText(response?.url);
      const finalAsin = cleanText(extractAsin(finalUrl)).toUpperCase();
      lastResolvedUrl = finalUrl || lastResolvedUrl;

      if (finalUrl && /amazon\.[a-z.]+/i.test(normalizeUrlHost(finalUrl)) && finalAsin) {
        return {
          attempted: true,
          resolved: true,
          finalUrl,
          asin: finalAsin,
          method,
          errorMessage: ''
        };
      }

      lastErrorMessage = finalUrl
        ? 'Shortlink wurde aufgeloest, aber nicht zu einer gueltigen Amazon-Produkt-URL.'
        : 'Shortlink konnte nicht aufgeloest werden.';
    } catch (error) {
      lastErrorMessage = error instanceof Error ? error.message : 'Shortlink konnte nicht aufgeloest werden.';
    }
  }

  return {
    attempted: true,
    resolved: false,
    finalUrl: lastResolvedUrl,
    asin: cleanText(extractAsin(lastResolvedUrl)).toUpperCase(),
    method: '',
    errorMessage: lastErrorMessage || 'Shortlink konnte nicht aufgeloest werden.'
  };
}

function decodeReaderHtml(value = '') {
  return String(value || '')
    .replace(/&amp;/g, '&')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>');
}

function stripReaderHtml(value = '') {
  return decodeReaderHtml(String(value || ''))
    .replace(/<script[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style[\s\S]*?<\/style>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function resolveReaderUrlCandidate(value = '', baseUrl = '') {
  const trimmed = cleanText(value);
  if (!trimmed) {
    return '';
  }

  try {
    return new URL(trimmed, baseUrl || undefined).toString();
  } catch {
    return '';
  }
}

function isLikelyReaderImageUrl(value = '') {
  const resolved = cleanText(value).toLowerCase();
  if (!resolved) {
    return false;
  }

  if (!/^https?:\/\//.test(resolved) && !/^data:image\//.test(resolved)) {
    return false;
  }

  if (resolved.endsWith('.svg') || resolved.includes('.svg?')) {
    return false;
  }

  return !/\/(logo|icon|sprite|pixel|spacer|favicon|loader)[^/]*($|[?#/])/i.test(resolved);
}

function extractReaderMetaContent(html = '', patterns = []) {
  for (const pattern of patterns) {
    const match = String(html || '').match(pattern);
    if (match?.[1]) {
      return decodeReaderHtml(match[1]);
    }
  }

  return '';
}

function extractGenericDealTitleFromHtml(html = '') {
  return cleanText(
    extractReaderMetaContent(html, [
      /<meta[^>]+property=["']og:title["'][^>]+content=["']([^"']+)["']/i,
      /<meta[^>]+name=["']twitter:title["'][^>]+content=["']([^"']+)["']/i,
      /<title>\s*([^<]+?)\s*<\/title>/i,
      /<h1[^>]*>\s*([^<]+?)\s*<\/h1>/i
    ])
  );
}

function extractGenericDealDescriptionFromHtml(html = '') {
  return cleanText(
    stripReaderHtml(
      extractReaderMetaContent(html, [
        /<meta[^>]+property=["']og:description["'][^>]+content=["']([^"']+)["']/i,
        /<meta[^>]+name=["']description["'][^>]+content=["']([^"']+)["']/i,
        /<meta[^>]+name=["']twitter:description["'][^>]+content=["']([^"']+)["']/i
      ])
    )
  );
}

function extractGenericDealPriceFromHtml(html = '') {
  const metaPrice = extractReaderMetaContent(html, [
    /<meta[^>]+property=["']product:price:amount["'][^>]+content=["']([^"']+)["']/i,
    /<meta[^>]+name=["']price["'][^>]+content=["']([^"']+)["']/i,
    /"price"\s*:\s*"([^"]+)"/i
  ]);
  if (metaPrice) {
    return metaPrice;
  }

  const strippedHtml = stripReaderHtml(html);
  const match = strippedHtml.match(/(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?|\d+(?:[.,]\d{2})?)\s*(?:€|eur)/i);
  return cleanText(match?.[0] || '');
}

function extractGenericDealImageCandidates(html = '', baseUrl = '') {
  const candidates = [
    {
      source: 'ogImage',
      value: resolveReaderUrlCandidate(
        extractReaderMetaContent(html, [/<meta[^>]+property=["']og:image["'][^>]+content=["']([^"']+)["']/i]),
        baseUrl
      )
    },
    {
      source: 'twitterImage',
      value: resolveReaderUrlCandidate(
        extractReaderMetaContent(html, [/<meta[^>]+name=["']twitter:image["'][^>]+content=["']([^"']+)["']/i]),
        baseUrl
      )
    },
    {
      source: 'scrapedImage',
      value: resolveReaderUrlCandidate(
        extractReaderMetaContent(html, [
          /<img[^>]+src=["']([^"']+)["'][^>]*>/i,
          /<source[^>]+srcset=["']([^"']+)["'][^>]*>/i
        ]),
        baseUrl
      )
    }
  ].filter((candidate) => isLikelyReaderImageUrl(candidate.value));

  return candidates;
}

function bufferToDataUrl(buffer, mimeType = 'image/jpeg') {
  if (!Buffer.isBuffer(buffer) || buffer.length === 0) {
    return '';
  }

  return `data:${mimeType};base64,${buffer.toString('base64')}`;
}

async function tryDownloadTelegramMediaDataUrl(message) {
  const media = message?.media;
  const hasImageMedia = Boolean(message?.photo || media?.photo || (media?.document && /^image\//i.test(cleanText(media?.document?.mimeType))));
  if (!hasImageMedia || typeof message?.downloadMedia !== 'function') {
    return '';
  }

  try {
    const downloaded = await message.downloadMedia({});
    const buffer = Buffer.isBuffer(downloaded) ? downloaded : typeof downloaded === 'string' ? Buffer.from(downloaded) : null;
    const mimeType = cleanText(media?.document?.mimeType) || 'image/jpeg';
    return bufferToDataUrl(buffer, mimeType);
  } catch {
    return '';
  }
}

function buildFallbackDealImageDataUrl({ title = '', price = '', dealType = 'NON_AMAZON' } = {}) {
  const safeTitle = String(title || 'Deal').replace(/[&<>"]/g, ' ').slice(0, 90);
  const safePrice = String(price || 'Preis folgt').replace(/[&<>"]/g, ' ').slice(0, 40);
  const safeDealType = cleanText(dealType).toUpperCase() === 'AMAZON' ? 'Amazon Deal' : 'Deal';
  const svg = `<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="1200" viewBox="0 0 1200 1200">
  <defs>
    <linearGradient id="bg" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0%" stop-color="#102542"/>
      <stop offset="100%" stop-color="#1f6f8b"/>
    </linearGradient>
  </defs>
  <rect width="1200" height="1200" rx="72" fill="url(#bg)"/>
  <circle cx="960" cy="220" r="180" fill="#f4b942" opacity="0.18"/>
  <circle cx="210" cy="980" r="220" fill="#ffffff" opacity="0.08"/>
  <text x="96" y="170" fill="#f6f7fb" font-family="Arial, sans-serif" font-size="54" font-weight="700">${safeDealType}</text>
  <text x="96" y="320" fill="#ffffff" font-family="Arial, sans-serif" font-size="74" font-weight="700">${safeTitle}</text>
  <text x="96" y="470" fill="#dbe7f0" font-family="Arial, sans-serif" font-size="46">Reader Testgruppe</text>
  <rect x="96" y="760" width="520" height="156" rx="28" fill="#ffffff"/>
  <text x="136" y="858" fill="#102542" font-family="Arial, sans-serif" font-size="68" font-weight="700">${safePrice}</text>
</svg>`;

  return `data:image/svg+xml;base64,${Buffer.from(svg).toString('base64')}`;
}

function buildSyntheticReaderDealId({ link = '', title = '', text = '', group = '' } = {}) {
  const seed = [cleanText(link), cleanText(title), cleanText(text).slice(0, 240), cleanText(group)].join('|') || 'reader-deal';
  return crypto.createHash('sha1').update(seed).digest('hex').slice(0, 10).toUpperCase();
}

function resolveReaderDealType({ amazonLink = '', detectedAsin = '' } = {}) {
  return cleanText(amazonLink) || cleanText(detectedAsin) ? 'AMAZON' : 'NON_AMAZON';
}

function collectProtectedSourceMatches(values = []) {
  const matches = [];

  for (const entry of values) {
    const source = cleanText(entry?.source) || 'unknown';
    const value = cleanText(entry?.value);
    if (!value) {
      continue;
    }

    for (const pattern of PROTECTED_SOURCE_PATTERNS) {
      if (pattern.regex.test(value)) {
        matches.push({
          source,
          key: pattern.key,
          value
        });
      }
    }
  }

  return matches;
}

function isProtectedSourceValue(value = '') {
  return collectProtectedSourceMatches([{ source: 'value', value }]).length > 0;
}

function sanitizeProtectedSourceValue(value = '', source = 'unknown') {
  const normalizedValue = cleanText(value);
  if (!normalizedValue) {
    return '';
  }

  if (isProtectedSourceValue(normalizedValue)) {
    console.info('[PROTECTED_SOURCE_TITLE_IGNORED]', {
      source,
      value: normalizedValue.slice(0, 160)
    });
    return '';
  }

  return normalizedValue;
}

function normalizeMatchText(value = '') {
  return sanitizeReaderDescriptionValue(value)
    .toLowerCase()
    .replace(/https?:\/\/\S+/g, ' ')
    .replace(/[^\p{L}\p{N}\s-]+/gu, ' ')
    .replace(/\b(?:jetzt|nur|heute|deal|angebot|amazon|partnerlink|anzeige)\b/gi, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function tokenizeMatchText(value = '') {
  return normalizeMatchText(value)
    .split(/\s+/)
    .filter((token) => token.length >= 3)
    .filter((token, index, allTokens) => allTokens.indexOf(token) === index);
}

function isProtectedSourceStatusCode(statusCode = 0) {
  const numericStatusCode = Number(statusCode);
  return numericStatusCode === 403 || numericStatusCode === 429;
}

function isProtectedSourceHost(value = '') {
  const host = normalizeUrlHost(value);
  return Boolean(host && PROTECTED_SOURCE_HOST_PATTERNS.some((pattern) => pattern.test(host)));
}

function extractSourcePatternCandidate(patterns = [], values = []) {
  for (const value of values) {
    const text = cleanText(value);
    if (!text) {
      continue;
    }

    for (const pattern of patterns) {
      const match = text.match(pattern);
      const candidate = cleanText(match?.[1] || match?.[0] || '');
      if (candidate) {
        return candidate;
      }
    }
  }

  return '';
}

function resolveSourceBrandCandidate(title = '') {
  return (
    tokenizeMatchText(title).find(
      (token) =>
        /^[\p{L}][\p{L}\p{N}-]{2,}$/iu.test(token) && !SOURCE_BRAND_STOPWORDS.has(token.toLowerCase())
    ) || ''
  );
}

function resolveSourceModelCandidate(title = '') {
  return tokenizeMatchText(title).find((token) => /[a-z]/i.test(token) && /\d/.test(token) && token.length >= 4) || '';
}

function resolveSourceMatchBasis(sourceFacts = {}) {
  if (cleanText(sourceFacts.asinCandidate)) {
    return 'asin';
  }
  if (cleanText(sourceFacts.brandCandidate) && cleanText(sourceFacts.modelCandidate)) {
    return 'brand_model';
  }
  if (cleanText(sourceFacts.titleCandidate) && sourceFacts.priceValue !== null) {
    return 'title_price';
  }
  if (cleanText(sourceFacts.titleCandidate)) {
    return 'title_only';
  }

  return 'none';
}

function resolveSourceMatchReasonLabel(matchBasis = '') {
  const normalizedMatchBasis = cleanText(matchBasis).toLowerCase();

  if (normalizedMatchBasis === 'asin') {
    return 'ASIN';
  }
  if (normalizedMatchBasis === 'brand_model') {
    return 'Marke-Modell';
  }
  if (normalizedMatchBasis === 'title_price') {
    return 'Titel / Preis';
  }
  if (normalizedMatchBasis === 'title_only') {
    return 'Titel';
  }

  return 'Unbekannt';
}

function buildSourceMatchReasonSummary(sourceFacts = {}, candidate = {}) {
  const parts = [];

  if (cleanText(sourceFacts.asinCandidate) && cleanText(candidate.asin).toUpperCase() === cleanText(sourceFacts.asinCandidate).toUpperCase()) {
    parts.push('ASIN');
  } else if (sourceFacts.matchBasis === 'brand_model') {
    parts.push('Marke-Modell');
  } else if (cleanText(sourceFacts.titleCandidate)) {
    parts.push('Titel');
  }

  if (sourceFacts.priceValue !== null && candidate.priceValue !== null) {
    parts.push('Preis');
  }

  return Array.from(new Set(parts)).join(' / ') || resolveSourceMatchReasonLabel(sourceFacts.matchBasis);
}

function buildAmazonSearchQuery(sourceFacts = {}) {
  if (sourceFacts.matchBasis === 'asin') {
    return cleanText(sourceFacts.asinCandidate || sourceFacts.query || '');
  }

  if (sourceFacts.matchBasis === 'brand_model') {
    return [sourceFacts.brandCandidate, sourceFacts.modelCandidate].filter(Boolean).join(' ').trim();
  }

  return cleanText(sourceFacts.titleCandidate || sourceFacts.query || '');
}

function isAmazonFallbackNoiseLine(value = '') {
  const line = cleanText(value);
  if (!line) {
    return true;
  }

  if (/^https?:\/\//i.test(line)) {
    return true;
  }

  if (/^(?:[\p{Extended_Pictographic}\uFE0F\s]+)$/u.test(line)) {
    return true;
  }

  if (/^(?:top|mega|hot|blitz|super)\s+deal$/i.test(line)) {
    return true;
  }

  if (/^(?:preisfehler|coupon|gutschein|rabatt(?:-badge)?|dealalarm)$/i.test(line)) {
    return true;
  }

  if (/^-?\d{1,3}(?:[.,]\d{1,2})?\s*%$/i.test(line)) {
    return true;
  }

  if (/\d+(?:[.,]\d{1,2})?\s*€/i.test(line) && (/\bstatt\b/i.test(line) || /-?\d{1,3}\s*%/i.test(line))) {
    return true;
  }

  if (/^(?:jetzt\s+f(?:u|ue)r\s+nur|nur|statt)\b/i.test(line) && /\d+(?:[.,]\d{1,2})?\s*€/i.test(line)) {
    return true;
  }

  return false;
}

function sanitizeAmazonFallbackQueryLine(value = '') {
  return sanitizeReaderDescriptionValue(value)
    .replace(/https?:\/\/\S+/gi, ' ')
    .replace(/[\p{Extended_Pictographic}\uFE0F]/gu, ' ')
    .replace(/\b(?:top|mega|hot|blitz|super)\s+deal\b/gi, ' ')
    .replace(/\b(?:preisfehler|coupon|gutschein|dealalarm|sale|angebot|rabatt(?:-badge)?|rabattcode|sparabo|sparen)\b/gi, ' ')
    .replace(/\b(?:jetzt|nur|heute|statt|code|couponcode|gutscheincode|aktion|deal)\b/gi, ' ')
    .replace(/\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?\s*€/gi, ' ')
    .replace(/-?\d{1,3}(?:[.,]\d{1,2})?\s*%/g, ' ')
    .replace(/[^\p{L}\p{N}\s/-]+/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function cleanSourceTitleCandidateLine(value = '') {
  return sanitizeReaderDescriptionValue(value)
    .replace(/https?:\/\/\S+/gi, ' ')
    .replace(/[\p{Extended_Pictographic}\uFE0F]/gu, ' ')
    .replace(/\b(?:top|mega|hot|super|blitz)\s+deal\b/gi, ' ')
    .replace(/\bpreisfehler\b/gi, ' ')
    .replace(/\b(?:jetzt|heute)\b/gi, ' ')
    .replace(/\b(?:anzeige|partnerlink)\b/gi, ' ')
    .replace(/\b(?:coupon|couponcode|gutschein|gutscheincode|rabattcode|rabatt|spar-?abo)\b[:\s-]*[A-Z0-9-]{0,20}/gi, ' ')
    .replace(/\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?\s*â‚¬\s*(?:statt|von)\s*\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?\s*â‚¬\s*-?\d{1,3}(?:[.,]\d{1,2})?\s*%?/gi, ' ')
    .replace(/\b(?:nur|statt)\s+\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?\s*â‚¬\b/gi, ' ')
    .replace(/-?\d{1,3}(?:[.,]\d{1,2})?\s*%/g, ' ')
    .replace(/[|•]+/g, ' ')
    .replace(/[^\p{L}\p{N}\s/+().,-]+/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, 240);
}

function isSourceTitleNoiseLine(value = '') {
  const line = cleanText(value);
  if (!line) {
    return true;
  }

  if (isAmazonFallbackNoiseLine(line)) {
    return true;
  }

  if (/\b(?:anzeige|partnerlink)\b/i.test(line) && !/[a-z0-9]{4,}/i.test(line.replace(/\b(?:anzeige|partnerlink)\b/gi, ''))) {
    return true;
  }

  if (/\b(?:coupon|couponcode|gutschein|gutscheincode|rabattcode|spar-?abo)\b/i.test(line) && !/[a-z]{3,}.*\d|[a-z]{4,}/i.test(line)) {
    return true;
  }

  return false;
}

function selectSourceTitleCandidate({ previewTitle = '', scrapedTitle = '', originalText = '', fallback = '' } = {}) {
  const lineCandidates = [];
  const rawLines = String(originalText || '')
    .split(/\r?\n/)
    .map((line) => cleanText(line))
    .filter(Boolean);

  if (cleanText(previewTitle)) {
    lineCandidates.push({ source: 'previewTitle', rawValue: cleanText(previewTitle) });
  }
  if (cleanText(scrapedTitle)) {
    lineCandidates.push({ source: 'scrapedTitle', rawValue: cleanText(scrapedTitle) });
  }
  for (const line of rawLines) {
    lineCandidates.push({ source: 'originalText', rawValue: line });
  }
  if (cleanText(fallback)) {
    lineCandidates.push({ source: 'fallback', rawValue: cleanText(fallback) });
  }

  for (const candidate of lineCandidates) {
    if (isSourceTitleNoiseLine(candidate.rawValue)) {
      continue;
    }

    const cleanedValue = cleanSourceTitleCandidateLine(candidate.rawValue);
    if (!cleanedValue || isSourceTitleNoiseLine(cleanedValue)) {
      continue;
    }

    const tokenCount = tokenizeMatchText(cleanedValue).length;
    if (cleanedValue.length < 8 && tokenCount < 2) {
      continue;
    }

    return {
      source: candidate.source,
      rawValue: candidate.rawValue,
      cleanedValue
    };
  }

  const cleanedFallback = cleanSourceTitleCandidateLine(fallback || previewTitle || scrapedTitle || '');
  return {
    source: cleanedFallback ? 'fallback' : 'missing',
    rawValue: cleanText(fallback || previewTitle || scrapedTitle || ''),
    cleanedValue: cleanedFallback
  };
}

function buildAmazonFallbackSearchQuery(sourceFacts = {}) {
  const rawLines = [
    cleanText(sourceFacts.titleCandidate),
    ...String(sourceFacts.originalText || '')
      .split(/\r?\n/)
      .map((line) => cleanText(line)),
    cleanText(sourceFacts.title),
    cleanText(sourceFacts.query)
  ].filter(Boolean);
  const cleanedSegments = [];

  for (const line of rawLines) {
    if (isAmazonFallbackNoiseLine(line)) {
      continue;
    }

    const cleanedLine = sanitizeAmazonFallbackQueryLine(line);
    if (cleanedLine) {
      cleanedSegments.push(cleanedLine);
    }

    if (cleanedSegments.length >= 4) {
      break;
    }
  }

  const fallbackTokenSource =
    cleanedSegments.join(' ') ||
    sanitizeAmazonFallbackQueryLine(sourceFacts.titleCandidate || sourceFacts.title || sourceFacts.query || '');
  const tokens = tokenizeMatchText(fallbackTokenSource).slice(0, 8);

  if (tokens.length) {
    return tokens.join(' ').trim();
  }

  return cleanText(fallbackTokenSource)
    .split(/\s+/)
    .filter(Boolean)
    .slice(0, 8)
    .join(' ')
    .trim();
}

function buildAmazonMatchCandidateFromPaapi(result = {}, asin = '') {
  const resolvedAsin = cleanText(result?.asin || asin).toUpperCase();
  const normalizedUrl = cleanText(result?.normalizedUrl || result?.detailPageUrl || (resolvedAsin ? `https://www.amazon.de/dp/${resolvedAsin}` : ''));
  const affiliateUrl = cleanText(result?.affiliateUrl);
  const priceDisplay = normalizeReaderPriceCandidate(result?.priceDisplay || '');

  return {
    asin: resolvedAsin,
    title: sanitizeReaderPostTitle(result?.title),
    imageUrl: cleanText(result?.imageUrl),
    priceValue: parseTelegramLocalizedNumber(priceDisplay),
    priceDisplay,
    normalizedUrl,
    affiliateUrl,
    brand: cleanText(result?.brand),
    titleSource: 'paapi',
    priceSource: priceDisplay ? 'paapi' : 'missing',
    imageSource: cleanText(result?.imageUrl) ? 'paapi' : 'missing',
    candidateSource: 'paapi'
  };
}

function buildAmazonMatchCandidateFromSearch(candidate = {}) {
  return {
    asin: cleanText(candidate?.asin).toUpperCase(),
    title: sanitizeReaderPostTitle(candidate?.title),
    imageUrl: cleanText(candidate?.imageUrl),
    priceValue: Number.isFinite(Number(candidate?.priceValue)) ? Number(candidate.priceValue) : null,
    priceDisplay:
      Number.isFinite(Number(candidate?.priceValue)) && Number(candidate.priceValue) > 0 ? formatPrice(Number(candidate.priceValue)) : '',
    normalizedUrl: cleanText(candidate?.normalizedUrl),
    affiliateUrl: '',
    brand: resolveSourceBrandCandidate(candidate?.title),
    titleSource: 'amazon_search',
    priceSource: Number.isFinite(Number(candidate?.priceValue)) ? 'amazon_search' : 'missing',
    imageSource: cleanText(candidate?.imageUrl) ? 'amazon_search' : 'missing',
    candidateSource: 'amazon_search'
  };
}

function buildAmazonMatchCandidateFromScrapedDeal(scrapedDeal = {}, asin = '') {
  const priceDisplay = normalizeReaderPriceCandidate(
    scrapedDeal?.basePrice || (scrapedDeal?.finalPriceCalculated === true ? scrapedDeal?.finalPrice : '') || scrapedDeal?.price || ''
  );

  return {
    asin: cleanText(scrapedDeal?.asin || asin).toUpperCase(),
    title: sanitizeReaderPostTitle(scrapedDeal?.productTitle || scrapedDeal?.title),
    imageUrl: cleanText(scrapedDeal?.imageUrl),
    priceValue: parseTelegramLocalizedNumber(priceDisplay),
    priceDisplay,
    normalizedUrl: cleanText(scrapedDeal?.normalizedUrl || scrapedDeal?.finalUrl || scrapedDeal?.resolvedUrl),
    affiliateUrl: '',
    brand: resolveSourceBrandCandidate(scrapedDeal?.productTitle || scrapedDeal?.title),
    titleSource: 'amazon_scrape',
    priceSource: priceDisplay ? 'amazon_scrape' : 'missing',
    imageSource: cleanText(scrapedDeal?.imageUrl) ? 'amazon_scrape' : 'missing',
    candidateSource: 'amazon_scrape',
    scrapedDeal
  };
}

function extractSourceProductFacts({
  sessionName = '',
  source = {},
  structuredMessage = {},
  scrapedDeal = {},
  pricing = {},
  originalLink = '',
  detectedAsin = ''
} = {}) {
  const originalText = cleanText(structuredMessage?.text);
  const sourceGroupRef = cleanText(source?.channelRef) || cleanText(structuredMessage?.channelRef);
  const sourceGroupTitle =
    cleanText(source?.channelTitle) || cleanText(structuredMessage?.channelTitle || structuredMessage?.group) || 'Unbekannt';
  const priceValue =
    pricing?.currentPrice !== null && pricing?.currentPrice !== undefined
      ? Number(pricing.currentPrice)
      : parseTelegramLocalizedNumber(scrapedDeal?.price) ?? null;
  const oldPriceValue =
    pricing?.oldPrice !== null && pricing?.oldPrice !== undefined ? Number(pricing.oldPrice) : parseTelegramLocalizedNumber(scrapedDeal?.oldPrice);
  const imageUrl =
    cleanText(structuredMessage?.telegramMediaDataUrl) ||
    cleanText(structuredMessage?.previewImage) ||
    cleanText(scrapedDeal?.previewImage) ||
    cleanText(scrapedDeal?.ogImage) ||
    cleanText(scrapedDeal?.imageUrl);
  const allLinks = [
    cleanText(originalLink),
    cleanText(structuredMessage?.externalLink),
    cleanText(structuredMessage?.previewUrl),
    cleanText(structuredMessage?.link),
    ...(Array.isArray(structuredMessage?.allLinks) ? structuredMessage.allLinks : [])
  ]
    .map((value) => cleanText(value))
    .filter(Boolean);
  const protectedSourceContext = resolveProtectedDealSourceContext({
    source,
    structuredMessage,
    originalLink: cleanText(allLinks[0] || ''),
    originalText
  });
  const titleSelection = selectSourceTitleCandidate({
    previewTitle: sanitizeProtectedSourceValue(structuredMessage?.previewTitle, 'previewTitle'),
    scrapedTitle: sanitizeProtectedSourceValue(scrapedDeal?.title, 'scrapedTitle'),
    originalText,
    fallback: sanitizeProtectedSourceValue(extractTelegramTitle(structuredMessage?.text, structuredMessage?.group), 'telegramTitle')
  });
  const titleCandidate = cleanText(titleSelection.cleanedValue);
  const asinCandidate = cleanText(detectedAsin || allLinks.map((value) => extractAsin(value)).find(Boolean) || extractAsin(originalText)).toUpperCase();
  const brandCandidate = resolveSourceBrandCandidate(titleCandidate);
  const modelCandidate = resolveSourceModelCandidate(titleCandidate);
  const colorCandidate = extractSourcePatternCandidate([/\b(?:farbe|color)[:\s-]*([\p{L}\p{N}-]{2,})/iu], [titleCandidate, originalText]);
  const sizeCandidate = extractSourcePatternCandidate(
    [/\b(?:größe|groesse|size)[:\s-]*([\p{L}\p{N}.,/-]{1,20})/iu],
    [titleCandidate, originalText]
  );
  const quantityCandidate = extractSourcePatternCandidate(
    [/\b(\d+\s?(?:x|er|stk|stück|pcs|pack|ml|l|g|kg))\b/iu],
    [titleCandidate, originalText]
  );
  const discountCandidate =
    pricing?.detectedDiscount !== null && pricing?.detectedDiscount !== undefined ? Number(pricing.detectedDiscount) : null;
  const couponCandidate = extractTelegramCouponCode(originalText);
  const sparAboCandidate = /\b(?:spar-?abo|subscribe\s*(?:&|and)?\s*save)\b/i.test(originalText);
  const host = normalizeUrlHost(allLinks[0] || '');
  const matchBasis = resolveSourceMatchBasis({
    asinCandidate,
    brandCandidate,
    modelCandidate,
    titleCandidate,
    priceValue
  });
  const query = buildAmazonSearchQuery({
    asinCandidate,
    brandCandidate,
    modelCandidate,
    titleCandidate,
    priceValue,
    matchBasis
  });
  const sourceFacts = {
    sourceGroupRef,
    sourceGroupTitle,
    protectedDealSourceKey: protectedSourceContext.sourceKey || '',
    protectedDealSourceLabel: protectedSourceContext.sourceLabel || '',
    protectedDealSourceMatched: protectedSourceContext.matched === true,
    originalText,
    originalLink: cleanText(allLinks[0] || ''),
    messageId: cleanText(structuredMessage?.messageId),
    imageUrl,
    title: titleCandidate,
    titleCandidate,
    priceValue,
    priceCandidate: priceValue !== null ? formatPrice(priceValue) : '',
    oldPriceCandidate: oldPriceValue !== null ? formatPrice(oldPriceValue) : '',
    discountCandidate,
    couponCandidate,
    sparAboCandidate,
    brandCandidate,
    modelCandidate,
    asinCandidate,
    colorCandidate,
    sizeCandidate,
    quantityCandidate,
    host,
    matchBasis,
    matchReasonLabel: resolveSourceMatchReasonLabel(matchBasis),
    query
  };

  console.info('[SOURCE_POST_DATA_EXTRACTED]', {
    sessionName,
    sourceId: source?.id ?? null,
    messageId: sourceFacts.messageId || '',
    sourceGroupRef: sourceFacts.sourceGroupRef || null,
    sourceGroupTitle: sourceFacts.sourceGroupTitle || 'Unbekannt',
    originalLink: sourceFacts.originalLink || null,
    matchBasis: sourceFacts.matchBasis,
    hasImage: Boolean(sourceFacts.imageUrl),
    brandCandidate: sourceFacts.brandCandidate || null,
    modelCandidate: sourceFacts.modelCandidate || null,
    asinCandidate: sourceFacts.asinCandidate || null,
    protectedDealSource: sourceFacts.protectedDealSourceLabel || null
  });
  console.info('[SOURCE_TITLE_CANDIDATE_CLEANED]', {
    sessionName,
    sourceId: source?.id ?? null,
    messageId: sourceFacts.messageId || '',
    source: titleSelection.source || 'missing',
    rawTitleCandidate: cleanText(titleSelection.rawValue || '').slice(0, 180) || null,
    cleanedTitleCandidate: sourceFacts.titleCandidate || null
  });
  console.info('[SOURCE_TITLE_CANDIDATE]', {
    sessionName,
    sourceId: source?.id ?? null,
    messageId: sourceFacts.messageId || '',
    titleCandidate: sourceFacts.titleCandidate || null
  });
  console.info('[SOURCE_PRICE_CANDIDATE_FOUND]', {
    sessionName,
    sourceId: source?.id ?? null,
    messageId: sourceFacts.messageId || '',
    priceCandidate: sourceFacts.priceCandidate || null,
    oldPriceCandidate: sourceFacts.oldPriceCandidate || null,
    discountCandidate: sourceFacts.discountCandidate,
    couponCandidate: sourceFacts.couponCandidate || null,
    sparAboCandidate: sourceFacts.sparAboCandidate === true
  });
  console.info('[SOURCE_PRICE_CANDIDATE]', {
    sessionName,
    sourceId: source?.id ?? null,
    messageId: sourceFacts.messageId || '',
    priceCandidate: sourceFacts.priceCandidate || null,
    oldPriceCandidate: sourceFacts.oldPriceCandidate || null,
    discountCandidate: sourceFacts.discountCandidate
  });
  console.info('[SOURCE_IMAGE_CANDIDATE]', {
    sessionName,
    sourceId: source?.id ?? null,
    messageId: sourceFacts.messageId || '',
    imageCandidate: sourceFacts.imageUrl || null
  });
  console.info('[SOURCE_ASIN_CANDIDATE]', {
    sessionName,
    sourceId: source?.id ?? null,
    messageId: sourceFacts.messageId || '',
    asinCandidate: sourceFacts.asinCandidate || null
  });

  return sourceFacts;
}

function parseAmazonSearchPrice(chunk = '') {
  const whole = cleanText(chunk.match(/a-price-whole[^>]*>\s*([^<]+)/i)?.[1] || '');
  const fraction = cleanText(chunk.match(/a-price-fraction[^>]*>\s*([^<]+)/i)?.[1] || '');
  if (!whole) {
    return null;
  }

  return parseTelegramLocalizedNumber(`${whole.replace(/[^\d]/g, '')},${fraction.replace(/[^\d]/g, '').slice(0, 2) || '00'}`);
}

function extractAmazonSearchCandidates(html = '') {
  const candidates = [];
  const resultMatches = html.matchAll(/<div[^>]+data-asin=["']([A-Z0-9]{10})["'][^>]+data-component-type=["']s-search-result["'][^>]*>([\s\S]*?)<\/div>\s*<\/div>/gi);

  for (const match of resultMatches) {
    const asin = cleanText(match?.[1] || '').toUpperCase();
    const chunk = match?.[2] || '';
    if (!asin || !chunk) {
      continue;
    }

    const title =
      sanitizeReaderDescriptionValue(
        chunk.match(/<h2[^>]*>[\s\S]*?<span[^>]*>([^<]+)<\/span>/i)?.[1] ||
          chunk.match(/<img[^>]+alt=["']([^"']+)["']/i)?.[1] ||
          ''
      ) || '';
    const imageUrl = cleanText(chunk.match(/<img[^>]+class=["'][^"']*s-image[^"']*["'][^>]+src=["']([^"']+)["']/i)?.[1] || '');
    const priceValue = parseAmazonSearchPrice(chunk);

    candidates.push({
      asin,
      title,
      imageUrl,
      priceValue,
      normalizedUrl: `https://www.amazon.de/dp/${asin}`
    });
  }

  return candidates;
}

function computeAmazonProductMatchScore(sourceFacts = {}, candidate = {}) {
  const normalizedSourceAsin = cleanText(sourceFacts.asinCandidate).toUpperCase();
  const normalizedCandidateAsin = cleanText(candidate.asin).toUpperCase();
  if (normalizedSourceAsin && normalizedCandidateAsin && normalizedSourceAsin === normalizedCandidateAsin) {
    return 100;
  }

  const sourceTokens = tokenizeMatchText(sourceFacts.titleCandidate || sourceFacts.title || sourceFacts.query || '');
  const candidateTokens = tokenizeMatchText(candidate.title || '');
  const candidateTokenSet = new Set(candidateTokens);
  const overlappingTokens = sourceTokens.filter((token) => candidateTokenSet.has(token));
  const titleOverlapRatio = sourceTokens.length > 0 ? overlappingTokens.length / sourceTokens.length : 0;
  let score = 0;

  if (titleOverlapRatio >= 0.85) {
    score += 20;
  } else if (titleOverlapRatio >= 0.65) {
    score += 16;
  } else if (titleOverlapRatio >= 0.45) {
    score += 10;
  } else if (titleOverlapRatio >= 0.25) {
    score += 5;
  }

  const sourceBrand = cleanText(sourceFacts.brandCandidate).toLowerCase();
  if (sourceBrand) {
    if (candidateTokenSet.has(sourceBrand)) {
      score += 20;
    } else {
      score -= 50;
    }
  }

  const sourceModel = cleanText(sourceFacts.modelCandidate).toLowerCase();
  if (sourceModel) {
    if (candidate.title.toLowerCase().includes(sourceModel)) {
      score += 40;
    } else if (titleOverlapRatio < 0.2) {
      score -= 40;
    }
  }

  if (sourceFacts.priceValue !== null && candidate.priceValue !== null) {
    const delta = Math.abs(candidate.priceValue - sourceFacts.priceValue);
    const ratio = sourceFacts.priceValue > 0 ? delta / sourceFacts.priceValue : 1;
    if (ratio <= 0.03) {
      score += 15;
    } else if (ratio <= 0.08) {
      score += 10;
    } else if (ratio <= 0.15) {
      score += 5;
    } else if (ratio >= 0.3) {
      score -= 30;
    }
  }

  if (cleanText(candidate.imageUrl)) {
    score += 10;
  }

  const sourceRoleInfo = extractProductRole(sourceFacts.titleCandidate || sourceFacts.title || '', []);
  const candidateRoleInfo = extractProductRole(candidate.title, candidate.features || []);
  const roleComparison = compareSimilarProductRoles({
    originalRoleInfo: sourceRoleInfo,
    candidateRoleInfo,
    originalTitle: sourceFacts.titleCandidate || sourceFacts.title || '',
    candidateTitle: candidate.title,
    candidateAsin: candidate.asin
  });
  if (!roleComparison.allowed) {
    return 0;
  }

  const sourceQuantityInfo = extractQuantityInfo(sourceFacts.titleCandidate || sourceFacts.title || '', [], {});
  const candidateQuantityInfo = extractQuantityInfo(
    candidate.title,
    candidate.features || [],
    candidate.rawItem?.ItemInfo || candidate.rawItem?.itemInfo || candidate.rawItem || candidate
  );
  const quantityComparison = compareSimilarQuantityInfo({
    originalQuantityInfo: sourceQuantityInfo,
    candidateQuantityInfo,
    originalPrice: sourceFacts.priceValue,
    candidatePrice: candidate.priceValue,
    originalTitle: sourceFacts.titleCandidate || sourceFacts.title || '',
    candidateTitle: candidate.title,
    candidateAsin: candidate.asin
  });
  if (!quantityComparison.allowed) {
    return 0;
  }

  const variantCandidates = [cleanText(sourceFacts.colorCandidate), cleanText(sourceFacts.sizeCandidate), cleanText(sourceFacts.quantityCandidate)]
    .filter(Boolean)
    .map((value) => value.toLowerCase());
  if (variantCandidates.some((value) => candidate.title.toLowerCase().includes(value))) {
    score += 10;
  }

  if (sourceTokens.length >= 4 && titleOverlapRatio < 0.2) {
    score -= 40;
  }

  return Math.max(0, Math.min(100, Math.round(score)));
}

function computeAmazonFallbackMatchScore(sourceFacts = {}, candidate = {}) {
  const normalizedSourceAsin = cleanText(sourceFacts.asinCandidate).toUpperCase();
  const normalizedCandidateAsin = cleanText(candidate.asin).toUpperCase();
  if (normalizedSourceAsin && normalizedCandidateAsin && normalizedSourceAsin === normalizedCandidateAsin) {
    return 100;
  }

  const baseScore = computeAmazonProductMatchScore(sourceFacts, candidate);
  const sourceTokens = tokenizeMatchText(sourceFacts.titleCandidate || sourceFacts.title || sourceFacts.query || '');
  const candidateTokens = tokenizeMatchText(candidate.title || '');
  const candidateTokenSet = new Set(candidateTokens);
  const overlappingTokens = sourceTokens.filter((token) => candidateTokenSet.has(token));
  const titleOverlapRatio = sourceTokens.length > 0 ? overlappingTokens.length / sourceTokens.length : 0;
  const normalizedCandidateTitle = cleanText(candidate.title).toLowerCase();
  const sourceBrand = cleanText(sourceFacts.brandCandidate).toLowerCase();
  const sourceModel = cleanText(sourceFacts.modelCandidate).toLowerCase();
  const brandMatched = Boolean(sourceBrand && candidateTokenSet.has(sourceBrand));
  const modelMatched = Boolean(sourceModel && normalizedCandidateTitle.includes(sourceModel));
  const hasBrandOrModelMatch = brandMatched || modelMatched;
  let score = baseScore;

  if (titleOverlapRatio >= 0.6) {
    score = Math.max(score, 72);
  }

  if (hasBrandOrModelMatch) {
    score = Math.max(score, 74);
  }

  if (titleOverlapRatio >= 0.6 && hasBrandOrModelMatch) {
    score = Math.max(score, 86);
  }

  if (sourceFacts.priceValue !== null && candidate.priceValue !== null) {
    const delta = Math.abs(candidate.priceValue - sourceFacts.priceValue);
    const ratio = sourceFacts.priceValue > 0 ? delta / sourceFacts.priceValue : 1;

    if (ratio <= 0.15 && titleOverlapRatio >= 0.45) {
      score = Math.max(score, 82);
    }
    if (ratio <= 0.08 && titleOverlapRatio >= 0.6) {
      score = Math.max(score, 90);
    }
  }

  if (cleanText(sourceFacts.imageUrl) && cleanText(candidate.imageUrl) && score >= 70) {
    score = Math.min(100, score + 5);
  }

  return Math.max(0, Math.min(100, Math.round(score)));
}

function logAmazonSearchCandidates({
  sessionName = '',
  source = {},
  structuredMessage = {},
  searchQuery = '',
  scoredCandidates = []
} = {}) {
  const bestCandidate = scoredCandidates[0] || null;

  for (const candidate of scoredCandidates.slice(0, 3)) {
    console.info('[AMAZON_MATCH_CANDIDATE_FOUND]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      asin: candidate.asin || '',
      matchScore: candidate.matchScore ?? 0,
      candidateSource: candidate.candidateSource || 'amazon_search',
      title: cleanText(candidate.title).slice(0, 160) || null
    });
    console.info('[AMAZON_MATCH_CANDIDATE]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      asin: candidate.asin || '',
      matchScore: candidate.matchScore ?? 0,
      candidateSource: candidate.candidateSource || 'amazon_search',
      title: cleanText(candidate.title).slice(0, 160) || null
    });
  }

  console.info('[PRODUCT_MATCH_SCORE]', {
    sessionName,
    sourceId: source?.id ?? null,
    messageId: structuredMessage?.messageId || '',
    bestAsin: bestCandidate?.asin || '',
    matchScore: bestCandidate?.matchScore ?? 0,
    query: searchQuery || ''
  });
}

async function fetchAmazonSearchCandidatesForQuery({
  sessionName = '',
  source = {},
  structuredMessage = {},
  sourceFacts = {},
  searchQuery = '',
  searchMode = 'title',
  fallbackMode = false
} = {}) {
  const normalizedQuery = cleanText(searchQuery);
  const scoringSourceFacts = fallbackMode
    ? {
        ...sourceFacts,
        title: normalizedQuery || sourceFacts.title,
        titleCandidate: normalizedQuery || sourceFacts.titleCandidate,
        query: normalizedQuery || sourceFacts.query
      }
    : sourceFacts;

  if (!normalizedQuery) {
    return {
      query: '',
      candidates: [],
      scoredCandidates: [],
      bestCandidate: null,
      scoringSourceFacts,
      blocked: false,
      blockedReason: ''
    };
  }

  if (searchMode === 'brand_model') {
    console.info('[AMAZON_MATCH_BY_TITLE]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      query: normalizedQuery,
      matchBasis: 'brand_model'
    });
    console.info('[AMAZON_SEARCH_BY_BRAND_MODEL]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      query: normalizedQuery
    });
  } else {
    console.info('[AMAZON_MATCH_BY_TITLE]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      query: normalizedQuery,
      matchBasis: 'title'
    });
    console.info('[AMAZON_SEARCH_BY_TITLE]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      query: normalizedQuery
    });
  }

  const response = await fetch(`https://www.amazon.de/s?k=${encodeURIComponent(normalizedQuery)}`, {
    headers: AMAZON_SEARCH_FETCH_HEADERS
  });
  const html = await response.text();
  const protectedMatches = collectProtectedSourceMatches([
    { source: fallbackMode === true ? 'amazon-search-fallback' : 'amazon-search', value: html.slice(0, 2000) }
  ]);

  if (protectedMatches.length || isProtectedSourceStatusCode(response?.status)) {
    return {
      query: normalizedQuery,
      candidates: [],
      scoredCandidates: [],
      bestCandidate: null,
      scoringSourceFacts,
      blocked: true,
      blockedReason: protectedMatches.length ? 'Amazon-Suche wurde durch Schutzseite blockiert.' : `Amazon-Suche antwortete mit ${response?.status || 0}.`
    };
  }

  const candidates = extractAmazonSearchCandidates(html).slice(0, 10);
  const scoredCandidates = candidates
    .map((candidate) => {
      const normalizedCandidate = buildAmazonMatchCandidateFromSearch(candidate);
      const matchScore = fallbackMode
        ? computeAmazonFallbackMatchScore(scoringSourceFacts, normalizedCandidate)
        : computeAmazonProductMatchScore(scoringSourceFacts, normalizedCandidate);

      return {
        ...normalizedCandidate,
        matchScore
      };
    })
    .sort((left, right) => right.matchScore - left.matchScore);

  return {
    query: normalizedQuery,
    candidates,
    scoredCandidates,
    bestCandidate: scoredCandidates[0] || null,
    scoringSourceFacts,
    blocked: false,
    blockedReason: ''
  };
}

function detectProductIntelligenceCategory(title = '') {
  const normalized = cleanText(title).toLowerCase();
  const matchedCategory = PRODUCT_INTELLIGENCE_CATEGORIES.find((entry) =>
    entry.keywords.some((keyword) => normalized.includes(keyword.toLowerCase()))
  );

  return matchedCategory?.category || 'Unbekannt';
}

function extractProductIntelligenceAttributes(title = '') {
  const text = cleanText(title);
  const lower = text.toLowerCase();
  const capacityMatch = text.match(/(\d{4,6})\s*m\s*a\s*h/i);
  const wattMatch = text.match(/(\d{2,3})\s*w(?:att)?\b/i);
  const colorMatch = text.match(/\b(schwarz|weiss|weiß|blau|rot|grau|silber|green|black|white|blue|red|grey|gray)\b/i);
  const sizeMatch = text.match(/\b(xs|s|m|l|xl|xxl|\d{2,3}\s?cm|\d{1,2}\s?zoll)\b/i);

  return {
    capacityMah: capacityMatch ? `${capacityMatch[1]}mAh` : '',
    watt: wattMatch ? `${wattMatch[1]}W` : '',
    usbC: /\busb[\s-]?c\b/i.test(text),
    pd: /\bpd\b|power\s*delivery/i.test(text),
    fastCharging: /schnellladen|fast\s*charg/i.test(lower),
    anc: /\banc\b|noise\s*cancell/i.test(lower),
    color: colorMatch ? colorMatch[1] : '',
    size: sizeMatch ? sizeMatch[1] : ''
  };
}

function buildProductIntelligenceAttributeKey(category = '', attributes = {}) {
  const parts = [];

  if (attributes.capacityMah) {
    parts.push(attributes.capacityMah);
  }
  if (attributes.watt) {
    parts.push(attributes.watt);
  }
  if (attributes.usbC) {
    parts.push('USBC');
  }
  if (attributes.pd) {
    parts.push('PD');
  }
  if (attributes.anc) {
    parts.push('ANC');
  }
  if (attributes.size) {
    parts.push(cleanText(attributes.size).replace(/\s+/g, ''));
  }
  if (attributes.color) {
    parts.push(cleanText(attributes.color).toUpperCase());
  }

  return parts.length ? parts.join('_') : cleanText(category || 'UNKNOWN').toUpperCase();
}

function buildProductIntelligenceSearchQuery(category = '', brand = '', attributes = {}, title = '') {
  const parts = [];

  if (category && category !== 'Unbekannt') {
    parts.push(category === 'Kopfhoerer' ? 'bluetooth kopfhoerer' : category);
  }
  if (brand) {
    parts.unshift(brand);
  }
  if (attributes.capacityMah) {
    parts.push(attributes.capacityMah);
  }
  if (attributes.usbC) {
    parts.push('usb c');
  }
  if (attributes.watt) {
    parts.push(attributes.watt);
  }
  if (attributes.pd) {
    parts.push('pd');
  }
  if (attributes.anc) {
    parts.push('anc');
  }

  return sanitizeAmazonFallbackQueryLine(parts.join(' ') || title)
    .replace(/%/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, 140);
}

function buildProductIntelligenceProfile(generatorInput = {}, scrapedDeal = {}) {
  const title = cleanText(generatorInput?.title || scrapedDeal?.productTitle || scrapedDeal?.title);
  const asin = cleanText(generatorInput?.asin || scrapedDeal?.asin).toUpperCase();
  const brand = cleanText(scrapedDeal?.brand || scrapedDeal?.paapiBrand || generatorInput?.brand || resolveSourceBrandCandidate(title));
  const category = detectProductIntelligenceCategory(title);
  const attributes = extractProductIntelligenceAttributes(title);
  const attributeKey = buildProductIntelligenceAttributeKey(category, attributes);
  const searchQuery = buildProductIntelligenceSearchQuery(category, brand, attributes, title);

  console.info('[PRODUCT_INTELLIGENCE_START]', {
    asin,
    titlePreview: title.slice(0, 140)
  });
  console.info('[PRODUCT_CATEGORY_DETECTED]', {
    asin,
    category,
    titlePreview: title.slice(0, 140)
  });
  console.info('[PRODUCT_ATTRIBUTES_EXTRACTED]', {
    asin,
    category,
    attributeKey,
    attributes
  });
  console.info('[SIMILAR_SEARCH_QUERY_BUILT]', {
    asin,
    category,
    attributeKey,
    query: searchQuery
  });

  return {
    category,
    brand,
    title,
    asin,
    price: parseTelegramLocalizedNumber(generatorInput?.currentPrice || scrapedDeal?.price || scrapedDeal?.basePrice),
    attributes,
    attributeKey,
    searchQuery
  };
}

function getProductIntelligenceMaster(profile = {}) {
  if (!profile.category || !profile.attributeKey) {
    return null;
  }

  const row = db
    .prepare(
      `
        SELECT *
        FROM product_intelligence_baselines
        WHERE category = ?
          AND attributeKey = ?
          AND COALESCE(brand, '') = COALESCE(?, '')
          AND isMasterBaseline = 1
          AND ignoredAsPriceError = 0
        ORDER BY price ASC, lastSeenAt DESC
        LIMIT 1
      `
    )
    .get(profile.category, profile.attributeKey, cleanText(profile.brand));

  if (row) {
    console.info('[BASELINE_MASTER_FOUND]', {
      category: row.category,
      attributeKey: row.attributeKey,
      brand: row.brand || '',
      asin: row.asin || '',
      price: row.price,
      sellerClass: row.sellerClass,
      similarityScore: row.similarityScore
    });
  }

  return row || null;
}

function upsertProductIntelligenceBaseline({
  profile = {},
  candidate = {},
  sellerClass = '',
  similarityScore = 100,
  source = '',
  isMasterBaseline = false,
  ignoredAsPriceError = false
} = {}) {
  const timestamp = nowIso();
  const asin = cleanText(candidate.asin || profile.asin).toUpperCase();
  const price = Number(candidate.price ?? profile.price);

  if (!profile.category || !profile.attributeKey || !asin || !Number.isFinite(price)) {
    return null;
  }

  const existing = db
    .prepare(
      `
        SELECT *
        FROM product_intelligence_baselines
        WHERE category = ?
          AND attributeKey = ?
          AND COALESCE(brand, '') = COALESCE(?, '')
          AND asin = ?
        LIMIT 1
      `
    )
    .get(profile.category, profile.attributeKey, cleanText(candidate.brand || profile.brand), asin);

  if (isMasterBaseline) {
    db.prepare(
      `
        UPDATE product_intelligence_baselines
        SET isMasterBaseline = 0
        WHERE category = ?
          AND attributeKey = ?
          AND COALESCE(brand, '') = COALESCE(?, '')
      `
    ).run(profile.category, profile.attributeKey, cleanText(candidate.brand || profile.brand));
  }

  if (existing) {
    db.prepare(
      `
        UPDATE product_intelligence_baselines
        SET title = @title,
            price = @price,
            sellerClass = @sellerClass,
            similarityScore = @similarityScore,
            source = @source,
            lastSeenAt = @lastSeenAt,
            timesSeen = timesSeen + 1,
            isMasterBaseline = @isMasterBaseline,
            ignoredAsPriceError = @ignoredAsPriceError
        WHERE id = @id
      `
    ).run({
      id: existing.id,
      title: cleanText(candidate.title || profile.title),
      price,
      sellerClass,
      similarityScore,
      source,
      lastSeenAt: timestamp,
      isMasterBaseline: isMasterBaseline ? 1 : 0,
      ignoredAsPriceError: ignoredAsPriceError ? 1 : 0
    });
    return db.prepare(`SELECT * FROM product_intelligence_baselines WHERE id = ?`).get(existing.id) || null;
  }

  const result = db.prepare(
    `
      INSERT INTO product_intelligence_baselines (
        category,
        attributeKey,
        brand,
        title,
        asin,
        price,
        sellerClass,
        similarityScore,
        source,
        firstSeenAt,
        lastSeenAt,
        timesSeen,
        isMasterBaseline,
        ignoredAsPriceError
      ) VALUES (
        @category,
        @attributeKey,
        @brand,
        @title,
        @asin,
        @price,
        @sellerClass,
        @similarityScore,
        @source,
        @firstSeenAt,
        @lastSeenAt,
        1,
        @isMasterBaseline,
        @ignoredAsPriceError
      )
    `
  ).run({
    category: profile.category,
    attributeKey: profile.attributeKey,
    brand: cleanText(candidate.brand || profile.brand),
    title: cleanText(candidate.title || profile.title),
    asin,
    price,
    sellerClass,
    similarityScore,
    source,
    firstSeenAt: timestamp,
    lastSeenAt: timestamp,
    isMasterBaseline: isMasterBaseline ? 1 : 0,
    ignoredAsPriceError: ignoredAsPriceError ? 1 : 0
  });

  return db.prepare(`SELECT * FROM product_intelligence_baselines WHERE id = ?`).get(result.lastInsertRowid) || null;
}

function maybeStoreProductIntelligenceMaster({
  profile = {},
  candidate = {},
  sellerClass = '',
  similarityScore = 100,
  source = ''
} = {}) {
  const normalizedSellerClass = normalizeSimilarSellerClass(sellerClass);
  const price = Number(candidate.price ?? profile.price);

  if (similarityScore < 70 || !['FBA', 'AMAZON_DIRECT'].includes(normalizedSellerClass) || !Number.isFinite(price)) {
    return {
      stored: false,
      priceErrorProtected: false,
      master: getProductIntelligenceMaster(profile),
      reason: 'Master-Regel nicht erfuellt.'
    };
  }

  const currentMaster = getProductIntelligenceMaster(profile);
  const priceErrorProtected =
    currentMaster?.price && Number(currentMaster.price) > 0 && price <= Number(currentMaster.price) * 0.5;

  if (priceErrorProtected) {
    const protectedRow = upsertProductIntelligenceBaseline({
      profile,
      candidate,
      sellerClass: normalizedSellerClass,
      similarityScore,
      source,
      isMasterBaseline: false,
      ignoredAsPriceError: true
    });
    console.info('[BASELINE_PRICE_ERROR_PROTECTED]', {
      category: profile.category,
      attributeKey: profile.attributeKey,
      asin: candidate.asin || profile.asin,
      price,
      masterPrice: currentMaster.price,
      ignoredAsPriceError: true
    });
    return {
      stored: false,
      priceErrorProtected: true,
      master: currentMaster,
      row: protectedRow,
      reason: 'Preis liegt mindestens 50% unter Master und wurde als Preisfehler geschuetzt.'
    };
  }

  const shouldCreateMaster = !currentMaster;
  const shouldUpdateMaster = currentMaster && price < Number(currentMaster.price || Infinity);
  const row = upsertProductIntelligenceBaseline({
    profile,
    candidate,
    sellerClass: normalizedSellerClass,
    similarityScore,
    source,
    isMasterBaseline: shouldCreateMaster || shouldUpdateMaster,
    ignoredAsPriceError: false
  });

  if (shouldCreateMaster) {
    console.info('[BASELINE_MASTER_CREATED]', {
      category: profile.category,
      attributeKey: profile.attributeKey,
      asin: row?.asin || '',
      price: row?.price ?? price,
      sellerClass: normalizedSellerClass
    });
  } else if (shouldUpdateMaster) {
    console.info('[BASELINE_MASTER_UPDATED]', {
      category: profile.category,
      attributeKey: profile.attributeKey,
      previousAsin: currentMaster.asin || '',
      previousPrice: currentMaster.price,
      asin: row?.asin || '',
      price: row?.price ?? price,
      sellerClass: normalizedSellerClass
    });
  }

  return {
    stored: Boolean(row),
    priceErrorProtected: false,
    master: row || currentMaster,
    row,
    reason: shouldCreateMaster ? 'Erster Master gespeichert.' : shouldUpdateMaster ? 'Master aktualisiert.' : 'Fund gespeichert, Master bleibt bestehen.'
  };
}

function normalizeSimilarSellerClass(value = '') {
  const sellerClass = cleanText(value).toUpperCase();

  if (sellerClass === 'FBA_THIRDPARTY') {
    return 'FBA';
  }

  if (sellerClass === 'FBM_THIRDPARTY') {
    return 'FBM';
  }

  return sellerClass || 'UNKNOWN';
}

function isSimilarProductTestModeActive() {
  const runtimeConfig = getReaderRuntimeConfig();
  return runtimeConfig.readerTestMode === true || process.env.READER_TEST_MODE === '1';
}

function resolveSimilarProductEligibility(generatorInput = {}, generatorContext = {}) {
  const sellerClass = normalizeSimilarSellerClass(
    generatorInput.sellerClass ||
      generatorContext?.seller?.sellerClass ||
      generatorContext?.decisionPolicy?.seller?.sellerClass ||
      generatorInput.sellerType
  );
  const optimizedTestModeRelaxed = isSimilarProductTestModeActive();

  if (sellerClass === 'FBA') {
    if (optimizedTestModeRelaxed) {
      console.info('[OPTIMIZED_TEST_MODE_RELAXED]', {
        sellerClass,
        allowed: true,
        reason: 'READER_TEST_MODE erlaubt Optimierte Deals fuer alle Nicht-FBM Seller.'
      });
    }
    return {
      allowed: true,
      checked: true,
      sellerClass,
      reason: 'FBA Deal: Similar Product Check aktiv.',
      fbmExcluded: false
    };
  }

  if (sellerClass === 'FBM' || sellerClass.includes('FBM')) {
    console.info('[OPTIMIZED_SKIP_FBM]', {
      sellerClass: 'FBM',
      allowed: false,
      reason: 'FBM bleibt fuer Optimierte Deals komplett blockiert.'
    });
    return {
      allowed: false,
      checked: optimizedTestModeRelaxed,
      sellerClass: 'FBM',
      reason: 'FBM bleibt fuer Optimierte Deals komplett blockiert.',
      fbmExcluded: true
    };
  }

  if (optimizedTestModeRelaxed) {
    const normalizedSellerClass = sellerClass || 'UNKNOWN';
    console.info('[OPTIMIZED_TEST_MODE_RELAXED]', {
      sellerClass: normalizedSellerClass,
      allowed: true,
      reason: 'READER_TEST_MODE erlaubt Optimierte Deals fuer alle Nicht-FBM Seller.'
    });
    if (normalizedSellerClass === 'UNKNOWN') {
      console.info('[OPTIMIZED_ALLOW_UNKNOWN]', {
        sellerClass: normalizedSellerClass,
        allowed: true
      });
    }
    if (normalizedSellerClass === 'FBA_OR_AMAZON_UNKNOWN') {
      console.info('[OPTIMIZED_ALLOW_FBA_UNKNOWN]', {
        sellerClass: normalizedSellerClass,
        allowed: true
      });
    }
    return {
      allowed: true,
      checked: true,
      sellerClass: normalizedSellerClass,
      reason: 'READER_TEST_MODE: Nicht-FBM Seller fuer Optimierte Deals erlaubt.',
      fbmExcluded: false
    };
  }

  if (sellerClass === 'AMAZON_DIRECT') {
    return {
      allowed: SIMILAR_PRODUCT_ALLOW_AMAZON_DIRECT === true,
      checked: SIMILAR_PRODUCT_ALLOW_AMAZON_DIRECT === true,
      sellerClass,
      reason: SIMILAR_PRODUCT_ALLOW_AMAZON_DIRECT
        ? 'Amazon Direct per SIMILAR_PRODUCT_ALLOW_AMAZON_DIRECT aktiviert.'
        : 'Amazon Direct fuer Optimierte Deals vorbereitet, aktuell deaktiviert.',
      fbmExcluded: false
    };
  }

  return {
    allowed: false,
    checked: false,
    sellerClass: sellerClass || 'UNKNOWN',
    reason: 'SellerClass ist nicht FBA.',
    fbmExcluded: false
  };
}

function buildSimilarProductSearchQuery(generatorInput = {}, scrapedDeal = {}) {
  const title = cleanText(generatorInput?.title || scrapedDeal?.productTitle || scrapedDeal?.title);
  const brand = cleanText(scrapedDeal?.brand || scrapedDeal?.paapiBrand || generatorInput?.brand || resolveSourceBrandCandidate(title));
  const cleanedTitle = sanitizeAmazonFallbackQueryLine(title);
  const tokens = tokenizeMatchText(cleanedTitle)
    .filter((token) => token.length >= 3)
    .filter((token) => !SIMILAR_PRODUCT_QUERY_STOPWORDS.has(token.toLowerCase()))
    .filter((token) => !/^\d{1,3}$/.test(token))
    .slice(0, 9);
  const queryParts = [];

  if (brand) {
    queryParts.push(brand);
  }

  for (const token of tokens) {
    if (!queryParts.some((part) => part.toLowerCase() === token.toLowerCase())) {
      queryParts.push(token);
    }
  }

  return queryParts.join(' ').replace(/%/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 140);
}

function buildSimilarProductShortSearchQuery(generatorInput = {}, scrapedDeal = {}, productIntelligenceProfile = {}) {
  const title = cleanText(generatorInput?.title || scrapedDeal?.productTitle || scrapedDeal?.title);
  const lower = title.toLowerCase();
  const attributes = productIntelligenceProfile?.attributes || extractProductIntelligenceAttributes(title);
  const category = cleanText(productIntelligenceProfile?.category);
  const parts = [];

  if (/\b(kratzbaum|katzenbaum|cat\s*tree)\b/i.test(title)) {
    parts.push('Katzenbaum');
    const heightMatch = title.match(/(\d{2,3})\s*cm/i);
    const height = heightMatch ? Number(heightMatch[1]) : 0;
    if (height >= 180 || /\bxxl\b/i.test(title)) {
      parts.push('XXL');
    }
  } else if (category === 'Powerbank' || /\bpower\s*bank|powerbank\b/i.test(title)) {
    parts.push('Powerbank');
    if (attributes.capacityMah) {
      parts.push(attributes.capacityMah);
    }
    if (attributes.usbC) {
      parts.push('USB C');
    }
    if (attributes.watt) {
      parts.push(attributes.watt);
    }
  } else if (category === 'Kopfhoerer' || /\b(kopfhoerer|kopfhörer|earbuds|bluetooth|anc)\b/i.test(lower)) {
    parts.push('Bluetooth Kopfhoerer');
    if (attributes.anc) {
      parts.push('ANC');
    }
    if (/\b(in\s*ear|inear|earbuds)\b/i.test(lower)) {
      parts.push('in ear');
    }
  } else if (category && category !== 'Unbekannt') {
    parts.push(category);
    if (attributes.watt) {
      parts.push(attributes.watt);
    }
    if (attributes.size) {
      parts.push(attributes.size);
    }
  }

  if (!parts.length) {
    parts.push(...resolveSimilarProductTypeTokens(title).slice(0, 4));
    parts.push(...resolveSimilarCoreFeatureTokens(title, scrapedDeal?.bulletPoints || scrapedDeal?.features || []).slice(0, 3));
  }

  return parts
    .filter(Boolean)
    .join(' ')
    .replace(/%/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, 100);
}

function buildSimilarProductSearchQueries({
  exactQuery = '',
  shortQuery = '',
  testMode = false
} = {}) {
  const queries = [];
  const entries =
    testMode && cleanText(shortQuery)
      ? [{ type: 'short', query: shortQuery }]
      : [{ type: 'exact', query: exactQuery }];

  for (const entry of entries) {
    if (!entry) {
      continue;
    }

    const normalizedQuery = cleanText(entry.query).replace(/\s+/g, ' ').trim();
    if (!normalizedQuery) {
      continue;
    }

    if (!queries.some((item) => item.query.toLowerCase() === normalizedQuery.toLowerCase())) {
      queries.push({
        ...entry,
        query: normalizedQuery
      });
    }
  }

  return queries;
}

function resolveSimilarProductTypeTokens(title = '') {
  return tokenizeMatchText(title)
    .filter((token) => token.length >= 4)
    .filter((token) => !SIMILAR_PRODUCT_QUERY_STOPWORDS.has(token.toLowerCase()))
    .filter((token) => !/^\d+$/.test(token))
    .slice(0, 10);
}

function resolveSimilarCoreFeatureTokens(title = '', features = []) {
  const sourceText = [title, ...(Array.isArray(features) ? features : [])].join(' ');
  return tokenizeMatchText(sourceText)
    .filter((token) => /(?:\d|mah|wh|watt|gb|tb|hz|zoll|usb|type|typ|xl|pro|max|mini)/i.test(token))
    .filter((token) => token.length >= 2)
    .slice(0, 12);
}

function parseSimilarQuantityNumber(value = '') {
  const parsed = Number.parseFloat(cleanText(value).replace(',', '.'));
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

function normalizeSimilarQuantityUnit(unit = '') {
  const normalized = cleanText(unit).toLowerCase();
  if (normalized === 'kg') {
    return { quantityUnit: 'g', multiplier: 1000, unitLabel: 'g' };
  }
  if (normalized === 'g') {
    return { quantityUnit: 'g', multiplier: 1, unitLabel: 'g' };
  }
  if (normalized === 'l') {
    return { quantityUnit: 'ml', multiplier: 1000, unitLabel: 'ml' };
  }
  if (normalized === 'ml') {
    return { quantityUnit: 'ml', multiplier: 1, unitLabel: 'ml' };
  }
  return { quantityUnit: 'stück', multiplier: 1, unitLabel: 'Stück' };
}

function formatSimilarQuantityTotal(total = null, unit = 'stück') {
  const numericTotal = Number(total);
  if (!Number.isFinite(numericTotal) || numericTotal <= 0) {
    return '';
  }

  const rounded = Math.round(numericTotal * 100) / 100;
  if (unit === 'g') {
    return rounded >= 1000 && rounded % 1000 === 0 ? `${rounded / 1000} kg` : `${rounded} g`;
  }
  if (unit === 'ml') {
    return rounded >= 1000 && rounded % 1000 === 0 ? `${rounded / 1000} l` : `${rounded} ml`;
  }
  return `${rounded} Stück`;
}

function buildSimilarQuantityInfo({ total = null, unit = 'stück', raw = '', source = '', multiplier = null, each = null } = {}) {
  const quantityTotal = Number(total);
  if (!Number.isFinite(quantityTotal) || quantityTotal <= 0) {
    return null;
  }

  const quantityUnit = normalizeSimilarQuantityUnit(unit).quantityUnit;
  let result = {
    quantityTotal,
    quantityUnit,
    quantityLabel: formatSimilarQuantityTotal(quantityTotal, quantityUnit),
    rawQuantity: cleanText(raw),
    source: cleanText(source) || 'title',
    multiplier: Number.isFinite(Number(multiplier)) ? Number(multiplier) : null,
    each: Number.isFinite(Number(each)) ? Number(each) : null
  };

  console.info('[QUANTITY_EXTRACTED]', {
    title: cleanText(source).slice(0, 180),
    quantityTotal: result.quantityTotal,
    quantityUnit: result.quantityUnit,
    quantityLabel: result.quantityLabel,
    rawQuantity: result.rawQuantity
  });

  return result;
}

function normalizeSimilarQuantitySource(title = '', features = [], itemInfo = {}) {
  const featureText = Array.isArray(features) ? features.join(' ') : cleanText(features);
  const itemInfoText = [
    itemInfo?.Title?.DisplayValue,
    itemInfo?.title?.displayValue,
    itemInfo?.title,
    ...(Array.isArray(itemInfo?.Features?.DisplayValues) ? itemInfo.Features.DisplayValues : []),
    ...(Array.isArray(itemInfo?.features?.displayValues) ? itemInfo.features.displayValues : [])
  ]
    .map((value) => cleanText(value))
    .filter(Boolean)
    .join(' ');

  return [title, featureText, itemInfoText]
    .map((value) => cleanText(value))
    .filter(Boolean)
    .join(' ')
    .replace(/\u00a0/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function extractQuantityInfo(title = '', features = [], itemInfo = {}) {
  const source = normalizeSimilarQuantitySource(title, features, itemInfo);
  if (!source) {
    return null;
  }

  const patterns = [
    {
      type: 'multiMetric',
      regex: /(\d+(?:[,.]\d+)?)\s*(?:x|×)\s*(\d+(?:[,.]\d+)?)\s*(kg|g|ml|l)\b/i
    },
    {
      type: 'multiCount',
      regex: /(\d+(?:[,.]\d+)?)\s*(?:x|×)\s*(\d+(?:[,.]\d+)?)\s*(?:er\b|stk\.?|stück|stueck|pcs?|karten?|rollen?|beutel|packs?|packungen?|sets?)\b/i
    },
    {
      type: 'count',
      regex: /(\d+(?:[,.]\d+)?)\s*er\s*(?:pack|packung|set|multipack|mehrfachpackung|vorratspack|karton|beutel|box|rollen?|stück|stueck|stk\.?|pcs?|karten?)\b/i
    },
    {
      type: 'count',
      regex: /(\d+(?:[,.]\d+)?)\s*(?:stück|stueck|stk\.?|pcs?|karten?|rollen?|beutel|packs?|packungen?|sets?)\b/i
    },
    {
      type: 'countAfter',
      regex: /(?:packung|pack|set|multipack|mehrfachpackung)\s*(?:mit|à|a)?\s*(\d+(?:[,.]\d+)?)\s*(?:stück|stueck|stk\.?|pcs?|karten?|rollen?)\b/i
    },
    {
      type: 'count',
      regex: /(\d+(?:[,.]\d+)?)\s*[- ]?\s*(?:teilig|tlg\.?|teile)\b/i
    },
    {
      type: 'metric',
      regex: /(\d+(?:[,.]\d+)?)\s*(kg|g|ml|l)\b/i
    }
  ];

  for (const pattern of patterns) {
    const match = source.match(pattern.regex);
    if (!match) {
      continue;
    }

    if (pattern.type === 'multiMetric') {
      const multiplier = parseSimilarQuantityNumber(match[1]);
      const each = parseSimilarQuantityNumber(match[2]);
      const unit = normalizeSimilarQuantityUnit(match[3]);
      if (multiplier && each) {
        return buildSimilarQuantityInfo({
          total: multiplier * each * unit.multiplier,
          unit: unit.quantityUnit,
          raw: match[0],
          source,
          multiplier,
          each
        });
      }
    }

    if (pattern.type === 'multiCount') {
      const multiplier = parseSimilarQuantityNumber(match[1]);
      const each = parseSimilarQuantityNumber(match[2]);
      if (multiplier && each) {
        return buildSimilarQuantityInfo({
          total: multiplier * each,
          unit: 'stück',
          raw: match[0],
          source,
          multiplier,
          each
        });
      }
    }

    if (pattern.type === 'count' || pattern.type === 'countAfter') {
      const total = parseSimilarQuantityNumber(match[1]);
      if (total) {
        return buildSimilarQuantityInfo({
          total,
          unit: 'stück',
          raw: match[0],
          source
        });
      }
    }

    if (pattern.type === 'metric') {
      const total = parseSimilarQuantityNumber(match[1]);
      const unit = normalizeSimilarQuantityUnit(match[2]);
      if (total) {
        return buildSimilarQuantityInfo({
          total: total * unit.multiplier,
          unit: unit.quantityUnit,
          raw: match[0],
          source
        });
      }
    }
  }

  if (/\b(multipack|mehrfachpackung|vorratspack)\b/i.test(source)) {
    console.info('[QUANTITY_EXTRACTED]', {
      title: source.slice(0, 180),
      quantityTotal: null,
      quantityUnit: 'stück',
      quantityLabel: 'Multipack ohne erkannte Anzahl',
      rawQuantity: 'multipack'
    });
    return {
      quantityTotal: null,
      quantityUnit: 'stück',
      quantityLabel: 'Multipack',
      rawQuantity: 'multipack',
      source
    };
  }

  return null;
}

function hasComparableSimilarQuantity(quantityInfo = null) {
  return Boolean(
    quantityInfo &&
      Number.isFinite(Number(quantityInfo.quantityTotal)) &&
      Number(quantityInfo.quantityTotal) > 0 &&
      cleanText(quantityInfo.quantityUnit)
  );
}

function resolveSimilarUnitPrice(price = null, quantityInfo = null) {
  const numericPrice = normalizeSimilarPositivePrice(price);
  if (numericPrice === null || !hasComparableSimilarQuantity(quantityInfo)) {
    return null;
  }

  const total = Number(quantityInfo.quantityTotal);
  const baseAmount = quantityInfo.quantityUnit === 'g' || quantityInfo.quantityUnit === 'ml' ? 100 : 1;
  return Math.round((numericPrice / total) * baseAmount * 10000) / 10000;
}

function formatSimilarUnitPrice(price = null, quantityInfo = null) {
  const unitPrice = resolveSimilarUnitPrice(price, quantityInfo);
  if (unitPrice === null) {
    return '';
  }

  const label = quantityInfo.quantityUnit === 'g' ? '100 g' : quantityInfo.quantityUnit === 'ml' ? '100 ml' : 'Stück';
  return `${formatPrice(unitPrice)} / ${label}`;
}

function compareSimilarQuantityInfo({
  originalQuantityInfo = null,
  candidateQuantityInfo = null,
  originalPrice = null,
  candidatePrice = null,
  originalTitle = '',
  candidateTitle = '',
  candidateAsin = ''
} = {}) {
  const originalHasQuantity = hasComparableSimilarQuantity(originalQuantityInfo);
  const candidateHasQuantity = hasComparableSimilarQuantity(candidateQuantityInfo);
  const originalUnitPrice = formatSimilarUnitPrice(originalPrice, originalQuantityInfo);
  const candidateUnitPrice = formatSimilarUnitPrice(candidatePrice, candidateQuantityInfo);

  if (originalHasQuantity && candidateHasQuantity) {
    console.info('[UNIT_PRICE_COMPARISON]', {
      originalUnitPrice,
      candidateUnitPrice,
      originalQuantity: originalQuantityInfo.quantityLabel,
      candidateQuantity: candidateQuantityInfo.quantityLabel,
      originalQuantityTotal: originalQuantityInfo.quantityTotal,
      candidateQuantityTotal: candidateQuantityInfo.quantityTotal,
      quantityUnit: originalQuantityInfo.quantityUnit,
      candidateAsin
    });
  }

  const buildResult = (allowed, reason, extra = {}) => ({
    allowed,
    rejectReason: allowed ? '' : 'PACK_SIZE_MISMATCH',
    reason,
    originalQuantityLabel: originalQuantityInfo?.quantityLabel || '',
    candidateQuantityLabel: candidateQuantityInfo?.quantityLabel || '',
    originalUnitPrice,
    candidateUnitPrice,
    originalQuantityInfo,
    candidateQuantityInfo,
    ...extra
  });

  if (!originalHasQuantity && !candidateHasQuantity) {
    return buildResult(true, 'Keine Mengenangaben erkannt, Mengencheck neutral.');
  }

  if (originalHasQuantity !== candidateHasQuantity) {
    console.warn('[PACK_SIZE_MISMATCH]', {
      original: originalQuantityInfo?.quantityLabel || 'fehlt',
      candidate: candidateQuantityInfo?.quantityLabel || 'fehlt',
      originalTitle: cleanText(originalTitle).slice(0, 140),
      candidateTitle: cleanText(candidateTitle).slice(0, 140),
      reason: 'Menge nur auf einer Seite erkannt.'
    });
    return buildResult(false, 'Andere oder fehlende Packgröße.');
  }

  if (originalQuantityInfo.quantityUnit !== candidateQuantityInfo.quantityUnit) {
    console.warn('[PACK_SIZE_MISMATCH]', {
      original: originalQuantityInfo.quantityLabel,
      candidate: candidateQuantityInfo.quantityLabel,
      originalUnit: originalQuantityInfo.quantityUnit,
      candidateUnit: candidateQuantityInfo.quantityUnit,
      reason: 'Unterschiedliche Mengeneinheit.'
    });
    return buildResult(false, 'Andere Mengeneinheit.');
  }

  const originalTotal = Number(originalQuantityInfo.quantityTotal);
  const candidateTotal = Number(candidateQuantityInfo.quantityTotal);
  const deviationPercent = originalTotal > 0 ? Math.abs(candidateTotal - originalTotal) / originalTotal : 1;

  if (deviationPercent > 0.15) {
    console.warn('[PACK_SIZE_MISMATCH]', {
      original: originalQuantityInfo.quantityLabel,
      candidate: candidateQuantityInfo.quantityLabel,
      originalQuantityTotal: originalTotal,
      candidateQuantityTotal: candidateTotal,
      deviationPercent: Math.round(deviationPercent * 1000) / 10,
      originalUnitPrice,
      candidateUnitPrice,
      reason: 'Packgröße weicht mehr als 15 Prozent ab.'
    });
    return buildResult(false, 'Andere Packgröße.', {
      deviationPercent: Math.round(deviationPercent * 1000) / 10
    });
  }

  return buildResult(true, 'Menge vergleichbar.', {
    deviationPercent: Math.round(deviationPercent * 1000) / 10
  });
}

function matchSimilarProductRolePattern(source = '', patterns = []) {
  for (const entry of patterns) {
    const match = source.match(entry.regex);
    if (match) {
      return {
        term: match[0],
        reason: entry.reason
      };
    }
  }

  return null;
}

function extractProductRole(title = '', features = []) {
  const source = normalizeSimilarQuantitySource(title, features, {});
  const lowerSource = source.toLowerCase();
  const strongSetMatch = matchSimilarProductRolePattern(lowerSource, [
    { regex: /\bkochgeschirr\s*[- ]?\s*set\b/i, reason: 'Kochgeschirr-Set erkannt.' },
    { regex: /\btopf\s*[- ]?\s*set\b|\btopfset\b/i, reason: 'Topfset erkannt.' },
    { regex: /\bpfannen\s*[- ]?\s*set\b|\bpfannenset\b/i, reason: 'Pfannenset erkannt.' },
    { regex: /\b\d+\s*[- ]?\s*(?:teilig|tlg\.?|teile)\b/i, reason: 'Mehrteiliges Set erkannt.' },
    { regex: /\bmehrteilig(?:es|er|e)?\b/i, reason: 'Mehrteiliges Produkt erkannt.' }
  ]);
  const sparePartMatch = matchSimilarProductRolePattern(lowerSource, [
    { regex: /\bersatz\s*(?:teil|deckel|filter|griff)\b/i, reason: 'Ersatzteil erkannt.' },
    { regex: /\bersatzteil\b|\bspare\s*part\b|\breplacement\b/i, reason: 'Ersatzteil erkannt.' }
  ]);
  const accessoryMatch = matchSimilarProductRolePattern(lowerSource, [
    { regex: /\b(?:topfdeckel|pfannendeckel|schutzdeckel|ersatzdeckel)\b/i, reason: 'Deckel/Zubehoer erkannt.' },
    { regex: /\bdeckel\b|\blid\b/i, reason: 'Deckel/Zubehoer erkannt.' },
    { regex: /\bgriff\b|\bhalterung\b|\baufsatz\b/i, reason: 'Zubehoer erkannt.' },
    { regex: /\bzubeh(?:oe|\u00f6)r\b|\bh(?:ue|\u00fc)lle\b|\btasche\b/i, reason: 'Zubehoer erkannt.' },
    { regex: /\bladekabel\b|\bcharging\s*cable\b|\bcable\b/i, reason: 'Kabel/Zubehoer erkannt.' }
  ]);
  const consumableMatch = matchSimilarProductRolePattern(lowerSource, [
    { regex: /\bersatzfilter\b|\bfilter\b|\bfilterset\b/i, reason: 'Verbrauchsmaterial/Filter erkannt.' }
  ]);
  const weakSetMatch = matchSimilarProductRolePattern(lowerSource, [
    { regex: /\bset\b|\bbundle\b/i, reason: 'Set/Bundle erkannt.' }
  ]);
  let role = 'MAIN_PRODUCT';
  let match = null;

  if (strongSetMatch) {
    role = 'SET_BUNDLE';
    match = strongSetMatch;
  } else if (sparePartMatch) {
    role = 'SPARE_PART';
    match = sparePartMatch;
  } else if (accessoryMatch) {
    role = 'ACCESSORY';
    match = accessoryMatch;
  } else if (consumableMatch) {
    role = 'CONSUMABLE';
    match = consumableMatch;
  } else if (weakSetMatch) {
    role = 'SET_BUNDLE';
    match = weakSetMatch;
  } else if (!source) {
    role = 'UNKNOWN';
  }

  const result = {
    role,
    reason: match?.reason || (role === 'MAIN_PRODUCT' ? 'Normales Hauptprodukt.' : 'Produktrolle nicht erkannt.'),
    matchedTerm: match?.term || '',
    source
  };

  console.info('[PRODUCT_ROLE_EXTRACTED]', {
    title: cleanText(title).slice(0, 180),
    role: result.role,
    matchedTerm: result.matchedTerm,
    reason: result.reason
  });

  return result;
}

function isSimilarAccessoryLikeRole(role = '') {
  return ['ACCESSORY', 'SPARE_PART', 'CONSUMABLE'].includes(cleanText(role).toUpperCase());
}

function formatSimilarProductRoleLabel(role = '') {
  const normalizedRole = cleanText(role).toUpperCase();
  if (normalizedRole === 'SET_BUNDLE') {
    return 'Set/Bundle';
  }
  if (normalizedRole === 'ACCESSORY') {
    return 'Zubehoer';
  }
  if (normalizedRole === 'SPARE_PART') {
    return 'Ersatzteil';
  }
  if (normalizedRole === 'CONSUMABLE') {
    return 'Verbrauchsmaterial';
  }
  if (normalizedRole === 'MAIN_PRODUCT') {
    return 'Hauptprodukt';
  }
  return 'Unbekannt';
}

function compareSimilarProductRoles({
  originalRoleInfo = null,
  candidateRoleInfo = null,
  originalTitle = '',
  candidateTitle = '',
  candidateAsin = ''
} = {}) {
  const originalRole = cleanText(originalRoleInfo?.role).toUpperCase() || 'UNKNOWN';
  const candidateRole = cleanText(candidateRoleInfo?.role).toUpperCase() || 'UNKNOWN';
  const buildResult = (allowed, reason) => ({
    allowed,
    rejectReason: allowed ? '' : 'PRODUCT_ROLE_MISMATCH',
    reason,
    originalRole,
    candidateRole,
    originalRoleLabel: formatSimilarProductRoleLabel(originalRole),
    candidateRoleLabel: formatSimilarProductRoleLabel(candidateRole),
    originalRoleInfo,
    candidateRoleInfo
  });
  let result = buildResult(true, 'Produktrollen vergleichbar.');

  if (originalRole === 'SET_BUNDLE' && candidateRole !== 'SET_BUNDLE') {
    result = buildResult(
      false,
      isSimilarAccessoryLikeRole(candidateRole) ? 'Zubehoer statt Set.' : 'Kandidat ist kein vergleichbares Set.'
    );
  } else if (candidateRole === 'SET_BUNDLE' && originalRole !== 'SET_BUNDLE') {
    result = buildResult(false, 'Set statt Hauptprodukt.');
  } else if ((originalRole === 'MAIN_PRODUCT' || originalRole === 'UNKNOWN') && isSimilarAccessoryLikeRole(candidateRole)) {
    result = buildResult(false, candidateRole === 'SPARE_PART' ? 'Ersatzteil statt Hauptprodukt.' : 'Zubehoer statt Hauptprodukt.');
  } else if (isSimilarAccessoryLikeRole(originalRole) && candidateRole !== originalRole) {
    result = buildResult(false, 'Andere Produktrolle.');
  }

  if (!result.allowed) {
    console.warn('[PRODUCT_ROLE_MISMATCH]', {
      original: result.originalRole,
      candidate: result.candidateRole,
      originalRoleLabel: result.originalRoleLabel,
      candidateRoleLabel: result.candidateRoleLabel,
      originalTitle: cleanText(originalTitle).slice(0, 140),
      candidateTitle: cleanText(candidateTitle).slice(0, 140),
      candidateAsin,
      reason: result.reason
    });
  }

  return result;
}

function isOptimizedVariationSellerAllowed(sellerClass = '', testMode = false) {
  const normalizedSellerClass = normalizeSimilarSellerClass(sellerClass);
  return (
    normalizedSellerClass === 'AMAZON_DIRECT' ||
    normalizedSellerClass === 'FBA' ||
    (testMode === true && normalizedSellerClass === 'FBA_OR_AMAZON_UNKNOWN')
  );
}

function resolveSimilarVariantLabel(variant = {}) {
  const attributeLabel = Array.isArray(variant.variationAttributes)
    ? variant.variationAttributes
        .map((entry) => cleanText(entry?.value || entry?.Value || entry?.displayValue || entry?.DisplayValue))
        .filter(Boolean)
        .join(' / ')
    : '';

  return cleanText(variant.variationLabel) || attributeLabel || shortenSimilarText(variant.title, 70) || cleanText(variant.asin).toUpperCase();
}

function applyVariantToSimilarResult(result = {}, variantPayload = {}) {
  const variant = variantPayload.variant || {};
  const variantPrice = normalizeSimilarPositivePrice(variantPayload.variantPrice);
  const originalPrice = normalizeSimilarPositivePrice(result.originalPriceValue);
  const previousCandidatePrice = normalizeSimilarPositivePrice(result.optimizedPriceValue);

  if (variantPrice === null || originalPrice === null || previousCandidatePrice === null) {
    return result;
  }

  const linkRecord = buildAmazonAffiliateLinkRecord(variant.normalizedUrl || variant.detailPageUrl || variant.asin, {
    asin: variant.asin
  });
  const totalDifferenceAmount = Math.max(0, originalPrice - variantPrice);
  const totalDifferencePercent = originalPrice > 0 ? Math.round((totalDifferenceAmount / originalPrice) * 1000) / 10 : 0;
  const variantExtraDifferenceAmount = Math.max(0, previousCandidatePrice - variantPrice);
  const variantExtraDifferencePercent =
    previousCandidatePrice > 0 ? Math.round((variantExtraDifferenceAmount / previousCandidatePrice) * 1000) / 10 : 0;

  return {
    ...result,
    similarCheaperPrice: formatPrice(variantPrice),
    similarCheaperPriceValue: variantPrice,
    similarCheaperAsin: variant.asin,
    similarCheaperTitle: cleanText(variant.title),
    similarCheaperReason: variantPayload.reason || result.similarCheaperReason,
    similarCheaperSellerClass: variantPayload.sellerDetection?.sellerClass || result.similarCheaperSellerClass,
    similarCheaperShipping: variantPayload.sellerDetection?.shipping || result.similarCheaperShipping,
    similarCheaperSellerSource: variantPayload.sellerDetection?.sellerSource || result.similarCheaperSellerSource,
    similarCheaperMerchantName: variantPayload.sellerDetection?.merchantName || '',
    similarCheaperIsAmazonFulfilled: variantPayload.sellerDetection?.isAmazonFulfilled === true,
    similarCheaperIsPrimeEligible: variantPayload.sellerDetection?.isPrimeEligible === true,
    similarCheaperRawSellerKeysFound: variantPayload.sellerDetection?.rawSellerKeysFound || [],
    similarCheaperAmazonFulfilledLabel: variantPayload.sellerDetection?.amazonFulfilledLabel || '',
    similarCheaperPrimeLabel: variantPayload.sellerDetection?.primeLabel || '',
    similarCheaperScore: variantPayload.scoring?.score ?? result.similarCheaperScore,
    optimizedTitle: cleanText(variant.title),
    optimizedPrice: formatPrice(variantPrice),
    optimizedPriceValue: variantPrice,
    optimizedAsin: variant.asin,
    optimizedAffiliateUrl: linkRecord.valid ? linkRecord.affiliateUrl : cleanText(variant.affiliateUrl),
    optimizedImageUrl: resolveOptimizedCandidateImageUrl(variant) || result.optimizedImageUrl,
    optimizedSellerClass: variantPayload.sellerDetection?.sellerClass || result.optimizedSellerClass,
    optimizedSellerSource: variantPayload.sellerDetection?.sellerSource || result.optimizedSellerSource,
    optimizedMerchantName: variantPayload.sellerDetection?.merchantName || '',
    optimizedIsAmazonFulfilled: variantPayload.sellerDetection?.isAmazonFulfilled === true,
    optimizedIsPrimeEligible: variantPayload.sellerDetection?.isPrimeEligible === true,
    optimizedRawSellerKeysFound: variantPayload.sellerDetection?.rawSellerKeysFound || [],
    similarityScore: variantPayload.scoring?.score ?? result.similarityScore,
    alternativePrice: formatPrice(variantPrice),
    alternativePriceValue: variantPrice,
    alternativeScore: variantPayload.scoring?.score ?? result.alternativeScore,
    alternativeShipping: variantPayload.sellerDetection?.shipping || result.alternativeShipping,
    affiliateUrl: linkRecord.valid ? linkRecord.affiliateUrl : cleanText(variant.affiliateUrl),
    amazonApiTitle: cleanText(variant.title),
    amazonAsin: cleanText(variant.asin).toUpperCase(),
    amazonMerchantName: variantPayload.sellerDetection?.merchantName || '',
    amazonMerchantId: cleanText(variant.merchantId || variant.sellerId || ''),
    amazonIsAmazonFulfilled: variantPayload.sellerDetection?.isAmazonFulfilled === true,
    amazonIsPrimeEligible: variantPayload.sellerDetection?.isPrimeEligible === true,
    amazonSellerClass: variantPayload.sellerDetection?.sellerClass || result.amazonSellerClass,
    amazonSellerSource: variantPayload.sellerDetection?.sellerSource || result.amazonSellerSource,
    differenceAmount: formatPrice(totalDifferenceAmount),
    differencePercent: totalDifferencePercent,
    originalProductRoleInfo: variantPayload.productRoleComparison?.originalRoleInfo || result.originalProductRoleInfo,
    optimizedProductRoleInfo: variantPayload.productRoleComparison?.candidateRoleInfo || result.optimizedProductRoleInfo,
    productRoleComparison: variantPayload.productRoleComparison || result.productRoleComparison,
    productRoleComparable: variantPayload.productRoleComparison?.allowed === true,
    originalProductRole: variantPayload.productRoleComparison?.originalRoleLabel || result.originalProductRole,
    optimizedProductRole: variantPayload.productRoleComparison?.candidateRoleLabel || result.optimizedProductRole,
    originalQuantityInfo: variantPayload.quantityComparison?.originalQuantityInfo || result.originalQuantityInfo,
    optimizedQuantityInfo: variantPayload.quantityComparison?.candidateQuantityInfo || result.optimizedQuantityInfo,
    quantityComparison: variantPayload.quantityComparison || result.quantityComparison,
    quantityComparable: variantPayload.quantityComparison?.allowed === true,
    originalQuantity: variantPayload.quantityComparison?.originalQuantityLabel || result.originalQuantity,
    optimizedQuantity: variantPayload.quantityComparison?.candidateQuantityLabel || result.optimizedQuantity,
    originalUnitPrice: variantPayload.quantityComparison?.originalUnitPrice || result.originalUnitPrice,
    optimizedUnitPrice: variantPayload.quantityComparison?.candidateUnitPrice || result.optimizedUnitPrice,
    rawPriceObject: resolveSimilarCandidateRawPriceObject(variant),
    extractedPrice: variantPrice,
    candidate: variant,
    variantSelected: true,
    variantLabel: variantPayload.label || resolveSimilarVariantLabel(variant),
    previousCandidatePrice: formatPrice(previousCandidatePrice),
    variantPrice: formatPrice(variantPrice),
    variantDifferenceAmount: formatPrice(variantExtraDifferenceAmount),
    variantDifferencePercent: variantExtraDifferencePercent,
    variantSourceAsin: result.optimizedAsin || result.similarCheaperAsin || ''
  };
}

async function findCheapestAllowedVariation({
  finalCandidate = {},
  originalProfile = {},
  currentResult = {},
  testMode = false,
  sessionName = '',
  sourceId = null,
  messageId = ''
} = {}) {
  if ((process.env.SIMILAR_VARIANT_CHECK_ENABLED || '0') !== '1') {
    return null;
  }

  const candidateAsin = cleanText(finalCandidate.asin || currentResult.optimizedAsin || currentResult.similarCheaperAsin).toUpperCase();
  const currentPrice = normalizeSimilarPositivePrice(currentResult.optimizedPriceValue || finalCandidate.priceValue);
  if (!candidateAsin || currentPrice === null) {
    return null;
  }

  console.info('[VARIANT_SCAN_START]', {
    sessionName,
    sourceId,
    messageId,
    asin: candidateAsin,
    currentPrice: formatPrice(currentPrice)
  });

  const variationResult = await loadAmazonAffiliateVariations({
    asin: candidateAsin,
    limit: Number.parseInt(process.env.SIMILAR_VARIANT_LIMIT || '10', 10) || 10
  });

  if (variationResult.status === 'throttled') {
    console.info('[VARIANT_SCAN_DONE]', {
      sessionName,
      sourceId,
      messageId,
      asin: candidateAsin,
      status: 'throttled',
      action: 'keep_original_candidate'
    });
    return null;
  }

  const variants = Array.isArray(variationResult.items) ? variationResult.items : [];
  let bestVariantPayload = null;

  for (const variant of variants) {
    const variantAsin = cleanText(variant.asin).toUpperCase();
    const variantPrice = resolveSimilarCandidatePrice(variant);
    const variantPriceValid = Number.isFinite(variantPrice) && variantPrice > 0;
    const sellerDetection = detectSimilarSellerClass(variant, { testMode });
    const sellerAllowed = isOptimizedVariationSellerAllowed(sellerDetection.sellerClass, testMode);
    const candidateRoleInfo = extractProductRole(variant.title, variant.features || []);
    const productRoleComparison = compareSimilarProductRoles({
      originalRoleInfo: originalProfile.productRoleInfo,
      candidateRoleInfo,
      originalTitle: originalProfile.title,
      candidateTitle: variant.title,
      candidateAsin: variantAsin
    });
    const candidateQuantityInfo = extractQuantityInfo(
      variant.title,
      variant.features || [],
      variant.rawItem?.ItemInfo || variant.rawItem?.itemInfo || variant.rawItem || variant
    );
    const quantityComparison = compareSimilarQuantityInfo({
      originalQuantityInfo: originalProfile.quantityInfo,
      candidateQuantityInfo,
      originalPrice: originalProfile.price,
      candidatePrice: variantPrice,
      originalTitle: originalProfile.title,
      candidateTitle: variant.title,
      candidateAsin: variantAsin
    });
    const scoring = scoreSimilarProductCandidate(originalProfile, variant, {
      testMode,
      sellerDetection
    });
    let rejectReason = '';

    if (!variantPriceValid) {
      rejectReason = 'PRICE_MISSING';
    } else if (variantPrice >= currentPrice) {
      rejectReason = 'PRICE_NOT_CHEAPER';
    } else if (!sellerAllowed) {
      rejectReason = sellerDetection.sellerClass === 'FBM' ? 'candidate_is_fbm' : 'SELLER_NOT_ALLOWED';
    } else if (!productRoleComparison.allowed) {
      rejectReason = 'PRODUCT_ROLE_MISMATCH';
    } else if (!quantityComparison.allowed) {
      rejectReason = 'PACK_SIZE_MISMATCH';
    } else if (scoring.score < SIMILAR_PRODUCT_MIN_SCORE) {
      rejectReason = 'SIMILARITY_TOO_LOW';
    }

    const allowed = !rejectReason;
    const label = resolveSimilarVariantLabel(variant);
    console.info('[VARIANT_CANDIDATE]', {
      asin: variantAsin,
      label,
      price: variantPriceValid ? formatPrice(variantPrice) : null,
      sellerClass: sellerDetection.sellerClass,
      allowed,
      rejectReason,
      similarityScore: scoring.score,
      productRole: productRoleComparison.candidateRoleLabel || '',
      quantity: quantityComparison.candidateQuantityLabel || ''
    });

    if (!allowed) {
      continue;
    }

    if (!bestVariantPayload || variantPrice < bestVariantPayload.variantPrice) {
      bestVariantPayload = {
        variant,
        variantPrice,
        label,
        sellerDetection,
        scoring,
        productRoleComparison,
        quantityComparison,
        reason: `Guenstigste erlaubte Variante gewaehlt: ${label}`
      };
    }
  }

  if (bestVariantPayload && bestVariantPayload.variantPrice < currentPrice) {
    console.info('[VARIANT_CHEAPER_FOUND]', {
      oldPrice: formatPrice(currentPrice),
      newPrice: formatPrice(bestVariantPayload.variantPrice),
      label: bestVariantPayload.label,
      asin: cleanText(bestVariantPayload.variant.asin).toUpperCase()
    });
    console.info('[VARIANT_SCAN_DONE]', {
      sessionName,
      sourceId,
      messageId,
      asin: candidateAsin,
      status: 'cheaper_found',
      selectedAsin: cleanText(bestVariantPayload.variant.asin).toUpperCase(),
      label: bestVariantPayload.label
    });
    return bestVariantPayload;
  }

  console.info('[VARIANT_SCAN_DONE]', {
    sessionName,
    sourceId,
    messageId,
    asin: candidateAsin,
    status: 'no_cheaper_allowed_variant',
    count: variants.length
  });
  return null;
}

function buildSimilarProductProfile(generatorInput = {}, scrapedDeal = {}) {
  const title = cleanText(generatorInput?.title || scrapedDeal?.productTitle || scrapedDeal?.title);
  const features = generatorInput?.features || generatorInput?.bulletPoints || scrapedDeal?.bulletPoints || scrapedDeal?.features || [];
  return {
    asin: cleanText(generatorInput?.asin || scrapedDeal?.asin).toUpperCase(),
    title,
    brand: cleanText(scrapedDeal?.brand || scrapedDeal?.paapiBrand || generatorInput?.brand || resolveSourceBrandCandidate(title)),
    price: normalizeSimilarPositivePrice(generatorInput?.currentPrice || scrapedDeal?.price || scrapedDeal?.basePrice),
    quantityInfo: extractQuantityInfo(title, features, generatorInput?.itemInfo || scrapedDeal?.itemInfo || {}),
    productRoleInfo: extractProductRole(title, features),
    productTypeTokens: resolveSimilarProductTypeTokens(title),
    coreFeatureTokens: resolveSimilarCoreFeatureTokens(title, features)
  };
}

function normalizeSimilarPositivePrice(value = null) {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value > 0 ? value : null;
  }

  const parsed = parseTelegramLocalizedNumber(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

function resolveFirstSimilarPositivePrice(...values) {
  for (const value of values) {
    const parsed = normalizeSimilarPositivePrice(value);
    if (parsed !== null) {
      return parsed;
    }
  }

  return null;
}

function resolveSimilarCandidateRawPriceObject(candidate = {}) {
  const listing = resolveSimilarCandidateListing(candidate);
  return (
    candidate.rawPriceObject ||
    listing?.Price ||
    listing?.price ||
    candidate.rawItem?.Price ||
    candidate.rawItem?.price ||
    {}
  );
}

function resolveSimilarCandidatePrice(candidate = {}) {
  const rawPriceObject = resolveSimilarCandidateRawPriceObject(candidate);
  const priceCandidates = [
    candidate.extractedPrice,
    candidate.priceValue,
    rawPriceObject?.Money?.Amount,
    rawPriceObject?.money?.amount,
    rawPriceObject?.Amount,
    rawPriceObject?.amount,
    candidate.rawItem?.OffersV2?.Listings?.[0]?.Price?.Money?.Amount,
    candidate.rawItem?.OffersV2?.Listings?.[0]?.Price?.Amount,
    candidate.rawItem?.Offers?.Listings?.[0]?.Price?.Amount,
    candidate.rawItem?.offersV2?.listings?.[0]?.price?.money?.amount,
    candidate.rawItem?.offersV2?.listings?.[0]?.price?.amount,
    candidate.rawItem?.offers?.listings?.[0]?.price?.amount,
    candidate.rawItem?.price?.amount,
    candidate.rawItem?.Price?.Amount,
    candidate.priceDisplay,
    rawPriceObject?.DisplayAmount,
    rawPriceObject?.displayAmount,
    rawPriceObject?.Money?.DisplayAmount,
    rawPriceObject?.money?.displayAmount
  ];

  return resolveFirstSimilarPositivePrice(...priceCandidates);
}

function resolveSimilarCandidateListing(candidate = {}) {
  return (
    candidate.rawItem?.OffersV2?.Listings?.[0] ||
    candidate.rawItem?.offersV2?.listings?.[0] ||
    candidate.rawItem?.Offers?.Listings?.[0] ||
    candidate.rawItem?.offers?.listings?.[0] ||
    null
  );
}

function hasSimilarCandidateMerchantInfo(candidate = {}) {
  const listing = resolveSimilarCandidateListing(candidate);
  return Boolean(
    listing?.MerchantInfo ||
      listing?.merchantInfo ||
      candidate.rawItem?.MerchantInfo ||
      candidate.rawItem?.merchantInfo ||
      cleanText(candidate.merchantName)
  );
}

function hasSimilarCandidateDeliveryInfo(candidate = {}) {
  const listing = resolveSimilarCandidateListing(candidate);
  return Boolean(
    listing?.DeliveryInfo ||
      listing?.deliveryInfo ||
      candidate.isAmazonFulfilled === true ||
      candidate.isPrimeEligible === true
  );
}

function resolveSimilarCandidateRawSellerKeys(candidate = {}) {
  const listing = resolveSimilarCandidateListing(candidate);
  const keys = Array.isArray(candidate.rawSellerKeysFound) ? [...candidate.rawSellerKeysFound] : [];
  const addKey = (condition, key) => {
    if (condition) {
      keys.push(key);
    }
  };

  addKey(Boolean(listing?.MerchantInfo || listing?.merchantInfo), 'Offers.Listings.MerchantInfo');
  addKey(Boolean(listing?.MerchantInfo?.Name || listing?.merchantInfo?.name || candidate.merchantName), 'Offers.Listings.MerchantInfo.Name');
  addKey(Boolean(listing?.DeliveryInfo || listing?.deliveryInfo), 'Offers.Listings.DeliveryInfo');
  addKey(
    listing?.DeliveryInfo?.IsAmazonFulfilled !== undefined || listing?.deliveryInfo?.isAmazonFulfilled !== undefined,
    'Offers.Listings.DeliveryInfo.IsAmazonFulfilled'
  );
  addKey(
    listing?.DeliveryInfo?.IsPrimeEligible !== undefined || listing?.deliveryInfo?.isPrimeEligible !== undefined,
    'Offers.Listings.DeliveryInfo.IsPrimeEligible'
  );
  addKey(Boolean(listing?.Availability?.Message || listing?.availability?.message || candidate.availability), 'Offers.Listings.Availability.Message');

  return [...new Set(keys.map((key) => cleanText(key)).filter(Boolean))];
}

function formatSimilarSellerDebugBoolean(value, rawSellerKeysFound = [], keyNeedle = '') {
  if (value === true) {
    return 'JA';
  }

  const keyKnown = rawSellerKeysFound.some((key) => cleanText(key).toLowerCase().includes(keyNeedle.toLowerCase()));
  return keyKnown ? 'NEIN' : 'fehlt';
}

function detectSimilarSellerClass(candidate = {}, options = {}) {
  const testMode = options.testMode === true;
  const asin = cleanText(candidate.asin).toUpperCase();
  const merchantName = cleanText(candidate.merchantName);
  const merchantNameLower = merchantName.toLowerCase();
  const availability = cleanText(candidate.availability).toLowerCase();
  const rawSellerKeysFound = resolveSimilarCandidateRawSellerKeys(candidate);
  const hasMerchantInfo = rawSellerKeysFound.some((key) => key.includes('MerchantInfo'));
  const hasDeliveryInfo = rawSellerKeysFound.some((key) => key.includes('DeliveryInfo'));
  const hasAmazonFulfilledKey = rawSellerKeysFound.some((key) => key.includes('IsAmazonFulfilled'));
  const isAmazonFulfilled = candidate.isAmazonFulfilled === true;
  const isPrimeEligible = candidate.isPrimeEligible === true;
  const configuredSellerClass = normalizeSimilarSellerClass(candidate.sellerClass || candidate.sellerType);
  const explicitFbmSignal =
    configuredSellerClass === 'FBM' ||
    configuredSellerClass.includes('FBM') ||
    /\b(versand durch verkaeufer|versand durch verk\u00e4ufer|ships from seller|seller fulfilled)\b/i.test(availability);
  const merchantIsAmazon = Boolean(merchantName && /amazon/i.test(merchantNameLower));
  const merchantIsThirdParty = Boolean(merchantName && !merchantIsAmazon);
  const merchantIndicatesFbm = merchantIsThirdParty && isAmazonFulfilled !== true;
  let sellerClass = 'UNKNOWN';
  let shipping = 'UNKNOWN';
  let allowed = false;
  let rejectReason = 'SELLER_UNKNOWN';
  let sellerSource = 'Keine Seller Felder in API Response gefunden';

  if (cleanText(candidate.offerApiStatus).toUpperCase() === 'THROTTLED' || cleanText(candidate.offerEnrichmentStatus) === 'api_throttled') {
    return {
      allowed: false,
      sellerClass: 'API_THROTTLED',
      shipping: 'UNKNOWN',
      reason: 'Amazon API gedrosselt, Offer-Daten konnten nicht geladen werden.',
      sellerSource: 'Amazon API gedrosselt, spaeter erneut pruefen.',
      merchantName: '',
      isAmazonFulfilled: false,
      isPrimeEligible: false,
      rawSellerKeysFound,
      rawSellerKeysFoundText: rawSellerKeysFound.length ? rawSellerKeysFound.join(', ') : 'keine Seller-Felder wegen API_LIMIT',
      rejectReason: 'API_THROTTLED',
      amazonFulfilledLabel: 'fehlt',
      primeLabel: 'fehlt',
      oldSellerClassField: configuredSellerClass,
      sellerDataMissing: true
    };
  }

  if ((!rawSellerKeysFound.length || (!hasMerchantInfo && !hasDeliveryInfo)) && candidate.sellerDataMissing !== true) {
    console.warn('[SELLER_DATA_MISSING]', {
      asin,
      api: candidate.dataSource || candidate.sourceLabel || 'paapi',
      availableKeys: rawSellerKeysFound
    });
  }

  if (explicitFbmSignal || merchantIndicatesFbm) {
    sellerClass = 'FBM';
    shipping = 'FBM';
    allowed = false;
    rejectReason = 'SELLER_NOT_ALLOWED';
    sellerSource = explicitFbmSignal
      ? 'Explizites FBM/Drittanbieter-Versand-Signal erkannt.'
      : 'MerchantInfo.Name ist Drittanbieter und IsAmazonFulfilled ist nicht true.';
  } else if (merchantIsAmazon) {
    sellerClass = 'AMAZON_DIRECT';
    shipping = 'Amazon';
    allowed = true;
    rejectReason = '';
    sellerSource = 'MerchantInfo enthaelt Amazon.';
  } else if (isAmazonFulfilled) {
    sellerClass = 'FBA';
    shipping = 'FBA';
    allowed = true;
    rejectReason = '';
    sellerSource = 'DeliveryInfo.IsAmazonFulfilled=true.';
  } else if (/\b(fulfilled by amazon|versand durch amazon|fba)\b/i.test(availability)) {
    sellerClass = 'FBA';
    shipping = 'FBA';
    allowed = true;
    rejectReason = '';
    sellerSource = 'Availability enthaelt Versand durch Amazon/FBA.';
  } else if (isPrimeEligible) {
    sellerClass = 'FBA_OR_AMAZON_UNKNOWN';
    shipping = 'Prime / Amazon unklar';
    allowed = testMode;
    rejectReason = testMode ? '' : 'SELLER_NOT_ALLOWED';
    sellerSource = hasAmazonFulfilledKey
      ? 'Prime=true, aber IsAmazonFulfilled ist nicht true.'
      : 'Prime=true aber MerchantInfo fehlt und IsAmazonFulfilled fehlt.';
  } else if (merchantIsThirdParty) {
    sellerClass = 'UNKNOWN';
    shipping = 'UNKNOWN';
    allowed = false;
    rejectReason = 'SELLER_UNKNOWN';
    sellerSource = 'MerchantInfo vorhanden, aber kein Amazon-Fulfillment-Signal gefunden.';
  }

  return {
    allowed,
    sellerClass,
    shipping,
    reason: sellerSource,
    sellerSource,
    merchantName,
    isAmazonFulfilled,
    isPrimeEligible,
    rawSellerKeysFound,
    rawSellerKeysFoundText: rawSellerKeysFound.length ? rawSellerKeysFound.join(', ') : 'keine Seller-Felder',
    rejectReason,
    amazonFulfilledLabel: formatSimilarSellerDebugBoolean(isAmazonFulfilled, rawSellerKeysFound, 'IsAmazonFulfilled'),
    primeLabel: formatSimilarSellerDebugBoolean(isPrimeEligible, rawSellerKeysFound, 'IsPrimeEligible'),
    oldSellerClassField: configuredSellerClass,
    sellerDataMissing: !rawSellerKeysFound.length || (!hasMerchantInfo && !hasDeliveryInfo)
  };
}

function buildSimilarCandidateDebugRow({
  candidate = {},
  candidateAsin = '',
  candidatePrice = null,
  scoring = {},
  cheaper = false,
  rejectReason = '',
  quantityComparison = {},
  productRoleComparison = {}
} = {}) {
  const rawPriceObject = resolveSimilarCandidateRawPriceObject(candidate);
  const priceValid = Number.isFinite(candidatePrice) && candidatePrice > 0;
  const sellerDetection = scoring.sellerDetection || scoring.shipping?.sellerDetection || detectSimilarSellerClass(candidate);

  return {
    title: cleanText(candidate.title).slice(0, 140),
    asin: candidateAsin || cleanText(candidate.asin).toUpperCase(),
    price: priceValid ? formatPrice(candidatePrice) : null,
    rawPriceObject,
    extractedPrice: priceValid ? candidatePrice : null,
    priceValid,
    seller: sellerDetection.sellerClass,
    sellerClass: sellerDetection.sellerClass,
    sellerSource: sellerDetection.sellerSource,
    merchantName: sellerDetection.merchantName || 'fehlt',
    isAmazonFulfilled: sellerDetection.isAmazonFulfilled === true,
    isPrimeEligible: sellerDetection.isPrimeEligible === true,
    similarityScore: scoring.score ?? null,
    cheaper: cheaper === true,
    rejectReason: rejectReason || '',
    originalProductRole: productRoleComparison.originalRoleLabel || '',
    candidateProductRole: productRoleComparison.candidateRoleLabel || '',
    productRoleComparable: productRoleComparison.allowed === true,
    originalQuantity: quantityComparison.originalQuantityLabel || '',
    candidateQuantity: quantityComparison.candidateQuantityLabel || '',
    originalUnitPrice: quantityComparison.originalUnitPrice || '',
    candidateUnitPrice: quantityComparison.candidateUnitPrice || '',
    offerApiStatus: cleanText(candidate.offerApiStatus).toUpperCase() || '',
    offerEnrichmentStatus: cleanText(candidate.offerEnrichmentStatus) || '',
    offerCacheHit: candidate.offerCacheHit === true,
    rawSellerKeysFound: sellerDetection.rawSellerKeysFound,
    source: candidate.dataSource || candidate.sourceLabel || 'paapi',
    merchantInfoRead: hasSimilarCandidateMerchantInfo(candidate),
    deliveryInfoRead: hasSimilarCandidateDeliveryInfo(candidate)
  };
}

function resolveSimilarCandidateShipping(candidate = {}, options = {}) {
  const sellerDetection = options.sellerDetection || detectSimilarSellerClass(candidate, options);
  return {
    ...sellerDetection,
    sellerDetection
  };
}

function scoreSimilarCandidatePreselect(originalProfile = {}, candidate = {}) {
  const candidateTitle = cleanText(candidate.title);
  const candidateBrand = cleanText(candidate.brand);
  const candidateTypeTokens = resolveSimilarProductTypeTokens(candidateTitle);
  const candidateFeatureTokens = resolveSimilarCoreFeatureTokens(candidateTitle, candidate.features || []);
  const originalTypeSet = new Set(originalProfile.productTypeTokens || []);
  const originalFeatureSet = new Set(originalProfile.coreFeatureTokens || []);
  const typeOverlap = candidateTypeTokens.filter((token) => originalTypeSet.has(token)).length;
  const featureOverlap = candidateFeatureTokens.filter((token) => originalFeatureSet.has(token)).length;
  const sameBrand =
    cleanText(originalProfile.brand) &&
    candidateBrand &&
    cleanText(originalProfile.brand).toLowerCase() === candidateBrand.toLowerCase();

  return (sameBrand ? 40 : 0) + Math.min(30, typeOverlap * 15) + Math.min(30, featureOverlap * 10);
}

function preselectSimilarCandidatesForOfferEnrichment(candidates = [], originalProfile = {}, limit = 10) {
  const safeLimit = Math.max(1, Math.min(10, Number.parseInt(limit || '10', 10) || 10));
  return [...(Array.isArray(candidates) ? candidates : [])]
    .map((candidate, index) => ({
      candidate,
      index,
      preselectScore: scoreSimilarCandidatePreselect(originalProfile, candidate)
    }))
    .sort((left, right) => right.preselectScore - left.preselectScore || left.index - right.index)
    .slice(0, safeLimit)
    .map((entry) => ({
      ...entry.candidate,
      similarPreselectScore: entry.preselectScore
    }));
}

function scoreSimilarProductCandidate(originalProfile = {}, candidate = {}, options = {}) {
  const candidateTitle = cleanText(candidate.title);
  const candidateBrand = cleanText(candidate.brand);
  const candidateTypeTokens = resolveSimilarProductTypeTokens(candidateTitle);
  const candidateFeatureTokens = resolveSimilarCoreFeatureTokens(candidateTitle, candidate.features || []);
  const originalTypeSet = new Set(originalProfile.productTypeTokens || []);
  const originalFeatureSet = new Set(originalProfile.coreFeatureTokens || []);
  const typeOverlap = candidateTypeTokens.filter((token) => originalTypeSet.has(token));
  const featureOverlap = candidateFeatureTokens.filter((token) => originalFeatureSet.has(token));
  const sameBrand =
    cleanText(originalProfile.brand) &&
    candidateBrand &&
    cleanText(originalProfile.brand).toLowerCase() === candidateBrand.toLowerCase();
  let score = 0;

  if (sameBrand) {
    score += 40;
  }

  if (typeOverlap.length) {
    score += 30;
  }

  if (featureOverlap.length) {
    score += Math.min(20, Math.max(8, featureOverlap.length * 7));
  }

  if (candidate.rating || candidate.reviewCount) {
    score += 5;
  }

  const sellerDetection = options.sellerDetection || detectSimilarSellerClass(candidate, options);
  const shipping = resolveSimilarCandidateShipping(candidate, {
    ...options,
    sellerDetection
  });
  if (shipping.allowed) {
    score += 10;
  }

  if (!typeOverlap.length && !sameBrand) {
    return {
      score: 0,
      shipping,
      sellerDetection,
      reason: 'Deutlich anderer Produkttyp.',
      rejectReason: 'CATEGORY_MISMATCH'
    };
  }

  return {
    score: Math.max(0, Math.min(100, Math.round(score))),
    shipping,
    sellerDetection,
    rejectReason: '',
    reason: [
      sameBrand ? 'gleiche Marke' : '',
      typeOverlap.length ? 'gleiche Produktart' : '',
      featureOverlap.length ? `Kernmerkmale: ${featureOverlap.slice(0, 4).join(', ')}` : '',
      shipping.allowed ? 'Amazon/FBA Versand' : ''
    ]
      .filter(Boolean)
      .join(' / ') || 'Aehnliches Produkt'
  };
}

function buildEmptySimilarProductCheck(overrides = {}) {
  return {
    checked: false,
    allowed: false,
    similarCheaperFound: false,
    fbmExcluded: false,
    alternativePrice: '',
    alternativeScore: null,
    alternativeShipping: 'UNKNOWN',
    reason: '',
    ...overrides
  };
}

async function enrichCandidatesWithOfferData(candidates = []) {
  const candidateList = Array.isArray(candidates) ? candidates : [];
  if (!candidateList.length) {
    return candidateList;
  }

  return enrichAmazonAffiliateProductsWithOfferData(candidateList, {
    limit: 3
  });
}

function getSimilarSearchResultLimit() {
  const envLimit = Number.parseInt(process.env.SIMILAR_MAX_SEARCH_RESULTS || '10', 10);
  const limit = Number.isFinite(envLimit) && envLimit > 0 ? envLimit : 10;
  return Math.max(1, Math.min(10, limit));
}

function resolveOptimizedOriginalSourceGroup({ source = {}, structuredMessage = {}, generatorInput = {} } = {}) {
  return (
    cleanText(structuredMessage?.channelRef) ||
    cleanText(structuredMessage?.channelTitle) ||
    cleanText(structuredMessage?.group) ||
    cleanText(generatorInput?.channelRef) ||
    cleanText(generatorInput?.channelTitle) ||
    cleanText(generatorInput?.group) ||
    cleanText(source?.channelRef) ||
    cleanText(source?.channelTitle) ||
    cleanText(source?.name) ||
    'Unbekannt'
  );
}

function resolveOptimizedCandidateImageUrl(candidate = {}) {
  return (
    cleanText(candidate.imageUrl) ||
    cleanText(candidate.image?.url) ||
    cleanText(candidate.images?.primary?.large?.url) ||
    cleanText(candidate.images?.primary?.medium?.url) ||
    cleanText(candidate.images?.primary?.small?.url)
  );
}

function formatSimilarSellerPostBoolean(value, fallback = 'fehlt') {
  if (value === true) {
    return 'JA';
  }

  if (value === false) {
    return 'NEIN';
  }

  return fallback;
}

function formatSimilarSellerKeys(value = []) {
  const keys = Array.isArray(value) ? value.map((entry) => cleanText(entry)).filter(Boolean) : [];
  return keys.length ? keys.join(', ') : 'keine Seller-Felder';
}

function looksLikeTelegramDealText(value = '') {
  const text = cleanText(value);
  return /https?:\/\/|www\.|amazon\.de|bester preis|partnerlink|coupon|angebot|deal|rabatt|preis:/i.test(text) || text.length > 120;
}

function normalizeSimilarDebugPrice(value = null) {
  const parsed = normalizeSimilarPositivePrice(value);
  return parsed === null ? 'fehlt' : formatPrice(parsed);
}

function pickBestSimilarCandidateDebugRow(rows = []) {
  if (!Array.isArray(rows) || !rows.length) {
    return null;
  }

  return [...rows].sort((left, right) => {
    const rightScore = Number(right.similarityScore ?? -1);
    const leftScore = Number(left.similarityScore ?? -1);
    const rightCheaper = right.cheaper === true ? 1 : 0;
    const leftCheaper = left.cheaper === true ? 1 : 0;
    return rightCheaper - leftCheaper || rightScore - leftScore;
  })[0] || null;
}

function buildSimilarDataSources({
  source = {},
  structuredMessage = {},
  generatorInput = {},
  originalProfile = {},
  originalPrice = null
} = {}) {
  return {
    sourceTelegramText: cleanText(structuredMessage?.text || generatorInput?.sourceTelegramText),
    sourceGroup: resolveOptimizedOriginalSourceGroup({ source, structuredMessage, generatorInput }),
    originalTitle: cleanText(originalProfile.title || generatorInput?.title),
    originalPrice: originalPrice === null ? 'n/a' : formatPrice(originalPrice),
    originalUrl: cleanText(generatorInput?.normalizedUrl || generatorInput?.productUrl || generatorInput?.link || structuredMessage?.externalLink),
    originalAsin: cleanText(originalProfile.asin || generatorInput?.asin).toUpperCase()
  };
}

function resolveSimilarOriginalSellerDebug(eligibility = {}, generatorInput = {}, generatorContext = {}) {
  const details =
    generatorInput?.sellerDetails && typeof generatorInput.sellerDetails === 'object'
      ? generatorInput.sellerDetails
      : generatorContext?.seller?.details || {};
  const rawSellerKeysFound = Array.isArray(generatorInput?.sellerDetectionSources)
    ? generatorInput.sellerDetectionSources
    : Array.isArray(details?.detectionSources)
      ? details.detectionSources
      : [];
  const merchantNameCandidate = cleanText(
    generatorInput?.amazonMerchantName ||
      generatorInput?.paapiMerchantInfo ||
      generatorInput?.offerMerchantInfo ||
      details?.paapiMerchantInfo ||
      details?.offerMerchantInfo
  );
  const merchantName = looksLikeTelegramDealText(merchantNameCandidate) ? '' : merchantNameCandidate;
  const shippedKnown = generatorInput?.shippedByAmazon === true || generatorInput?.shippedByAmazon === false;
  const noRealSellerFields = !merchantName && !shippedKnown;
  const fallbackSellerClass = eligibility.sellerClass || cleanText(generatorInput?.sellerClass) || 'UNKNOWN';
  const sellerClass =
    fallbackSellerClass === 'FBA_OR_AMAZON_UNKNOWN' && noRealSellerFields ? 'UNKNOWN' : fallbackSellerClass;
  const sellerSource =
    noRealSellerFields
      ? 'Keine echten Seller Felder in API Response gefunden.'
      : cleanText(generatorInput?.sellerRecognitionMessage) ||
        cleanText(details?.sellerRecognitionMessage) ||
        cleanText(details?.recognitionMessage) ||
        cleanText(eligibility.reason) ||
        cleanText(generatorInput?.sellerDetectionSource) ||
        'Keine Seller-Zusatzinfo verfuegbar.';

  return {
    sellerClass,
    sellerSource,
    merchantName,
    sourceTelegramText: cleanText(generatorInput?.sourceTelegramText),
    amazonFulfilledLabel: generatorInput?.shippedByAmazon === true ? 'JA' : generatorInput?.shippedByAmazon === false ? 'NEIN' : 'fehlt',
    primeLabel: 'fehlt',
    rawSellerKeysFound
  };
}

function resolveSimilarPostSellerDebug(similarCheck = {}, mode = 'optimized') {
  const prefix = mode === 'optimized' ? 'optimized' : '';
  const fallbackPrefix = mode === 'optimized' ? 'similarCheaper' : '';
  const sellerClass =
    cleanText(similarCheck[`${prefix}SellerClass`]) ||
    cleanText(similarCheck[`${fallbackPrefix}SellerClass`]) ||
    cleanText(similarCheck.sellerClass) ||
    'UNKNOWN';
  const sellerSource =
    cleanText(similarCheck[`${prefix}SellerSource`]) ||
    cleanText(similarCheck[`${fallbackPrefix}SellerSource`]) ||
    cleanText(similarCheck.sellerSource) ||
    'Keine Seller Quelle vorhanden.';
  const merchantNameCandidate =
    cleanText(similarCheck[`${prefix}MerchantName`]) ||
    cleanText(similarCheck[`${fallbackPrefix}MerchantName`]) ||
    cleanText(similarCheck.merchantName);
  const merchantName = looksLikeTelegramDealText(merchantNameCandidate) ? '' : merchantNameCandidate;
  const rawSellerKeysFound =
    similarCheck[`${prefix}RawSellerKeysFound`] ||
    similarCheck[`${fallbackPrefix}RawSellerKeysFound`] ||
    similarCheck.rawSellerKeysFound ||
    [];
  const amazonFulfilled =
    similarCheck[`${prefix}IsAmazonFulfilled`] ??
    similarCheck[`${fallbackPrefix}IsAmazonFulfilled`] ??
    similarCheck.isAmazonFulfilled;
  const prime =
    similarCheck[`${prefix}IsPrimeEligible`] ??
    similarCheck[`${fallbackPrefix}IsPrimeEligible`] ??
    similarCheck.isPrimeEligible;

  return {
    sellerClass,
    sellerSource,
    merchantName: merchantName || 'fehlt',
    amazonFulfilled:
      cleanText(similarCheck[`${fallbackPrefix}AmazonFulfilledLabel`] || similarCheck.amazonFulfilledLabel) ||
      formatSimilarSellerPostBoolean(amazonFulfilled),
    prime:
      cleanText(similarCheck[`${fallbackPrefix}PrimeLabel`] || similarCheck.primeLabel) ||
      formatSimilarSellerPostBoolean(prime),
    rawSellerKeysFoundText: formatSimilarSellerKeys(rawSellerKeysFound)
  };
}

async function runSimilarProductOptimizationCheck({
  sessionName = '',
  source = {},
  structuredMessage = {},
  generatorInput = {},
  generatorContext = {},
  scrapedDeal = {}
} = {}) {
  const disabledReason = getOptimizedDealsDisabledReason();
  if (disabledReason) {
    logOptimizedDealsDisabledCheckSkipped({
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      asin: generatorInput?.asin || '',
      reason: disabledReason
    });
    return buildEmptySimilarProductCheck({
      checked: false,
      allowed: false,
      sellerClass: normalizeSimilarSellerClass(generatorInput?.sellerClass || generatorInput?.sellerType),
      originalTitle: cleanText(generatorInput?.title || scrapedDeal?.productTitle || scrapedDeal?.title),
      originalPrice: cleanText(generatorInput?.currentPrice || scrapedDeal?.price || ''),
      reason: disabledReason,
      detail: 'Optimierte Deals sind per OPTIMIZED_DEALS_ENABLED deaktiviert.'
    });
  }

  const eligibility = resolveSimilarProductEligibility(generatorInput, generatorContext);
  const originalProfile = buildSimilarProductProfile(generatorInput, scrapedDeal);
  const productIntelligenceProfile = buildProductIntelligenceProfile(generatorInput, scrapedDeal);
  const similarProductTestMode = isSimilarProductTestModeActive();
  const query = productIntelligenceProfile.searchQuery || buildSimilarProductSearchQuery(generatorInput, scrapedDeal);
  const shortQuery = buildSimilarProductShortSearchQuery(generatorInput, scrapedDeal, productIntelligenceProfile);
  const searchQueries = buildSimilarProductSearchQueries({
    exactQuery: query,
    shortQuery,
    testMode: similarProductTestMode
  });
  const primaryQuery = searchQueries[0]?.query || query || shortQuery || '';
  const strictScoreThreshold = SIMILAR_PRODUCT_MIN_SCORE;
  const minimumPostScoreThreshold = SIMILAR_PRODUCT_MIN_SCORE;
  const searchResultLimit = getSimilarSearchResultLimit();
  const originalSellerDebug = resolveSimilarOriginalSellerDebug(eligibility, generatorInput, generatorContext);
  const originalDataSources = buildSimilarDataSources({
    source,
    structuredMessage,
    generatorInput,
    originalProfile,
    originalPrice: originalProfile.price
  });

  if (similarProductTestMode) {
    console.info('[API_SAVING_MODE_ACTIVE]', {
      strictScoreThreshold,
      minimumPostScoreThreshold,
      searchResultLimit,
      enrichLimit: 3,
      priceRule: 'candidatePrice < originalPrice',
      minimumDifferencePercent: 3,
      fbmBlocked: true,
      debugSpamPosts: false
    });
  }

  console.info('[SIMILAR_PRODUCT_CHECK_ALLOWED]', {
    sessionName,
    sourceId: source?.id ?? null,
    messageId: structuredMessage?.messageId || '',
    sellerClass: eligibility.sellerClass,
    allowed: eligibility.allowed === true,
    reason: eligibility.reason,
    sellerSource: originalSellerDebug.sellerSource,
    merchantName: originalSellerDebug.merchantName || 'fehlt',
    rawSellerKeysFound: originalSellerDebug.rawSellerKeysFound
  });
  console.info('[SIMILAR_PRODUCT_ORIGINAL_SELLER_DEBUG]', {
    sessionName,
    sourceId: source?.id ?? null,
    messageId: structuredMessage?.messageId || '',
    readerTestMode: similarProductTestMode,
    resolvedSellerClass: eligibility.sellerClass,
    generatorSellerClass: generatorInput?.sellerClass || '',
    generatorSellerType: generatorInput?.sellerType || '',
    contextSellerClass:
      generatorContext?.seller?.sellerClass || generatorContext?.decisionPolicy?.seller?.sellerClass || '',
    allowed: eligibility.allowed === true,
    fbmExcluded: eligibility.fbmExcluded === true,
    sellerSource: originalSellerDebug.sellerSource,
    merchantName: originalSellerDebug.merchantName || 'fehlt',
    amazonFulfilled: originalSellerDebug.amazonFulfilledLabel,
    prime: originalSellerDebug.primeLabel,
    rawSellerKeysFound: originalSellerDebug.rawSellerKeysFound
  });

  if (!eligibility.allowed) {
    console.info('[SIMILAR_PRODUCT_OPTIMIZED_CHANNEL_SKIPPED]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      asin: originalProfile.asin,
      sellerClass: originalSellerDebug.sellerClass,
      reason: 'seller_not_fba',
      detail: eligibility.reason
    });
    return buildEmptySimilarProductCheck({
      checked: eligibility.checked,
      allowed: false,
      sellerClass: originalSellerDebug.sellerClass,
      sourceTelegramText: originalDataSources.sourceTelegramText,
      sourceGroup: originalDataSources.sourceGroup,
      originalUrl: originalDataSources.originalUrl,
      originalAsin: originalDataSources.originalAsin,
      sellerSource: originalSellerDebug.sellerSource,
      merchantName: originalSellerDebug.merchantName || '',
      amazonFulfilledLabel: originalSellerDebug.amazonFulfilledLabel,
      primeLabel: originalSellerDebug.primeLabel,
      rawSellerKeysFound: originalSellerDebug.rawSellerKeysFound,
      fbmExcluded: eligibility.fbmExcluded,
      productIntelligence: productIntelligenceProfile,
      originalTitle: originalProfile.title,
      originalPrice: originalProfile.price === null ? 'n/a' : formatPrice(originalProfile.price),
      originalSourceGroup: resolveOptimizedOriginalSourceGroup({ source, structuredMessage, generatorInput }),
      candidateCount: 0,
      bestScore: null,
      query: primaryQuery,
      reason: 'seller_not_fba',
      detail: eligibility.reason
    });
  }

  console.info('[SIMILAR_PRODUCT_SEARCH_STARTED]', {
    sessionName,
    sourceId: source?.id ?? null,
    messageId: structuredMessage?.messageId || '',
    asin: originalProfile.asin,
    sellerClass: eligibility.sellerClass,
    query: primaryQuery,
    shortQuery: similarProductTestMode ? shortQuery : ''
  });
  console.info('[SIMILAR_SEARCH_STARTED]', {
    sessionName,
    sourceId: source?.id ?? null,
    messageId: structuredMessage?.messageId || '',
    asin: originalProfile.asin,
    sellerClass: eligibility.sellerClass,
    query: primaryQuery,
    shortQuery: similarProductTestMode ? shortQuery : ''
  });
  console.info('[SIMILAR_PRODUCT_SEARCH_START]', {
    sessionName,
    sourceId: source?.id ?? null,
    messageId: structuredMessage?.messageId || '',
    asin: originalProfile.asin,
    sellerClass: eligibility.sellerClass,
    query: primaryQuery,
    shortQuery: similarProductTestMode ? shortQuery : ''
  });

  if (!searchQueries.length || originalProfile.price === null) {
    console.info('[SIMILAR_PRODUCT_NO_CHEAPER_FOUND]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      asin: originalProfile.asin,
      reason: !searchQueries.length ? 'no_similar_candidates' : 'no_cheaper_candidate',
      detail: !searchQueries.length ? 'Keine Similar-Search-Query verfuegbar.' : 'Originalpreis fehlt.'
    });
    return buildEmptySimilarProductCheck({
      checked: true,
      allowed: true,
      sellerClass: originalSellerDebug.sellerClass,
      sourceTelegramText: originalDataSources.sourceTelegramText,
      sourceGroup: originalDataSources.sourceGroup,
      originalUrl: originalDataSources.originalUrl,
      originalAsin: originalDataSources.originalAsin,
      sellerSource: originalSellerDebug.sellerSource,
      merchantName: originalSellerDebug.merchantName || '',
      amazonFulfilledLabel: originalSellerDebug.amazonFulfilledLabel,
      primeLabel: originalSellerDebug.primeLabel,
      rawSellerKeysFound: originalSellerDebug.rawSellerKeysFound,
      productIntelligence: productIntelligenceProfile,
      originalTitle: originalProfile.title,
      originalPrice: originalProfile.price === null ? 'n/a' : formatPrice(originalProfile.price),
      originalSourceGroup: resolveOptimizedOriginalSourceGroup({ source, structuredMessage, generatorInput }),
      candidateCount: 0,
      bestScore: null,
      query: primaryQuery,
      shortQuery: similarProductTestMode ? shortQuery : '',
      reason: !searchQueries.length ? 'no_similar_candidates' : 'no_cheaper_candidate',
      detail: !searchQueries.length ? 'Keine Similar-Search-Query verfuegbar.' : 'Originalpreis fehlt.'
    });
  }

  const originalBaselineResult = maybeStoreProductIntelligenceMaster({
    profile: productIntelligenceProfile,
    candidate: {
      asin: originalProfile.asin,
      brand: originalProfile.brand,
      title: originalProfile.title,
      price: originalProfile.price
    },
    sellerClass: eligibility.sellerClass,
    similarityScore: 100,
    source: 'current_deal'
  });

  const searchResults = [];
  const candidateMap = new Map();

  for (const searchQuery of searchQueries) {
    if (searchQuery.type === 'short') {
      console.info('[SIMILAR_PRODUCT_SHORT_QUERY_USED]', {
        sessionName,
        sourceId: source?.id ?? null,
        messageId: structuredMessage?.messageId || '',
        asin: originalProfile.asin,
        exactQuery: query,
        shortQuery: searchQuery.query
      });
    }

    const searchResult = await searchAmazonAffiliateProducts({
      keywords: searchQuery.query,
      itemCount: searchResultLimit
    });
    const items = Array.isArray(searchResult.items) ? searchResult.items : [];
    searchResults.push({
      ...searchResult,
      query: searchQuery.query,
      queryType: searchQuery.type,
      count: items.length
    });

    for (const item of items) {
      const asin = cleanText(item.asin).toUpperCase();
      if (!asin || candidateMap.has(asin)) {
        continue;
      }

      candidateMap.set(asin, {
        ...item,
        similarSearchQuery: searchQuery.query,
        similarSearchQueryType: searchQuery.type
      });
    }
  }

  const rawCandidates = preselectSimilarCandidatesForOfferEnrichment([...candidateMap.values()], originalProfile, searchResultLimit);
  const candidates = await enrichCandidatesWithOfferData(rawCandidates);
  const primarySearchResult = searchResults[0] || {};
  const searchStatus = searchResults.some((entry) => entry.status === 'success')
    ? 'success'
    : primarySearchResult.status || 'unknown';

  console.info('[SIMILAR_PRODUCT_CANDIDATES_FOUND]', {
    sessionName,
    sourceId: source?.id ?? null,
    messageId: structuredMessage?.messageId || '',
    asin: originalProfile.asin,
    query: primaryQuery,
    searchQueries: searchResults.map((entry) => ({
      type: entry.queryType,
      query: entry.query,
      count: entry.count,
      status: entry.status || 'unknown'
    })),
    count: candidates.length,
    status: searchStatus
  });
  console.info('[SIMILAR_PRODUCTS_FOUND]', {
    sessionName,
    sourceId: source?.id ?? null,
    messageId: structuredMessage?.messageId || '',
    asin: originalProfile.asin,
    query: primaryQuery,
    searchQueries: searchResults.map((entry) => ({
      type: entry.queryType,
      query: entry.query,
      count: entry.count,
      status: entry.status || 'unknown'
    })),
    count: candidates.length,
    status: searchStatus
  });

  const enrichmentStats = {
    enrichedCount: candidates.filter((candidate) => candidate.offerEnriched === true && candidate.offerCacheHit !== true).length,
    cacheHits: candidates.filter((candidate) => candidate.offerCacheHit === true).length,
    throttled:
      searchResults.some((entry) => cleanText(entry.status).toLowerCase() === 'throttled') ||
      candidates.some(
        (candidate) =>
          cleanText(candidate.offerApiStatus).toUpperCase() === 'THROTTLED' ||
          cleanText(candidate.offerEnrichmentStatus) === 'api_throttled'
      )
  };

  let bestCandidate = null;
  let bestObservedScore = null;
  let fbmExcluded = false;
  let productRoleMismatchExcluded = false;
  let packSizeMismatchExcluded = false;
  const candidateDebugRows = [];

  for (const candidate of candidates) {
    const candidateAsin = cleanText(candidate.asin).toUpperCase();
    if (!candidateAsin || candidateAsin === originalProfile.asin) {
      continue;
    }

    const candidatePrice = resolveSimilarCandidatePrice(candidate);
    const candidatePriceValid = Number.isFinite(candidatePrice) && candidatePrice > 0;
    const candidateQuantityInfo = extractQuantityInfo(
      candidate.title,
      candidate.features || [],
      candidate.rawItem?.ItemInfo || candidate.rawItem?.itemInfo || candidate.rawItem || candidate
    );
    const candidateRoleInfo = extractProductRole(candidate.title, candidate.features || []);
    const productRoleComparison = compareSimilarProductRoles({
      originalRoleInfo: originalProfile.productRoleInfo,
      candidateRoleInfo,
      originalTitle: originalProfile.title,
      candidateTitle: candidate.title,
      candidateAsin
    });
    const quantityComparison = compareSimilarQuantityInfo({
      originalQuantityInfo: originalProfile.quantityInfo,
      candidateQuantityInfo,
      originalPrice: originalProfile.price,
      candidatePrice,
      originalTitle: originalProfile.title,
      candidateTitle: candidate.title,
      candidateAsin
    });
    const sellerDetection = detectSimilarSellerClass(candidate, {
      testMode: similarProductTestMode
    });
    console.info('[SELLER_DETECTION_RESULT]', {
      asin: candidateAsin,
      sellerClass: sellerDetection.sellerClass,
      source: sellerDetection.sellerSource,
      reason: sellerDetection.reason,
      merchantName: sellerDetection.merchantName || 'fehlt',
      isAmazonFulfilled: sellerDetection.isAmazonFulfilled === true,
      isPrimeEligible: sellerDetection.isPrimeEligible === true,
      rawSellerKeysFound: sellerDetection.rawSellerKeysFound
    });
    const scoring = scoreSimilarProductCandidate(originalProfile, candidate, {
      testMode: similarProductTestMode,
      sellerDetection
    });
    bestObservedScore = bestObservedScore === null ? scoring.score : Math.max(bestObservedScore, scoring.score);
    const cheaper = candidatePriceValid && candidatePrice < originalProfile.price;
    const candidateDifferencePercent =
      cheaper && Number.isFinite(originalProfile.price) && originalProfile.price > 0
        ? Math.round(((originalProfile.price - candidatePrice) / originalProfile.price) * 1000) / 10
        : 0;
    let rejectReason = '';

    if (cleanText(candidate.offerApiStatus).toUpperCase() === 'THROTTLED' || cleanText(candidate.offerEnrichmentStatus) === 'api_throttled') {
      rejectReason = 'API_THROTTLED';
    } else if (!candidatePriceValid) {
      rejectReason = 'PRICE_MISSING';
    } else if (!scoring.shipping.allowed) {
      rejectReason = scoring.shipping.rejectReason || 'SELLER_NOT_ALLOWED';
    } else if (scoring.rejectReason === 'CATEGORY_MISMATCH') {
      rejectReason = 'CATEGORY_MISMATCH';
    } else if (!productRoleComparison.allowed) {
      rejectReason = 'PRODUCT_ROLE_MISMATCH';
    } else if (!quantityComparison.allowed) {
      rejectReason = 'PACK_SIZE_MISMATCH';
    } else if (scoring.score < minimumPostScoreThreshold) {
      rejectReason = 'SIMILARITY_TOO_LOW';
    } else if (!cheaper) {
      rejectReason = 'PRICE_NOT_CHEAPER';
    } else if (candidateDifferencePercent < 3) {
      rejectReason = 'SAVING_TOO_LOW';
    }

    if (candidateDebugRows.length < 10) {
      candidateDebugRows.push(
        buildSimilarCandidateDebugRow({
          candidate,
          candidateAsin,
          candidatePrice,
          scoring,
          cheaper,
          rejectReason,
          quantityComparison,
          productRoleComparison
        })
      );
    }

    console.info('[SIMILAR_CANDIDATE_DEBUG]', {
      asin: candidateAsin,
      title: cleanText(candidate.title).slice(0, 140),
      price: candidatePriceValid ? formatPrice(candidatePrice) : null,
      sellerClass: sellerDetection.sellerClass,
      merchantName: sellerDetection.merchantName || null,
      isAmazonFulfilled: sellerDetection.isAmazonFulfilled === true,
      isPrimeEligible: sellerDetection.isPrimeEligible === true,
      similarity: scoring.score,
      rejectReason,
      offerApiStatus: cleanText(candidate.offerApiStatus).toUpperCase() || '',
      offerEnrichmentStatus: cleanText(candidate.offerEnrichmentStatus) || '',
      offerCacheHit: candidate.offerCacheHit === true,
      sellerSource: sellerDetection.sellerSource,
      rawSellerKeysFound: sellerDetection.rawSellerKeysFound,
      originalProductRole: productRoleComparison.originalRoleLabel || '',
      candidateProductRole: productRoleComparison.candidateRoleLabel || '',
      productRoleComparable: productRoleComparison.allowed === true,
      originalQuantity: quantityComparison.originalQuantityLabel || '',
      candidateQuantity: quantityComparison.candidateQuantityLabel || '',
      originalUnitPrice: quantityComparison.originalUnitPrice || '',
      candidateUnitPrice: quantityComparison.candidateUnitPrice || '',
      quantityComparable: quantityComparison.allowed === true
    });

    console.info('[SIMILAR_PRODUCT_MATCH_SCORE]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      originalAsin: originalProfile.asin,
      asin: candidateAsin,
      searchQuery: candidate.similarSearchQuery || '',
      searchQueryType: candidate.similarSearchQueryType || '',
      category: productIntelligenceProfile.category,
      attributeKey: productIntelligenceProfile.attributeKey,
      score: scoring.score,
      shipping: scoring.shipping.shipping,
      sellerClass: scoring.shipping.sellerClass,
      sellerSource: sellerDetection.sellerSource,
      merchantName: sellerDetection.merchantName || 'fehlt',
      isAmazonFulfilled: sellerDetection.isAmazonFulfilled === true,
      isPrimeEligible: sellerDetection.isPrimeEligible === true,
      rawSellerKeysFound: sellerDetection.rawSellerKeysFound,
      rawPriceObject: resolveSimilarCandidateRawPriceObject(candidate),
      extractedPrice: candidatePrice,
      priceValid: candidatePriceValid,
      originalProductRole: productRoleComparison.originalRoleLabel || '',
      candidateProductRole: productRoleComparison.candidateRoleLabel || '',
      productRoleComparable: productRoleComparison.allowed === true,
      originalQuantity: quantityComparison.originalQuantityLabel || '',
      candidateQuantity: quantityComparison.candidateQuantityLabel || '',
      originalUnitPrice: quantityComparison.originalUnitPrice || '',
      candidateUnitPrice: quantityComparison.candidateUnitPrice || '',
      quantityComparable: quantityComparison.allowed === true,
      offerApiStatus: cleanText(candidate.offerApiStatus).toUpperCase() || '',
      offerEnrichmentStatus: cleanText(candidate.offerEnrichmentStatus) || '',
      offerCacheHit: candidate.offerCacheHit === true,
      cheaper,
      rejectReason,
      reason: scoring.reason
    });

    if (!scoring.shipping.allowed) {
      if (scoring.shipping.sellerClass === 'FBM') {
        fbmExcluded = true;
      }
      console.info(
        scoring.shipping.sellerClass === 'FBM'
          ? '[SIMILAR_PRODUCT_CANDIDATE_REJECTED_FBM]'
          : '[SIMILAR_PRODUCT_CANDIDATE_REJECTED_SELLER]',
        {
        sessionName,
        sourceId: source?.id ?? null,
        messageId: structuredMessage?.messageId || '',
        asin: candidateAsin,
        title: cleanText(candidate.title).slice(0, 140),
        reason: rejectReason,
        legacyReason: scoring.shipping.sellerClass === 'FBM' ? 'candidate_is_fbm' : '',
        shipping: scoring.shipping.shipping,
        sellerClass: scoring.shipping.sellerClass,
        sellerSource: sellerDetection.sellerSource,
        merchantName: sellerDetection.merchantName || 'fehlt',
        isAmazonFulfilled: sellerDetection.isAmazonFulfilled === true,
        isPrimeEligible: sellerDetection.isPrimeEligible === true,
        rawSellerKeysFound: sellerDetection.rawSellerKeysFound,
        detail: scoring.shipping.reason
        }
      );
      continue;
    }

    if (scoring.rejectReason === 'CATEGORY_MISMATCH') {
      continue;
    }

    if (!productRoleComparison.allowed) {
      productRoleMismatchExcluded = true;
      console.info('[SIMILAR_PRODUCT_CANDIDATE_REJECTED_ROLE]', {
        sessionName,
        sourceId: source?.id ?? null,
        messageId: structuredMessage?.messageId || '',
        asin: candidateAsin,
        reason: 'PRODUCT_ROLE_MISMATCH',
        originalRole: productRoleComparison.originalRoleLabel || '',
        candidateRole: productRoleComparison.candidateRoleLabel || '',
        detail: productRoleComparison.reason
      });
      continue;
    }

    if (candidatePriceValid && !quantityComparison.allowed) {
      packSizeMismatchExcluded = true;
      console.info('[SIMILAR_PRODUCT_CANDIDATE_REJECTED_PACK_SIZE]', {
        sessionName,
        sourceId: source?.id ?? null,
        messageId: structuredMessage?.messageId || '',
        asin: candidateAsin,
        reason: 'PACK_SIZE_MISMATCH',
        originalQuantity: quantityComparison.originalQuantityLabel || '',
        candidateQuantity: quantityComparison.candidateQuantityLabel || '',
        originalUnitPrice: quantityComparison.originalUnitPrice || '',
        candidateUnitPrice: quantityComparison.candidateUnitPrice || '',
        detail: quantityComparison.reason
      });
      continue;
    }

    if (scoring.score < minimumPostScoreThreshold) {
      console.info('[SIMILAR_PRODUCT_CANDIDATE_REJECTED_LOW_SCORE]', {
        sessionName,
        sourceId: source?.id ?? null,
        messageId: structuredMessage?.messageId || '',
        asin: candidateAsin,
        reason: 'SIMILARITY_TOO_LOW',
        score: scoring.score,
        threshold: minimumPostScoreThreshold,
        strictThreshold: strictScoreThreshold,
        testMode: similarProductTestMode,
        detail: scoring.reason
      });
      continue;
    }

    if (!candidatePriceValid) {
      console.info('[SIMILAR_PRODUCT_CANDIDATE_REJECTED_PRICE]', {
        sessionName,
        sourceId: source?.id ?? null,
        messageId: structuredMessage?.messageId || '',
        asin: candidateAsin,
        title: cleanText(candidate.title).slice(0, 140),
        reason: 'PRICE_MISSING',
        rawPriceObject: resolveSimilarCandidateRawPriceObject(candidate),
        extractedPrice: candidatePrice,
        priceValid: false
      });
      continue;
    }

    if (candidatePrice >= originalProfile.price) {
      continue;
    }

    if (candidateDifferencePercent < 3) {
      continue;
    }

    const enrichedCandidate = {
      ...candidate,
      priceValue: candidatePrice,
      similarCheaperScore: scoring.score,
      similarCheaperReason: scoring.reason,
      similarCheaperSellerClass: scoring.shipping.sellerClass,
      similarCheaperShipping: scoring.shipping.shipping,
      similarCheaperSellerSource: sellerDetection.sellerSource,
      similarCheaperMerchantName: sellerDetection.merchantName || '',
      similarCheaperIsAmazonFulfilled: sellerDetection.isAmazonFulfilled === true,
      similarCheaperIsPrimeEligible: sellerDetection.isPrimeEligible === true,
      similarCheaperRawSellerKeysFound: sellerDetection.rawSellerKeysFound,
      similarCheaperAmazonFulfilledLabel: sellerDetection.amazonFulfilledLabel,
      similarCheaperPrimeLabel: sellerDetection.primeLabel,
      originalProductRoleInfo: originalProfile.productRoleInfo,
      optimizedProductRoleInfo: candidateRoleInfo,
      productRoleComparison,
      originalProductRole: productRoleComparison.originalRoleLabel || '',
      optimizedProductRole: productRoleComparison.candidateRoleLabel || '',
      productRoleComparable: productRoleComparison.allowed === true,
      originalQuantityInfo: originalProfile.quantityInfo,
      optimizedQuantityInfo: candidateQuantityInfo,
      quantityComparison,
      originalQuantity: quantityComparison.originalQuantityLabel || '',
      optimizedQuantity: quantityComparison.candidateQuantityLabel || '',
      originalUnitPrice: quantityComparison.originalUnitPrice || '',
      optimizedUnitPrice: quantityComparison.candidateUnitPrice || '',
      quantityComparable: quantityComparison.allowed === true,
      softOptimizedDeal: similarProductTestMode && scoring.score < strictScoreThreshold,
      similarSearchQuery: candidate.similarSearchQuery || '',
      similarSearchQueryType: candidate.similarSearchQueryType || ''
    };

    if (!bestCandidate || candidatePrice < bestCandidate.priceValue || (candidatePrice === bestCandidate.priceValue && scoring.score > bestCandidate.similarCheaperScore)) {
      bestCandidate = enrichedCandidate;
    }
  }

  console.info('[SIMILAR_PRODUCT_TOP_CANDIDATES_DEBUG]', {
    sessionName,
    sourceId: source?.id ?? null,
    messageId: structuredMessage?.messageId || '',
    originalAsin: originalProfile.asin,
    originalPrice: formatPrice(originalProfile.price),
    originalSellerClass: eligibility.sellerClass,
    readerTestMode: similarProductTestMode,
    query: primaryQuery,
    shortQuery: similarProductTestMode ? shortQuery : '',
    strictScoreThreshold,
    minimumPostScoreThreshold,
    enrichmentStats,
    candidates: candidateDebugRows
  });

  const bestCandidateDebug = pickBestSimilarCandidateDebugRow(candidateDebugRows);

  if (!bestCandidate) {
    const anyThrottled = enrichmentStats.throttled === true;
    const anySearchError = searchResults.some((entry) => entry.status === 'error' || entry.status === 'api_error' || entry.status === 'throttled');
    const noCheaperReason = anyThrottled
      ? 'API_THROTTLED'
      : productRoleMismatchExcluded
        ? 'PRODUCT_ROLE_MISMATCH'
      : packSizeMismatchExcluded
        ? 'PACK_SIZE_MISMATCH'
        : candidates.length
          ? 'no_cheaper_candidate'
          : anySearchError
            ? 'api_error'
            : 'no_similar_candidates';
    const noCheaperDetail =
      (anyThrottled ? 'Amazon API gedrosselt, spaeter erneut pruefen.' : '') ||
      (productRoleMismatchExcluded ? 'Andere Produktrolle: Zubehoer/Ersatzteil nicht gegen Hauptprodukt oder Set erlaubt.' : '') ||
      (packSizeMismatchExcluded ? 'Andere Packgröße: Kandidat nicht als echter optimierter Deal erlaubt.' : '') ||
      searchResults.find((entry) => cleanText(entry.reason))?.reason ||
      (similarProductTestMode
        ? 'Keine guenstigere erlaubte Amazon/FBA-Testalternative gefunden.'
        : 'Keine guenstigere FBA-Alternative gefunden.');
    console.info('[SIMILAR_PRODUCT_NO_CHEAPER_FOUND]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      asin: originalProfile.asin,
      query: primaryQuery,
      shortQuery: similarProductTestMode ? shortQuery : '',
      fbmExcluded,
      candidateCount: candidates.length,
      enrichedCount: enrichmentStats.enrichedCount,
      cacheHits: enrichmentStats.cacheHits,
      apiStatus: anyThrottled ? 'THROTTLED' : '',
      productRoleMismatchExcluded,
      packSizeMismatchExcluded,
      bestScore: bestObservedScore,
      reason: noCheaperReason,
      detail: noCheaperDetail
    });
    console.info('[SIMILAR_NO_CHEAPER_FOUND]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      asin: originalProfile.asin,
      query: primaryQuery,
      shortQuery: similarProductTestMode ? shortQuery : '',
      sellerClass: eligibility.sellerClass,
      candidateCount: candidates.length,
      enrichedCount: enrichmentStats.enrichedCount,
      cacheHits: enrichmentStats.cacheHits,
      apiStatus: anyThrottled ? 'THROTTLED' : '',
      productRoleMismatchExcluded,
      packSizeMismatchExcluded,
      bestScore: bestObservedScore,
      reason: noCheaperReason,
      detail: noCheaperDetail
    });
    return buildEmptySimilarProductCheck({
      checked: true,
      allowed: true,
      sellerClass: originalSellerDebug.sellerClass,
      sourceTelegramText: originalDataSources.sourceTelegramText,
      sourceGroup: originalDataSources.sourceGroup,
      originalUrl: originalDataSources.originalUrl,
      originalAsin: originalDataSources.originalAsin,
      sellerSource: originalSellerDebug.sellerSource,
      merchantName: originalSellerDebug.merchantName || '',
      amazonFulfilledLabel: originalSellerDebug.amazonFulfilledLabel,
      primeLabel: originalSellerDebug.primeLabel,
      rawSellerKeysFound: originalSellerDebug.rawSellerKeysFound,
      fbmExcluded,
      productRoleMismatchExcluded,
      packSizeMismatchExcluded,
      productIntelligence: productIntelligenceProfile,
      baselineMaster: originalBaselineResult?.master || null,
      originalTitle: originalProfile.title,
      originalPrice: formatPrice(originalProfile.price),
      originalSourceGroup: resolveOptimizedOriginalSourceGroup({ source, structuredMessage, generatorInput }),
      candidateCount: candidates.length,
      enrichedCount: enrichmentStats.enrichedCount,
      cacheHits: enrichmentStats.cacheHits,
      apiStatus: anyThrottled ? 'THROTTLED' : '',
      bestScore: bestObservedScore,
      bestCandidateDebug,
      query: primaryQuery,
      shortQuery: similarProductTestMode ? shortQuery : '',
      reason: noCheaperReason,
      detail: noCheaperDetail
    });
  }

  const bestCandidatePrice = normalizeSimilarPositivePrice(bestCandidate.priceValue);
  const originalPrice = normalizeSimilarPositivePrice(originalProfile.price);

  if (bestCandidatePrice === null || originalPrice === null || bestCandidatePrice >= originalPrice) {
    const rejectReason = bestCandidatePrice === null || originalPrice === null ? 'PRICE_MISSING' : 'PRICE_NOT_CHEAPER';
    console.warn('[SIMILAR_PRODUCT_OPTIMIZED_CHANNEL_SKIPPED]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      originalAsin: originalProfile.asin,
      similarCheaperAsin: bestCandidate?.asin || '',
      reason: rejectReason,
      originalPrice,
      extractedPrice: bestCandidatePrice,
      priceValid: bestCandidatePrice !== null && originalPrice !== null,
      rawPriceObject: resolveSimilarCandidateRawPriceObject(bestCandidate)
    });
    return buildEmptySimilarProductCheck({
      checked: true,
      allowed: true,
      sellerClass: originalSellerDebug.sellerClass,
      sourceTelegramText: originalDataSources.sourceTelegramText,
      sourceGroup: originalDataSources.sourceGroup,
      originalUrl: originalDataSources.originalUrl,
      originalAsin: originalDataSources.originalAsin,
      sellerSource: originalSellerDebug.sellerSource,
      merchantName: originalSellerDebug.merchantName || '',
      amazonFulfilledLabel: originalSellerDebug.amazonFulfilledLabel,
      primeLabel: originalSellerDebug.primeLabel,
      rawSellerKeysFound: originalSellerDebug.rawSellerKeysFound,
      fbmExcluded,
      productIntelligence: productIntelligenceProfile,
      baselineMaster: originalBaselineResult?.master || null,
      originalTitle: originalProfile.title,
      originalPrice: originalPrice === null ? 'n/a' : formatPrice(originalPrice),
      originalSourceGroup: resolveOptimizedOriginalSourceGroup({ source, structuredMessage, generatorInput }),
      candidateCount: candidates.length,
      bestScore: bestObservedScore,
      bestCandidateDebug,
      query: primaryQuery,
      shortQuery: similarProductTestMode ? shortQuery : '',
      reason: rejectReason,
      detail:
        rejectReason === 'PRICE_MISSING'
          ? 'Originalpreis oder Kandidatenpreis fehlt/ist ungueltig.'
          : 'Kandidatenpreis ist nicht guenstiger als Originalpreis.'
    });
  }

  bestCandidate.priceValue = bestCandidatePrice;

  const differenceAmount = Math.max(0, originalPrice - bestCandidatePrice);
  const differencePercent = originalPrice > 0 ? Math.round((differenceAmount / originalPrice) * 1000) / 10 : 0;
  const linkRecord = buildAmazonAffiliateLinkRecord(bestCandidate.normalizedUrl || bestCandidate.detailPageUrl || bestCandidate.asin, {
    asin: bestCandidate.asin
  });
  const alternativeBaselineResult = maybeStoreProductIntelligenceMaster({
    profile: productIntelligenceProfile,
    candidate: {
      asin: bestCandidate.asin,
      brand: bestCandidate.brand,
      title: bestCandidate.title,
      price: bestCandidate.priceValue
    },
    sellerClass: bestCandidate.similarCheaperSellerClass,
    similarityScore: bestCandidate.similarCheaperScore,
    source: 'similar_search'
  });
  const result = {
    checked: true,
    allowed: true,
    sellerClass: eligibility.sellerClass,
    similarCheaperFound: true,
    similarCheaperPrice: formatPrice(bestCandidate.priceValue),
    similarCheaperPriceValue: bestCandidate.priceValue,
    similarCheaperAsin: bestCandidate.asin,
    similarCheaperTitle: cleanText(bestCandidate.title),
    similarCheaperReason: bestCandidate.similarCheaperReason,
    similarCheaperSellerClass: bestCandidate.similarCheaperSellerClass,
    similarCheaperShipping: bestCandidate.similarCheaperShipping,
    similarCheaperSellerSource: bestCandidate.similarCheaperSellerSource || '',
    similarCheaperMerchantName: bestCandidate.similarCheaperMerchantName || '',
    similarCheaperIsAmazonFulfilled: bestCandidate.similarCheaperIsAmazonFulfilled === true,
    similarCheaperIsPrimeEligible: bestCandidate.similarCheaperIsPrimeEligible === true,
    similarCheaperRawSellerKeysFound: bestCandidate.similarCheaperRawSellerKeysFound || [],
    similarCheaperAmazonFulfilledLabel: bestCandidate.similarCheaperAmazonFulfilledLabel || '',
    similarCheaperPrimeLabel: bestCandidate.similarCheaperPrimeLabel || '',
    similarCheaperScore: bestCandidate.similarCheaperScore,
    optimizedTitle: cleanText(bestCandidate.title),
    optimizedPrice: formatPrice(bestCandidate.priceValue),
    optimizedPriceValue: bestCandidate.priceValue,
    optimizedAsin: bestCandidate.asin,
    optimizedAffiliateUrl: linkRecord.valid ? linkRecord.affiliateUrl : cleanText(bestCandidate.affiliateUrl),
    optimizedImageUrl: resolveOptimizedCandidateImageUrl(bestCandidate),
    optimizedSellerClass: bestCandidate.similarCheaperSellerClass,
    optimizedSellerSource: bestCandidate.similarCheaperSellerSource || '',
    optimizedMerchantName: bestCandidate.similarCheaperMerchantName || '',
    optimizedIsAmazonFulfilled: bestCandidate.similarCheaperIsAmazonFulfilled === true,
    optimizedIsPrimeEligible: bestCandidate.similarCheaperIsPrimeEligible === true,
    optimizedRawSellerKeysFound: bestCandidate.similarCheaperRawSellerKeysFound || [],
    similarityScore: bestCandidate.similarCheaperScore,
    alternativePrice: formatPrice(bestCandidate.priceValue),
    alternativePriceValue: bestCandidate.priceValue,
    alternativeScore: bestCandidate.similarCheaperScore,
    alternativeShipping: bestCandidate.similarCheaperShipping,
    affiliateUrl: linkRecord.valid ? linkRecord.affiliateUrl : cleanText(bestCandidate.affiliateUrl),
    sourceTelegramText: originalDataSources.sourceTelegramText,
    sourceGroup: originalDataSources.sourceGroup,
    originalTitle: originalProfile.title,
    originalPrice: formatPrice(originalPrice),
    originalPriceValue: originalPrice,
    originalUrl: originalDataSources.originalUrl,
    originalAsin: originalDataSources.originalAsin,
    originalSourceGroup: resolveOptimizedOriginalSourceGroup({ source, structuredMessage, generatorInput }),
    amazonApiTitle: cleanText(bestCandidate.title),
    amazonAsin: cleanText(bestCandidate.asin).toUpperCase(),
    amazonMerchantName: bestCandidate.similarCheaperMerchantName || '',
    amazonMerchantId: cleanText(bestCandidate.merchantId || bestCandidate.sellerId || ''),
    amazonIsAmazonFulfilled: bestCandidate.similarCheaperIsAmazonFulfilled === true,
    amazonIsPrimeEligible: bestCandidate.similarCheaperIsPrimeEligible === true,
    amazonSellerClass: bestCandidate.similarCheaperSellerClass,
    amazonSellerSource: bestCandidate.similarCheaperSellerSource || '',
    candidateCount: candidates.length,
    bestScore: bestObservedScore,
    differenceAmount: formatPrice(differenceAmount),
    differencePercent,
    originalProductRoleInfo: bestCandidate.originalProductRoleInfo || originalProfile.productRoleInfo || null,
    optimizedProductRoleInfo: bestCandidate.optimizedProductRoleInfo || null,
    productRoleComparison: bestCandidate.productRoleComparison || null,
    productRoleComparable: bestCandidate.productRoleComparable === true,
    originalProductRole: bestCandidate.originalProductRole || '',
    optimizedProductRole: bestCandidate.optimizedProductRole || '',
    originalQuantityInfo: bestCandidate.originalQuantityInfo || originalProfile.quantityInfo || null,
    optimizedQuantityInfo: bestCandidate.optimizedQuantityInfo || null,
    quantityComparison: bestCandidate.quantityComparison || null,
    quantityComparable: bestCandidate.quantityComparable === true,
    originalQuantity: bestCandidate.originalQuantity || '',
    optimizedQuantity: bestCandidate.optimizedQuantity || '',
    originalUnitPrice: bestCandidate.originalUnitPrice || '',
    optimizedUnitPrice: bestCandidate.optimizedUnitPrice || '',
    priceValid: true,
    rawPriceObject: resolveSimilarCandidateRawPriceObject(bestCandidate),
    extractedPrice: bestCandidate.priceValue,
    softOptimizedDeal: bestCandidate.softOptimizedDeal === true,
    strictScoreThreshold,
    minimumPostScoreThreshold,
    similarSearchQuery: bestCandidate.similarSearchQuery || '',
    similarSearchQueryType: bestCandidate.similarSearchQueryType || '',
    fbmExcluded,
    productIntelligence: productIntelligenceProfile,
    baselineMaster: alternativeBaselineResult?.master || originalBaselineResult?.master || null,
    baselineResult: alternativeBaselineResult || null,
    priceErrorProtected: alternativeBaselineResult?.priceErrorProtected === true,
    ignoredAsPriceError: alternativeBaselineResult?.priceErrorProtected === true,
    query: primaryQuery,
    shortQuery: similarProductTestMode ? shortQuery : '',
    candidate: bestCandidate
  };

  const cheaperVariantPayload = await findCheapestAllowedVariation({
    finalCandidate: bestCandidate,
    originalProfile,
    currentResult: result,
    testMode: similarProductTestMode,
    sessionName,
    sourceId: source?.id ?? null,
    messageId: structuredMessage?.messageId || ''
  });
  if (cheaperVariantPayload) {
    result = applyVariantToSimilarResult(result, cheaperVariantPayload);
  }

  if (result.softOptimizedDeal === true) {
    console.info('[SIMILAR_PRODUCT_SOFT_OPTIMIZED_FOUND]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      originalAsin: originalProfile.asin,
      similarCheaperAsin: result.similarCheaperAsin,
      similarCheaperPrice: result.similarCheaperPrice,
      similarityScore: result.similarityScore,
      threshold: minimumPostScoreThreshold,
      strictThreshold: strictScoreThreshold,
      query: result.similarSearchQuery || result.query
    });
  }

  console.info('[SIMILAR_PRODUCT_CHEAPER_FOUND]', {
    sessionName,
    sourceId: source?.id ?? null,
    messageId: structuredMessage?.messageId || '',
    originalAsin: originalProfile.asin,
    similarCheaperAsin: result.similarCheaperAsin,
    similarCheaperPrice: result.similarCheaperPrice,
    similarCheaperScore: result.similarCheaperScore,
    shipping: result.similarCheaperShipping,
    fbmExcluded
  });
  console.info('[SIMILAR_CHEAPER_FOUND]', {
    sessionName,
    sourceId: source?.id ?? null,
    messageId: structuredMessage?.messageId || '',
    originalAsin: originalProfile.asin,
    similarCheaperAsin: result.similarCheaperAsin,
    similarCheaperPrice: result.similarCheaperPrice,
    similarCheaperScore: result.similarCheaperScore,
    candidateCount: candidates.length,
    bestScore: bestObservedScore
  });
  console.info('[OPTIMIZED_DEAL_FOUND]', {
    sessionName,
    sourceId: source?.id ?? null,
    messageId: structuredMessage?.messageId || '',
    originalAsin: originalProfile.asin,
    similarCheaperAsin: result.similarCheaperAsin,
    similarCheaperPrice: result.similarCheaperPrice,
    similarityScore: result.similarCheaperScore,
    priceErrorProtected: result.priceErrorProtected
  });
  console.info('[OPTIMIZED_DEAL_ORIGINAL_CONTEXT_ATTACHED]', {
    originalTitle: result.originalTitle,
    originalPrice: result.originalPrice,
    originalSourceGroup: result.originalSourceGroup
  });
  console.info('[OPTIMIZED_DEAL_AFFILIATE_LINK_USED]', {
    optimizedAsin: result.optimizedAsin,
    optimizedAffiliateUrl: result.optimizedAffiliateUrl,
    ownAffiliateLink: Boolean(result.optimizedAffiliateUrl)
  });
  console.info('[OPTIMIZED_DEAL_IMAGE_SELECTED]', {
    optimizedAsin: result.optimizedAsin,
    optimizedImageUrl: result.optimizedImageUrl || '',
    imageSource: result.optimizedImageUrl ? 'amazon_product_data' : 'missing'
  });
  console.info('[OPTIMIZED_DEAL_OUTPUT_BUILT]', {
    originalTitle: result.originalTitle,
    originalPrice: result.originalPrice,
    originalSourceGroup: result.originalSourceGroup,
    optimizedTitle: result.optimizedTitle,
    optimizedPrice: result.optimizedPrice,
    optimizedAsin: result.optimizedAsin,
    optimizedAffiliateUrl: result.optimizedAffiliateUrl,
    optimizedImageUrl: result.optimizedImageUrl,
    optimizedSellerClass: result.optimizedSellerClass,
    optimizedSellerSource: result.optimizedSellerSource,
    optimizedMerchantName: result.optimizedMerchantName,
    optimizedIsAmazonFulfilled: result.optimizedIsAmazonFulfilled,
    optimizedIsPrimeEligible: result.optimizedIsPrimeEligible,
    optimizedRawSellerKeysFound: result.optimizedRawSellerKeysFound,
    similarityScore: result.similarityScore,
    differenceAmount: result.differenceAmount,
    differencePercent: result.differencePercent,
    originalProductRole: result.originalProductRole,
    optimizedProductRole: result.optimizedProductRole,
    productRoleComparable: result.productRoleComparable === true,
    originalQuantity: result.originalQuantity,
    optimizedQuantity: result.optimizedQuantity,
    originalUnitPrice: result.originalUnitPrice,
    optimizedUnitPrice: result.optimizedUnitPrice,
    quantityComparable: result.quantityComparable === true,
    similarCheaperReason: result.similarCheaperReason
  });

  return result;
}

function shortenSimilarText(value = '', maxLength = 120) {
  const text = cleanText(value).replace(/\s+/g, ' ');
  if (!text || text.length <= maxLength) {
    return text;
  }

  return `${text.slice(0, maxLength - 3).trim()}...`;
}

function resolveOptimizedCouponContext({ generatorInput = {}, similarCheck = {}, structuredMessage = {} } = {}) {
  const couponCode =
    cleanText(similarCheck.couponCode) ||
    cleanText(generatorInput.couponCode) ||
    extractTelegramCouponCode(structuredMessage?.text || '');
  const couponValue = cleanText(similarCheck.couponValue || generatorInput.couponValue);
  const subscribeDiscount = cleanText(similarCheck.subscribeDiscount || generatorInput.subscribeDiscount);
  const couponDetected = similarCheck.couponDetected === true || generatorInput.couponDetected === true || Boolean(couponValue);
  const subscribeDetected =
    similarCheck.subscribeDetected === true || generatorInput.subscribeDetected === true || Boolean(subscribeDiscount);
  const infoParts = [];

  if (couponDetected) {
    infoParts.push(couponValue ? `Amazon Coupon: ${couponValue}` : 'Amazon Coupon erkannt');
  }
  if (subscribeDetected) {
    infoParts.push(subscribeDiscount ? `Spar-Abo: ${subscribeDiscount}` : 'Spar-Abo erkannt');
  }
  if (couponCode) {
    infoParts.push(`Code: ${couponCode}`);
  }

  return {
    couponCode,
    couponInfo: infoParts.join(' | ') || 'Kein Coupon erkannt',
    couponDetected,
    subscribeDetected,
    couponValue,
    subscribeDiscount
  };
}

function resolveOptimizedVariantInfo(similarCheck = {}) {
  if (similarCheck.variantSelected === true) {
    return shortenSimilarText(similarCheck.variantLabel || 'Guenstigste Variante gewaehlt', 90);
  }

  return 'Keine guenstigere erlaubte Variante gewaehlt';
}

function buildOptimizedSimilarDealPost(similarCheck = {}) {
  const optimizedTitleRaw = cleanText(similarCheck.optimizedTitle || similarCheck.similarCheaperTitle) || 'Optimierter Amazon Deal';
  const optimizedTitleShort = escapeTelegramHtml(shortenSimilarText(optimizedTitleRaw, 90));
  const originalTitle = escapeTelegramHtml(shortenSimilarText(similarCheck.originalTitle, 90) || 'Urspruenglicher Deal');
  const sourceGroup = escapeTelegramHtml(cleanText(similarCheck.originalSourceGroup) || 'Unbekannt');
  const optimizedPrice = escapeTelegramHtml(formatPrice(similarCheck.optimizedPriceValue || similarCheck.similarCheaperPriceValue || similarCheck.optimizedPrice || similarCheck.similarCheaperPrice) || similarCheck.optimizedPrice || similarCheck.similarCheaperPrice || 'n/a');
  const optimizedSellerClass = escapeTelegramHtml(similarCheck.optimizedSellerClass || similarCheck.similarCheaperSellerClass || 'Amazon/FBA');
  const similarityScore = escapeTelegramHtml(String(similarCheck.similarityScore ?? similarCheck.similarCheaperScore ?? 'n/a'));
  const reason = escapeTelegramHtml(shortenSimilarText(similarCheck.similarCheaperReason || 'Aehnliches Produkt guenstiger gefunden.', 120));
  const affiliateUrl = cleanText(similarCheck.optimizedAffiliateUrl || similarCheck.affiliateUrl);
  const couponCode = cleanText(similarCheck.couponCode);
  const couponInfo = escapeTelegramHtml(cleanText(similarCheck.couponInfo) || 'Kein Coupon erkannt');
  const variantInfo = escapeTelegramHtml(resolveOptimizedVariantInfo(similarCheck));
  const originalQuantity = cleanText(similarCheck.originalQuantity);
  const optimizedQuantity = cleanText(similarCheck.optimizedQuantity);
  const originalUnitPrice = cleanText(similarCheck.originalUnitPrice);
  const optimizedUnitPrice = cleanText(similarCheck.optimizedUnitPrice);
  const quantityLine =
    originalQuantity || optimizedQuantity
      ? `\u{1F4E6} Menge: ${escapeTelegramHtml(originalQuantity || 'n/a')} \u2192 ${escapeTelegramHtml(optimizedQuantity || 'n/a')}`
      : '';
  const unitPriceLine =
    originalUnitPrice || optimizedUnitPrice
      ? `\u{1F4B6} Grundpreis: ${escapeTelegramHtml(originalUnitPrice || 'n/a')} \u2192 ${escapeTelegramHtml(optimizedUnitPrice || 'n/a')}`
      : '';
  const generatorPost = generatePostText({
    productTitle: optimizedTitleRaw,
    neuerPreis: similarCheck.optimizedPriceValue || similarCheck.similarCheaperPriceValue || similarCheck.optimizedPrice || similarCheck.similarCheaperPrice,
    amazonLink: affiliateUrl,
    textBaustein: '',
    extraOptions: couponCode ? [COUPON_OPTION_LABEL] : [],
    freiText: '',
    rabattgutscheinCode: couponCode
  }).telegramCaption.trim();

  return [
    generatorPost,
    '\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501',
    '',
    '\u{1F4CA} Optimiert gefunden',
    '',
    '\u{1F4E2} Ursprung:',
    sourceGroup,
    '',
    '\u{1F6D2} Original:',
    originalTitle,
    '',
    `\u{1F4B6} Originalpreis: ${escapeTelegramHtml(similarCheck.originalPrice || 'n/a')}`,
    '',
    '\u2705 Besserer Fund:',
    optimizedTitleShort,
    '',
    `\u{1F525} Neuer Preis: ${optimizedPrice}`,
    '',
    `\u{1F4B0} Ersparnis: ${escapeTelegramHtml(similarCheck.differenceAmount || 'n/a')} (${escapeTelegramHtml(String(similarCheck.differencePercent ?? 'n/a'))}%)`,
    '',
    '\u{1F39F} Coupon:',
    couponInfo,
    '',
    '\u{1F3A8} Beste Variante:',
    variantInfo,
    '',
    ...(similarCheck.variantSelected === true
      ? [
          `\u{1F4B6} Vorheriger Fund: ${escapeTelegramHtml(similarCheck.previousCandidatePrice || 'n/a')}`,
          `\u{1F525} Beste Variante: ${escapeTelegramHtml(similarCheck.variantPrice || optimizedPrice)}`,
          `\u{1F4B0} Extra gespart: ${escapeTelegramHtml(similarCheck.variantDifferenceAmount || 'n/a')} (${escapeTelegramHtml(String(similarCheck.variantDifferencePercent ?? 'n/a'))}%)`,
          ''
        ]
      : []),
    ...(quantityLine ? [quantityLine, ''] : []),
    ...(unitPriceLine ? [unitPriceLine, ''] : []),
    `\u{1F4E6} Versand/Seller: ${optimizedSellerClass}`,
    '',
    `\u{1F50E} Aehnlichkeit: ${similarityScore}/100`,
    '',
    '\u{1F6E1}\uFE0F Schutz:',
    'Nur Amazon/FBA akzeptiert.',
    'FBM wurde ausgeschlossen.',
    '',
    '\u{1F4CC} Warum besser:',
    reason
  ].join('\n');
}

async function publishSimilarProductOptimizedChannel({
  sessionName = '',
  source = {},
  structuredMessage = {},
  generatorInput = {},
  similarCheck = null
} = {}) {
  const disabledReason = getOptimizedDealsDisabledReason();
  if (disabledReason) {
    logOptimizedDealsDisabledSendBlocked({
      context: 'similar_product_optimized_publish',
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      asin: generatorInput?.asin || similarCheck?.similarCheaperAsin || '',
      reason: disabledReason
    });
    return { sent: false, reason: disabledReason };
  }

  if (!similarCheck?.similarCheaperFound) {
    console.info('[SIMILAR_PRODUCT_OPTIMIZED_CHANNEL_SKIPPED]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      asin: generatorInput?.asin || '',
      reason: similarCheck?.reason || 'no_cheaper_candidate'
    });
    optimizedChannelCache.lastSkipReason = similarCheck?.reason || 'no_cheaper_candidate';
    return { sent: false, reason: optimizedChannelCache.lastSkipReason };
  }

  const optimizedSellerClass = normalizeSimilarSellerClass(
    similarCheck.optimizedSellerClass || similarCheck.similarCheaperSellerClass || similarCheck.similarCheaperShipping
  );
  const optimizedTestMode = isSimilarProductTestModeActive();
  const optimizedSellerAllowed =
    optimizedSellerClass === 'AMAZON_DIRECT' ||
    optimizedSellerClass === 'FBA' ||
    (optimizedTestMode && optimizedSellerClass === 'FBA_OR_AMAZON_UNKNOWN');

  if (
    cleanText(similarCheck.apiStatus).toUpperCase() === 'THROTTLED' ||
    cleanText(similarCheck.reason).toUpperCase() === 'API_THROTTLED'
  ) {
    console.info('[SIMILAR_PRODUCT_OPTIMIZED_CHANNEL_SKIPPED]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      asin: similarCheck.similarCheaperAsin || '',
      reason: 'API_THROTTLED'
    });
    optimizedChannelCache.lastSkipReason = 'API_THROTTLED';
    return { sent: false, reason: 'API_THROTTLED' };
  }

  if (!optimizedSellerAllowed) {
    const skipReason = optimizedSellerClass.includes('FBM')
      ? 'candidate_is_fbm'
      : optimizedSellerClass === 'UNKNOWN'
        ? 'SELLER_UNKNOWN_NOT_ALLOWED'
        : 'SELLER_NOT_ALLOWED';
    console.info('[SIMILAR_PRODUCT_OPTIMIZED_CHANNEL_SKIPPED]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      asin: similarCheck.similarCheaperAsin || '',
      reason: skipReason,
      sellerClass: optimizedSellerClass,
      shipping: similarCheck.similarCheaperShipping || 'UNKNOWN'
    });
    optimizedChannelCache.lastSkipReason = skipReason;
    return { sent: false, reason: skipReason };
  }

  if (similarCheck.productRoleComparable === false || similarCheck.productRoleComparison?.allowed === false) {
    console.info('[SIMILAR_PRODUCT_OPTIMIZED_CHANNEL_SKIPPED]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      asin: similarCheck.similarCheaperAsin || '',
      reason: 'PRODUCT_ROLE_MISMATCH',
      originalRole: similarCheck.originalProductRole || similarCheck.productRoleComparison?.originalRoleLabel || '',
      optimizedRole: similarCheck.optimizedProductRole || similarCheck.productRoleComparison?.candidateRoleLabel || '',
      detail: similarCheck.productRoleComparison?.reason || 'Produktrolle nicht vergleichbar.'
    });
    optimizedChannelCache.lastSkipReason = 'PRODUCT_ROLE_MISMATCH';
    return { sent: false, reason: 'PRODUCT_ROLE_MISMATCH' };
  }

  if (similarCheck.quantityComparable === false || similarCheck.quantityComparison?.allowed === false) {
    console.info('[SIMILAR_PRODUCT_OPTIMIZED_CHANNEL_SKIPPED]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      asin: similarCheck.similarCheaperAsin || '',
      reason: 'PACK_SIZE_MISMATCH',
      originalQuantity: similarCheck.originalQuantity || similarCheck.quantityComparison?.originalQuantityLabel || '',
      optimizedQuantity: similarCheck.optimizedQuantity || similarCheck.quantityComparison?.candidateQuantityLabel || '',
      originalUnitPrice: similarCheck.originalUnitPrice || similarCheck.quantityComparison?.originalUnitPrice || '',
      optimizedUnitPrice: similarCheck.optimizedUnitPrice || similarCheck.quantityComparison?.candidateUnitPrice || ''
    });
    optimizedChannelCache.lastSkipReason = 'PACK_SIZE_MISMATCH';
    return { sent: false, reason: 'PACK_SIZE_MISMATCH' };
  }

  const publishOptimizedPrice = resolveFirstSimilarPositivePrice(
    similarCheck.optimizedPriceValue,
    similarCheck.similarCheaperPriceValue,
    similarCheck.alternativePriceValue,
    similarCheck.candidate?.priceValue,
    similarCheck.optimizedPrice,
    similarCheck.similarCheaperPrice
  );
  const publishOriginalPrice = resolveFirstSimilarPositivePrice(similarCheck.originalPriceValue, similarCheck.originalPrice);

  if (publishOptimizedPrice === null || publishOriginalPrice === null) {
    console.info('[SIMILAR_PRODUCT_OPTIMIZED_CHANNEL_SKIPPED]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      asin: similarCheck.similarCheaperAsin || '',
      reason: 'PRICE_MISSING',
      rawPriceObject: similarCheck.rawPriceObject || resolveSimilarCandidateRawPriceObject(similarCheck.candidate || {}),
      extractedPrice: publishOptimizedPrice,
      originalPrice: publishOriginalPrice,
      priceValid: false
    });
    optimizedChannelCache.lastSkipReason = 'PRICE_MISSING';
    return { sent: false, reason: 'PRICE_MISSING' };
  }

  if (publishOptimizedPrice >= publishOriginalPrice) {
    console.info('[SIMILAR_PRODUCT_OPTIMIZED_CHANNEL_SKIPPED]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      asin: similarCheck.similarCheaperAsin || '',
      reason: 'PRICE_NOT_CHEAPER',
      extractedPrice: publishOptimizedPrice,
      originalPrice: publishOriginalPrice,
      priceValid: true
    });
    optimizedChannelCache.lastSkipReason = 'PRICE_NOT_CHEAPER';
    return { sent: false, reason: 'PRICE_NOT_CHEAPER' };
  }

  const publishDifferenceAmount = Math.max(0, publishOriginalPrice - publishOptimizedPrice);
  const publishDifferencePercent = publishOriginalPrice > 0 ? Math.round((publishDifferenceAmount / publishOriginalPrice) * 1000) / 10 : 0;
  const publishSimilarityScore = Number(similarCheck.similarityScore ?? similarCheck.similarCheaperScore ?? 0);

  if (!Number.isFinite(publishSimilarityScore) || publishSimilarityScore < SIMILAR_PRODUCT_MIN_SCORE) {
    console.info('[SIMILAR_PRODUCT_OPTIMIZED_CHANNEL_SKIPPED]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      asin: similarCheck.similarCheaperAsin || '',
      reason: 'SIMILARITY_TOO_LOW',
      similarityScore: Number.isFinite(publishSimilarityScore) ? publishSimilarityScore : null,
      threshold: SIMILAR_PRODUCT_MIN_SCORE
    });
    optimizedChannelCache.lastSkipReason = 'SIMILARITY_TOO_LOW';
    return { sent: false, reason: 'SIMILARITY_TOO_LOW' };
  }

  if (publishDifferencePercent < 3) {
    console.info('[SIMILAR_PRODUCT_OPTIMIZED_CHANNEL_SKIPPED]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      asin: similarCheck.similarCheaperAsin || '',
      reason: 'SAVING_TOO_LOW',
      differencePercent: publishDifferencePercent,
      minimumDifferencePercent: 3
    });
    optimizedChannelCache.lastSkipReason = 'SAVING_TOO_LOW';
    return { sent: false, reason: 'SAVING_TOO_LOW' };
  }

  similarCheck.optimizedPriceValue = publishOptimizedPrice;
  similarCheck.similarCheaperPriceValue = publishOptimizedPrice;
  similarCheck.originalPriceValue = publishOriginalPrice;
  similarCheck.optimizedPrice = formatPrice(publishOptimizedPrice);
  similarCheck.similarCheaperPrice = formatPrice(publishOptimizedPrice);
  similarCheck.originalPrice = formatPrice(publishOriginalPrice);
  similarCheck.differenceAmount = formatPrice(publishDifferenceAmount);
  similarCheck.differencePercent = publishDifferencePercent;
  similarCheck.priceValid = true;
  const optimizedCouponContext = resolveOptimizedCouponContext({
    generatorInput,
    similarCheck,
    structuredMessage
  });
  similarCheck.couponCode = optimizedCouponContext.couponCode;
  similarCheck.couponInfo = optimizedCouponContext.couponInfo;
  similarCheck.couponDetected = optimizedCouponContext.couponDetected;
  similarCheck.couponValue = optimizedCouponContext.couponValue;
  similarCheck.subscribeDetected = optimizedCouponContext.subscribeDetected;
  similarCheck.subscribeDiscount = optimizedCouponContext.subscribeDiscount;

  if (optimizedCouponContext.couponDetected === true || optimizedCouponContext.subscribeDetected === true) {
    console.info('[COUPON_APPLIED]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      asin: similarCheck.similarCheaperAsin || '',
      couponInfo: optimizedCouponContext.couponInfo,
      codeDetected: Boolean(optimizedCouponContext.couponCode)
    });
  }
  if (similarCheck.variantSelected === true) {
    console.info('[VARIANT_SELECTED]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      asin: similarCheck.similarCheaperAsin || '',
      label: similarCheck.variantLabel || '',
      previousCandidatePrice: similarCheck.previousCandidatePrice || '',
      variantPrice: similarCheck.variantPrice || ''
    });
  }

  if (!cleanText(similarCheck.affiliateUrl)) {
    console.info('[SIMILAR_PRODUCT_OPTIMIZED_CHANNEL_SKIPPED]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      asin: similarCheck.similarCheaperAsin || '',
      reason: 'affiliate_link_missing'
    });
    optimizedChannelCache.lastSkipReason = 'affiliate_link_missing';
    return { sent: false, reason: 'affiliate_link_missing' };
  }

  if (!OPTIMIZED_CHANNEL_ENABLED) {
    console.info('[SIMILAR_PRODUCT_OPTIMIZED_CHANNEL_SKIPPED]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      asin: similarCheck.similarCheaperAsin || '',
      reason: 'optimized_channel_disabled'
    });
    optimizedChannelCache.lastSkipReason = 'optimized_channel_disabled';
    return { sent: false, reason: 'optimized_channel_disabled' };
  }

  const chatId = await resolveOptimizedDealsChannelId();
  if (!chatId) {
    console.info('[SIMILAR_PRODUCT_OPTIMIZED_CHANNEL_SKIPPED]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      asin: similarCheck.similarCheaperAsin || '',
      reason: 'optimized_channel_missing_target'
    });
    optimizedChannelCache.lastSkipReason = 'optimized_channel_missing_target';
    return { sent: false, reason: 'optimized_channel_missing_target' };
  }

  console.info('[SIMILAR_PRODUCT_OPTIMIZED_CHANNEL_SEND]', {
    sessionName,
    sourceId: source?.id ?? null,
    messageId: structuredMessage?.messageId || '',
    originalAsin: generatorInput?.asin || '',
    similarCheaperAsin: similarCheck.similarCheaperAsin,
    chatId,
    score: similarCheck.similarCheaperScore
  });

  const optimizedPostText = buildOptimizedSimilarDealPost(similarCheck);
  const finalSendDisabledReason = getOptimizedDealsDisabledReason();
  if (finalSendDisabledReason) {
    logOptimizedDealsDisabledSendBlocked({
      context: 'similar_product_optimized_final_send',
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      asin: similarCheck.similarCheaperAsin || '',
      reason: finalSendDisabledReason
    });
    return { sent: false, reason: finalSendDisabledReason };
  }

  const result = await sendTelegramPost({
    text: optimizedPostText,
    chatId,
    imageUrl: cleanText(similarCheck.optimizedImageUrl),
    disableWebPagePreview: false,
    titlePreview: similarCheck.similarCheaperTitle || 'Optimierter Deal',
    hasAffiliateLink: true,
    postContext: 'similar_product_optimized'
  });
  let couponCodeResult = null;
  if (cleanText(similarCheck.couponCode)) {
    try {
      couponCodeResult = await sendTelegramPost({
        text: ['\u{1F4CB} CODE:', cleanText(similarCheck.couponCode)].join('\n'),
        chatId,
        disableWebPagePreview: true,
        titlePreview: 'Optimierter Deal Code',
        hasAffiliateLink: false,
        postContext: 'similar_product_optimized_code'
      });
    } catch (couponCodeError) {
      console.warn('[OPTIMIZED_DEAL_CODE_SEND_FAILED]', {
        sessionName,
        sourceId: source?.id ?? null,
        messageId: structuredMessage?.messageId || '',
        asin: similarCheck.similarCheaperAsin || '',
        reason: couponCodeError instanceof Error ? couponCodeError.message : 'Code-Nachricht konnte nicht gesendet werden.'
      });
    }
  }

  console.info('[SIMILAR_PRODUCT_OPTIMIZED_CHANNEL_SENT]', {
    sessionName,
    sourceId: source?.id ?? null,
    messageId: structuredMessage?.messageId || '',
    originalAsin: generatorInput?.asin || '',
    similarCheaperAsin: similarCheck.similarCheaperAsin,
    chatId,
    telegramMessageId: result?.messageId || null,
    couponCodeMessageId: couponCodeResult?.messageId || null
  });
  optimizedChannelCache.lastSkipReason = '';
  optimizedChannelCache.lastOptimizedDeal = {
    originalTitle: similarCheck.originalTitle || '',
    originalPrice: similarCheck.originalPrice || '',
    originalSourceGroup: similarCheck.originalSourceGroup || '',
    optimizedTitle: similarCheck.optimizedTitle || similarCheck.similarCheaperTitle || '',
    optimizedPrice: similarCheck.optimizedPrice || similarCheck.similarCheaperPrice || '',
    optimizedAsin: similarCheck.optimizedAsin || similarCheck.similarCheaperAsin || '',
    optimizedAffiliateUrl: similarCheck.optimizedAffiliateUrl || similarCheck.affiliateUrl || '',
    optimizedImageUrl: similarCheck.optimizedImageUrl || '',
    optimizedSellerClass: similarCheck.optimizedSellerClass || similarCheck.similarCheaperSellerClass || '',
    optimizedSellerSource: similarCheck.optimizedSellerSource || similarCheck.similarCheaperSellerSource || '',
    optimizedMerchantName: similarCheck.optimizedMerchantName || similarCheck.similarCheaperMerchantName || '',
    optimizedIsAmazonFulfilled: similarCheck.optimizedIsAmazonFulfilled ?? similarCheck.similarCheaperIsAmazonFulfilled ?? null,
    optimizedIsPrimeEligible: similarCheck.optimizedIsPrimeEligible ?? similarCheck.similarCheaperIsPrimeEligible ?? null,
    optimizedRawSellerKeysFound: similarCheck.optimizedRawSellerKeysFound || similarCheck.similarCheaperRawSellerKeysFound || [],
    similarityScore: similarCheck.similarityScore ?? similarCheck.similarCheaperScore ?? null,
    differenceAmount: similarCheck.differenceAmount || '',
    differencePercent: similarCheck.differencePercent ?? null,
    couponInfo: similarCheck.couponInfo || '',
    couponCode: similarCheck.couponCode || '',
    variantSelected: similarCheck.variantSelected === true,
    variantLabel: similarCheck.variantLabel || '',
    similarCheaperReason: similarCheck.similarCheaperReason || '',
    sentAt: nowIso(),
    messageId: result?.messageId || null,
    couponCodeMessageId: couponCodeResult?.messageId || null
  };
  optimizedChannelCache.lastOriginalSourceGroup = similarCheck.originalSourceGroup || '';
  optimizedChannelCache.lastComparisonPrice = similarCheck.optimizedPrice || similarCheck.similarCheaperPrice || '';
  console.info('[OPTIMIZED_DEAL_SENT]', {
    sessionName,
    sourceId: source?.id ?? null,
    messageId: structuredMessage?.messageId || '',
    originalAsin: generatorInput?.asin || '',
    similarCheaperAsin: similarCheck.similarCheaperAsin,
    chatId,
    telegramMessageId: result?.messageId || null,
    couponCodeMessageId: couponCodeResult?.messageId || null
  });
  console.info('[OPTIMIZED_DEAL_POSTED]', {
    sessionName,
    sourceId: source?.id ?? null,
    messageId: structuredMessage?.messageId || '',
    originalAsin: generatorInput?.asin || '',
    optimizedAsin: similarCheck.optimizedAsin || similarCheck.similarCheaperAsin || '',
    chatId,
    telegramMessageId: result?.messageId || null,
    couponCodeMessageId: couponCodeResult?.messageId || null,
    couponInfo: similarCheck.couponInfo || '',
    variantInfo: resolveOptimizedVariantInfo(similarCheck)
  });
  console.info('[OPTIMIZED_DEAL_SENT_WITH_COMPARISON]', {
    sessionName,
    sourceId: source?.id ?? null,
    messageId: structuredMessage?.messageId || '',
    originalTitle: similarCheck.originalTitle || '',
    originalPrice: similarCheck.originalPrice || '',
    originalSourceGroup: similarCheck.originalSourceGroup || '',
    optimizedTitle: similarCheck.optimizedTitle || similarCheck.similarCheaperTitle || '',
    optimizedPrice: similarCheck.optimizedPrice || similarCheck.similarCheaperPrice || '',
    optimizedAsin: similarCheck.optimizedAsin || similarCheck.similarCheaperAsin || '',
    optimizedAffiliateUrl: similarCheck.optimizedAffiliateUrl || similarCheck.affiliateUrl || '',
    optimizedImageUrl: similarCheck.optimizedImageUrl || '',
    optimizedSellerClass: similarCheck.optimizedSellerClass || similarCheck.similarCheaperSellerClass || '',
    optimizedSellerSource: similarCheck.optimizedSellerSource || similarCheck.similarCheaperSellerSource || '',
    optimizedMerchantName: similarCheck.optimizedMerchantName || similarCheck.similarCheaperMerchantName || '',
    optimizedIsAmazonFulfilled: similarCheck.optimizedIsAmazonFulfilled ?? similarCheck.similarCheaperIsAmazonFulfilled ?? null,
    optimizedIsPrimeEligible: similarCheck.optimizedIsPrimeEligible ?? similarCheck.similarCheaperIsPrimeEligible ?? null,
    optimizedRawSellerKeysFound: similarCheck.optimizedRawSellerKeysFound || similarCheck.similarCheaperRawSellerKeysFound || [],
    similarityScore: similarCheck.similarityScore ?? similarCheck.similarCheaperScore ?? null,
    differenceAmount: similarCheck.differenceAmount || '',
    differencePercent: similarCheck.differencePercent ?? null,
    couponInfo: similarCheck.couponInfo || '',
    couponCode: similarCheck.couponCode || '',
    variantSelected: similarCheck.variantSelected === true,
    variantLabel: similarCheck.variantLabel || '',
    similarCheaperReason: similarCheck.similarCheaperReason || '',
    chatId,
    telegramMessageId: result?.messageId || null
  });

  return { sent: true, messageId: result?.messageId || null };
}

function classifyRelaxedAmazonMatchScore(matchScore = 0, sourceFacts = {}, candidate = {}) {
  const numericScore = Number(matchScore);
  const safeScore = Number.isFinite(numericScore) ? numericScore : 0;
  const reviewReasons = [];

  if (sourceFacts.matchBasis === 'title_only') {
    reviewReasons.push('Nur Titel vorhanden.');
  }
  if (candidate.priceSource !== 'paapi') {
    reviewReasons.push('Preis nicht direkt ueber PAAPI verifiziert.');
  }
  if (!cleanText(candidate.imageUrl) && cleanText(sourceFacts.imageUrl)) {
    reviewReasons.push('Bild nur in der Quelle vorhanden.');
  }
  if (candidate.sourceOnlyPriceUsed === true) {
    reviewReasons.push('Preis stammt nur aus dem Quelltext.');
  }

  if (safeScore >= 90 && !reviewReasons.length) {
    return {
      tier: 'auto_post',
      decision: 'APPROVE',
      matched: true,
      reason: 'Starker Amazon-Match erkannt.',
      reviewReasons
    };
  }

  if (safeScore >= 70) {
    return {
      tier: 'review',
      decision: 'REVIEW',
      matched: true,
      reason: reviewReasons[0] || 'Match ist nur fuer Review freigegeben.',
      reviewReasons
    };
  }

  return {
    tier: 'debug',
    decision: 'REVIEW',
    matched: false,
    reason: 'Match-Score unter 70.',
    reviewReasons
  };
}

function buildMatchedAmazonRecoveryDeal({ sourceFacts = {}, candidate = {}, linkRecord = {} } = {}) {
  const normalizedAsin = cleanText(linkRecord?.asin || candidate?.asin).toUpperCase();
  const resolvedUrl = cleanText(linkRecord?.normalizedUrl || candidate?.normalizedUrl || (normalizedAsin ? `https://www.amazon.de/dp/${normalizedAsin}` : ''));
  const candidatePriceDisplay = normalizeReaderPriceCandidate(candidate?.priceDisplay || '');
  const sourceFallbackPrice =
    !candidatePriceDisplay && sourceFacts.priceValue !== null ? normalizeReaderPriceCandidate(formatPrice(sourceFacts.priceValue)) : '';
  const resolvedPrice = candidatePriceDisplay || sourceFallbackPrice;
  const dataSource =
    cleanText(candidate?.priceSource).toLowerCase() === 'paapi' || cleanText(candidate?.titleSource).toLowerCase() === 'paapi'
      ? 'paapi'
      : 'amazon_match';
  const selectedSource = cleanText(candidate?.imageSource).toLowerCase() === 'paapi' ? 'paapi' : 'amazon_match';

  return {
    success: true,
    asin: normalizedAsin,
    title: sanitizeReaderPostTitle(candidate?.title),
    productTitle: sanitizeReaderPostTitle(candidate?.title),
    titleDataSource: dataSource === 'paapi' ? 'paapi' : 'amazon',
    imageUrl: cleanText(candidate?.imageUrl),
    imageDataSource: selectedSource === 'paapi' ? 'paapi' : 'amazon',
    price: resolvedPrice,
    basePrice: resolvedPrice,
    paapiPrice: cleanText(candidate?.priceSource).toLowerCase() === 'paapi' ? candidatePriceDisplay : '',
    amazonPrice: cleanText(candidate?.priceSource).toLowerCase() === 'paapi' ? candidatePriceDisplay : '',
    paapiCurrentPrice: cleanText(candidate?.priceSource).toLowerCase() === 'paapi' ? candidatePriceDisplay : '',
    finalPrice: '',
    finalPriceCalculated: false,
    finalUrl: resolvedUrl,
    resolvedUrl,
    normalizedUrl: resolvedUrl,
    originalUrl: resolvedUrl,
    previewImage: '',
    ogImage: '',
    bulletPoints: [],
    sellerType: 'UNKNOWN',
    sellerClass: 'UNKNOWN',
    soldByAmazon: null,
    shippedByAmazon: null,
    sellerDetails: {
      detectionSource: 'amazon-match',
      detectionSources: ['amazon-match'],
      merchantText: '',
      matchedPatterns: [],
      dealType: 'AMAZON',
      isAmazonDeal: true
    },
    dataSource,
    imageDebug: {
      paapiStatus: selectedSource === 'paapi' ? 'available' : 'match_candidate',
      selectedSource
    }
  };
}

async function searchAmazonProductBySourceData({
  sessionName = '',
  source = {},
  structuredMessage = {},
  scrapedDeal = {},
  pricing = {},
  originalLink = '',
  detectedAsin = ''
} = {}) {
  const sourceFacts = extractSourceProductFacts({
    sessionName,
    source,
    structuredMessage,
    scrapedDeal,
    pricing,
    originalLink,
    detectedAsin
  });
  const searchQuery = buildAmazonSearchQuery(sourceFacts);

  console.info('[AMAZON_MATCH_STARTED]', {
    sessionName,
    sourceId: source?.id ?? null,
    messageId: structuredMessage?.messageId || '',
    matchBasis: sourceFacts.matchBasis,
    query: searchQuery || '',
    asinCandidate: sourceFacts.asinCandidate || ''
  });

  if (!searchQuery && !cleanText(sourceFacts.asinCandidate)) {
    console.info('[PRODUCT_MATCH_REJECTED]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      matchScore: 0,
      reason: 'Zu wenig Quelldaten fuer eine Amazon-Suche.'
    });
    console.warn('[AMAZON_MATCH_FAILED]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      reason: 'Zu wenig Quelldaten fuer eine Amazon-Suche.'
    });
    return {
      attempted: true,
      matched: false,
      matchScore: 0,
      reason: 'Zu wenig Quelldaten fuer eine Amazon-Suche.',
      sourceFacts
    };
  }

  try {
    if (cleanText(sourceFacts.asinCandidate)) {
      console.info('[AMAZON_MATCH_BY_ASIN]', {
        sessionName,
        sourceId: source?.id ?? null,
        messageId: structuredMessage?.messageId || '',
        asin: sourceFacts.asinCandidate
      });
      console.info('[AMAZON_SEARCH_BY_ASIN]', {
        sessionName,
        sourceId: source?.id ?? null,
        messageId: structuredMessage?.messageId || '',
        asin: sourceFacts.asinCandidate
      });

      const paapiContext = await loadAmazonAffiliateContext({ asin: sourceFacts.asinCandidate });
      const paapiCandidate = paapiContext?.available === true ? buildAmazonMatchCandidateFromPaapi(paapiContext?.result, sourceFacts.asinCandidate) : null;
      let bestDirectCandidate = paapiCandidate;

      if (
        (!bestDirectCandidate || !cleanText(bestDirectCandidate.title)) &&
        sourceFacts.protectedDealSourceMatched !== true
      ) {
        const linkRecord = buildAmazonAffiliateLinkRecord(sourceFacts.asinCandidate, { asin: sourceFacts.asinCandidate });
        if (linkRecord.valid && cleanText(linkRecord.normalizedUrl)) {
          try {
            const directScrapedDeal = await scrapeAmazonProduct(linkRecord.normalizedUrl);
            if (directScrapedDeal && (cleanText(directScrapedDeal?.productTitle) || cleanText(directScrapedDeal?.imageUrl))) {
              bestDirectCandidate = buildAmazonMatchCandidateFromScrapedDeal(directScrapedDeal, sourceFacts.asinCandidate);
            }
          } catch (error) {
            console.warn('[AMAZON_MATCH_FAILED]', {
              sessionName,
              sourceId: source?.id ?? null,
              messageId: structuredMessage?.messageId || '',
              asin: sourceFacts.asinCandidate,
              reason: error instanceof Error ? error.message : 'Direkter Amazon-ASIN-Aufruf fehlgeschlagen.'
            });
          }
        }
      } else if ((!bestDirectCandidate || !cleanText(bestDirectCandidate.title)) && sourceFacts.protectedDealSourceMatched === true) {
        console.info('[PROTECTED_SOURCE_NO_BYPASS]', {
          sessionName,
          sourceId: source?.id ?? null,
          messageId: structuredMessage?.messageId || '',
          sourceLabel: sourceFacts.protectedDealSourceLabel || null,
          sourceHost: sourceFacts.host || null,
          reason: 'ASIN vorhanden, aber Direkt-Scrape fuer geschuetzte Quelle uebersprungen.'
        });
      }

      if (bestDirectCandidate && cleanText(bestDirectCandidate.asin)) {
        const matchScore = cleanText(bestDirectCandidate.asin).toUpperCase() === cleanText(sourceFacts.asinCandidate).toUpperCase()
          ? 100
          : computeAmazonProductMatchScore(sourceFacts, bestDirectCandidate);
        const matchDecision = classifyRelaxedAmazonMatchScore(matchScore, sourceFacts, bestDirectCandidate);
        const linkRecord = buildAmazonAffiliateLinkRecord(bestDirectCandidate.normalizedUrl || bestDirectCandidate.asin, {
          asin: bestDirectCandidate.asin
        });
        const recoveryDeal =
          bestDirectCandidate.scrapedDeal && typeof bestDirectCandidate.scrapedDeal === 'object'
            ? {
                ...bestDirectCandidate.scrapedDeal,
                finalUrl: cleanText(bestDirectCandidate.scrapedDeal?.finalUrl || linkRecord.normalizedUrl),
                resolvedUrl: cleanText(bestDirectCandidate.scrapedDeal?.resolvedUrl || linkRecord.normalizedUrl),
                normalizedUrl: cleanText(bestDirectCandidate.scrapedDeal?.normalizedUrl || linkRecord.normalizedUrl),
                asin: cleanText(bestDirectCandidate.scrapedDeal?.asin || bestDirectCandidate.asin).toUpperCase()
              }
            : buildMatchedAmazonRecoveryDeal({
                sourceFacts,
                candidate: {
                  ...bestDirectCandidate,
                  sourceOnlyPriceUsed:
                    cleanText(bestDirectCandidate.priceSource).toLowerCase() !== 'paapi' && sourceFacts.priceValue !== null && !bestDirectCandidate.priceDisplay
                },
                linkRecord
              });
        const matchReason = buildSourceMatchReasonSummary(sourceFacts, bestDirectCandidate);

        console.info('[PRODUCT_MATCH_SCORE]', {
          sessionName,
          sourceId: source?.id ?? null,
          messageId: structuredMessage?.messageId || '',
          bestAsin: bestDirectCandidate.asin || '',
          matchScore,
          query: searchQuery || ''
        });
        console.info('[AMAZON_MATCH_SELECTED]', {
          sessionName,
          sourceId: source?.id ?? null,
          messageId: structuredMessage?.messageId || '',
          asin: bestDirectCandidate.asin || '',
          candidateSource: bestDirectCandidate.candidateSource || 'unknown',
          matchScore,
          matchReason
        });

        if (matchDecision.matched === true && linkRecord.valid && cleanText(linkRecord.affiliateUrl)) {
          console.info(matchDecision.decision === 'APPROVE' ? '[PRODUCT_MATCH_APPROVED]' : '[PRODUCT_MATCH_REVIEW]', {
            sessionName,
            sourceId: source?.id ?? null,
            messageId: structuredMessage?.messageId || '',
            asin: bestDirectCandidate.asin || '',
            matchScore,
            matchReason,
            reason: matchDecision.reason
          });
          return {
            attempted: true,
            matched: true,
            matchScore,
            reason: matchDecision.reason,
            sourceFacts,
            linkRecord,
            scrapedDeal: recoveryDeal,
            matchTier: matchDecision.tier,
            decision: matchDecision.decision,
            matchReason,
            reviewReasons: matchDecision.reviewReasons || []
          };
        }
      }
    }

    let activeSourceFacts = sourceFacts;
    let searchResult = await fetchAmazonSearchCandidatesForQuery({
      sessionName,
      source,
      structuredMessage,
      sourceFacts,
      searchQuery,
      searchMode: sourceFacts.matchBasis === 'brand_model' ? 'brand_model' : 'title',
      fallbackMode: false
    });

    if (searchResult.blocked) {
      console.info('[PRODUCT_MATCH_REJECTED]', {
        sessionName,
        sourceId: source?.id ?? null,
        messageId: structuredMessage?.messageId || '',
        matchScore: 0,
        reason: searchResult.blockedReason || 'Amazon-Suche wurde durch Schutzseite blockiert.'
      });
      console.warn('[AMAZON_MATCH_FAILED]', {
        sessionName,
        sourceId: source?.id ?? null,
        messageId: structuredMessage?.messageId || '',
        reason: searchResult.blockedReason || 'Amazon-Suche wurde durch Schutzseite blockiert.'
      });
      return {
        attempted: true,
        matched: false,
        matchScore: 0,
        reason: searchResult.blockedReason || 'Amazon-Suche wurde durch Schutzseite blockiert.',
        sourceFacts
      };
    }

    logAmazonSearchCandidates({
      sessionName,
      source,
      structuredMessage,
      searchQuery: searchResult.query || searchQuery,
      scoredCandidates: searchResult.scoredCandidates
    });

    if (!searchResult.bestCandidate && searchResult.candidates.length === 0) {
      console.info('[AMAZON_MATCH_PRIMARY_FAILED]', {
        sessionName,
        sourceId: source?.id ?? null,
        messageId: structuredMessage?.messageId || '',
        query: searchQuery || '',
        reason: 'Keine Amazon-Kandidaten im ersten Suchlauf.'
      });

      const fallbackQuery = buildAmazonFallbackSearchQuery(sourceFacts);
      const normalizedPrimaryQuery = cleanText(searchQuery).toLowerCase();
      const normalizedFallbackQuery = cleanText(fallbackQuery).toLowerCase();

      if (normalizedFallbackQuery && normalizedFallbackQuery !== normalizedPrimaryQuery) {
        console.info('[AMAZON_MATCH_FALLBACK_STARTED]', {
          sessionName,
          sourceId: source?.id ?? null,
          messageId: structuredMessage?.messageId || '',
          primaryQuery: searchQuery || '',
          fallbackQuery
        });

        const fallbackSearchResult = await fetchAmazonSearchCandidatesForQuery({
          sessionName,
          source,
          structuredMessage,
          sourceFacts,
          searchQuery: fallbackQuery,
          searchMode: 'title',
          fallbackMode: true
        });

        if (fallbackSearchResult.blocked) {
          console.warn('[AMAZON_MATCH_FALLBACK_FAILED]', {
            sessionName,
            sourceId: source?.id ?? null,
            messageId: structuredMessage?.messageId || '',
            fallbackQuery,
            reason: fallbackSearchResult.blockedReason || 'Fallback-Suche wurde blockiert.'
          });
          console.warn('[AMAZON_MATCH_FAILED]', {
            sessionName,
            sourceId: source?.id ?? null,
            messageId: structuredMessage?.messageId || '',
            reason: fallbackSearchResult.blockedReason || 'Amazon-Suche wurde durch Schutzseite blockiert.'
          });
          return {
            attempted: true,
            matched: false,
            matchScore: 0,
            reason: fallbackSearchResult.blockedReason || 'Amazon-Suche wurde durch Schutzseite blockiert.',
            sourceFacts,
            matchTier: 'debug',
            decision: 'REVIEW'
          };
        }

        logAmazonSearchCandidates({
          sessionName,
          source,
          structuredMessage,
          searchQuery: fallbackSearchResult.query || fallbackQuery,
          scoredCandidates: fallbackSearchResult.scoredCandidates
        });

        if (fallbackSearchResult.bestCandidate) {
          searchResult = fallbackSearchResult;
          activeSourceFacts = fallbackSearchResult.scoringSourceFacts || sourceFacts;
          console.info('[AMAZON_MATCH_FALLBACK_SUCCESS]', {
            sessionName,
            sourceId: source?.id ?? null,
            messageId: structuredMessage?.messageId || '',
            fallbackQuery,
            asin: searchResult.bestCandidate.asin || '',
            matchScore: searchResult.bestCandidate.matchScore ?? 0
          });
        } else {
          console.warn('[AMAZON_MATCH_FALLBACK_FAILED]', {
            sessionName,
            sourceId: source?.id ?? null,
            messageId: structuredMessage?.messageId || '',
            fallbackQuery,
            reason: 'Auch mit bereinigter Query keine Amazon-Kandidaten gefunden.'
          });
        }
      } else {
        console.warn('[AMAZON_MATCH_FALLBACK_FAILED]', {
          sessionName,
          sourceId: source?.id ?? null,
          messageId: structuredMessage?.messageId || '',
          primaryQuery: searchQuery || '',
          fallbackQuery: fallbackQuery || '',
          reason: 'Keine bereinigte Fallback-Query verfuegbar.'
        });
      }
    }

    const bestCandidate = searchResult.bestCandidate || null;

    if (!bestCandidate) {
      console.info('[PRODUCT_MATCH_REJECTED]', {
        sessionName,
        sourceId: source?.id ?? null,
        messageId: structuredMessage?.messageId || '',
        matchScore: 0,
        reason: 'Keine Amazon-Kandidaten gefunden.'
      });
      console.warn('[AMAZON_MATCH_FAILED]', {
        sessionName,
        sourceId: source?.id ?? null,
        messageId: structuredMessage?.messageId || '',
        reason: 'Keine Amazon-Kandidaten gefunden.'
      });
      return {
        attempted: true,
        matched: false,
        matchScore: 0,
        reason: 'Keine Amazon-Kandidaten gefunden.',
        sourceFacts,
        matchTier: 'debug',
        decision: 'REVIEW'
      };
    }

    const matchReason = buildSourceMatchReasonSummary(activeSourceFacts, bestCandidate);
    const relaxedMatch = classifyRelaxedAmazonMatchScore(bestCandidate?.matchScore ?? 0, activeSourceFacts, bestCandidate);
    console.info('[AMAZON_MATCH_SELECTED]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      asin: bestCandidate.asin || '',
      candidateSource: bestCandidate.candidateSource || 'amazon_search',
      matchScore: bestCandidate.matchScore ?? 0,
      matchReason
    });

    if (relaxedMatch.matched !== true) {
      console.info('[PRODUCT_MATCH_REJECTED]', {
        sessionName,
        sourceId: source?.id ?? null,
        messageId: structuredMessage?.messageId || '',
        matchScore: bestCandidate?.matchScore ?? 0,
        reason: relaxedMatch.reason
      });
      return {
        attempted: true,
        matched: false,
        matchScore: bestCandidate?.matchScore ?? 0,
        reason: relaxedMatch.reason,
        sourceFacts,
        matchTier: relaxedMatch.tier,
        decision: relaxedMatch.decision,
        matchReason,
        reviewReasons: relaxedMatch.reviewReasons || []
      };
    }

    const linkRecord = buildAmazonAffiliateLinkRecord(bestCandidate.normalizedUrl || bestCandidate.asin, {
      asin: bestCandidate.asin
    });
    if (!linkRecord.valid || !cleanText(linkRecord.affiliateUrl)) {
      console.warn('[AMAZON_MATCH_FAILED]', {
        sessionName,
        sourceId: source?.id ?? null,
        messageId: structuredMessage?.messageId || '',
        asin: bestCandidate.asin || '',
        reason: 'Amazon-Match erkannt, aber Partnerlink konnte nicht gebaut werden.'
      });
      return {
        attempted: true,
        matched: false,
        matchScore: bestCandidate.matchScore,
        reason: 'Amazon-Match erkannt, aber Partnerlink konnte nicht gebaut werden.',
        sourceFacts,
        matchTier: 'review',
        decision: 'REVIEW',
        matchReason,
        reviewReasons: relaxedMatch.reviewReasons || []
      };
    }

    const matchedScrapedDeal = buildMatchedAmazonRecoveryDeal({
      sourceFacts,
      candidate: {
        ...bestCandidate,
        sourceOnlyPriceUsed:
          cleanText(bestCandidate.priceSource).toLowerCase() !== 'paapi' && sourceFacts.priceValue !== null && !bestCandidate.priceDisplay
      },
      linkRecord
    });
    console.info(relaxedMatch.decision === 'APPROVE' ? '[PRODUCT_MATCH_APPROVED]' : '[PRODUCT_MATCH_REVIEW]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      asin: bestCandidate.asin || '',
      matchScore: bestCandidate.matchScore,
      matchReason,
      reason: relaxedMatch.reason
    });

    return {
      attempted: true,
      matched: true,
      matchScore: bestCandidate.matchScore,
      sourceFacts,
      linkRecord,
      scrapedDeal: matchedScrapedDeal,
      matchTier: relaxedMatch.tier,
      decision: relaxedMatch.decision,
      reason: relaxedMatch.reason,
      matchReason,
      reviewReasons: relaxedMatch.reviewReasons || []
    };
  } catch (error) {
    console.info('[PRODUCT_MATCH_REJECTED]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      matchScore: 0,
      reason: error instanceof Error ? error.message : 'Amazon-Suche aus Quellendaten fehlgeschlagen.'
    });
    console.warn('[AMAZON_MATCH_FAILED]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      reason: error instanceof Error ? error.message : 'Amazon-Suche aus Quellendaten fehlgeschlagen.'
    });
    return {
      attempted: true,
      matched: false,
      matchScore: 0,
      reason: error instanceof Error ? error.message : 'Amazon-Suche aus Quellendaten fehlgeschlagen.',
      sourceFacts,
      matchTier: 'debug',
      decision: 'REVIEW'
    };
  }
}

function isOwnAmazonAffiliateLink(value = '', asin = '') {
  const trimmed = cleanText(value);
  const normalizedAsin = cleanText(asin).toUpperCase();
  if (!trimmed || !normalizedAsin) {
    return false;
  }

  return new RegExp(`^https://www\\.amazon\\.de/dp/${normalizedAsin}\\?tag=`, 'i').test(trimmed);
}

function resolveProductVerification({
  sessionName = '',
  source = {},
  structuredMessage = {},
  dealType = 'AMAZON',
  linkRecord = {},
  scrapedDeal = {},
  generatorInput = {},
  sourceMeta = {},
  readerConfig = {}
} = {}) {
  console.info('[PRODUCT_VERIFICATION_START]', {
    sessionName,
    sourceId: source?.id ?? null,
    messageId: structuredMessage?.messageId || '',
    dealType,
    asin: cleanText(generatorInput?.asin || scrapedDeal?.asin).toUpperCase() || ''
  });

  const issues = [];
  const relaxedTestMode = isReaderTestGroupAllMode(readerConfig);
  const normalizedDealType = cleanText(dealType).toUpperCase() || 'AMAZON';
  const asin = cleanText(generatorInput?.asin || scrapedDeal?.asin).toUpperCase();
  const title = cleanText(generatorInput?.title || scrapedDeal?.productTitle || scrapedDeal?.title);
  const verifiedAmazonPrice = cleanText(
    scrapedDeal?.basePrice ||
      (scrapedDeal?.finalPriceCalculated === true ? scrapedDeal?.finalPrice : '') ||
      scrapedDeal?.price
  );
  const priceValue = parseTelegramLocalizedNumber(verifiedAmazonPrice);
  const hasAmazonImage = cleanText(scrapedDeal?.imageUrl) ? true : false;
  const hasScreenshot = cleanText(generatorInput?.generatedImagePath || generatorInput?.uploadedImagePath) ? true : false;
  const affiliateUrl = cleanText(linkRecord?.affiliateUrl || generatorInput?.link);
  const hasReaderLink = Boolean(cleanText(linkRecord?.affiliateUrl || generatorInput?.link));
  const hasReaderImage = Boolean(cleanText(generatorInput?.generatedImagePath || generatorInput?.uploadedImagePath));

  if (normalizedDealType !== 'AMAZON') {
    if (!title) {
      issues.push('Titel fehlt.');
    }
    if (!(priceValue > 0)) {
      issues.push('Preis fehlt oder ist ungueltig.');
    }
    if (!hasReaderLink) {
      issues.push('Original-Link fehlt.');
    }
    if (!hasReaderImage) {
      issues.push('Bild fehlt.');
    }

    if (issues.length && relaxedTestMode) {
      console.info('[PRODUCT_VERIFICATION_WARNING]', {
        sessionName,
        sourceId: source?.id ?? null,
        messageId: structuredMessage?.messageId || '',
        dealType: normalizedDealType,
        issues
      });
      return {
        verified: true,
        warningOnly: true,
        reason: issues.join(' '),
        issues
      };
    }

    if (!issues.length) {
      console.info('[PRODUCT_VERIFIED]', {
        sessionName,
        sourceId: source?.id ?? null,
        messageId: structuredMessage?.messageId || '',
        asin,
        title,
        affiliateUrl
      });
      return {
        verified: true,
        warningOnly: false,
        reason: '',
        issues: []
      };
    }
  }

  if (sourceMeta?.protectedSource === true) {
    issues.push('Cloudflare / geschuetzte Quelle erkannt.');
  }
  if (!asin) {
    issues.push('ASIN fehlt.');
  }
  if (!title || isProtectedSourceValue(title)) {
    issues.push('Amazon-Titel fehlt oder ist ungueltig.');
  }
  if (!(priceValue > 0)) {
    issues.push('Amazon-Preis fehlt oder ist ungueltig.');
  }
  if (!isOwnAmazonAffiliateLink(affiliateUrl, asin)) {
    issues.push('Eigener Amazon-Partnerlink fehlt.');
    if (!relaxedTestMode) {
      console.error('[AFFILIATE_LINK_REQUIRED]', {
        sessionName,
        sourceId: source?.id ?? null,
        messageId: structuredMessage?.messageId || '',
        asin,
        affiliateUrl: affiliateUrl || ''
      });
    }
  }
  if (!hasAmazonImage && !hasScreenshot) {
    issues.push('Amazon-Bild oder Screenshot fehlt.');
    if (!relaxedTestMode) {
      console.error('[AMAZON_IMAGE_REQUIRED]', {
        sessionName,
        sourceId: source?.id ?? null,
        messageId: structuredMessage?.messageId || '',
        asin
      });
      console.error('[GENERATOR_SCREENSHOT_REQUIRED]', {
        sessionName,
        sourceId: source?.id ?? null,
        messageId: structuredMessage?.messageId || '',
        asin
      });
    }
  }

  if (issues.length) {
    if (normalizedDealType !== 'AMAZON' && relaxedTestMode) {
      console.info('[PRODUCT_VERIFICATION_WARNING]', {
        sessionName,
        sourceId: source?.id ?? null,
        messageId: structuredMessage?.messageId || '',
        asin,
        issues
      });
      return {
        verified: true,
        warningOnly: true,
        reason: issues.join(' '),
        issues
      };
    }

    console.error('[PRODUCT_VERIFICATION_FAILED]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      asin,
      issues
    });
    console.error('[UNVERIFIED_PRODUCT_BLOCKED]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      asin,
      issues
    });
    return {
      verified: false,
      warningOnly: false,
      reason: issues.join(' '),
      issues
    };
  }

  console.info('[PRODUCT_VERIFIED]', {
    sessionName,
    sourceId: source?.id ?? null,
    messageId: structuredMessage?.messageId || '',
    asin,
    title,
    affiliateUrl
  });

  return {
    verified: true,
    warningOnly: false,
    reason: '',
    issues: []
  };
}

function buildReaderDiagnosticPostText({
  reason = '',
  sourceHost = '',
  blockedCode = '',
  liveAllowed = false
} = {}) {
  const lines = ['⚠️ Testpost nicht freigegeben'];
  lines.push(`Grund: ${cleanText(reason) || 'Produkt nicht verifiziert.'}`);
  if (sourceHost) {
    lines.push(`Quelle: ${sourceHost}`);
  }
  if (blockedCode) {
    lines.push(`Code: ${blockedCode}`);
  }
  lines.push(`Live: ${liveAllowed === true ? 'JA' : 'NEIN'}`);
  return lines.join('\n');
}

function buildReaderDiagnosticPostTextV2({
  reason = '',
  sourceHost = '',
  blockedCode = '',
  liveAllowed = false,
  testGroupPosted = false,
  channelRef = '',
  channelTitle = '',
  group = '',
  mainPostBlocked = false,
  titleSource = '',
  imageSource = '',
  affiliateLinkSource = ''
} = {}) {
  console.info('[READER_DIAGNOSIS_MERGED_INTO_DEBUG]', {
    source: 'reader_diagnostic',
    blockedCode: cleanText(blockedCode) || null,
    sourceHost: cleanText(sourceHost) || null
  });
  console.info('[DEBUG_INFO_MERGED]', {
    source: 'reader_diagnostic',
    blockedCode: cleanText(blockedCode) || null,
    sourceHost: cleanText(sourceHost) || null
  });

  return buildTelegramDealDebugInfoExtended(
    buildTelegramDiagnosticDebugValues({
      reason,
      blockedCode,
      sourceHost,
      channelRef,
      channelTitle,
      group,
      mainPostBlocked,
      titleSource,
      imageSource,
      affiliateLinkSource,
      liveAllowed,
      testGroupPosted,
      marketComparisonStatus: 'blocked'
    })
  );
}

async function scrapeGenericDealPage(inputUrl = '') {
  const targetUrl = cleanText(inputUrl);
  if (!targetUrl || !/^https?:\/\//i.test(targetUrl)) {
    return {
      success: false,
      statusCode: 0,
      blockedByProtection: false,
      protectedMatches: [],
      finalUrl: targetUrl,
      normalizedUrl: targetUrl,
      title: '',
      productDescription: '',
      price: '',
      imageUrl: '',
      imageSource: '',
      ogImage: '',
      previewImage: ''
    };
  }

  const response = await fetch(targetUrl, {
    headers: {
      'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36',
      'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8'
    }
  });
  const html = await response.text();
  const finalUrl = cleanText(response.url) || targetUrl;
  const imageCandidates = extractGenericDealImageCandidates(html, finalUrl);
  const winner = imageCandidates[0] || null;
  const protectedMatches = collectProtectedSourceMatches([{ source: 'generic_html', value: html.slice(0, 2000) }]);
  const blockedByProtection = isProtectedSourceStatusCode(response?.status) || protectedMatches.length > 0;

  return {
    success: response.ok,
    statusCode: Number(response?.status || 0),
    blockedByProtection,
    protectedMatches,
    finalUrl,
    resolvedUrl: finalUrl,
    normalizedUrl: finalUrl,
    title: extractGenericDealTitleFromHtml(html),
    productDescription: extractGenericDealDescriptionFromHtml(html),
    price: extractGenericDealPriceFromHtml(html),
    imageUrl: winner?.value || '',
    imageSource: winner?.source || '',
    ogImage: imageCandidates.find((candidate) => candidate.source === 'ogImage')?.value || '',
    previewImage: imageCandidates.find((candidate) => candidate.source === 'twitterImage')?.value || '',
    scrapedImage: imageCandidates.find((candidate) => candidate.source === 'scrapedImage')?.value || '',
    bulletPoints: [],
    sellerType: 'UNKNOWN',
    sellerClass: 'UNKNOWN',
    soldByAmazon: null,
    shippedByAmazon: null,
    sellerDetails: {
      detectionSource: 'non-amazon',
      detectionSources: ['non-amazon'],
      merchantText: '',
      matchedPatterns: [],
      dealType: 'NON_AMAZON',
      isAmazonDeal: false
    }
  };
}

function parseTelegramLocalizedNumber(value = '') {
  const raw = cleanText(String(value)).replace(/[^0-9.,-]/g, '');
  if (!raw) {
    return null;
  }

  let normalized = raw;

  if (raw.includes(',') && raw.includes('.')) {
    normalized =
      raw.lastIndexOf(',') > raw.lastIndexOf('.')
        ? raw.replace(/\./g, '').replace(',', '.')
        : raw.replace(/,/g, '');
  } else if (raw.includes(',')) {
    normalized = raw.replace(',', '.');
  }

  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeReaderPriceCandidate(value = '') {
  return cleanText(formatPrice(value));
}

function readerPricesEqual(firstValue = '', secondValue = '') {
  const first = parseTelegramLocalizedNumber(firstValue);
  const second = parseTelegramLocalizedNumber(secondValue);

  if (first === null || second === null) {
    return false;
  }

  return Math.abs(first - second) < 0.005;
}

function buildReaderAmazonPriceCandidates(scrapedDeal = {}) {
  return [
    { source: 'amazonBuyBox', value: normalizeReaderPriceCandidate(scrapedDeal?.basePrice) },
    {
      source: 'paapiPrice',
      value: normalizeReaderPriceCandidate(
        scrapedDeal?.paapiPrice ||
          scrapedDeal?.amazonPrice ||
          scrapedDeal?.paapiCurrentPrice ||
          scrapedDeal?.imageDebug?.paapiPrice
      )
    },
    {
      source: 'amazonFinalPrice',
      value: normalizeReaderPriceCandidate(scrapedDeal?.finalPriceCalculated === true ? scrapedDeal?.finalPrice : '')
    },
    { source: 'amazonScrapePrice', value: normalizeReaderPriceCandidate(scrapedDeal?.price) }
  ].filter((candidate) => candidate.value);
}

function resolveReaderPaapiPriceCandidate(scrapedDeal = {}) {
  const explicitPaapiPrice = normalizeReaderPriceCandidate(
    scrapedDeal?.paapiPrice ||
      scrapedDeal?.amazonPrice ||
      scrapedDeal?.paapiCurrentPrice ||
      scrapedDeal?.imageDebug?.paapiPrice
  );

  if (explicitPaapiPrice) {
    return explicitPaapiPrice;
  }

  const dataSource = cleanText(scrapedDeal?.dataSource).toLowerCase();
  const imageSource = cleanText(scrapedDeal?.imageDebug?.selectedSource).toLowerCase();
  const paapiBackedPrice =
    dataSource === 'paapi' || imageSource === 'paapi'
      ? normalizeReaderPriceCandidate(scrapedDeal?.basePrice || scrapedDeal?.price)
      : '';

  return paapiBackedPrice || '';
}

function resolveReaderKeepaPriceCandidate(scrapedDeal = {}, pricing = {}) {
  return normalizeReaderPriceCandidate(
    scrapedDeal?.keepaPrice || scrapedDeal?.keepaCurrentPrice || pricing?.keepaPrice || pricing?.keepaCurrentPrice
  );
}

function normalizeReaderTransparentPriceSource(rawPriceSource = '') {
  const source = cleanText(rawPriceSource).toLowerCase();

  if (source.includes('paapi')) {
    return 'paapi';
  }

  if (source.includes('scrape') || source.includes('scraped') || source.includes('amazonfinal') || source.includes('amazonbuybox')) {
    return 'scrape';
  }

  if (source.includes('keepa')) {
    return 'keepa';
  }

  return 'fallback';
}

function logReaderTransparentPriceSource({ dealType = '', priceSource = '', rawPriceSource = '', price = '' } = {}) {
  const normalizedSource = normalizeReaderTransparentPriceSource(priceSource || rawPriceSource);
  const tagBySource = {
    paapi: '[PRICE_SOURCE_PAAPI]',
    scrape: '[PRICE_SOURCE_SCRAPE]',
    keepa: '[PRICE_SOURCE_KEEPA]'
  };
  const tag = tagBySource[normalizedSource];

  if (!tag) {
    return;
  }

  console.info(tag, {
    dealType: cleanText(dealType).toUpperCase() || 'AMAZON',
    source: normalizedSource,
    rawSource: cleanText(rawPriceSource) || 'unknown',
    price: cleanText(price) || null
  });
}

function resolveReaderPricePayload({ dealType = 'AMAZON', scrapedDeal = {}, pricing = {} } = {}) {
  const normalizedDealType = cleanText(dealType).toUpperCase() || 'AMAZON';
  const amazonPriceCandidates = buildReaderAmazonPriceCandidates(scrapedDeal);
  const amazonBuyBoxPrice = amazonPriceCandidates.find((candidate) => candidate.source === 'amazonBuyBox')?.value || '';
  const amazonDealPrice =
    amazonPriceCandidates.find((candidate) => candidate.source === 'amazonFinalPrice')?.value ||
    amazonPriceCandidates.find((candidate) => candidate.source === 'amazonScrapePrice')?.value ||
    '';
  const scrapedPrice = amazonPriceCandidates.find((candidate) => candidate.source === 'amazonScrapePrice')?.value || '';
  const telegramPrice =
    pricing?.currentPrice !== null && pricing?.currentPrice !== undefined
      ? normalizeReaderPriceCandidate(String(pricing.currentPrice))
      : '';
  const paapiPrice = resolveReaderPaapiPriceCandidate(scrapedDeal);
  const keepaPrice = resolveReaderKeepaPriceCandidate(scrapedDeal, pricing);
  const distinctAmazonPrices = [];

  for (const candidate of amazonPriceCandidates) {
    if (!distinctAmazonPrices.some((entry) => readerPricesEqual(entry.value, candidate.value))) {
      distinctAmazonPrices.push(candidate);
    }
  }

  let currentPrice = '';
  let priceSource = 'unknown';
  let rawPriceSource = 'unknown';

  if (normalizedDealType === 'AMAZON') {
    const selectedAmazonCandidate =
      [
        { source: 'paapiPrice', value: paapiPrice },
        { source: 'amazonFinalPrice', value: amazonDealPrice },
        { source: 'amazonScrapePrice', value: scrapedPrice },
        { source: 'amazonBuyBox', value: amazonBuyBoxPrice },
        { source: 'keepaPrice', value: keepaPrice },
        { source: 'telegramPrice', value: telegramPrice }
      ].find((candidate) => candidate.value) || null;

    if (distinctAmazonPrices.length > 1) {
      console.info('[PRICE_DEDUPED]', {
        dealType: normalizedDealType,
        selectedSource: selectedAmazonCandidate?.source || 'unknown',
        selectedPrice: selectedAmazonCandidate?.value || null,
        droppedSources: distinctAmazonPrices.slice(1).map((candidate) => candidate.source),
        droppedPrices: distinctAmazonPrices.slice(1).map((candidate) => candidate.value)
      });
    }

    if (selectedAmazonCandidate?.value) {
      currentPrice = selectedAmazonCandidate.value;
      priceSource = normalizeReaderTransparentPriceSource(selectedAmazonCandidate.source);
      rawPriceSource = selectedAmazonCandidate.source;
    }

    if (telegramPrice && rawPriceSource === 'paapiPrice' && !readerPricesEqual(currentPrice, telegramPrice)) {
      console.info('[SOURCE_VALUES_STRIPPED]', {
        dealType: normalizedDealType,
        strippedField: 'price',
        blockedSource: 'telegram',
        blockedValue: telegramPrice,
        keptSource: rawPriceSource,
        keptValue: currentPrice || null
      });
      console.info('[DUPLICATE_PRICE_BLOCKED]', {
        dealType: normalizedDealType,
        keptSource: rawPriceSource || 'amazon',
        blockedSource: 'telegram',
        keptPrice: currentPrice || null,
        blockedPrice: telegramPrice
      });
    }
    if (scrapedPrice && rawPriceSource !== 'unknown' && !readerPricesEqual(currentPrice, scrapedPrice)) {
      console.info('[SOURCE_VALUES_STRIPPED]', {
        dealType: normalizedDealType,
        strippedField: 'price',
        blockedSource: 'amazon_scrape',
        blockedValue: scrapedPrice,
        keptSource: rawPriceSource,
        keptValue: currentPrice || null
      });
    }
  } else if (scrapedPrice) {
    currentPrice = scrapedPrice;
    priceSource = 'scrape';
    rawPriceSource = 'scraped';
  } else if (telegramPrice) {
    currentPrice = telegramPrice;
    priceSource = 'fallback';
    rawPriceSource = 'telegram';
  }

  console.info('[FINAL_PRICE_SELECTED]', {
    dealType: normalizedDealType,
    source: priceSource,
    rawSource: rawPriceSource,
    price: currentPrice || null
  });
  console.info('[PRICE_SOURCE_SELECTED]', {
    dealType: normalizedDealType,
    source: priceSource,
    rawSource: rawPriceSource,
    price: currentPrice || null
  });
  logReaderTransparentPriceSource({
    dealType: normalizedDealType,
    priceSource,
    rawPriceSource,
    price: currentPrice
  });

  return {
    currentPrice,
    oldPrice: '',
    priceSource,
    rawPriceSource,
    amazonBuyBoxPrice,
    amazonDealPrice,
    telegramPrice,
    keepaPrice,
    amazonPriceCandidates: distinctAmazonPrices
  };
}

function resolveInvalidPriceState(value = '') {
  const normalizedValue = typeof value === 'number' ? String(value) : cleanText(String(value || ''));
  const parsedValue = parseTelegramLocalizedNumber(normalizedValue);

  if (parsedValue === null) {
    return {
      invalid: true,
      parsedValue: null,
      reason: 'Preis fehlt oder konnte nicht erkannt werden.'
    };
  }

  if (parsedValue <= 0) {
    return {
      invalid: true,
      parsedValue,
      reason: 'Preis ist 0,00€ oder ungueltig.'
    };
  }

  return {
    invalid: false,
    parsedValue,
    reason: ''
  };
}

function extractTelegramDealPricing(text = '') {
  const priceMatches = Array.from(
    String(text || '').matchAll(/(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?|\d+(?:[.,]\d{2})?)\s*€/gi)
  )
    .map((match) => parseTelegramLocalizedNumber(match[1]))
    .filter((value) => value !== null);
  const currentPrice = priceMatches[0] ?? null;
  let oldPrice = priceMatches[1] ?? null;

  if (currentPrice !== null && oldPrice !== null && oldPrice < currentPrice) {
    const nextOldPrice = currentPrice;
    const nextCurrentPrice = oldPrice;
    oldPrice = nextOldPrice;
    return {
      currentPrice: nextCurrentPrice,
      oldPrice: nextOldPrice,
      detectedDiscount:
        nextCurrentPrice > 0
          ? Math.round(((nextOldPrice - nextCurrentPrice) / nextOldPrice) * 10000) / 100
          : null
    };
  }

  const discountMatch = String(text || '').match(/(-?\d{1,3}(?:[.,]\d{1,2})?)\s*%/i);
  const detectedDiscount = discountMatch ? Math.abs(parseTelegramLocalizedNumber(discountMatch[1]) ?? 0) : null;

  return {
    currentPrice,
    oldPrice,
    detectedDiscount:
      detectedDiscount !== null
        ? detectedDiscount
        : currentPrice !== null && oldPrice !== null && oldPrice > 0
          ? Math.round(((oldPrice - currentPrice) / oldPrice) * 10000) / 100
          : null
  };
}

function extractTelegramCouponCode(text = '') {
  const match = String(text || '').match(/\b(?:code|coupon|gutschein(?:code)?)[:\s-]*([A-Z0-9-]{4,})\b/i);
  return cleanText(match?.[1] || '').toUpperCase();
}

function extractTelegramTitle(text = '', fallback = '') {
  const lines = String(text || '')
    .split(/\r?\n/)
    .map((line) => cleanText(line))
    .filter(Boolean);

  const selectedLine =
    lines.find((line) => !/^https?:\/\//i.test(line) && !/^\d+[.,]?\d*\s*€?$/i.test(line)) ||
    lines[0] ||
    cleanText(fallback) ||
    'Telegram Deal';

  return selectedLine.slice(0, 240);
}

function sanitizeReaderDescriptionValue(value = '') {
  return String(value || '')
    .replace(/<script[\s\S]*?<\/script>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function wrapReaderDescription(value = '', maxLineLength = 72, maxLines = 2) {
  const cleaned = sanitizeReaderDescriptionValue(value);
  if (!cleaned) {
    return '';
  }

  const words = cleaned.split(/\s+/).filter(Boolean);
  const lines = [];
  let currentLine = '';

  for (const word of words) {
    const nextLine = currentLine ? `${currentLine} ${word}` : word;
    if (nextLine.length <= maxLineLength) {
      currentLine = nextLine;
      continue;
    }

    if (currentLine) {
      lines.push(currentLine);
      if (lines.length >= maxLines) {
        break;
      }
    }

    currentLine = word;
  }

  if (currentLine && lines.length < maxLines) {
    lines.push(currentLine);
  }

  return lines.join('\n').slice(0, 180).trim();
}

function sanitizeReaderPostTitle(value = '', fallback = '') {
  const cleaned = sanitizeReaderDescriptionValue(value || fallback || '')
    .replace(/\s+/g, ' ')
    .trim();

  return cleaned.slice(0, 240);
}

function selectReaderAmazonTitleCandidate(scrapedDeal = {}) {
  return (
    [
      { source: 'amazonProductTitle', value: sanitizeReaderPostTitle(scrapedDeal?.productTitle) },
      { source: 'amazonScrapedTitle', value: sanitizeReaderPostTitle(scrapedDeal?.title) }
    ].find((candidate) => candidate.value) || null
  );
}

function resolveReaderTitlePayload({ dealType = 'AMAZON', scrapedDeal = {}, structuredMessage = {} } = {}) {
  const normalizedDealType = cleanText(dealType).toUpperCase() || 'AMAZON';
  const telegramCandidate = sanitizeReaderPostTitle(
    cleanText(structuredMessage?.previewTitle) || extractTelegramTitle(structuredMessage?.text, structuredMessage?.group)
  );

  if (normalizedDealType === 'AMAZON') {
    const amazonCandidate = selectReaderAmazonTitleCandidate(scrapedDeal);

    if (telegramCandidate && telegramCandidate !== amazonCandidate?.value) {
      console.info('[SOURCE_VALUES_STRIPPED]', {
        dealType: normalizedDealType,
        strippedField: 'title',
        blockedSource: 'telegram',
        blockedValue: telegramCandidate.slice(0, 120),
        keptSource: amazonCandidate?.source || 'missing_amazon_title',
        keptValue: amazonCandidate?.value?.slice(0, 120) || null
      });
      console.info('[TITLE_SOURCE_BLOCKED]', {
        dealType: normalizedDealType,
        blockedSource: 'telegram',
        blockedTitle: telegramCandidate.slice(0, 120)
      });
    }

    console.info('[TITLE_SOURCE_SELECTED]', {
      dealType: normalizedDealType,
      source: 'amazon',
      titleSource: amazonCandidate?.source || 'amazonDefaultFallback',
      usedFallback: amazonCandidate ? false : true
    });

    return {
      title: amazonCandidate?.value || '',
      titleSource: 'amazon',
      rawTitleSource: amazonCandidate?.source || 'amazonMissingTitle'
    };
  }

  const selectedCandidate =
    [
      { source: 'scraped', value: sanitizeReaderPostTitle(scrapedDeal?.title) },
      { source: 'preview', value: sanitizeReaderPostTitle(structuredMessage?.previewTitle) },
      { source: 'telegram', value: sanitizeReaderPostTitle(extractTelegramTitle(structuredMessage?.text, structuredMessage?.group)) }
    ].find((candidate) => candidate.value) || null;

  console.info('[TITLE_SOURCE_SELECTED]', {
    dealType: normalizedDealType,
    source: selectedCandidate?.source || 'fallback',
    titleSource: selectedCandidate?.source || 'fallback'
  });

  return {
    title: selectedCandidate?.value || 'Deal',
    titleSource: selectedCandidate?.source || 'fallback',
    rawTitleSource: selectedCandidate?.source || 'fallback'
  };
}

function extractReaderProductDescription({ scrapedDeal = {}, structuredMessage = {} } = {}) {
  const productTitle = sanitizeReaderDescriptionValue(scrapedDeal?.productTitle || scrapedDeal?.title || '');
  const productDescription = sanitizeReaderDescriptionValue(scrapedDeal?.productDescription || '');
  const bulletPoints = Array.isArray(scrapedDeal?.bulletPoints)
    ? scrapedDeal.bulletPoints.map((value) => sanitizeReaderDescriptionValue(value)).filter(Boolean)
    : [];
  const fallbackTitle = sanitizeReaderDescriptionValue(extractTelegramTitle(structuredMessage?.text, structuredMessage?.group));
  const candidates = [
    { source: 'productTitle', value: productTitle },
    { source: 'productDescription', value: productDescription },
    { source: 'bulletPoints', value: bulletPoints.slice(0, 2).join(' • ') },
    { source: 'fallbackTitle', value: fallbackTitle }
  ];
  const selectedCandidate = candidates.find((candidate) => candidate.value) || {
    source: 'fallbackTitle',
    value: 'Amazon Produkt'
  };
  const description = wrapReaderDescription(selectedCandidate.value);

  if (selectedCandidate.source === 'fallbackTitle') {
    console.info('[PRODUCT_DESCRIPTION_FALLBACK]', {
      source: selectedCandidate.source,
      value: description
    });
  } else {
    console.info('[PRODUCT_DESCRIPTION_EXTRACTED]', {
      source: selectedCandidate.source,
      value: description
    });
  }

  return description || 'Amazon Produkt';
}

function inferTelegramSellerSignals(text = '') {
  return extractSellerSignalsFromText(cleanText(text), {
    detectionSource: 'telegram_message'
  });
}

async function formatTelegramMessage(message, fallbackGroup = '') {
  const text = cleanText(message?.message || message?.text || '');
  let chat = null;

  try {
    chat = await message.getChat();
  } catch {
    chat = null;
  }

  const username = cleanText(chat?.username);
  const chatId = cleanText(chat?.id ? String(chat.id) : '');
  const allLinks = extractAllLinks(text);
  const externalLink = extractFirstLink(text);
  const webpage = message?.media?.webpage || message?.webpage || null;
  const previewUrl = cleanText(webpage?.url || webpage?.displayUrl || '');
  const previewTitle = cleanText(webpage?.title || '');
  const previewDescription = cleanText(webpage?.description || '');
  const previewImage = cleanText(webpage?.imageUrl || webpage?.image?.url || webpage?.photo?.url || '');
  const telegramMediaDataUrl = await tryDownloadTelegramMediaDataUrl(message);
  const messageLink =
    externalLink ||
    (username && message?.id ? `https://t.me/${username}/${message.id}` : '');
  const group =
    cleanText(chat?.title) ||
    cleanText(chat?.username) ||
    fallbackGroup ||
    chatId ||
    'Telegram';
  const channelRef = username ? `@${username}` : '';
  const channelTitle = group;
  const normalizedTimestamp = normalizeTelegramDate(message?.date);

  console.log('[CHANNEL DETECTED]', {
    chatId: chatId,
    channelRef: channelRef,
    channelTitle: channelTitle
  });

  return {
    sessionName: '',
    messageId: message?.id ? String(message.id) : '',
    chatId,
    channelRef,
    channelTitle,
    text,
    link: messageLink,
    externalLink,
    allLinks,
    previewUrl,
    previewTitle,
    previewDescription,
    previewImage,
    telegramMediaDataUrl,
    group,
    timestamp: normalizedTimestamp?.toISOString() || nowIso()
  };
}

function updateChannelCheckpoint(channelId, lastSeenMessageId, lastSeenMessageAt) {
  db.prepare(
    `
      UPDATE telegram_reader_channels
      SET last_seen_message_id = @lastSeenMessageId,
          last_seen_message_at = @lastSeenMessageAt,
          updated_at = @updatedAt
      WHERE id = @id
    `
  ).run({
    id: channelId,
    lastSeenMessageId: lastSeenMessageId ? String(lastSeenMessageId) : null,
    lastSeenMessageAt: lastSeenMessageAt || null,
    updatedAt: nowIso()
  });
}

function updateChannelLastChecked(channelId, lastCheckedAt = nowIso()) {
  db.prepare(
    `
      UPDATE telegram_reader_channels
      SET last_checked_at = @lastCheckedAt,
          updated_at = @updatedAt
      WHERE id = @id
    `
  ).run({
    id: channelId,
    lastCheckedAt,
    updatedAt: nowIso()
  });
}

function findAmazonLinkInText(text = '') {
  const matches = String(text || '').match(/https?:\/\/\S+/gi) || [];

  return (
    matches.find((candidate) => /(?:amzn\.to|amazon\.[a-z.]+)/i.test(candidate)) ||
    ''
  );
}

function buildTelegramReaderTemplatePayload({
  title = '',
  description = '',
  affiliateUrl = '',
  currentPrice = '',
  oldPrice = '',
  couponCode = '',
  extraOptions = []
}) {
  const finalTitle = sanitizeReaderPostTitle(title, 'Amazon Produkt') || 'Amazon Produkt';
  const generatorRenderInput = {
    productTitle: finalTitle,
    freiText: '',
    textBaustein: [],
    alterPreis: '',
    neuerPreis: cleanText(currentPrice),
    alterPreisLabel: 'Vorher',
    neuerPreisLabel: 'Jetzt',
    amazonLink: cleanText(affiliateUrl),
    werbung: false,
    extraOptions: [...(Array.isArray(extraOptions) ? extraOptions : []), ...(cleanText(couponCode) ? [COUPON_OPTION_LABEL] : [])],
    rabattgutscheinCode: cleanText(couponCode)
  };
  console.info('[READER_OWN_TEMPLATE_DISABLED]', {
    title: finalTitle,
    templateFunction: 'generatePostText'
  });
  console.info('[READER_USING_GENERATOR_RENDERER]', {
    title: finalTitle,
    templateFunction: 'generatePostText',
    hasAffiliateLink: Boolean(cleanText(affiliateUrl)),
    hasCurrentPrice: Boolean(cleanText(currentPrice))
  });
  return generatePostText(generatorRenderInput);
  const generatedPost = generatePostText({
    productTitle: displayDescription,
    freiText: '',
    textBaustein: [],
    alterPreis: '',
    neuerPreis: cleanText(currentPrice),
    alterPreisLabel: 'Vorher',
    neuerPreisLabel: 'Jetzt',
    amazonLink: cleanText(affiliateUrl),
    werbung: false,
    extraOptions: [...(Array.isArray(extraOptions) ? extraOptions : []), ...(cleanText(couponCode) ? [COUPON_OPTION_LABEL] : [])],
    rabattgutscheinCode: cleanText(couponCode)
  });

  const formattedCurrentPrice = formatCompactPostPrice(currentPrice);
  const formattedOldPrice = formatCompactPostPrice(oldPrice);
  const formattedDescription = wrapReaderDescription(description || title || 'Amazon Produkt');
  const telegramLines = [];
  const whatsappLines = [];

  if (formattedDescription) {
    telegramLines.push(escapeTelegramHtml(formattedDescription));
    whatsappLines.push(formattedDescription);
    console.info('[GENERATOR_DESCRIPTION_ADDED]', {
      title: cleanText(title) || 'Amazon Produkt',
      descriptionLength: formattedDescription.length
    });
  }

  if (formattedCurrentPrice) {
    const telegramPriceLine = `🔥 Jetzt <b>${escapeTelegramHtml(formattedCurrentPrice)}</b>`;
    const whatsappPriceLine = `🔥 Jetzt *${formattedCurrentPrice}*`;
    telegramLines.push(telegramPriceLine);
    whatsappLines.push(whatsappPriceLine);
  }

  if (formattedOldPrice) {
    console.info('[OLD_PRICE_REMOVED]', {
      title: cleanText(title) || 'Amazon Produkt',
      removedOldPrice: formattedOldPrice
    });
  }

  if (cleanText(affiliateUrl)) {
    telegramLines.push('', `👉 <b>${escapeTelegramHtml(cleanText(affiliateUrl))}</b>`);
    whatsappLines.push('', `👉 *${cleanText(affiliateUrl)}*`);
  }

  telegramLines.push('', '<i>Anzeige/Partnerlink</i>');
  whatsappLines.push('', '_Anzeige/Partnerlink_');

  console.info('[GENERATOR_TEMPLATE_APPLIED]', {
    title: cleanText(title) || 'Amazon Produkt',
    hasOldPrice: false,
    hasCurrentPrice: Boolean(formattedCurrentPrice),
    hasAffiliateLink: Boolean(cleanText(affiliateUrl))
  });
  console.info('[GENERATOR_TEMPLATE_UPDATED]', {
    title: cleanText(title) || 'Amazon Produkt',
    oldPriceRemoved: true,
    hasAffiliateLink: Boolean(cleanText(affiliateUrl))
  });
  console.info('[DESCRIPTION_BOLD_APPLIED]', {
    title: cleanText(title) || 'Amazon Produkt',
    descriptionLength: displayDescription.length
  });
  console.info('[LAYOUT_SPACING_FIXED]', {
    title: cleanText(title) || 'Amazon Produkt',
    blankLineBetweenDescriptionAndPrice: true
  });
  console.info('[EMOJI_FIXED]', {
    title: cleanText(title) || 'Amazon Produkt',
    linkIcon: '➡️'
  });
  console.info('[GENERATOR_TEMPLATE_ENFORCED]', {
    title: cleanText(title) || 'Amazon Produkt',
    usesGeneratePostText: true
  });

  return generatedPost;
}

function collectMissingReaderGeneratorInputFields(generatorInput = {}) {
  const missingFields = [];
  const normalizedTitle = cleanText(generatorInput?.title);
  const hasImage = Boolean(cleanText(generatorInput?.generatedImagePath) || cleanText(generatorInput?.uploadedImagePath));

  if (!normalizedTitle || /^(Amazon Produkt|Deal erkannt)$/i.test(normalizedTitle)) {
    missingFields.push('title');
  }
  if (!cleanText(generatorInput?.currentPrice)) {
    missingFields.push('price');
  }
  if (!cleanText(generatorInput?.asin)) {
    missingFields.push('asin');
  }
  if (!cleanText(generatorInput?.link)) {
    missingFields.push('affiliateLink');
  }
  if (!hasImage) {
    missingFields.push('image');
  }

  return missingFields;
}

function logReaderPipelineError(reason = '', payload = {}) {
  const normalizedReason = cleanText(reason) || 'Unbekannter Pipeline-Fehler.';
  console.error('[ERROR_REASON]', {
    reason: normalizedReason,
    ...payload
  });
}

function assertTelegramReaderDebugMode() {
  const readerConfig = getReaderConfig();

  if (readerConfig.readerDebugMode !== true && readerConfig.readerTestMode !== true) {
    throw new Error('READER_DEBUG_MODE=1 oder READER_TEST_MODE=1 ist erforderlich.');
  }

  return readerConfig;
}

function getDebugScanChannels(sessionName, channelRef = '') {
  const normalizedChannelMatchKey = normalizeChannelMatchKey(channelRef);
  const watchedChannels = listWatchedChannels(sessionName).filter((item) => item.isActive && cleanText(item.channelRef));

  if (!normalizedChannelMatchKey) {
    return watchedChannels;
  }

  const matchedChannels = watchedChannels.filter(
    (item) => normalizeChannelMatchKey(item.channelRef) === normalizedChannelMatchKey
  );

  if (!matchedChannels.length) {
    throw new Error('Keine aktive Watchlist fuer den angegebenen channelRef gefunden.');
  }

  return matchedChannels;
}

function escapeTelegramHtml(value = '') {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;');
}

function parseDebugNumber(value, fallback = null) {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }

  if (typeof value !== 'string') {
    return fallback;
  }

  const trimmed = value.trim();
  if (!trimmed) {
    return fallback;
  }

  const parsed = Number.parseFloat(trimmed.replace(/[^\d,.-]/g, '').replace(',', '.'));
  return Number.isFinite(parsed) ? parsed : fallback;
}

function formatDebugPrice(value) {
  const numeric = parseDebugNumber(value, null);
  if (numeric === null) {
    return 'n/a';
  }

  return `${new Intl.NumberFormat('de-DE', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  }).format(numeric)} EUR`;
}

function formatCompactPostPrice(value) {
  return cleanText(formatPrice(value));
}

function formatDebugPercent(value) {
  const numeric = parseDebugNumber(value, null);
  if (numeric === null) {
    return 'n/a';
  }

  return `${numeric.toFixed(1)}%`;
}

function formatDebugScore(value) {
  const numeric = parseDebugNumber(value, null);
  return numeric === null ? 'n/a' : `${Math.round(numeric)}`;
}

function formatDebugBoolean(value) {
  if (value === true) {
    return 'ja';
  }

  if (value === false) {
    return 'nein';
  }

  return 'n/a';
}

function buildReaderSavingsOptionLines(scrapedDeal = {}) {
  const lines = [];

  if (cleanText(scrapedDeal?.couponValue)) {
    lines.push(`💸 Coupon: ${cleanText(scrapedDeal.couponValue)}`);
  }

  if (cleanText(scrapedDeal?.subscribeDiscount)) {
    lines.push(`🔁 Spar-Abo: ${cleanText(scrapedDeal.subscribeDiscount)}`);
  }

  return lines;
}

function normalizeReaderDebugSellerType(value = '') {
  const normalized = cleanText(value) ? normalizeSellerType(value) : '';
  const label = normalized || 'unbekannt';

  console.info('[SELLER_TYPE_NORMALIZED]', {
    input: cleanText(value) || null,
    normalized: label
  });

  return label;
}

function resolveReaderDecisionSource(generatorContext = {}) {
  const learning = generatorContext?.learning || {};
  const internetAvailable = generatorContext?.internet?.available === true;
  const keepaAvailable = generatorContext?.keepa?.available === true;

  if (learning.internetPrimary === true || learning.primaryDecisionSource === 'internetvergleich' || internetAvailable) {
    return 'market';
  }

  if (learning.keepaFallbackUsed === true && keepaAvailable) {
    return 'keepa';
  }

  if (learning.fallbackUsed === true || learning.primaryDecisionSource === 'keepa_fallback') {
    return 'fallback';
  }

  return 'generator';
}

function resolveReaderLinkType(amazonLink = '', affiliateLinkBuilt = false) {
  if (affiliateLinkBuilt === true) {
    return 'eigener Partnerlink';
  }

  if (isAmazonShortLink(amazonLink)) {
    return 'blockierter Kurzlink';
  }

  return 'unbekannt';
}

function resolveReaderDecisionLabel(generatorContext = {}, normalDecision = {}) {
  const learningDecision = cleanText(generatorContext?.learning?.routingDecision).toLowerCase();
  const evaluationDecision = cleanText(generatorContext?.evaluation?.decision).toLowerCase();
  const decisionValue = cleanText(normalDecision?.decision).toLowerCase();

  if (decisionValue === 'review') {
    return 'REVIEW';
  }

  if (['block', 'hold'].includes(learningDecision) || ['hold'].includes(evaluationDecision)) {
    return 'REJECT';
  }

  if (decisionValue === 'test_group' || learningDecision === 'test_group') {
    return 'QUEUE';
  }

  if (decisionValue === 'review' || learningDecision === 'review' || evaluationDecision === 'manual_review') {
    return 'REVIEW';
  }

  return 'APPROVE';
}

function resolveReaderPriceSourceLabel({ scrapePrice = null, detectedPrice = null, keepaPrice = null, comparisonPrice = null } = {}) {
  if (scrapePrice !== null) {
    return 'Scrape';
  }

  if (detectedPrice !== null) {
    return 'Telegram';
  }

  if (keepaPrice !== null) {
    return 'Keepa';
  }

  if (comparisonPrice !== null) {
    return 'Markt';
  }

  return 'unbekannt';
}

function resolveNormalizedReaderAsin({ amazonLink = '', scrapedDeal = null, linkRecord = null } = {}) {
  return (
    cleanText(linkRecord?.asin).toUpperCase() ||
    cleanText(scrapedDeal?.asin).toUpperCase() ||
    extractAsin(
      cleanText(scrapedDeal?.normalizedUrl) ||
        cleanText(scrapedDeal?.resolvedUrl) ||
        cleanText(scrapedDeal?.finalUrl) ||
        cleanText(amazonLink)
    ) ||
    ''
  );
}

function resolveScrapedDealSellerIdentity(scrapedDeal = {}, fallbackText = '') {
  const scrapedSellerDetails = scrapedDeal?.sellerDetails && typeof scrapedDeal.sellerDetails === 'object' ? scrapedDeal.sellerDetails : {};

  return resolveSellerIdentity({
    sellerType: scrapedDeal?.sellerType,
    sellerClass: scrapedDeal?.sellerClass,
    soldByAmazon: scrapedDeal?.soldByAmazon,
    shippedByAmazon: scrapedDeal?.shippedByAmazon,
    sellerDetectionSource: scrapedSellerDetails.detectionSource,
    detectionSources: scrapedSellerDetails.detectionSources || [],
    matchedPatterns: scrapedSellerDetails.matchedPatterns || [],
    sellerDetails: scrapedSellerDetails,
    merchantText: scrapedSellerDetails.merchantText || fallbackText
  });
}

async function runUnknownSellerSecondPass({
  sessionName = '',
  source = null,
  structuredMessage = {},
  amazonLink = '',
  scrapedDeal = null
} = {}) {
  const firstPassIdentity = resolveScrapedDealSellerIdentity(scrapedDeal, structuredMessage?.text || '');

  if (firstPassIdentity.sellerClass !== 'UNKNOWN') {
    return scrapedDeal;
  }

  const secondPassUrl =
    cleanText(scrapedDeal?.normalizedUrl) ||
    cleanText(scrapedDeal?.resolvedUrl) ||
    cleanText(scrapedDeal?.finalUrl) ||
    cleanText(amazonLink);

  if (!secondPassUrl) {
    return scrapedDeal;
  }

  console.info('[SELLER_UNKNOWN_SECOND_PASS_START]', {
    sessionName,
    sourceId: source?.id || null,
    messageId: structuredMessage?.messageId || '',
    initialSellerClass: firstPassIdentity.sellerClass,
    secondPassUrl
  });

  try {
    const secondPassDeal = await scrapeAmazonProduct(secondPassUrl);
    const secondPassIdentity = resolveScrapedDealSellerIdentity(secondPassDeal, structuredMessage?.text || '');
    const mergedDeal = {
      ...(scrapedDeal && typeof scrapedDeal === 'object' ? scrapedDeal : {}),
      ...(secondPassDeal && typeof secondPassDeal === 'object' ? secondPassDeal : {}),
      sellerType: secondPassIdentity.sellerType || secondPassDeal?.sellerType || scrapedDeal?.sellerType || 'UNKNOWN',
      sellerClass: secondPassIdentity.sellerClass || secondPassDeal?.sellerClass || scrapedDeal?.sellerClass || 'UNKNOWN',
      soldByAmazon: secondPassIdentity.soldByAmazon,
      shippedByAmazon: secondPassIdentity.shippedByAmazon,
      sellerDetails: secondPassDeal?.sellerDetails || scrapedDeal?.sellerDetails || {},
      asin: cleanText(secondPassDeal?.asin).toUpperCase() || cleanText(scrapedDeal?.asin).toUpperCase(),
      normalizedUrl: cleanText(secondPassDeal?.normalizedUrl) || cleanText(scrapedDeal?.normalizedUrl),
      finalUrl: cleanText(secondPassDeal?.finalUrl) || cleanText(scrapedDeal?.finalUrl),
      resolvedUrl: cleanText(secondPassDeal?.resolvedUrl) || cleanText(scrapedDeal?.resolvedUrl)
    };

    console.info('[SELLER_UNKNOWN_SECOND_PASS_RESULT]', {
      sessionName,
      sourceId: source?.id || null,
      messageId: structuredMessage?.messageId || '',
      initialSellerClass: firstPassIdentity.sellerClass,
      sellerClass: secondPassIdentity.sellerClass,
      soldByAmazon: secondPassIdentity.soldByAmazon,
      shippedByAmazon: secondPassIdentity.shippedByAmazon,
      detectionSource: secondPassIdentity.details?.detectionSource || 'unknown',
      asin: mergedDeal.asin || ''
    });

    return mergedDeal;
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : 'Seller-Second-Pass fehlgeschlagen.';
    console.error('[SELLER_UNKNOWN_SECOND_PASS_RESULT]', {
      sessionName,
      sourceId: source?.id || null,
      messageId: structuredMessage?.messageId || '',
      initialSellerClass: firstPassIdentity.sellerClass,
      sellerClass: 'UNKNOWN',
      error: errorMessage
    });
    return scrapedDeal;
  }
}

function resolveAmazonDirectRequiredCheckBlock({ generatorInput = {}, generatorContext = {} } = {}) {
  const sellerClass = cleanText(generatorInput?.sellerClass || generatorContext?.seller?.sellerClass).toUpperCase();
  const learning = generatorContext?.learning || {};

  if (sellerClass !== 'AMAZON_DIRECT') {
    return {
      blocked: false,
      reason: '',
      missingChecks: []
    };
  }

  const missingChecks = [];

  if (learning.marketComparisonRequired === true && learning.marketComparisonStatus !== 'success') {
    missingChecks.push(`Marktvergleich: ${cleanText(learning.marketComparisonReason) || 'nicht erfolgreich ausgefuehrt.'}`);
  }

  if (learning.aiRequired === true && learning.aiCheckStatus !== 'success') {
    missingChecks.push(`KI: ${cleanText(learning.aiCheckReason) || 'nicht erfolgreich ausgefuehrt.'}`);
  }

  if (!missingChecks.length) {
    return {
      blocked: false,
      reason: '',
      missingChecks: []
    };
  }

  return {
    blocked: true,
    reason: `Pflichtpruefung fehlt: ${missingChecks.join(' | ')}`,
    missingChecks
  };
}

function resolveReaderPostingStatusLabel(forcedByDebug = false) {
  return forcedByDebug ? 'GEPOSTET / NICHT FÜR LIVE FREIGEGEBEN' : 'GEPOSTET';
}

function isReaderTestGroupAllMode(readerConfig = {}) {
  return readerConfig?.readerDebugMode === true || readerConfig?.readerTestMode === true;
}

function resolveReaderSellerProfileSnapshot({ generatorInput = {}, generatorContext = {}, scrapedDeal = {} } = {}) {
  const sellerProfile =
    generatorInput?.sellerProfile && typeof generatorInput.sellerProfile === 'object'
      ? generatorInput.sellerProfile
      : generatorInput?.sellerDetails?.sellerProfile && typeof generatorInput?.sellerDetails?.sellerProfile === 'object'
        ? generatorInput.sellerDetails.sellerProfile
        : generatorContext?.seller?.details?.sellerProfile && typeof generatorContext?.seller?.details?.sellerProfile === 'object'
          ? generatorContext.seller.details.sellerProfile
          : scrapedDeal?.sellerProfile && typeof scrapedDeal.sellerProfile === 'object'
            ? scrapedDeal.sellerProfile
            : scrapedDeal?.sellerDetails?.sellerProfile && typeof scrapedDeal?.sellerDetails?.sellerProfile === 'object'
              ? scrapedDeal.sellerDetails.sellerProfile
              : {};
  const positivePercent = Number.isFinite(Number(sellerProfile?.positivePercent)) ? Number(sellerProfile.positivePercent) : null;
  const periodMonths = Number.isFinite(Number(sellerProfile?.periodMonths)) ? Number(sellerProfile.periodMonths) : null;

  return {
    sellerName: cleanText(sellerProfile?.sellerName),
    positivePercent,
    periodMonths,
    periodLabel: cleanText(sellerProfile?.periodLabel) || (periodMonths !== null ? `${periodMonths} Monate` : ''),
    status: cleanText(sellerProfile?.status) || 'missing',
    required: sellerProfile?.required === true,
    checked: sellerProfile?.checked === true,
    profileOk: sellerProfile?.profileOk === true || sellerProfile?.fbmAllowed === true,
    fbmAllowed: sellerProfile?.fbmAllowed === true || sellerProfile?.profileOk === true,
    reason: cleanText(sellerProfile?.reason),
    profileUrl: cleanText(sellerProfile?.profileUrl)
  };
}

function resolveFbmSellerProfileReviewBlock({ generatorInput = {}, generatorContext = {}, scrapedDeal = {} } = {}) {
  const sellerClass = cleanText(generatorInput?.sellerClass || generatorContext?.seller?.sellerClass).toUpperCase();

  if (sellerClass !== 'FBM_THIRDPARTY') {
    return {
      blocked: false,
      reason: '',
      sellerProfile: resolveReaderSellerProfileSnapshot({ generatorInput, generatorContext, scrapedDeal })
    };
  }

  const sellerProfile = resolveReaderSellerProfileSnapshot({ generatorInput, generatorContext, scrapedDeal });
  if (sellerProfile.profileOk === true) {
    return {
      blocked: false,
      reason: '',
      sellerProfile
    };
  }

  return {
    blocked: true,
    reason:
      sellerProfile.reason ||
      'FBM-Haendlerprofil fehlt oder erfuellt weniger als 80% positive Bewertungen bzw. weniger als 12 Monate Historie.',
    sellerProfile
  };
}

function formatDebugList(values = []) {
  return Array.isArray(values) && values.length ? values.join(', ') : 'n/a';
}

function shortenDebugReason(value = '', fallback = '-') {
  const normalized = cleanText(String(value || ''));
  if (!normalized) {
    return fallback;
  }

  return normalized.length > 110 ? `${normalized.slice(0, 107)}...` : normalized;
}

function formatReaderCheckStatus(status = '', started = false) {
  const normalized = cleanText(status).toLowerCase();

  if (normalized === 'error') {
    return 'Fehler';
  }

  if (normalized === 'success') {
    return 'gestartet';
  }

  if (started === true) {
    return 'gestartet';
  }

  return 'nicht gestartet';
}

function formatReaderSellerChannel(value) {
  if (value === true) {
    return 'Amazon';
  }

  if (value === false) {
    return 'Drittanbieter';
  }

  return 'Unbekannt';
}

function formatStructuredCheckStatus(status = '', started = false) {
  const normalized = cleanText(status).toLowerCase();

  if (normalized === 'success') {
    return '✅ gestartet';
  }

  if (normalized === 'error') {
    return '❌ Fehler';
  }

  if (normalized === 'blocked') {
    return '⛔ blockiert';
  }

  if (started === true) {
    return '✅ gestartet';
  }

  return '➖ nicht gestartet';
}

function normalizeSellerProfileStatusLabel(status = '') {
  const normalized = cleanText(status).toUpperCase();

  if (!normalized) {
    return 'NICHT NÖTIG';
  }

  if (normalized === 'NICHT NOETIG') {
    return 'NICHT NÖTIG';
  }

  return normalized;
}

function resolveReaderComparisonUsage(debugValues = {}) {
  const used = [];
  const notUsed = [];

  if (debugValues.marketComparisonUsed === true) {
    used.push('Marktvergleich');
  } else {
    notUsed.push('Marktvergleich');
  }

  if (debugValues.keepaFallbackUsed === true || (debugValues.keepaUsed === true && debugValues.marketComparisonUsed !== true)) {
    used.push('Keepa');
  } else if (debugValues.keepaUsed !== true) {
    notUsed.push('Keepa');
  }

  if (debugValues.aiUsed === true || debugValues.aiCheckStarted === true) {
    used.push(debugValues.marketComparisonUsed === true ? 'KI' : 'KI (Fallback)');
  } else {
    notUsed.push('KI');
  }

  const comparisonSourceLabel =
    cleanText(debugValues.marketComparisonSourceName) ||
    (debugValues.marketComparisonUsed === true
      ? 'Internetvergleich'
      : debugValues.keepaFallbackUsed === true || debugValues.keepaUsed === true
        ? 'Keepa'
        : 'KEINE');

  return {
    usedLabel: used.length ? used.join(', ') : 'KEIN Vergleich',
    notUsedLabel: notUsed.length ? notUsed.join(', ') : '',
    comparisonSourceLabel
  };
}

function resolveReaderAiUsageMode(debugValues = {}) {
  if (debugValues.aiUsed === true || debugValues.aiCheckStarted === true) {
    if (debugValues.marketComparisonUsed === true) {
      return debugValues.aiOnlyOnUncertainty === true ? 'Standard (bei Unsicherheit)' : 'Standard';
    }

    return 'Fallback (kein Marktvergleich)';
  }

  if (debugValues.aiAllowed === false) {
    return 'Deaktiviert';
  }

  return 'Nicht gestartet';
}

function formatDebugVisibleEuroPrice(value, fallback = 'n/a') {
  const formatted = formatDebugPrice(value);
  return formatted === 'n/a' ? fallback : formatted.replace(' EUR', '\u20AC');
}

function resolveDebugSourceGroupLabel(debugValues = {}) {
  return (
    cleanText(debugValues.channelRef) ||
    cleanText(debugValues.channelTitle) ||
    cleanText(debugValues.group) ||
    'Unbekannt'
  );
}

function normalizeVisibleReaderPriceSource(debugValues = {}) {
  if (debugValues.couponDetected === true) {
    return 'Amazon + Coupon';
  }

  const source = cleanText(debugValues.rawPriceSource || debugValues.priceSource).toLowerCase();

  if (source.includes('paapi')) {
    return 'PAAPI';
  }

  if (source.includes('creator')) {
    return 'Creator API';
  }

  if (source.includes('scrape') || source.includes('scraped') || source.includes('amazonfinal') || source.includes('amazonbuybox')) {
    return 'Scrape';
  }

  if (source.includes('telegram')) {
    return 'Telegram';
  }

  if (source.includes('keepa')) {
    return 'Keepa';
  }

  return cleanText(debugValues.priceSource) || 'Fallback';
}

function formatDebugDiscountLabel(value = '', fallback = 'Coupon') {
  const text = cleanText(value);
  if (!text) {
    return fallback;
  }

  const numeric = parseDebugNumber(text, null);
  if (numeric === null) {
    return text;
  }

  if (/%/.test(text)) {
    return `${Math.round(numeric * 10) / 10}%`;
  }

  if (/\u20AC|eur/i.test(text)) {
    return formatDebugVisibleEuroPrice(numeric);
  }

  return text;
}

function resolveReaderPriceTransparency(debugValues = {}) {
  const couponDetected = debugValues.couponDetected === true;
  const subscribeDetected = debugValues.subscribeDetected === true;
  const basePrice =
    parseDebugNumber(debugValues.basePrice, null) ??
    parseDebugNumber(debugValues.priceFromAmazonScrape, null) ??
    parseDebugNumber(debugValues.detectedPrice, null);
  const finalPrice =
    parseDebugNumber(debugValues.finalPrice, null) ??
    parseDebugNumber(debugValues.detectedPrice, null) ??
    basePrice;
  const visiblePrice = formatDebugVisibleEuroPrice(finalPrice);
  const visibleSource = normalizeVisibleReaderPriceSource(debugValues);
  const sourceGroup = resolveDebugSourceGroupLabel(debugValues);
  let calculation = 'keine Coupon-Berechnung';

  if (couponDetected) {
    const basePriceText = formatDebugVisibleEuroPrice(basePrice);
    const couponText = formatDebugDiscountLabel(debugValues.couponValue, 'unbekannter');
    const finalPriceText = formatDebugVisibleEuroPrice(finalPrice);

    calculation = subscribeDetected
      ? `${basePriceText} minus ${couponText} Coupon minus ${formatDebugDiscountLabel(
          debugValues.subscribeDiscount,
          'Sparabo'
        )} Sparabo = ${finalPriceText}`
      : `${basePriceText} minus ${couponText} Coupon = ${finalPriceText}`;
  }

  return {
    price: visiblePrice,
    source: visibleSource,
    calculation,
    sourceGroup,
    couponCalculationVisible: couponDetected,
    basePrice: formatDebugVisibleEuroPrice(basePrice),
    finalPrice: visiblePrice
  };
}

function buildStructuredReaderDebugLines(debugValues = {}, options = {}) {
  const lines = [];
  const statusReason = shortenDebugReason(debugValues.reason || debugValues.invalidPriceReason || '-', '-');
  const marketStatusLabel = formatStructuredCheckStatus(
    debugValues.marketComparisonStatus,
    debugValues.marketComparisonStarted === true
  );
  const marketReason = shortenDebugReason(debugValues.marketComparisonReason || '-', '-');
  const aiStatusLabel = formatStructuredCheckStatus(debugValues.aiCheckStatus, debugValues.aiCheckStarted === true);
  const aiReason = shortenDebugReason(debugValues.aiReason || '-', '-');
  const comparisonUsage = resolveReaderComparisonUsage(debugValues);
  const aiUsageMode = resolveReaderAiUsageMode(debugValues);
  const sellerUnknown =
    debugValues.sellerClass === 'UNKNOWN' ||
    (debugValues.soldByAmazon === null && debugValues.shippedByAmazon === null && cleanText(debugValues.sellerRecognitionMessage));
  const priceTransparency = resolveReaderPriceTransparency(debugValues);

  console.info('[PRICE_SOURCE_VISIBLE]', {
    context: 'test_group_debug',
    asin: cleanText(debugValues.asin).toUpperCase() || '',
    sourceGroup: priceTransparency.sourceGroup,
    price: priceTransparency.price,
    priceSource: priceTransparency.source,
    calculation: priceTransparency.calculation
  });
  if (priceTransparency.couponCalculationVisible === true) {
    console.info('[PRICE_COUPON_CALCULATION_VISIBLE]', {
      context: 'test_group_debug',
      asin: cleanText(debugValues.asin).toUpperCase() || '',
      sourceGroup: priceTransparency.sourceGroup,
      basePrice: priceTransparency.basePrice,
      couponValue: cleanText(debugValues.couponValue),
      finalPrice: priceTransparency.finalPrice,
      calculation: priceTransparency.calculation
    });
    console.info('[PRICE_FINAL_CALCULATED]', {
      context: 'test_group_debug',
      asin: cleanText(debugValues.asin).toUpperCase() || '',
      basePrice: priceTransparency.basePrice,
      couponValue: cleanText(debugValues.couponValue),
      finalPrice: priceTransparency.finalPrice
    });
  }

  lines.push('\u{1F4B6} <b>PREIS CHECK</b>');
  lines.push(`\u{1F4B6} Preis: ${escapeTelegramHtml(priceTransparency.price)}`);
  lines.push(`\u{1F50E} Preisquelle: ${escapeTelegramHtml(priceTransparency.source)}`);
  lines.push(`\u{1F4CC} Berechnung: ${escapeTelegramHtml(priceTransparency.calculation)}`);
  lines.push(`\u{1F4E2} Quellgruppe: ${escapeTelegramHtml(priceTransparency.sourceGroup)}`);
  lines.push(`\u{1F50D} Aehnliche Produkte geprueft: ${escapeTelegramHtml(formatDebugBoolean(debugValues.similarProductsChecked === true))}`);
  lines.push(`\u{1F4E6} Optimierte Alternative gefunden: ${escapeTelegramHtml(formatDebugBoolean(debugValues.similarCheaperFound === true))}`);
  lines.push(`\u{1F4B6} Alternative Preis: ${escapeTelegramHtml(debugValues.similarCheaperPrice || 'n/a')}`);
  lines.push(
    `\u{1F4CA} Aehnlichkeit: ${escapeTelegramHtml(
      debugValues.similarCheaperScore === null || debugValues.similarCheaperScore === undefined
        ? 'n/a'
        : `${debugValues.similarCheaperScore}/100`
    )}`
  );
  lines.push(`\u{1F4E6} Versand Alternative: ${escapeTelegramHtml(debugValues.similarCheaperShipping || 'UNKNOWN')}`);
  lines.push(`\u{1F6AB} FBM ausgeschlossen: ${escapeTelegramHtml(formatDebugBoolean(debugValues.similarFbmExcluded === true))}`);
  lines.push('');

  if (options.diagnosticHeader === true) {
    lines.push('⚠️ <b>TESTPOST NICHT FREIGEGEBEN</b>');
    lines.push(`Grund: ${escapeTelegramHtml(statusReason)}`);
    lines.push('');
  } else if (debugValues.forcedByDebug === true) {
    lines.push('⚠️ <b>TESTPOST</b>');
    lines.push('');
  }

  lines.push('🧾 <b>DEAL STATUS</b>');
  lines.push(`📌 Entscheidung: ${escapeTelegramHtml(debugValues.decisionDisplay || 'REVIEW')}`);
  lines.push(`🚀 Live: ${escapeTelegramHtml(debugValues.wouldPostNormally === true ? 'JA' : 'NEIN')}`);
  lines.push(`🧪 Testgruppe: ${escapeTelegramHtml(debugValues.testGroupPosted === true ? 'JA' : 'NEIN')}`);
  lines.push(`⚠️ Grund: ${escapeTelegramHtml(statusReason)}`);
  if (debugValues.sourceMatchTier === 'review' || debugValues.sourceMatchTier === 'debug') {
    lines.push(
      `⚠️ Match: ${escapeTelegramHtml(
        debugValues.sourceMatchTier === 'review'
          ? 'Kein perfekter Match'
          : 'Sehr niedriger Match, nur Debug'
      )}`
    );
  }
  if (debugValues.shortlinkResolved === true) {
    lines.push('⚠️ Link: aus Shortlink extrahiert');
  } else if (debugValues.shortlinkFallback === true) {
    lines.push('⚠️ Link: Shortlink-Fallback aktiv');
  }
  if (cleanText(debugValues.imageSource) && debugValues.imageSource !== 'amazon') {
    lines.push('⚠️ Bild: Fallback genutzt');
  }
  if (cleanText(debugValues.productVerificationWarning)) {
    lines.push(`⚠️ Prüfung: ${escapeTelegramHtml(shortenDebugReason(debugValues.productVerificationWarning))}`);
  }
  if (cleanText(debugValues.amazonDirectExecutionWarning)) {
    lines.push(`⚠️ Hinweis: ${escapeTelegramHtml(shortenDebugReason(debugValues.amazonDirectExecutionWarning))}`);
  }
  lines.push('');

  lines.push('🏪 <b>SELLER CHECK</b>');
  lines.push(`🛒 Seller: ${escapeTelegramHtml(debugValues.sellerClass || 'UNKNOWN')}`);
  lines.push(`🔎 Seller Quelle: ${escapeTelegramHtml(debugValues.sellerDetectionSource || 'unknown')}`);
  lines.push(`📌 Seller Grund: ${escapeTelegramHtml(debugValues.sellerRecognitionMessage || 'Keine Zusatzinfo')}`);
  lines.push(`📦 Verkauf: ${escapeTelegramHtml(formatReaderSellerChannel(debugValues.soldByAmazon))}`);
  lines.push(`🚚 Versand: ${escapeTelegramHtml(formatReaderSellerChannel(debugValues.shippedByAmazon))}`);
  lines.push(`👤 Händlerprofil: ${escapeTelegramHtml(normalizeSellerProfileStatusLabel(debugValues.sellerProfileStatus))}`);
  if (debugValues.sellerPositivePercent !== null && debugValues.sellerPositivePercent !== undefined) {
    lines.push(`⭐ Bewertung: ${escapeTelegramHtml(`${debugValues.sellerPositivePercent}%`)}`);
  }
  if (cleanText(debugValues.sellerPeriodLabel)) {
    lines.push(`🗓️ Zeitraum: ${escapeTelegramHtml(debugValues.sellerPeriodLabel)}`);
  }
  if (sellerUnknown) {
    lines.push('❌ Seller unklar');
    lines.push('👉 Problem: Scraper / Seller Detection');
  }
  lines.push('');

  lines.push('📊 <b>VERGLEICH & KI</b>');
  lines.push(`🌍 Marktvergleich: ${escapeTelegramHtml(marketStatusLabel)}`);
  lines.push(`📉 Grund: ${escapeTelegramHtml(marketReason)}`);
  lines.push(`🤖 KI: ${escapeTelegramHtml(aiStatusLabel)}`);
  lines.push(`🧠 Modus: ${escapeTelegramHtml(aiUsageMode)}`);
  if (debugValues.aiCheckStatus !== 'success' && aiReason !== '-') {
    lines.push(`💬 Hinweis: ${escapeTelegramHtml(aiReason)}`);
  }
  lines.push(`📊 Vergleich genutzt: ${escapeTelegramHtml(comparisonUsage.usedLabel)}`);
  if (comparisonUsage.notUsedLabel) {
    lines.push(`🚫 Vergleich NICHT genutzt: ${escapeTelegramHtml(comparisonUsage.notUsedLabel)}`);
  }
  lines.push('📚 Vergleichsdaten:');
  lines.push(`→ Quelle: ${escapeTelegramHtml(comparisonUsage.comparisonSourceLabel)}`);
  lines.push(`→ Erwartet: ${escapeTelegramHtml(debugValues.comparisonExpectedSources || 'Internetvergleich / Idealo / Shops')}`);
  if (debugValues.marketComparisonStatus !== 'success') {
    lines.push('❌ Marktvergleich fehlt');
    lines.push('👉 Einstellung prüfen: Marktvergleich aktiv');
  }
  if (debugValues.aiCheckStatus !== 'success') {
    lines.push('❌ KI nicht gestartet');
    lines.push('👉 Einstellung prüfen: KI aktiv');
  }
  lines.push('');

  lines.push('⚙️ <b>SYSTEM REGELN</b>');
  lines.push(`🎯 Min Rabatt: ${escapeTelegramHtml(formatDebugPercent(debugValues.thresholds?.minDiscountPercent))}`);
  lines.push(`→ ändern unter: ${escapeTelegramHtml(debugValues.settingsAreas?.sampling || 'Sampling & Qualität')}`);
  lines.push(`📊 Min Score: ${escapeTelegramHtml(formatDebugScore(debugValues.thresholds?.minScore))}`);
  lines.push(`→ ändern unter: ${escapeTelegramHtml(debugValues.settingsAreas?.sampling || 'Sampling & Qualität')}`);
  lines.push(`🛑 Fake Limit: ${escapeTelegramHtml(formatDebugPercent(debugValues.thresholds?.fakeRejectThreshold))}`);
  lines.push(`→ ändern unter: ${escapeTelegramHtml(debugValues.settingsAreas?.sampling || 'Sampling & Qualität')}`);
  lines.push(
    `🔍 KI aktiviert: ${escapeTelegramHtml(
      debugValues.aiAllowed === true || debugValues.aiCheckStarted === true || debugValues.aiUsed === true ? 'JA' : 'NEIN'
    )}`
  );
  lines.push(`→ ändern unter: ${escapeTelegramHtml(debugValues.settingsAreas?.decision || 'Entscheidungslogik')}`);
  lines.push(`🌍 Marktvergleich Pflicht: ${escapeTelegramHtml(debugValues.marketComparisonRequired === true ? 'JA' : 'NEIN')}`);
  lines.push(`→ ändern unter: ${escapeTelegramHtml(debugValues.settingsAreas?.decision || 'Entscheidungslogik')}`);
  lines.push('💡 Anpassbar in:');
  lines.push(`→ ${escapeTelegramHtml(debugValues.settingsAreas?.sampling || 'Sampling & Qualität')}`);
  lines.push(`→ ${escapeTelegramHtml(debugValues.settingsAreas?.decision || 'Entscheidungslogik')}`);

  console.info('[DEBUG_BLOCK_STRUCTURED]', {
    decisionDisplay: debugValues.decisionDisplay || 'REVIEW',
    diagnostic: options.diagnosticHeader === true,
    sections: ['DEAL STATUS', 'SELLER CHECK', 'VERGLEICH & KI', 'SYSTEM REGELN'],
    lineCount: lines.length
  });

  return lines;
}

function buildReaderThresholds(readerConfig = {}, generatorContext = {}) {
  const sellerConfig = generatorContext?.evaluation?.config || {};
  const thresholds = {
    readerTestMode: readerConfig.readerTestMode === true,
    readerDebugMode: readerConfig.readerDebugMode === true,
    allowRawReaderFallback: readerConfig.allowRawReaderFallback === true,
    minDiscountPercent: Number(readerConfig?.readerTestThresholds?.minDiscountPercent || 0),
    minScore: Number(readerConfig?.readerTestThresholds?.minScore || 0),
    fakeRejectThreshold: Number(readerConfig?.readerTestThresholds?.clearFakeRejectRisk || 0),
    sellerTypeMinDiscount: parseDebugNumber(sellerConfig.minDiscount, null),
    sellerTypeMinScore: parseDebugNumber(sellerConfig.minScore, null),
    sellerTypeFakeThreshold: parseDebugNumber(sellerConfig.maxFakeDropRisk, null)
  };

  console.info('[THRESHOLDS_LOADED]', thresholds);
  return thresholds;
}

function collectReaderDebugValues({
  sessionName,
  source,
  structuredMessage,
  amazonLink,
  pricing,
  scrapedDeal,
  linkRecord,
  generatorInput,
  generatorContext,
  readerConfig,
  readerDecision,
  normalDecision
}) {
  const metrics = generatorContext?.evaluation?.metrics || {};
  const learning = generatorContext?.learning || {};
  const internet = generatorContext?.internet || {};
  const keepa = generatorContext?.keepa || {};
  const amazon = generatorContext?.amazon || {};
  const sellerProfile = generatorContext?.seller || {};
  const decisionPolicy = generatorContext?.decisionPolicy || {};
  const thresholds = buildReaderThresholds(readerConfig, generatorContext);
  const sellerType = normalizeReaderDebugSellerType(generatorInput?.sellerType || scrapedDeal?.sellerType || '');
  const sellerClass = cleanText(generatorInput?.sellerClass || sellerProfile?.sellerClass || scrapedDeal?.sellerClass) || 'UNKNOWN';
  const soldByAmazon = generatorInput?.soldByAmazon ?? sellerProfile?.soldByAmazon ?? scrapedDeal?.soldByAmazon ?? null;
  const shippedByAmazon = generatorInput?.shippedByAmazon ?? sellerProfile?.shippedByAmazon ?? scrapedDeal?.shippedByAmazon ?? null;
  const sellerDetectionSource =
    cleanText(
      generatorInput?.sellerDetectionSource ||
        sellerProfile?.details?.detectionSource ||
        scrapedDeal?.sellerDetails?.detectionSource
    ) || 'unknown';
  const sellerRecognitionMessage =
    cleanText(sellerProfile?.details?.recognitionMessage) ||
    (sellerClass === 'UNKNOWN' ? 'Seller konnte nicht erkannt werden.' : '');
  const fbmSellerProfile = resolveReaderSellerProfileSnapshot({
    generatorInput,
    generatorContext,
    scrapedDeal
  });
  const sellerProfileStatus =
    sellerClass === 'FBM_THIRDPARTY' ? (fbmSellerProfile.profileOk === true ? 'OK' : 'FEHLT') : 'NICHT NOETIG';
  const invalidPrice = generatorInput?.invalidPrice === true;
  const invalidPriceReason = cleanText(generatorInput?.invalidPriceReason);
  const detectedPrice = pricing?.currentPrice ?? parseDebugNumber(generatorInput?.currentPrice, null);
  const basePrice = parseDebugNumber(generatorInput?.basePrice || scrapedDeal?.basePrice, null);
  const scrapePrice = parseDebugNumber(scrapedDeal?.price, null);
  const keepaPrice = parseDebugNumber(keepa.currentPrice, null);
  const keepaReferencePrice = parseDebugNumber(keepa.referencePrice, null);
  const marketComparisonPrice = parseDebugNumber(internet.comparisonPrice, null);
  const comparisonPrice = marketComparisonPrice ?? parseDebugNumber(keepa.comparisonPrice, null) ?? keepaReferencePrice;
  const keepaDiscount = parseDebugNumber(metrics.keepaDiscount, null);
  const finalScore = parseDebugNumber(metrics.finalScore, null);
  const keepaDealScore = parseDebugNumber(metrics.keepaDealScore, null);
  const fakeRisk = parseDebugNumber(metrics.fakeDropRisk, null);
  const scoreAdjustment = parseDebugNumber(metrics.combinedScoreAdjustment, null);
  const sellerScoreAdjustment = parseDebugNumber(metrics.feedbackScoreAdjustment, null);
  const similarCaseScoreAdjustment = parseDebugNumber(metrics.similarCaseScoreAdjustment, null);
  const riskAdjustment = parseDebugNumber(metrics.combinedRiskAdjustment, null);
  const sellerFakeThreshold = thresholds.sellerTypeFakeThreshold;
  const riskPenalty =
    fakeRisk !== null && sellerFakeThreshold !== null ? Math.max(0, fakeRisk - sellerFakeThreshold) * 0.7 : null;
  const affiliateLinkBuilt = Boolean(cleanText(linkRecord?.affiliateUrl));
  const linkType = resolveReaderLinkType(amazonLink, affiliateLinkBuilt);
  const decision = resolveReaderDecisionLabel(generatorContext, normalDecision);
  const wouldPostNormally = invalidPrice ? false : normalDecision?.accepted === true;
  const decisionDisplay = wouldPostNormally ? 'APPROVE' : decision === 'REJECT' ? 'REJECT' : 'REVIEW';
  const forcedByDebug = isReaderTestGroupAllMode(readerConfig) && normalDecision?.accepted !== true;
  const liveStatus = resolveReaderPostingStatusLabel(forcedByDebug);
  const queueId = DEBUG_QUEUE_ID_PLACEHOLDER;
  const queueStatus = cleanText(generatorContext?.queue?.currentStatus) || 'not_enqueued';
  const lockStatus = generatorContext?.dealLock?.blocked === true ? 'blockiert' : 'frei';
  const reason = cleanText(normalDecision?.reason || readerDecision?.reason || learning?.reason) || 'n/a';
  const priceSource =
    cleanText(generatorInput?.priceSource) ||
    resolveReaderPriceSourceLabel({
      scrapePrice,
      detectedPrice,
      keepaPrice,
      comparisonPrice
    });
  const comparisonValues = [marketComparisonPrice, keepaReferencePrice, keepaPrice].filter((value) => value !== null);
  const comparisonMin = comparisonValues.length ? Math.min(...comparisonValues) : null;
  const comparisonMax = comparisonValues.length ? Math.max(...comparisonValues) : null;
  const comparisonSource = marketComparisonPrice !== null ? 'Markt' : comparisonValues.length ? 'Keepa' : 'unbekannt';
  const comparisonSourceName = cleanText(internet.comparisonSource) || '';
  const whyKeepaUsed =
    learning.keepaFallbackUsed === true
      ? cleanText(internet.reason || learning.reason || keepa.strengthReason || 'Keepa-Fallback aktiv.')
      : '';
  const whyMarketNotUsed = internet.available === true ? '' : cleanText(internet.reason || internet.status || 'Marktvergleich nicht verfuegbar.');
  const similarProductCheck =
    generatorInput?.similarProductCheck && typeof generatorInput.similarProductCheck === 'object'
      ? generatorInput.similarProductCheck
      : {};
  const settingsAreas = {
    sampling: 'Sampling & Qualität',
    decision: 'Entscheidungslogik'
  };
  const missingChecks = [];
  if (internet.available !== true) {
    missingChecks.push('Marktvergleich');
  }
  if (amazon.available !== true) {
    missingChecks.push('Amazon Daten');
  }
  if (keepa.available !== true) {
    missingChecks.push('Keepa');
  }
  const calculations = {
    messagePrice: detectedPrice,
    scrapePrice,
    keepaPrice,
    comparisonPrice,
    keepaDiscount,
    keepaDealScore,
    sellerScoreAdjustment,
    similarCaseScoreAdjustment,
    scoreAdjustment,
    riskAdjustment,
    riskPenalty,
    finalScore
  };
  const missingValues = Object.entries({
    asin: cleanText(generatorInput?.asin),
    detectedPrice,
    comparisonPrice,
    keepaDiscount,
    finalScore,
    fakeRisk
  })
    .filter(([, value]) => value === null || value === '')
    .map(([key]) => key);

  if (missingValues.length) {
    console.info('[DEBUG_VALUES_MISSING]', {
      sessionName,
      sourceId: source?.id || null,
      messageId: structuredMessage.messageId,
      missingValues
    });
  }

  console.info('[CALCULATION_SUMMARY]', {
    sessionName,
    sourceId: source?.id || null,
    messageId: structuredMessage.messageId,
    ...calculations
  });

  const debugValues = {
    source: 'Telegram Reader / Copybot',
    channelRef: cleanText(source?.channelRef) || cleanText(structuredMessage?.channelRef),
    channelTitle: cleanText(source?.channelTitle) || cleanText(structuredMessage?.channelTitle),
    group: cleanText(structuredMessage?.group),
    messageLink: cleanText(structuredMessage?.link),
    sellerType,
    sellerClass,
    soldByAmazon,
    shippedByAmazon,
    soldByAmazonLabel: formatSellerBoolean(soldByAmazon),
    shippedByAmazonLabel: formatSellerBoolean(shippedByAmazon),
    sellerDetectionSource,
    sellerRecognitionMessage,
    sellerProfileStatus,
    sellerPositivePercent: fbmSellerProfile.positivePercent,
    sellerPeriodMonths: fbmSellerProfile.periodMonths,
    sellerPeriodLabel: fbmSellerProfile.periodLabel,
    sellerProfileReason: fbmSellerProfile.reason,
    invalidPrice,
    invalidPriceReason,
    decisionSource: resolveReaderDecisionSource(generatorContext),
    asin: cleanText(generatorInput?.asin) || 'n/a',
    detectedPrice,
    basePrice,
    marketPrice: comparisonPrice,
    priceSource,
    rawPriceSource: cleanText(generatorInput?.rawPriceSource || scrapedDeal?.rawPriceSource),
    discountPercent: keepaDiscount,
    minDiscountPercent: thresholds.minDiscountPercent,
    score: finalScore,
    minScore: thresholds.minScore,
    fakeRisk,
    fakeRejectThreshold: thresholds.fakeRejectThreshold,
    keepaFallbackUsed: learning.keepaFallbackUsed === true,
    marketComparisonRequired: learning.marketComparisonRequired === true,
    marketComparisonStarted: learning.marketComparisonStarted === true,
    marketComparisonStatus: cleanText(learning.marketComparisonStatus) || 'skipped',
    marketComparisonUsed:
      learning.marketComparisonUsed === true ||
      generatorContext?.internet?.available === true ||
      learning.internetPrimary === true,
    marketComparisonAllowed: learning.marketComparisonAllowed === true,
    marketComparisonSourceName: comparisonSourceName,
    marketComparisonReason:
      cleanText(learning.marketComparisonReason) ||
      (learning.marketComparisonAllowed === true
        ? cleanText(generatorContext?.internet?.reason)
        : cleanText(learning.marketComparisonBlockedReason) || cleanText(decisionPolicy.marketComparison?.reason)),
    aiNeeded: learning.aiRequired === true,
    aiCheckStarted: learning.aiCheckStarted === true,
    aiCheckStatus: cleanText(learning.aiCheckStatus) || 'skipped',
    aiUsed: learning.aiResolutionUsed === true,
    aiAllowed: learning.aiAllowed === true,
    aiOnlyOnUncertainty: learning.aiOnlyOnUncertainty === true,
    aiReason: cleanText(learning.aiCheckReason) || cleanText(learning.aiBlockedReason) || cleanText(decisionPolicy.ai?.reason),
    amazonDirectExecutionWarning: cleanText(learning.amazonDirectExecutionWarning),
    sourceMatchScore: parseDebugNumber(generatorInput?.matchScore, null),
    sourceMatchTier: cleanText(generatorInput?.matchTier || '').toLowerCase(),
    sourceMatchWarning: cleanText(generatorInput?.matchWarningReason),
    sourceMatchReason: cleanText(generatorInput?.matchReason),
    sourceMatchDecision: cleanText(generatorInput?.matchDecision || generatorInput?.matchTier).toUpperCase(),
    sourceProtected: generatorInput?.protectedSource === true,
    sourceStatus: cleanText(generatorInput?.sourceStatus || ''),
    shortlinkResolved: generatorInput?.shortlinkResolved === true,
    shortlinkFallback: generatorInput?.shortlinkFallback === true,
    mainPostBlocked: generatorInput?.mainPostBlocked === true,
    mainPostBlockReason: cleanText(generatorInput?.mainPostBlockReason),
    mainPostTitleSource: cleanText(generatorInput?.mainPostTitleSource || generatorInput?.titleSource || ''),
    mainPostImageSource: cleanText(generatorInput?.mainPostImageSource || generatorInput?.imageSource || ''),
    affiliateLinkSource: cleanText(generatorInput?.affiliateLinkSource || ''),
    imageSource: cleanText(generatorInput?.imageSource || ''),
    productVerificationWarning: cleanText(generatorInput?.productVerificationWarning),
    decision,
    decisionDisplay,
    wouldPostNormally,
    reason,
    queueId,
    lockStatus,
    linkType,
    affiliateLinkBuilt,
    thresholds,
    calculations,
    priceFromMessage: detectedPrice,
    priceFromAmazonScrape: scrapePrice,
    couponDetected: generatorInput?.couponDetected === true,
    couponValue: cleanText(generatorInput?.couponValue),
    subscribeDetected: generatorInput?.subscribeDetected === true,
    subscribeDiscount: cleanText(generatorInput?.subscribeDiscount),
    finalPriceCalculated: generatorInput?.finalPriceCalculated === true,
    finalPrice: cleanText(generatorInput?.finalPrice),
    similarProductsChecked: similarProductCheck.checked === true,
    similarCheaperFound: similarProductCheck.similarCheaperFound === true,
    similarCheaperPrice: cleanText(similarProductCheck.similarCheaperPrice || similarProductCheck.alternativePrice),
    similarCheaperScore:
      similarProductCheck.similarCheaperScore === null || similarProductCheck.similarCheaperScore === undefined
        ? similarProductCheck.alternativeScore ?? null
        : similarProductCheck.similarCheaperScore,
    similarCheaperShipping: cleanText(similarProductCheck.similarCheaperShipping || similarProductCheck.alternativeShipping || 'UNKNOWN'),
    similarFbmExcluded: similarProductCheck.fbmExcluded === true,
    comparisonPrice,
    scoreBase: keepaDealScore,
    scoreAdjustment,
    riskAdjustment,
    riskPenalty,
    finalScore,
    sellerTypeThresholds: {
      minDiscount: thresholds.sellerTypeMinDiscount,
      minScore: thresholds.sellerTypeMinScore,
      fakeThreshold: thresholds.sellerTypeFakeThreshold
    },
    routingDecision: cleanText(learning.routingDecision) || 'n/a',
    queueStatus,
    keepaUsed: generatorContext?.keepa?.available === true,
    marketUsed: generatorContext?.internet?.available === true,
    forcedByDebug,
    testGroupPosted: isReaderTestGroupAllMode(readerConfig),
    liveStatus,
    keepaPrice,
    keepaReferencePrice,
    marketComparisonPrice,
    comparisonNeeded: internet.available !== true || learning.keepaFallbackUsed === true,
    comparisonMin,
    comparisonMax,
    comparisonSource,
    comparisonExpectedSources: 'Internetvergleich / Idealo / Shops',
    whyKeepaUsed,
    whyMarketNotUsed,
    missingChecks,
    referencePrice: comparisonPrice,
    settingsAreas,
    scoreComponents: {
      priceAdvantage: keepaDiscount,
      sellerBonusMalus: sellerScoreAdjustment,
      fakeRiskMalus: riskPenalty,
      keepaOrMarketSafety: cleanText(generatorContext?.evaluation?.keepaRating || '') || resolveReaderDecisionSource(generatorContext),
      finalScore
    }
  };

  const comparisonUsage = resolveReaderComparisonUsage(debugValues);
  const aiUsageMode = resolveReaderAiUsageMode(debugValues);
  const debugSourceGroupMeta = resolveTelegramDebugSourceMeta(debugValues);
  debugValues.comparisonUsageLabel = comparisonUsage.usedLabel;
  debugValues.comparisonNotUsedLabel = comparisonUsage.notUsedLabel;
  debugValues.comparisonSourceLabel = comparisonUsage.comparisonSourceLabel;
  debugValues.aiUsageMode = aiUsageMode;
  debugValues.sourceGroupRef = debugSourceGroupMeta.sourceGroupRef;
  debugValues.sourceGroupField = debugSourceGroupMeta.sourceField;
  console.info('[DEBUG_SOURCE_VALUE]', {
    sessionName,
    sourceId: source?.id || null,
    messageId: structuredMessage.messageId,
    channelRef: cleanText(debugValues.channelRef) || null,
    channelTitle: cleanText(debugValues.channelTitle) || null,
    group: cleanText(debugValues.group) || null,
    resolvedSourceValue: debugSourceGroupMeta.sourceGroupRef,
    resolvedSourceField: debugSourceGroupMeta.sourceField
  });

  console.info('[MARKET_COMPARE_STATUS]', {
    sessionName,
    sourceId: source?.id || null,
    messageId: structuredMessage.messageId,
    status: debugValues.marketComparisonStatus,
    started: debugValues.marketComparisonStarted === true,
    used: debugValues.marketComparisonUsed === true,
    reason: debugValues.marketComparisonReason || ''
  });
  console.info('[AI_USAGE_MODE]', {
    sessionName,
    sourceId: source?.id || null,
    messageId: structuredMessage.messageId,
    status: debugValues.aiCheckStatus,
    started: debugValues.aiCheckStarted === true,
    used: debugValues.aiUsed === true,
    mode: aiUsageMode,
    reason: debugValues.aiReason || ''
  });
  console.info('[SETTINGS_REFERENCE_ADDED]', {
    sessionName,
    sourceId: source?.id || null,
    messageId: structuredMessage.messageId,
    settingsAreas
  });

  console.info('[DEBUG_DEAL_VALUES]', debugValues);

  if (forcedByDebug) {
    console.info('[DEBUG_TEST_POST_FORCED]', {
      sessionName,
      sourceId: source?.id || null,
      messageId: structuredMessage.messageId,
      decision,
      reason
    });
  }

  return debugValues;
}

function buildReaderDebugBlock(debugValues = {}) {
  const lines = [];

  if (debugValues.forcedByDebug === true) {
    lines.push('⚠️ Testpost trotz REVIEW/REJECT');
    lines.push('Nicht fuer echten Kanal freigegeben.');
    lines.push('');
  }

  lines.push('🧪 Testdaten');
  lines.push(`Quelle: ${escapeTelegramHtml(debugValues.source || 'Telegram Reader / Copybot')}`);
  lines.push(`Seller: ${escapeTelegramHtml(debugValues.sellerType || 'unbekannt')}`);
  lines.push(`Decision Source: ${escapeTelegramHtml(debugValues.decisionSource || 'generator')}`);
  lines.push(`ASIN: ${escapeTelegramHtml(debugValues.asin || 'n/a')}`);
  lines.push(`Preis erkannt: ${escapeTelegramHtml(formatDebugPrice(debugValues.detectedPrice))}`);
  lines.push(`Vergleichspreis: ${escapeTelegramHtml(formatDebugPrice(debugValues.marketPrice))}`);
  lines.push(`Rabatt %: ${escapeTelegramHtml(formatDebugPercent(debugValues.discountPercent))}`);
  lines.push(`Mindest-Rabatt eingestellt: ${escapeTelegramHtml(formatDebugPercent(debugValues.sellerTypeThresholds?.minDiscount))}`);
  lines.push(`Score: ${escapeTelegramHtml(formatDebugScore(debugValues.score))}`);
  lines.push(`Mindest-Score eingestellt: ${escapeTelegramHtml(formatDebugScore(debugValues.sellerTypeThresholds?.minScore))}`);
  lines.push(`Fake-Risiko: ${escapeTelegramHtml(formatDebugPercent(debugValues.fakeRisk))}`);
  lines.push(`Fake-Schwelle: ${escapeTelegramHtml(formatDebugPercent(debugValues.sellerTypeThresholds?.fakeThreshold))}`);
  lines.push(`Keepa genutzt: ${escapeTelegramHtml(formatDebugBoolean(debugValues.keepaUsed))}`);
  lines.push(`Market Vergleich genutzt: ${escapeTelegramHtml(formatDebugBoolean(debugValues.marketUsed))}`);
  lines.push(`KI benoetigt: ${escapeTelegramHtml(formatDebugBoolean(debugValues.aiNeeded))}`);
  lines.push(`KI genutzt: ${escapeTelegramHtml(formatDebugBoolean(debugValues.aiUsed))}`);
  lines.push(`Entscheidung: ${escapeTelegramHtml(debugValues.decision || 'REVIEW')}`);
  lines.push(`Grund: ${escapeTelegramHtml(debugValues.reason || 'n/a')}`);
  lines.push(`Queue-ID: ${escapeTelegramHtml(debugValues.queueId || 'n/a')}`);
  lines.push(`Sperrstatus: ${escapeTelegramHtml(debugValues.lockStatus || 'n/a')}`);
  lines.push(`Link-Typ: ${escapeTelegramHtml(debugValues.linkType || 'unbekannt')}`);
  lines.push(`Partnerlink gebaut: ${escapeTelegramHtml(formatDebugBoolean(debugValues.affiliateLinkBuilt))}`);
  lines.push(`Preis aus Beitrag: ${escapeTelegramHtml(formatDebugPrice(debugValues.priceFromMessage))}`);
  lines.push(`Preis aus Amazon Scrape: ${escapeTelegramHtml(formatDebugPrice(debugValues.priceFromAmazonScrape))}`);
  lines.push(`Marktpreis / Vergleichspreis: ${escapeTelegramHtml(formatDebugPrice(debugValues.comparisonPrice))}`);
  lines.push(`Score-Basis: ${escapeTelegramHtml(formatDebugScore(debugValues.scoreBase))}`);
  lines.push(
    `Abzuege / Boni: ${escapeTelegramHtml(
      `Score ${formatDebugScore(debugValues.scoreAdjustment)} | Risiko ${formatDebugPercent(debugValues.riskAdjustment)} | Penalty ${
        debugValues.riskPenalty === null ? 'n/a' : debugValues.riskPenalty.toFixed(1)
      }`
    )}`
  );
  lines.push(`finalScore: ${escapeTelegramHtml(formatDebugScore(debugValues.finalScore))}`);
  lines.push(
    `Reader-Thresholds: ${escapeTelegramHtml(
      `TestMode ${formatDebugBoolean(debugValues.thresholds?.readerTestMode)} | DebugMode ${formatDebugBoolean(
        debugValues.thresholds?.readerDebugMode
      )} | RawFallback ${formatDebugBoolean(debugValues.thresholds?.allowRawReaderFallback)} | Rabatt ${
        debugValues.thresholds?.minDiscountPercent ?? 'n/a'
      } | Score ${debugValues.thresholds?.minScore ?? 'n/a'} | Fake ${debugValues.thresholds?.fakeRejectThreshold ?? 'n/a'}`
    )}`
  );

  const block = `\n\n${lines.join('\n')}`;
  console.info('[DEBUG_BLOCK_ADDED]', {
    lineCount: lines.length,
    decision: debugValues.decision || 'REVIEW',
    forcedByDebug: debugValues.forcedByDebug === true
  });
  return block;
}

function buildReaderShortDiagnosisBlock(debugValues = {}) {
  const lines = buildStructuredReaderDebugLines(debugValues, {
    diagnosticHeader: true
  });
  return `${lines.join('\n')}\n\n`;

  if (debugValues.forcedByDebug === true) {
    lines.push('⚠️ <b>TESTPOST</b>');
    lines.push('Freigabe: <b>NEIN</b>');
    lines.push(`Grund: ${escapeTelegramHtml(debugValues.reason || 'n/a')}`);
    lines.push('');
  }

  lines.push('🧪 <b>TEST-AUSWERTUNG</b>');
  lines.push(`Status: ${escapeTelegramHtml(debugValues.liveStatus || 'GEPOSTET')}`);
  lines.push(`Entscheidung: ${escapeTelegramHtml(debugValues.decision || 'REVIEW')}`);
  lines.push(`Hauptgrund: ${escapeTelegramHtml(debugValues.reason || 'n/a')}`);
  lines.push(`Seller: ${escapeTelegramHtml(debugValues.sellerType || 'unbekannt')}`);
  lines.push(`Preisquelle: ${escapeTelegramHtml(debugValues.priceSource || 'unbekannt')}`);
  lines.push(`Preis gefunden: ${escapeTelegramHtml(formatDebugPrice(debugValues.detectedPrice))}`);
  lines.push(`Vergleichspreis: ${escapeTelegramHtml(formatDebugPrice(debugValues.marketPrice))}`);
  lines.push(`Rabatt: ${escapeTelegramHtml(formatDebugPercent(debugValues.discountPercent))}`);
  lines.push(
    `Score: ${escapeTelegramHtml(formatDebugScore(debugValues.score))} / Mindestscore ${escapeTelegramHtml(
      formatDebugScore(debugValues.thresholds?.minScore)
    )}`
  );
  lines.push(
    `Fake-Risiko: ${escapeTelegramHtml(formatDebugPercent(debugValues.fakeRisk))} / Schwelle ${escapeTelegramHtml(
      formatDebugPercent(debugValues.thresholds?.fakeRejectThreshold)
    )}`
  );
  lines.push(`Link: ${escapeTelegramHtml(debugValues.linkType || 'unbekannt')}`);

  return `${lines.join('\n')}\n\n`;
}

function buildReaderExtendedDebugBlock(debugValues = {}) {
  const lines = [];

  lines.push('📊 Preisprüfung');
  lines.push(`Beitragspreis: ${escapeTelegramHtml(formatDebugPrice(debugValues.priceFromMessage))} | Quelle: Telegram Text`);
  lines.push(`Amazon Scrape Preis: ${escapeTelegramHtml(formatDebugPrice(debugValues.priceFromAmazonScrape))} | Quelle: Amazon Seite/PAAPI`);
  lines.push(`Keepa Preis: ${escapeTelegramHtml(formatDebugPrice(debugValues.keepaPrice))} | Quelle: Keepa`);
  lines.push(`Marktvergleich Preis: ${escapeTelegramHtml(formatDebugPrice(debugValues.marketComparisonPrice))} | Quelle: Internetvergleich`);
  lines.push(`Vergleich benötigt: ${escapeTelegramHtml(formatDebugBoolean(debugValues.comparisonNeeded))}`);
  lines.push(`Fehlende Prüfung: ${escapeTelegramHtml(formatDebugList(debugValues.missingChecks))}`);
  lines.push(`Warum Keepa genutzt: ${escapeTelegramHtml(debugValues.whyKeepaUsed || 'n/a')}`);
  lines.push(`Warum Marktvergleich nicht genutzt: ${escapeTelegramHtml(debugValues.whyMarketNotUsed || 'n/a')}`);
  lines.push('');
  lines.push('⚙️ Regeln');
  lines.push(`READER_TEST_MODE: ${escapeTelegramHtml(debugValues.thresholds?.readerTestMode === true ? '1' : '0')}`);
  lines.push(`READER_DEBUG_MODE: ${escapeTelegramHtml(debugValues.thresholds?.readerDebugMode === true ? '1' : '0')}`);
  lines.push(`minDiscountPercent: ${escapeTelegramHtml(formatDebugPercent(debugValues.thresholds?.minDiscountPercent))}`);
  lines.push(`minScore: ${escapeTelegramHtml(formatDebugScore(debugValues.thresholds?.minScore))}`);
  lines.push(`fakeRejectThreshold: ${escapeTelegramHtml(formatDebugPercent(debugValues.thresholds?.fakeRejectThreshold))}`);
  lines.push(`dealLockBypass: ${escapeTelegramHtml(formatDebugBoolean(debugValues.thresholds?.readerTestMode || debugValues.thresholds?.readerDebugMode))}`);
  lines.push(`allowRawReaderFallback: ${escapeTelegramHtml(formatDebugBoolean(debugValues.thresholds?.allowRawReaderFallback))}`);
  lines.push('');
  lines.push('🧮 Berechnung');
  lines.push(`erkannter Preis: ${escapeTelegramHtml(formatDebugPrice(debugValues.priceFromMessage))}`);
  lines.push(`Referenzpreis: ${escapeTelegramHtml(formatDebugPrice(debugValues.referencePrice))}`);
  lines.push(
    `Rabattberechnung: ${escapeTelegramHtml(
      `${formatDebugPrice(debugValues.referencePrice)} -> ${formatDebugPrice(debugValues.priceFromAmazonScrape || debugValues.priceFromMessage)} = ${formatDebugPercent(
        debugValues.discountPercent
      )}`
    )}`
  );
  lines.push(`Preisvorteil: ${escapeTelegramHtml(formatDebugPercent(debugValues.scoreComponents?.priceAdvantage))}`);
  lines.push(`Seller Bonus/Malus: ${escapeTelegramHtml(formatDebugScore(debugValues.scoreComponents?.sellerBonusMalus))}`);
  lines.push(`Fake-Risiko Malus: ${escapeTelegramHtml(formatDebugPercent(debugValues.scoreComponents?.fakeRiskMalus))}`);
  lines.push(`Keepa/Market Sicherheit: ${escapeTelegramHtml(String(debugValues.scoreComponents?.keepaOrMarketSafety || 'n/a'))}`);
  lines.push(`Final Score: ${escapeTelegramHtml(formatDebugScore(debugValues.scoreComponents?.finalScore))}`);

  return `\n\n${lines.join('\n')}`;
}

function buildReaderCompactDebugBlock(debugValues = {}) {
  const lines = [];

  if (debugValues.forcedByDebug === true) {
    lines.push('⚠️ <b>Testpost (nicht freigegeben)</b>');
  }

  lines.push('📊 <b>Kurzinfo</b>');
  lines.push(`🛒 Seller: ${escapeTelegramHtml(debugValues.sellerType || 'unbekannt')}`);
  lines.push(`📦 Quelle: ${escapeTelegramHtml(debugValues.priceSource || 'unbekannt')}`);
  lines.push(`🤖 KI: ${escapeTelegramHtml(formatDebugBoolean(debugValues.aiUsed === true || debugValues.aiNeeded === true))}`);
  lines.push(`💰 Preis: ${escapeTelegramHtml(formatDebugPrice(debugValues.detectedPrice))}`);
  lines.push(`📉 Rabatt: ${escapeTelegramHtml(formatDebugPercent(debugValues.discountPercent))}`);
  lines.push(
    `📊 Score: ${escapeTelegramHtml(formatDebugScore(debugValues.score))} / Mindest ${escapeTelegramHtml(
      formatDebugScore(debugValues.thresholds?.minScore)
    )}`
  );
  lines.push(`⚠️ Fake-Risiko: ${escapeTelegramHtml(formatDebugPercent(debugValues.fakeRisk))}`);

  if (debugValues.comparisonMin !== null || debugValues.comparisonMax !== null) {
    lines.push('');
    lines.push('📈 Vergleich:');
    lines.push(`Min: ${escapeTelegramHtml(formatDebugPrice(debugValues.comparisonMin))}`);
    lines.push(`Max: ${escapeTelegramHtml(formatDebugPrice(debugValues.comparisonMax))}`);
    lines.push(`Quelle: ${escapeTelegramHtml(debugValues.comparisonSource || 'unbekannt')}`);
  }

  const block = `\n\n${lines.join('\n')}`;
  console.info('[DEBUG_BLOCK_ADDED]', {
    lineCount: lines.length,
    decision: debugValues.decision || 'REVIEW',
    forcedByDebug: debugValues.forcedByDebug === true
  });
  return block;
}

function buildReaderCompactDebugBlockV2(debugValues = {}) {
  const lines = [];

  if (debugValues.forcedByDebug === true) {
    lines.push('⚠️ <b>Testpost (nicht freigegeben)</b>');
  }

  lines.push('📊 <b>Kurzinfo</b>');
  lines.push(`🛒 Seller: ${escapeTelegramHtml(debugValues.sellerType || 'unbekannt')}`);
  lines.push(`📦 Quelle: ${escapeTelegramHtml(debugValues.priceSource || 'unbekannt')}`);
  lines.push(`🤖 Entscheidung: ${escapeTelegramHtml(debugValues.decisionDisplay || 'REVIEW')}`);
  lines.push(`🧠 Würde gepostet werden: ${escapeTelegramHtml(debugValues.wouldPostNormally === true ? 'JA' : 'NEIN')}`);
  lines.push(`💰 Preis: ${escapeTelegramHtml(formatCompactPostPrice(debugValues.detectedPrice) || 'n/a')}`);
  lines.push(`📉 Rabatt: ${escapeTelegramHtml(formatDebugPercent(debugValues.discountPercent))}`);
  lines.push(
    `📊 Score: ${escapeTelegramHtml(formatDebugScore(debugValues.score))} / Mindest ${escapeTelegramHtml(
      formatDebugScore(debugValues.thresholds?.minScore)
    )}`
  );
  lines.push(`⚠️ Fake: ${escapeTelegramHtml(formatDebugPercent(debugValues.fakeRisk))}`);
  lines.push(`Coupon erkannt: ${escapeTelegramHtml(formatDebugBoolean(debugValues.couponDetected === true))}`);
  lines.push(`Spar-Abo erkannt: ${escapeTelegramHtml(formatDebugBoolean(debugValues.subscribeDetected === true))}`);
  lines.push(`Endpreis berechnet: ${escapeTelegramHtml(formatDebugBoolean(debugValues.finalPriceCalculated === true))}`);

  if (debugValues.comparisonMin !== null || debugValues.comparisonMax !== null) {
    lines.push('');
    lines.push('📈 Vergleich:');
    lines.push(`Min: ${escapeTelegramHtml(formatCompactPostPrice(debugValues.comparisonMin) || 'n/a')}`);
    lines.push(`Max: ${escapeTelegramHtml(formatCompactPostPrice(debugValues.comparisonMax) || 'n/a')}`);
  }

  const block = `\n\n${lines.join('\n')}`;
  console.info('[DEBUG_SHORT_BLOCK_READY]', {
    decisionDisplay: debugValues.decisionDisplay || 'REVIEW',
    wouldPostNormally: debugValues.wouldPostNormally === true,
    lineCount: lines.length
  });
  console.info('[DEBUG_BLOCK_ADDED]', {
    lineCount: lines.length,
    decision: debugValues.decision || 'REVIEW',
    forcedByDebug: debugValues.forcedByDebug === true
  });
  return block;
}

function buildReaderCompactDebugBlockV3(debugValues = {}) {
  const structuredLines = buildStructuredReaderDebugLines(debugValues);
  const structuredBlock = `\n\n${structuredLines.join('\n')}`;
  console.info('[DEBUG_SHORT_BLOCK_COMPACTED]', {
    decisionDisplay: debugValues.decisionDisplay || 'REVIEW',
    wouldPostNormally: debugValues.wouldPostNormally === true,
    lineCount: structuredLines.length
  });
  console.info('[DEBUG_SHORT_BLOCK_READY]', {
    decisionDisplay: debugValues.decisionDisplay || 'REVIEW',
    wouldPostNormally: debugValues.wouldPostNormally === true,
    lineCount: structuredLines.length
  });
  console.info('[DEBUG_BLOCK_ADDED]', {
    lineCount: structuredLines.length,
    decision: debugValues.decision || 'REVIEW',
    forcedByDebug: debugValues.forcedByDebug === true
  });
  return structuredBlock;

  const lines = [];

  if (debugValues.forcedByDebug === true) {
    lines.push('Testpost (nicht freigegeben)');
  }
  if (debugValues.invalidPrice === true) {
    lines.push('⚠️ Testpost, Preis ungueltig');
  }

  lines.push('📊 <b>Kurzinfo</b>');
  lines.push(`Seller: ${escapeTelegramHtml(debugValues.sellerClass || 'UNKNOWN')}`);
  lines.push(`Haendlerprofil: ${escapeTelegramHtml(debugValues.sellerProfileStatus || 'NICHT NOETIG')}`);
  lines.push(
    `Bewertung: ${escapeTelegramHtml(
      debugValues.sellerPositivePercent !== null && debugValues.sellerPositivePercent !== undefined
        ? `${debugValues.sellerPositivePercent}%`
        : '-'
    )}`
  );
  lines.push(`Zeitraum: ${escapeTelegramHtml(debugValues.sellerPeriodLabel || '-')}`);
  if (debugValues.amazonDirectExecutionWarning) {
    lines.push(`⚠️ ${escapeTelegramHtml(debugValues.amazonDirectExecutionWarning)}`);
  }
  lines.push(
    `Markt: ${escapeTelegramHtml(
      formatReaderCheckStatus(debugValues.marketComparisonStatus, debugValues.marketComparisonStarted === true)
    )}`
  );
  lines.push(`Markt-Grund: ${escapeTelegramHtml(shortenDebugReason(debugValues.marketComparisonReason || '-'))}`);
  lines.push(`KI: ${escapeTelegramHtml(formatReaderCheckStatus(debugValues.aiCheckStatus, debugValues.aiCheckStarted === true))}`);
  lines.push(`KI-Grund: ${escapeTelegramHtml(shortenDebugReason(debugValues.aiReason || '-'))}`);
  lines.push(`Entscheidung: ${escapeTelegramHtml(debugValues.decisionDisplay || 'REVIEW')}`);
  lines.push(`Live: ${escapeTelegramHtml(debugValues.wouldPostNormally === true ? 'JA' : 'NEIN')}`);
  lines.push(`Testgruppe: ${escapeTelegramHtml(debugValues.testGroupPosted === true ? 'JA' : 'NEIN')}`);
  if (debugValues.reason) {
    lines.push(`Grund: ${escapeTelegramHtml(shortenDebugReason(debugValues.reason))}`);
  }
  lines.push(
    `Preis: ${escapeTelegramHtml(
      debugValues.invalidPrice === true ? 'ungueltig' : formatCompactPostPrice(debugValues.detectedPrice) || 'n/a'
    )}`
  );
  lines.push(`Rabatt: ${escapeTelegramHtml(formatDebugPercent(debugValues.discountPercent))}`);
  lines.push(
    `Score: ${escapeTelegramHtml(formatDebugScore(debugValues.score))} / Mindest ${escapeTelegramHtml(
      formatDebugScore(debugValues.thresholds?.minScore)
    )}`
  );
  lines.push(`Fake: ${escapeTelegramHtml(formatDebugPercent(debugValues.fakeRisk))}`);

  const compactBlock = `\n\n${lines.join('\n')}`;
  console.info('[DEBUG_SHORT_BLOCK_COMPACTED]', {
    decisionDisplay: debugValues.decisionDisplay || 'REVIEW',
    wouldPostNormally: debugValues.wouldPostNormally === true,
    lineCount: lines.length
  });
  console.info('[DEBUG_SHORT_BLOCK_READY]', {
    decisionDisplay: debugValues.decisionDisplay || 'REVIEW',
    wouldPostNormally: debugValues.wouldPostNormally === true,
    lineCount: lines.length
  });
  console.info('[DEBUG_BLOCK_ADDED]', {
    lineCount: lines.length,
    decision: debugValues.decision || 'REVIEW',
    forcedByDebug: debugValues.forcedByDebug === true
  });
  return compactBlock;

  lines.push('📊 <b>Kurzinfo</b>');
  lines.push(`Seller-Klasse: ${escapeTelegramHtml(debugValues.sellerClass || 'UNKNOWN')}`);
  lines.push(`Verkauf durch Amazon: ${escapeTelegramHtml(debugValues.soldByAmazonLabel || 'unbekannt')}`);
  lines.push(`Versand durch Amazon: ${escapeTelegramHtml(debugValues.shippedByAmazonLabel || 'unbekannt')}`);
  lines.push(`Seller-Erkennungsquelle: ${escapeTelegramHtml(debugValues.sellerDetectionSource || 'unknown')}`);
  if (debugValues.sellerRecognitionMessage) {
    lines.push(`${escapeTelegramHtml(debugValues.sellerRecognitionMessage)}`);
  }
  if (debugValues.amazonDirectExecutionWarning) {
    lines.push(`⚠️ ${escapeTelegramHtml(debugValues.amazonDirectExecutionWarning)}`);
  }
  lines.push(`Marktvergleich: ${escapeTelegramHtml(debugValues.marketComparisonUsed === true ? 'genutzt' : 'nicht genutzt')}`);
  lines.push(
    `Marktvergleich Grund: ${escapeTelegramHtml(
      debugValues.marketComparisonUsed === true
        ? debugValues.marketComparisonReason || 'Erfolgreich ausgefuehrt.'
        : debugValues.marketComparisonReason || '-'
    )}`
  );
  lines.push(`KI: ${escapeTelegramHtml(debugValues.aiUsed === true ? 'genutzt' : 'nicht genutzt')}`);
  lines.push(`KI Grund: ${escapeTelegramHtml(debugValues.aiReason || '-')}`);
  lines.push(`Entscheidung: ${escapeTelegramHtml(debugValues.decisionDisplay || 'REVIEW')}`);
  lines.push(`Wuerde live gepostet: ${escapeTelegramHtml(debugValues.wouldPostNormally === true ? 'ja' : 'nein')}`);
  lines.push(`Quelle: ${escapeTelegramHtml(debugValues.priceSource || 'unbekannt')}`);
  lines.push(`Preis: ${escapeTelegramHtml(formatCompactPostPrice(debugValues.detectedPrice) || 'n/a')}`);
  lines.push(`Rabatt: ${escapeTelegramHtml(formatDebugPercent(debugValues.discountPercent))}`);
  lines.push(
    `Score: ${escapeTelegramHtml(formatDebugScore(debugValues.score))} / Mindest ${escapeTelegramHtml(
      formatDebugScore(debugValues.thresholds?.minScore)
    )}`
  );
  lines.push(`Fake-Risiko: ${escapeTelegramHtml(formatDebugPercent(debugValues.fakeRisk))}`);
  lines.push(`Coupon erkannt: ${escapeTelegramHtml(formatDebugBoolean(debugValues.couponDetected === true))}`);
  lines.push(`Spar-Abo erkannt: ${escapeTelegramHtml(formatDebugBoolean(debugValues.subscribeDetected === true))}`);
  lines.push(`Endpreis berechnet: ${escapeTelegramHtml(formatDebugBoolean(debugValues.finalPriceCalculated === true))}`);

  if (debugValues.comparisonMin !== null || debugValues.comparisonMax !== null) {
    lines.push('');
    lines.push('Vergleich:');
    lines.push(`Min: ${escapeTelegramHtml(formatCompactPostPrice(debugValues.comparisonMin) || 'n/a')}`);
    lines.push(`Max: ${escapeTelegramHtml(formatCompactPostPrice(debugValues.comparisonMax) || 'n/a')}`);
  }

  const block = `\n\n${lines.join('\n')}`;
  console.info('[DEBUG_SHORT_BLOCK_READY]', {
    decisionDisplay: debugValues.decisionDisplay || 'REVIEW',
    wouldPostNormally: debugValues.wouldPostNormally === true,
    lineCount: lines.length
  });
  console.info('[DEBUG_BLOCK_ADDED]', {
    lineCount: lines.length,
    decision: debugValues.decision || 'REVIEW',
    forcedByDebug: debugValues.forcedByDebug === true
  });
  return block;
}

function buildTelegramDealDebugInfo(debugValues = {}) {
  const lines = [
    '⚠️ <b>TESTPOST</b>',
    '',
    '🧾 <b>DEAL STATUS</b>',
    `📌 Entscheidung: ${escapeTelegramHtml(debugValues.decisionDisplay || 'REVIEW')}`,
    `🚀 Live: ${escapeTelegramHtml(debugValues.wouldPostNormally === true ? 'JA' : 'NEIN')}`,
    `🧪 Testgruppe: ${escapeTelegramHtml(debugValues.testGroupPosted === true ? 'JA' : 'NEIN')}`,
    '',
    '📊 <b>PRÜFUNGEN</b>',
    `🌍 Markt: ${escapeTelegramHtml(formatStructuredCheckStatus(debugValues.marketComparisonStatus, debugValues.marketComparisonStarted === true))}`,
    `🤖 KI: ${escapeTelegramHtml(formatStructuredCheckStatus(debugValues.aiCheckStatus, debugValues.aiCheckStarted === true))}`,
    `📈 Keepa: ${escapeTelegramHtml(debugValues.keepaUsed === true || debugValues.keepaFallbackUsed === true ? 'verfuegbar' : 'nicht genutzt')}`
  ];

  return lines.join('\n');
}

function buildTelegramDiagnosticReason({
  reason = '',
  blockedCode = '',
  sourceHost = '',
  sourceLabel = '',
  warningLines = []
} = {}) {
  const details = [];
  const safeReason = cleanText(reason);
  const safeBlockedCode = cleanText(blockedCode);
  const safeSource = cleanText(sourceLabel) || cleanText(sourceHost);

  if (safeReason) {
    details.push(safeReason);
  }
  if (safeBlockedCode) {
    details.push(`Code ${safeBlockedCode}`);
  }
  if (safeSource) {
    details.push(`Quelle ${safeSource}`);
  }

  for (const warningLine of Array.isArray(warningLines) ? warningLines : []) {
    const safeWarningLine = cleanText(warningLine);
    if (safeWarningLine) {
      details.push(`Hinweis ${safeWarningLine}`);
    }
  }

  return shortenDebugReason(details.join(' | ') || 'n/a', 'n/a');
}

function buildTelegramDiagnosticDebugValues({
  reason = '',
  blockedCode = '',
  sourceHost = '',
  sourceLabel = '',
  channelRef = '',
  channelTitle = '',
  group = '',
  mainPostBlocked = false,
  titleSource = '',
  imageSource = '',
  affiliateLinkSource = '',
  warningLines = [],
  liveAllowed = false,
  testGroupPosted = true,
  marketComparisonStatus = 'blocked',
  aiCheckStarted = false,
  aiUsed = false,
  keepaUsed = false,
  keepaFallbackUsed = false,
  sellerType = 'UNKNOWN',
  sellerClass = 'UNKNOWN',
  detectedPrice = null,
  discountPercent = null,
  score = null,
  minScore = null,
  fakeRisk = null,
  couponDetected = null,
  subscribeDetected = null
} = {}) {
  const debugValues = {
    decisionDisplay: 'REVIEW',
    wouldPostNormally: liveAllowed === true,
    testGroupPosted: testGroupPosted !== false,
    channelRef: cleanText(channelRef),
    channelTitle: cleanText(channelTitle),
    group: cleanText(group),
    mainPostBlocked: mainPostBlocked === true,
    mainPostBlockReason: cleanText(reason),
    mainPostTitleSource: cleanText(titleSource),
    mainPostImageSource: cleanText(imageSource),
    affiliateLinkSource: cleanText(affiliateLinkSource),
    reason: buildTelegramDiagnosticReason({
      reason,
      blockedCode,
      sourceHost,
      sourceLabel,
      warningLines
    }),
    marketComparisonStatus,
    marketComparisonUsed: false,
    marketComparisonStarted: false,
    aiCheckStarted: aiCheckStarted === true,
    aiUsed: aiUsed === true,
    keepaUsed: keepaUsed === true,
    keepaFallbackUsed: keepaFallbackUsed === true,
    sellerType: cleanText(sellerType).toUpperCase() || 'UNKNOWN',
    sellerClass: cleanText(sellerClass).toUpperCase() || 'UNKNOWN',
    detectedPrice,
    discountPercent,
    score,
    thresholds: {
      minScore
    },
    fakeRisk,
    couponDetected,
    subscribeDetected
  };

  const sourceGroupMeta = resolveTelegramDebugSourceMeta(debugValues);
  debugValues.sourceGroupRef = sourceGroupMeta.sourceGroupRef;
  debugValues.sourceGroupField = sourceGroupMeta.sourceField;

  return debugValues;
}

function looksLikeTelegramDebugSourceHost(value = '') {
  const normalizedValue = cleanText(value);
  if (!normalizedValue || normalizedValue.startsWith('@') || /\s/.test(normalizedValue)) {
    return false;
  }

  if (normalizeUrlHost(normalizedValue)) {
    return true;
  }

  return /^(?:www\.)?[a-z0-9-]+(?:\.[a-z0-9-]+)+(?:[/?#:].*)?$/i.test(normalizedValue);
}

function resolveTelegramDebugSourceMeta(debugValues = {}, options = {}) {
  const shouldLog = options.log !== false;
  const candidates = [
    { field: 'channelRef', value: cleanText(debugValues.channelRef) },
    { field: 'channelTitle', value: cleanText(debugValues.channelTitle) },
    { field: 'group', value: cleanText(debugValues.group) }
  ];

  for (const candidate of candidates) {
    if (!candidate.value) {
      continue;
    }

    if (looksLikeTelegramDebugSourceHost(candidate.value)) {
      if (shouldLog) {
        console.info('[DEBUG_SOURCE_HOST_REJECTED]', {
          field: candidate.field,
          rejectedValue: candidate.value,
          normalizedHost: normalizeUrlHost(candidate.value) || candidate.value.toLowerCase()
        });
      }
      continue;
    }

    const resolved = {
      sourceGroupRef: candidate.value,
      sourceField: candidate.field
    };

    if (shouldLog) {
      console.info('[DEBUG_SOURCE_GROUP_RESOLVED]', resolved);
    }

    return resolved;
  }

  const fallback = {
    sourceGroupRef: 'Unbekannt',
    sourceField: 'fallback'
  };

  if (shouldLog) {
    console.info('[DEBUG_SOURCE_GROUP_RESOLVED]', fallback);
  }

  return fallback;
}

function resolveTelegramDebugSourceValue(debugValues = {}) {
  return resolveTelegramDebugSourceMeta(debugValues, { log: false }).sourceGroupRef;
}

function translateTelegramDebugDecisionToGerman(decision = '') {
  const normalizedDecision = cleanText(decision).toUpperCase();

  if (normalizedDecision === 'APPROVE') {
    return 'FREIGEGEBEN';
  }
  if (normalizedDecision === 'REJECT') {
    return 'ABGELEHNT';
  }

  return 'PRÜFEN';
}

function resolveTelegramDebugMarketStatus(debugValues = {}) {
  const status = cleanText(debugValues.marketComparisonStatus).toLowerCase();

  if (debugValues.marketComparisonUsed === true || status === 'success') {
    return 'genutzt';
  }
  if (status === 'error') {
    return 'Fehler';
  }
  if (status === 'blocked') {
    return 'blockiert';
  }

  return status ? 'n/a' : 'n/a';
}

function resolveTelegramDebugAiStatus(debugValues = {}) {
  const status = cleanText(debugValues.aiCheckStatus).toLowerCase();

  if (debugValues.aiUsed === true || status === 'success') {
    return 'genutzt';
  }
  if (status === 'error') {
    return 'Fehler';
  }
  if (debugValues.aiCheckStarted === true || status === 'started' || status === 'skipped') {
    return 'nicht gestartet';
  }

  return 'n/a';
}

function resolveTelegramDebugKeepaStatus(debugValues = {}) {
  const status = cleanText(debugValues.keepaStatus).toLowerCase();

  if (debugValues.keepaFallbackUsed === true || status === 'used' || status === 'genutzt') {
    return 'genutzt';
  }
  if (debugValues.keepaUsed === true || status === 'available' || status === 'verfuegbar') {
    return 'verfuegbar';
  }
  if (status === 'skipped' || debugValues.keepaUsed === false) {
    return 'nicht gestartet';
  }

  return 'n/a';
}

function resolveTelegramDebugSellerLabel(debugValues = {}) {
  const sellerClass = cleanText(debugValues.sellerClass).toUpperCase();
  const sellerType = cleanText(debugValues.sellerType).toUpperCase();

  if (sellerClass === 'AMAZON_DIRECT') {
    return 'AMAZON_DIRECT';
  }
  if (sellerType === 'FBA' || sellerClass.includes('FBA')) {
    return 'FBA';
  }
  if (sellerType === 'FBM' || sellerClass.includes('FBM')) {
    return 'FBM';
  }

  return sellerType || sellerClass || 'UNKNOWN';
}

function buildTelegramDealDebugInfoExtended(debugValues = {}) {
  const marketStatus = resolveTelegramDebugMarketStatus(debugValues);
  const aiStatus = resolveTelegramDebugAiStatus(debugValues);
  const keepaStatus = resolveTelegramDebugKeepaStatus(debugValues);
  const sellerLabel = resolveTelegramDebugSellerLabel(debugValues);
  const sourceGroupValue = cleanText(debugValues.sourceGroupRef) || resolveTelegramDebugSourceValue(debugValues);
  const sourceGroupField = cleanText(debugValues.sourceGroupField) || 'fallback';
  const decisionGermanLabel = translateTelegramDebugDecisionToGerman(debugValues.decisionDisplay || debugValues.decision);
  const priceLabel =
    debugValues.invalidPrice === true ? 'n/a' : formatCompactPostPrice(debugValues.detectedPrice) || 'n/a';
  const reasonLabel = shortenDebugReason(debugValues.reason || debugValues.invalidPriceReason || 'n/a', 'n/a');
  const minScoreLabel = formatDebugScore(debugValues.thresholds?.minScore ?? debugValues.minScore);
  const matchScoreLabel =
    Number.isFinite(Number(debugValues.sourceMatchScore)) ? `${Math.round(Number(debugValues.sourceMatchScore))}/100` : 'n/a';
  const matchReasonLabel = cleanText(debugValues.sourceMatchReason) || 'n/a';
  const mainPostBlocked = debugValues.mainPostBlocked === true;
  const mainPostBlockReasonLabel = shortenDebugReason(
    debugValues.mainPostBlockReason || 'Titel/Bild stammt aus ungesicherter Quelle.',
    'Titel/Bild stammt aus ungesicherter Quelle.'
  );
  const mainPostImageSourceLabel = cleanText(debugValues.mainPostImageSource || debugValues.imageSource) || 'unknown';
  const mainPostTitleSourceLabel = cleanText(debugValues.mainPostTitleSource) || 'unknown';
  const lines = [
    '\u26A0\uFE0F <b>TESTPOST</b>',
    '',
    '\u{1F4E2} <b>QUELLE</b>',
    `\u{1F4E2} Gruppe: ${escapeTelegramHtml(sourceGroupValue)}`,
    `\u{1F310} Quelle geschuetzt: ${escapeTelegramHtml(debugValues.sourceProtected === true ? 'JA' : 'NEIN')}`,
    '',
    '\u{1F9FE} <b>DEAL STATUS</b>',
    `\u{1F4CC} Entscheidung: ${escapeTelegramHtml(decisionGermanLabel)}`,
    `\u{1F50E} Match-Score: ${escapeTelegramHtml(matchScoreLabel)}`,
    `\u{1F4CC} Match-Grund: ${escapeTelegramHtml(matchReasonLabel)}`,
    `\u{1F680} Live: ${escapeTelegramHtml(debugValues.wouldPostNormally === true ? 'JA' : 'NEIN')}`,
    '\u{1F9EA} Testgruppe: JA',
    `\u{1F4DD} Grund: ${escapeTelegramHtml(reasonLabel)}`,
    '',
    '\u{1F4CA} <b>PR\u00DCFUNGEN</b>',
    `\u{1F30D} Marktvergleich: ${escapeTelegramHtml(marketStatus)}`,
    `\u{1F916} KI-Pr\u00FCfung: ${escapeTelegramHtml(aiStatus)}`,
    `\u{1F4C8} Keepa: ${escapeTelegramHtml(keepaStatus)}`,
    `\u{1F6D2} Seller: ${escapeTelegramHtml(sellerLabel)}`,
    '',
    '\u2699\uFE0F <b>WERTE</b>',
    `\u{1F4B6} Preis: ${escapeTelegramHtml(priceLabel)}`,
    `\u{1F4C9} Rabatt: ${escapeTelegramHtml(formatDebugPercent(debugValues.discountPercent))}`,
    `\u2B50 Score: ${escapeTelegramHtml(formatDebugScore(debugValues.score))} / Mindest ${escapeTelegramHtml(minScoreLabel)}`,
    `\u26A0\uFE0F Fake-Risiko: ${escapeTelegramHtml(formatDebugPercent(debugValues.fakeRisk))}`,
    `\u{1F39F} Coupon: ${escapeTelegramHtml(formatDebugBoolean(debugValues.couponDetected))}`,
    `\u{1F501} Spar-Abo: ${escapeTelegramHtml(formatDebugBoolean(debugValues.subscribeDetected))}`,
    '',
    '\u{1F6E0} <b>WO EINSTELLBAR?</b>',
    '\u{1F4C9} Mindest-Rabatt \u2192 Sampling & Qualit\u00E4t',
    '\u2B50 Mindest-Score \u2192 Sampling & Qualit\u00E4t',
    '\u{1F30D} Marktvergleich Pflicht \u2192 Entscheidungslogik',
    '\u{1F916} KI-Pr\u00FCfung \u2192 Entscheidungslogik',
    '\u{1F6D2} Amazon / FBA / FBM Regeln \u2192 Deal Engine / Entscheidungslogik',
    '\u26A0\uFE0F Fake-Schwelle \u2192 Entscheidungslogik'
  ];

  if (mainPostBlocked) {
    lines.splice(
      13,
      0,
      `\u{1F6AB} Hauptpost blockiert: ${escapeTelegramHtml('JA')}`,
      `\u{1F4CC} Grund: ${escapeTelegramHtml(mainPostBlockReasonLabel)}`,
      `\u{1F5BC} Bildquelle: ${escapeTelegramHtml(mainPostImageSourceLabel)}`,
      `\u{1F4DD} Titelquelle: ${escapeTelegramHtml(mainPostTitleSourceLabel)}`
    );
  }

  console.info('[DEBUG_POST_NORMALIZED]', {
    decisionDisplay: debugValues.decisionDisplay || 'REVIEW',
    marketStatus,
    aiStatus,
    keepaStatus,
    seller: sellerLabel,
    missingPrice: priceLabel === 'n/a'
  });
  console.info('[DEBUG_SOURCE_ADDED]', {
    sourceGroupValue,
    channelRef: cleanText(debugValues.channelRef) || null,
    channelTitle: cleanText(debugValues.channelTitle) || null,
    group: cleanText(debugValues.group) || null
  });
  console.info('[DEBUG_SOURCE_GROUP_RENDERED]', {
    sourceGroupRef: sourceGroupValue,
    sourceField: sourceGroupField,
    channelRef: cleanText(debugValues.channelRef) || null,
    channelTitle: cleanText(debugValues.channelTitle) || null,
    group: cleanText(debugValues.group) || null
  });
  console.info('[DEBUG_MATCH_INFO_ADDED]', {
    sourceProtected: debugValues.sourceProtected === true,
    sourceStatus: cleanText(debugValues.sourceStatus) || 'NORMAL_SOURCE',
    matchScore: debugValues.sourceMatchScore ?? null,
    matchReason: matchReasonLabel
  });
  console.info('[DECISION_TRANSLATED_TO_GERMAN]', {
    originalDecision: cleanText(debugValues.decisionDisplay || debugValues.decision) || 'REVIEW',
    translatedDecision: decisionGermanLabel
  });
  console.info('[DEBUG_POST_ONLY_UPDATED]', {
    decisionDisplay: debugValues.decisionDisplay || 'REVIEW',
    marketStatus,
    aiStatus,
    keepaStatus,
    seller: sellerLabel,
    lineCount: lines.length
  });
  console.info('[DEBUG_INFO_EXTENDED_WITH_SETTINGS_HINTS]', {
    decisionDisplay: debugValues.decisionDisplay || 'REVIEW',
    seller: sellerLabel,
    lineCount: lines.length
  });

  return lines.join('\n');
}

function evaluateTelegramReaderGeneratorCandidate(generatorContext, readerConfig) {
  const learning = generatorContext?.learning || {};
  const sellerClass = cleanText(generatorContext?.seller?.sellerClass).toUpperCase();
  const keepaAvailable = generatorContext?.keepa?.available === true;
  const dealLockBlocked = generatorContext?.dealLock?.blocked === true || learning?.dealLockBlocked === true;

  if (dealLockBlocked) {
    return {
      accepted: false,
      decision: 'review',
      reason: cleanText(generatorContext?.dealLock?.blockReason) || learning?.reason || 'Deal-Lock aktiv.'
    };
  }

  if (
    sellerClass === 'FBA_OR_AMAZON_UNKNOWN' &&
    cleanText(learning?.routingDecision).toLowerCase() === 'approve' &&
    cleanText(learning?.primaryDecisionSource).toLowerCase() === 'test_seller_routing'
  ) {
    const reason = learning?.reason || 'Testmodus: Amazon Produktdaten verifiziert, Seller noch nicht eindeutig.';

    console.info('[TEST_APPROVE_FBA_OR_AMAZON_UNKNOWN]', {
      sellerClass,
      decision: 'APPROVE',
      routingDecision: 'approve',
      reason
    });
    console.info('[ROUTING_DECISION_FORCED_APPROVE]', {
      sellerClass,
      normalDecision: 'approve',
      wouldPostNormally: true,
      testGroupApproved: true
    });
    console.info('[APPROVED_CHANNEL_SEND_EXPECTED]', {
      sellerClass,
      routingDecision: 'approve',
      reason
    });

    return {
      accepted: true,
      decision: 'approve',
      decisionDisplay: 'APPROVE',
      routingDecision: 'approve',
      wouldPostNormally: true,
      testGroupApproved: true,
      reason
    };
  }

  if (readerConfig?.readerDebugMode === true) {
    return {
      accepted: true,
      decision: 'debug_mode',
      reason: 'READER_DEBUG_MODE postet jeden erkannten Deal in die Testgruppe.'
    };
  }

  if (readerConfig?.readerTestMode === true) {
    return {
      accepted: true,
      decision: 'test_group',
      reason:
        learning?.reason ||
        (keepaAvailable
          ? 'READER_TEST_MODE postet jeden erkannten Deal in die Testgruppe; Keepa war zusaetzlich verfuegbar.'
          : 'READER_TEST_MODE postet jeden erkannten Deal in die Testgruppe.')
    };
  }

  if (learning?.routingDecision === 'test_group') {
    return {
      accepted: true,
      decision: 'test_group',
      reason: learning?.reason || 'Generator-Pipeline hat den Deal fuer die Testgruppe freigegeben.'
    };
  }

  return {
    accepted: false,
    decision: 'review',
    reason: learning?.reason || 'Generator-Pipeline hat den Deal nicht fuer die Testgruppe freigegeben.'
  };
}

function buildReaderLinkRecord({ dealType = 'AMAZON', amazonLink = '', fallbackLink = '', asin = '', structuredMessage = {} } = {}) {
  if (cleanText(dealType).toUpperCase() === 'AMAZON') {
    return buildAmazonAffiliateLinkRecord(amazonLink, { asin });
  }

  const resolvedLink = cleanText(fallbackLink || structuredMessage?.externalLink || structuredMessage?.previewUrl || structuredMessage?.link);
  const syntheticAsin =
    cleanText(asin).toUpperCase() ||
    buildSyntheticReaderDealId({
      link: resolvedLink,
      title: structuredMessage?.previewTitle || extractTelegramTitle(structuredMessage?.text, structuredMessage?.group),
      text: structuredMessage?.text,
      group: structuredMessage?.group
    });

  return {
    valid: true,
    affiliateUrl: resolvedLink,
    normalizedUrl: resolvedLink,
    originalUrl: resolvedLink,
    asin: syntheticAsin
  };
}

function resolveReaderMainPostTitleSource({ dealType = 'AMAZON', titlePayload = {}, scrapedDeal = {} } = {}) {
  const normalizedDealType = cleanText(dealType).toUpperCase() || 'AMAZON';
  const normalizedTitle = cleanText(titlePayload?.title);
  const rawTitleSource = cleanText(titlePayload?.rawTitleSource).toLowerCase();
  const titleDataSource = cleanText(scrapedDeal?.titleDataSource).toLowerCase();
  const scrapedDataSource = cleanText(scrapedDeal?.dataSource).toLowerCase();
  const detectionSource = cleanText(scrapedDeal?.sellerDetails?.detectionSource).toLowerCase();

  if (!normalizedTitle || rawTitleSource === 'amazonmissingtitle' || titleDataSource === 'missing') {
    return 'unknown';
  }
  if (titleDataSource === 'telegram' || detectionSource === 'protected_source_no_bypass') {
    return 'telegram';
  }
  if (['preview', 'source', 'originaltext', 'fallback_source'].includes(titleDataSource)) {
    return titleDataSource;
  }
  if (titleDataSource === 'paapi' || rawTitleSource === 'paapi' || scrapedDataSource === 'paapi') {
    return 'paapi';
  }
  if (titleDataSource === 'keepa_verified' || rawTitleSource === 'keepa_verified') {
    return 'keepa_verified';
  }
  if (normalizedDealType === 'AMAZON' && (titleDataSource === 'amazon' || ['amazonproducttitle', 'amazonscrapedtitle'].includes(rawTitleSource))) {
    return 'amazon';
  }
  return titleDataSource || cleanText(titlePayload?.titleSource).toLowerCase() || 'unknown';
}

function resolveReaderMainPostImageSource({ dealType = 'AMAZON', imagePayload = {}, scrapedDeal = {} } = {}) {
  const normalizedDealType = cleanText(dealType).toUpperCase() || 'AMAZON';
  const rawImageSource = cleanText(imagePayload?.imageSource).toLowerCase();
  const imageDataSource = cleanText(scrapedDeal?.imageDataSource).toLowerCase();
  const hasFinalImage = Boolean(cleanText(imagePayload?.generatedImagePath || imagePayload?.uploadedImagePath));

  if (!hasFinalImage || imageDataSource === 'missing') {
    return 'unknown';
  }
  if (rawImageSource === 'manual_upload' || imageDataSource === 'manual_upload') {
    return 'manual_upload';
  }
  if (rawImageSource === 'paapi' || imageDataSource === 'paapi') {
    return 'paapi';
  }
  if (rawImageSource === 'keepa_verified' || imageDataSource === 'keepa_verified') {
    return 'keepa_verified';
  }
  if (['telegram', 'source', 'originalimage', 'preview', 'fallback_source', 'og', 'scraped'].includes(imageDataSource)) {
    return imageDataSource;
  }
  if (normalizedDealType === 'AMAZON' && (rawImageSource === 'scrape' || imageDataSource === 'amazon')) {
    return 'amazon';
  }
  if (rawImageSource === 'telegram') {
    return 'telegram';
  }
  if (rawImageSource === 'og') {
    return 'originalimage';
  }
  if (rawImageSource === 'fallback') {
    return 'fallback_source';
  }
  if (rawImageSource === 'scraped' && normalizedDealType !== 'AMAZON') {
    return 'scraped';
  }
  return imageDataSource || rawImageSource || 'unknown';
}

function resolveReaderAffiliateLinkSource({
  affiliateUrl = '',
  asin = '',
  inputSource = '',
  manualOriginalUrl = '',
  preserveInputLink = false,
  productVerified = false
} = {}) {
  const normalizedAffiliateUrl = cleanText(affiliateUrl);
  const normalizedAsin = cleanText(asin).toUpperCase();
  const normalizedInputSource = cleanText(inputSource).toLowerCase();

  if (isOwnAmazonAffiliateLink(normalizedAffiliateUrl, normalizedAsin)) {
    return 'own_affiliate';
  }

  if (
    normalizedInputSource === 'manual_generator' &&
    preserveInputLink === true &&
    productVerified === true &&
    cleanText(manualOriginalUrl) &&
    normalizedAffiliateUrl === cleanText(manualOriginalUrl)
  ) {
    return 'manual_original_link';
  }

  if (!normalizedAffiliateUrl) {
    return 'missing';
  }

  return 'source';
}

function validateMainPostSources(postData = {}) {
  const titleSource = cleanText(postData?.mainPostTitleSource || postData?.titleSource).toLowerCase() || 'unknown';
  const imageSource = cleanText(postData?.mainPostImageSource || postData?.imageSource).toLowerCase() || 'unknown';
  const affiliateLinkSource = cleanText(postData?.affiliateLinkSource || postData?.linkSource).toLowerCase() || 'unknown';
  const priceSource = cleanText(postData?.priceSource).toLowerCase() || 'unknown';
  const title = cleanText(postData?.title);
  const hasImage = Boolean(cleanText(postData?.generatedImagePath || postData?.uploadedImagePath));
  const issues = [];
  const blockedFields = [];
  let manualLinkAllowed = false;

  if (
    affiliateLinkSource === 'manual_original_link' &&
    cleanText(postData?.inputSource).toLowerCase() === 'manual_generator' &&
    postData?.productVerified === true
  ) {
    manualLinkAllowed = true;
  }

  console.info('[MAIN_POST_SOURCE_GUARD_START]', {
    asin: cleanText(postData?.asin).toUpperCase() || '',
    inputSource: cleanText(postData?.inputSource) || 'unknown',
    titleSource,
    imageSource,
    affiliateLinkSource,
    priceSource
  });

  if (!ALLOWED_MAIN_POST_TITLE_SOURCES.has(titleSource) || !title) {
    blockedFields.push('title');
    issues.push(
      !title
        ? 'Kein verifizierter Amazon-Titel.'
        : titleSource === 'telegram' || BLOCKED_MAIN_POST_SOURCE_VALUES.has(titleSource)
          ? 'Titel stammt aus ungesicherter Quelle.'
          : 'Kein verifizierter Amazon-Titel.'
    );
    console.warn('[MAIN_POST_TITLE_SOURCE_BLOCKED]', {
      asin: cleanText(postData?.asin).toUpperCase() || '',
      titleSource,
      rawTitleSource: cleanText(postData?.rawTitleSource) || null,
      title: title ? title.slice(0, 180) : null
    });
  }

  if (!ALLOWED_MAIN_POST_IMAGE_SOURCES.has(imageSource) || !hasImage) {
    blockedFields.push('image');
    issues.push(
      !hasImage
        ? 'Kein verifiziertes Amazon-Bild.'
        : imageSource === 'telegram' || BLOCKED_MAIN_POST_SOURCE_VALUES.has(imageSource)
          ? 'Bild stammt aus ungesicherter Quelle.'
          : 'Kein verifiziertes Amazon-Bild.'
    );
    console.warn('[MAIN_POST_IMAGE_SOURCE_BLOCKED]', {
      asin: cleanText(postData?.asin).toUpperCase() || '',
      imageSource,
      rawImageSource: cleanText(postData?.imageSource) || null,
      hasImage
    });
  }

  if (!(ALLOWED_MAIN_POST_LINK_SOURCES.has(affiliateLinkSource) || manualLinkAllowed === true)) {
    blockedFields.push('link');
    issues.push('Link stammt aus ungesicherter Quelle.');
  }

  if (!priceSource || ['unknown', 'missing', 'fallback', 'fallback_source'].includes(priceSource)) {
    blockedFields.push('price');
    issues.push('Preisquelle ist nicht nachvollziehbar.');
  }

  if (
    ['title', 'image', 'link'].some((field) => blockedFields.includes(field)) &&
    [titleSource, imageSource, affiliateLinkSource].some((value) => BLOCKED_MAIN_POST_SOURCE_VALUES.has(value) || value === 'telegram' || value === 'source')
  ) {
    console.warn('[MAIN_POST_FOREIGN_DATA_BLOCKED]', {
      asin: cleanText(postData?.asin).toUpperCase() || '',
      blockedFields,
      titleSource,
      imageSource,
      affiliateLinkSource
    });
  }

  if (issues.length) {
    const uniqueIssues = Array.from(new Set(issues));
    const reason = uniqueIssues.join(' ');
    console.error('[MAIN_POST_BLOCKED_UNVERIFIED_SOURCE]', {
      asin: cleanText(postData?.asin).toUpperCase() || '',
      blockedFields,
      titleSource,
      imageSource,
      affiliateLinkSource,
      priceSource,
      reason
    });
    return {
      valid: false,
      titleSource,
      imageSource,
      affiliateLinkSource,
      priceSource,
      blockedFields,
      issues: uniqueIssues,
      reason
    };
  }

  console.info('[MAIN_POST_SOURCE_GUARD_PASSED]', {
    asin: cleanText(postData?.asin).toUpperCase() || '',
    titleSource,
    imageSource,
    affiliateLinkSource,
    priceSource
  });
  return {
    valid: true,
    titleSource,
    imageSource,
    affiliateLinkSource,
    priceSource,
    blockedFields: [],
    issues: [],
    reason: ''
  };
}

function resolveReaderImagePayload({ scrapedDeal = {}, structuredMessage = {}, dealType = 'AMAZON', title = '', currentPrice = '' } = {}) {
  const normalizedDealType = cleanText(dealType).toUpperCase() || 'AMAZON';
  const blockedSourceImageContext = resolveBlockedMainPostSourceImageContext(structuredMessage);
  const blockedSourceImageCandidates = [
    {
      source: 'sourceImage',
      imageUrl: cleanText(structuredMessage?.telegramMediaDataUrl)
    },
    {
      source: 'telegramImage',
      imageUrl: cleanText(structuredMessage?.previewImage) || cleanText(scrapedDeal?.previewImage)
    },
    {
      source: 'originalMessageImage',
      imageUrl: cleanText(scrapedDeal?.ogImage)
    }
  ].filter(
    (candidate, index, items) =>
      candidate.imageUrl &&
      items.findIndex((entry) => entry.source === candidate.source && entry.imageUrl === candidate.imageUrl) === index
  );
  const logImageFallback = (source, imageUrl = '') => {
    console.info('[IMAGE_FALLBACK_USED]', {
      dealType: normalizedDealType,
      source,
      imageUrl: imageUrl || ''
    });
  };
  const logBlockedSourceImage = ({ selectedImageSource = 'none', selectedImageUrl = '', requiredAmazonImage = false } = {}) => {
    if (!blockedSourceImageContext) {
      return;
    }

    const blockedImageSources = blockedSourceImageCandidates.map((candidate) => candidate.source);
    console.warn('[SOURCE_BRANDED_IMAGE_DETECTED]', {
      dealType: normalizedDealType,
      sourceGroupRef: blockedSourceImageContext.sourceGroupRef || null,
      sourceGroupTitle: blockedSourceImageContext.sourceGroupTitle || 'Unbekannt',
      matchedPolicyKey: blockedSourceImageContext.matchedPolicyKey,
      detectionMode: 'source_policy',
      blockedImageSources
    });
    console.warn('[SOURCE_IMAGE_BLOCKED_FROM_MAIN_POST]', {
      dealType: normalizedDealType,
      sourceGroupRef: blockedSourceImageContext.sourceGroupRef || null,
      sourceGroupTitle: blockedSourceImageContext.sourceGroupTitle || 'Unbekannt',
      blockedImageSources,
      selectedImageSource,
      selectedImageUrl: selectedImageUrl || null
    });
    console.info('[MAIN_POST_IMAGE_SOURCE_SANITIZED]', {
      dealType: normalizedDealType,
      sourceGroupRef: blockedSourceImageContext.sourceGroupRef || null,
      sourceGroupTitle: blockedSourceImageContext.sourceGroupTitle || 'Unbekannt',
      blockedImageSources,
      selectedImageSource,
      selectedImageUrl: selectedImageUrl || null
    });
    if (requiredAmazonImage) {
      console.warn('[AMAZON_IMAGE_REQUIRED_FOR_SOURCE]', {
        dealType: normalizedDealType,
        sourceGroupRef: blockedSourceImageContext.sourceGroupRef || null,
        sourceGroupTitle: blockedSourceImageContext.sourceGroupTitle || 'Unbekannt',
        blockedImageSources,
        requiredImageSources: ['paapiImage', 'amazonProductImage', 'generatorImageFromAmazonProduct']
      });
    }
  };
  const amazonImage = normalizedDealType === 'AMAZON' ? resolveDealImageUrlFromScrape(scrapedDeal || {}) : '';
  if (amazonImage) {
    const resolvedAmazonImageSource =
      cleanText(scrapedDeal?.imageDebug?.selectedSource).toLowerCase() === 'paapi' || cleanText(scrapedDeal?.dataSource).toLowerCase() === 'paapi'
        ? 'paapi'
        : 'scrape';
    if (blockedSourceImageContext) {
      logBlockedSourceImage({
        selectedImageSource: resolvedAmazonImageSource === 'paapi' ? 'paapiImage' : 'amazonProductImage',
        selectedImageUrl: amazonImage,
        requiredAmazonImage: false
      });
    }
    console.info('[IMAGE_SOURCE_FOUND]', {
      source: resolvedAmazonImageSource === 'paapi' ? 'paapiImage' : 'scrapedAmazonImage',
      dealType: normalizedDealType,
      imageUrl: amazonImage
    });
    console.info('[IMAGE_SOURCE]', {
      source: resolvedAmazonImageSource,
      dealType: normalizedDealType,
      imageUrl: amazonImage
    });
    return {
      generatedImagePath: amazonImage,
      uploadedImagePath: '',
      imageSource: resolvedAmazonImageSource,
      telegramImageSource: 'standard',
      whatsappImageSource: 'standard'
    };
  }

  if (blockedSourceImageContext) {
    logBlockedSourceImage({
      selectedImageSource: 'none',
      selectedImageUrl: '',
      requiredAmazonImage: true
    });
    return {
      generatedImagePath: '',
      uploadedImagePath: '',
      imageSource: '',
      telegramImageSource: 'none',
      whatsappImageSource: 'none',
      imageBlockReason: 'Quellenbild blockiert. Amazon/PAAPI-Bild erforderlich.'
    };
  }

  if (normalizedDealType === 'AMAZON') {
    const sourceOnlyImage =
      cleanText(structuredMessage?.telegramMediaDataUrl) ||
      cleanText(structuredMessage?.previewImage) ||
      cleanText(scrapedDeal?.previewImage) ||
      cleanText(scrapedDeal?.ogImage);
    if (sourceOnlyImage) {
      console.info('[SOURCE_VALUES_STRIPPED]', {
        dealType: normalizedDealType,
        strippedField: 'image',
        blockedSource: 'telegram_or_source',
        blockedValue: sourceOnlyImage,
        keptSource: 'missing_amazon_image',
        keptValue: null
      });
      console.info('[SOURCE_IMAGE_MATCH_ONLY]', {
        sourceHost: normalizeUrlHost(
          structuredMessage?.externalLink || structuredMessage?.previewUrl || structuredMessage?.link || ''
        ) || 'unknown',
        imageUrl: sourceOnlyImage
      });
    }

    return {
      generatedImagePath: '',
      uploadedImagePath: '',
      imageSource: '',
      telegramImageSource: 'none',
      whatsappImageSource: 'none',
      imageBlockReason: ''
    };
  }

  if (cleanText(structuredMessage?.telegramMediaDataUrl)) {
    if (normalizedDealType === 'AMAZON') {
      logImageFallback('telegramMedia', '');
    }
    console.info('[IMAGE_SOURCE_FOUND]', {
      source: 'telegramMedia',
      dealType: normalizedDealType
    });
    console.info('[IMAGE_SOURCE]', {
      source: 'telegram',
      dealType: normalizedDealType
    });
    return {
      generatedImagePath: '',
      uploadedImagePath: cleanText(structuredMessage.telegramMediaDataUrl),
      imageSource: 'telegram',
      telegramImageSource: 'upload',
      whatsappImageSource: 'upload',
      imageBlockReason: ''
    };
  }

  const previewImage = cleanText(structuredMessage?.previewImage || scrapedDeal?.previewImage || '');
  if (isLikelyReaderImageUrl(previewImage)) {
    if (normalizedDealType === 'AMAZON') {
      logImageFallback('previewImage', previewImage);
    }
    console.info('[IMAGE_SOURCE_FOUND]', {
      source: 'telegramPreview',
      dealType: normalizedDealType,
      imageUrl: previewImage
    });
    console.info('[IMAGE_SOURCE]', {
      source: 'telegram',
      dealType: normalizedDealType,
      imageUrl: previewImage
    });
    return {
      generatedImagePath: previewImage,
      uploadedImagePath: '',
      imageSource: 'telegram',
      telegramImageSource: 'standard',
      whatsappImageSource: 'standard',
      imageBlockReason: ''
    };
  }

  const ogImage = cleanText(scrapedDeal?.ogImage || '');
  if (isLikelyReaderImageUrl(ogImage)) {
    if (normalizedDealType === 'AMAZON') {
      logImageFallback('ogImage', ogImage);
    }
    console.info('[IMAGE_SOURCE_FOUND]', {
      source: 'ogImage',
      dealType: normalizedDealType,
      imageUrl: ogImage
    });
    console.info('[IMAGE_SOURCE]', {
      source: 'og',
      dealType: normalizedDealType,
      imageUrl: ogImage
    });
    return {
      generatedImagePath: ogImage,
      uploadedImagePath: '',
      imageSource: 'og',
      telegramImageSource: 'standard',
      whatsappImageSource: 'standard',
      imageBlockReason: ''
    };
  }

  const scrapedImage = normalizedDealType === 'AMAZON' ? '' : resolveDealImageUrlFromScrape(scrapedDeal || {});
  if (scrapedImage) {
    console.info('[IMAGE_SOURCE_FOUND]', {
      source: 'scrapedImage',
      dealType: normalizedDealType,
      imageUrl: scrapedImage
    });
    console.info('[IMAGE_SOURCE]', {
      source: 'scraped',
      dealType: normalizedDealType,
      imageUrl: scrapedImage
    });
    return {
      generatedImagePath: scrapedImage,
      uploadedImagePath: '',
      imageSource: 'scraped',
      telegramImageSource: 'standard',
      whatsappImageSource: 'standard',
      imageBlockReason: ''
    };
  }

  const fallbackDataUrl = buildFallbackDealImageDataUrl({
    title,
    price: formatCompactPostPrice(currentPrice) || cleanText(currentPrice) || 'Preis folgt',
    dealType: normalizedDealType
  });
  logImageFallback('placeholder', '');
  console.info('[IMAGE_SOURCE_FOUND]', {
    source: 'fallback',
    dealType: normalizedDealType
  });
  console.info('[IMAGE_SOURCE]', {
    source: 'fallback',
    dealType: normalizedDealType
  });
  return {
    generatedImagePath: '',
    uploadedImagePath: fallbackDataUrl,
    imageSource: 'fallback',
    telegramImageSource: 'upload',
    whatsappImageSource: 'upload',
    imageBlockReason: ''
  };
}

async function loadReaderKeepaPriceFallback({
  asin = '',
  scrapedDeal = {},
  affiliateUrl = '',
  normalizedUrl = '',
  title = ''
} = {}) {
  const normalizedAsin = cleanText(asin).toUpperCase();
  if (!normalizedAsin) {
    return '';
  }

  try {
    const keepaContext = await loadKeepaClientByAsin({
      asin: normalizedAsin,
      currentPrice: null,
      title: cleanText(title || scrapedDeal?.productTitle || scrapedDeal?.title),
      productUrl: cleanText(normalizedUrl || affiliateUrl || scrapedDeal?.normalizedUrl || scrapedDeal?.finalUrl),
      imageUrl: cleanText(scrapedDeal?.imageUrl || scrapedDeal?.previewImage || scrapedDeal?.ogImage),
      source: 'telegram_reader_price_fallback',
      maxAgeMinutes: 720
    });
    const keepaPrice = normalizeReaderPriceCandidate(keepaContext?.result?.currentPrice);

    console.info('[PRICE_FALLBACK_KEEPA]', {
      asin: normalizedAsin,
      status: cleanText(keepaContext?.status) || 'unknown',
      available: keepaContext?.available === true,
      price: keepaPrice || null
    });

    return keepaPrice || '';
  } catch (error) {
    console.warn('[PRICE_FALLBACK_KEEPA_FAILED]', {
      asin: normalizedAsin,
      error: error instanceof Error ? error.message : 'Keepa-Preisfallback fehlgeschlagen.'
    });
    return '';
  }
}

async function buildTelegramReaderGeneratorInput({
  structuredMessage,
  scrapedDeal,
  normalizedAsin,
  affiliateUrl,
  normalizedUrl,
  couponCode,
  pricing,
  dealType = 'AMAZON'
}) {
  const inferredSellerSignals = inferTelegramSellerSignals(structuredMessage?.text || '');
  const scrapedSellerDetails = scrapedDeal?.sellerDetails && typeof scrapedDeal.sellerDetails === 'object' ? scrapedDeal.sellerDetails : {};
  const sellerProfile =
    scrapedSellerDetails.sellerProfile && typeof scrapedSellerDetails.sellerProfile === 'object'
      ? scrapedSellerDetails.sellerProfile
      : scrapedDeal?.sellerProfile && typeof scrapedDeal.sellerProfile === 'object'
        ? scrapedDeal.sellerProfile
        : null;
  const sellerIdentity = resolveSellerIdentity({
    sellerType: scrapedDeal?.sellerType,
    sellerClass: scrapedDeal?.sellerClass,
    soldByAmazon: scrapedDeal?.soldByAmazon ?? inferredSellerSignals.soldByAmazon,
    shippedByAmazon: scrapedDeal?.shippedByAmazon ?? inferredSellerSignals.shippedByAmazon,
    sellerDetectionSource: scrapedSellerDetails.detectionSource || inferredSellerSignals.detectionSource,
    detectionSources: scrapedSellerDetails.detectionSources || [],
    matchedPatterns: scrapedSellerDetails.matchedPatterns || inferredSellerSignals.matchedPatterns || [],
    matchedDirectAmazonPatterns: scrapedSellerDetails.matchedDirectAmazonPatterns || inferredSellerSignals.matchedDirectAmazonPatterns || [],
    hasCombinedAmazonMatch:
      scrapedSellerDetails.hasCombinedAmazonMatch === true || inferredSellerSignals.hasCombinedAmazonMatch === true,
    sellerDetails: scrapedSellerDetails,
    merchantText: scrapedSellerDetails.merchantText || '',
    dealType,
    isAmazonDeal: cleanText(dealType).toUpperCase() === 'AMAZON'
  });
  const titlePayload = resolveReaderTitlePayload({
    dealType,
    scrapedDeal,
    structuredMessage
  });
  const normalizedDealType = cleanText(dealType).toUpperCase() || 'AMAZON';
  const paapiPriceCandidate = resolveReaderPaapiPriceCandidate(scrapedDeal);
  const telegramPriceCandidate =
    pricing?.currentPrice !== null && pricing?.currentPrice !== undefined
      ? normalizeReaderPriceCandidate(String(pricing.currentPrice))
      : '';
  const keepaPriceCandidate =
    normalizedDealType === 'AMAZON' && !paapiPriceCandidate && !telegramPriceCandidate
      ? await loadReaderKeepaPriceFallback({
          asin: normalizedAsin || scrapedDeal?.asin,
          scrapedDeal,
          affiliateUrl,
          normalizedUrl,
          title: titlePayload.title
        })
      : '';
  const scrapedDealWithFallbacks =
    keepaPriceCandidate && !resolveReaderKeepaPriceCandidate(scrapedDeal, pricing)
      ? {
          ...(scrapedDeal && typeof scrapedDeal === 'object' ? scrapedDeal : {}),
          keepaPrice: keepaPriceCandidate
        }
      : scrapedDeal;
  const pricePayload = resolveReaderPricePayload({
    dealType,
    scrapedDeal: scrapedDealWithFallbacks,
    pricing: {
      ...(pricing && typeof pricing === 'object' ? pricing : {}),
      keepaPrice: keepaPriceCandidate || pricing?.keepaPrice || ''
    }
  });
  const productDescription = extractReaderProductDescription({
    scrapedDeal: scrapedDealWithFallbacks,
    structuredMessage
  });
  const rawCurrentPrice = cleanText(pricePayload.currentPrice);
  const rawOldPrice = cleanText(pricePayload.oldPrice);
  const invalidPriceState = resolveInvalidPriceState(rawCurrentPrice);
  const template = buildTelegramReaderTemplatePayload({
    title: titlePayload.title,
    description: '',
    affiliateUrl,
    currentPrice: invalidPriceState.invalid ? '' : rawCurrentPrice,
    oldPrice: '',
    couponCode: '',
    extraOptions: []
  });
  const imagePayload = resolveReaderImagePayload({
    scrapedDeal: scrapedDealWithFallbacks,
    structuredMessage,
    dealType,
    title: titlePayload.title || template.productTitle || 'Deal',
    currentPrice: invalidPriceState.invalid ? '' : rawCurrentPrice
  });
  const mainPostTitleSource = resolveReaderMainPostTitleSource({
    dealType,
    titlePayload,
    scrapedDeal: scrapedDealWithFallbacks
  });
  const mainPostImageSource = resolveReaderMainPostImageSource({
    dealType,
    imagePayload: {
      ...imagePayload,
      generatedImagePath: imagePayload.generatedImagePath,
      uploadedImagePath: imagePayload.uploadedImagePath
    },
    scrapedDeal: scrapedDealWithFallbacks
  });
  const affiliateLinkSource = resolveReaderAffiliateLinkSource({
    affiliateUrl,
    asin: normalizedAsin || scrapedDeal?.asin,
    inputSource: 'telegram_reader'
  });

  if (imagePayload.generatedImagePath) {
    console.info('[GENERATOR_SCREENSHOT_REUSED]', {
      asin: cleanText(normalizedAsin || scrapedDeal?.asin).toUpperCase() || '',
      imageSource: imagePayload.imageSource
    });
    console.info('[GENERATOR_IMAGE_PATH_USED]', {
      asin: cleanText(normalizedAsin || scrapedDeal?.asin).toUpperCase() || '',
      imageUrl: imagePayload.generatedImagePath
    });
  } else if (!imagePayload.uploadedImagePath) {
    console.error('[GENERATOR_IMAGE_MISSING_ERROR]', {
      asin: cleanText(normalizedAsin || scrapedDeal?.asin).toUpperCase() || '',
      imageSource: imagePayload.imageSource || 'none',
      reason: 'Kein Generator-Bild aus dem Amazon-Scrape aufloesbar.'
    });
  }

  if (cleanText(dealType).toUpperCase() === 'AMAZON') {
    console.info('[FINAL_VALUES_FROM_AMAZON_PRODUCT]', {
      asin: cleanText(normalizedAsin || scrapedDeal?.asin).toUpperCase() || '',
      titleSource: titlePayload.rawTitleSource || 'unknown',
      title: titlePayload.title || null,
      priceSource: pricePayload.rawPriceSource || 'unknown',
      price: invalidPriceState.invalid ? null : rawCurrentPrice || null,
      imageSource: imagePayload.imageSource || 'missing',
      affiliateUrl: cleanText(affiliateUrl) || null
    });
  }

  return {
    title: titlePayload.title || template.productTitle || 'Deal',
    titleSource: titlePayload.titleSource || 'fallback',
    rawTitleSource: titlePayload.rawTitleSource || 'unknown',
    mainPostTitleSource,
    link: cleanText(affiliateUrl),
    affiliateLinkSource,
    normalizedUrl: cleanText(normalizedUrl),
    asin: cleanText(normalizedAsin || scrapedDeal?.asin).toUpperCase(),
    sellerType: sellerIdentity.sellerType || 'UNKNOWN',
    sellerClass: sellerIdentity.sellerClass || 'UNKNOWN',
    soldByAmazon: sellerIdentity.soldByAmazon,
    shippedByAmazon: sellerIdentity.shippedByAmazon,
    sellerDetectionSource: sellerIdentity.details?.detectionSource || 'unknown',
    sellerDetectionSources: sellerIdentity.details?.detectionSources || [],
    sellerMatchedPatterns: sellerIdentity.details?.matchedPatterns || [],
    sellerRawText: [sellerIdentity.details?.merchantText, structuredMessage?.text].map((value) => cleanText(value)).filter(Boolean).join(' | '),
    sellerDetails: {
      detectionSource: sellerIdentity.details?.detectionSource || 'unknown',
      detectionSources: sellerIdentity.details?.detectionSources || [],
      matchedPatterns: sellerIdentity.details?.matchedPatterns || [],
      matchedDirectAmazonPatterns: sellerIdentity.details?.matchedDirectAmazonPatterns || [],
      hasCombinedAmazonMatch: sellerIdentity.details?.hasCombinedAmazonMatch === true,
      merchantText: sellerIdentity.details?.merchantText || '',
      amazonDataset: {
        asin: cleanText(normalizedAsin || scrapedDeal?.asin).toUpperCase(),
        title: titlePayload.title || template.productTitle || '',
        price: invalidPriceState.invalid ? '' : rawCurrentPrice,
        imageUrl: imagePayload.generatedImagePath || imagePayload.uploadedImagePath || '',
        affiliateUrl: cleanText(affiliateUrl),
        normalizedUrl: cleanText(normalizedUrl),
        productUrl: cleanText(normalizedUrl),
        source: 'telegram_reader_verified_product'
      },
      sellerProfile,
      dealType: cleanText(dealType).toUpperCase() || 'AMAZON',
      isAmazonDeal: cleanText(dealType).toUpperCase() === 'AMAZON'
    },
    sellerProfile,
    sellerProfileOk: sellerProfile?.profileOk === true,
    sellerPositivePercent: Number.isFinite(Number(sellerProfile?.positivePercent)) ? Number(sellerProfile.positivePercent) : null,
    sellerAgeMonths: Number.isFinite(Number(sellerProfile?.periodMonths)) ? Number(sellerProfile.periodMonths) : null,
    productDescription,
    couponDetected: scrapedDeal?.couponDetected === true,
    couponValue: cleanText(scrapedDeal?.couponValue),
    subscribeDetected: scrapedDeal?.subscribeDetected === true,
    subscribeDiscount: cleanText(scrapedDeal?.subscribeDiscount),
    finalPriceCalculated: scrapedDeal?.finalPriceCalculated === true,
    finalPrice: cleanText(scrapedDeal?.finalPrice),
    basePrice: cleanText(scrapedDealWithFallbacks?.basePrice || scrapedDeal?.basePrice),
    rawCurrentPrice,
    invalidPrice: invalidPriceState.invalid,
    invalidPriceReason: invalidPriceState.reason,
    currentPrice: invalidPriceState.invalid ? '' : rawCurrentPrice,
    oldPrice: rawOldPrice,
    priceSource: pricePayload.priceSource || 'unknown',
    rawPriceSource: pricePayload.rawPriceSource || 'unknown',
    couponCode: cleanText(couponCode),
    dealType: cleanText(dealType).toUpperCase() || 'AMAZON',
    isAmazonDeal: cleanText(dealType).toUpperCase() === 'AMAZON',
    textByChannel: {
      telegram: template.telegramCaption,
      whatsapp: template.whatsappText,
      facebook: template.whatsappText
    },
    debugInfoByChannel: {
      telegram: '',
      whatsapp: '',
      facebook: ''
    },
    generatedImagePath: imagePayload.generatedImagePath,
    uploadedImagePath: imagePayload.uploadedImagePath,
    imageSource: imagePayload.imageSource || 'unknown',
    mainPostImageSource,
    imageBlockReason: cleanText(imagePayload.imageBlockReason),
    uploadedImageFile: null,
    telegramImageSource: imagePayload.telegramImageSource || 'none',
    whatsappImageSource: imagePayload.whatsappImageSource || 'none',
    facebookImageSource: 'link_preview',
    enableTelegram: true,
    enableWhatsapp: false,
    enableFacebook: false,
    queueSourceType: 'generator_direct',
    inputSource: 'telegram_reader',
    originOverride: 'automatic',
    contextSource: 'telegram_reader_polling',
    testMode: false
  };
}

async function publishEmergencyReaderTestDeal(sessionName, options = {}) {
  const asin = 'B08ZJH1BGQ';
  const price = cleanText(options.price) || '9.99';
  const linkRecord = buildAmazonAffiliateLinkRecord(asin, { asin });

  console.info('[EMERGENCY_TEST_DEAL_TRIGGERED]', {
    sessionName,
    asin,
    price
  });

  console.info('[GENERATOR_FORCE_START]', {
    sessionName,
    sourceId: null,
    messageId: 'emergency-test-deal',
    amazonLink: linkRecord.affiliateUrl,
    trigger: 'emergency_test_deal'
  });

  const generatedPost = buildTelegramReaderTemplatePayload({
    title: `Emergency Reader Test Deal ${asin}`,
    affiliateUrl: linkRecord.affiliateUrl,
    currentPrice: price,
    oldPrice: '',
    couponCode: ''
  });

  const publishInput = {
    title: generatedPost.productTitle || `Emergency Reader Test Deal ${asin}`,
    link: linkRecord.affiliateUrl,
    normalizedUrl: linkRecord.normalizedUrl,
    asin,
    sellerType: normalizeSellerType('AMAZON'),
    sellerClass: 'AMAZON_DIRECT',
    soldByAmazon: true,
    shippedByAmazon: true,
    currentPrice: price,
    oldPrice: '',
    couponCode: '',
    textByChannel: {
      telegram: generatedPost.telegramCaption,
      whatsapp: generatedPost.whatsappText,
      facebook: generatedPost.whatsappText
    },
    generatedImagePath: '',
    uploadedImagePath: '',
    uploadedImageFile: null,
    telegramImageSource: 'none',
    whatsappImageSource: 'none',
    facebookImageSource: 'link_preview',
    enableTelegram: true,
    enableWhatsapp: false,
    enableFacebook: false,
    queueSourceType: 'generator_direct',
    originOverride: 'automatic',
    contextSource: 'telegram_reader_emergency_test',
    skipDealLock: true
  };

  console.info('[GENERATOR_FORCE_SUCCESS]', {
    sessionName,
    sourceId: null,
    messageId: 'emergency-test-deal',
    asin,
    decision: 'EMERGENCY_TEST',
    trigger: 'emergency_test_deal'
  });
  console.info('[PUBLISHER_FORCE_START]', {
    sessionName,
    sourceId: null,
    messageId: 'emergency-test-deal',
    asin,
    decision: 'EMERGENCY_TEST',
    trigger: 'emergency_test_deal'
  });

  const result = await publishGeneratorPostDirect(publishInput);
  return {
    asin,
    queueId: result?.queue?.id || null,
    telegramMessageId: result?.results?.telegram?.messageId || null
  };
}

async function enqueueTelegramReaderOutput({
  source,
  structuredMessage,
  amazonLink,
  importedDealId = null,
  pipelineStatus = '',
  pipelineReason = ''
}) {
  const testGroupConfig = getTelegramTestGroupConfig();
  const queueEntry = createPublishingEntry({
    sourceType: 'telegram_reader',
    sourceId: importedDealId ?? source?.id ?? null,
    originOverride: 'automatic',
    payload: {
      sourceId: importedDealId ?? source?.id ?? null,
      link: amazonLink,
      normalizedUrl: amazonLink,
      asin: '',
      sellerType: 'AMAZON',
      title: extractTelegramTitle(structuredMessage.text, structuredMessage.group),
      currentPrice: '',
      oldPrice: '',
      couponCode: extractTelegramCouponCode(structuredMessage.text),
      telegramChatIds: testGroupConfig.chatId ? [String(testGroupConfig.chatId)] : [],
      textByChannel: {
        telegram: structuredMessage.text,
        whatsapp: structuredMessage.text,
        facebook: structuredMessage.text
      },
      imageVariants: {
        standard: '',
        upload: ''
      },
      targetImageSources: {
        telegram: 'none',
        whatsapp: 'none',
        facebook: 'link_preview'
      },
      skipDealLock: true,
      meta: {
        sessionName: structuredMessage.sessionName,
        group: structuredMessage.group,
        messageId: structuredMessage.messageId,
        chatId: structuredMessage.chatId,
        pipelineStatus,
        pipelineReason
      }
    },
    targets: [{ channelType: 'telegram', isEnabled: true, imageSource: 'none' }]
  });

  console.info('[QUEUE_JOB_CREATED]', {
    queueId: queueEntry?.id || null,
    sourceType: 'telegram_reader',
    sourceId: importedDealId ?? source?.id ?? null,
    trigger: 'telegram_reader_output_fallback',
    messageId: structuredMessage.messageId
  });

  return queueEntry;
}

async function enqueueTelegramReaderDiagnosticPost({
  source,
  structuredMessage,
  diagnosticText = '',
  blockedCode = '',
  blockedReason = ''
}) {
  const testGroupConfig = getTelegramTestGroupConfig();
  const targetChatId = cleanText(testGroupConfig.chatId);
  const targetSource = cleanText(process.env.TELEGRAM_TEST_CHAT_ID)
    ? 'TELEGRAM_TEST_CHAT_ID'
    : cleanText(process.env.TELEGRAM_CHAT_ID)
      ? 'TELEGRAM_CHAT_ID'
      : 'missing';
  console.info('[TESTGROUP_TARGET_RESOLVED]', {
    context: 'reader_diagnostic',
    targetChatId: targetChatId || null,
    targetSource,
    tokenConfigured: Boolean(cleanText(testGroupConfig.token))
  });
  const queueEntry = createPublishingEntry({
    sourceType: 'telegram_reader',
    sourceId: source?.id ?? null,
    originOverride: 'automatic',
    payload: {
      sourceId: source?.id ?? null,
      link: '',
      normalizedUrl: '',
      asin: '',
      sellerType: 'UNKNOWN',
      title: '',
      currentPrice: '',
      oldPrice: '',
      couponCode: '',
      telegramChatIds: testGroupConfig.chatId ? [String(testGroupConfig.chatId)] : [],
      textByChannel: {
        telegram: cleanText(diagnosticText),
        whatsapp: cleanText(diagnosticText),
        facebook: cleanText(diagnosticText)
      },
      imageVariants: {
        standard: '',
        upload: ''
      },
      targetImageSources: {
        telegram: 'none',
        whatsapp: 'none',
        facebook: 'link_preview'
      },
      skipDealLock: true,
      meta: {
        sessionName: structuredMessage?.sessionName || '',
        group: structuredMessage?.group || '',
        messageId: structuredMessage?.messageId || '',
        chatId: structuredMessage?.chatId || '',
        blockedCode,
        blockedReason
      }
    },
    targets: [{ channelType: 'telegram', isEnabled: true, imageSource: 'none' }]
  });

  try {
    const publishResult = await processPublishingQueueEntry(queueEntry.id);
    const telegramResult =
      Array.isArray(publishResult?.results) ? publishResult.results.find((item) => item?.channelType === 'telegram') : null;
    const queueMessageId = telegramResult?.messageId || null;

    if (queueMessageId) {
      return {
        queueId: queueEntry?.id || null,
        queueStatus: publishResult?.queue?.status || '',
        messageId: queueMessageId
      };
    }

    throw new Error('Publisher hat keine Telegram messageId fuer den Diagnose-Testpost geliefert.');
  } catch (error) {
    const directResult = await sendTelegramPost({
      text: cleanText(diagnosticText),
      disableWebPagePreview: true,
      chatId: targetChatId
    });

    return {
      queueId: queueEntry?.id || null,
      queueStatus: 'direct_fallback',
      messageId: directResult?.messageId || null
    };
  }
}

async function handleBlockedReaderDiagnostic({
  sessionName = '',
  source = {},
  structuredMessage = {},
  readerConfig = {},
  trigger = 'reader',
  blockedCode = '',
  blockedReason = '',
  sourceHost = '',
  forceDiagnosticPost = false,
  mainPostBlocked = false,
  titleSource = '',
  imageSource = '',
  affiliateLinkSource = ''
} = {}) {
  const reason = cleanText(blockedReason) || 'Produkt nicht verifiziert.';
  if (!isReaderTestGroupAllMode(readerConfig) || forceDiagnosticPost === true) {
    console.error('[RAW_SOURCE_POST_BLOCKED]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      blockedCode,
      reason,
      sourceHost: sourceHost || 'unknown'
    });
  }
  if (
    ['UNVERIFIED_PRODUCT_BLOCKED', 'MAIN_POST_BLOCKED_UNVERIFIED_SOURCE'].includes(blockedCode) &&
    (!isReaderTestGroupAllMode(readerConfig) || forceDiagnosticPost === true)
  ) {
    console.error('[UNVERIFIED_PRODUCT_BLOCKED]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      reason,
      sourceHost: sourceHost || 'unknown'
    });
  }

  if (readerConfig.readerDebugMode === true || readerConfig.readerTestMode === true || forceDiagnosticPost === true) {
    const diagnosticText = buildReaderDiagnosticPostTextV2({
      reason,
      sourceHost,
      blockedCode,
      liveAllowed: false,
      testGroupPosted: true,
      channelRef: cleanText(source?.channelRef) || cleanText(structuredMessage?.channelRef),
      channelTitle: cleanText(source?.channelTitle) || cleanText(structuredMessage?.channelTitle),
      group: cleanText(structuredMessage?.group),
      mainPostBlocked,
      titleSource,
      imageSource,
      affiliateLinkSource
    });
    const diagnosticResult = await enqueueTelegramReaderDiagnosticPost({
      source,
      structuredMessage,
      diagnosticText,
      blockedCode,
      blockedReason: reason
    });
    console.info('[DEBUG_MESSAGE_ONLY]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      blockedCode: blockedCode || 'blocked',
      reason
    });
    return {
      accepted: false,
      status: 'diagnostic',
      review: true,
      reason,
      reasonCode: blockedCode || 'blocked',
      decision: 'REVIEW',
      queueId: diagnosticResult.queueId,
      queueStatus: diagnosticResult.queueStatus,
      messageId: diagnosticResult.messageId,
      postedToTestGroup: Boolean(diagnosticResult.messageId),
      forcedToTestGroup: true,
      trigger
    };
  }

  return {
    accepted: false,
    status: 'review',
    review: true,
    reason,
    reasonCode: blockedCode || 'blocked',
    decision: 'REVIEW',
    queueId: null,
    queueStatus: '',
    messageId: null,
    postedToTestGroup: false,
    forcedToTestGroup: false,
    trigger
  };
}

function isReaderTestGroupModeActive(readerConfig = {}) {
  return readerConfig?.readerTestMode === true && readerConfig?.readerDebugMode !== true;
}

function isTestGroupPublisherDisabledError(error) {
  const message = cleanText(error instanceof Error ? error.message : String(error || '')).toLowerCase();

  return (
    message.includes('telegram bot client ist deaktiviert') ||
    message.includes('keine telegram-zielgruppe') ||
    message.includes('keine aktiven ziele')
  );
}

function getReaderTestGroupChatId() {
  return cleanText(process.env.TELEGRAM_TEST_CHAT_ID);
}

function getReaderTestGroupImagePayload(generatorInput = {}) {
  if (generatorInput?.uploadedImageFile?.buffer && Buffer.isBuffer(generatorInput.uploadedImageFile.buffer)) {
    return {
      uploadedFile: generatorInput.uploadedImageFile,
      uploadedImage: '',
      imageUrl: '',
      imageSource: 'uploaded_file'
    };
  }

  const candidates = [cleanText(generatorInput.generatedImagePath), cleanText(generatorInput.uploadedImagePath)].filter(Boolean);

  for (const candidate of candidates) {
    if (candidate.startsWith('data:image')) {
      return {
        uploadedFile: null,
        uploadedImage: candidate,
        imageUrl: '',
        imageSource: 'data_url'
      };
    }

    if (/^https?:\/\//i.test(candidate)) {
      return {
        uploadedFile: null,
        uploadedImage: '',
        imageUrl: candidate,
        imageSource: 'remote_url'
      };
    }

    try {
      if (!fs.existsSync(candidate)) {
        continue;
      }

      const buffer = fs.readFileSync(candidate);
      if (!Buffer.isBuffer(buffer) || buffer.length === 0) {
        continue;
      }

      const extension = path.extname(candidate).toLowerCase();
      const mimetype =
        extension === '.png'
          ? 'image/png'
          : extension === '.webp'
            ? 'image/webp'
            : extension === '.gif'
              ? 'image/gif'
              : 'image/jpeg';

      return {
        uploadedFile: {
          buffer,
          mimetype,
          originalname: path.basename(candidate) || `reader-testgroup${extension || '.jpg'}`
        },
        uploadedImage: '',
        imageUrl: '',
        imageSource: 'local_file'
      };
    } catch {}
  }

  return {
    uploadedFile: null,
    uploadedImage: '',
    imageUrl: '',
    imageSource: 'none'
  };
}

async function sendReaderTestGroupPostDirect({
  sessionName = '',
  source = {},
  structuredMessage = {},
  generatorInput = {},
  trigger = 'reader',
  fallbackReason = ''
} = {}) {
  const targetChatId = getReaderTestGroupChatId();
  if (!targetChatId) {
    throw new Error('TELEGRAM_TEST_CHAT_ID fehlt im Backend. Live bleibt deaktiviert.');
  }

  const imagePayload = getReaderTestGroupImagePayload(generatorInput);
  const telegramText = [
    cleanText(generatorInput?.textByChannel?.telegram),
    cleanText(generatorInput?.debugInfoByChannel?.telegram)
  ]
    .filter(Boolean)
    .join('\n\n');

  const directResult = await sendTelegramPost({
    text: telegramText,
    uploadedFile: imagePayload.uploadedFile,
    uploadedImage: imagePayload.uploadedImage,
    imageUrl: imagePayload.imageUrl,
    disableWebPagePreview:
      !imagePayload.uploadedFile && !imagePayload.uploadedImage && !imagePayload.imageUrl,
    rabattgutscheinCode: cleanText(generatorInput?.couponCode),
    chatId: targetChatId,
    titlePreview: cleanText(generatorInput?.title).slice(0, 120),
    hasAffiliateLink: Boolean(cleanText(generatorInput?.link)),
    postContext: 'generic'
  });

  return {
    success: true,
    postedAt: nowIso(),
    queue: {
      id: null,
      status: 'direct_test_group_sent'
    },
    results: {
      telegram: {
        channelType: 'telegram',
        status: 'sent',
        imageSource: imagePayload.imageSource,
        deliveries: [
          {
            chatId: directResult?.chatId || targetChatId || null,
            targetChatId: directResult?.chatId || targetChatId || null,
            targetRef: targetChatId || null,
            targetName: 'Reader Test Group',
            messageId: directResult?.messageId || null,
            extraMessageIds: Array.isArray(directResult?.extraMessageIds) ? directResult.extraMessageIds : [],
            method: directResult?.method || 'sendMessage'
          }
        ],
        messageId: directResult?.messageId || null,
        chatId: directResult?.chatId || targetChatId || null
      }
    },
    deliveries: {
      telegram: [
        {
          chatId: directResult?.chatId || targetChatId || null,
          targetChatId: directResult?.chatId || targetChatId || null,
          targetRef: targetChatId || null,
          targetName: 'Reader Test Group',
          messageId: directResult?.messageId || null,
          extraMessageIds: Array.isArray(directResult?.extraMessageIds) ? directResult.extraMessageIds : [],
          method: directResult?.method || 'sendMessage'
        }
      ],
      whatsapp: [],
      facebook: []
    },
    meta: {
      deliveryMode: 'direct_test_group_fallback',
      fallbackReason: cleanText(fallbackReason),
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      trigger
    }
  };
}

async function processTelegramReaderPipeline(sessionName, source, structuredMessage, options = {}) {
  let detectedAsin = extractAsin(structuredMessage.text) || extractAsin(structuredMessage.link) || extractAsin(structuredMessage.externalLink);
  const explicitAmazonLink =
    findAmazonLinkInText(structuredMessage.text) ||
    findAmazonLinkInText(structuredMessage.link) ||
    findAmazonLinkInText(structuredMessage.externalLink);
  let amazonLink = cleanText(explicitAmazonLink) || (detectedAsin ? `https://www.amazon.de/dp/${detectedAsin}` : '');
  let originalLink =
    cleanText(structuredMessage.externalLink) ||
    cleanText(structuredMessage.previewUrl) ||
    cleanText(structuredMessage.link);
  const readerConfig = getReaderConfig();
  const testGroupModeActive = isReaderTestGroupModeActive(readerConfig);
  const relaxedTestMode = isReaderTestGroupAllMode(readerConfig);
  const trigger = cleanText(options.trigger) || 'reader';
  console.info('[PIPELINE_START]', {
    sessionName,
    sourceId: source?.id || null,
    group: structuredMessage?.group || '',
    messageId: structuredMessage?.messageId || '',
    trigger,
    readerTestMode: readerConfig.readerTestMode === true,
    readerDebugMode: readerConfig.readerDebugMode === true
  });
  const protectedDealSource = resolveProtectedDealSourceContext({
    source,
    structuredMessage,
    originalLink,
    originalText: cleanText(structuredMessage?.text)
  });
  const shortlinkCandidate = resolveReaderShortlinkCandidate({
    explicitAmazonLink: amazonLink,
    structuredMessage,
    originalLink
  });
  const shortlinkState = {
    detected: Boolean(shortlinkCandidate),
    resolved: false,
    originalUrl: shortlinkCandidate,
    finalUrl: '',
    asin: '',
    failed: false,
    errorMessage: ''
  };

  if (shortlinkCandidate) {
    console.info('[SHORTLINK_DETECTED]', {
      sessionName,
      sourceId: source?.id || null,
      messageId: structuredMessage.messageId,
      shortlinkUrl: shortlinkCandidate,
      sourceHost: normalizeUrlHost(shortlinkCandidate) || 'unknown'
    });

    if (protectedDealSource.matched === true) {
      console.info('[PROTECTED_SOURCE_NO_BYPASS]', {
        sessionName,
        sourceId: source?.id || null,
        messageId: structuredMessage.messageId,
        sourceLabel: protectedDealSource.sourceLabel || null,
        sourceField: protectedDealSource.matchedField || null,
        sourceHost: normalizeUrlHost(shortlinkCandidate) || 'unknown',
        reason: 'Shortlink-Aufloesung fuer geschuetzte Quelle uebersprungen.'
      });
    } else {
      const shortlinkResolution = await resolveReaderShortlink(shortlinkCandidate);
      if (shortlinkResolution.resolved === true) {
        shortlinkState.resolved = true;
        shortlinkState.finalUrl = cleanText(shortlinkResolution.finalUrl);
        shortlinkState.asin = cleanText(shortlinkResolution.asin).toUpperCase();
        amazonLink = shortlinkState.finalUrl;
        detectedAsin = shortlinkState.asin || detectedAsin;
        originalLink = shortlinkState.finalUrl || originalLink;

        console.info('[SHORTLINK_RESOLVED]', {
          sessionName,
          sourceId: source?.id || null,
          messageId: structuredMessage.messageId,
          shortlinkUrl: shortlinkCandidate,
          finalUrl: shortlinkState.finalUrl,
          asin: shortlinkState.asin,
          method: shortlinkResolution.method || 'follow'
        });
      } else {
        const shortlinkFailureReason =
          cleanText(shortlinkResolution.errorMessage) ||
          'Shortlink konnte nicht zu einer gueltigen Amazon-Produkt-URL aufgeloest werden.';
        shortlinkState.failed = true;
        shortlinkState.errorMessage = shortlinkFailureReason;
        shortlinkState.finalUrl = cleanText(shortlinkResolution.finalUrl);
        shortlinkState.asin = cleanText(shortlinkResolution.asin).toUpperCase();
        originalLink = shortlinkState.finalUrl || shortlinkCandidate || originalLink;
        amazonLink = shortlinkState.asin ? `https://www.amazon.de/dp/${shortlinkState.asin}` : '';
        detectedAsin = shortlinkState.asin || detectedAsin;

        console.warn('[SHORTLINK_RESOLVE_FAILED]', {
          sessionName,
          sourceId: source?.id || null,
          messageId: structuredMessage.messageId,
          shortlinkUrl: shortlinkCandidate,
          finalUrl: cleanText(shortlinkResolution.finalUrl) || null,
          asin: cleanText(shortlinkResolution.asin).toUpperCase() || null,
          reason: shortlinkFailureReason
        });
        console.info('[PROTECTED_SOURCE_NO_BYPASS]', {
          sessionName,
          sourceId: source?.id || null,
          messageId: structuredMessage.messageId,
          sourceHost: normalizeUrlHost(shortlinkCandidate) || 'unknown',
          reason: shortlinkFailureReason
        });
      }
    }
  }

  let dealType = resolveReaderDealType({
    amazonLink,
    detectedAsin
  });
  const sourceHost = normalizeUrlHost(originalLink || amazonLink);
  const sourceProtectionMatches = collectProtectedSourceMatches([
    { source: 'telegramText', value: structuredMessage.text },
    { source: 'previewTitle', value: structuredMessage.previewTitle },
    { source: 'previewDescription', value: structuredMessage.previewDescription }
  ]);
  const foreignShortlinkDetected =
    Boolean(originalLink) &&
    normalizeUrlHost(originalLink) !== 'amazon.de' &&
    !/amazon\./i.test(normalizeUrlHost(originalLink)) &&
    /^(?:amzn\.to|s\.[a-z0-9.-]+|[a-z0-9-]+\.[a-z0-9-]+\.[a-z0-9.-]+)$/i.test(sourceHost || '');
  const protectedSourceDetected =
    protectedDealSource.matched === true ||
    sourceProtectionMatches.length > 0 ||
    shortlinkState.failed === true ||
    foreignShortlinkDetected === true ||
    isProtectedSourceHost(originalLink) ||
    isProtectedSourceHost(amazonLink);
  const sourceStatus = protectedSourceDetected ? 'PROTECTED_SOURCE' : 'NORMAL_SOURCE';

  if (protectedDealSource.matched === true) {
    console.warn('[PROTECTED_DEAL_SOURCE_DETECTED]', {
      sessionName,
      sourceId: source?.id || null,
      messageId: structuredMessage.messageId,
      sourceKey: protectedDealSource.sourceKey,
      sourceLabel: protectedDealSource.sourceLabel,
      matchedField: protectedDealSource.matchedField,
      matchedValue: protectedDealSource.matchedValue || null,
      sourceHost: protectedDealSource.originalLinkHost || null
    });
    if (['channelRef', 'channelTitle', 'group'].includes(protectedDealSource.matchedField)) {
      console.info('[PROTECTED_SOURCE_CHANNEL_MATCHED]', {
        sessionName,
        sourceId: source?.id || null,
        messageId: structuredMessage.messageId,
        sourceKey: protectedDealSource.sourceKey,
        sourceLabel: protectedDealSource.sourceLabel,
        matchedField: protectedDealSource.matchedField,
        matchedValue: protectedDealSource.matchedValue || null
      });
    }
  }

  console.info('[READER_INPUT_RECEIVED]', {
    sessionName,
    sourceId: source?.id || null,
    group: structuredMessage.group,
    messageId: structuredMessage.messageId,
    chatId: structuredMessage.chatId,
    originalLink,
    amazonLink,
    detectedAsin: detectedAsin || '',
    preview: structuredMessage.text.slice(0, TELEGRAM_RAW_EVENT_TEXT_LIMIT)
  });
  console.info('[READER_DEAL_FOUND]', {
    sessionName,
    sourceId: source?.id || null,
    group: structuredMessage.group,
    messageId: structuredMessage.messageId,
    chatId: structuredMessage.chatId,
    amazonLink,
    preview: structuredMessage.text.slice(0, TELEGRAM_RAW_EVENT_TEXT_LIMIT)
  });
  console.info('[PIPELINE_RECEIVED]', {
    sessionName,
    sourceId: source?.id || null,
    group: structuredMessage.group,
    messageId: structuredMessage.messageId,
    chatId: structuredMessage.chatId,
    amazonLink,
    readerTestMode: readerConfig.readerTestMode === true,
    readerDebugMode: readerConfig.readerDebugMode === true
  });
  if (testGroupModeActive) {
    console.info('[TEST_GROUP_MODE_ACTIVE]', {
      sessionName,
      sourceId: source?.id || null,
      messageId: structuredMessage.messageId,
      readerTestMode: true,
      readerDebugMode: false,
      liveAllowed: false,
      explicitTestGroupChatConfigured: Boolean(getReaderTestGroupChatId())
    });
  }
  console.info('[DEAL_TYPE_DETECTED]', {
    sessionName,
    sourceId: source?.id || null,
    messageId: structuredMessage.messageId,
    dealType
  });
  if (protectedSourceDetected) {
    console.warn('[PROTECTED_SOURCE_DETECTED]', {
      sessionName,
      sourceId: source?.id || null,
      messageId: structuredMessage.messageId,
      sourceHost: sourceHost || 'unknown',
      sourceStatus,
      shortlinkFailed: shortlinkState.failed === true,
      matches: sourceProtectionMatches.map((entry) => `${entry.source}:${entry.key}`)
    });
    if (
      shortlinkState.failed === true ||
      sourceProtectionMatches.some((entry) => ['just_a_moment', 'checking_your_browser', 'cloudflare', 'cf_ray'].includes(entry.key))
    ) {
      console.warn('[CLOUDFLARE_SOURCE_DETECTED]', {
        sessionName,
        sourceId: source?.id || null,
        messageId: structuredMessage.messageId,
        sourceHost: sourceHost || 'unknown',
        matches: sourceProtectionMatches.map((entry) => `${entry.source}:${entry.key}`),
        reason: shortlinkState.errorMessage || ''
      });
    }
    console.warn('[CLOUDFLARE_DETECTED]', {
      sessionName,
      sourceId: source?.id || null,
      messageId: structuredMessage.messageId,
      sourceHost: sourceHost || 'unknown',
      matches: sourceProtectionMatches.map((entry) => `${entry.source}:${entry.key}`)
    });
  }

  if (!source?.id) {
    logReaderPipelineError('Telegram-Quelle fehlt oder ist nicht registriert.', {
      sessionName,
      messageId: structuredMessage.messageId
    });
    return {
      accepted: false,
      status: 'skipped',
      reason: 'missing_source',
      reasonCode: 'missing_source',
      decision: 'REVIEW',
      queueId: null,
      queueStatus: '',
      messageId: null,
      postedToTestGroup: false,
      forcedToTestGroup: false,
      trigger
    };
  }

  if (isReaderTestGroupAllMode(readerConfig)) {
    console.info('[TESTGROUP_POST_ALL_MODE]', {
      sessionName,
      sourceId: source.id,
      messageId: structuredMessage.messageId,
      dealType,
      originalLink,
      readerTestMode: readerConfig.readerTestMode === true,
      readerDebugMode: readerConfig.readerDebugMode === true,
      reason: 'Reader-Test/Debugmodus postet jeden eingehenden Deal in die Testgruppe.'
    });
  }

  console.info('[GENERATOR_FORMAT_START]', {
    sessionName,
    sourceId: source.id,
    messageId: structuredMessage.messageId,
    amazonLink: amazonLink || originalLink,
    readerTestMode: readerConfig.readerTestMode === true,
    readerDebugMode: readerConfig.readerDebugMode === true
  });
  console.info('[GENERATOR_FORCE_START]', {
    sessionName,
    sourceId: source.id,
    messageId: structuredMessage.messageId,
    amazonLink: amazonLink || originalLink,
    trigger
  });

  try {
    const pricing = extractTelegramDealPricing(structuredMessage.text);
    const couponCode = extractTelegramCouponCode(structuredMessage.text);
    if (foreignShortlinkDetected && !relaxedTestMode) {
      console.error('[FOREIGN_SHORTLINK_BLOCKED]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        sourceHost: sourceHost || 'unknown',
        originalLink
      });
    }
    if (dealType === 'AMAZON' && isAmazonShortLink(amazonLink) && !relaxedTestMode) {
      console.info('[AUTOMATION_SHORTLINK_BLOCKED]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        originalUrl: amazonLink
      });
    }
    let scrapedDeal;
    if (protectedDealSource.matched === true) {
      const protectedTitleSelection = selectSourceTitleCandidate({
        previewTitle: sanitizeProtectedSourceValue(structuredMessage?.previewTitle, 'previewTitle'),
        scrapedTitle: '',
        originalText: cleanText(structuredMessage?.text),
        fallback: sanitizeProtectedSourceValue(extractTelegramTitle(structuredMessage?.text, structuredMessage?.group), 'telegramTitle')
      });
      console.info('[PROTECTED_SOURCE_NO_BYPASS]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        sourceLabel: protectedDealSource.sourceLabel,
        sourceField: protectedDealSource.matchedField || null,
        originalLink: originalLink || null,
        amazonLink: amazonLink || null,
        reason: 'Geschuetzte Quelle wird nur ueber sichtbare Telegram-Post-Daten verarbeitet.'
      });
      scrapedDeal = {
        success: false,
        blockedByProtection: true,
        protectedMatches: [
          {
            source: 'protected_deal_source',
            key: protectedDealSource.sourceKey || 'protected_source',
            value: protectedDealSource.sourceLabel || 'Geschuetzte Quelle'
          }
        ],
        title: cleanText(protectedTitleSelection.cleanedValue || ''),
        productTitle: cleanText(protectedTitleSelection.cleanedValue || ''),
        titleDataSource: 'telegram',
        productDescription: cleanText(structuredMessage?.previewDescription) || '',
        price:
          pricing?.currentPrice !== null && pricing?.currentPrice !== undefined ? formatPrice(pricing.currentPrice) : '',
        oldPrice:
          pricing?.oldPrice !== null && pricing?.oldPrice !== undefined ? formatPrice(pricing.oldPrice) : '',
        imageUrl: '',
        imageDataSource: 'missing',
        previewImage: '',
        ogImage: '',
        asin: detectedAsin || '',
        finalUrl: '',
        resolvedUrl: '',
        originalUrl: originalLink || amazonLink || '',
        normalizedUrl: '',
        sellerType: 'UNKNOWN',
        sellerClass: 'UNKNOWN',
        soldByAmazon: null,
        shippedByAmazon: null,
        sellerDetails: {
          detectionSource: 'protected_source_no_bypass',
          detectionSources: ['protected_source_no_bypass'],
          merchantText: '',
          matchedPatterns: [],
          dealType: dealType === 'AMAZON' ? 'AMAZON' : 'NON_AMAZON',
          isAmazonDeal: dealType === 'AMAZON'
        },
        imageDebug: {
          paapiStatus: 'protected_source_no_bypass'
        }
      };
    } else if (dealType === 'AMAZON') {
      try {
        scrapedDeal = await scrapeAmazonProduct(amazonLink);
        const paapiStatus = cleanText(scrapedDeal?.imageDebug?.paapiStatus || '');
        if (paapiStatus && paapiStatus !== 'available') {
          console.warn('[AMAZON_API_FAIL_FALLBACK]', {
            sessionName,
            sourceId: source.id,
            messageId: structuredMessage.messageId,
            amazonLink,
            paapiStatus
          });
          console.info('[AMAZON_FALLBACK_ACTIVE]', {
            sessionName,
            sourceId: source.id,
            messageId: structuredMessage.messageId,
            priceSource: pricing?.currentPrice !== null && pricing?.currentPrice !== undefined ? 'telegram_text' : 'scrape_or_default'
          });
        }
      } catch (scrapeError) {
        const scrapeErrorMessage = scrapeError instanceof Error ? scrapeError.message : 'Amazon Scrape fehlgeschlagen.';
        console.warn('[AMAZON_API_FAIL_FALLBACK]', {
          sessionName,
          sourceId: source.id,
          messageId: structuredMessage.messageId,
          amazonLink,
          reason: scrapeErrorMessage
        });
        console.info('[AMAZON_FALLBACK_ACTIVE]', {
          sessionName,
          sourceId: source.id,
          messageId: structuredMessage.messageId,
          priceSource:
            pricing?.currentPrice !== null && pricing?.currentPrice !== undefined
              ? 'telegram_text'
              : 'default'
        });
        scrapedDeal = {
          success: false,
          title: '',
          productTitle: '',
          titleDataSource: 'missing',
          imageUrl: '',
          imageDataSource: 'missing',
          price:
            pricing?.currentPrice !== null && pricing?.currentPrice !== undefined ? String(pricing.currentPrice) : '',
          oldPrice:
            pricing?.oldPrice !== null && pricing?.oldPrice !== undefined ? String(pricing.oldPrice) : '',
          asin: detectedAsin || '',
          finalUrl: amazonLink,
          resolvedUrl: amazonLink,
          originalUrl: amazonLink,
          normalizedUrl: amazonLink,
          sellerType: 'UNKNOWN',
          sellerClass: 'UNKNOWN',
          sellerDetails: {
            detectionSource: 'amazon-fallback',
            detectionSources: ['amazon-fallback'],
            merchantText: '',
            matchedPatterns: []
          },
          imageDebug: {
            paapiStatus: 'fallback_after_scrape_error'
          }
        };
      }
      scrapedDeal = await runUnknownSellerSecondPass({
        sessionName,
        source,
        structuredMessage,
        amazonLink,
        scrapedDeal
      });
    } else {
      console.info('[NON_AMAZON_PIPELINE_STARTED]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        originalLink
      });
      try {
        scrapedDeal = await scrapeGenericDealPage(originalLink);
      } catch (scrapeError) {
        scrapedDeal = {
          success: false,
          finalUrl: originalLink,
          resolvedUrl: originalLink,
          normalizedUrl: originalLink,
          title: '',
          productDescription: '',
          price: '',
          imageUrl: '',
          previewImage: '',
          ogImage: '',
          scrapedImage: '',
          sellerType: 'UNKNOWN',
          sellerClass: 'UNKNOWN',
          soldByAmazon: null,
          shippedByAmazon: null,
          sellerDetails: {
            detectionSource: 'non-amazon',
            detectionSources: ['non-amazon'],
            merchantText: '',
            matchedPatterns: [],
            dealType: 'NON_AMAZON',
            isAmazonDeal: false
          },
          scrapeError: scrapeError instanceof Error ? scrapeError.message : 'Non-Amazon-Scrape fehlgeschlagen.'
        };
      }
    }
    const scrapedProtectionMatches = [
      ...collectProtectedSourceMatches([
        { source: 'scrapedTitle', value: scrapedDeal?.title },
        { source: 'scrapedDescription', value: scrapedDeal?.productDescription }
      ]),
      ...(Array.isArray(scrapedDeal?.protectedMatches) ? scrapedDeal.protectedMatches : [])
    ];
    const sourceMeta = {
      sourceHost,
      sourceStatus,
      protectedSource:
        protectedSourceDetected || scrapedProtectionMatches.length > 0 || scrapedDeal?.blockedByProtection === true || isProtectedSourceStatusCode(scrapedDeal?.statusCode),
      blockedCode: '',
      blockedReason: '',
      matchScore: null,
      matchTier: '',
      relaxedReason: '',
      matchReason: '',
      matchDecision: '',
      matchedBySource: false,
      shortlinkResolved: shortlinkState.resolved === true,
      shortlinkFallback: false
    };

    if (scrapedProtectionMatches.length || scrapedDeal?.blockedByProtection === true || isProtectedSourceStatusCode(scrapedDeal?.statusCode)) {
      sourceMeta.sourceStatus = 'PROTECTED_SOURCE';
      console.warn('[PROTECTED_SOURCE_DETECTED]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        sourceHost: sourceHost || 'unknown',
        sourceStatus: 'PROTECTED_SOURCE',
        statusCode: Number(scrapedDeal?.statusCode || 0),
        matches: scrapedProtectionMatches.map((entry) => `${entry.source}:${entry.key}`)
      });
      console.info('[PROTECTED_SOURCE_NO_BYPASS]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        sourceHost: sourceHost || 'unknown',
        statusCode: Number(scrapedDeal?.statusCode || 0),
        reason:
          cleanText(scrapedDeal?.scrapeError) ||
          (isProtectedSourceStatusCode(scrapedDeal?.statusCode) ? `HTTP ${scrapedDeal.statusCode}` : 'Schutzseite erkannt.')
      });
      console.warn('[CLOUDFLARE_DETECTED]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        sourceHost: sourceHost || 'unknown',
        matches: scrapedProtectionMatches.map((entry) => `${entry.source}:${entry.key}`)
      });
    }

    let resolvedAmazonUrl =
      cleanText(scrapedDeal?.finalUrl) || cleanText(scrapedDeal?.resolvedUrl) || cleanText(scrapedDeal?.normalizedUrl);
    let linkRecord =
      dealType === 'AMAZON'
        ? buildReaderLinkRecord({
            dealType,
            amazonLink,
            fallbackLink: resolvedAmazonUrl || originalLink,
            asin: cleanText(scrapedDeal?.asin || detectedAsin),
            structuredMessage
          })
        : {
            valid: false,
            affiliateUrl: '',
            normalizedUrl: '',
            asin: ''
          };

    if (dealType === 'AMAZON' && (!linkRecord.valid || !cleanText(linkRecord.affiliateUrl) || !cleanText(linkRecord.asin))) {
      console.error('[AFFILIATE_LINK_REQUIRED]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        amazonLink,
        resolvedAmazonUrl,
        scrapedAsin: cleanText(scrapedDeal?.asin)
      });
    }

    const needsAmazonRecovery =
      dealType !== 'AMAZON' ||
      sourceMeta.protectedSource === true ||
      !linkRecord.valid ||
      !cleanText(linkRecord.affiliateUrl) ||
      !cleanText(linkRecord.asin);
    const requiresVerifiedAmazonFinalPost =
      dealType === 'AMAZON' ||
      Boolean(cleanText(amazonLink)) ||
      foreignShortlinkDetected === true ||
      protectedDealSource.matched === true;

    if (needsAmazonRecovery) {
      if (sourceMeta.protectedSource === true && !relaxedTestMode) {
        console.error('[CLOUDFLARE_SOURCE_BLOCKED]', {
          sessionName,
          sourceId: source.id,
          messageId: structuredMessage.messageId,
          sourceHost: sourceHost || 'unknown'
        });
      }
      if (sourceMeta.sourceStatus === 'PROTECTED_SOURCE' || shortlinkState.failed === true || foreignShortlinkDetected === true) {
        console.info('[PROTECTED_SOURCE_FALLBACK_TO_MATCHING]', {
          sessionName,
          sourceId: source.id,
          messageId: structuredMessage.messageId,
          sourceHost: sourceHost || 'unknown',
          sourceStatus: sourceMeta.sourceStatus,
          shortlinkFailed: shortlinkState.failed === true,
          foreignShortlinkDetected
        });
      }

      const recoveryResult = await searchAmazonProductBySourceData({
        sessionName,
        source,
        structuredMessage,
        scrapedDeal,
        pricing,
        originalLink,
        detectedAsin
      });

      if (recoveryResult.matched === true && recoveryResult.scrapedDeal) {
        dealType = 'AMAZON';
        detectedAsin = cleanText(recoveryResult.linkRecord?.asin || recoveryResult.scrapedDeal?.asin).toUpperCase();
        amazonLink = cleanText(recoveryResult.linkRecord?.normalizedUrl || recoveryResult.scrapedDeal?.normalizedUrl || amazonLink);
        scrapedDeal = recoveryResult.scrapedDeal;
        resolvedAmazonUrl =
          cleanText(scrapedDeal?.finalUrl) || cleanText(scrapedDeal?.resolvedUrl) || cleanText(scrapedDeal?.normalizedUrl);
        linkRecord = recoveryResult.linkRecord;
        sourceMeta.protectedSource = false;
        sourceMeta.blockedCode = '';
        sourceMeta.blockedReason = '';
        sourceMeta.matchScore = Number.isFinite(Number(recoveryResult.matchScore)) ? Number(recoveryResult.matchScore) : null;
        sourceMeta.matchTier = cleanText(recoveryResult.matchTier || 'auto_post').toLowerCase();
        sourceMeta.matchReason = cleanText(recoveryResult.matchReason);
        sourceMeta.matchDecision = cleanText(recoveryResult.decision || sourceMeta.matchTier).toUpperCase();
        sourceMeta.relaxedReason = cleanText(recoveryResult.reason);
        sourceMeta.matchedBySource = true;
        if (foreignShortlinkDetected === true || isAmazonShortLink(amazonLink)) {
          sourceMeta.shortlinkResolved = true;
          console.info('[SHORTLINK_RESOLVED]', {
            sessionName,
            sourceId: source.id,
            messageId: structuredMessage.messageId,
            asin: detectedAsin,
            originalLink,
            affiliateUrl: cleanText(linkRecord?.affiliateUrl || '')
          });
        }
      } else {
        sourceMeta.blockedCode =
          sourceMeta.protectedSource === true
            ? 'CLOUDFLARE_OR_PROTECTED_SOURCE'
            : foreignShortlinkDetected === true
              ? 'FOREIGN_SHORTLINK_BLOCKED'
              : 'UNVERIFIED_PRODUCT_BLOCKED';
        sourceMeta.blockedReason =
          recoveryResult.reason ||
          (sourceMeta.protectedSource === true
            ? 'Produkt nicht verifiziert: geschuetzte oder Cloudflare-Quelle.'
            : 'Produkt nicht verifiziert.');

        if (relaxedTestMode && !requiresVerifiedAmazonFinalPost) {
          const relaxedReason = cleanText(recoveryResult.reason || sourceMeta.blockedReason) || 'Kein perfekter Match.';
          const fallbackTitle =
            cleanText(recoveryResult.sourceFacts?.title) ||
            cleanText(scrapedDeal?.title || scrapedDeal?.productTitle) ||
            cleanText(structuredMessage.previewTitle) ||
            extractTelegramTitle(structuredMessage.text, structuredMessage.group);
          const fallbackPrice =
            recoveryResult.sourceFacts?.priceValue !== null && recoveryResult.sourceFacts?.priceValue !== undefined
              ? formatPrice(recoveryResult.sourceFacts.priceValue)
              : cleanText(scrapedDeal?.price) ||
                (pricing?.currentPrice !== null && pricing?.currentPrice !== undefined ? formatPrice(pricing.currentPrice) : '');
          const fallbackImage =
            cleanText(recoveryResult.sourceFacts?.imageUrl) ||
            cleanText(scrapedDeal?.imageUrl || scrapedDeal?.previewImage || scrapedDeal?.ogImage || structuredMessage.previewImage);

          sourceMeta.matchScore = Number.isFinite(Number(recoveryResult.matchScore)) ? Number(recoveryResult.matchScore) : null;
          sourceMeta.matchTier = cleanText(recoveryResult.matchTier || 'debug').toLowerCase();
          sourceMeta.matchReason = cleanText(recoveryResult.matchReason);
          sourceMeta.matchDecision = cleanText(recoveryResult.decision || sourceMeta.matchTier).toUpperCase();
          sourceMeta.relaxedReason = relaxedReason;
          sourceMeta.shortlinkFallback = foreignShortlinkDetected === true || isAmazonShortLink(amazonLink);

          if (sourceMeta.shortlinkFallback === true) {
            console.info('[SHORTLINK_FALLBACK]', {
              sessionName,
              sourceId: source.id,
              messageId: structuredMessage.messageId,
              sourceHost: sourceHost || 'unknown',
              reason: relaxedReason
            });
          }

          dealType = 'NON_AMAZON';
          amazonLink = '';
          detectedAsin = '';
          resolvedAmazonUrl = originalLink || resolvedAmazonUrl;
          scrapedDeal = {
            ...scrapedDeal,
            success: true,
            asin: '',
            title: fallbackTitle || 'Deal',
            titleDataSource: 'source',
            productDescription: cleanText(scrapedDeal?.productDescription) || cleanText(structuredMessage.previewDescription) || '',
            price: fallbackPrice || '',
            basePrice: '',
            finalPrice: '',
            finalPriceCalculated: false,
            imageUrl: fallbackImage || '',
            imageDataSource: fallbackImage ? 'source' : 'missing',
            previewImage: cleanText(scrapedDeal?.previewImage) || cleanText(structuredMessage.previewImage) || fallbackImage || '',
            ogImage: cleanText(scrapedDeal?.ogImage) || fallbackImage || '',
            finalUrl: resolvedAmazonUrl || originalLink,
            resolvedUrl: resolvedAmazonUrl || originalLink,
            normalizedUrl: resolvedAmazonUrl || originalLink,
            sellerType: 'UNKNOWN',
            sellerClass: 'UNKNOWN',
            soldByAmazon: null,
            shippedByAmazon: null,
            sellerDetails: {
              ...(scrapedDeal?.sellerDetails && typeof scrapedDeal.sellerDetails === 'object' ? scrapedDeal.sellerDetails : {}),
              detectionSource: 'relaxed-test-fallback',
              detectionSources: ['relaxed-test-fallback'],
              matchedPatterns: [],
              dealType: 'NON_AMAZON',
              isAmazonDeal: false
            }
          };
          linkRecord = buildReaderLinkRecord({
            dealType,
            fallbackLink: resolvedAmazonUrl || originalLink,
            structuredMessage
          });
        } else {
          if (requiresVerifiedAmazonFinalPost) {
            console.info('[SOURCE_VALUES_STRIPPED]', {
              dealType: 'AMAZON',
              strippedField: 'recovery_fallback',
              blockedSource: foreignShortlinkDetected === true ? 'foreign_shortlink' : 'unverified_source_values',
              blockedValue: originalLink || amazonLink || null,
              keptSource: 'debug_only',
              keptValue: null
            });
          }
          return await handleBlockedReaderDiagnostic({
            sessionName,
            source,
            structuredMessage,
            readerConfig,
            trigger,
            blockedCode: sourceMeta.blockedCode,
            blockedReason: sourceMeta.blockedReason,
            sourceHost: sourceHost || recoveryResult.sourceFacts?.host || 'unknown'
          });
        }
      }
    }

    const affiliateUrl = cleanText(linkRecord.affiliateUrl);
    const normalizedUrl = cleanText(linkRecord.normalizedUrl || resolvedAmazonUrl);
    const normalizedAsin = resolveNormalizedReaderAsin({
      amazonLink,
      scrapedDeal,
      linkRecord
    });

    if (dealType === 'AMAZON' && affiliateUrl) {
      const rawSourceLink = cleanText(originalLink || amazonLink);
      if (rawSourceLink && rawSourceLink !== affiliateUrl) {
        console.info('[FOREIGN_LINK_REMOVED]', {
          sessionName,
          sourceId: source.id,
          messageId: structuredMessage.messageId,
          originalLink: rawSourceLink,
          affiliateUrl,
          sourceHost: normalizeUrlHost(rawSourceLink) || 'unknown'
        });
      }
      console.info('[OWN_AFFILIATE_LINK_USED]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        asin: normalizedAsin || cleanText(linkRecord.asin).toUpperCase() || '',
        affiliateUrl
      });
    }

    const normalizedScrapedDeal = {
      ...scrapedDeal,
      asin: normalizedAsin || cleanText(scrapedDeal?.asin).toUpperCase(),
      normalizedUrl: normalizedUrl || cleanText(scrapedDeal?.normalizedUrl),
      finalUrl: resolvedAmazonUrl || cleanText(scrapedDeal?.finalUrl),
      resolvedUrl: resolvedAmazonUrl || cleanText(scrapedDeal?.resolvedUrl),
      sellerType: cleanText(scrapedDeal?.sellerType || '').toUpperCase() || (dealType === 'AMAZON' ? 'UNKNOWN' : 'UNKNOWN'),
      sellerClass: cleanText(scrapedDeal?.sellerClass || '').toUpperCase() || 'UNKNOWN',
      sellerDetails: {
        ...(scrapedDeal?.sellerDetails && typeof scrapedDeal.sellerDetails === 'object' ? scrapedDeal.sellerDetails : {}),
        dealType,
        isAmazonDeal: dealType === 'AMAZON'
      },
      titleDataSource:
        cleanText(scrapedDeal?.titleDataSource) ||
        (dealType === 'AMAZON'
          ? cleanText(scrapedDeal?.dataSource).toLowerCase() === 'paapi'
            ? 'paapi'
            : cleanText(scrapedDeal?.productTitle || scrapedDeal?.title)
              ? 'amazon'
              : 'missing'
          : cleanText(scrapedDeal?.title)
            ? 'source'
            : 'missing'),
      imageDataSource:
        cleanText(scrapedDeal?.imageDataSource) ||
        (dealType === 'AMAZON'
          ? cleanText(scrapedDeal?.imageUrl)
            ? cleanText(scrapedDeal?.imageDebug?.selectedSource).toLowerCase() === 'paapi' ||
              cleanText(scrapedDeal?.dataSource).toLowerCase() === 'paapi'
              ? 'paapi'
              : 'amazon'
            : 'missing'
          : cleanText(scrapedDeal?.imageUrl || scrapedDeal?.previewImage || scrapedDeal?.ogImage)
            ? 'source'
            : 'missing'),
      title:
        dealType === 'AMAZON'
          ? cleanText(scrapedDeal?.productTitle || scrapedDeal?.title)
          : cleanText(scrapedDeal?.title) ||
            cleanText(structuredMessage.previewTitle) ||
            extractTelegramTitle(structuredMessage.text, structuredMessage.group),
      productDescription:
        dealType === 'AMAZON'
          ? cleanText(scrapedDeal?.productDescription)
          : cleanText(scrapedDeal?.productDescription) || cleanText(structuredMessage.previewDescription) || '',
      price:
        cleanText(scrapedDeal?.price) ||
        (pricing?.currentPrice !== null && pricing?.currentPrice !== undefined ? String(pricing.currentPrice) : ''),
      basePrice: cleanText(scrapedDeal?.basePrice || ''),
      finalPrice: cleanText(scrapedDeal?.finalPrice || ''),
      finalPriceCalculated: scrapedDeal?.finalPriceCalculated === true,
      previewImage: cleanText(scrapedDeal?.previewImage || ''),
      ogImage: cleanText(scrapedDeal?.ogImage || ''),
      imageUrl: cleanText(scrapedDeal?.imageUrl || '')
    };
    const generatorInput = await buildTelegramReaderGeneratorInput({
      structuredMessage,
      scrapedDeal: normalizedScrapedDeal,
      normalizedAsin,
      affiliateUrl,
      normalizedUrl,
      couponCode,
      pricing,
      dealType
    });
    generatorInput.matchScore = sourceMeta.matchScore;
    generatorInput.matchTier = sourceMeta.matchTier;
    generatorInput.matchWarningReason = sourceMeta.relaxedReason || sourceMeta.blockedReason || '';
    generatorInput.matchReason = sourceMeta.matchReason;
    generatorInput.matchDecision = sourceMeta.matchDecision;
    generatorInput.sourceStatus = sourceMeta.sourceStatus || 'NORMAL_SOURCE';
    generatorInput.protectedSource = sourceMeta.sourceStatus === 'PROTECTED_SOURCE';
    generatorInput.shortlinkResolved = sourceMeta.shortlinkResolved === true;
    generatorInput.shortlinkFallback = sourceMeta.shortlinkFallback === true;
    const missingGeneratorFields = collectMissingReaderGeneratorInputFields(generatorInput);
    console.info('[READER_GENERATOR_INPUT_BUILT]', {
      sessionName,
      sourceId: source.id,
      messageId: structuredMessage.messageId,
      asin: generatorInput.asin,
      hasTitle: Boolean(cleanText(generatorInput.title)),
      hasPrice: Boolean(cleanText(generatorInput.currentPrice)),
      hasImage: Boolean(cleanText(generatorInput.generatedImagePath) || cleanText(generatorInput.uploadedImagePath)),
      hasAffiliateLink: Boolean(cleanText(generatorInput.link)),
      missingFields: missingGeneratorFields
    });
    console.info('[FINAL_POST_DATA_SOURCES]', {
      sessionName,
      sourceId: source.id,
      messageId: structuredMessage.messageId,
      asin: generatorInput.asin,
      titleSource: cleanText(generatorInput.mainPostTitleSource || generatorInput.titleSource) || 'unknown',
      rawTitleSource: cleanText(generatorInput.rawTitleSource) || 'unknown',
      priceSource: cleanText(generatorInput.priceSource) || 'unknown',
      imageSource: cleanText(generatorInput.mainPostImageSource || generatorInput.imageSource) || 'unknown',
      rawImageSource: cleanText(generatorInput.imageSource) || 'unknown',
      affiliateLinkSource: cleanText(generatorInput.affiliateLinkSource) || 'missing'
    });
    if (sourceMeta.matchedBySource === true) {
      console.info('[GENERATOR_INPUT_FROM_MATCHING_BUILT]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        asin: generatorInput.asin,
        matchScore: generatorInput.matchScore,
        matchTier: generatorInput.matchTier || 'unknown',
        matchReason: generatorInput.matchReason || null,
        sourceStatus: generatorInput.sourceStatus || 'NORMAL_SOURCE'
      });
      if (
        cleanText(generatorInput.matchTier).toLowerCase() === 'review' ||
        cleanText(generatorInput.priceSource).toLowerCase() === 'telegram' ||
        generatorInput.protectedSource === true
      ) {
        console.info('[GENERATOR_INPUT_FROM_MATCHING_REVIEW]', {
          sessionName,
          sourceId: source.id,
          messageId: structuredMessage.messageId,
          asin: generatorInput.asin,
          matchScore: generatorInput.matchScore,
          priceSource: cleanText(generatorInput.priceSource) || 'unknown',
          imageSource: cleanText(generatorInput.imageSource) || 'unknown',
          reason: generatorInput.matchWarningReason || generatorInput.matchReason || 'Review erforderlich.'
        });
      }
    }
    if (missingGeneratorFields.length) {
      const imageBlockReason = cleanText(generatorInput.imageBlockReason);
      const missingFieldsReason = imageBlockReason && missingGeneratorFields.includes('image')
        ? `GeneratorInput unvollstaendig. Fehlende Felder: ${missingGeneratorFields.join(', ')}. ${imageBlockReason}`
        : `GeneratorInput unvollstaendig. Fehlende Felder: ${missingGeneratorFields.join(', ')}`;
      console.error('[GENERATOR_INPUT_MISSING_FIELDS]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        asin: generatorInput.asin,
        missingFields: missingGeneratorFields
      });
      if (readerConfig.readerDebugMode === true || readerConfig.readerTestMode === true) {
        const diagnosticText = buildReaderDiagnosticPostTextV2({
          reason: missingFieldsReason,
          sourceHost: sourceHost || 'unknown',
          blockedCode: 'GENERATOR_INPUT_MISSING_FIELDS',
          liveAllowed: false,
          testGroupPosted: true,
          channelRef: cleanText(source?.channelRef) || cleanText(structuredMessage?.channelRef),
          channelTitle: cleanText(source?.channelTitle) || cleanText(structuredMessage?.channelTitle),
          group: cleanText(structuredMessage?.group)
        });
        const diagnosticResult = await enqueueTelegramReaderDiagnosticPost({
          source,
          structuredMessage,
          diagnosticText,
          blockedCode: 'GENERATOR_INPUT_MISSING_FIELDS',
          blockedReason: missingFieldsReason
        });
        return {
          accepted: false,
          status: 'diagnostic',
          review: true,
          reason: missingFieldsReason,
          reasonCode: 'generator_input_missing_fields',
          decision: 'REVIEW',
          queueId: diagnosticResult.queueId,
          queueStatus: diagnosticResult.queueStatus,
          messageId: diagnosticResult.messageId,
          postedToTestGroup: Boolean(diagnosticResult.messageId),
          forcedToTestGroup: true,
          trigger
        };
      }
      return {
        accepted: false,
        status: 'review',
        review: true,
        reason: missingFieldsReason,
        reasonCode: 'generator_input_missing_fields',
        decision: 'REVIEW',
        queueId: null,
        queueStatus: '',
        messageId: null,
        postedToTestGroup: false,
        forcedToTestGroup: false,
        trigger
      };
    }
    const productVerification = resolveProductVerification({
      sessionName,
      source,
      structuredMessage,
      dealType,
      linkRecord,
      scrapedDeal: normalizedScrapedDeal,
      generatorInput,
      sourceMeta,
      readerConfig
    });
    generatorInput.productVerificationWarning = productVerification.warningOnly === true ? productVerification.reason : '';
    generatorInput.productVerificationIssues = Array.isArray(productVerification.issues) ? productVerification.issues : [];
    if (productVerification.verified !== true) {
      return await handleBlockedReaderDiagnostic({
        sessionName,
        source,
        structuredMessage,
        readerConfig,
        trigger,
        blockedCode: sourceMeta.blockedCode || 'UNVERIFIED_PRODUCT_BLOCKED',
        blockedReason: productVerification.reason,
        sourceHost: sourceHost || 'unknown'
      });
    }
    const mainPostSourceValidation = validateMainPostSources({
      ...generatorInput,
      productVerified: productVerification.verified === true
    });
    generatorInput.mainPostBlocked = mainPostSourceValidation.valid !== true;
    generatorInput.mainPostBlockReason = cleanText(mainPostSourceValidation.reason);
    generatorInput.mainPostTitleSource = mainPostSourceValidation.titleSource || generatorInput.mainPostTitleSource || 'unknown';
    generatorInput.mainPostImageSource = mainPostSourceValidation.imageSource || generatorInput.mainPostImageSource || 'unknown';
    generatorInput.affiliateLinkSource = mainPostSourceValidation.affiliateLinkSource || generatorInput.affiliateLinkSource || 'unknown';
    if (mainPostSourceValidation.valid !== true) {
      return await handleBlockedReaderDiagnostic({
        sessionName,
        source,
        structuredMessage,
        readerConfig,
        trigger,
        blockedCode: 'MAIN_POST_BLOCKED_UNVERIFIED_SOURCE',
        blockedReason: mainPostSourceValidation.reason,
        sourceHost: sourceHost || 'unknown',
        forceDiagnosticPost: true,
        mainPostBlocked: true,
        titleSource: generatorInput.mainPostTitleSource,
        imageSource: generatorInput.mainPostImageSource,
        affiliateLinkSource: generatorInput.affiliateLinkSource
      });
    }
    const debugPostEnabled = readerConfig.readerDebugMode === true || readerConfig.readerTestMode === true;

    if (isReaderTestGroupAllMode(readerConfig)) {
      console.info('[PIPELINE_CONTINUE_FORCED]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        dealType,
        sellerClass: generatorInput.sellerClass,
        reason: 'Reader-Test/Debugmodus umgeht Seller-, Score- und Amazon-Link-Filter.'
      });
    }

    if (generatorInput.invalidPrice === true) {
      console.error('[PRICE_INVALID_ZERO]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        asin: generatorInput.asin,
        reason: generatorInput.invalidPriceReason || 'Preis ist 0,00€ oder ungueltig.'
      });
    }

    if (debugPostEnabled && !generatorInput.generatedImagePath && !generatorInput.uploadedImagePath) {
      const imageMissingReason = 'Generator-Bild fehlt im Reader-Testmodus.';
      console.error('[GENERATOR_IMAGE_MISSING_ERROR]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        asin: generatorInput.asin,
        reason: imageMissingReason
      });
    }

    const generatorContext = await buildGeneratorDealContext({
      asin: generatorInput.asin,
      sellerType: generatorInput.sellerType,
      sellerClass: generatorInput.sellerClass,
      soldByAmazon: generatorInput.soldByAmazon,
      shippedByAmazon: generatorInput.shippedByAmazon,
      sellerDetectionSource: generatorInput.sellerDetectionSource,
      sellerDetectionSources: generatorInput.sellerDetectionSources,
      sellerMatchedPatterns: generatorInput.sellerMatchedPatterns,
      sellerDetails: generatorInput.sellerDetails,
      sellerRawText: generatorInput.sellerRawText,
      dealType: generatorInput.dealType,
      isAmazonDeal: generatorInput.isAmazonDeal,
      currentPrice: generatorInput.currentPrice,
      title: generatorInput.title,
      productUrl: generatorInput.normalizedUrl || generatorInput.link,
      normalizedUrl: generatorInput.normalizedUrl,
      affiliateUrl: generatorInput.link,
      affiliateLink: generatorInput.link,
      imageUrl: generatorInput.generatedImagePath || generatorInput.uploadedImagePath,
      source: generatorInput.contextSource,
      origin: generatorInput.originOverride
    });
    if (readerConfig.readerTestMode === true && generatorContext?.seller) {
      const resolvedSeller = generatorContext.seller;
      const resolvedSellerDetails = resolvedSeller.details || {};
      const resolvedSellerClass = cleanText(resolvedSeller.sellerClass);

      if (resolvedSellerClass && resolvedSellerClass !== cleanText(generatorInput.sellerClass)) {
        generatorInput.sellerClass = resolvedSellerClass;
        generatorInput.sellerType = cleanText(resolvedSeller.sellerType) || generatorInput.sellerType;
        generatorInput.soldByAmazon = resolvedSeller.soldByAmazon;
        generatorInput.shippedByAmazon = resolvedSeller.shippedByAmazon;
        generatorInput.sellerDetectionSource = cleanText(resolvedSellerDetails.detectionSource) || generatorInput.sellerDetectionSource;
        generatorInput.sellerDetectionSources = Array.isArray(resolvedSellerDetails.detectionSources)
          ? resolvedSellerDetails.detectionSources
          : generatorInput.sellerDetectionSources;
        generatorInput.sellerDetails = {
          ...(generatorInput.sellerDetails && typeof generatorInput.sellerDetails === 'object' ? generatorInput.sellerDetails : {}),
          ...resolvedSellerDetails
        };
        console.info('[SELLER_TESTMODE_INPUT_SYNCED]', {
          asin: generatorInput.asin,
          sellerClass: generatorInput.sellerClass,
          sellerType: generatorInput.sellerType,
          sellerDetectionSource: generatorInput.sellerDetectionSource,
          reason: cleanText(resolvedSellerDetails.recognitionMessage || resolvedSellerDetails.sellerRecognitionMessage)
        });
      }
    }
    let similarProductCheck = null;
    const similarProductCheckPromise = runSimilarProductOptimizationCheck({
      sessionName,
      source,
      structuredMessage,
      generatorInput,
      generatorContext,
      scrapedDeal: normalizedScrapedDeal
    }).catch((error) => {
      const reason = error instanceof Error ? error.message : 'Similar Product Check fehlgeschlagen.';
      console.warn('[SIMILAR_PRODUCT_NO_CHEAPER_FOUND]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        asin: generatorInput.asin,
        reason
      });
      return buildEmptySimilarProductCheck({
        checked: false,
        allowed: false,
        reason
      });
    });
    if (generatorContext?.learning?.marketComparisonStarted === true) {
      console.info('[MARKET_CHECK_STARTED]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        dealType,
        asin: generatorInput.asin
      });
    }
    if (generatorContext?.learning?.aiCheckStarted === true) {
      console.info('[AI_CHECK_STARTED]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        dealType,
        asin: generatorInput.asin
      });
    }
    let normalDecision = evaluateTelegramReaderGeneratorCandidate(generatorContext, {
      ...readerConfig,
      readerDebugMode: false,
      readerTestMode: false
    });
    let readerDecision = evaluateTelegramReaderGeneratorCandidate(generatorContext, readerConfig);

    if (sourceMeta.matchTier === 'review' || sourceMeta.matchTier === 'debug' || productVerification.warningOnly === true) {
      const reviewOnlyDecision = {
        accepted: false,
        decision: 'review',
        reason:
          cleanText(sourceMeta.relaxedReason) ||
          cleanText(productVerification.reason) ||
          'Match wurde nur fuer Review freigegeben.'
      };

      normalDecision = reviewOnlyDecision;
      if (relaxedTestMode || sourceMeta.matchTier === 'review' || sourceMeta.matchTier === 'debug') {
        readerDecision = reviewOnlyDecision;
      }
      if (cleanText(sourceMeta.matchTier).toLowerCase() === 'review' || cleanText(sourceMeta.matchTier).toLowerCase() === 'debug') {
        console.info('[PRODUCT_MATCH_REVIEW]', {
          sessionName,
          sourceId: source.id,
          messageId: structuredMessage.messageId,
          asin: generatorInput.asin,
          matchScore: sourceMeta.matchScore,
          matchReason: sourceMeta.matchReason || null,
          reason: reviewOnlyDecision.reason
        });
      }
    }
    const requiredCheckBlock = resolveAmazonDirectRequiredCheckBlock({
      generatorInput,
      generatorContext
    });
    const fbmProfileBlock = resolveFbmSellerProfileReviewBlock({
      generatorInput,
      generatorContext,
      scrapedDeal: normalizedScrapedDeal
    });

    if (generatorInput.invalidPrice === true) {
      const invalidPriceDecision = {
        accepted: false,
        decision: 'review',
        reason: generatorInput.invalidPriceReason || 'Preis ist 0,00€ oder ungueltig.'
      };

      normalDecision = invalidPriceDecision;
      readerDecision = invalidPriceDecision;

      if (debugPostEnabled) {
        console.info('[TEST_POST_INVALID_PRICE_ONLY]', {
          sessionName,
          sourceId: source.id,
          messageId: structuredMessage.messageId,
          asin: generatorInput.asin,
          reason: invalidPriceDecision.reason
        });
        generatorInput.allowInvalidPriceTestPost = true;
      } else {
        console.error('[POST_BLOCKED_INVALID_PRICE]', {
          sessionName,
          sourceId: source.id,
          messageId: structuredMessage.messageId,
          asin: generatorInput.asin,
          reason: invalidPriceDecision.reason
        });
      }
    }

    if (requiredCheckBlock.blocked === true) {
      const requiredCheckDecision = {
        accepted: false,
        decision: 'review',
        reason: requiredCheckBlock.reason
      };

      normalDecision = requiredCheckDecision;
      readerDecision = requiredCheckDecision;

      console.error('[APPROVE_BLOCKED_REQUIRED_CHECK_MISSING]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        asin: generatorInput.asin,
        sellerClass: generatorInput.sellerClass,
        missingChecks: requiredCheckBlock.missingChecks,
        reason: requiredCheckBlock.reason
      });
    }

    if (fbmProfileBlock.blocked === true) {
      normalDecision = {
        accepted: false,
        decision: 'review',
        reason: fbmProfileBlock.reason
      };
      if (!debugPostEnabled) {
        readerDecision = normalDecision;
      }
    }

    let debugValues = null;

    if (debugPostEnabled) {
      similarProductCheck = await similarProductCheckPromise;
      generatorInput.similarProductCheck = similarProductCheck;
      debugValues = collectReaderDebugValues({
        sessionName,
        source,
        structuredMessage,
        amazonLink,
        pricing,
        scrapedDeal: normalizedScrapedDeal,
        linkRecord,
        generatorInput,
        generatorContext,
        readerConfig,
        readerDecision,
        normalDecision
      });
      generatorInput.debugInfoByChannel = {
        ...(generatorInput.debugInfoByChannel && typeof generatorInput.debugInfoByChannel === 'object'
          ? generatorInput.debugInfoByChannel
          : {}),
        telegram: buildTelegramDealDebugInfoExtended(debugValues),
        whatsapp: '',
        facebook: ''
      };
      generatorInput.testMode = true;
      console.info('[GENERATOR_STYLE_POST]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        asin: generatorInput.asin,
        hasImage: Boolean(generatorInput.generatedImagePath || generatorInput.uploadedImagePath),
        captionLength: cleanText(generatorInput.textByChannel.telegram).length
      });
      console.info('[GENERATOR_STYLE_POST_READY]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        asin: generatorInput.asin,
        hasImage: Boolean(generatorInput.generatedImagePath || generatorInput.uploadedImagePath),
        captionLength: cleanText(generatorInput.textByChannel.telegram).length,
        debugLength: cleanText(generatorInput.debugInfoByChannel?.telegram || '').length
      });
    }

    if (!debugPostEnabled) {
      console.info('[GENERATOR_STYLE_POST]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        asin: generatorInput.asin,
        hasImage: Boolean(generatorInput.generatedImagePath || generatorInput.uploadedImagePath),
        captionLength: cleanText(generatorInput.textByChannel.telegram).length
      });
      console.info('[GENERATOR_STYLE_POST_READY]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        asin: generatorInput.asin,
        hasImage: Boolean(generatorInput.generatedImagePath || generatorInput.uploadedImagePath),
        captionLength: cleanText(generatorInput.textByChannel.telegram).length
      });
    }

    const decisionLabel = resolveReaderDecisionLabel(generatorContext, normalDecision);

    console.info('[GENERATOR_FORMAT_SUCCESS]', {
      sessionName,
      sourceId: source.id,
      messageId: structuredMessage.messageId,
      asin: generatorInput.asin,
      title: generatorInput.title,
      affiliateUrl: generatorInput.link,
      hasImage: Boolean(generatorInput.generatedImagePath || generatorInput.uploadedImagePath),
      routingDecision: generatorContext?.learning?.routingDecision || '',
      readerDecision: readerDecision.decision,
      normalDecision: normalDecision.decision,
      readerTestMode: readerConfig.readerTestMode === true,
      readerDebugMode: readerConfig.readerDebugMode === true
    });
    console.info('[GENERATOR_POST_BUILT]', {
      sessionName,
      sourceId: source.id,
      messageId: structuredMessage.messageId,
      asin: generatorInput.asin,
      title: generatorInput.title,
      hasAffiliateLink: Boolean(cleanText(generatorInput.link)),
      hasTelegramText: Boolean(cleanText(generatorInput.textByChannel?.telegram)),
      hasImage: Boolean(generatorInput.generatedImagePath || generatorInput.uploadedImagePath)
    });
    console.info('[GENERATOR_FORCE_SUCCESS]', {
      sessionName,
      sourceId: source.id,
      messageId: structuredMessage.messageId,
      asin: generatorInput.asin,
      decision: decisionLabel,
      trigger
    });

    const forceTestGroupPost = options.forceTestGroupPost === true || isReaderTestGroupAllMode(readerConfig);
    let testGroupDeliveryMode = 'publisher_queue';

    if (forceTestGroupPost) {
      console.info('[TESTGROUP_POST_FORCED]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        asin: generatorInput.asin,
        sellerClass: generatorInput.sellerClass,
        normalDecision: normalDecision?.decision || 'review',
        wouldPostNormally: normalDecision?.accepted === true,
        reason:
          options.forceTestGroupPost === true
            ? 'Explizit fuer die Testgruppe erzwungen.'
            : 'Reader-Test/Debugmodus postet den Deal unabhaengig von Score, Fake-Risiko oder Seller-Freigabe in die Testgruppe.'
      });
    }
    if (testGroupModeActive) {
      console.info('[TEST_GROUP_POST_ENABLED]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        asin: generatorInput.asin,
        deliveryMode: testGroupDeliveryMode,
        liveAllowed: false,
        reason: 'READER_TEST_MODE erlaubt Posting in die Testgruppe; Live bleibt deaktiviert.'
      });
    }

    if (!readerDecision.accepted) {
      logReaderPipelineError(readerDecision.reason, {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        asin: generatorInput.asin,
        routingDecision: generatorContext?.learning?.routingDecision || '',
        readerDecision: readerDecision.decision
      });
      logTelegramReaderEvent({
        level: 'info',
        eventType: 'telegram.generator.review',
        sourceId: source.id,
        message: readerDecision.reason,
        payload: {
          sessionName,
          messageId: structuredMessage.messageId,
          group: structuredMessage.group,
          amazonLink,
          asin: generatorInput.asin,
          readerDecision: readerDecision.decision,
          readerTestMode: readerConfig.readerTestMode === true
        }
      });

      if (!forceTestGroupPost) {
        return {
          accepted: false,
          status: 'review',
          review: true,
          reason: readerDecision.reason,
          reasonCode: 'reader_rejected',
          decision: decisionLabel,
          queueId: null,
          queueStatus: '',
          messageId: null,
          postedToTestGroup: false,
          forcedToTestGroup: false,
          trigger
        };
      }
    }

    console.info('[GENERATOR_OUTPUT_READY]', {
      sessionName,
      sourceId: source.id,
      messageId: structuredMessage.messageId,
      asin: generatorInput.asin,
      normalizedUrl: generatorInput.normalizedUrl,
      imageSource: generatorInput.telegramImageSource,
      affiliateUrl: generatorInput.link
    });
    console.info('[PUBLISHER_FORCE_START]', {
      sessionName,
      sourceId: source.id,
      messageId: structuredMessage.messageId,
      asin: generatorInput.asin,
      decision: decisionLabel,
      trigger
    });
    console.info('[ROUTING_START]', {
      sessionName,
      sourceId: source.id,
      messageId: structuredMessage.messageId,
      asin: generatorInput.asin,
      decision: decisionLabel,
      routeStage: 'primary_test_group_and_secondary_routes'
    });
    console.info('[READER_USING_GENERATOR_PUBLISHER]', {
      sessionName,
      sourceId: source.id,
      messageId: structuredMessage.messageId,
      asin: generatorInput.asin,
      queueSourceType: generatorInput.queueSourceType || 'generator_direct',
      hasDebugInfo: Boolean(cleanText(generatorInput.debugInfoByChannel?.telegram || ''))
    });

    let publishResult;
    try {
      publishResult = await publishGeneratorPostDirect({
        ...generatorInput,
        generatorContext,
        skipDealLock: readerConfig.readerDebugMode === true || readerConfig.readerTestMode === true
      });
    } catch (publishError) {
      const publishErrorMessage =
        publishError instanceof Error ? publishError.message : 'Generator-Publisher konnte nicht ausgefuehrt werden.';
      if ((testGroupModeActive || forceTestGroupPost) && (isTestGroupPublisherDisabledError(publishError) || forceTestGroupPost)) {
        testGroupDeliveryMode = 'direct_test_group_fallback';
        console.warn('[PIPELINE_ERROR_CONTINUED]', {
          sessionName,
          sourceId: source.id,
          messageId: structuredMessage.messageId,
          asin: generatorInput.asin,
          stage: 'publisher_queue',
          fallback: testGroupDeliveryMode,
          reason: publishErrorMessage
        });
        console.info('[TEST_GROUP_POST_ENABLED]', {
          sessionName,
          sourceId: source.id,
          messageId: structuredMessage.messageId,
          asin: generatorInput.asin,
          deliveryMode: testGroupDeliveryMode,
          liveAllowed: false,
          reason: publishErrorMessage
        });
        try {
          publishResult = await sendReaderTestGroupPostDirect({
            sessionName,
            source,
            structuredMessage,
            generatorInput,
            trigger,
            fallbackReason: publishErrorMessage
          });
        } catch (fallbackError) {
          const fallbackErrorMessage =
            fallbackError instanceof Error ? fallbackError.message : 'Direktes Testgruppen-Posting ist fehlgeschlagen.';
          logNoPostReason('Telegram Send Fehler', {
            sessionName,
            sourceId: source.id,
            messageId: structuredMessage.messageId,
            detail: `${publishErrorMessage} | ${fallbackErrorMessage}`
          });
          console.error('[PUBLISHER_ERROR]', {
            sessionName,
            sourceId: source.id,
            messageId: structuredMessage.messageId,
            asin: generatorInput.asin,
            error: fallbackErrorMessage
          });
          logReaderPipelineError(fallbackErrorMessage, {
            sessionName,
            sourceId: source.id,
            messageId: structuredMessage.messageId,
            asin: generatorInput.asin
          });
          console.error('[PUBLISHER_FORCE_ERROR]', {
            sessionName,
            sourceId: source.id,
            messageId: structuredMessage.messageId,
            asin: generatorInput.asin,
            reason: fallbackErrorMessage,
            trigger
          });
          throw fallbackError;
        }
      } else {
        logNoPostReason('Telegram Send Fehler', {
          sessionName,
          sourceId: source.id,
          messageId: structuredMessage.messageId,
          detail: publishErrorMessage
        });
        console.error('[PUBLISHER_ERROR]', {
          sessionName,
          sourceId: source.id,
          messageId: structuredMessage.messageId,
          asin: generatorInput.asin,
          error: publishErrorMessage
        });
        logReaderPipelineError(publishErrorMessage, {
          sessionName,
          sourceId: source.id,
          messageId: structuredMessage.messageId,
          asin: generatorInput.asin
        });
        console.error('[PUBLISHER_FORCE_ERROR]', {
          sessionName,
          sourceId: source.id,
          messageId: structuredMessage.messageId,
          asin: generatorInput.asin,
          reason: publishErrorMessage,
          trigger
        });
        throw publishError;
      }
    }

    if (!publishResult?.queue?.id && testGroupDeliveryMode !== 'direct_test_group_fallback') {
      console.error('[QUEUE_ERROR]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        asin: generatorInput.asin,
        reason: 'Kein Queue-Eintrag vom Generator-Publisher zurueckgegeben.'
      });
      logReaderPipelineError('Generator-Publisher hat keinen Queue-Eintrag geliefert.', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        asin: generatorInput.asin
      });
    }
    if (!publishResult?.results?.telegram?.messageId) {
      const queueStatus = cleanText(publishResult?.queue?.status) || 'unknown';
      logNoPostReason('Telegram Send Fehler', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        detail: `Keine Telegram messageId geliefert (Queue-Status: ${queueStatus}).`
      });
      console.error('[PUBLISHER_ERROR]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        asin: generatorInput.asin,
        queueStatus,
        error: 'Publisher hat keinen Telegram messageId-Wert geliefert.'
      });
      logReaderPipelineError(`Publisher blieb ohne Telegram-Auslieferung (Queue-Status: ${queueStatus}).`, {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        asin: generatorInput.asin,
        queueStatus
      });
    }

    const postedMessageId = publishResult?.results?.telegram?.messageId || null;
    const queueId = publishResult?.queue?.id || null;
    const queueStatus = publishResult?.queue?.status || '';
    const debugMessageIds = Array.isArray(publishResult?.results?.telegram?.deliveries?.[0]?.extraMessageIds)
      ? publishResult.results.telegram.deliveries[0].extraMessageIds
      : [];
    const forcedRejectToTestGroup = forceTestGroupPost && ['REJECT', 'REVIEW'].includes(decisionLabel);

    if (forcedRejectToTestGroup && postedMessageId) {
      console.info('[DEBUG_REJECT_POSTED_TO_TESTGROUP]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        asin: generatorInput.asin,
        decision: decisionLabel,
        queueId,
        telegramMessageId: postedMessageId
      });
    }
    if (postedMessageId) {
      console.info('[GENERATOR_OUTPUT_SENT]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        asin: generatorInput.asin,
        queueId,
        telegramMessageId: postedMessageId
      });
      console.info('[PUBLISHER_FORCE_SUCCESS]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        asin: generatorInput.asin,
        decision: decisionLabel,
        queueId,
        telegramMessageId: postedMessageId,
        trigger
      });
      if (testGroupModeActive) {
        console.info('[TEST_GROUP_MESSAGE_SENT]', {
          sessionName,
          sourceId: source.id,
          messageId: structuredMessage.messageId,
          asin: generatorInput.asin,
          queueId,
          queueStatus,
          telegramMessageId: postedMessageId,
          deliveryMode: testGroupDeliveryMode,
          liveAllowed: false
        });
      }
      console.info('[ROUTING_SENT_TEST]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        asin: generatorInput.asin,
        queueId,
        telegramMessageId: postedMessageId,
        deliveryMode: testGroupDeliveryMode
      });
    }
    if (publishResult?.routingOutputs?.approved?.messageId) {
      console.info('[ROUTING_SENT_APPROVED]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        asin: generatorInput.asin,
        queueId: publishResult.routingOutputs.approved.queueId || null,
        telegramMessageId: publishResult.routingOutputs.approved.messageId
      });
    }
    if (publishResult?.routingOutputs?.rejected?.messageId) {
      console.info('[ROUTING_SENT_REJECTED]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        asin: generatorInput.asin,
        telegramMessageId: publishResult.routingOutputs.rejected.messageId
      });
    }
    if (debugMessageIds.length) {
      console.info('[TEST_DEBUG_SENT_AFTER_GENERATOR]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        asin: generatorInput.asin,
        queueId,
        mainTelegramMessageId: postedMessageId,
        debugMessageIds
      });
    }

    let optimizedChannelResult = { sent: false, reason: 'not_checked' };
    try {
      if (!similarProductCheck) {
        similarProductCheck = await similarProductCheckPromise;
        generatorInput.similarProductCheck = similarProductCheck;
      }
      console.info('[ROUTING_START]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        asin: generatorInput.asin,
        routeStage: 'optimized_channel',
        similarCheaperFound: similarProductCheck?.similarCheaperFound === true
      });
      optimizedChannelResult = await publishSimilarProductOptimizedChannel({
        sessionName,
        source,
        structuredMessage,
        generatorInput,
        similarCheck: similarProductCheck
      });
      if (optimizedChannelResult?.sent === true || optimizedChannelResult?.messageId) {
        console.info('[ROUTING_SENT_OPTIMIZED]', {
          sessionName,
          sourceId: source.id,
          messageId: structuredMessage.messageId,
          asin: generatorInput.asin,
          telegramMessageId: optimizedChannelResult?.messageId || null
        });
      }
    } catch (optimizedError) {
      optimizedChannelResult = {
        sent: false,
        status: 'error',
        reason: optimizedError instanceof Error ? optimizedError.message : 'Optimierte Deals konnten nicht verarbeitet werden.'
      };
      console.error('[PIPELINE_ERROR_CONTINUED]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        asin: generatorInput.asin,
        stage: 'optimized_channel',
        reason: optimizedChannelResult.reason
      });
    }

    return {
      accepted: true,
      status: 'sent',
      reason: readerDecision.reason,
      reasonCode: postedMessageId ? 'posted' : 'publisher_without_message_id',
      decision: decisionLabel,
      queueId,
      queueStatus,
      messageId: postedMessageId,
      postedToTestGroup: Boolean(postedMessageId),
      forcedToTestGroup: forceTestGroupPost,
      similarProductCheck,
      optimizedChannelResult,
      trigger
    };
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : 'Generator-Aufbereitung fehlgeschlagen.';

    console.error('[GENERATOR_FORMAT_ERROR]', {
      sessionName,
      sourceId: source.id,
      messageId: structuredMessage.messageId,
      amazonLink,
      error: errorMessage
    });
    console.error('[PIPELINE_ERROR_CONTINUED]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      amazonLink,
      stage: 'processTelegramReaderPipeline',
      reason: errorMessage
    });
    logReaderPipelineError(errorMessage, {
      sessionName,
      sourceId: source.id,
      messageId: structuredMessage.messageId,
      amazonLink
    });
    console.error('[GENERATOR_FORCE_ERROR]', {
      sessionName,
      sourceId: source.id,
      messageId: structuredMessage.messageId,
      amazonLink,
      reason: errorMessage,
      trigger
    });
    logTelegramReaderEvent({
      level: 'warning',
      eventType: 'telegram.generator.error',
      sourceId: source.id,
      message: errorMessage,
      payload: {
        sessionName,
        messageId: structuredMessage.messageId,
        group: structuredMessage.group,
        amazonLink
      }
    });

    if (readerConfig.allowRawReaderFallback === true) {
      console.info('[READER_OWN_TEMPLATE_DISABLED]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        reason: 'Raw Reader Fallback bleibt deaktiviert und wird nicht mehr gepostet.'
      });
    }

    if (readerConfig.readerDebugMode === true || readerConfig.readerTestMode === true) {
      try {
        return await handleBlockedReaderDiagnostic({
          sessionName,
          source,
          structuredMessage,
          readerConfig,
          trigger,
          blockedCode: 'PIPELINE_ERROR_CONTINUED',
          blockedReason: errorMessage,
          sourceHost: normalizeUrlHost(originalLink || amazonLink),
          forceDiagnosticPost: true,
          mainPostBlocked: true,
          titleSource: 'pipeline_error',
          imageSource: 'pipeline_error',
          affiliateLinkSource: 'pipeline_error'
        });
      } catch (diagnosticError) {
        console.error('[PIPELINE_ERROR_CONTINUED]', {
          sessionName,
          sourceId: source?.id ?? null,
          messageId: structuredMessage?.messageId || '',
          stage: 'diagnostic_fallback',
          reason: diagnosticError instanceof Error ? diagnosticError.message : 'Diagnosepost konnte nicht gesendet werden.'
        });
      }
    }

    return {
      accepted: false,
      status: 'error',
      review: true,
      reason: errorMessage,
      reasonCode: 'generator_format_error',
      decision: 'ERROR',
      queueId: null,
      queueStatus: '',
      messageId: null,
      postedToTestGroup: false,
      forcedToTestGroup: false,
      trigger
    };
  }
}

function sortTelegramMessagesAscending(messages = []) {
  return [...messages].sort((left, right) => Number(left?.id || 0) - Number(right?.id || 0));
}

function createReaderLoopSummary() {
  return {
    groupsScanned: 0,
    messagesChecked: 0,
    amazonLinksFound: 0,
    postedApprove: 0,
    postedReview: 0,
    postedReject: 0,
    skippedNoAmazon: 0,
    errors: 0
  };
}

function updateReaderLoopSummary(summary, pipelineResult = {}, hasAmazonLink = false) {
  if (!summary || typeof summary !== 'object') {
    return summary;
  }

  summary.messagesChecked += 1;
  if (hasAmazonLink) {
    summary.amazonLinksFound += 1;
  }

  if (pipelineResult?.reasonCode === 'missing_amazon_link') {
    summary.skippedNoAmazon += 1;
    return summary;
  }

  if (pipelineResult?.status === 'error') {
    summary.errors += 1;
    return summary;
  }

  if (pipelineResult?.postedToTestGroup === true) {
    if (pipelineResult?.decision === 'REJECT') {
      summary.postedReject += 1;
    } else if (pipelineResult?.decision === 'REVIEW') {
      summary.postedReview += 1;
    } else {
      summary.postedApprove += 1;
    }
  }

  return summary;
}

async function pollTelegramWatchedDialogs(sessionName, client, options = {}) {
  const normalizedSessionName = normalizeSessionName(sessionName);
  const active = activeClients.get(normalizedSessionName);
  const trigger = cleanText(options.trigger) || 'interval';

  if (!active?.client || active.client !== client) {
    console.info('[POLLING_ACTIVE]', {
      sessionName: normalizedSessionName,
      trigger,
      active: false,
      reason: 'inactive_client'
    });
    logNoPostReason('Reader nicht aktiv', {
      sessionName: normalizedSessionName,
      detail: 'Polling wurde aufgerufen, aber es gibt keinen aktiven Client.'
    });
    return {
      skipped: true,
      reason: 'inactive_client'
    };
  }

  if (active.pollingInFlight) {
    console.info('[POLLING_ACTIVE]', {
      sessionName: normalizedSessionName,
      trigger,
      active: true,
      reason: 'poll_in_flight'
    });
    return {
      skipped: true,
      reason: 'poll_in_flight'
    };
  }

  active.pollingInFlight = true;
  const watchedChannels = listWatchedChannels(normalizedSessionName).filter(
    (item) => item.isActive && cleanText(item.channelTitle) && cleanText(item.channelRef)
  );
  const pollStartedAt = nowIso();
  const polledDialogs = [];
  const loopSummary = createReaderLoopSummary();
  active.lastPollAt = pollStartedAt;
  active.lastPolledDialogs = [];

  console.info('[READER_HEARTBEAT]', {
    sessionName: normalizedSessionName,
    trigger,
    startedAt: pollStartedAt,
    pollingIntervalMs: TELEGRAM_POLL_INTERVAL_MS,
    listenerActive: active.listenerAttached === true,
    pollingActive: active.pollingActive === true
  });
  console.info('[READER_ALIVE]', {
    sessionName: normalizedSessionName,
    trigger,
    startedAt: pollStartedAt,
    listenerActive: active.listenerAttached === true,
    pollingActive: active.pollingActive === true,
    watchedChannels: watchedChannels.length
  });
  console.info('[WATCHLIST_COUNT]', {
    sessionName: normalizedSessionName,
    trigger,
    count: watchedChannels.length
  });
  console.info('[POLLING_ACTIVE]', {
    sessionName: normalizedSessionName,
    trigger,
    active: true,
    pollingIntervalMs: TELEGRAM_POLL_INTERVAL_MS
  });

  if (!watchedChannels.length) {
    logNoPostReason('Watchlist leer', {
      sessionName: normalizedSessionName,
      detail: 'Keine aktiven Watchlist-Kanäle für das Polling gefunden.'
    });
  }

  console.info('[READER_LOOP_START]', {
    trigger: 'polling',
    sessionName: normalizedSessionName,
    dialogCount: watchedChannels.length,
    startedAt: pollStartedAt
  });
  console.info('[TELEGRAM_POLL_TICK]', {
    sessionName: normalizedSessionName,
    trigger,
    dialogCount: watchedChannels.length,
    startedAt: pollStartedAt
  });
  console.info('[TELEGRAM_POLL_START]', {
    sessionName: normalizedSessionName,
    trigger,
    dialogCount: watchedChannels.length,
    intervalMs: TELEGRAM_POLL_INTERVAL_MS
  });

  try {
    for (const channel of watchedChannels) {
      const checkedAt = nowIso();
      loopSummary.groupsScanned += 1;
      const dialogPayload = {
        sessionName: normalizedSessionName,
        channelId: channel.id,
        channelRef: channel.channelRef,
        channelTitle: channel.channelTitle,
        lastSeenMessageId: channel.lastSeenMessageId || '',
        lastCheckedAt: channel.lastCheckedAt || null
      };

      console.info('[READER_GROUP_SCAN_START]', {
        trigger: 'polling',
        sessionName: normalizedSessionName,
        channelId: channel.id,
        channelRef: channel.channelRef,
        channelTitle: channel.channelTitle
      });
      console.info('[TELEGRAM_POLL_DIALOG]', dialogPayload);

      try {
        const entityRef = resolveDialogRef(channel.channelRef);
        const fetchedMessages = await client.getMessages(entityRef, {
          limit: TELEGRAM_POLL_MESSAGE_LIMIT
        });
        const orderedMessages = sortTelegramMessagesAscending(Array.from(fetchedMessages || []).filter(Boolean));
        const newestMessageId = Number(orderedMessages.at(-1)?.id || 0) || null;
        let latestSeenId = channel.lastSeenMessageId ? Number(channel.lastSeenMessageId) : 0;
        let latestSeenAt = channel.lastSeenMessageAt || null;
        let foundCount = 0;
        let newMessageCount = 0;
        let groupErrorCount = 0;
        let groupSkippedCount = 0;

        console.info('[TELEGRAM_POLL_MESSAGE_COUNT]', {
          sessionName: normalizedSessionName,
          channelId: channel.id,
          channelRef: channel.channelRef,
          fetchedCount: orderedMessages.length,
          newestMessageId,
          lastSeenMessageId: channel.lastSeenMessageId || ''
        });

        for (const message of orderedMessages) {
          const currentMessageId = Number(message?.id || 0);

          if (!currentMessageId || (latestSeenId && currentMessageId <= latestSeenId)) {
            continue;
          }

          const structuredMessage = {
            ...(await formatTelegramMessage(message, channel.channelTitle)),
            sessionName: normalizedSessionName
          };
          const amazonLink = findAmazonLinkInText(structuredMessage.text) || findAmazonLinkInText(structuredMessage.link);
          console.info('[READER_MESSAGE_EVALUATE]', {
            trigger: 'polling',
            sessionName: normalizedSessionName,
            channelId: channel.id,
            channelRef: channel.channelRef,
            messageId: structuredMessage.messageId,
            hasAmazonLink: Boolean(amazonLink)
          });

          console.info('[TELEGRAM_POLL_MESSAGE_FOUND]', {
            sessionName: normalizedSessionName,
            channelId: channel.id,
            channelRef: channel.channelRef,
            messageId: structuredMessage.messageId,
            timestamp: structuredMessage.timestamp,
            textPreview: structuredMessage.text.slice(0, TELEGRAM_RAW_EVENT_TEXT_LIMIT)
          });
          console.info('[TELEGRAM_POLL_AMAZON_LINK]', {
            sessionName: normalizedSessionName,
            channelId: channel.id,
            channelRef: channel.channelRef,
            hasAmazonLink: Boolean(amazonLink),
            amazonLink
          });

          try {
            const handledResult = await handleWatchedTelegramMessage(normalizedSessionName, message, {
              pipelineOptions: {
                trigger: 'polling'
              }
            });
            const pipelineResult = handledResult?.pipelineResult || null;
            updateReaderLoopSummary(loopSummary, pipelineResult, Boolean(amazonLink));

            if (pipelineResult?.reasonCode === 'missing_amazon_link') {
              groupSkippedCount += 1;
              console.info('[READER_MESSAGE_SKIPPED]', {
                trigger: 'polling',
                sessionName: normalizedSessionName,
                channelId: channel.id,
                channelRef: channel.channelRef,
                messageId: structuredMessage.messageId,
                reason: pipelineResult.reasonCode
              });
            } else if (pipelineResult?.status === 'error') {
              groupErrorCount += 1;
              console.error('[READER_MESSAGE_ERROR_CONTINUE]', {
                trigger: 'polling',
                sessionName: normalizedSessionName,
                channelId: channel.id,
                channelRef: channel.channelRef,
                messageId: structuredMessage.messageId,
                reason: pipelineResult.reason || 'Telegram Reader Pipeline fehlgeschlagen.'
              });
            }
          } catch (messageError) {
            groupErrorCount += 1;
            loopSummary.errors += 1;
            console.error('[READER_MESSAGE_ERROR_CONTINUE]', {
              trigger: 'polling',
              sessionName: normalizedSessionName,
              channelId: channel.id,
              channelRef: channel.channelRef,
              messageId: structuredMessage.messageId,
              reason: messageError instanceof Error ? messageError.message : 'Telegram Reader Nachricht konnte nicht verarbeitet werden.'
            });
            continue;
          }

          console.info('[TELEGRAM_POLL_PIPELINE_SENT]', {
            sessionName: normalizedSessionName,
            channelId: channel.id,
            channelRef: channel.channelRef,
            messageId: structuredMessage.messageId
          });

          latestSeenId = Math.max(latestSeenId, currentMessageId);
          latestSeenAt = structuredMessage.timestamp;
          foundCount += 1;
          newMessageCount += 1;
          active.lastFoundMessageAt = structuredMessage.timestamp;
          active.lastFoundMessagePreview = structuredMessage.text.slice(0, TELEGRAM_RAW_EVENT_TEXT_LIMIT);
        }

        console.info('[TELEGRAM_POLL_NEW_MESSAGES_COUNT]', {
          sessionName: normalizedSessionName,
          channelId: channel.id,
          channelRef: channel.channelRef,
          newMessagesCount: newMessageCount,
          newestMessageId,
          lastSeenMessageId: channel.lastSeenMessageId || ''
        });
        if (newMessageCount === 0) {
          console.info('[TELEGRAM_POLL_NO_NEW_MESSAGES]', {
            sessionName: normalizedSessionName,
            channelId: channel.id,
            channelRef: channel.channelRef,
            lastSeenMessageId: channel.lastSeenMessageId || '',
            newestMessageId
          });
        }

        updateChannelLastChecked(channel.id, checkedAt);
        if (latestSeenId) {
          updateChannelCheckpoint(channel.id, latestSeenId, latestSeenAt);
        }
        const dialogResult = {
          channelId: channel.id,
          channelRef: channel.channelRef,
          latestSeenMessageId: latestSeenId ? String(latestSeenId) : channel.lastSeenMessageId || '',
          foundCount,
          skippedCount: groupSkippedCount,
          errorCount: groupErrorCount,
          lastCheckedAt: checkedAt
        };
        polledDialogs.push(dialogResult);
        active.lastPolledDialogs = polledDialogs.slice(0, MAX_DIALOGS);
        console.info('[READER_GROUP_SCAN_DONE]', {
          trigger: 'polling',
          sessionName: normalizedSessionName,
          channelId: channel.id,
          channelRef: channel.channelRef,
          foundCount,
          skippedCount: groupSkippedCount,
          errorCount: groupErrorCount,
          newestMessageId,
          latestSeenMessageId: latestSeenId ? String(latestSeenId) : channel.lastSeenMessageId || ''
        });

        if (latestSeenId) {
          const sessionRow = getSessionRowByName(normalizedSessionName);
          if (sessionRow) {
            upsertSessionRow({
              name: normalizedSessionName,
              loginMode: sessionRow.login_mode,
              phoneNumber: sessionRow.phone_number || '',
              sessionPath: sessionRow.session_path,
              status: getSessionRuntimeStatus(normalizedSessionName),
              lastMessageAt: latestSeenAt || null,
              lastError: ''
            });
          }
        }
      } catch (error) {
        updateChannelLastChecked(channel.id, checkedAt);
        const lastError = error instanceof Error ? error.message : 'Telegram Polling fehlgeschlagen.';

        console.error('[TELEGRAM_POLL_ERROR]', {
          sessionName: normalizedSessionName,
          channelId: channel.id,
          channelRef: channel.channelRef,
          error: lastError
        });

        const dialogResult = {
          channelId: channel.id,
          channelRef: channel.channelRef,
          foundCount: 0,
          lastCheckedAt: checkedAt,
          error: lastError
        };
        polledDialogs.push(dialogResult);
        active.lastPolledDialogs = polledDialogs.slice(0, MAX_DIALOGS);

        const sessionRow = getSessionRowByName(normalizedSessionName);
        if (sessionRow) {
          upsertSessionRow({
            name: normalizedSessionName,
            loginMode: sessionRow.login_mode,
            phoneNumber: sessionRow.phone_number || '',
            sessionPath: sessionRow.session_path,
            status: getSessionRuntimeStatus(normalizedSessionName),
            lastError
          });
        }
      }
    }

    active.lastPollAt = pollStartedAt;
    active.lastPolledDialogs = polledDialogs.slice(0, MAX_DIALOGS);

    const postedCount =
      Number(loopSummary.postedApprove || 0) + Number(loopSummary.postedReview || 0) + Number(loopSummary.postedReject || 0);

    if (watchedChannels.length > 0 && loopSummary.messagesChecked === 0 && loopSummary.errors === 0) {
      logNoPostReason('keine neuen Nachrichten', {
        sessionName: normalizedSessionName,
        detail: 'Polling hat keine neuen Nachrichten oberhalb der Last-Seen-Marke gefunden.',
        watchlistCount: watchedChannels.length
      });
    } else if (loopSummary.messagesChecked > 0 && postedCount === 0 && loopSummary.errors === 0) {
      logNoPostReason('alles durch Block-Regel gestoppt', {
        sessionName: normalizedSessionName,
        detail: 'Nachrichten wurden geprüft, aber es wurde nichts in die Testgruppe gesendet.',
        messagesChecked: loopSummary.messagesChecked
      });
    }

    return {
      skipped: false,
      summary: loopSummary,
      polledDialogs
    };
  } finally {
    console.info('[READER_LOOP_DONE]', {
      trigger: 'polling',
      sessionName: normalizedSessionName,
      ...loopSummary
    });
    active.pollingInFlight = false;
  }
}

async function ensureSessionPolling(sessionName, client) {
  const normalizedSessionName = normalizeSessionName(sessionName);
  const active = activeClients.get(normalizedSessionName);

  if (!active?.client || active.client !== client) {
    return null;
  }

  if (active.pollingIntervalId) {
    active.pollingActive = true;
    active.pollingIntervalMs = TELEGRAM_POLL_INTERVAL_MS;
    return active;
  }

  active.pollingIntervalId = setInterval(() => {
    void pollTelegramWatchedDialogs(normalizedSessionName, client, { trigger: 'interval' });
  }, TELEGRAM_POLL_INTERVAL_MS);
  active.pollingActive = true;
  active.pollingIntervalMs = TELEGRAM_POLL_INTERVAL_MS;

  void pollTelegramWatchedDialogs(normalizedSessionName, client, { trigger: 'startup' });

  return active;
}

function createStructuredMessageKey(item = {}) {
  if (item.sessionName || item.chatId || item.messageId) {
    return [item.sessionName || '', item.chatId || '', item.messageId || ''].join(':');
  }

  return [item.group || '', item.timestamp || '', item.link || '', item.text || ''].join(':');
}

function mergeStructuredMessages(...groups) {
  const merged = [];
  const seen = new Set();

  for (const group of groups) {
    for (const item of group || []) {
      const key = createStructuredMessageKey(item);

      if (seen.has(key)) {
        continue;
      }

      seen.add(key);
      merged.push(item);
    }
  }

  return merged;
}

async function handleWatchedTelegramMessage(sessionName, message, options = {}) {
  const normalizedSessionName = normalizeSessionName(sessionName);
  const watchedLookup = buildWatchedChannelLookup(normalizedSessionName);
  const bypassWatchlist = TELEGRAM_DIAGNOSTIC_DISABLE_WATCHLIST === true;

  if (!message?.id) {
    return null;
  }

  let chat = null;

  try {
    chat = await message.getChat();
  } catch {
    chat = null;
  }

  const matchKeys = buildMessageMatchKeys(message, chat);
  const matchedChannel = matchKeys.map((key) => watchedLookup.get(key)).find(Boolean) || null;
  const fallbackChannel = {
    id: null,
    channelRef: matchKeys[0] || '',
    channelTitle: cleanText(chat?.title) || cleanText(chat?.username) || 'Telegram',
    channelType: 'group',
    isActive: true
  };
  const effectiveChannel = matchedChannel || (bypassWatchlist ? fallbackChannel : null);

  if (!effectiveChannel) {
    return null;
  }

  const source = matchedChannel ? upsertTelegramReaderSource(normalizedSessionName, matchedChannel) : null;
  const structuredMessage = {
    ...(await formatTelegramMessage(message, effectiveChannel.channelTitle)),
    sessionName: normalizedSessionName
  };

  if (matchedChannel?.id) {
    updateChannelCheckpoint(matchedChannel.id, message.id, structuredMessage.timestamp);
  }
  appendRecentMessage(normalizedSessionName, structuredMessage);

  const sessionRow = getSessionRowByName(normalizedSessionName);
  if (sessionRow) {
    upsertSessionRow({
      name: normalizedSessionName,
      loginMode: sessionRow.login_mode,
      phoneNumber: sessionRow.phone_number || '',
      sessionPath: sessionRow.session_path,
      status: getSessionRuntimeStatus(normalizedSessionName),
      lastMessageAt: structuredMessage.timestamp,
      lastError: ''
    });
  }

  const logPayload = {
    sessionName: normalizedSessionName,
    group: structuredMessage.group,
    messageId: structuredMessage.messageId,
    chatId: structuredMessage.chatId,
    sourceId: source?.id || null,
    watchlistBypassed: bypassWatchlist && !matchedChannel
  };

  logTelegramRuntime('Telegram event received', logPayload);
  console.info('[TELEGRAM_EVENT_RECEIVED]', logPayload);
  logTelegramRuntime('Telegram message received', logPayload);
  logTelegramReaderEvent({
    eventType: 'telegram.message.received',
    sourceId: source?.id || null,
    message: 'Telegram message received',
    payload: {
      ...logPayload,
      link: structuredMessage.link,
      text: structuredMessage.text
    }
  });

  let pipelineResult = null;

  try {
    pipelineResult = await processTelegramReaderPipeline(normalizedSessionName, source, structuredMessage, options.pipelineOptions || {});
  } catch (error) {
    const messageText = error instanceof Error ? error.message : 'Telegram Reader Pipeline fehlgeschlagen.';

    console.error('[TELEGRAM_POLL_ERROR]', {
      sessionName: normalizedSessionName,
      sourceId: source?.id || null,
      group: structuredMessage.group,
      messageId: structuredMessage.messageId,
      error: messageText
    });
    logTelegramReaderEvent({
      level: 'warning',
      eventType: 'telegram.pipeline.error',
      sourceId: source?.id || null,
      message: messageText,
      payload: {
        sessionName: normalizedSessionName,
        group: structuredMessage.group,
        messageId: structuredMessage.messageId
      }
    });

    pipelineResult = {
      accepted: false,
      status: 'error',
      reason: messageText,
      reasonCode: 'pipeline_error',
      decision: 'ERROR',
      queueId: null,
      queueStatus: '',
      messageId: null,
      postedToTestGroup: false,
      forcedToTestGroup: false
    };
  }

  return {
    structuredMessage,
    source,
    matchedChannel,
    effectiveChannel,
    pipelineResult
  };
}

async function ensureSessionListener(sessionName, client) {
  const normalizedSessionName = normalizeSessionName(sessionName);
  const active = activeClients.get(normalizedSessionName);

  if (!active?.client || active.client !== client) {
    return null;
  }

  const hasMessageListener = active.listenerAttached === true && active.listenerHandler && active.listenerEvent;
  const hasRawListener = active.rawListenerAttached === true && active.rawListenerHandler;

  if (hasMessageListener && hasRawListener) {
    console.log('[TELEGRAM_STATUS]', {
      connected: Boolean(client.connected),
      listenerActive: true,
      watchlistCount: listActiveWatchedChannels(normalizedSessionName).length,
      newMessageHandler: true,
      incomingFilter: true
    });
    console.log('[TELEGRAM_LISTENER_READY] no chat filter active', {
      sessionName: normalizedSessionName,
      incomingFilter: true
    });
    return active;
  }

  if (!hasRawListener) {
    const rawListenerHandler = async (event) => {
      logTelegramRawEvent(event);
      await extractAndProcessTelegramMessage(normalizedSessionName, event, client, {
        logPrefix: 'TELEGRAM_NEW_MESSAGE_RAW'
      });
    };

    client.addEventHandler(rawListenerHandler);
    active.rawListenerAttached = true;
    active.rawListenerHandler = rawListenerHandler;
  }

  if (!hasMessageListener) {
    const listenerEvent = new NewMessage({
      incoming: true
    });
    const listenerHandler = async (event) => {
      try {
        await extractAndProcessTelegramMessage(normalizedSessionName, event, client, {
          logPrefix: 'TELEGRAM_NEW_MESSAGE_RAW'
        });
      } catch (error) {
        const sessionRow = getSessionRowByName(normalizedSessionName);
        const lastError = error instanceof Error ? error.message : 'Telegram Event konnte nicht verarbeitet werden.';

        if (sessionRow) {
          upsertSessionRow({
            name: normalizedSessionName,
            loginMode: sessionRow.login_mode,
            phoneNumber: sessionRow.phone_number || '',
            sessionPath: sessionRow.session_path,
            status: sessionRow.status || 'connected',
            lastError
          });
        }

        logTelegramRuntime('Telegram listener error', {
          sessionName: normalizedSessionName,
          error: lastError
        });
        logTelegramReaderEvent({
          level: 'warning',
          eventType: 'telegram.listener.error',
          message: lastError,
          payload: {
            sessionName: normalizedSessionName
          }
        });
      }
    };

    client.addEventHandler(listenerHandler, listenerEvent);
    active.listenerAttached = true;
    active.listenerHandler = listenerHandler;
    active.listenerEvent = listenerEvent;
    active.newMessageHandlerRegistered = true;
  }

  active.listenerStartedAt = nowIso();
  active.listenerStatus = 'active';
  active.listenerWatchCount = listActiveWatchedChannels(normalizedSessionName).length;

  const sessionRow = getSessionRowByName(normalizedSessionName);
  if (sessionRow) {
    upsertSessionRow({
      name: normalizedSessionName,
      loginMode: sessionRow.login_mode,
      phoneNumber: sessionRow.phone_number || '',
      sessionPath: sessionRow.session_path,
      status: getSessionRuntimeStatus(normalizedSessionName),
      lastError: ''
    });
  }

  if (!hasMessageListener) {
    logTelegramRuntime('Telegram listener started', {
      sessionName: normalizedSessionName,
      watchedChannels: active.listenerWatchCount
    });
    console.log('[TELEGRAM_LISTENER_READY] no chat filter active', {
      sessionName: normalizedSessionName,
      incomingFilter: true
    });
  }
  console.log('[TELEGRAM_STATUS]', {
    connected: Boolean(client.connected),
    listenerActive: true,
    watchlistCount: active.listenerWatchCount,
    newMessageHandler: true,
    incomingFilter: true
  });
  if (!hasMessageListener) {
    logTelegramReaderEvent({
      eventType: 'telegram.listener.started',
      message: 'Telegram listener started',
      payload: {
        sessionName: normalizedSessionName,
        watchedChannels: active.listenerWatchCount
      }
    });
  }

  return active;
}

function ensureChannelRow(sessionName, input = {}) {
  const session = upsertSessionRow({
    name: sessionName,
    loginMode: input.loginMode || 'phone',
    phoneNumber: input.phoneNumber || ''
  });
  const channelRef = normalizeConfiguredChannelRef(input.channelRef);
  const existing = db
    .prepare(`SELECT * FROM telegram_reader_channels WHERE session_id = ? AND channel_ref = ? LIMIT 1`)
    .get(session.id, channelRef);

  if (existing) {
    db.prepare(
      `
        UPDATE telegram_reader_channels
        SET channel_title = @channelTitle,
            channel_type = @channelType,
            is_active = @isActive,
            updated_at = @updatedAt
        WHERE id = @id
      `
    ).run({
      id: existing.id,
      channelTitle: cleanText(input.channelTitle) || existing.channel_title || channelRef,
      channelType: cleanText(input.channelType) || existing.channel_type || 'group',
      isActive: input.isActive === false ? 0 : 1,
      updatedAt: nowIso()
    });
  } else {
    const timestamp = nowIso();
    db.prepare(
      `
        INSERT INTO telegram_reader_channels (
          session_id,
          channel_ref,
          channel_title,
          channel_type,
          is_active,
          last_seen_message_id,
          last_seen_message_at,
          created_at,
          updated_at
        ) VALUES (
          @sessionId,
          @channelRef,
          @channelTitle,
          @channelType,
          @isActive,
          NULL,
          NULL,
          @createdAt,
          @updatedAt
        )
      `
    ).run({
      sessionId: session.id,
      channelRef,
      channelTitle: cleanText(input.channelTitle) || channelRef,
      channelType: cleanText(input.channelType) || 'group',
      isActive: input.isActive === false ? 0 : 1,
      createdAt: timestamp,
      updatedAt: timestamp
    });
  }

  const savedChannel = listWatchedChannels(sessionName).find((item) => item.channelRef === channelRef) || null;
  syncTelegramSourcesForSession(sessionName);
  return savedChannel;
}

function removeChannelRow(channelId) {
  const row =
    db
      .prepare(
        `
          SELECT c.*, s.name AS session_name
          FROM telegram_reader_channels c
          LEFT JOIN telegram_reader_sessions s ON s.id = c.session_id
          WHERE c.id = ?
          LIMIT 1
        `
      )
      .get(Number(channelId)) || null;

  db.prepare(`DELETE FROM telegram_reader_channels WHERE id = ?`).run(Number(channelId));
  return row;
}

function buildReaderGroupSlotStatus(item = {}) {
  if (!cleanText(item.name)) {
    return 'leer';
  }

  if (!item.enabled) {
    return 'inaktiv';
  }

  if (!cleanText(item.username)) {
    return 'unvollstaendig';
  }

  return 'aktiv';
}

function listReaderGroupSlotRows(sessionName = '') {
  const resolvedSessionName = resolveReaderSessionName(sessionName);

  return db
    .prepare(
      `
        SELECT c.*, s.name AS session_name
        FROM telegram_reader_channels c
        LEFT JOIN telegram_reader_sessions s ON s.id = c.session_id
        WHERE s.name = ?
          AND c.slot_index IS NOT NULL
        ORDER BY c.slot_index ASC, c.id ASC
      `
    )
    .all(resolvedSessionName)
    .map((row) => ({
      id: row.id,
      sessionName: row.session_name || resolvedSessionName,
      slotIndex: Number(row.slot_index || 0),
      name: row.channel_title || '',
      username: row.channel_ref || '',
      enabled: row.is_active === 1
    }));
}

function mapReaderGroupSlotsPayload(sessionName = '', slotCount = MIN_READER_GROUP_SLOTS) {
  const resolvedSessionName = resolveReaderSessionName(sessionName);
  const resolvedSlotCount = clampReaderGroupSlotCount(slotCount);
  const existingRows = new Map(listReaderGroupSlotRows(resolvedSessionName).map((item) => [item.slotIndex, item]));
  const activeSession = getPreferredReaderSessionRow();
  const sessionRow = getSessionRowByName(resolvedSessionName);

  return {
    sessionName: resolvedSessionName,
    slotCount: resolvedSlotCount,
    maxSlots: MAX_READER_GROUP_SLOTS,
    stats: {
      activeCount: Array.from(existingRows.values()).filter(
        (item) => item.enabled && cleanText(item.name) && cleanText(item.username)
      ).length,
      configuredCount: Array.from(existingRows.values()).filter((item) => cleanText(item.name)).length,
      visibleSlots: resolvedSlotCount,
      activeSessionName: activeSession?.name || '',
      sessionStatus: sessionRow?.status || activeSession?.status || 'disconnected'
    },
    items: Array.from({ length: resolvedSlotCount }, (_, index) => {
      const slotIndex = index + 1;
      const existing = existingRows.get(slotIndex) || null;
      const item = {
        id: existing?.id || null,
        slotIndex,
        name: existing?.name || '',
        username: existing?.username || '',
        enabled: existing?.enabled === true
      };

      return {
        ...item,
        status: buildReaderGroupSlotStatus(item)
      };
    })
  };
}

function upsertReaderGroupSlot(sessionName, item = {}) {
  const resolvedSessionName = resolveReaderSessionName(sessionName);
  const session = upsertSessionRow({
    name: resolvedSessionName,
    loginMode: getSessionRowByName(resolvedSessionName)?.login_mode || 'phone',
    phoneNumber: getSessionRowByName(resolvedSessionName)?.phone_number || ''
  });
  const slotIndex = Number(item.slotIndex);
  const name = cleanText(item.name);
  const configuredRef = normalizeConfiguredChannelRef(item.username || item.link || '');
  const channelRef = configuredRef;
  const existing =
    db.prepare(`SELECT * FROM telegram_reader_channels WHERE session_id = ? AND slot_index = ? LIMIT 1`).get(session.id, slotIndex) ||
    null;
  const duplicateByRef =
    channelRef
      ? db
          .prepare(`SELECT * FROM telegram_reader_channels WHERE session_id = ? AND channel_ref = ? LIMIT 1`)
          .get(session.id, channelRef) || null
      : null;

  if (!name) {
    if (existing) {
      db.prepare(`DELETE FROM telegram_reader_channels WHERE id = ?`).run(existing.id);
    }
    return null;
  }

  const timestamp = nowIso();
  let targetRow = existing;

  if (duplicateByRef && (!targetRow || duplicateByRef.id !== targetRow.id)) {
    if (targetRow && targetRow.id !== duplicateByRef.id) {
      db.prepare(`DELETE FROM telegram_reader_channels WHERE id = ?`).run(targetRow.id);
    }
    targetRow = duplicateByRef;
  }

  if (targetRow) {
    db.prepare(
      `
        UPDATE telegram_reader_channels
        SET channel_ref = @channelRef,
            channel_title = @channelTitle,
            channel_type = 'group',
            is_active = @isActive,
            slot_index = @slotIndex,
            last_seen_message_id = NULL,
            last_seen_message_at = NULL,
            updated_at = @updatedAt
        WHERE id = @id
      `
    ).run({
      id: targetRow.id,
      channelRef: channelRef || '',
      channelTitle: name,
      isActive: item.enabled === true ? 1 : 0,
      slotIndex,
      updatedAt: timestamp
    });
  } else {
    db.prepare(
      `
        INSERT INTO telegram_reader_channels (
          session_id,
          slot_index,
          channel_ref,
          channel_title,
          channel_type,
          is_active,
          last_seen_message_id,
          last_seen_message_at,
          created_at,
          updated_at
        ) VALUES (
          @sessionId,
          @slotIndex,
          @channelRef,
          @channelTitle,
          'group',
          @isActive,
          NULL,
          NULL,
          @createdAt,
          @updatedAt
        )
      `
    ).run({
      sessionId: session.id,
      slotIndex,
      channelRef: channelRef || '',
      channelTitle: name,
      isActive: item.enabled === true ? 1 : 0,
      createdAt: timestamp,
      updatedAt: timestamp
    });
  }

  return true;
}

export function getTelegramReaderGroupConfig(input = {}) {
  const resolvedSessionName = resolveReaderSessionName(input.sessionName);
  const slotCount = getReaderGroupSlotCount();
  syncTelegramSourcesForSession(resolvedSessionName);
  return mapReaderGroupSlotsPayload(resolvedSessionName, slotCount);
}

export function saveTelegramReaderGroupConfig(input = {}) {
  const resolvedSessionName = resolveReaderSessionName(input.sessionName);
  const slotCount = saveReaderGroupSlotCount(input.slotCount);
  const items = Array.isArray(input.items) ? input.items : [];
  const desiredSlotIndexes = new Set();

  for (const item of items) {
    const slotIndex = Number(item?.slotIndex || 0);
    if (!Number.isFinite(slotIndex) || slotIndex < 1 || slotIndex > slotCount) {
      continue;
    }

    desiredSlotIndexes.add(slotIndex);
    upsertReaderGroupSlot(resolvedSessionName, item);
  }

  const existingRows = listReaderGroupSlotRows(resolvedSessionName);
  for (const row of existingRows) {
    if (row.slotIndex > slotCount || !desiredSlotIndexes.has(row.slotIndex)) {
      db.prepare(`DELETE FROM telegram_reader_channels WHERE id = ?`).run(row.id);
    }
  }

  syncTelegramSourcesForSession(resolvedSessionName);
  return mapReaderGroupSlotsPayload(resolvedSessionName, slotCount);
}

async function listTelegramDialogSummaries(sessionName, limit = MAX_DIALOGS) {
  const normalizedSessionName = normalizeSessionName(sessionName);
  const client = await ensureAuthorizedClient(normalizedSessionName);
  const dialogs = await client.getDialogs({
    limit: Math.min(MAX_DIALOGS, Math.max(1, Number(limit) || MAX_DIALOGS))
  });

  return dialogs
    .filter((dialog) => Boolean(dialog?.isChannel || dialog?.isGroup))
    .map((dialog) => {
      const username = cleanText(dialog?.entity?.username);
      const channelRef = username ? `@${username}` : String(dialog.id);
      const dialogIdCandidates = buildTelegramDialogIdCandidates(dialog);
      const matchKeys = new Set([
        normalizeChannelMatchKey(channelRef),
        ...dialogIdCandidates.map((value) => normalizeChannelMatchKey(value))
      ]);

      return {
        id: String(dialog.id),
        entityId: cleanText(dialog?.entity?.id ? String(dialog.entity.id) : ''),
        dialogIdCandidates,
        channelRef,
        title: cleanText(dialog.title) || username || channelRef,
        type: dialog?.isChannel ? 'channel' : 'group',
        username,
        matchKeys: Array.from(matchKeys).filter(Boolean)
      };
    });
}

async function getTelegramWatchedDialogDiagnostics(sessionName) {
  const watchedChannels = listWatchedChannels(sessionName).filter((item) => item.isActive && cleanText(item.channelRef));
  const dialogs = await listTelegramDialogSummaries(sessionName, MAX_DIALOGS);

  return watchedChannels.map((channel) => {
    const storedRef = cleanText(channel.channelRef);
    const storedKey = normalizeChannelMatchKey(storedRef);
    const matchedDialog = dialogs.find((dialog) => dialog.matchKeys.includes(storedKey)) || null;

    return {
      channelId: channel.id,
      storedRef,
      storedKey,
      matched: Boolean(matchedDialog),
      dialogId: matchedDialog?.id || null,
      entityId: matchedDialog?.entityId || null,
      dialogIdCandidates: matchedDialog?.dialogIdCandidates || [],
      title: matchedDialog?.title || cleanText(channel.channelTitle),
      type: matchedDialog?.type || cleanText(channel.channelType),
      username: matchedDialog?.username || '',
      accountInDialog: Boolean(matchedDialog)
    };
  });
}

export async function getTelegramUserClientStatus() {
  const config = getReaderConfig();
  const rawSessions = listSessionRows();
  rawSessions.forEach((session) => syncTelegramSourcesForSession(session.name));
  const watchedDialogIdsBySession = new Map(
    await Promise.all(
      rawSessions.map(async (session) => {
        try {
          return [session.name, await getTelegramWatchedDialogDiagnostics(session.name)];
        } catch (error) {
          return [
            session.name,
            [
              {
                matched: false,
                error: error instanceof Error ? error.message : 'Dialog-Abgleich fehlgeschlagen.'
              }
            ]
          ];
        }
      })
    )
  );
  const sessions = rawSessions.map((session) => {
    const active = activeClients.get(session.name);
    return {
      ...session,
      listenerActive: active?.listenerAttached === true,
      newMessageHandlerRegistered: active?.newMessageHandlerRegistered === true || active?.listenerAttached === true,
      pollingActive: active?.pollingActive === true,
      pollingIntervalMs: Number(active?.pollingIntervalMs || TELEGRAM_POLL_INTERVAL_MS),
      listenerStartedAt: active?.listenerStartedAt || null,
      lastNewMessageAt: active?.lastNewMessageAt || null,
      lastPollAt: active?.lastPollAt || null,
      lastPolledDialogs: Array.isArray(active?.lastPolledDialogs) ? active.lastPolledDialogs : [],
      lastFoundMessageAt: active?.lastFoundMessageAt || null,
      lastFoundMessagePreview: active?.lastFoundMessagePreview || '',
      bufferedMessages: Array.isArray(active?.recentMessages) ? active.recentMessages.length : 0,
      watchedDialogIds: watchedDialogIdsBySession.get(session.name) || []
    };
  });
  const channels = listWatchedChannels();
  const pendingLogins = sessions.map((session) => buildPendingAuthSummary(session.name)).filter(Boolean);
  const activeSourceCount =
    db.prepare(`SELECT COUNT(*) AS count FROM sources WHERE platform = 'telegram' AND is_active = 1`).get()?.count || 0;
  const preferredSessionName = resolveReaderSessionName();
  const preferredActive = activeClients.get(preferredSessionName);

  return {
    configured: Boolean(config.apiId && config.apiHash),
    enabled: config.enabled === true,
    loginMode: config.loginMode,
    sessionDir: config.sessionDir,
    listenerActive: preferredActive?.listenerAttached === true,
    newMessageHandlerRegistered:
      preferredActive?.newMessageHandlerRegistered === true || preferredActive?.listenerAttached === true,
    pollingActive: preferredActive?.pollingActive === true,
    pollingIntervalMs: Number(preferredActive?.pollingIntervalMs || TELEGRAM_POLL_INTERVAL_MS),
    lastNewMessageAt: preferredActive?.lastNewMessageAt || null,
    lastPollAt: preferredActive?.lastPollAt || null,
    lastPolledDialogs: Array.isArray(preferredActive?.lastPolledDialogs) ? preferredActive.lastPolledDialogs : [],
    lastFoundMessageAt: preferredActive?.lastFoundMessageAt || null,
    lastFoundMessagePreview: preferredActive?.lastFoundMessagePreview || '',
    watchedDialogIds: watchedDialogIdsBySession.get(preferredSessionName) || [],
    sessions,
    channels,
    pendingLogins,
    listenerSessions: sessions.filter((session) => session.listenerActive).length,
    activeSourceCount: Number(activeSourceCount || 0)
  };
}

export async function startTelegramUserReaderRuntime() {
  const sessions = listSessionRows();
  console.info('[ENV_FLAGS_LOADED]', buildReaderRuntimeFlagSnapshot());

  sessions.forEach((session) => syncTelegramSourcesForSession(session.name));

  const config = getReaderConfig();
  if (!config.apiId || !config.apiHash || !config.sessionDir) {
    logNoPostReason('Reader nicht aktiv', {
      detail: 'Telegram Reader ist nicht vollständig konfiguriert.',
      apiConfigured: Boolean(config.apiId && config.apiHash),
      sessionDirConfigured: Boolean(config.sessionDir)
    });
    return {
      startedSessions: 0,
      skipped: true
    };
  }

  let startedSessions = 0;

  for (const session of sessions) {
    if (!['connected', 'active', 'watching'].includes(session.status)) {
      continue;
    }

    try {
      await ensureAuthorizedClient(session.name);
      startedSessions += 1;
    } catch (error) {
      const sessionRow = getSessionRowByName(session.name);
      const lastError = error instanceof Error ? error.message : 'Telegram Runtime konnte nicht gestartet werden.';

      if (sessionRow) {
        upsertSessionRow({
          name: session.name,
          loginMode: sessionRow.login_mode,
          phoneNumber: sessionRow.phone_number || '',
          sessionPath: sessionRow.session_path,
          status: sessionRow.status || 'disconnected',
          lastError
        });
      }

      logTelegramRuntime('Telegram runtime bootstrap failed', {
        sessionName: session.name,
        error: lastError
      });
      logTelegramReaderEvent({
        level: 'warning',
        eventType: 'telegram.runtime.bootstrap_failed',
        message: lastError,
        payload: {
          sessionName: session.name
        }
      });
    }
  }

  const activeWatchlistCount = sessions.reduce((sum, session) => sum + listActiveWatchedChannels(session.name).length, 0);
  console.info('[WATCHLIST_COUNT]', {
    source: 'runtime_boot',
    sessionCount: sessions.length,
    activeWatchlistCount
  });

  if (startedSessions === 0) {
    logNoPostReason('Reader nicht aktiv', {
      detail: 'Keine aktive Telegram-Reader-Session wurde beim Boot gestartet.',
      sessionCount: sessions.length
    });
  } else if (activeWatchlistCount === 0) {
    logNoPostReason('Watchlist leer', {
      detail: 'Es sind keine aktiven Watchlist-Kanäle vorhanden.',
      startedSessions
    });
  }

  return {
    startedSessions,
    skipped: false
  };
}

export async function startTelegramPhoneLogin(input = {}) {
  const config = ensureReaderConfigured();
  const sessionName = normalizeSessionName(input.sessionName);
  const phoneNumber = cleanText(input.phoneNumber) || cleanText(config.phoneNumber);

  if (!phoneNumber) {
    throw new Error('Telefonnummer fehlt fuer den Telegram User Login.');
  }

  clearPendingAuthState(sessionName);
  await releaseClient(sessionName);

  const { client } = await createConnectedClient(sessionName);
  const authorized = await client.checkAuthorization();

  if (authorized) {
    return {
      status: 'connected',
      ...(await finalizeAuthorizedSession(sessionName, client, {
        loginMode: 'phone',
        phoneNumber
      }))
    };
  }

  const { phoneCodeHash, isCodeViaApp } = await client.sendCode(
    {
      apiId: config.apiId,
      apiHash: config.apiHash
    },
    phoneNumber,
    false
  );

  pendingAuthStates.set(sessionName, {
    type: 'phone',
    client,
    phoneNumber,
    phoneCodeHash,
    isCodeViaApp,
    status: 'code_requested',
    createdAt: nowIso(),
    lastError: ''
  });

  const session = upsertSessionRow({
    name: sessionName,
    loginMode: 'phone',
    phoneNumber,
    sessionPath: getSessionFilePath(sessionName),
    status: 'code_requested',
    lastError: ''
  });

  return {
    status: 'code_requested',
    session,
    pendingLogin: buildPendingAuthSummary(sessionName)
  };
}

export async function completeTelegramPhoneLogin(input = {}) {
  const config = ensureReaderConfigured();
  const sessionName = normalizeSessionName(input.sessionName);
  const phoneCode = cleanText(input.phoneCode);
  const password = cleanText(input.password);
  const state = pendingAuthStates.get(sessionName);

  if (!state || state.type !== 'phone') {
    throw new Error('Kein offener Telegram Telefon-Login gefunden.');
  }

  if (!phoneCode) {
    throw new Error('Telefon-Code fehlt.');
  }

  try {
    await state.client.invoke(
      new Api.auth.SignIn({
        phoneNumber: state.phoneNumber,
        phoneCodeHash: state.phoneCodeHash,
        phoneCode
      })
    );
  } catch (error) {
    if (error?.errorMessage === 'SESSION_PASSWORD_NEEDED') {
      if (!password) {
        state.status = 'password_required';
        state.lastError = '';
        const session = upsertSessionRow({
          name: sessionName,
          loginMode: 'phone',
          phoneNumber: state.phoneNumber,
          sessionPath: getSessionFilePath(sessionName),
          status: 'password_required',
          lastError: ''
        });

        return {
          status: 'password_required',
          session,
          pendingLogin: buildPendingAuthSummary(sessionName)
        };
      }

      let passwordError = null;

      try {
        await state.client.signInWithPassword(
          {
            apiId: config.apiId,
            apiHash: config.apiHash
          },
          {
            password: async () => password,
            onError: async (err) => {
              passwordError = err;
              return true;
            }
          }
        );
      } catch (passwordAuthError) {
        throw passwordError || passwordAuthError;
      }
    } else {
      throw error;
    }
  }

  return {
    status: 'connected',
    ...(await finalizeAuthorizedSession(sessionName, state.client, {
      loginMode: 'phone',
      phoneNumber: state.phoneNumber
    }))
  };
}

async function waitForQrReady(sessionName) {
  const startedAt = Date.now();

  while (Date.now() - startedAt < QR_READY_WAIT_MS) {
    const state = pendingAuthStates.get(sessionName);

    if (!state) {
      break;
    }

    if (state.qrDataUrl || state.status === 'error' || state.status === 'connected') {
      break;
    }

    await sleep(QR_READY_POLL_MS);
  }
}

function createPasswordPromise(state) {
  return new Promise((resolve, reject) => {
    state.passwordResolver = resolve;
    state.passwordRejecter = reject;
  });
}

export async function startTelegramQrLogin(input = {}) {
  const config = ensureReaderConfigured();
  const sessionName = normalizeSessionName(input.sessionName);

  clearPendingAuthState(sessionName);
  await releaseClient(sessionName);

  const { client } = await createConnectedClient(sessionName);
  const authorized = await client.checkAuthorization();

  if (authorized) {
    return {
      status: 'connected',
      ...(await finalizeAuthorizedSession(sessionName, client, {
        loginMode: 'qr'
      }))
    };
  }

  const state = {
    type: 'qr',
    client,
    status: 'starting',
    createdAt: nowIso(),
    qrUrl: '',
    qrDataUrl: '',
    qrExpiresAt: null,
    passwordHint: '',
    passwordResolver: null,
    passwordRejecter: null,
    passwordPromise: null,
    lastError: ''
  };

  pendingAuthStates.set(sessionName, state);
  upsertSessionRow({
    name: sessionName,
    loginMode: 'qr',
    sessionPath: getSessionFilePath(sessionName),
    status: 'qr_starting',
    qrLoginRequestedAt: nowIso(),
    lastError: ''
  });

  state.passwordPromise = createPasswordPromise(state);

  void client
    .signInUserWithQrCode(
      {
        apiId: config.apiId,
        apiHash: config.apiHash
      },
      {
        qrCode: async ({ token, expires }) => {
          state.status = 'qr_waiting';
          state.qrUrl = `tg://login?token=${toBase64Url(token)}`;
          state.qrDataUrl = await QRCode.toDataURL(state.qrUrl, {
            margin: 1,
            width: 280
          });
          state.qrExpiresAt = Number.isFinite(Number(expires))
            ? new Date(Number(expires) * 1000).toISOString()
            : null;

          upsertSessionRow({
            name: sessionName,
            loginMode: 'qr',
            sessionPath: getSessionFilePath(sessionName),
            status: 'qr_waiting',
            qrLoginRequestedAt: nowIso(),
            lastError: ''
          });
        },
        password: async (hint) => {
          state.status = 'password_required';
          state.passwordHint = cleanText(hint);
          upsertSessionRow({
            name: sessionName,
            loginMode: 'qr',
            sessionPath: getSessionFilePath(sessionName),
            status: 'password_required',
            lastError: ''
          });

          return await state.passwordPromise;
        },
        onError: async (error) => {
          state.lastError = error instanceof Error ? error.message : String(error);
          return false;
        }
      }
    )
    .then(async () => {
      state.status = 'connected';
      await finalizeAuthorizedSession(sessionName, client, {
        loginMode: 'qr'
      });
    })
    .catch((error) => {
      state.status = 'error';
      state.lastError = error instanceof Error ? error.message : 'QR-Login fehlgeschlagen.';
      upsertSessionRow({
        name: sessionName,
        loginMode: 'qr',
        sessionPath: getSessionFilePath(sessionName),
        status: 'error',
        lastError: state.lastError
      });
    });

  await waitForQrReady(sessionName);

  return {
    status: pendingAuthStates.get(sessionName)?.status || 'qr_waiting',
    session: mapSessionRow(getSessionRowByName(sessionName)),
    pendingLogin: buildPendingAuthSummary(sessionName)
  };
}

export async function submitTelegramQrPassword(input = {}) {
  const sessionName = normalizeSessionName(input.sessionName);
  const password = cleanText(input.password);
  const state = pendingAuthStates.get(sessionName);

  if (!state || state.type !== 'qr' || state.status !== 'password_required' || typeof state.passwordResolver !== 'function') {
    throw new Error('Kein QR-Login wartet aktuell auf ein Passwort.');
  }

  if (!password) {
    throw new Error('2FA-Passwort fehlt.');
  }

  const resolvePassword = state.passwordResolver;
  state.passwordResolver = null;
  state.passwordRejecter = null;
  state.passwordPromise = Promise.resolve(password);
  state.status = 'authorizing';
  resolvePassword(password);

  return {
    status: 'authorizing',
    pendingLogin: buildPendingAuthSummary(sessionName)
  };
}

export async function disconnectTelegramUserSession(input = {}) {
  const sessionName = normalizeSessionName(input.sessionName);
  clearPendingAuthState(sessionName);
  await releaseClient(sessionName);
  upsertSessionRow({
    name: sessionName,
    loginMode: getSessionRowByName(sessionName)?.login_mode || 'phone',
    phoneNumber: getSessionRowByName(sessionName)?.phone_number || '',
    sessionPath: getSessionFilePath(sessionName),
    status: 'disconnected',
    lastError: ''
  });

  return {
    status: 'disconnected',
    session: mapSessionRow(getSessionRowByName(sessionName))
  };
}

export async function listTelegramUserDialogs(input = {}) {
  const sessionName = normalizeSessionName(input.sessionName);
  const watchedSet = new Set(listWatchedChannels(sessionName).map((item) => normalizeChannelMatchKey(item.channelRef)).filter(Boolean));
  const dialogs = await listTelegramDialogSummaries(sessionName, input.limit);

  return dialogs.map((dialog) => ({
    id: dialog.id,
    entityId: dialog.entityId,
    dialogIdCandidates: dialog.dialogIdCandidates,
    channelRef: dialog.channelRef,
    title: dialog.title,
    type: dialog.type,
    username: dialog.username,
    watched: dialog.matchKeys.some((key) => watchedSet.has(key))
  }));
}

export async function watchTelegramDialog(input = {}) {
  const sessionName = normalizeSessionName(input.sessionName);
  await ensureAuthorizedClient(sessionName);

  if (!cleanText(input.channelRef)) {
    throw new Error('Dialog-Referenz fehlt.');
  }

  return ensureChannelRow(sessionName, {
    channelRef: normalizeConfiguredChannelRef(input.channelRef),
    channelTitle: cleanText(input.channelTitle),
    channelType: cleanText(input.channelType) || 'group',
    isActive: true,
    loginMode: getSessionRowByName(sessionName)?.login_mode || 'phone',
    phoneNumber: getSessionRowByName(sessionName)?.phone_number || ''
  });
}

export function unwatchTelegramDialog(input = {}) {
  const removedChannel = removeChannelRow(input.channelId);

  if (removedChannel?.session_name) {
    syncTelegramSourcesForSession(removedChannel.session_name);
  }

  return {
    success: true
  };
}

export async function syncTelegramWatchedMessages(input = {}) {
  const sessionName = resolveReaderSessionName(input.sessionName);
  const client = await ensureAuthorizedClient(sessionName);
  const bufferedMessagesBeforeSync = consumeRecentMessages(sessionName);
  const watchedChannels = listWatchedChannels(sessionName).filter(
    (item) => item.isActive && cleanText(item.channelTitle) && cleanText(item.channelRef)
  );
  const resultItems = [];

  for (const channel of watchedChannels) {
    try {
      const entityRef = resolveDialogRef(channel.channelRef);
      let latestSeenId = channel.lastSeenMessageId ? Number(channel.lastSeenMessageId) : 0;
      let latestSeenAt = channel.lastSeenMessageAt || null;

      for await (const message of client.iterMessages(entityRef, {
        limit: Math.min(MAX_SYNC_PER_CHANNEL, Math.max(1, Number(input.limit) || MAX_SYNC_PER_CHANNEL))
      })) {
        const currentMessageId = Number(message?.id || 0);

        if (latestSeenId && currentMessageId <= latestSeenId) {
          continue;
        }

        const structuredMessage = {
          ...(await formatTelegramMessage(message, channel.channelTitle)),
          sessionName
        };
        resultItems.push(structuredMessage);

        if (currentMessageId > latestSeenId) {
          latestSeenId = currentMessageId;
        }

        latestSeenAt = structuredMessage.timestamp;
      }

      if (latestSeenId) {
        updateChannelCheckpoint(channel.id, latestSeenId, latestSeenAt);
        const session = getSessionRowByName(sessionName);

        if (session) {
          upsertSessionRow({
            name: sessionName,
            loginMode: session.login_mode,
            phoneNumber: session.phone_number || '',
            sessionPath: session.session_path,
            status: getSessionRuntimeStatus(sessionName),
            lastMessageAt: latestSeenAt || null,
            lastError: ''
          });
        }
      }
    } catch (error) {
      const session = getSessionRowByName(sessionName);

      if (session) {
        upsertSessionRow({
          name: sessionName,
          loginMode: session.login_mode,
          phoneNumber: session.phone_number || '',
          sessionPath: session.session_path,
          status: getSessionRuntimeStatus(sessionName),
          lastError: error instanceof Error ? error.message : `Gruppe ${channel.channelTitle} konnte nicht gelesen werden.`
        });
      }
    }
  }

  const bufferedMessagesAfterSync = consumeRecentMessages(sessionName);
  const mergedItems = mergeStructuredMessages(bufferedMessagesBeforeSync, resultItems, bufferedMessagesAfterSync);
  mergedItems.sort((left, right) => new Date(left.timestamp).getTime() - new Date(right.timestamp).getTime());

  return {
    items: mergedItems
  };
}

export function resetTelegramReaderLastSeen(input = {}) {
  assertTelegramReaderDebugMode();

  const sessionName = resolveReaderSessionName(input.sessionName);
  const requestedChannelRef = cleanText(input.channelRef);
  const channels = getDebugScanChannels(sessionName, requestedChannelRef);

  channels.forEach((channel) => {
    updateChannelCheckpoint(channel.id, null, null);
  });

  console.info('[TELEGRAM_LAST_SEEN_RESET]', {
    sessionName,
    channelRef: normalizeConfiguredChannelRef(requestedChannelRef),
    resetCount: channels.length,
    channelIds: channels.map((channel) => channel.id)
  });

  return {
    success: true,
    sessionName,
    channelRef: normalizeConfiguredChannelRef(requestedChannelRef),
    resetCount: channels.length,
    items: channels.map((channel) => ({
      channelId: channel.id,
      channelRef: channel.channelRef,
      channelTitle: channel.channelTitle
    }))
  };
}

export async function forceScanTelegramReader(input = {}) {
  const readerConfig = assertTelegramReaderDebugMode();

  const sessionName = resolveReaderSessionName(input.sessionName);
  const requestedChannelRef = cleanText(input.channelRef);
  const client = await ensureAuthorizedClient(sessionName);
  const active = activeClients.get(sessionName);
  const limitPerGroup = Math.max(
    1,
    Math.min(50, Number(input.limitPerGroup ?? input.limit ?? TELEGRAM_DEBUG_SCAN_MESSAGE_LIMIT) || TELEGRAM_DEBUG_SCAN_MESSAGE_LIMIT)
  );
  const maxGroups = Math.max(1, Math.min(MAX_DIALOGS, Number(input.maxGroups) || MAX_DIALOGS));
  const ignoreLastSeen = input.ignoreLastSeen !== false;
  const postRejectedToTestGroup = input.postRejectedToTestGroup !== false;

  if (!active?.client || active.client !== client) {
    throw new Error('Telegram Reader Client ist nicht aktiv.');
  }

  if (active.pollingInFlight) {
    throw new Error('Telegram Polling laeuft bereits.');
  }

  const channels = getDebugScanChannels(sessionName, requestedChannelRef).filter(
    (item) => cleanText(item.channelTitle) && cleanText(item.channelRef)
  ).slice(0, maxGroups);

  const runtimeFlags = buildReaderRuntimeFlagSnapshot();
  const summary = createReaderLoopSummary();

  console.info('[TELEGRAM_FORCE_SCAN_START]', {
    sessionName,
    channelRef: normalizeConfiguredChannelRef(requestedChannelRef),
    dialogCount: channels.length,
    limit: limitPerGroup
  });
  console.info('[FORCE_READER_SCAN_START]', {
    sessionName,
    channelRef: normalizeConfiguredChannelRef(requestedChannelRef),
    dialogCount: channels.length,
    limit: limitPerGroup,
    maxGroups,
    ignoreLastSeen,
    postRejectedToTestGroup,
    ...runtimeFlags
  });
  console.info('[FORCE_SCAN_START]', {
    sessionName,
    channelRef: normalizeConfiguredChannelRef(requestedChannelRef),
    dialogCount: channels.length,
    limitPerGroup,
    maxGroups,
    ignoreLastSeen,
    postRejectedToTestGroup,
    ...runtimeFlags
  });
  console.info('[READER_LOOP_START]', {
    trigger: 'force_scan',
    sessionName,
    dialogCount: channels.length,
    limitPerGroup,
    maxGroups
  });

  active.pollingInFlight = true;
  const checkedDialogs = [];

  try {
    for (const channel of channels) {
      const checkedAt = nowIso();
      summary.groupsScanned += 1;
      let latestSeenId = channel.lastSeenMessageId ? Number(channel.lastSeenMessageId) : 0;
      let latestSeenAt = channel.lastSeenMessageAt || null;
      let scannedCount = 0;
      let pipelineSentCount = 0;
      let groupSkippedCount = 0;
      let groupErrorCount = 0;

      console.info('[FORCE_SCAN_GROUP]', {
        sessionName,
        channelId: channel.id,
        channelRef: channel.channelRef,
        channelTitle: channel.channelTitle,
        lastSeenMessageId: channel.lastSeenMessageId || '',
        ignoreLastSeen
      });
      console.info('[READER_GROUP_SCAN_START]', {
        trigger: 'force_scan',
        sessionName,
        channelId: channel.id,
        channelRef: channel.channelRef,
        channelTitle: channel.channelTitle
      });

      try {
        const entityRef = resolveDialogRef(channel.channelRef);
        console.info('[FORCE_SCAN_FETCH_MESSAGES]', {
          sessionName,
          channelId: channel.id,
          channelRef: channel.channelRef,
          limitPerGroup
        });
        const fetchedMessages = await client.getMessages(entityRef, {
          limit: limitPerGroup
        });
        const orderedMessages = sortTelegramMessagesAscending(Array.from(fetchedMessages || []).filter(Boolean));
        const newestMessageId = Number(orderedMessages.at(-1)?.id || 0) || null;
        console.info('[FORCE_SCAN_MESSAGE_COUNT]', {
          sessionName,
          channelId: channel.id,
          channelRef: channel.channelRef,
          fetchedCount: orderedMessages.length,
          newestMessageId
        });
        if (orderedMessages.length === 0) {
          console.info('[FORCE_SCAN_EMPTY_GROUP]', {
            sessionName,
            channelId: channel.id,
            channelRef: channel.channelRef
          });
        }

        for (const message of orderedMessages) {
          const currentMessageId = Number(message?.id || 0);

          if (!currentMessageId) {
            continue;
          }

           if (!ignoreLastSeen && latestSeenId && currentMessageId <= latestSeenId) {
            continue;
          }

          const structuredMessage = {
            ...(await formatTelegramMessage(message, channel.channelTitle)),
            sessionName
          };
          const amazonLink = findAmazonLinkInText(structuredMessage.text) || findAmazonLinkInText(structuredMessage.link);

          console.info('[FORCE_SCAN_MESSAGE]', {
            sessionName,
            channelId: channel.id,
            channelRef: channel.channelRef,
            messageId: structuredMessage.messageId,
            hasAmazonLink: Boolean(amazonLink)
          });
          console.info('[FORCE_SCAN_MESSAGE_EVALUATE]', {
            sessionName,
            channelId: channel.id,
            channelRef: channel.channelRef,
            messageId: structuredMessage.messageId,
            hasAmazonLink: Boolean(amazonLink)
          });
          console.info('[READER_MESSAGE_EVALUATE]', {
            trigger: 'force_scan',
            sessionName,
            channelId: channel.id,
            channelRef: channel.channelRef,
            messageId: structuredMessage.messageId,
            hasAmazonLink: Boolean(amazonLink)
          });
          console.info('[TELEGRAM_FORCE_SCAN_MESSAGE_FOUND]', {
            sessionName,
            channelId: channel.id,
            channelRef: channel.channelRef,
            messageId: structuredMessage.messageId,
            timestamp: structuredMessage.timestamp,
            hasAmazonLink: Boolean(amazonLink),
            amazonLink,
            textPreview: structuredMessage.text.slice(0, TELEGRAM_RAW_EVENT_TEXT_LIMIT)
          });
          console.info('[FORCE_READER_SCAN_MESSAGE_FOUND]', {
            sessionName,
            channelId: channel.id,
            channelRef: channel.channelRef,
            messageId: structuredMessage.messageId,
            timestamp: structuredMessage.timestamp,
            hasAmazonLink: Boolean(amazonLink),
            amazonLink,
            textPreview: structuredMessage.text.slice(0, TELEGRAM_RAW_EVENT_TEXT_LIMIT)
          });

          latestSeenId = Math.max(latestSeenId, currentMessageId);
          latestSeenAt = structuredMessage.timestamp || latestSeenAt;
          scannedCount += 1;

          try {
            const handledResult = await handleWatchedTelegramMessage(sessionName, message, {
              pipelineOptions: {
                trigger: 'force_scan',
                forceTestGroupPost: postRejectedToTestGroup === true
              }
            });
            const pipelineResult = handledResult?.pipelineResult || null;
            updateReaderLoopSummary(summary, pipelineResult, Boolean(amazonLink));

            if (pipelineResult?.reasonCode === 'missing_amazon_link') {
              groupSkippedCount += 1;
              console.info('[READER_MESSAGE_SKIPPED]', {
                trigger: 'force_scan',
                sessionName,
                channelId: channel.id,
                channelRef: channel.channelRef,
                messageId: structuredMessage.messageId,
                reason: pipelineResult.reasonCode
              });
              continue;
            }

            if (pipelineResult?.status === 'error') {
              groupErrorCount += 1;
              console.error('[READER_MESSAGE_ERROR_CONTINUE]', {
                trigger: 'force_scan',
                sessionName,
                channelId: channel.id,
                channelRef: channel.channelRef,
                messageId: structuredMessage.messageId,
                reason: pipelineResult.reason || 'Telegram Reader Pipeline fehlgeschlagen.'
              });
              continue;
            }

            console.info('[TELEGRAM_FORCE_SCAN_PIPELINE_SENT]', {
              sessionName,
              channelId: channel.id,
              channelRef: channel.channelRef,
              messageId: structuredMessage.messageId
            });
            console.info('[FORCE_READER_SCAN_PIPELINE_SENT]', {
              sessionName,
              channelId: channel.id,
              channelRef: channel.channelRef,
              messageId: structuredMessage.messageId
            });

            if (pipelineResult?.postedToTestGroup === true) {
              pipelineSentCount += 1;
              active.lastFoundMessageAt = structuredMessage.timestamp;
              active.lastFoundMessagePreview = structuredMessage.text.slice(0, TELEGRAM_RAW_EVENT_TEXT_LIMIT);
              if (['REJECT', 'REVIEW'].includes(pipelineResult?.decision || '')) {
                console.info('[FORCE_SCAN_REJECT_POSTED]', {
                  sessionName,
                  channelId: channel.id,
                  channelRef: channel.channelRef,
                  messageId: structuredMessage.messageId,
                  decision: pipelineResult.decision,
                  queueId: pipelineResult.queueId || null
                });
              } else {
                console.info('[FORCE_SCAN_POSTED]', {
                  sessionName,
                  channelId: channel.id,
                  channelRef: channel.channelRef,
                  messageId: structuredMessage.messageId,
                  decision: pipelineResult?.decision || 'APPROVE',
                  queueId: pipelineResult?.queueId || null
                });
              }
            }
          } catch (messageError) {
            groupErrorCount += 1;
            summary.errors += 1;
            console.error('[READER_MESSAGE_ERROR_CONTINUE]', {
              trigger: 'force_scan',
              sessionName,
              channelId: channel.id,
              channelRef: channel.channelRef,
              messageId: structuredMessage.messageId,
              reason: messageError instanceof Error ? messageError.message : 'Telegram Reader Nachricht konnte nicht verarbeitet werden.'
            });
            continue;
          }
        }

        updateChannelLastChecked(channel.id, checkedAt);
        if (latestSeenId) {
          updateChannelCheckpoint(channel.id, latestSeenId, latestSeenAt);
        }

        checkedDialogs.push({
          channelId: channel.id,
          channelRef: channel.channelRef,
          channelTitle: channel.channelTitle,
          scannedCount,
          pipelineSentCount,
          skippedCount: groupSkippedCount,
          errorCount: groupErrorCount,
          newestMessageId,
          latestSeenMessageId: latestSeenId ? String(latestSeenId) : channel.lastSeenMessageId || '',
          lastCheckedAt: checkedAt
        });
        console.info('[READER_GROUP_SCAN_DONE]', {
          trigger: 'force_scan',
          sessionName,
          channelId: channel.id,
          channelRef: channel.channelRef,
          scannedCount,
          pipelineSentCount,
          skippedCount: groupSkippedCount,
          errorCount: groupErrorCount,
          newestMessageId,
          latestSeenMessageId: latestSeenId ? String(latestSeenId) : channel.lastSeenMessageId || ''
        });
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : 'Telegram Force-Scan fehlgeschlagen.';

        updateChannelLastChecked(channel.id, checkedAt);
        summary.errors += 1;
        console.error('[TELEGRAM_FORCE_SCAN_ERROR]', {
          sessionName,
          channelId: channel.id,
          channelRef: channel.channelRef,
          error: errorMessage
        });
        logReaderPipelineError(errorMessage, {
          sessionName,
          channelId: channel.id,
          channelRef: channel.channelRef,
          trigger: 'force_scan'
        });

        checkedDialogs.push({
          channelId: channel.id,
          channelRef: channel.channelRef,
          channelTitle: channel.channelTitle,
          scannedCount,
          pipelineSentCount,
          lastCheckedAt: checkedAt,
          error: errorMessage
        });
      }
    }

    let emergencyResult = null;
    const postedCount = Number(summary.postedApprove || 0) + Number(summary.postedReview || 0) + Number(summary.postedReject || 0);

    if (postedCount === 0) {
      try {
        emergencyResult = await publishEmergencyReaderTestDeal(sessionName, {
          price: '9.99'
        });
        summary.postedApprove += emergencyResult?.telegramMessageId ? 1 : 0;
      } catch (error) {
        summary.errors += 1;
        console.error('[ERROR_REASON]', {
          reason: error instanceof Error ? error.message : 'Emergency Reader Test Deal konnte nicht gepostet werden.',
          trigger: 'emergency_test_deal',
          sessionName
        });
      }
    }

    active.lastPollAt = nowIso();
    active.lastPolledDialogs = checkedDialogs.slice(0, MAX_DIALOGS);
    console.info('[FORCE_SCAN_SUMMARY]', summary);
    console.info('[FORCE_SCAN_DONE]', {
      sessionName,
      ...summary,
      emergencyTriggered: Boolean(emergencyResult),
      emergencyTelegramMessageId: emergencyResult?.telegramMessageId || null
    });

    return {
      success: true,
      sessionName,
      channelRef: normalizeConfiguredChannelRef(requestedChannelRef),
      runtimeFlags: {
        readerTestMode: readerConfig.readerTestMode === true,
        readerDebugMode: readerConfig.readerDebugMode === true,
        allowRawReaderFallback: readerConfig.allowRawReaderFallback === true,
        dealLockBypass: getReaderRuntimeConfig().dealLockBypass === true
      },
      options: {
        limitPerGroup,
        maxGroups,
        ignoreLastSeen,
        postRejectedToTestGroup
      },
      summary,
      emergencyResult,
      items: checkedDialogs
    };
  } finally {
    console.info('[READER_LOOP_DONE]', {
      trigger: 'force_scan',
      sessionName,
      ...summary
    });
    active.pollingInFlight = false;
  }
}

function createForceTestgroupSummary() {
  return {
    groupsScanned: 0,
    messagesChecked: 0,
    amazonLinksFound: 0,
    foreignLinksFound: 0,
    sentToTestGroup: 0,
    errors: 0,
    skipped: 0
  };
}

function buildForceTestgroupDiagnosticText({
  title = '',
  reason = '',
  sourceHost = '',
  sourceLabel = '',
  channelRef = '',
  channelTitle = '',
  group = '',
  warningLines = []
} = {}) {
  console.info('[READER_DIAGNOSIS_MERGED_INTO_DEBUG]', {
    source: 'force_testgroup_diagnostic',
    sourceHost: cleanText(sourceHost) || null,
    sourceLabel: cleanText(sourceLabel) || null
  });
  console.info('[DEBUG_INFO_MERGED]', {
    source: 'force_testgroup_diagnostic',
    sourceHost: cleanText(sourceHost) || null,
    sourceLabel: cleanText(sourceLabel) || null
  });

  return buildTelegramDealDebugInfoExtended(
    buildTelegramDiagnosticDebugValues({
      reason,
      sourceHost,
      sourceLabel,
      channelRef,
      channelTitle,
      group,
      warningLines,
      liveAllowed: false,
      testGroupPosted: true,
      marketComparisonStatus: 'blocked'
    })
  );
}

async function sendForceTestgroupDiagnosticPost({
  sessionName = '',
  source = {},
  structuredMessage = {},
  blockedCode = '',
  reason = '',
  sourceHost = '',
  sourceLabel = '',
  warningLines = []
} = {}) {
  const diagnosticText = buildForceTestgroupDiagnosticText({
    title:
      structuredMessage?.previewTitle ||
      extractTelegramTitle(structuredMessage?.text, structuredMessage?.group) ||
      '',
    reason,
    sourceHost,
    sourceLabel,
    channelRef: cleanText(source?.channelRef) || cleanText(structuredMessage?.channelRef),
    channelTitle: cleanText(source?.channelTitle) || cleanText(structuredMessage?.channelTitle),
    group: cleanText(structuredMessage?.group),
    warningLines
  });

  console.info('[FORCE_TESTGROUP_SEND_ATTEMPT]', {
    sessionName,
    sourceId: source?.id ?? null,
    messageId: structuredMessage?.messageId || '',
    mode: 'diagnostic',
    blockedCode: cleanText(blockedCode) || 'review_required',
    reason: cleanText(reason) || 'Review noetig.'
  });

  const result = await enqueueTelegramReaderDiagnosticPost({
    source,
    structuredMessage,
    diagnosticText,
    blockedCode: cleanText(blockedCode) || 'review_required',
    blockedReason: cleanText(reason) || 'Review noetig.'
  });

  if (!result?.messageId) {
    throw new Error('Diagnose-Testpost konnte nicht in die Telegram-Testgruppe gesendet werden.');
  }

  console.info('[FORCE_TESTGROUP_SEND_SUCCESS]', {
    sessionName,
    sourceId: source?.id ?? null,
    messageId: structuredMessage?.messageId || '',
    telegramMessageId: result.messageId,
    mode: 'diagnostic',
    blockedCode: cleanText(blockedCode) || 'review_required'
  });

  return result;
}

export async function forceTestgroupFeed(input = {}) {
  const readerConfig = assertTelegramReaderDebugMode();
  const sessionName = resolveReaderSessionName(input.sessionName);
  const requestedChannelRef = cleanText(input.channelRef);
  const client = await ensureAuthorizedClient(sessionName);
  const limitPerGroup = Math.max(1, Math.min(50, Number(input.limitPerGroup ?? 20) || 20));
  const maxGroups = Math.max(1, Math.min(MAX_READER_GROUP_SLOTS, Number(input.maxGroups ?? 100) || 100));
  const ignoreLastSeen = input.ignoreLastSeen !== false;
  const sendEverythingToTestGroup = input.sendEverythingToTestGroup !== false;
  const channels = getDebugScanChannels(sessionName, requestedChannelRef)
    .filter((item) => cleanText(item.channelTitle) && cleanText(item.channelRef))
    .slice(0, maxGroups);
  const summary = createForceTestgroupSummary();
  const seenMessages = new Set();
  const items = [];
  const testGroupConfig = getTelegramTestGroupConfig();
  const targetChatId = cleanText(testGroupConfig.chatId);
  const targetSource = cleanText(process.env.TELEGRAM_TEST_CHAT_ID)
    ? 'TELEGRAM_TEST_CHAT_ID'
    : cleanText(process.env.TELEGRAM_CHAT_ID)
      ? 'TELEGRAM_CHAT_ID'
      : 'missing';

  console.info('[FORCE_TESTGROUP_START]', {
    sessionName,
    channelRef: normalizeConfiguredChannelRef(requestedChannelRef),
    groupsRequested: channels.length,
    limitPerGroup,
    maxGroups,
    ignoreLastSeen,
    sendEverythingToTestGroup,
    targetChatId: targetChatId || null,
    targetSource,
    readerTestMode: readerConfig.readerTestMode === true,
    readerDebugMode: readerConfig.readerDebugMode === true
  });
  console.info('[TESTGROUP_TARGET_RESOLVED]', {
    context: 'force_testgroup_feed',
    targetChatId: targetChatId || null,
    targetSource,
    tokenConfigured: Boolean(cleanText(testGroupConfig.token))
  });

  for (const channel of channels) {
    summary.groupsScanned += 1;
    const groupResult = {
      channelId: channel.id,
      channelRef: channel.channelRef,
      channelTitle: channel.channelTitle,
      messagesFound: 0,
      sentToTestGroup: 0,
      errors: 0,
      skipped: 0
    };
    items.push(groupResult);

    console.info('[FORCE_TESTGROUP_GROUP_START]', {
      sessionName,
      channelId: channel.id,
      channelRef: channel.channelRef,
      channelTitle: channel.channelTitle
    });

    try {
      const entityRef = resolveDialogRef(channel.channelRef);
      const fetchedMessages = await client.getMessages(entityRef, {
        limit: limitPerGroup
      });
      const orderedMessages = sortTelegramMessagesAscending(Array.from(fetchedMessages || []).filter(Boolean));
      groupResult.messagesFound = orderedMessages.length;

      console.info('[FORCE_TESTGROUP_MESSAGES_FOUND]', {
        sessionName,
        channelId: channel.id,
        channelRef: channel.channelRef,
        channelTitle: channel.channelTitle,
        count: orderedMessages.length,
        ignoreLastSeen
      });

      for (const message of orderedMessages) {
        const currentMessageId = Number(message?.id || 0);

        if (!currentMessageId) {
          continue;
        }

        if (!ignoreLastSeen && Number(channel.lastSeenMessageId || 0) && currentMessageId <= Number(channel.lastSeenMessageId || 0)) {
          continue;
        }

        let structuredMessage = null;
        let source = null;

        try {
          structuredMessage = {
            ...(await formatTelegramMessage(message, channel.channelTitle)),
            sessionName
          };
          const messageKey = createStructuredMessageKey(structuredMessage);
          summary.messagesChecked += 1;

          if (seenMessages.has(messageKey)) {
            summary.skipped += 1;
            groupResult.skipped += 1;
            continue;
          }
          seenMessages.add(messageKey);

          const hasRenderableContent = Boolean(
            cleanText(structuredMessage.text) ||
              cleanText(structuredMessage.previewTitle) ||
              cleanText(structuredMessage.previewDescription) ||
              cleanText(structuredMessage.externalLink) ||
              cleanText(structuredMessage.previewUrl) ||
              cleanText(structuredMessage.telegramMediaDataUrl)
          );
          if (!hasRenderableContent) {
            summary.skipped += 1;
            groupResult.skipped += 1;
            continue;
          }

          source = upsertTelegramReaderSource(sessionName, channel) || {
            id: null,
            name: channel.channelTitle || channel.channelRef || 'Telegram Quelle'
          };

          const detectedAsin =
            extractAsin(structuredMessage.text) ||
            extractAsin(structuredMessage.externalLink) ||
            extractAsin(structuredMessage.previewUrl) ||
            extractAsin(structuredMessage.link);
          const amazonLink =
            findAmazonLinkInText(structuredMessage.text) ||
            findAmazonLinkInText(structuredMessage.externalLink) ||
            findAmazonLinkInText(structuredMessage.previewUrl) ||
            findAmazonLinkInText(structuredMessage.link) ||
            (detectedAsin ? `https://www.amazon.de/dp/${detectedAsin}` : '');
          const externalLinks = Array.isArray(structuredMessage.allLinks)
            ? structuredMessage.allLinks.filter((candidate) => cleanText(candidate))
            : [];
          const foreignLinks = externalLinks.filter((candidate) => !/(?:amzn\.to|amazon\.[a-z.]+)/i.test(candidate));
          const primaryForeignLink =
            cleanText(structuredMessage.externalLink) && !/(?:amzn\.to|amazon\.[a-z.]+)/i.test(structuredMessage.externalLink)
              ? cleanText(structuredMessage.externalLink)
              : foreignLinks[0] || '';
          const sourceHost = normalizeUrlHost(primaryForeignLink || structuredMessage.previewUrl || structuredMessage.link);

          if (amazonLink || detectedAsin) {
            summary.amazonLinksFound += 1;
          } else if (primaryForeignLink) {
            summary.foreignLinksFound += 1;
          }

          console.info('[FORCE_TESTGROUP_MESSAGE_PROCESS]', {
            sessionName,
            channelId: channel.id,
            channelRef: channel.channelRef,
            messageId: structuredMessage.messageId,
            hasAmazonLink: Boolean(amazonLink || detectedAsin),
            hasForeignLink: Boolean(primaryForeignLink)
          });

          if (amazonLink || detectedAsin) {
            console.info('[FORCE_TESTGROUP_SEND_ATTEMPT]', {
              sessionName,
              sourceId: source?.id ?? null,
              messageId: structuredMessage.messageId,
              mode: 'generator_path',
              amazonLink: amazonLink || `https://www.amazon.de/dp/${detectedAsin}`
            });
            const pipelineResult = await processTelegramReaderPipeline(sessionName, source, structuredMessage, {
              trigger: 'force_testgroup_feed',
              forceTestGroupPost: sendEverythingToTestGroup === true
            });

            if (pipelineResult?.postedToTestGroup === true && pipelineResult?.messageId) {
              summary.sentToTestGroup += 1;
              groupResult.sentToTestGroup += 1;
              console.info('[FORCE_TESTGROUP_SEND_SUCCESS]', {
                sessionName,
                sourceId: source?.id ?? null,
                messageId: structuredMessage.messageId,
                telegramMessageId: pipelineResult.messageId,
                mode: 'generator_path',
                decision: pipelineResult.decision || 'UNKNOWN'
              });
              continue;
            }

            const diagnosticResult = await sendForceTestgroupDiagnosticPost({
              sessionName,
              source,
              structuredMessage,
              blockedCode: cleanText(pipelineResult?.reasonCode) || 'PIPELINE_REVIEW_REQUIRED',
              reason: cleanText(pipelineResult?.reason) || 'Amazon-Fund konnte nicht normal verarbeitet werden.',
              sourceHost: normalizeUrlHost(amazonLink || structuredMessage.previewUrl || structuredMessage.link),
              warningLines: [
                'Generator-Pfad wurde versucht.',
                'Testgruppe zeigt den Fund deshalb als Diagnose.'
              ]
            });
            summary.sentToTestGroup += diagnosticResult?.messageId ? 1 : 0;
            groupResult.sentToTestGroup += diagnosticResult?.messageId ? 1 : 0;
            continue;
          }

          let protectedMatches = collectProtectedSourceMatches([
            { source: 'telegramText', value: structuredMessage.text },
            { source: 'previewTitle', value: structuredMessage.previewTitle },
            { source: 'previewDescription', value: structuredMessage.previewDescription }
          ]);
          let diagnosticReason = primaryForeignLink
            ? 'Fremdlink erkannt, Review noetig.'
            : 'Keine eindeutige Produktquelle erkannt, Review noetig.';
          const warningLines = [];

          if (primaryForeignLink) {
            try {
              const scrapedDeal = await scrapeGenericDealPage(primaryForeignLink);
              const scrapedProtectedMatches = collectProtectedSourceMatches([
                { source: 'scrapedTitle', value: scrapedDeal?.title },
                { source: 'scrapedDescription', value: scrapedDeal?.productDescription }
              ]);
              if (scrapedProtectedMatches.length) {
                protectedMatches = [...protectedMatches, ...scrapedProtectedMatches];
              }
            } catch (error) {
              warningLines.push(
                `Quellseite konnte nicht sauber gelesen werden: ${
                  error instanceof Error ? error.message : 'Scrape fehlgeschlagen.'
                }`
              );
            }
          }

          if (protectedMatches.length > 0) {
            diagnosticReason = 'Quelle geschuetzt, Review noetig.';
          }

          const diagnosticResult = await sendForceTestgroupDiagnosticPost({
            sessionName,
            source,
            structuredMessage,
            blockedCode: protectedMatches.length > 0 ? 'CLOUDFLARE_OR_PROTECTED_SOURCE' : 'FOREIGN_LINK_REVIEW',
            reason: diagnosticReason,
            sourceHost,
            sourceLabel: primaryForeignLink || structuredMessage.previewUrl || structuredMessage.link,
            warningLines: [
              protectedMatches.length > 0 ? 'Cloudflare oder Schutzseite erkannt.' : '',
              primaryForeignLink ? 'Fremdlink wurde nicht als Live-Deal freigegeben.' : ''
            ].concat(warningLines)
          });
          summary.sentToTestGroup += diagnosticResult?.messageId ? 1 : 0;
          groupResult.sentToTestGroup += diagnosticResult?.messageId ? 1 : 0;
        } catch (error) {
          summary.errors += 1;
          groupResult.errors += 1;
          const errorMessage =
            error instanceof Error ? error.message : 'Force-Testgruppen-Nachricht konnte nicht verarbeitet werden.';

          console.error('[FORCE_TESTGROUP_SEND_ERROR]', {
            sessionName,
            sourceId: source?.id ?? null,
            messageId: structuredMessage?.messageId || '',
            channelId: channel.id,
            channelRef: channel.channelRef,
            reason: errorMessage
          });

          try {
            const diagnosticResult = await sendForceTestgroupDiagnosticPost({
              sessionName,
              source,
              structuredMessage,
              blockedCode: 'FORCE_TESTGROUP_ERROR',
              reason: errorMessage,
              sourceHost: normalizeUrlHost(
                structuredMessage?.externalLink || structuredMessage?.previewUrl || structuredMessage?.link
              ),
              warningLines: ['Nachricht wurde im Notfall-Feed nur als Diagnose ausgegeben.']
            });
            summary.sentToTestGroup += diagnosticResult?.messageId ? 1 : 0;
            groupResult.sentToTestGroup += diagnosticResult?.messageId ? 1 : 0;
          } catch (diagnosticError) {
            console.error('[FORCE_TESTGROUP_SEND_ERROR]', {
              sessionName,
              sourceId: source?.id ?? null,
              messageId: structuredMessage?.messageId || '',
              channelId: channel.id,
              channelRef: channel.channelRef,
              reason:
                diagnosticError instanceof Error
                  ? diagnosticError.message
                  : 'Diagnose-Testpost konnte nicht gesendet werden.'
            });
          }
        }
      }
    } catch (error) {
      summary.errors += 1;
      groupResult.errors += 1;
      console.error('[FORCE_TESTGROUP_SEND_ERROR]', {
        sessionName,
        channelId: channel.id,
        channelRef: channel.channelRef,
        channelTitle: channel.channelTitle,
        reason: error instanceof Error ? error.message : 'Watchlist-Gruppe konnte nicht gelesen werden.'
      });
    }

    console.info('[FORCE_TESTGROUP_GROUP_DONE]', {
      sessionName,
      channelId: channel.id,
      channelRef: channel.channelRef,
      channelTitle: channel.channelTitle,
      messagesFound: groupResult.messagesFound,
      sentToTestGroup: groupResult.sentToTestGroup,
      errors: groupResult.errors,
      skipped: groupResult.skipped
    });
  }

  if (summary.sentToTestGroup === 0) {
    try {
      const infoResult = await sendTelegramPost({
        text: 'Force-Test abgeschlossen. Keine sendbaren Deals gefunden.',
        disableWebPagePreview: true,
        chatId: targetChatId,
        titlePreview: 'Force-Testgruppe',
        hasAffiliateLink: false,
        postContext: 'force_testgroup_empty_status'
      });
      if (infoResult?.messageId) {
        summary.sentToTestGroup += 1;
        console.info('[FORCE_TESTGROUP_EMPTY_STATUS_SENT]', {
          sessionName,
          messageId: infoResult.messageId,
          chatId: infoResult.chatId || targetChatId || null
        });
        console.info('[FORCE_TESTGROUP_SEND_SUCCESS]', {
          sessionName,
          sourceId: null,
          messageId: 'force-testgroup-summary',
          telegramMessageId: infoResult.messageId,
          mode: 'summary'
        });
      }
    } catch (error) {
      summary.errors += 1;
      console.error('[FORCE_TESTGROUP_SEND_ERROR]', {
        sessionName,
        sourceId: null,
        messageId: 'force-testgroup-summary',
        reason: error instanceof Error ? error.message : 'Force-Test-Statuspost konnte nicht gesendet werden.'
      });
    }
  }

  console.info('[FORCE_TESTGROUP_SUMMARY]', {
    sessionName,
    ...summary
  });

  return {
    success: true,
    sessionName,
    channelRef: normalizeConfiguredChannelRef(requestedChannelRef),
    targetChatId: targetChatId || null,
    targetSource,
    tokenConfigured: Boolean(cleanText(testGroupConfig.token)),
    options: {
      limitPerGroup,
      maxGroups,
      ignoreLastSeen,
      sendEverythingToTestGroup
    },
    summary,
    items
  };
}

export const __testablesTelegramUserClient = {
  resolveAmazonDirectRequiredCheckBlock,
  resolveScrapedDealSellerIdentity,
  resolveFbmSellerProfileReviewBlock,
  evaluateTelegramReaderGeneratorCandidate,
  resolveReaderTitlePayload,
  resolveReaderPricePayload,
  resolveReaderImagePayload,
  buildTelegramReaderTemplatePayload,
  buildReaderLinkRecord,
  resolveReaderDealType,
  collectProtectedSourceMatches,
  classifyRelaxedAmazonMatchScore,
  extractSourceProductFacts,
  resolveProductVerification,
  isOwnAmazonAffiliateLink,
  buildReaderCompactDebugBlockV3,
  buildTelegramDealDebugInfoExtended
};
