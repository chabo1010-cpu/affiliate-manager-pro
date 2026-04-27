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
import { buildAmazonAffiliateLinkRecord, extractAsin, isAmazonShortLink, normalizeSellerType } from './dealHistoryService.js';
import { upsertAppSession } from './databaseService.js';
import { publishGeneratorPostDirect } from './directPublisher.js';
import { buildGeneratorDealContext } from './generatorDealScoringService.js';
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

function extractSourceProductFacts({ structuredMessage = {}, scrapedDeal = {}, pricing = {} } = {}) {
  const titleCandidates = [
    sanitizeProtectedSourceValue(structuredMessage?.previewTitle, 'previewTitle'),
    sanitizeProtectedSourceValue(scrapedDeal?.title, 'scrapedTitle'),
    sanitizeProtectedSourceValue(extractTelegramTitle(structuredMessage?.text, structuredMessage?.group), 'telegramTitle')
  ].filter(Boolean);
  const rawTitle = titleCandidates[0] || '';
  const priceValue =
    pricing?.currentPrice !== null && pricing?.currentPrice !== undefined
      ? Number(pricing.currentPrice)
      : parseTelegramLocalizedNumber(scrapedDeal?.price) ?? null;
  const imageUrl =
    cleanText(structuredMessage?.previewImage) ||
    cleanText(scrapedDeal?.previewImage) ||
    cleanText(scrapedDeal?.ogImage) ||
    cleanText(scrapedDeal?.imageUrl);
  const host = normalizeUrlHost(
    structuredMessage?.externalLink || structuredMessage?.previewUrl || structuredMessage?.link || scrapedDeal?.finalUrl || ''
  );
  const modelToken =
    tokenizeMatchText(rawTitle).find((token) => /[a-z]/i.test(token) && /\d/.test(token) && token.length >= 4) || '';
  const brandToken = tokenizeMatchText(rawTitle)[0] || '';
  const query = [brandToken, modelToken, rawTitle].filter(Boolean).join(' ').slice(0, 220).trim();

  if (imageUrl) {
    console.info('[SOURCE_IMAGE_MATCH_ONLY]', {
      sourceHost: host || 'unknown',
      imageUrl
    });
  }

  return {
    title: rawTitle,
    priceValue,
    imageUrl,
    host,
    brand: brandToken,
    model: modelToken,
    category: '',
    query: query || rawTitle
  };
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
  const sourceTokens = tokenizeMatchText(sourceFacts.title || sourceFacts.query || '');
  const candidateTokens = tokenizeMatchText(candidate.title || '');
  const candidateTokenSet = new Set(candidateTokens);
  const overlappingTokens = sourceTokens.filter((token) => candidateTokenSet.has(token));
  const titleScore =
    sourceTokens.length > 0 ? Math.min(80, Math.round((overlappingTokens.length / sourceTokens.length) * 80)) : 0;

  let priceScore = 0;
  if (sourceFacts.priceValue !== null && Number.isFinite(sourceFacts.priceValue) && candidate.priceValue !== null) {
    const delta = Math.abs(candidate.priceValue - sourceFacts.priceValue);
    const ratio = sourceFacts.priceValue > 0 ? delta / sourceFacts.priceValue : 1;
    if (ratio <= 0.02) {
      priceScore = 20;
    } else if (ratio <= 0.05) {
      priceScore = 12;
    } else if (ratio <= 0.12) {
      priceScore = 6;
    }
  }

  const brandScore =
    sourceFacts.brand && candidateTokenSet.has(sourceFacts.brand.toLowerCase())
      ? 10
      : 0;
  const modelScore =
    sourceFacts.model && candidate.title.toLowerCase().includes(sourceFacts.model.toLowerCase())
      ? 10
      : 0;

  return Math.min(100, titleScore + priceScore + brandScore + modelScore);
}

function classifyRelaxedAmazonMatchScore(matchScore = 0) {
  const numericScore = Number(matchScore);
  const safeScore = Number.isFinite(numericScore) ? numericScore : 0;

  if (safeScore >= 60) {
    return {
      tier: 'auto_post',
      decision: 'APPROVE',
      matched: true,
      reason: 'Amazon-Match >= 60 erkannt.'
    };
  }

  if (safeScore >= 40) {
    return {
      tier: 'review',
      decision: 'REVIEW',
      matched: false,
      reason: 'Kein perfekter Match, aber fuer die Testgruppe ausreichend.'
    };
  }

  return {
    tier: 'debug',
    decision: 'DEBUG',
    matched: false,
    reason: 'Kein perfekter Match; Deal bleibt in der Testgruppe als Debug sichtbar.'
  };
}

async function searchAmazonProductBySourceData({
  sessionName = '',
  source = {},
  structuredMessage = {},
  scrapedDeal = {},
  pricing = {}
} = {}) {
  const sourceFacts = extractSourceProductFacts({
    structuredMessage,
    scrapedDeal,
    pricing
  });

  console.info('[AMAZON_SEARCH_BY_SOURCE_DATA]', {
    sessionName,
    sourceId: source?.id ?? null,
    messageId: structuredMessage?.messageId || '',
    query: sourceFacts.query || '',
    title: sourceFacts.title || '',
    sourceHost: sourceFacts.host || 'unknown'
  });

  if (!sourceFacts.query) {
    return {
      attempted: true,
      matched: false,
      matchScore: 0,
      reason: 'Zu wenig Quelldaten fuer eine Amazon-Suche.',
      sourceFacts
    };
  }

  const searchUrl = `https://www.amazon.de/s?k=${encodeURIComponent(sourceFacts.query)}`;

  try {
    const response = await fetch(searchUrl, {
      headers: AMAZON_SEARCH_FETCH_HEADERS
    });
    const html = await response.text();
    const protectedMatches = collectProtectedSourceMatches([
      { source: 'amazon-search', value: html.slice(0, 2000) }
    ]);

    if (protectedMatches.length) {
      return {
        attempted: true,
        matched: false,
        matchScore: 0,
        reason: 'Amazon-Suche wurde durch Schutzseite blockiert.',
        sourceFacts
      };
    }

    const candidates = extractAmazonSearchCandidates(html).slice(0, 10);
    const scoredCandidates = candidates
      .map((candidate) => ({
        ...candidate,
        matchScore: computeAmazonProductMatchScore(sourceFacts, candidate)
      }))
      .sort((left, right) => right.matchScore - left.matchScore);
    const bestCandidate = scoredCandidates[0] || null;

    console.info('[PRODUCT_MATCH_SCORE]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      bestAsin: bestCandidate?.asin || '',
      matchScore: bestCandidate?.matchScore ?? 0,
      query: sourceFacts.query || ''
    });

    const relaxedMatch = classifyRelaxedAmazonMatchScore(bestCandidate?.matchScore ?? 0);
    console.info('[MATCH_SCORE_RELAXED]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      bestAsin: bestCandidate?.asin || '',
      matchScore: bestCandidate?.matchScore ?? 0,
      tier: relaxedMatch.tier,
      decision: relaxedMatch.decision,
      autoPostThreshold: 60,
      reviewThresholdMin: 40
    });

    if (!bestCandidate || relaxedMatch.matched !== true) {
      console.info('[PRODUCT_MATCH_REVIEW]', {
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
        decision: relaxedMatch.decision
      };
    }

    const linkRecord = buildAmazonAffiliateLinkRecord(bestCandidate.normalizedUrl, {
      asin: bestCandidate.asin
    });
    if (!linkRecord.valid || !cleanText(linkRecord.affiliateUrl)) {
      return {
        attempted: true,
        matched: false,
        matchScore: bestCandidate.matchScore,
        reason: 'Amazon-Match erkannt, aber Partnerlink konnte nicht gebaut werden.',
        sourceFacts,
        matchTier: 'review',
        decision: 'REVIEW'
      };
    }

    const matchedScrapedDeal = await scrapeAmazonProduct(linkRecord.normalizedUrl);
    return {
      attempted: true,
      matched: true,
      matchScore: bestCandidate.matchScore,
      sourceFacts,
      linkRecord,
      scrapedDeal: matchedScrapedDeal,
      matchTier: relaxedMatch.tier,
      decision: relaxedMatch.decision
    };
  } catch (error) {
    return {
      attempted: true,
      matched: false,
      matchScore: 0,
      reason: error instanceof Error ? error.message : 'Amazon-Suche aus Quellendaten fehlgeschlagen.',
      sourceFacts,
      matchTier: 'debug',
      decision: 'DEBUG'
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
  testGroupPosted = false
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

  return {
    success: response.ok,
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
    const selectedAmazonCandidate = amazonPriceCandidates[0] || null;

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
      priceSource = 'amazon';
      rawPriceSource = selectedAmazonCandidate.source;
    }

    if (telegramPrice && (!currentPrice || !readerPricesEqual(currentPrice, telegramPrice))) {
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
  } else if (scrapedPrice) {
    currentPrice = scrapedPrice;
    priceSource = 'scraped';
    rawPriceSource = 'scraped';
  } else if (telegramPrice) {
    currentPrice = telegramPrice;
    priceSource = 'telegram';
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

  return {
    currentPrice,
    oldPrice: '',
    priceSource,
    rawPriceSource,
    amazonBuyBoxPrice,
    amazonDealPrice,
    telegramPrice,
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
  const normalizedTimestamp = normalizeTelegramDate(message?.date);

  return {
    sessionName: '',
    messageId: message?.id ? String(message.id) : '',
    chatId,
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
    marketPrice: comparisonPrice,
    priceSource,
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
    shortlinkResolved: generatorInput?.shortlinkResolved === true,
    shortlinkFallback: generatorInput?.shortlinkFallback === true,
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
  debugValues.comparisonUsageLabel = comparisonUsage.usedLabel;
  debugValues.comparisonNotUsedLabel = comparisonUsage.notUsedLabel;
  debugValues.comparisonSourceLabel = comparisonUsage.comparisonSourceLabel;
  debugValues.aiUsageMode = aiUsageMode;

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
  return {
    decisionDisplay: 'REVIEW',
    wouldPostNormally: liveAllowed === true,
    testGroupPosted: testGroupPosted !== false,
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
  const priceLabel =
    debugValues.invalidPrice === true ? 'n/a' : formatCompactPostPrice(debugValues.detectedPrice) || 'n/a';
  const reasonLabel = shortenDebugReason(debugValues.reason || debugValues.invalidPriceReason || 'n/a', 'n/a');
  const minScoreLabel = formatDebugScore(debugValues.thresholds?.minScore ?? debugValues.minScore);
  const lines = [
    '\u26A0\uFE0F <b>TESTPOST</b>',
    '',
    '\u{1F9FE} <b>ERGEBNIS</b>',
    `\u{1F4CC} Entscheidung: ${escapeTelegramHtml(debugValues.decisionDisplay || 'REVIEW')}`,
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

  console.info('[DEBUG_POST_NORMALIZED]', {
    decisionDisplay: debugValues.decisionDisplay || 'REVIEW',
    marketStatus,
    aiStatus,
    keepaStatus,
    seller: sellerLabel,
    missingPrice: priceLabel === 'n/a'
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
  const keepaAvailable = generatorContext?.keepa?.available === true;
  const dealLockBlocked = generatorContext?.dealLock?.blocked === true || learning?.dealLockBlocked === true;

  if (dealLockBlocked) {
    return {
      accepted: false,
      decision: 'review',
      reason: cleanText(generatorContext?.dealLock?.blockReason) || learning?.reason || 'Deal-Lock aktiv.'
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

function resolveReaderImagePayload({ scrapedDeal = {}, structuredMessage = {}, dealType = 'AMAZON', title = '', currentPrice = '' } = {}) {
  const normalizedDealType = cleanText(dealType).toUpperCase() || 'AMAZON';
  const logImageFallback = (source, imageUrl = '') => {
    console.info('[IMAGE_FALLBACK_USED]', {
      dealType: normalizedDealType,
      source,
      imageUrl: imageUrl || ''
    });
  };
  const amazonImage = normalizedDealType === 'AMAZON' ? resolveDealImageUrlFromScrape(scrapedDeal || {}) : '';
  if (amazonImage) {
    console.info('[IMAGE_SOURCE_FOUND]', {
      source: 'amazonProductImage',
      dealType: normalizedDealType,
      imageUrl: amazonImage
    });
    console.info('[IMAGE_SOURCE]', {
      source: 'amazon',
      dealType: normalizedDealType,
      imageUrl: amazonImage
    });
    return {
      generatedImagePath: amazonImage,
      uploadedImagePath: '',
      imageSource: 'amazon',
      telegramImageSource: 'standard',
      whatsappImageSource: 'standard'
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
      whatsappImageSource: 'none'
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
      whatsappImageSource: 'upload'
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
      whatsappImageSource: 'standard'
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
      whatsappImageSource: 'standard'
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
      whatsappImageSource: 'standard'
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
    whatsappImageSource: 'upload'
  };
}

function buildTelegramReaderGeneratorInput({
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
  const pricePayload = resolveReaderPricePayload({
    dealType,
    scrapedDeal,
    pricing
  });
  const productDescription = extractReaderProductDescription({
    scrapedDeal,
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
    scrapedDeal,
    structuredMessage,
    dealType,
    title: titlePayload.title || template.productTitle || 'Deal',
    currentPrice: invalidPriceState.invalid ? '' : rawCurrentPrice
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
    link: cleanText(affiliateUrl),
    normalizedUrl: cleanText(normalizedUrl),
    asin: cleanText(normalizedAsin || scrapedDeal?.asin).toUpperCase(),
    sellerType: sellerIdentity.sellerType || 'UNKNOWN',
    sellerClass: sellerIdentity.sellerClass || 'UNKNOWN',
    soldByAmazon: sellerIdentity.soldByAmazon,
    shippedByAmazon: sellerIdentity.shippedByAmazon,
    sellerDetectionSource: sellerIdentity.details?.detectionSource || 'unknown',
    sellerDetectionSources: sellerIdentity.details?.detectionSources || [],
    sellerMatchedPatterns: sellerIdentity.details?.matchedPatterns || [],
    sellerRawText: sellerIdentity.details?.merchantText || '',
    sellerDetails: {
      detectionSource: sellerIdentity.details?.detectionSource || 'unknown',
      detectionSources: sellerIdentity.details?.detectionSources || [],
      matchedPatterns: sellerIdentity.details?.matchedPatterns || [],
      matchedDirectAmazonPatterns: sellerIdentity.details?.matchedDirectAmazonPatterns || [],
      hasCombinedAmazonMatch: sellerIdentity.details?.hasCombinedAmazonMatch === true,
      merchantText: sellerIdentity.details?.merchantText || '',
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
    rawCurrentPrice,
    invalidPrice: invalidPriceState.invalid,
    invalidPriceReason: invalidPriceState.reason,
    currentPrice: invalidPriceState.invalid ? '' : rawCurrentPrice,
    oldPrice: rawOldPrice,
    priceSource: pricePayload.priceSource || 'unknown',
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
    uploadedImageFile: null,
    telegramImageSource: imagePayload.telegramImageSource || 'none',
    whatsappImageSource: imagePayload.whatsappImageSource || 'none',
    facebookImageSource: 'link_preview',
    enableTelegram: true,
    enableWhatsapp: false,
    enableFacebook: false,
    queueSourceType: 'generator_direct',
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
  sourceHost = ''
} = {}) {
  const reason = cleanText(blockedReason) || 'Produkt nicht verifiziert.';
  if (!isReaderTestGroupAllMode(readerConfig)) {
    console.error('[RAW_SOURCE_POST_BLOCKED]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      blockedCode,
      reason,
      sourceHost: sourceHost || 'unknown'
    });
  }
  if (blockedCode === 'UNVERIFIED_PRODUCT_BLOCKED' && !isReaderTestGroupAllMode(readerConfig)) {
    console.error('[UNVERIFIED_PRODUCT_BLOCKED]', {
      sessionName,
      sourceId: source?.id ?? null,
      messageId: structuredMessage?.messageId || '',
      reason,
      sourceHost: sourceHost || 'unknown'
    });
  }

  if (readerConfig.readerDebugMode === true || readerConfig.readerTestMode === true) {
    const diagnosticText = buildReaderDiagnosticPostTextV2({
      reason,
      sourceHost,
      blockedCode,
      liveAllowed: false,
      testGroupPosted: true
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

async function processTelegramReaderPipeline(sessionName, source, structuredMessage, options = {}) {
  let detectedAsin = extractAsin(structuredMessage.text) || extractAsin(structuredMessage.link) || extractAsin(structuredMessage.externalLink);
  const explicitAmazonLink =
    findAmazonLinkInText(structuredMessage.text) ||
    findAmazonLinkInText(structuredMessage.link) ||
    findAmazonLinkInText(structuredMessage.externalLink);
  let amazonLink = cleanText(explicitAmazonLink) || (detectedAsin ? `https://www.amazon.de/dp/${detectedAsin}` : '');
  const originalLink =
    cleanText(structuredMessage.externalLink) ||
    cleanText(structuredMessage.previewUrl) ||
    cleanText(structuredMessage.link);
  let dealType = resolveReaderDealType({
    amazonLink,
    detectedAsin
  });
  const readerConfig = getReaderConfig();
  const relaxedTestMode = isReaderTestGroupAllMode(readerConfig);
  const trigger = cleanText(options.trigger) || 'reader';
  const sourceHost = normalizeUrlHost(originalLink || amazonLink);
  const sourceProtectionMatches = collectProtectedSourceMatches([
    { source: 'telegramText', value: structuredMessage.text },
    { source: 'previewTitle', value: structuredMessage.previewTitle },
    { source: 'previewDescription', value: structuredMessage.previewDescription }
  ]);
  const protectedSourceDetected = sourceProtectionMatches.length > 0;
  const foreignShortlinkDetected =
    Boolean(originalLink) &&
    normalizeUrlHost(originalLink) !== 'amazon.de' &&
    !/amazon\./i.test(normalizeUrlHost(originalLink)) &&
    /^(?:amzn\.to|s\.[a-z0-9.-]+|[a-z0-9-]+\.[a-z0-9-]+\.[a-z0-9.-]+)$/i.test(sourceHost || '');

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
  console.info('[DEAL_TYPE_DETECTED]', {
    sessionName,
    sourceId: source?.id || null,
    messageId: structuredMessage.messageId,
    dealType
  });
  if (protectedSourceDetected) {
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
    if (dealType === 'AMAZON') {
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
          imageUrl: '',
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
    const scrapedProtectionMatches = collectProtectedSourceMatches([
      { source: 'scrapedTitle', value: scrapedDeal?.title },
      { source: 'scrapedDescription', value: scrapedDeal?.productDescription }
    ]);
    const sourceMeta = {
      sourceHost,
      protectedSource: protectedSourceDetected || scrapedProtectionMatches.length > 0,
      blockedCode: '',
      blockedReason: '',
      matchScore: null,
      matchTier: '',
      relaxedReason: '',
      shortlinkResolved: false,
      shortlinkFallback: false
    };

    if (scrapedProtectionMatches.length) {
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
      dealType === 'AMAZON' || Boolean(cleanText(amazonLink)) || foreignShortlinkDetected === true;

    if (needsAmazonRecovery) {
      if (sourceMeta.protectedSource === true && !relaxedTestMode) {
        console.error('[CLOUDFLARE_SOURCE_BLOCKED]', {
          sessionName,
          sourceId: source.id,
          messageId: structuredMessage.messageId,
          sourceHost: sourceHost || 'unknown'
        });
      }

      const recoveryResult = await searchAmazonProductBySourceData({
        sessionName,
        source,
        structuredMessage,
        scrapedDeal,
        pricing
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
            productDescription: cleanText(scrapedDeal?.productDescription) || cleanText(structuredMessage.previewDescription) || '',
            price: fallbackPrice || '',
            basePrice: '',
            finalPrice: '',
            finalPriceCalculated: false,
            imageUrl: fallbackImage || '',
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
    const generatorInput = buildTelegramReaderGeneratorInput({
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
    if (missingGeneratorFields.length) {
      const missingFieldsReason = `GeneratorInput unvollstaendig. Fehlende Felder: ${missingGeneratorFields.join(', ')}`;
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
          testGroupPosted: true
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
      imageUrl: generatorInput.generatedImagePath || generatorInput.uploadedImagePath,
      source: generatorInput.contextSource,
      origin: generatorInput.originOverride
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

    if (
      relaxedTestMode &&
      (sourceMeta.matchTier === 'review' || sourceMeta.matchTier === 'debug' || productVerification.warningOnly === true)
    ) {
      normalDecision = {
        accepted: false,
        decision: 'review',
        reason:
          cleanText(sourceMeta.relaxedReason) ||
          cleanText(productVerification.reason) ||
          'Testmodus: Deal wird nur mit Warnungen angezeigt.'
      };
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
    console.info('[GENERATOR_FORCE_SUCCESS]', {
      sessionName,
      sourceId: source.id,
      messageId: structuredMessage.messageId,
      asin: generatorInput.asin,
      decision: decisionLabel,
      trigger
    });

    const forceTestGroupPost = options.forceTestGroupPost === true || isReaderTestGroupAllMode(readerConfig);

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

    if (!publishResult?.queue?.id) {
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
  buildReaderCompactDebugBlockV3
};
