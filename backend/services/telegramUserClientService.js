import fs from 'fs';
import path from 'path';
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
import { COUPON_OPTION_LABEL, formatPrice, generatePostText } from '../../frontend/src/lib/postGenerator.js';

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

function cleanText(value) {
  return typeof value === 'string' ? value.trim() : '';
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
  const normalized = cleanText(text).toLowerCase();
  const soldByAmazon =
    /verkauf(?:t)? und versand durch amazon|sold and shipped by amazon|sold by amazon/i.test(normalized);
  const shippedByAmazon =
    soldByAmazon || /versand durch amazon|fulfilled by amazon|dispatches from amazon/i.test(normalized);

  return {
    soldByAmazon,
    shippedByAmazon
  };
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
  const messageLink =
    extractFirstLink(text) ||
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
  const displayDescription = wrapReaderDescription(description || title || 'Amazon Produkt') || 'Amazon Produkt';
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

function resolveReaderPostingStatusLabel(forcedByDebug = false) {
  return forcedByDebug ? 'GEPOSTET / NICHT FÜR LIVE FREIGEGEBEN' : 'GEPOSTET';
}

function formatDebugList(values = []) {
  return Array.isArray(values) && values.length ? values.join(', ') : 'n/a';
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
  const thresholds = buildReaderThresholds(readerConfig, generatorContext);
  const sellerType = normalizeReaderDebugSellerType(generatorInput?.sellerType || scrapedDeal?.sellerType || '');
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
  const wouldPostNormally = normalDecision?.accepted === true;
  const decisionDisplay = wouldPostNormally ? 'POST' : decision === 'REJECT' ? 'REJECT' : 'REVIEW';
  const forcedByDebug =
    (readerConfig.readerDebugMode === true || readerConfig.readerTestMode === true) && normalDecision?.accepted !== true;
  const liveStatus = resolveReaderPostingStatusLabel(forcedByDebug);
  const queueId = DEBUG_QUEUE_ID_PLACEHOLDER;
  const queueStatus = cleanText(generatorContext?.queue?.currentStatus) || 'not_enqueued';
  const lockStatus = generatorContext?.dealLock?.blocked === true ? 'blockiert' : 'frei';
  const reason = cleanText(normalDecision?.reason || readerDecision?.reason || learning?.reason) || 'n/a';
  const priceSource = resolveReaderPriceSourceLabel({
    scrapePrice,
    detectedPrice,
    keepaPrice,
    comparisonPrice
  });
  const comparisonValues = [marketComparisonPrice, keepaReferencePrice, keepaPrice].filter((value) => value !== null);
  const comparisonMin = comparisonValues.length ? Math.min(...comparisonValues) : null;
  const comparisonMax = comparisonValues.length ? Math.max(...comparisonValues) : null;
  const comparisonSource = marketComparisonPrice !== null ? 'Markt' : comparisonValues.length ? 'Keepa' : 'unbekannt';
  const whyKeepaUsed =
    learning.keepaFallbackUsed === true
      ? cleanText(internet.reason || learning.reason || keepa.strengthReason || 'Keepa-Fallback aktiv.')
      : '';
  const whyMarketNotUsed = internet.available === true ? '' : cleanText(internet.reason || internet.status || 'Marktvergleich nicht verfuegbar.');
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
    marketComparisonUsed: generatorContext?.internet?.available === true || learning.internetPrimary === true,
    aiNeeded: learning.aiRequired === true,
    aiUsed: learning.aiRequired === true && learning.worksWithoutAi !== true,
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
    liveStatus,
    keepaPrice,
    keepaReferencePrice,
    marketComparisonPrice,
    comparisonNeeded: internet.available !== true || learning.keepaFallbackUsed === true,
    comparisonMin,
    comparisonMax,
    comparisonSource,
    whyKeepaUsed,
    whyMarketNotUsed,
    missingChecks,
    referencePrice: comparisonPrice,
    scoreComponents: {
      priceAdvantage: keepaDiscount,
      sellerBonusMalus: sellerScoreAdjustment,
      fakeRiskMalus: riskPenalty,
      keepaOrMarketSafety: cleanText(generatorContext?.evaluation?.keepaRating || '') || resolveReaderDecisionSource(generatorContext),
      finalScore
    }
  };

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
  const lines = [];

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

function evaluateTelegramReaderGeneratorCandidate(generatorContext, readerConfig) {
  const learning = generatorContext?.learning || {};
  const evaluation = generatorContext?.evaluation || {};
  const metrics = evaluation?.metrics || {};
  const thresholds = readerConfig?.readerTestThresholds || {};
  const keepaAvailable = generatorContext?.keepa?.available === true;
  const dealLockBlocked = generatorContext?.dealLock?.blocked === true || learning?.dealLockBlocked === true;
  const keepaDiscount = Number(metrics.keepaDiscount);
  const finalScore = Number(metrics.finalScore);
  const fakeDropRisk = Number(metrics.fakeDropRisk);
  const fakeDropClassification = cleanText(metrics.fakeDropClassification || '');
  const minDiscountPercent = Number.isFinite(Number(thresholds.minDiscountPercent)) ? Number(thresholds.minDiscountPercent) : 5;
  const minScore = Number.isFinite(Number(thresholds.minScore)) ? Number(thresholds.minScore) : 20;

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
      reason: 'READER_DEBUG_MODE hat den Reader-Deal ohne Score-/Review-Blocker freigegeben.'
    };
  }

  if (readerConfig?.readerTestMode === true) {
    if (
      learning?.routingDecision === 'test_group' ||
      keepaAvailable ||
      (Number.isFinite(keepaDiscount) && keepaDiscount >= minDiscountPercent) ||
      (Number.isFinite(finalScore) && finalScore >= minScore)
    ) {
      return {
        accepted: true,
        decision: 'test_group',
        reason:
          learning?.reason ||
          (fakeDropClassification === 'wahrscheinlicher_fake_drop' && Number.isFinite(fakeDropRisk)
            ? `Reader Testmodus hat den Deal trotz Fake-Drop Risiko ${fakeDropRisk} fuer den Generator-Publisher freigegeben.`
            : 'Reader Testmodus hat den Deal fuer den Generator-Publisher freigegeben.')
      };
    }

    return {
      accepted: false,
      decision: 'review',
      reason:
        learning?.reason ||
        `Unter Reader-Testschwellen (Discount ${Number.isFinite(keepaDiscount) ? keepaDiscount : 'n/a'} / Score ${
          Number.isFinite(finalScore) ? finalScore : 'n/a'
        }).`
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

function buildTelegramReaderGeneratorInput({
  structuredMessage,
  scrapedDeal,
  normalizedAsin,
  affiliateUrl,
  normalizedUrl,
  couponCode,
  pricing
}) {
  const productDescription = extractReaderProductDescription({
    scrapedDeal,
    structuredMessage
  });
  const savingsOptionLines = buildReaderSavingsOptionLines(scrapedDeal);
  const template = buildTelegramReaderTemplatePayload({
    title: cleanText(scrapedDeal?.title) || extractTelegramTitle(structuredMessage.text, structuredMessage.group),
    description: productDescription,
    affiliateUrl,
    currentPrice:
      cleanText(scrapedDeal?.price) || (pricing?.currentPrice !== null && pricing?.currentPrice !== undefined ? String(pricing.currentPrice) : ''),
    oldPrice:
      cleanText(scrapedDeal?.oldPrice) || (pricing?.oldPrice !== null && pricing?.oldPrice !== undefined ? String(pricing.oldPrice) : ''),
    couponCode,
    extraOptions: savingsOptionLines
  });
  const imageUrl = cleanText(scrapedDeal?.imageUrl);

  return {
    title: cleanText(scrapedDeal?.title) || template.productTitle || extractTelegramTitle(structuredMessage.text, structuredMessage.group),
    link: cleanText(affiliateUrl),
    normalizedUrl: cleanText(normalizedUrl),
    asin: cleanText(normalizedAsin || scrapedDeal?.asin).toUpperCase(),
    sellerType: cleanText(scrapedDeal?.sellerType) || 'FBM',
    productDescription,
    couponDetected: scrapedDeal?.couponDetected === true,
    couponValue: cleanText(scrapedDeal?.couponValue),
    subscribeDetected: scrapedDeal?.subscribeDetected === true,
    subscribeDiscount: cleanText(scrapedDeal?.subscribeDiscount),
    finalPriceCalculated: scrapedDeal?.finalPriceCalculated === true,
    finalPrice: cleanText(scrapedDeal?.finalPrice),
    currentPrice:
      cleanText(scrapedDeal?.price) || (pricing?.currentPrice !== null && pricing?.currentPrice !== undefined ? String(pricing.currentPrice) : ''),
    oldPrice:
      cleanText(scrapedDeal?.oldPrice) || (pricing?.oldPrice !== null && pricing?.oldPrice !== undefined ? String(pricing.oldPrice) : ''),
    couponCode: cleanText(couponCode),
    textByChannel: {
      telegram: template.telegramCaption,
      whatsapp: template.whatsappText,
      facebook: template.whatsappText
    },
    generatedImagePath: imageUrl,
    uploadedImagePath: '',
    uploadedImageFile: null,
    telegramImageSource: imageUrl ? 'standard' : 'none',
    whatsappImageSource: imageUrl ? 'standard' : 'none',
    facebookImageSource: 'link_preview',
    enableTelegram: true,
    enableWhatsapp: false,
    enableFacebook: false,
    queueSourceType: 'generator_direct',
    originOverride: 'automatic',
    contextSource: 'telegram_reader_polling'
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

async function processTelegramReaderPipeline(sessionName, source, structuredMessage, options = {}) {
  const amazonLink = findAmazonLinkInText(structuredMessage.text) || findAmazonLinkInText(structuredMessage.link);
  const readerConfig = getReaderConfig();
  const trigger = cleanText(options.trigger) || 'reader';

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

  if (!amazonLink) {
    logReaderPipelineError('In der Telegram-Nachricht wurde kein Amazon-Link gefunden.', {
      sessionName,
      sourceId: source.id,
      messageId: structuredMessage.messageId
    });
    return {
      accepted: false,
      status: 'skipped',
      reason: 'missing_amazon_link',
      reasonCode: 'missing_amazon_link',
      decision: 'SKIPPED',
      queueId: null,
      queueStatus: '',
      messageId: null,
      postedToTestGroup: false,
      forcedToTestGroup: false,
      trigger
    };
  }

  console.info('[GENERATOR_FORMAT_START]', {
    sessionName,
    sourceId: source.id,
    messageId: structuredMessage.messageId,
    amazonLink,
    readerTestMode: readerConfig.readerTestMode === true,
    readerDebugMode: readerConfig.readerDebugMode === true
  });
  console.info('[GENERATOR_FORCE_START]', {
    sessionName,
    sourceId: source.id,
    messageId: structuredMessage.messageId,
    amazonLink,
    trigger
  });

  try {
    const pricing = extractTelegramDealPricing(structuredMessage.text);
    const couponCode = extractTelegramCouponCode(structuredMessage.text);
    if (isAmazonShortLink(amazonLink)) {
      console.info('[AUTOMATION_SHORTLINK_BLOCKED]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        originalUrl: amazonLink
      });
    }
    let scrapedDeal;
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
        title: extractTelegramTitle(structuredMessage.text, structuredMessage.group),
        imageUrl: '',
        price:
          pricing?.currentPrice !== null && pricing?.currentPrice !== undefined ? String(pricing.currentPrice) : '9.99',
        oldPrice:
          pricing?.oldPrice !== null && pricing?.oldPrice !== undefined ? String(pricing.oldPrice) : '',
        asin: '',
        finalUrl: amazonLink,
        resolvedUrl: amazonLink,
        originalUrl: amazonLink,
        normalizedUrl: amazonLink,
        sellerType: 'unbekannt',
        imageDebug: {
          paapiStatus: 'fallback_after_scrape_error'
        }
      };
    }
    const resolvedAmazonUrl =
      cleanText(scrapedDeal?.finalUrl) || cleanText(scrapedDeal?.resolvedUrl) || cleanText(scrapedDeal?.normalizedUrl);
    const linkRecord = buildAmazonAffiliateLinkRecord(amazonLink, {
      resolvedUrl: resolvedAmazonUrl,
      asin: cleanText(scrapedDeal?.asin)
    });

    if (!linkRecord.valid || !cleanText(linkRecord.affiliateUrl) || !cleanText(linkRecord.asin)) {
      const affiliateLinkErrorReason = 'ASIN fehlt oder Partnerlink konnte nicht gebaut werden.';
      logReaderPipelineError(affiliateLinkErrorReason, {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        amazonLink,
        resolvedAmazonUrl,
        scrapedAsin: cleanText(scrapedDeal?.asin)
      });
      logTelegramReaderEvent({
        level: 'warning',
        eventType: 'telegram.generator.review',
        sourceId: source.id,
        message: affiliateLinkErrorReason,
        payload: {
          sessionName,
          messageId: structuredMessage.messageId,
          group: structuredMessage.group,
          amazonLink,
          resolvedAmazonUrl,
          scrapedAsin: cleanText(scrapedDeal?.asin)
        }
      });

      return {
        accepted: false,
        status: 'review',
        review: true,
        reason: affiliateLinkErrorReason,
        reasonCode: 'affiliate_link_error',
        decision: 'REVIEW',
        queueId: null,
        queueStatus: '',
        messageId: null,
        postedToTestGroup: false,
        forcedToTestGroup: false,
        trigger
      };
    }

    const affiliateUrl = cleanText(linkRecord.affiliateUrl);
    const normalizedUrl = cleanText(linkRecord.normalizedUrl);
    const normalizedAsin = resolveNormalizedReaderAsin({
      amazonLink,
      scrapedDeal,
      linkRecord
    });
    const normalizedScrapedDeal = {
      ...scrapedDeal,
      asin: normalizedAsin || cleanText(scrapedDeal?.asin).toUpperCase(),
      normalizedUrl: normalizedUrl || cleanText(scrapedDeal?.normalizedUrl),
      finalUrl: resolvedAmazonUrl || cleanText(scrapedDeal?.finalUrl),
      resolvedUrl: resolvedAmazonUrl || cleanText(scrapedDeal?.resolvedUrl)
    };
    const generatorInput = buildTelegramReaderGeneratorInput({
      structuredMessage,
      scrapedDeal: normalizedScrapedDeal,
      normalizedAsin,
      affiliateUrl,
      normalizedUrl,
      couponCode,
      pricing
    });
    const generatorContext = await buildGeneratorDealContext({
      asin: generatorInput.asin,
      sellerType: generatorInput.sellerType,
      currentPrice: generatorInput.currentPrice,
      title: generatorInput.title,
      productUrl: generatorInput.normalizedUrl || generatorInput.link,
      imageUrl: generatorInput.generatedImagePath,
      source: generatorInput.contextSource,
      origin: generatorInput.originOverride
    });
    const normalDecision = evaluateTelegramReaderGeneratorCandidate(generatorContext, {
      ...readerConfig,
      readerDebugMode: false
    });
    const readerDecision = evaluateTelegramReaderGeneratorCandidate(generatorContext, readerConfig);
    const debugPostEnabled = readerConfig.readerDebugMode === true || readerConfig.readerTestMode === true;
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
      generatorInput.textByChannel.telegram = `${generatorInput.textByChannel.telegram || ''}${buildReaderCompactDebugBlockV2(debugValues)}`;
      console.info('[GENERATOR_STYLE_POST]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        asin: generatorInput.asin,
        hasImage: Boolean(generatorInput.generatedImagePath),
        captionLength: cleanText(generatorInput.textByChannel.telegram).length
      });
    }

    if (!debugPostEnabled) {
      console.info('[GENERATOR_STYLE_POST]', {
        sessionName,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        asin: generatorInput.asin,
        hasImage: Boolean(generatorInput.generatedImagePath),
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
      hasImage: Boolean(generatorInput.generatedImagePath),
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

    const forceTestGroupPost =
      options.forceTestGroupPost === true ||
      ((readerConfig.readerDebugMode === true || readerConfig.readerTestMode === true) && normalDecision?.accepted !== true);

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
      const fallbackQueueEntry = await enqueueTelegramReaderOutput({
        source,
        structuredMessage,
        amazonLink,
        importedDealId: null,
        pipelineStatus: 'debug_raw_fallback',
        pipelineReason: errorMessage
      });

      console.info('[PUBLISHER_TRIGGERED]', {
        queueId: fallbackQueueEntry?.id || null,
        sourceId: source.id,
        messageId: structuredMessage.messageId,
        trigger: 'debug_raw_fallback'
      });

      await processPublishingQueueEntry(fallbackQueueEntry.id);

      return {
        accepted: true,
        status: 'debug_raw_fallback',
        reason: errorMessage,
        queueId: fallbackQueueEntry?.id || null
      };
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

  if (!active?.client || active.client !== client) {
    return {
      skipped: true,
      reason: 'inactive_client'
    };
  }

  if (active.pollingInFlight) {
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

  console.info('[READER_LOOP_START]', {
    trigger: 'polling',
    sessionName: normalizedSessionName,
    dialogCount: watchedChannels.length,
    startedAt: pollStartedAt
  });
  console.info('[TELEGRAM_POLL_TICK]', {
    sessionName: normalizedSessionName,
    trigger: cleanText(options.trigger) || 'interval',
    dialogCount: watchedChannels.length,
    startedAt: pollStartedAt
  });
  console.info('[TELEGRAM_POLL_START]', {
    sessionName: normalizedSessionName,
    trigger: cleanText(options.trigger) || 'interval',
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
