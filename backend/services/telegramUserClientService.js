import fs from 'fs';
import path from 'path';
import { Api, TelegramClient } from 'telegram';
import { StringSession } from 'telegram/sessions/index.js';
import QRCode from 'qrcode';
import { getDb } from '../db.js';
import { getTelegramUserReaderConfig } from '../env.js';
import { upsertAppSession } from './databaseService.js';

const db = getDb();
const activeClients = new Map();
const pendingAuthStates = new Map();
const DEFAULT_SESSION_NAME = 'default-user';
const QR_READY_WAIT_MS = 5000;
const QR_READY_POLL_MS = 100;
const MAX_DIALOGS = 80;
const MAX_SYNC_PER_CHANNEL = 15;

function cleanText(value) {
  return typeof value === 'string' ? value.trim() : '';
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

function listWatchedChannels(sessionName = '') {
  const rows = sessionName
    ? db
        .prepare(
          `
            SELECT c.*, s.name AS session_name
            FROM telegram_reader_channels c
            LEFT JOIN telegram_reader_sessions s ON s.id = c.session_id
            WHERE s.name = ?
            ORDER BY c.channel_title COLLATE NOCASE ASC, c.id ASC
          `
        )
        .all(normalizeSessionName(sessionName))
    : db
        .prepare(
          `
            SELECT c.*, s.name AS session_name
            FROM telegram_reader_channels c
            LEFT JOIN telegram_reader_sessions s ON s.id = c.session_id
            ORDER BY c.channel_title COLLATE NOCASE ASC, c.id ASC
          `
        )
        .all();

  return rows.map((row) => ({
    id: row.id,
    sessionName: row.session_name || '',
    channelRef: row.channel_ref,
    channelTitle: row.channel_title || '',
    channelType: row.channel_type || 'group',
    isActive: row.is_active === 1,
    lastSeenMessageId: row.last_seen_message_id || '',
    lastSeenMessageAt: row.last_seen_message_at || null,
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
  const sessionRow = upsertSessionRow({
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
    recentMessages: []
  });
  clearPendingAuthState(sessionName);

  return {
    session: sessionRow,
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

function resolveDialogRef(ref) {
  const normalizedRef = cleanText(ref);
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

  return {
    text,
    link: messageLink,
    group,
    timestamp:
      message?.date instanceof Date
        ? message.date.toISOString()
        : message?.date
          ? new Date(message.date).toISOString()
          : nowIso()
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

function ensureChannelRow(sessionName, input = {}) {
  const session = upsertSessionRow({
    name: sessionName,
    loginMode: input.loginMode || 'phone',
    phoneNumber: input.phoneNumber || ''
  });
  const channelRef = cleanText(input.channelRef);
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

  return listWatchedChannels(sessionName).find((item) => item.channelRef === channelRef) || null;
}

function removeChannelRow(channelId) {
  db.prepare(`DELETE FROM telegram_reader_channels WHERE id = ?`).run(Number(channelId));
}

export async function getTelegramUserClientStatus() {
  const config = getReaderConfig();
  const sessions = listSessionRows();
  const channels = listWatchedChannels();
  const pendingLogins = sessions.map((session) => buildPendingAuthSummary(session.name)).filter(Boolean);

  return {
    configured: Boolean(config.apiId && config.apiHash),
    enabled: config.enabled === true,
    loginMode: config.loginMode,
    sessionDir: config.sessionDir,
    sessions,
    channels,
    pendingLogins
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
  const client = await ensureAuthorizedClient(sessionName);
  const watchedSet = new Set(listWatchedChannels(sessionName).map((item) => item.channelRef));
  const dialogs = await client.getDialogs({
    limit: Math.min(MAX_DIALOGS, Math.max(1, Number(input.limit) || MAX_DIALOGS))
  });

  return dialogs
    .filter((dialog) => Boolean(dialog?.isChannel || dialog?.isGroup))
    .map((dialog) => {
      const username = cleanText(dialog?.entity?.username);
      const channelRef = username ? `@${username}` : String(dialog.id);

      return {
        id: String(dialog.id),
        channelRef,
        title: cleanText(dialog.title) || username || channelRef,
        type: dialog?.isChannel ? 'channel' : 'group',
        username,
        watched: watchedSet.has(channelRef)
      };
    });
}

export async function watchTelegramDialog(input = {}) {
  const sessionName = normalizeSessionName(input.sessionName);
  await ensureAuthorizedClient(sessionName);

  if (!cleanText(input.channelRef)) {
    throw new Error('Dialog-Referenz fehlt.');
  }

  return ensureChannelRow(sessionName, {
    channelRef: cleanText(input.channelRef),
    channelTitle: cleanText(input.channelTitle),
    channelType: cleanText(input.channelType) || 'group',
    isActive: true,
    loginMode: getSessionRowByName(sessionName)?.login_mode || 'phone',
    phoneNumber: getSessionRowByName(sessionName)?.phone_number || ''
  });
}

export function unwatchTelegramDialog(input = {}) {
  removeChannelRow(input.channelId);
  return {
    success: true
  };
}

export async function syncTelegramWatchedMessages(input = {}) {
  const sessionName = normalizeSessionName(input.sessionName);
  const client = await ensureAuthorizedClient(sessionName);
  const watchedChannels = listWatchedChannels(sessionName).filter((item) => item.isActive);
  const resultItems = [];

  for (const channel of watchedChannels) {
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

      const structuredMessage = await formatTelegramMessage(message, channel.channelTitle);
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
          status: session.status || 'connected',
          lastMessageAt: latestSeenAt || null,
          lastError: ''
        });
      }
    }
  }

  resultItems.sort((left, right) => new Date(left.timestamp).getTime() - new Date(right.timestamp).getTime());

  return {
    items: resultItems
  };
}
