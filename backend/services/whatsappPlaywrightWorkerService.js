import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import QRCode from 'qrcode';
import { getWhatsappPlaywrightConfig } from '../env.js';
import { getDb } from '../db.js';
import { upsertAppSession } from './databaseService.js';

const db = getDb();
const WHATSAPP_WEB_URL = 'https://web.whatsapp.com/';
const DEFAULT_WHATSAPP_TEST_CHANNEL_ID = '0029VbCsyVY7NoZryjRrBU2P';
const DEFAULT_WHATSAPP_TEST_CHANNEL_DISPLAY_NAME = 'Aff.Manager Tests Output';
const DESKTOP_VIEWPORT = {
  width: 1920,
  height: 1400
};
const DEFAULT_BROWSER_PATHS = {
  msedge: [
    'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe',
    'C:\\Program Files\\Microsoft\\Edge\\Application\\msedge.exe'
  ],
  chrome: [
    'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',
    'C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe'
  ]
};
const PHASE_SESSION_KEY_PREFIX = 'whatsapp_output:phase_send:';
const WHATSAPP_RUNTIME_SESSION_KEY = 'whatsapp_output:session:default';
const WHATSAPP_WORKER_OWNER_SESSION_KEY = 'whatsapp_output:playwright_owner:default';
const WHATSAPP_BLOCKED_EXTERNAL_PROTOCOLS = ['whatsapp://', 'whatsapp-desktop://', 'ms-windows-store://'];
const WHATSAPP_OPEN_IN_APP_TEXT_HINTS = [
  'Open in WhatsApp',
  'View in WhatsApp',
  'Continue to WhatsApp',
  'Continue in Browser',
  'View channel',
  'Open channel',
  'In WhatsApp ansehen',
  'In WhatsApp oeffnen',
  'Weiter im Browser',
  'Kanal ansehen'
];

let automationOverride = null;
const runtimeState = {
  launchPromise: null,
  loginMonitorPromise: null,
  loginMonitorAbort: false,
  session: null,
  sessionId: '',
  browserStatus: 'stopped'
};

function nowIso() {
  return new Date().toISOString();
}

function cleanText(value = '') {
  return typeof value === 'string' ? value.trim() : '';
}

function parseJson(value, fallback = null) {
  if (!value) {
    return fallback;
  }

  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function parseBool(value, fallback = false) {
  if (value === true || value === false) {
    return value;
  }
  if (value === 1 || value === '1' || value === 'true') {
    return true;
  }
  if (value === 0 || value === '0' || value === 'false') {
    return false;
  }
  return fallback;
}

function parseInteger(value, fallback = 0) {
  const parsed = Number.parseInt(String(value ?? ''), 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function buildWhatsappPlaywrightError(message, options = {}) {
  const error = new Error(message);
  error.code = cleanText(options.code) || 'WHATSAPP_PLAYWRIGHT_ERROR';
  error.retryable = options.retryable !== false;
  return error;
}

function ensureDirectory(targetPath = '') {
  if (!targetPath) {
    return;
  }

  fs.mkdirSync(targetPath, { recursive: true });
}

function logPlaywrightWorkerEvent(eventType = '', message = '', payload = null, level = 'info') {
  db.prepare(
    `
      INSERT INTO publishing_logs (
        queue_id,
        target_id,
        worker_type,
        level,
        event_type,
        message,
        payload_json,
        created_at
      ) VALUES (NULL, NULL, 'whatsapp', ?, ?, ?, ?, ?)
    `
  ).run(level, cleanText(eventType) || 'whatsapp.playwright', cleanText(message) || 'WhatsApp Playwright Event', payload ? JSON.stringify(payload) : null, nowIso());
}

function isKeepBrowserOpenEnabled(providerInfo = null) {
  const config = providerInfo?.config || getWhatsappPlaywrightConfig();
  return config?.keepBrowserOpen === true;
}

function readAppSession(sessionKey = '') {
  return db.prepare(`SELECT * FROM app_sessions WHERE session_key = ? LIMIT 1`).get(sessionKey) || null;
}

function readRuntimeMeta() {
  const row = readAppSession(WHATSAPP_RUNTIME_SESSION_KEY);
  return parseJson(row?.meta_json, {}) || {};
}

function persistRuntimePatch(patch = {}, status = '') {
  const providerInfo = buildProviderInfo();
  const currentMeta = readRuntimeMeta();
  const nextMeta = {
    ...currentMeta,
    ...patch
  };
  const nextStatus = cleanText(status) || cleanText(nextMeta.connectionStatus) || 'not_connected';

  return upsertAppSession({
    sessionKey: WHATSAPP_RUNTIME_SESSION_KEY,
    module: 'whatsapp_output',
    sessionType: 'worker',
    status: nextStatus,
    storagePath: cleanText(providerInfo.config?.sessionDir),
    externalRef: 'primary',
    meta: nextMeta,
    lastSeenAt: nowIso()
  });
}

function isProcessAlive(pid) {
  const numericPid = parseInteger(pid, 0);
  if (numericPid <= 0) {
    return false;
  }

  try {
    process.kill(numericPid, 0);
    return true;
  } catch (error) {
    return error?.code === 'EPERM';
  }
}

function getStorageStatePath(config = getWhatsappPlaywrightConfig()) {
  const sessionDir = cleanText(config.sessionDir);
  ensureDirectory(sessionDir);
  return path.join(sessionDir, 'storage-state.json');
}

function getProfileResetBackupPath(config = getWhatsappPlaywrightConfig()) {
  const sessionDir = cleanText(config.sessionDir);
  ensureDirectory(sessionDir);
  const stamp = nowIso().replace(/[:.]/g, '-');
  return path.join(sessionDir, `browser-profile-reset-${stamp}`);
}

function ensureProfileWritable(profileDir = '') {
  try {
    ensureDirectory(profileDir);
    const probePath = path.join(profileDir, '.write-test');
    fs.writeFileSync(probePath, `${process.pid}`, 'utf8');
    fs.unlinkSync(probePath);
    return true;
  } catch {
    return false;
  }
}

function readWorkerOwnerState() {
  const row = readAppSession(WHATSAPP_WORKER_OWNER_SESSION_KEY);
  return {
    row,
    meta: parseJson(row?.meta_json, {}) || {}
  };
}

function claimWorkerOwnership(providerInfo) {
  const profileDir = getProfileDirectory(providerInfo.config);
  const { row, meta } = readWorkerOwnerState();
  const ownerPid = parseInteger(meta.pid, 0);
  const ownerStatus = cleanText(row?.status).toLowerCase();

  if (ownerStatus === 'running' && ownerPid > 0 && ownerPid !== process.pid && isProcessAlive(ownerPid)) {
    logPlaywrightWorkerEvent(
      'whatsapp.worker.multiple_detected',
      `[WHATSAPP_MULTIPLE_WORKERS_DETECTED] Bereits laufender WhatsApp Worker mit PID ${ownerPid} erkannt.`,
      {
        ownerPid,
        requestedPid: process.pid,
        profileDir
      },
      'warning'
    );
    throw buildWhatsappPlaywrightError('Es laeuft bereits ein anderer WhatsApp Worker mit demselben Browser-Profil.', {
      code: 'WHATSAPP_MULTIPLE_WORKERS_DETECTED',
      retryable: false
    });
  }

  upsertAppSession({
    sessionKey: WHATSAPP_WORKER_OWNER_SESSION_KEY,
    module: 'whatsapp_output',
    sessionType: 'playwright_owner',
    status: 'running',
    storagePath: profileDir,
    externalRef: 'primary',
    meta: {
      pid: process.pid,
      profileDir,
      executablePath: cleanText(providerInfo.executablePath),
      claimedAt: nowIso()
    },
    lastSeenAt: nowIso()
  });

  const profileWritable = ensureProfileWritable(profileDir);
  logPlaywrightWorkerEvent(
    'whatsapp.profile.path',
    `[WHATSAPP_PROFILE_PATH] ${profileDir}`,
    {
      profileDir,
      executablePath: cleanText(providerInfo.executablePath),
      headless: providerInfo.config?.headless === true,
      profileWritable
    }
  );
  persistRuntimePatch(
    {
      browserChannel: cleanText(providerInfo.browserChannel),
      browserProfileDir: profileDir,
      browserExecutablePath: cleanText(providerInfo.executablePath),
      workerPid: process.pid,
      profileWritable,
      loginTimeoutMs: resolveLoginTimeoutMs(providerInfo)
    },
    'not_connected'
  );
}

function releaseWorkerOwnership() {
  const row = readAppSession(WHATSAPP_WORKER_OWNER_SESSION_KEY);
  if (!row) {
    return;
  }

  db.prepare(
    `
      UPDATE app_sessions
      SET status = 'stopped',
          last_seen_at = ?,
          updated_at = ?
      WHERE session_key = ?
    `
  ).run(nowIso(), nowIso(), WHATSAPP_WORKER_OWNER_SESSION_KEY);
}

function hasClosedSession(session) {
  return Boolean(session?.context?.isClosed?.() || session?.page?.isClosed?.());
}

function resolveLoginTimeoutMs(providerInfo) {
  const configuredTimeout = Number(providerInfo?.config?.loginTimeoutMs || 120000);
  if (automationOverride) {
    return Math.max(100, configuredTimeout);
  }

  return Math.max(120000, configuredTimeout);
}

function resolveBrowserPath(config = getWhatsappPlaywrightConfig()) {
  const explicitPath = cleanText(config.executablePath);
  if (explicitPath && fs.existsSync(explicitPath)) {
    return explicitPath;
  }

  const preferredChannel = cleanText(config.browserChannel).toLowerCase();
  const channelCandidates = preferredChannel ? DEFAULT_BROWSER_PATHS[preferredChannel] || [] : [];
  const fallbackCandidates = [...channelCandidates, ...DEFAULT_BROWSER_PATHS.chrome, ...DEFAULT_BROWSER_PATHS.msedge];
  return fallbackCandidates.find((candidate) => fs.existsSync(candidate)) || '';
}

function getProfileDirectory(config = getWhatsappPlaywrightConfig()) {
  const sessionDir = cleanText(config.sessionDir);
  ensureDirectory(sessionDir);
  const profileDir = path.join(sessionDir, 'browser-profile');
  ensureDirectory(profileDir);
  return profileDir;
}

function getTempDirectory(config = getWhatsappPlaywrightConfig()) {
  const sessionDir = cleanText(config.sessionDir);
  ensureDirectory(sessionDir);
  const tempDir = path.join(sessionDir, 'worker-temp');
  ensureDirectory(tempDir);
  return tempDir;
}

function getPhaseSessionKey(sendId = '') {
  const normalizedSendId = cleanText(sendId);
  return normalizedSendId ? `${PHASE_SESSION_KEY_PREFIX}${normalizedSendId}` : '';
}

function readPhaseSession(sendId = '') {
  const sessionKey = getPhaseSessionKey(sendId);
  if (!sessionKey) {
    return null;
  }

  const row = db.prepare(`SELECT * FROM app_sessions WHERE session_key = ? LIMIT 1`).get(sessionKey) || null;
  if (!row?.meta_json) {
    return null;
  }

  try {
    return JSON.parse(row.meta_json);
  } catch {
    return null;
  }
}

export function getRememberedWhatsappPhaseDelivery(sendId = '') {
  return readPhaseSession(sendId);
}

export function rememberWhatsappPhaseDelivery(sendId = '', meta = {}) {
  const normalizedSendId = cleanText(sendId);
  if (!normalizedSendId) {
    return null;
  }

  return upsertAppSession({
    sessionKey: getPhaseSessionKey(normalizedSendId),
    module: 'whatsapp_output',
    sessionType: 'phase_send',
    status: 'sent',
    externalRef: normalizedSendId,
    storagePath: getProfileDirectory(),
    meta: {
      ...meta,
      sendId: normalizedSendId,
      storedAt: nowIso()
    },
    lastSeenAt: nowIso()
  });
}

function getAutomationAdapter() {
  const defaults = {
    startSession: defaultStartSession,
    stopSession: defaultStopSession,
    inspectSession: defaultInspectSession,
    refreshSession: defaultRefreshSession,
    testChannel: defaultTestChannel,
    debugChannel: defaultDebugChannel,
    manualChannelDebugCapture: defaultManualChannelDebugCapture,
    manualChannelDebugWait: defaultManualChannelDebugWait,
    sendPhase: defaultSendPhase
  };

  return automationOverride ? { ...defaults, ...automationOverride } : defaults;
}

async function loadPlaywrightModule() {
  try {
    return await import('playwright-core');
  } catch (error) {
    throw buildWhatsappPlaywrightError(
      `Playwright-Core ist nicht installiert: ${error instanceof Error ? error.message : 'Import fehlgeschlagen.'}`,
      {
        code: 'WHATSAPP_PLAYWRIGHT_UNAVAILABLE',
        retryable: false
      }
    );
  }
}

async function navigateTo(page, targetUrl, timeoutMs) {
  try {
    await page.goto(targetUrl, {
      waitUntil: 'domcontentloaded',
      timeout: timeoutMs
    });
  } catch {
    await page.waitForLoadState('domcontentloaded', { timeout: Math.max(2500, Math.round(timeoutMs / 2)) }).catch(() => null);
  }
}

async function waitForSettledUi(page) {
  await page.waitForTimeout(900).catch(() => null);
  await page.waitForLoadState('networkidle', { timeout: 3500 }).catch(() => null);
}

function normalizeChannelUrl(targetRef = '', targetMeta = {}) {
  const explicitUrl = cleanText(targetMeta?.channelUrl || targetMeta?.targetRef || targetRef);
  if (!explicitUrl) {
    throw buildWhatsappPlaywrightError('WhatsApp Kanal-URL fehlt.', {
      code: 'WHATSAPP_CHANNEL_URL_MISSING',
      retryable: false
    });
  }

  const normalizedExplicitUrl = explicitUrl.replace(/\/+$/, '');
  const channelMatch = normalizedExplicitUrl.match(
    /^https?:\/\/(?:www\.)?(?:web\.)?whatsapp\.com\/channel\/([^/?#]+)/i
  );
  if (channelMatch?.[1]) {
    return `https://web.whatsapp.com/channel/${channelMatch[1]}`;
  }

  if (/^https?:\/\//i.test(explicitUrl)) {
    return explicitUrl;
  }

  return `https://web.whatsapp.com/channel/${explicitUrl.replace(/^\/+/, '')}`;
}

function normalizeComparableText(value = '') {
  return cleanText(value)
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase();
}

function dedupeTextValues(values = []) {
  return Array.from(
    new Set(
      values
        .map((value) => cleanText(value))
        .filter(Boolean)
    )
  );
}

function extractWhatsappChannelId(targetRef = '', targetMeta = {}) {
  const explicitUrl = cleanText(targetMeta?.channelUrl || targetMeta?.targetRef || targetRef).replace(/\/+$/, '');
  if (!explicitUrl) {
    return '';
  }

  const urlMatch = explicitUrl.match(/(?:^|\/)channel\/([^/?#]+)/i);
  if (urlMatch?.[1]) {
    return cleanText(urlMatch[1]);
  }

  if (/^[0-9A-Za-z_-]{10,}$/.test(explicitUrl)) {
    return explicitUrl;
  }

  return '';
}

function extractChannelNameAlias(value = '') {
  const normalized = cleanText(value).replace(/\s+/g, ' ');
  if (!normalized) {
    return '';
  }

  const patterns = [
    /nachricht an\s+(.+?)\s+schreiben/i,
    /message to\s+(.+?)(?:\s+(?:write|send)|$)/i,
    /^(.+?)\s+(?:schreiben|write|send)$/i
  ];

  for (const pattern of patterns) {
    const match = normalized.match(pattern);
    const alias = cleanText(match?.[1] || '');
    if (alias) {
      return alias;
    }
  }

  return '';
}

function buildChannelLookupAliases(targetRef = '', targetMeta = {}) {
  const aliases = [];
  const channelId = extractWhatsappChannelId(targetRef, targetMeta);
  const runtimeMeta = readRuntimeMeta();
  const runtimeTargetRef = cleanText(runtimeMeta.lastChannelTargetRef);

  if (
    runtimeTargetRef &&
    channelId &&
    extractWhatsappChannelId(runtimeTargetRef, {
      channelUrl: runtimeTargetRef
    }) === channelId
  ) {
    const candidates = Array.isArray(runtimeMeta.lastChannelComposerCandidates)
      ? runtimeMeta.lastChannelComposerCandidates
      : [];

    for (const candidate of candidates) {
      aliases.push(
        extractChannelNameAlias(candidate?.ariaLabel),
        extractChannelNameAlias(candidate?.placeholder),
        cleanText(candidate?.title)
      );
    }
  }

  if (channelId === DEFAULT_WHATSAPP_TEST_CHANNEL_ID) {
    aliases.push(DEFAULT_WHATSAPP_TEST_CHANNEL_DISPLAY_NAME);
  }

  return dedupeTextValues(aliases);
}

function buildWhatsappChannelPlan(targetRef = '', targetMeta = {}) {
  const explicitUrl = cleanText(targetMeta?.channelUrl || targetMeta?.targetRef || targetRef);
  if (!explicitUrl) {
    throw buildWhatsappPlaywrightError('WhatsApp Kanal-URL fehlt.', {
      code: 'WHATSAPP_CHANNEL_URL_MISSING',
      retryable: false
    });
  }

  const channelId = extractWhatsappChannelId(targetRef, targetMeta);
  const normalizedUrl = normalizeChannelUrl(targetRef, targetMeta);
  const externalChannelUrl = channelId ? `https://whatsapp.com/channel/${channelId}` : explicitUrl;
  const lookupAliases = buildChannelLookupAliases(targetRef, targetMeta);
  const lookupTerms = dedupeTextValues([
    targetMeta?.targetLabel,
    targetMeta?.name,
    targetMeta?.channelName,
    targetMeta?.targetRef,
    targetRef,
    explicitUrl,
    channelId,
    ...lookupAliases
  ]);

  return {
    explicitUrl,
    normalizedUrl,
    webChannelUrl: normalizedUrl,
    externalChannelUrl,
    channelId,
    targetLabel: cleanText(targetMeta?.targetLabel || targetMeta?.name || targetRef || explicitUrl),
    lookupTerms
  };
}

function isWebWhatsappUrl(url = '') {
  return /^https?:\/\/web\.whatsapp\.com(?:\/|$)/i.test(cleanText(url));
}

function classifyWhatsappWebOnlyTarget(candidate = {}) {
  const href = cleanText(candidate.href || candidate.url);
  const text = cleanText(candidate.text || candidate.label || candidate.title);
  const normalizedHref = href.toLowerCase();
  const normalizedText = normalizeComparableText(text);
  const blockedProtocol = WHATSAPP_BLOCKED_EXTERNAL_PROTOCOLS.find((protocol) => normalizedHref.startsWith(protocol));
  const openInAppText = WHATSAPP_OPEN_IN_APP_TEXT_HINTS.some((hint) =>
    normalizedText.includes(normalizeComparableText(hint))
  );
  const externalLanding = Boolean(href) && /^https?:\/\/(?:www\.)?whatsapp\.com\//i.test(href) && !isWebWhatsappUrl(href);

  if (blockedProtocol) {
    const isDesktopAppTarget =
      blockedProtocol === 'whatsapp-desktop://' || blockedProtocol === 'ms-windows-store://';
    return {
      blocked: true,
      reason: isDesktopAppTarget ? 'desktop_app' : 'deep_link',
      href,
      text,
      blockedProtocol,
      logCode: isDesktopAppTarget ? 'WHATSAPP_DESKTOP_APP_NOT_ALLOWED' : 'WHATSAPP_DEEP_LINK_BLOCKED'
    };
  }

  if (externalLanding || openInAppText) {
    return {
      blocked: true,
      reason: 'open_in_app',
      href,
      text,
      logCode: 'WHATSAPP_OPEN_IN_APP_BUTTON_SKIPPED'
    };
  }

  return {
    blocked: false,
    reason: '',
    href,
    text,
    logCode: ''
  };
}

function buildChannelDebugRunId(plan = {}) {
  const base = cleanText(plan.channelId || plan.targetLabel || 'channel')
    .replace(/[^a-zA-Z0-9_-]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '')
    .toLowerCase();

  return `${base || 'channel'}-${Date.now()}`;
}

function buildChannelDebugContext(plan = {}, options = {}) {
  return {
    enabled: options.debug === true,
    runId: cleanText(options.debugRunId) || buildChannelDebugRunId(plan),
    artifacts: []
  };
}

function isWhatsappChannelTarget(targetRef = '', targetMeta = {}) {
  const explicitUrl = cleanText(targetMeta?.channelUrl || targetMeta?.targetRef || targetRef);
  const targetType = cleanText(targetMeta?.targetType).toUpperCase();

  return (
    /^https?:\/\/(?:www\.)?(?:web\.)?whatsapp\.com\/channel\//i.test(explicitUrl) ||
    /^channel\//i.test(explicitUrl) ||
    targetType === 'WHATSAPP_CHANNEL' ||
    targetType === 'WHATSAPP_TEST_CHANNEL'
  );
}

async function clickVisibleActionByText(page, selectors = [], textHints = [], timeoutMs = 2500) {
  const locator = page.locator(selectors.join(', '));
  const normalizedHints = textHints.map((value) => normalizeComparableText(value)).filter(Boolean);
  const startedAt = Date.now();

  while (Date.now() - startedAt <= timeoutMs) {
    const count = await locator.count().catch(() => 0);
    for (let index = 0; index < count; index += 1) {
      const candidate = locator.nth(index);
      const visible = await candidate.isVisible().catch(() => false);
      if (!visible) {
        continue;
      }

      const candidateText = await candidate
        .evaluate((element) => {
          const values = [
            element.innerText,
            element.textContent,
            element.getAttribute('aria-label'),
            element.getAttribute('title'),
            element.getAttribute('href')
          ];
          return values.filter(Boolean).join(' ').replace(/\s+/g, ' ').trim();
        })
        .catch(() => '');
      const normalizedCandidateText = normalizeComparableText(candidateText);

      if (!normalizedCandidateText) {
        continue;
      }

      if (!normalizedHints.some((hint) => normalizedCandidateText.includes(hint))) {
        continue;
      }

      await candidate.scrollIntoViewIfNeeded().catch(() => null);
      await candidate.click().catch(() => null);
      return {
        matchedText: candidateText
      };
    }

    await page.waitForTimeout(250).catch(() => null);
  }

  return null;
}

async function findBlockedOpenInAppAction(page, timeoutMs = 1500) {
  const locator = page.locator('a, button, [role="button"]');
  const startedAt = Date.now();

  while (Date.now() - startedAt <= timeoutMs) {
    const count = await locator.count().catch(() => 0);
    for (let index = 0; index < count; index += 1) {
      const candidate = locator.nth(index);
      const visible = await candidate.isVisible().catch(() => false);
      if (!visible) {
        continue;
      }

      const meta = await candidate
        .evaluate((element) => {
          const href =
            element.getAttribute('href') ||
            element.getAttribute('data-href') ||
            element.getAttribute('data-link') ||
            '';
          const text = [
            element.innerText,
            element.textContent,
            element.getAttribute('aria-label'),
            element.getAttribute('title')
          ]
            .filter(Boolean)
            .join(' ')
            .replace(/\s+/g, ' ')
            .trim();
          return { href, text };
        })
        .catch(() => ({ href: '', text: '' }));
      const decision = classifyWhatsappWebOnlyTarget(meta);
      if (decision.blocked) {
        return decision;
      }
    }

    await page.waitForTimeout(150).catch(() => null);
  }

  return null;
}

function logWebOnlyMode(providerInfo = null, payload = null) {
  const config = providerInfo?.config || getWhatsappPlaywrightConfig();
  logPlaywrightWorkerEvent(
    'whatsapp.web_only_mode',
    '[WHATSAPP_WEB_ONLY_MODE] WhatsApp Worker bleibt ausschliesslich auf web.whatsapp.com und nutzt keine Desktop-App-Links.',
    {
      webUrl: cleanText(config.webUrl) || WHATSAPP_WEB_URL,
      blockedProtocols: WHATSAPP_BLOCKED_EXTERNAL_PROTOCOLS,
      ...(payload && typeof payload === 'object' ? payload : {})
    }
  );
}

function logBlockedExternalAction(decision = {}, payload = {}) {
  const href = cleanText(decision.href);
  const text = cleanText(decision.text);
  const basePayload = {
    href,
    text,
    reason: cleanText(decision.reason),
    blockedProtocol: cleanText(decision.blockedProtocol),
    ...(payload && typeof payload === 'object' ? payload : {})
  };

  if (decision.reason === 'deep_link') {
    logPlaywrightWorkerEvent(
      'whatsapp.external_app_blocked',
      `[WHATSAPP_EXTERNAL_APP_BLOCKED] Externer WhatsApp Deep Link wurde blockiert: ${href || text}.`,
      basePayload,
      'warning'
    );
    logPlaywrightWorkerEvent(
      'whatsapp.deep_link_blocked',
      `[WHATSAPP_DEEP_LINK_BLOCKED] ${href || text}`,
      basePayload,
      'warning'
    );
    return;
  }

  if (decision.reason === 'desktop_app') {
    logPlaywrightWorkerEvent(
      'whatsapp.external_app_blocked',
      `[WHATSAPP_EXTERNAL_APP_BLOCKED] Desktop-App-Ziel wurde blockiert: ${href || text}.`,
      basePayload,
      'warning'
    );
    logPlaywrightWorkerEvent(
      'whatsapp.desktop_app_not_allowed',
      `[WHATSAPP_DESKTOP_APP_NOT_ALLOWED] ${href || text}`,
      basePayload,
      'warning'
    );
    return;
  }

  if (decision.reason === 'open_in_app') {
    logPlaywrightWorkerEvent(
      'whatsapp.open_in_app_button_skipped',
      `[WHATSAPP_OPEN_IN_APP_BUTTON_SKIPPED] ${text || href || 'Open-in-App Aktion'} wurde bewusst uebersprungen.`,
      basePayload,
      'warning'
    );
  }
}

async function installWebOnlyModeGuards(context, providerInfo) {
  if (!context || automationOverride) {
    return;
  }

  await context
    .addInitScript(
      ({ blockedProtocols }) => {
        const normalize = (value) => String(value || '').trim().toLowerCase();
        const shouldBlock = (value) => blockedProtocols.some((protocol) => normalize(value).startsWith(protocol));
        const pushBlocked = (value) => {
          window.__waBlockedExternalUrls = window.__waBlockedExternalUrls || [];
          window.__waBlockedExternalUrls.push(String(value || ''));
        };
        const originalWindowOpen = window.open;
        window.open = function patchedWindowOpen(url, ...args) {
          if (shouldBlock(url)) {
            pushBlocked(url);
            return null;
          }
          return originalWindowOpen.call(window, url, ...args);
        };

        document.addEventListener(
          'click',
          (event) => {
            const element = event.target?.closest?.('a[href], button[data-href], [role="button"][data-href]');
            const href =
              element?.getAttribute?.('href') ||
              element?.getAttribute?.('data-href') ||
              element?.getAttribute?.('data-link') ||
              '';
            if (shouldBlock(href)) {
              pushBlocked(href);
              event.preventDefault();
              event.stopImmediatePropagation();
            }
          },
          true
        );
      },
      {
        blockedProtocols: WHATSAPP_BLOCKED_EXTERNAL_PROTOCOLS
      }
    )
    .catch(() => null);

  context.on('page', (popup) => {
    setTimeout(async () => {
      await popup.waitForLoadState('domcontentloaded', { timeout: 1500 }).catch(() => null);
      const decision = classifyWhatsappWebOnlyTarget({
        href: popup.url()
      });
      if (!decision.blocked) {
        return;
      }

      logBlockedExternalAction(decision, {
        source: 'context.page',
        currentUrl: popup.url()
      });
      await popup.close().catch(() => null);
    }, 0);
  });

  logWebOnlyMode(providerInfo, {
    source: 'context.init'
  });
}

async function extractDomState(page) {
  return await page.evaluate(() => {
    const isVisible = (element) => {
      if (!element) {
        return false;
      }

      const style = window.getComputedStyle(element);
      const rect = element.getBoundingClientRect();
      return style.visibility !== 'hidden' && style.display !== 'none' && rect.width > 0 && rect.height > 0;
    };

    const pickText = (value) => String(value || '').trim();
    const qrRefElement = [...document.querySelectorAll('[data-ref]')].find((element) => pickText(element.getAttribute('data-ref')).length > 16);
    const canvasVisible = [...document.querySelectorAll('canvas')].some((element) => isVisible(element));
    const connectedSelectors = [
      '#pane-side',
      '[data-testid="chat-list-search"]',
      '[data-testid="menu"]',
      '[data-testid="chat-list"]',
      '[data-testid="chat-list-search-container"]',
      'nav [role="textbox"]',
      '[aria-label="Chat list"]',
      '[data-icon="menu"]',
      '[data-icon="chat"]'
    ];
    const composerSelectors = [
      'footer [data-testid="conversation-compose-box-input"]',
      'footer [contenteditable="true"]',
      'footer [role="textbox"]',
      '[data-testid="conversation-compose-box-input"]',
      '[role="textbox"][contenteditable="true"]',
      'textarea[aria-label]',
      '[aria-placeholder][contenteditable="true"]',
      '[data-lexical-editor="true"]'
    ];

    return {
      title: pickText(document.title),
      url: window.location.href,
      bodyText: pickText(document.body?.innerText || ''),
      bodyHtml: pickText(document.body?.innerHTML || ''),
      qrRef: pickText(qrRefElement?.getAttribute('data-ref') || ''),
      qrCanvasVisible: canvasVisible,
      connectedUi: connectedSelectors.some((selector) => isVisible(document.querySelector(selector))),
      composerVisible: composerSelectors.some((selector) => isVisible(document.querySelector(selector)))
    };
  });
}

async function buildQrDataUrl(page, domState) {
  if (cleanText(domState?.qrRef)) {
    try {
      return await QRCode.toDataURL(domState.qrRef, {
        margin: 1,
        width: 320
      });
    } catch {
      return '';
    }
  }

  if (domState?.qrCanvasVisible) {
    try {
      const qrCanvas = page.locator('canvas').first();
      if ((await qrCanvas.count()) > 0) {
        const buffer = await qrCanvas.screenshot({
          type: 'png'
        });
        return `data:image/png;base64,${buffer.toString('base64')}`;
      }
    } catch {
      return '';
    }
  }

  return '';
}

function buildSnapshotFromDom(domState = {}, qrCodeDataUrl = '', previousState = null) {
  const bodyText = cleanText(domState.bodyText).toLowerCase();
  const bodyHtml = cleanText(domState.bodyHtml).toLowerCase();
  const title = cleanText(domState.title).toLowerCase();
  const hadValidSession =
    previousState?.sessionValid === true ||
    cleanText(previousState?.connectionStatus) === 'connected' ||
    cleanText(previousState?.connectionStatus) === 'session_expired';
  const hasQr = Boolean(cleanText(domState.qrRef) || domState.qrCanvasVisible);
  const loginHints = [
    'scan the qr code',
    'mit deinem telefon',
    'use whatsapp on your computer',
    'log into whatsapp web',
    'link a device',
    'devices',
    'verknuepfe ein geraet',
    'geraet verknuepfen',
    'qr code',
    'lade whatsapp auf deinem computer'
  ];
  const waitingForLogin =
    loginHints.some((hint) => bodyText.includes(hint) || bodyHtml.includes(hint) || title.includes(hint)) ||
    (cleanText(domState.url).startsWith(WHATSAPP_WEB_URL) && cleanText(previousState?.connectionStatus) === 'qr_required');
  const explicitError =
    bodyText.includes('something went wrong') || bodyText.includes('verbindung fehlgeschlagen') || bodyText.includes('error');
  const syncingSplash =
    bodyText.includes('deine nachrichten werden heruntergeladen') ||
    bodyText.includes('your messages are being downloaded') ||
    bodyText.includes('do not close this window') ||
    (bodyText.includes('schlie') && bodyText.includes('fenster nicht'));

  if (domState.connectedUi || domState.composerVisible) {
    return {
      connectionStatus: 'connected',
      workerStatus: 'running',
      sessionValid: true,
      qrRequired: false,
      qrCodeDataUrl: '',
      browserStatus: 'running',
      channelReachable: Boolean(domState.composerVisible)
    };
  }

  if (hasQr) {
    return {
      connectionStatus: hadValidSession ? 'session_expired' : 'qr_required',
      workerStatus: 'running',
      sessionValid: false,
      qrRequired: true,
      qrCodeDataUrl,
      browserStatus: 'running',
      channelReachable: false
    };
  }

  if (waitingForLogin) {
    return {
      connectionStatus: hadValidSession ? 'session_expired' : 'qr_required',
      workerStatus: 'running',
      sessionValid: false,
      qrRequired: true,
      qrCodeDataUrl,
      browserStatus: 'running',
      channelReachable: false
    };
  }

  if (syncingSplash && hadValidSession) {
    return {
      connectionStatus: 'connected',
      workerStatus: 'running',
      sessionValid: true,
      qrRequired: false,
      qrCodeDataUrl: '',
      browserStatus: 'running',
      channelReachable: false
    };
  }

  if (explicitError) {
    return {
      connectionStatus: 'error',
      workerStatus: 'error',
      sessionValid: false,
      qrRequired: false,
      qrCodeDataUrl: '',
      browserStatus: 'error',
      channelReachable: false
    };
  }

  return {
    connectionStatus: hadValidSession ? 'session_expired' : 'not_connected',
    workerStatus: 'running',
    sessionValid: false,
    qrRequired: false,
    qrCodeDataUrl: '',
    browserStatus: 'running',
    channelReachable: false
  };
}

async function locateFirstVisible(page, selectors = [], timeoutMs = 5000) {
  for (const selector of selectors) {
    const locator = page.locator(selector).last();

    try {
      if ((await locator.count()) === 0) {
        continue;
      }

      await locator.waitFor({
        state: 'visible',
        timeout: timeoutMs
      });
      return locator;
    } catch {
      continue;
    }
  }

  return null;
}

async function collectVisibleUiSummary(page) {
  return await page.evaluate(() => {
    const isVisible = (element) => {
      if (!element) {
        return false;
      }

      const style = window.getComputedStyle(element);
      const rect = element.getBoundingClientRect();
      return style.visibility !== 'hidden' && style.display !== 'none' && rect.width > 0 && rect.height > 0;
    };

    const pickText = (value) => String(value || '').replace(/\s+/g, ' ').trim();
    const collectText = (selector, limit = 12) =>
      [...document.querySelectorAll(selector)]
        .filter((element) => isVisible(element))
        .map((element) => pickText(element.innerText || element.getAttribute('aria-label') || element.getAttribute('title') || ''))
        .filter(Boolean)
        .slice(0, limit);

    return {
      url: window.location.href,
      title: document.title,
      buttons: collectText('button, [role="button"]'),
      headings: collectText('h1, h2, h3'),
      links: collectText('a'),
      textboxes: collectText('[role="textbox"], textarea, [contenteditable="true"]'),
      bodySnippet: pickText(document.body?.innerText || '').slice(0, 1200)
    };
  });
}

function buildChannelSurfaceState(summary = {}, plan = {}) {
  const joinedText = normalizeComparableText(
    [
      summary.title,
      ...(Array.isArray(summary.buttons) ? summary.buttons : []),
      ...(Array.isArray(summary.headings) ? summary.headings : []),
      ...(Array.isArray(summary.links) ? summary.links : []),
      ...(Array.isArray(summary.textboxes) ? summary.textboxes : []),
      summary.bodySnippet
    ]
      .filter(Boolean)
      .join(' ')
  );
  const lookupTerms = Array.isArray(plan.lookupTerms) ? plan.lookupTerms.map((value) => normalizeComparableText(value)).filter(Boolean) : [];
  const targetMatched = lookupTerms.some((value) => joinedText.includes(value));
  const url = cleanText(summary.url);
  const normalizedUrl = normalizeComparableText(url);
  const channelId = cleanText(plan.channelId);
  const urlMatchesChannel = Boolean(channelId && normalizedUrl.includes(normalizeComparableText(channelId)));
  const isWebApp = /^https?:\/\/web\.whatsapp\.com/i.test(url);
  const isExternalChannelLanding = /^https?:\/\/(?:www\.)?whatsapp\.com\/channel\//i.test(url);
  const buttonsAndLinks = normalizeComparableText(
    [...(Array.isArray(summary.buttons) ? summary.buttons : []), ...(Array.isArray(summary.links) ? summary.links : [])].join(' ')
  );
  const landingActionVisible =
    isExternalChannelLanding ||
    [
      'open in whatsapp',
      'continue to whatsapp',
      'continue in browser',
      'view channel',
      'open channel',
      'weiter im browser',
      'in whatsapp offnen',
      'in whatsapp oeffnen',
      'kanal ansehen'
    ].some((hint) => buttonsAndLinks.includes(normalizeComparableText(hint)));
  const adminActionVisible = [
    'create update',
    'create post',
    'new post',
    'new update',
    'post update',
    'compose',
    'message',
    'update',
    'beitrag',
    'erstellen',
    'posten'
  ].some((hint) => buttonsAndLinks.includes(normalizeComparableText(hint)));
  const readOnlyHintVisible = [
    'follow',
    'share',
    'abonnieren',
    'teilen',
    'mute',
    'stumm',
    'report',
    'melden'
  ].some((hint) => buttonsAndLinks.includes(normalizeComparableText(hint)));

  return {
    ...summary,
    channelFound: Boolean(isWebApp && (urlMatchesChannel || /\/channel\//i.test(url) || targetMatched)),
    urlMatchesChannel,
    targetMatched,
    isWebApp,
    isExternalChannelLanding,
    landingActionVisible,
    adminActionVisible,
    readOnlyHintVisible
  };
}

async function captureComposerDebugArtifacts(page, options = {}) {
  const config = getWhatsappPlaywrightConfig();
  const sessionDir = cleanText(config.sessionDir);
  ensureDirectory(sessionDir);

  const stamp = nowIso().replace(/[:.]/g, '-');
  const prefix = cleanText(options.prefix) || 'composer-debug';
  const label =
    cleanText(options.label)
      .replace(/[^a-zA-Z0-9_-]+/g, '-')
      .replace(/-+/g, '-')
      .replace(/^-|-$/g, '')
      .toLowerCase() || '';
  const suffix = label ? `-${label}` : '';
  const screenshotPath = path.join(sessionDir, `${prefix}${suffix}-${stamp}.png`);
  const jsonPath = path.join(sessionDir, `${prefix}${suffix}-${stamp}.json`);
  const summary = await collectVisibleUiSummary(page).catch(() => ({}));

  try {
    await page.screenshot({
      path: screenshotPath,
      fullPage: true
    });
  } catch {
    // Best-effort only. The JSON summary is the important artifact.
  }

  try {
    fs.writeFileSync(jsonPath, JSON.stringify(summary, null, 2), 'utf8');
  } catch {
    // Best-effort only.
  }

  return {
    screenshotPath,
    jsonPath,
    summary,
    label: cleanText(options.label)
  };
}

async function captureChannelDebugStep(page, debugContext, label, payload = null) {
  if (!debugContext?.enabled) {
    return null;
  }

  const artifact = await captureComposerDebugArtifacts(page, {
    prefix: `channel-debug-${debugContext.runId}`,
    label
  });
  const entry = {
    step: cleanText(label),
    screenshotPath: artifact.screenshotPath,
    jsonPath: artifact.jsonPath,
    createdAt: nowIso(),
    url: cleanText(artifact.summary?.url),
    payload: payload && typeof payload === 'object' ? payload : null
  };
  debugContext.artifacts.push(entry);
  return entry;
}

async function pushDebugArtifact(debugContext, artifact = {}, label = '', payload = null) {
  if (!debugContext?.enabled || !artifact) {
    return;
  }

  debugContext.artifacts.push({
    step: cleanText(label) || cleanText(artifact.label) || 'artifact',
    screenshotPath: cleanText(artifact.screenshotPath),
    jsonPath: cleanText(artifact.jsonPath),
    htmlPath: cleanText(artifact.htmlPath),
    createdAt: nowIso(),
    url: null,
    payload: payload && typeof payload === 'object' ? payload : null
  });
}

function stringifyCandidateForLog(candidate = {}) {
  const bits = [
    cleanText(candidate.preferredSelector),
    cleanText(candidate.ariaLabel),
    cleanText(candidate.placeholder),
    cleanText(candidate.dataTestid),
    cleanText(candidate.dataTab)
  ].filter(Boolean);
  return bits.join(' | ');
}

function getManualCandidateScore(candidate = {}) {
  let score = 0;
  if (candidate.insideFooter) {
    score += 60;
  }
  if (candidate.dataTestid) {
    score += 50;
  }
  if (candidate.role === 'textbox') {
    score += 35;
  }
  if (candidate.contentEditable === 'true') {
    score += 25;
  }
  if (candidate.ariaLabel) {
    score += 20;
  }
  if (candidate.placeholder) {
    score += 15;
  }
  if (candidate.dataTab) {
    score += 10;
  }
  if (candidate.domPath && candidate.domPath.includes('footer')) {
    score += 10;
  }
  return score;
}

function buildPreferredManualSelector(candidate = {}) {
  const scopePrefix = candidate.insideFooter ? 'footer ' : '';
  if (cleanText(candidate.dataTestid)) {
    return `${scopePrefix}[data-testid="${candidate.dataTestid}"]`;
  }
  if (cleanText(candidate.role) === 'textbox' && cleanText(candidate.ariaLabel)) {
    return `${scopePrefix}[role="textbox"][aria-label="${candidate.ariaLabel.replace(/"/g, '\\"')}"]`;
  }
  if (candidate.contentEditable === 'true' && cleanText(candidate.dataTab)) {
    return `${scopePrefix}[contenteditable="true"][data-tab="${candidate.dataTab.replace(/"/g, '\\"')}"]`;
  }
  if (candidate.contentEditable === 'true' && cleanText(candidate.ariaLabel)) {
    return `${scopePrefix}[contenteditable="true"][aria-label="${candidate.ariaLabel.replace(/"/g, '\\"')}"]`;
  }
  if (cleanText(candidate.placeholder)) {
    return `${scopePrefix}[placeholder="${candidate.placeholder.replace(/"/g, '\\"')}"]`;
  }
  if (cleanText(candidate.domPath)) {
    return candidate.domPath;
  }
  return '';
}

function choosePreferredManualSelector(candidates = []) {
  const enriched = (Array.isArray(candidates) ? candidates : [])
    .map((candidate) => {
      const preferredSelector = buildPreferredManualSelector(candidate);
      return {
        ...candidate,
        preferredSelector,
        selectorScore: getManualCandidateScore(candidate)
      };
    })
    .filter((candidate) => cleanText(candidate.preferredSelector));

  enriched.sort((left, right) => right.selectorScore - left.selectorScore);
  return {
    candidates: enriched,
    preferredSelector: cleanText(enriched[0]?.preferredSelector)
  };
}

async function collectManualChannelDebugSnapshot(page) {
  return await page.evaluate(() => {
    const isVisible = (element) => {
      if (!element) {
        return false;
      }

      const style = window.getComputedStyle(element);
      const rect = element.getBoundingClientRect();
      return style.visibility !== 'hidden' && style.display !== 'none' && rect.width > 0 && rect.height > 0;
    };

    const pickText = (value) => String(value || '').replace(/\s+/g, ' ').trim();
    const buildDomPath = (element) => {
      if (!element || !element.tagName) {
        return '';
      }

      const segments = [];
      let current = element;
      while (current && current.nodeType === Node.ELEMENT_NODE && segments.length < 8) {
        const tagName = current.tagName.toLowerCase();
        if (current.id) {
          segments.unshift(`${tagName}#${current.id}`);
          break;
        }

        let segment = tagName;
        if (current.getAttribute('data-testid')) {
          segment += `[data-testid="${current.getAttribute('data-testid')}"]`;
        } else if (current.getAttribute('data-tab')) {
          segment += `[data-tab="${current.getAttribute('data-tab')}"]`;
        } else if (current.getAttribute('role')) {
          segment += `[role="${current.getAttribute('role')}"]`;
        } else {
          const parent = current.parentElement;
          if (parent) {
            const siblings = [...parent.children].filter((item) => item.tagName === current.tagName);
            if (siblings.length > 1) {
              const index = siblings.indexOf(current) + 1;
              segment += `:nth-of-type(${index})`;
            }
          }
        }
        segments.unshift(segment);
        current = current.parentElement;
      }

      return segments.join(' > ');
    };

    const toElementInfo = (element) => ({
      tagName: pickText(element.tagName || '').toLowerCase(),
      role: pickText(element.getAttribute('role')),
      ariaLabel: pickText(element.getAttribute('aria-label')),
      title: pickText(element.getAttribute('title')),
      placeholder: pickText(element.getAttribute('placeholder') || element.getAttribute('aria-placeholder')),
      dataTab: pickText(element.getAttribute('data-tab')),
      dataTestid: pickText(element.getAttribute('data-testid')),
      contentEditable: pickText(element.getAttribute('contenteditable')),
      text: pickText(element.innerText || element.textContent || ''),
      domPath: buildDomPath(element),
      insideFooter: Boolean(element.closest('footer')),
      htmlSnippet: pickText(element.outerHTML || '').slice(0, 400)
    });

    const collectElements = (selector, limit = 25) =>
      [...document.querySelectorAll(selector)]
        .filter((element) => isVisible(element))
        .slice(0, limit)
        .map(toElementInfo);

    const collectTexts = (selector, limit = 16) =>
      [...document.querySelectorAll(selector)]
        .filter((element) => isVisible(element))
        .map((element) => pickText(element.innerText || element.textContent || element.getAttribute('aria-label') || element.getAttribute('title') || ''))
        .filter(Boolean)
        .slice(0, limit);

    const composerSelectorList = [
      'footer [data-testid="conversation-compose-box-input"]',
      'footer [contenteditable="true"]',
      'footer [role="textbox"]',
      '[role="textbox"][contenteditable="true"]',
      'div[contenteditable="true"][data-tab]',
      'textarea[aria-label]',
      '[aria-placeholder][contenteditable="true"]',
      '[data-lexical-editor="true"]',
      '[contenteditable="true"]',
      '[role="textbox"]',
      'textarea'
    ];

    const candidateMap = new Map();
    for (const selector of composerSelectorList) {
      for (const element of collectElements(selector, 40)) {
        const key = element.domPath || `${element.tagName}:${element.ariaLabel}:${element.dataTab}`;
        if (!candidateMap.has(key)) {
          candidateMap.set(key, element);
        }
      }
    }

    return {
      url: window.location.href,
      title: document.title,
      buttons: collectTexts('button, [role="button"]'),
      headings: collectTexts('h1, h2, h3'),
      links: collectTexts('a'),
      textboxes: collectTexts('[role="textbox"], textarea, [contenteditable="true"]'),
      bodySnippet: pickText(document.body?.innerText || '').slice(0, 2000),
      composerCandidates: Array.from(candidateMap.values()),
      contentEditableFields: collectElements('[contenteditable="true"]', 40),
      roleTextboxes: collectElements('[role="textbox"], textarea', 40),
      ariaLabels: collectElements('[aria-label]', 60),
      dataTabFields: collectElements('[data-tab]', 60)
    };
  });
}

async function captureManualChannelDebugArtifacts(page, snapshot = {}, options = {}) {
  const config = getWhatsappPlaywrightConfig();
  const sessionDir = cleanText(config.sessionDir);
  ensureDirectory(sessionDir);

  const stamp = nowIso().replace(/[:.]/g, '-');
  const prefix = cleanText(options.prefix) || 'manual-channel-debug';
  const screenshotPath = path.join(sessionDir, `${prefix}-${stamp}.png`);
  const jsonPath = path.join(sessionDir, `${prefix}-${stamp}.json`);
  const htmlPath = path.join(sessionDir, `${prefix}-${stamp}.html`);

  try {
    await page.screenshot({
      path: screenshotPath,
      fullPage: true
    });
  } catch {
    // Best-effort only.
  }

  try {
    fs.writeFileSync(jsonPath, JSON.stringify(snapshot, null, 2), 'utf8');
  } catch {
    // Best-effort only.
  }

  try {
    fs.writeFileSync(htmlPath, await page.content(), 'utf8');
  } catch {
    // Best-effort only.
  }

  return {
    screenshotPath,
    jsonPath,
    htmlPath
  };
}

async function buildManualChannelDebugCapture(session, providerInfo, input = {}, options = {}) {
  const page = session.page;
  const plan = buildWhatsappChannelPlan(input.targetRef, input.targetMeta && typeof input.targetMeta === 'object' ? input.targetMeta : input);
  const waitTimeoutMs = Math.max(Number(input.waitTimeoutMs || options.waitTimeoutMs || 120000), 5000);
  const pollIntervalMs = Math.max(500, Number(input.pollIntervalMs || 1500));
  const requireChannelMatch = options.requireChannelMatch !== false;
  const startedAt = Date.now();

  logPlaywrightWorkerEvent(
    'whatsapp.manual.channel.debug.start',
    `[WHATSAPP_MANUAL_CHANNEL_DEBUG_START] Warte auf manuell geoeffneten WhatsApp Kanal ${plan.targetLabel || plan.externalChannelUrl}.`,
    {
      targetRef: plan.externalChannelUrl,
      waitTimeoutMs,
      pollIntervalMs
    }
  );

  let snapshot = null;
  let surface = null;
  while (Date.now() - startedAt <= waitTimeoutMs) {
    await waitForSettledUi(page);
    snapshot = await collectManualChannelDebugSnapshot(page);
    surface = buildChannelSurfaceState(snapshot, plan);
    const hasComposerCandidates = Array.isArray(snapshot.composerCandidates) && snapshot.composerCandidates.length > 0;
    const manualReady = requireChannelMatch ? surface.channelFound && hasComposerCandidates : hasComposerCandidates || surface.channelFound;

    if (manualReady) {
      break;
    }

    if (!options.waitForChannel) {
      break;
    }

    await page.waitForTimeout(pollIntervalMs).catch(() => null);
  }

  snapshot = snapshot || (await collectManualChannelDebugSnapshot(page));
  surface = surface || buildChannelSurfaceState(snapshot, plan);
  const selectorInfo = choosePreferredManualSelector(snapshot.composerCandidates || []);
  const artifacts = await captureManualChannelDebugArtifacts(page, {
    ...snapshot,
    channelSurface: surface,
    preferredSelector: selectorInfo.preferredSelector,
    selectorCandidates: selectorInfo.candidates
  }, {
    prefix: cleanText(options.prefix) || 'manual-channel-debug'
  });

  logPlaywrightWorkerEvent(
    'whatsapp.manual.channel.current_url',
    `[WHATSAPP_CHANNEL_CURRENT_URL] ${cleanText(snapshot.url) || page.url()}`,
    {
      currentUrl: cleanText(snapshot.url) || page.url(),
      title: cleanText(snapshot.title)
    }
  );
  logPlaywrightWorkerEvent(
    'whatsapp.manual.channel.dom_captured',
    `[WHATSAPP_CHANNEL_DOM_CAPTURED] DOM und Screenshot gespeichert.`,
    {
      screenshotPath: artifacts.screenshotPath,
      jsonPath: artifacts.jsonPath,
      htmlPath: artifacts.htmlPath
    }
  );
  logPlaywrightWorkerEvent(
    'whatsapp.manual.channel.composer_candidates',
    `[WHATSAPP_CHANNEL_COMPOSER_CANDIDATES_FOUND] ${Array.isArray(selectorInfo.candidates) ? selectorInfo.candidates.length : 0} Kandidaten erkannt.`,
    {
      candidates: (selectorInfo.candidates || []).slice(0, 8).map((candidate) => ({
        preferredSelector: candidate.preferredSelector,
        ariaLabel: candidate.ariaLabel,
        dataTestid: candidate.dataTestid,
        dataTab: candidate.dataTab,
        insideFooter: candidate.insideFooter
      }))
    }
  );
  if (selectorInfo.preferredSelector) {
    logPlaywrightWorkerEvent(
      'whatsapp.manual.channel.selector_saved',
      `[WHATSAPP_CHANNEL_SELECTOR_SAVED] ${selectorInfo.preferredSelector}`,
      {
        preferredSelector: selectorInfo.preferredSelector
      }
    );
  }

  const channelNavigationStatus = surface.channelFound
    ? selectorInfo.preferredSelector
      ? 'WHATSAPP_CHANNEL_COMPOSER_FOUND'
      : 'WHATSAPP_CHANNEL_FOUND'
    : 'WHATSAPP_CHANNEL_NAVIGATION_NOT_IMPLEMENTED';
  const channelAdminStatus = selectorInfo.preferredSelector
    ? 'ADMIN_CONTROLS_VISIBLE'
    : surface.readOnlyHintVisible
      ? 'WHATSAPP_CHANNEL_NO_ADMIN_RIGHTS'
      : 'unknown';
  const lastDebugMessage =
    channelNavigationStatus === 'WHATSAPP_CHANNEL_COMPOSER_FOUND'
      ? 'Manueller Channel-Debug erfolgreich. Composer-Kandidaten gespeichert.'
      : `WHATSAPP_CHANNEL_NAVIGATION_NOT_IMPLEMENTED: ${plan.targetLabel || plan.externalChannelUrl} wurde manuell noch nicht sendefaehig erkannt.`;

  persistRuntimePatch(
    {
      channelReachable: Boolean(selectorInfo.preferredSelector),
      channelNavigationStatus,
      channelAdminStatus,
      lastChannelTargetRef: cleanText(plan.externalChannelUrl),
      lastChannelDebugAt: nowIso(),
      lastChannelDebugMessage: lastDebugMessage,
      lastChannelDebugArtifacts: [
        {
          step: 'manual-channel-debug',
          screenshotPath: artifacts.screenshotPath,
          jsonPath: artifacts.jsonPath,
          htmlPath: artifacts.htmlPath,
          createdAt: nowIso()
        }
      ],
      lastChannelPreferredSelector: cleanText(selectorInfo.preferredSelector),
      lastChannelComposerCandidates: selectorInfo.candidates || [],
      lastChannelDomSnapshotPath: artifacts.jsonPath,
      lastChannelHtmlSnapshotPath: artifacts.htmlPath,
      lastChannelScreenshotPath: artifacts.screenshotPath,
      currentUrl: cleanText(snapshot.url) || page.url(),
      currentTitle: cleanText(snapshot.title),
      lastError:
        channelNavigationStatus === 'WHATSAPP_CHANNEL_COMPOSER_FOUND'
          ? ''
          : `WHATSAPP_CHANNEL_NAVIGATION_NOT_IMPLEMENTED: ${plan.targetLabel || plan.externalChannelUrl} wurde manuell noch nicht sendefaehig erkannt.`,
      lastErrorAt: channelNavigationStatus === 'WHATSAPP_CHANNEL_COMPOSER_FOUND' ? null : nowIso()
    },
    cleanText(readRuntimeMeta().connectionStatus) || 'connected'
  );

  if (options.waitForChannel && channelNavigationStatus !== 'WHATSAPP_CHANNEL_COMPOSER_FOUND') {
    throw buildWhatsappPlaywrightError(
      `WHATSAPP_CHANNEL_NAVIGATION_NOT_IMPLEMENTED: ${plan.targetLabel || plan.externalChannelUrl} konnte in der manuellen Debug-Sitzung noch nicht mit sendefaehigem Composer erkannt werden.`,
      {
        code: 'WHATSAPP_CHANNEL_NAVIGATION_NOT_IMPLEMENTED',
        retryable: false
      }
    );
  }

  return {
    success: true,
    channelReachable: Boolean(selectorInfo.preferredSelector),
    targetUrl: plan.externalChannelUrl,
    currentUrl: cleanText(snapshot.url) || page.url(),
    currentTitle: cleanText(snapshot.title),
    channelNavigationStatus,
    channelAdminStatus,
    preferredSelector: cleanText(selectorInfo.preferredSelector),
    composerCandidates: selectorInfo.candidates || [],
    contentEditableFields: snapshot.contentEditableFields || [],
    roleTextboxes: snapshot.roleTextboxes || [],
    ariaLabels: snapshot.ariaLabels || [],
    dataTabFields: snapshot.dataTabFields || [],
    visibleButtons: snapshot.buttons || [],
    screenshotPath: artifacts.screenshotPath,
    domSnapshotPath: artifacts.jsonPath,
    htmlSnapshotPath: artifacts.htmlPath,
    debugArtifacts: [
      {
        step: 'manual-channel-debug',
        screenshotPath: artifacts.screenshotPath,
        jsonPath: artifacts.jsonPath,
        htmlPath: artifacts.htmlPath,
        createdAt: nowIso()
      }
    ]
  };
}

function buildComposerSelectorList(preferredSelectors = []) {
  return dedupeTextValues([
    ...(Array.isArray(preferredSelectors) ? preferredSelectors : []),
    'footer [data-testid="conversation-compose-box-input"]',
    'footer [role="textbox"][contenteditable="true"]',
    'footer [contenteditable="true"][data-tab]',
    'footer [contenteditable="true"][aria-label*="Nachricht"]',
    'footer [contenteditable="true"][aria-label*="Message"]',
    'footer [contenteditable="true"]',
    'footer [contenteditable="true"]',
    'footer [role="textbox"]',
    '[role="textbox"][contenteditable="true"]',
    '[role="textbox"][aria-label*="Nachricht"]',
    '[role="textbox"][aria-label*="Message"]',
    'div[contenteditable="true"][data-tab]',
    'textarea[aria-label]',
    '[aria-placeholder][contenteditable="true"]',
    '[data-lexical-editor="true"]',
    '[contenteditable="true"]',
    '[role="textbox"]',
    'textarea'
  ]);
}

async function revealChannelComposerViewport(page) {
  return await page.evaluate(() => {
    const isVisible = (element) => {
      if (!element) {
        return false;
      }

      const style = window.getComputedStyle(element);
      const rect = element.getBoundingClientRect();
      return style.visibility !== 'hidden' && style.display !== 'none' && rect.width > 0 && rect.height > 0;
    };

    const pickText = (value) => String(value || '').replace(/\s+/g, ' ').trim();
    const footer = document.querySelector('footer');
    const main =
      document.querySelector('main') ||
      document.querySelector('[role="main"]') ||
      document.querySelector('#main') ||
      document.querySelector('#app');

    if (main && typeof main.focus === 'function') {
      main.focus();
    }

    if (main && isVisible(main) && typeof main.scrollIntoView === 'function') {
      main.scrollIntoView({
        block: 'center',
        inline: 'nearest'
      });
    }

    const scrollableCandidates = [...document.querySelectorAll('body, #app, #main, [role="main"], main, section, div')]
      .filter((element) => {
        if (!isVisible(element)) {
          return false;
        }

        const style = window.getComputedStyle(element);
        const overflowY = style.overflowY || '';
        return element.scrollHeight > element.clientHeight + 24 && /(auto|scroll|overlay)/i.test(overflowY);
      })
      .slice(0, 60);

    for (const element of scrollableCandidates) {
      try {
        element.scrollTop = element.scrollHeight;
      } catch {
        // Ignore individual scroll failures.
      }
    }

    if (footer && isVisible(footer) && typeof footer.scrollIntoView === 'function') {
      footer.scrollIntoView({
        block: 'end',
        inline: 'nearest'
      });
    }

    window.scrollTo({
      top: document.body?.scrollHeight || document.documentElement?.scrollHeight || 0,
      behavior: 'instant'
    });

    return {
      footerVisible: isVisible(footer),
      footerText: pickText(footer?.innerText || footer?.getAttribute?.('aria-label') || ''),
      mainVisible: isVisible(main)
    };
  });
}

async function prepareComposerViewport(page, timeoutMs, options = {}) {
  const debugContext = options.debugContext || null;
  const captureArtifacts = options.captureArtifacts !== false;
  const prefixBase =
    cleanText(options.prefixBase) ||
    (debugContext?.runId ? `channel-debug-${debugContext.runId}` : 'composer-viewport');
  const beforeSnapshot = await collectManualChannelDebugSnapshot(page).catch(() => null);

  if (captureArtifacts && beforeSnapshot) {
    const beforeArtifacts = await captureManualChannelDebugArtifacts(
      page,
      {
        ...beforeSnapshot,
        stage: 'before-scroll'
      },
      {
        prefix: `${prefixBase}-before-scroll`
      }
    ).catch(() => null);
    await pushDebugArtifact(debugContext, beforeArtifacts, 'before-composer-scroll', {
      currentUrl: cleanText(beforeSnapshot.url)
    });
  }

  logPlaywrightWorkerEvent(
    'whatsapp.composer.scroll.start',
    '[WHATSAPP_COMPOSER_SCROLL_START] Versuche den unteren WhatsApp Channel-Composer sichtbar zu machen.',
    {
      currentUrl: page.url(),
      viewport: DESKTOP_VIEWPORT
    }
  );

  const revealResult = await revealChannelComposerViewport(page).catch(() => ({
    footerVisible: false,
    mainVisible: false,
    footerText: ''
  }));
  await page.mouse.wheel(0, 1800).catch(() => null);
  await page.keyboard.press('End').catch(() => null);
  await page.waitForTimeout(Math.min(1200, Math.max(600, Math.floor(timeoutMs / 8)))).catch(() => null);
  await waitForSettledUi(page);

  const afterSnapshot = await collectManualChannelDebugSnapshot(page).catch(() => null);
  if (captureArtifacts && afterSnapshot) {
    const afterArtifacts = await captureManualChannelDebugArtifacts(
      page,
      {
        ...afterSnapshot,
        stage: 'after-scroll',
        revealResult
      },
      {
        prefix: `${prefixBase}-after-scroll`
      }
    ).catch(() => null);
    await pushDebugArtifact(debugContext, afterArtifacts, 'after-composer-scroll', {
      currentUrl: cleanText(afterSnapshot.url),
      revealResult
    });
  }

  return {
    beforeSnapshot,
    afterSnapshot,
    revealResult,
    selectorInfo: choosePreferredManualSelector(afterSnapshot?.composerCandidates || [])
  };
}

async function captureVisibleComposerArtifacts(page, selectorInfo = {}, options = {}) {
  const snapshot = await collectManualChannelDebugSnapshot(page).catch(() => null);
  if (!snapshot) {
    return null;
  }

  return await captureManualChannelDebugArtifacts(
    page,
    {
      ...snapshot,
      stage: 'composer-visible',
      preferredSelector: cleanText(selectorInfo.preferredSelector),
      selectorCandidates: selectorInfo.candidates || []
    },
    {
      prefix: cleanText(options.prefix) || 'composer-visible'
    }
  ).catch(() => null);
}

async function locateComposer(page, timeoutMs, options = {}) {
  const debugContext = options.debugContext || null;
  const captureArtifacts = options.captureArtifacts !== false;
  const storedPreferredSelector = cleanText(readRuntimeMeta().lastChannelPreferredSelector);
  let viewportState = await prepareComposerViewport(page, timeoutMs, {
    debugContext,
    captureArtifacts,
    prefixBase: cleanText(options.prefixBase) || (debugContext?.runId ? `channel-debug-${debugContext.runId}` : 'composer-viewport')
  }).catch(() => ({
    selectorInfo: {
      preferredSelector: '',
      candidates: []
    }
  }));
  let composerSelectors = buildComposerSelectorList([
    storedPreferredSelector,
    cleanText(viewportState?.selectorInfo?.preferredSelector)
  ]);
  let composer = await locateFirstVisible(page, composerSelectors, timeoutMs);

  if (!composer) {
    const revealComposerButton = await locateFirstVisible(
      page,
      [
        'button[aria-label*="Update"]',
        'button[aria-label*="Nachricht"]',
        'button[aria-label*="message"]',
        'button[title*="Update"]',
        'button[title*="Nachricht"]',
        'button[title*="message"]',
        'button:has-text("Update")',
        'button:has-text("Erstellen")',
        'button:has-text("Nachricht")',
        'button:has-text("Message")',
        '[data-testid="compose"]',
        '[data-testid="channel-compose"]'
      ],
      Math.max(1500, Math.floor(timeoutMs / 2))
    ).catch(() => null);

    if (revealComposerButton) {
      await revealComposerButton.click().catch(() => null);
      await page.waitForTimeout(700).catch(() => null);
      viewportState = await prepareComposerViewport(page, timeoutMs, {
        debugContext,
        captureArtifacts,
        prefixBase: cleanText(options.prefixBase) || (debugContext?.runId ? `channel-debug-${debugContext.runId}` : 'composer-viewport')
      }).catch(() => viewportState);
      composerSelectors = buildComposerSelectorList([
        storedPreferredSelector,
        cleanText(viewportState?.selectorInfo?.preferredSelector)
      ]);
      composer = await locateFirstVisible(page, composerSelectors, Math.max(1500, Math.floor(timeoutMs / 2)));
    }
  }

  if (composer) {
    const selectorInfo = viewportState?.selectorInfo || {
      preferredSelector: '',
      candidates: []
    };
    logPlaywrightWorkerEvent(
      'whatsapp.composer.visible',
      '[WHATSAPP_COMPOSER_VISIBLE] Unterer WhatsApp Channel-Composer ist sichtbar.',
      {
        currentUrl: page.url(),
        preferredSelector: cleanText(selectorInfo.preferredSelector)
      }
    );
    logPlaywrightWorkerEvent(
      'whatsapp.channel.composer.visible',
      '[WHATSAPP_CHANNEL_COMPOSER_VISIBLE] Channel-Composer im rechten WhatsApp Bereich ist sichtbar.',
      {
        currentUrl: page.url(),
        preferredSelector: cleanText(selectorInfo.preferredSelector)
      }
    );
    if (cleanText(selectorInfo.preferredSelector)) {
      logPlaywrightWorkerEvent(
        'whatsapp.composer.selector_found',
        `[WHATSAPP_COMPOSER_SELECTOR_FOUND] ${cleanText(selectorInfo.preferredSelector)}`,
        {
          currentUrl: page.url(),
          preferredSelector: cleanText(selectorInfo.preferredSelector)
        }
      );
    }

    const visibleArtifacts = captureArtifacts
      ? await captureVisibleComposerArtifacts(page, selectorInfo, {
          prefix: cleanText(options.prefixBase) || (debugContext?.runId ? `channel-debug-${debugContext.runId}-composer-visible` : 'composer-visible')
        })
      : null;
    await pushDebugArtifact(debugContext, visibleArtifacts, 'composer-visible', {
      preferredSelector: cleanText(selectorInfo.preferredSelector)
    });

    return {
      locator: composer,
      selectorInfo,
      visibleArtifacts
    };
  }

  if (!composer) {
    const debugArtifacts = await captureComposerDebugArtifacts(page).catch(() => null);
    const visibleButtons = (debugArtifacts?.summary?.buttons || []).slice(0, 4).join(' | ');
    const debugHint = debugArtifacts?.jsonPath ? ` Debug: ${debugArtifacts.jsonPath}` : '';
    const buttonHint = visibleButtons ? ` Sichtbare Buttons: ${visibleButtons}.` : '';
    throw buildWhatsappPlaywrightError(`Kein WhatsApp Eingabefeld gefunden.${buttonHint}${debugHint}`, {
      code: 'WHATSAPP_COMPOSER_MISSING'
    });
  }

  return {
    locator: composer,
    selectorInfo: viewportState?.selectorInfo || {
      preferredSelector: '',
      candidates: []
    },
    visibleArtifacts: null
  };
}

async function locateSendButton(page, timeoutMs) {
  const button = await locateFirstVisible(
    page,
    [
      '[data-testid="send"]',
      'button[aria-label*="Send"]',
      'button[aria-label*="Senden"]',
      'button[title*="Send"]',
      'button[title*="Senden"]',
      'button:has-text("Senden")',
      'button:has-text("Send")',
      'span[data-icon="send"]'
    ],
    timeoutMs
  );

  if (!button) {
    throw buildWhatsappPlaywrightError('Kein WhatsApp Senden-Button gefunden.', {
      code: 'WHATSAPP_SEND_BUTTON_MISSING'
    });
  }

  return button;
}

async function waitForInteractiveWhatsapp(page, timeoutMs, previousState = null) {
  const startedAt = Date.now();
  let lastDomState = null;

  while (Date.now() - startedAt <= timeoutMs) {
    await waitForSettledUi(page);
    lastDomState = await extractDomState(page).catch(() => null);
    if (!lastDomState) {
      await page.waitForTimeout(400).catch(() => null);
      continue;
    }

    const snapshot = buildSnapshotFromDom(lastDomState, '', previousState);
    const splashText = normalizeComparableText(lastDomState.bodyText || '');
    const stillDownloading =
      splashText.includes(normalizeComparableText('deine nachrichten werden heruntergeladen')) ||
      splashText.includes(normalizeComparableText('your messages are being downloaded'));

    if (snapshot.connectionStatus === 'connected' && !stillDownloading) {
      return lastDomState;
    }

    if (snapshot.qrRequired === true || snapshot.connectionStatus === 'error') {
      return lastDomState;
    }

    if (snapshot.connectionStatus === 'connected' && snapshot.sessionValid === true && stillDownloading) {
      await page.waitForTimeout(900).catch(() => null);
      continue;
    }

    await page.waitForTimeout(650).catch(() => null);
  }

  return lastDomState;
}

async function ensureChannelsTabOpen(page, timeoutMs, debugContext) {
  const channelsTab = await locateFirstVisible(
    page,
    [
      '[data-testid="navbar-item-newsletters"]',
      'button[aria-label="Kanäle"]',
      'button[aria-label="Kanaele"]',
      'button[aria-label="Channels"]'
    ],
    Math.max(1500, Math.floor(timeoutMs / 4))
  ).catch(() => null);

  if (!channelsTab) {
    return false;
  }

  const alreadyActive = await channelsTab
    .evaluate((element) => {
      const pressed = element.getAttribute('aria-pressed');
      const selected = element.getAttribute('data-navbar-item-selected');
      return pressed === 'true' || selected === 'true';
    })
    .catch(() => false);

  if (!alreadyActive) {
    await channelsTab.click().catch(() => null);
    await page.waitForTimeout(800).catch(() => null);
  }

  await captureChannelDebugStep(page, debugContext, 'channels-tab-open', {
    alreadyActive
  }).catch(() => null);
  logPlaywrightWorkerEvent(
    'whatsapp.nav.channels_tab_open',
    '[WHATSAPP_NAV_CHANNELS_TAB_OPEN] WhatsApp Kanäle/Updates Ansicht wurde geöffnet.',
    {
      alreadyActive,
      currentUrl: page.url()
    }
  );

  return true;
}

async function detectWrongChatSearchArea(page) {
  return await page
    .evaluate(() => {
      const text = String(document.body?.innerText || '').replace(/\s+/g, ' ').trim().toLowerCase();
      return (
        text.includes('es wurden keine chats, kontakte oder nachrichten gefunden') ||
        text.includes('no chats, contacts or messages found')
      );
    })
    .catch(() => false);
}

async function detectChannelList(page, plan) {
  return await page
    .evaluate((lookupTerms) => {
      const normalize = (value) =>
        String(value || '')
          .normalize('NFKD')
          .replace(/[\u0300-\u036f]/g, '')
          .toLowerCase()
          .trim();
      const terms = (Array.isArray(lookupTerms) ? lookupTerms : []).map(normalize).filter(Boolean);
      const candidates = [
        ...document.querySelectorAll('[data-testid="cell-frame-container"], [role="listitem"], [role="option"], span[title], div[title], a')
      ];
      const visible = candidates.filter((element) => {
        const style = window.getComputedStyle(element);
        const rect = element.getBoundingClientRect();
        return style.visibility !== 'hidden' && style.display !== 'none' && rect.width > 0 && rect.height > 0;
      });
      const visibleTexts = visible
        .map((element) => String(element.innerText || element.textContent || element.getAttribute('title') || '').replace(/\s+/g, ' ').trim())
        .filter(Boolean)
        .slice(0, 20);
      const matchedItem = visibleTexts.find((value) => {
        const normalizedValue = normalize(value);
        return terms.some((term) => normalizedValue.includes(term));
      });
      const hasChannelList =
        visible.some((element) => String(element.getAttribute('data-testid') || '').includes('cell-frame')) ||
        visibleTexts.some((value) => normalize(value).includes('follower'));

      return {
        hasChannelList,
        matchedItem: matchedItem || '',
        visibleTexts
      };
    }, plan.lookupTerms)
    .catch(() => ({
      hasChannelList: false,
      matchedItem: '',
      visibleTexts: []
    }));
}

async function openChannelFromChannelsArea(page, plan, timeoutMs, debugContext) {
  const channelsTabOpened = await ensureChannelsTabOpen(page, timeoutMs, debugContext);
  if (!channelsTabOpened) {
    return false;
  }

  const wrongArea = await detectWrongChatSearchArea(page);
  if (wrongArea) {
    logPlaywrightWorkerEvent(
      'whatsapp.nav.chat_search_wrong_area',
      '[WHATSAPP_NAV_CHAT_SEARCH_WRONG_AREA] Die normale Chat-Suche wurde erkannt und wird für Channel-Navigation nicht verwendet.',
      {
        currentUrl: page.url()
      },
      'warning'
    );
  }

  const channelListState = await detectChannelList(page, plan);
  if (channelListState.hasChannelList) {
    logPlaywrightWorkerEvent(
      'whatsapp.nav.channel_list_found',
      '[WHATSAPP_CHANNEL_LIST_FOUND] WhatsApp Channel-Liste ist sichtbar.',
      {
        currentUrl: page.url(),
        matchedItem: channelListState.matchedItem,
        visibleItems: channelListState.visibleTexts.slice(0, 6)
      }
    );
  }

  const directChannelMatch = await clickVisibleActionByText(
    page,
    [
      '[data-testid="cell-frame-container"]',
      '[role="listitem"]',
      '[role="option"]',
      'span[title]',
      'div[title]',
      'a'
    ],
    plan.lookupTerms,
    Math.max(1800, Math.floor(timeoutMs / 3))
  ).catch(() => null);

  if (directChannelMatch) {
    await page.waitForTimeout(1200).catch(() => null);
    await captureChannelDebugStep(page, debugContext, 'after-channel-list-select', {
      matchedText: directChannelMatch.matchedText
    }).catch(() => null);
    logPlaywrightWorkerEvent(
      'whatsapp.nav.channel_target_selected',
      `[WHATSAPP_CHANNEL_TARGET_SELECTED] ${directChannelMatch.matchedText || plan.targetLabel}`,
      {
        currentUrl: page.url(),
        matchedText: directChannelMatch.matchedText
      }
    );
    return true;
  }

  const searchBox = await locateFirstVisible(
    page,
    [
      '[data-testid="chat-list-search"]',
      '[data-testid="chat-list-search-container"] [role="textbox"]',
      'nav [role="textbox"]',
      '#side [role="textbox"]',
      '[contenteditable="true"][role="textbox"]'
    ],
    Math.max(2000, Math.floor(timeoutMs / 3))
  ).catch(() => null);

  if (!searchBox || !plan.lookupTerms.length) {
    return false;
  }

  const searchTerms = dedupeTextValues([
    plan.targetLabel,
    ...plan.lookupTerms,
    cleanText(plan.channelId)
  ]).filter((term) => !/^https?:\/\//i.test(term));

  for (const searchTerm of searchTerms) {
    if (!cleanText(searchTerm)) {
      continue;
    }

    await searchBox.click().catch(() => null);
    await searchBox.fill('').catch(() => null);
    await searchBox.fill(searchTerm).catch(() => null);
    await page.waitForTimeout(900).catch(() => null);
    const searchWrongArea = await detectWrongChatSearchArea(page);
    if (searchWrongArea) {
      logPlaywrightWorkerEvent(
        'whatsapp.nav.chat_search_wrong_area',
        '[WHATSAPP_NAV_CHAT_SEARCH_WRONG_AREA] Die Suche befindet sich noch im falschen Chat-Bereich und wird uebersprungen.',
        {
          currentUrl: page.url(),
          searchTerm
        },
        'warning'
      );
      await searchBox.fill('').catch(() => null);
      continue;
    }
    await captureChannelDebugStep(page, debugContext, 'after-sidebar-search', {
      searchTerm
    }).catch(() => null);

    const clickedEntry = await clickVisibleActionByText(
      page,
      [
        '[data-testid="cell-frame-container"]',
        '[role="listitem"]',
        '[role="option"]',
        'span[title]',
        'div[title]',
        'a'
      ],
      [searchTerm, ...plan.lookupTerms],
      Math.max(2500, Math.floor(timeoutMs / 3))
    );

    if (!clickedEntry) {
      continue;
    }

    await page.waitForTimeout(1200).catch(() => null);
    await captureChannelDebugStep(page, debugContext, 'after-sidebar-open', {
      matchedText: clickedEntry.matchedText,
      searchTerm
    }).catch(() => null);
    logPlaywrightWorkerEvent(
      'whatsapp.nav.channel_target_selected',
      `[WHATSAPP_CHANNEL_TARGET_SELECTED] ${clickedEntry.matchedText || searchTerm}`,
      {
        currentUrl: page.url(),
        matchedText: clickedEntry.matchedText,
        searchTerm
      }
    );
    return true;
  }

  return false;
}

function buildChannelNavigationResult(page, plan, surface, debugContext, extras = {}) {
  return {
    success: true,
    channelReachable: true,
    targetUrl: plan.webChannelUrl,
    currentUrl: cleanText(surface?.url) || page.url(),
    currentTitle: cleanText(surface?.title),
    channelNavigationStatus: cleanText(extras.channelNavigationStatus) || 'channel_found',
    channelAdminStatus: cleanText(extras.channelAdminStatus) || 'admin_unknown',
    debugArtifacts: Array.isArray(debugContext?.artifacts) ? debugContext.artifacts : []
  };
}

async function throwChannelNavigationError(page, plan, debugContext, code, message, extras = {}) {
  const summary = buildChannelSurfaceState(await collectVisibleUiSummary(page).catch(() => ({})), plan);
  await captureChannelDebugStep(page, debugContext, 'navigation-error', {
    code,
    message
  }).catch(() => null);

  const statusPatch = {
    lastError: message,
    lastErrorAt: nowIso(),
    channelReachable: false,
    channelNavigationStatus: cleanText(extras.channelNavigationStatus) || cleanText(code),
    channelAdminStatus: cleanText(extras.channelAdminStatus) || '',
    lastChannelTargetRef: cleanText(plan.externalChannelUrl || plan.webChannelUrl),
    lastChannelDebugAt: nowIso(),
    lastChannelDebugMessage: message,
    lastChannelDebugArtifacts: Array.isArray(debugContext?.artifacts) ? debugContext.artifacts : [],
    currentUrl: cleanText(summary.url) || page.url(),
    currentTitle: cleanText(summary.title)
  };
  persistRuntimePatch(statusPatch, cleanText(readRuntimeMeta().connectionStatus) || 'connected');

  throw buildWhatsappPlaywrightError(message, {
    code,
    retryable: false
  });
}

async function ensureChannelSurface(session, providerInfo, input = {}, options = {}) {
  const page = session.page;
  const plan = buildWhatsappChannelPlan(input.targetRef, input.targetMeta && typeof input.targetMeta === 'object' ? input.targetMeta : input);
  const timeoutMs = Math.max(Number(providerInfo.config.actionTimeoutMs || 15000), 18000);
  const debugContext = buildChannelDebugContext(plan, options);
  const previousRuntime = readRuntimeMeta();

  logPlaywrightWorkerEvent(
    'whatsapp.channel.nav.start',
    `[WHATSAPP_CHANNEL_NAV_START] Navigiere zu ${plan.targetLabel || plan.webChannelUrl}.`,
    {
      channelId: plan.channelId,
      targetRef: plan.externalChannelUrl,
      debug: debugContext.enabled
    }
  );
  logWebOnlyMode(providerInfo, {
    source: 'channel.navigation',
    targetRef: plan.webChannelUrl
  });

  await navigateTo(page, cleanText(providerInfo.config.webUrl) || WHATSAPP_WEB_URL, providerInfo.config.startupTimeoutMs);
  await waitForInteractiveWhatsapp(page, Math.max(6000, Math.floor(timeoutMs / 2)), previousRuntime);
  await captureChannelDebugStep(page, debugContext, 'after-web-whatsapp', {
    url: page.url()
  }).catch(() => null);
  const initialBlockedAction = await findBlockedOpenInAppAction(page, 500).catch(() => null);
  if (initialBlockedAction) {
    logBlockedExternalAction(initialBlockedAction, {
      currentUrl: page.url(),
      phase: 'after-web-whatsapp'
    });
  }

  logPlaywrightWorkerEvent(
    'whatsapp.channel.link.open',
    `[WHATSAPP_CHANNEL_LINK_OPEN] Oeffne ${plan.webChannelUrl}.`,
    {
      channelId: plan.channelId,
      targetRef: plan.webChannelUrl
    }
  );
  await navigateTo(page, plan.webChannelUrl, timeoutMs);
  await waitForInteractiveWhatsapp(page, Math.max(5000, timeoutMs), previousRuntime);
  await captureChannelDebugStep(page, debugContext, 'after-web-channel-route', {
    url: page.url()
  }).catch(() => null);

  let surface = buildChannelSurfaceState(await collectVisibleUiSummary(page).catch(() => ({})), plan);
  if (Array.isArray(surface.headings) && surface.headings.length) {
    plan.lookupTerms = dedupeTextValues([...plan.lookupTerms, ...surface.headings]);
  }
  const redirectedOffWeb = cleanText(surface.url) && !isWebWhatsappUrl(surface.url);
  if (redirectedOffWeb) {
    logPlaywrightWorkerEvent(
      'whatsapp.channel.web_redirect',
      `[WHATSAPP_CHANNEL_WEB_REDIRECT] Unerwuenschte Weiterleitung erkannt: ${surface.url}`,
      {
        targetRef: plan.webChannelUrl,
        redirectedUrl: surface.url
      }
    );
  }

  const blockedLandingAction = await findBlockedOpenInAppAction(page, 900).catch(() => null);
  if (blockedLandingAction) {
    logBlockedExternalAction(blockedLandingAction, {
      currentUrl: page.url(),
      phase: 'channel-route-check'
    });
  }

  if (!surface.channelFound && (surface.landingActionVisible || redirectedOffWeb || blockedLandingAction)) {
    await navigateTo(page, cleanText(providerInfo.config.webUrl) || WHATSAPP_WEB_URL, timeoutMs);
    await waitForInteractiveWhatsapp(page, Math.max(5000, timeoutMs), previousRuntime);
    await captureChannelDebugStep(page, debugContext, 'after-web-only-reset', {
      url: page.url()
    }).catch(() => null);
    surface = buildChannelSurfaceState(await collectVisibleUiSummary(page).catch(() => ({})), plan);
    if (Array.isArray(surface.headings) && surface.headings.length) {
      plan.lookupTerms = dedupeTextValues([...plan.lookupTerms, ...surface.headings]);
    }
  }

  if (!surface.channelFound) {
    const searchOpened = await openChannelFromChannelsArea(page, plan, timeoutMs, debugContext).catch(() => false);
    if (searchOpened) {
      surface = buildChannelSurfaceState(await collectVisibleUiSummary(page).catch(() => ({})), plan);
      if (Array.isArray(surface.headings) && surface.headings.length) {
        plan.lookupTerms = dedupeTextValues([...plan.lookupTerms, ...surface.headings]);
      }
    }
  }

  if (!surface.channelFound) {
    await throwChannelNavigationError(
      page,
      plan,
      debugContext,
      'WHATSAPP_CHANNEL_NAVIGATION_NOT_IMPLEMENTED',
      `WHATSAPP_CHANNEL_NAVIGATION_NOT_IMPLEMENTED: ${plan.targetLabel || plan.webChannelUrl} konnte in WhatsApp Web noch nicht bis zur sendefaehigen Channel-Oberflaeche navigiert werden.`,
      {
        channelNavigationStatus: 'WHATSAPP_CHANNEL_NAVIGATION_NOT_IMPLEMENTED',
        channelAdminStatus: 'unknown'
      }
    );
  }

  logPlaywrightWorkerEvent(
    'whatsapp.channel.found',
    `[WHATSAPP_CHANNEL_FOUND] ${plan.targetLabel || plan.webChannelUrl} wurde geoeffnet.`,
    {
      channelId: plan.channelId,
      currentUrl: surface.url
    }
  );

  const shouldCaptureComposerArtifacts = debugContext.enabled || cleanText(input?.targetMeta?.targetType) === 'WHATSAPP_TEST_CHANNEL';
  let composerMatch = await locateComposer(page, Math.max(2000, Math.floor(timeoutMs / 2)), {
    debugContext,
    captureArtifacts: shouldCaptureComposerArtifacts,
    prefixBase: debugContext?.runId ? `channel-debug-${debugContext.runId}` : 'channel-composer'
  }).catch(() => null);
  let composer = composerMatch?.locator || null;
  let channelAdminStatus = surface.adminActionVisible ? 'admin_controls_visible' : 'unknown';

  if (!composer) {
    const clickedComposerAction = await clickVisibleActionByText(
      page,
      ['button', '[role="button"]', 'a'],
      ['Create update', 'Create post', 'Post update', 'New post', 'Update', 'Erstellen', 'Beitrag', 'Message'],
      Math.max(2500, Math.floor(timeoutMs / 3))
    );

    if (clickedComposerAction) {
      channelAdminStatus = 'admin_controls_visible';
      await page.waitForTimeout(1000).catch(() => null);
      await captureChannelDebugStep(page, debugContext, 'after-compose-trigger', {
        matchedText: clickedComposerAction.matchedText
      }).catch(() => null);
      composerMatch = await locateComposer(page, Math.max(2500, Math.floor(timeoutMs / 2)), {
        debugContext,
        captureArtifacts: shouldCaptureComposerArtifacts,
        prefixBase: debugContext?.runId ? `channel-debug-${debugContext.runId}` : 'channel-composer'
      }).catch(() => composerMatch);
      composer = composerMatch?.locator || null;
    }
  }

  if (!composer) {
    const refreshedSurface = buildChannelSurfaceState(await collectVisibleUiSummary(page).catch(() => ({})), plan);
    const noAdminRights = refreshedSurface.readOnlyHintVisible || (!refreshedSurface.adminActionVisible && refreshedSurface.channelFound);
    if (noAdminRights) {
      await throwChannelNavigationError(
        page,
        plan,
        debugContext,
        'WHATSAPP_CHANNEL_NO_ADMIN_RIGHTS',
        `WHATSAPP_CHANNEL_NO_ADMIN_RIGHTS: ${plan.targetLabel || plan.webChannelUrl} ist sichtbar, aber fuer dieses Konto nicht als Posting-Admin erkennbar.`,
        {
          channelNavigationStatus: 'WHATSAPP_CHANNEL_FOUND',
          channelAdminStatus: 'WHATSAPP_CHANNEL_NO_ADMIN_RIGHTS'
        }
      );
    }

    await throwChannelNavigationError(
      page,
      plan,
      debugContext,
      'WHATSAPP_CHANNEL_COMPOSER_NOT_FOUND',
      `WHATSAPP_CHANNEL_COMPOSER_NOT_FOUND: ${plan.targetLabel || plan.webChannelUrl} wurde geoeffnet, aber der Channel-Composer wurde noch nicht gefunden.`,
      {
        channelNavigationStatus: 'WHATSAPP_CHANNEL_FOUND',
        channelAdminStatus: channelAdminStatus || 'unknown'
      }
    );
  }

  logPlaywrightWorkerEvent(
    'whatsapp.channel.composer.found',
    `[WHATSAPP_CHANNEL_COMPOSER_FOUND] Composer fuer ${plan.targetLabel || plan.webChannelUrl} erkannt.`,
    {
      channelId: plan.channelId,
      currentUrl: page.url()
    }
  );
  await captureChannelDebugStep(page, debugContext, 'after-composer-found', {
    url: page.url()
  }).catch(() => null);

  const result = buildChannelNavigationResult(page, plan, surface, debugContext, {
    channelNavigationStatus: 'WHATSAPP_CHANNEL_COMPOSER_FOUND',
    channelAdminStatus: channelAdminStatus || 'admin_controls_visible'
  });
  persistRuntimePatch(
    {
      channelReachable: true,
      channelNavigationStatus: result.channelNavigationStatus,
      channelAdminStatus: result.channelAdminStatus,
      lastChannelTargetRef: cleanText(plan.externalChannelUrl),
      lastChannelDebugAt: debugContext.enabled ? nowIso() : readRuntimeMeta().lastChannelDebugAt,
      lastChannelDebugMessage: debugContext.enabled ? 'Channel-Debug erfolgreich abgeschlossen.' : '',
      lastChannelDebugArtifacts: Array.isArray(debugContext.artifacts) ? debugContext.artifacts : [],
      lastChannelPreferredSelector: cleanText(composerMatch?.selectorInfo?.preferredSelector),
      lastChannelComposerCandidates: Array.isArray(composerMatch?.selectorInfo?.candidates)
        ? composerMatch.selectorInfo.candidates
        : [],
      lastChannelScreenshotPath: cleanText(composerMatch?.visibleArtifacts?.screenshotPath),
      lastChannelDomSnapshotPath: cleanText(composerMatch?.visibleArtifacts?.jsonPath),
      lastChannelHtmlSnapshotPath: cleanText(composerMatch?.visibleArtifacts?.htmlPath),
      currentUrl: result.currentUrl,
      currentTitle: result.currentTitle,
      lastError: '',
      lastErrorAt: null
    },
    cleanText(readRuntimeMeta().connectionStatus) || 'connected'
  );

  return {
    ...result,
    composer,
    preferredSelector: cleanText(composerMatch?.selectorInfo?.preferredSelector),
    composerCandidates: Array.isArray(composerMatch?.selectorInfo?.candidates) ? composerMatch.selectorInfo.candidates : []
  };
}

async function ensureConnectedSnapshot(session, previousState = null) {
  const snapshot = await getAutomationAdapter().inspectSession(session, session.providerInfo, {
    preserveLocation: true,
    previousState
  });

  if (snapshot.connectionStatus === 'qr_required') {
    throw buildWhatsappPlaywrightError('WhatsApp QR Login ist erforderlich.', {
      code: 'WHATSAPP_QR_REQUIRED'
    });
  }

  if (snapshot.connectionStatus === 'session_expired') {
    throw buildWhatsappPlaywrightError('WhatsApp Session ist abgelaufen.', {
      code: 'WHATSAPP_SESSION_EXPIRED'
    });
  }

  if (snapshot.connectionStatus !== 'connected' || snapshot.sessionValid !== true) {
    throw buildWhatsappPlaywrightError('WhatsApp ist nicht verbunden.', {
      code: 'WHATSAPP_NOT_CONNECTED'
    });
  }

  return snapshot;
}

async function listVisibleModalDialogs(page) {
  return await page
    .evaluate(() => {
      const isVisible = (element) => {
        if (!element) {
          return false;
        }

        const style = window.getComputedStyle(element);
        const rect = element.getBoundingClientRect();
        return style.visibility !== 'hidden' && style.display !== 'none' && rect.width > 0 && rect.height > 0;
      };
      const pickText = (value) => String(value || '').replace(/\s+/g, ' ').trim();

      return [...document.querySelectorAll('[role="dialog"][aria-modal="true"]')]
        .filter((element) => isVisible(element))
        .slice(0, 5)
        .map((element) => ({
          text: pickText(element.innerText || element.textContent || ''),
          ariaLabel: pickText(element.getAttribute('aria-label') || ''),
          htmlSnippet: String(element.outerHTML || '').slice(0, 600)
        }));
    })
    .catch(() => []);
}

async function dismissBlockingDialogIfPresent(page, timeoutMs = 1200) {
  const startedAt = Date.now();
  let visibleDialogs = [];

  while (Date.now() - startedAt <= timeoutMs) {
    visibleDialogs = await listVisibleModalDialogs(page);
    if (!visibleDialogs.length) {
      return {
        dialogs: [],
        dismissed: true
      };
    }

    const closeButton = await locateFirstVisible(
      page,
      [
        '[role="dialog"][aria-modal="true"] button[aria-label*="Schließen"]',
        '[role="dialog"][aria-modal="true"] button[aria-label*="Schliessen"]',
        '[role="dialog"][aria-modal="true"] button[aria-label*="Close"]',
        '[role="dialog"][aria-modal="true"] button[aria-label*="Dismiss"]',
        '[role="dialog"][aria-modal="true"] button[title*="Schließen"]',
        '[role="dialog"][aria-modal="true"] button[title*="Close"]',
        '[role="dialog"][aria-modal="true"] button:has-text("Schließen")',
        '[role="dialog"][aria-modal="true"] button:has-text("Schliessen")',
        '[role="dialog"][aria-modal="true"] button:has-text("Close")',
        '[role="dialog"][aria-modal="true"] button:has-text("Abbrechen")',
        '[role="dialog"][aria-modal="true"] button:has-text("Jetzt nicht")',
        '[role="dialog"][aria-modal="true"] button:has-text("Not now")',
        '[role="dialog"][aria-modal="true"] button:has-text("Später")',
        '[role="dialog"][aria-modal="true"] button:has-text("Later")',
        '[role="dialog"][aria-modal="true"] [data-testid="x"]',
        '[role="dialog"][aria-modal="true"] [data-icon="x"]'
      ],
      350
    ).catch(() => null);

    if (closeButton) {
      await closeButton
        .evaluate((element) => {
          const target = element instanceof HTMLElement ? element.closest('button, [role="button"]') || element : null;
          if (!(target instanceof HTMLElement)) {
            return false;
          }

          target.focus();
          target.click();
          return true;
        })
        .catch(() => null);
    } else {
      await page.keyboard.press('Escape').catch(() => null);
      await page
        .evaluate(() => {
          const dialogs = [...document.querySelectorAll('[role="dialog"][aria-modal="true"]')];
          for (const dialog of dialogs) {
            if (!(dialog instanceof HTMLElement)) {
              continue;
            }

            dialog.dataset.codexIgnoredDialog = 'true';
            dialog.style.pointerEvents = 'none';
          }
        })
        .catch(() => null);
    }

    await page.waitForTimeout(250).catch(() => null);
  }

  return {
    dialogs: visibleDialogs,
    dismissed: false
  };
}

async function focusComposerWithoutPointer(page, composer, timeoutMs = 1500) {
  const dialogState = await dismissBlockingDialogIfPresent(page, Math.min(timeoutMs, 1200)).catch(() => ({
    dialogs: [],
    dismissed: false
  }));

  const focused = await composer
    .evaluate((element) => {
      if (!(element instanceof HTMLElement)) {
        return false;
      }

      element.scrollIntoView({
        block: 'center',
        inline: 'nearest'
      });
      element.focus();

      const selection = window.getSelection();
      const range = document.createRange();
      range.selectNodeContents(element);
      range.collapse(false);
      selection?.removeAllRanges();
      selection?.addRange(range);

      return document.activeElement === element || element.contains(document.activeElement);
    })
    .catch(() => false);

  if (!focused) {
    await composer.focus().catch(() => null);
  }

  await page.waitForTimeout(80).catch(() => null);
  return dialogState;
}

async function clearComposerAndType(page, composer, text) {
  const messageText = cleanText(text);
  await focusComposerWithoutPointer(page, composer);
  await page.keyboard.press('Control+A').catch(() => null);
  await page.keyboard.press('Backspace').catch(() => null);
  await composer
    .evaluate((element) => {
      if (!(element instanceof HTMLElement)) {
        return false;
      }

      element.innerHTML = '<p dir="auto"><br></p>';
      const selection = window.getSelection();
      const range = document.createRange();
      range.selectNodeContents(element);
      range.collapse(true);
      selection?.removeAllRanges();
      selection?.addRange(range);
      element.dispatchEvent(new InputEvent('input', { bubbles: true, inputType: 'deleteContentBackward' }));
      return true;
    })
    .catch(() => null);

  if (!messageText) {
    return;
  }

  await page.keyboard.insertText(messageText).catch(() => null);

  const typedText = await composer
    .evaluate((element) => String(element?.innerText || element?.textContent || '').replace(/\u00A0/g, ' ').trim())
    .catch(() => '');
  if (typedText) {
    return;
  }

  await composer
    .evaluate((element, nextText) => {
      if (!(element instanceof HTMLElement)) {
        return false;
      }

      const lines = String(nextText || '').split(/\r?\n/);
      element.innerHTML = '';
      for (const line of lines) {
        const paragraph = document.createElement('p');
        paragraph.setAttribute('dir', 'auto');
        if (line) {
          paragraph.textContent = line;
        } else {
          paragraph.appendChild(document.createElement('br'));
        }
        element.appendChild(paragraph);
      }

      const selection = window.getSelection();
      const range = document.createRange();
      range.selectNodeContents(element);
      range.collapse(false);
      selection?.removeAllRanges();
      selection?.addRange(range);
      element.dispatchEvent(new InputEvent('input', { bubbles: true, inputType: 'insertText', data: String(nextText || '') }));
      return true;
    }, messageText)
    .catch(() => null);
}

async function activateElementWithoutPointer(locator) {
  return await locator
    .evaluate((element) => {
      const target = element instanceof HTMLElement ? element.closest('button, [role="button"]') || element : null;
      if (!(target instanceof HTMLElement)) {
        return false;
      }

      target.focus();
      target.click();
      return true;
    })
    .catch(() => false);
}

async function triggerSendWithoutPointer(page, timeoutMs, options = {}) {
  const sendButton = await locateSendButton(page, Math.max(1200, Math.floor(timeoutMs / 2))).catch(() => null);
  if (sendButton) {
    const triggered = await activateElementWithoutPointer(sendButton);
    if (triggered) {
      await page.waitForTimeout(900).catch(() => null);
      return true;
    }
  }

  if (options.allowKeyboardFallback !== false) {
    await page.keyboard.press('Enter').catch(() => null);
    await page.waitForTimeout(900).catch(() => null);
    return true;
  }

  return false;
}

function resolveTempImageExtension(imageInput = '') {
  const normalizedInput = cleanText(imageInput);

  if (/^data:image\//i.test(normalizedInput)) {
    const match = normalizedInput.match(/^data:image\/([a-z0-9.+-]+);base64,/i);
    const mimeExtension = cleanText(match?.[1] || '').toLowerCase();
    if (mimeExtension === 'jpeg') {
      return '.jpg';
    }
    if (mimeExtension === 'svg+xml') {
      return '.svg';
    }
    return mimeExtension ? `.${mimeExtension}` : '.png';
  }

  try {
    const parsedUrl = new URL(normalizedInput);
    return path.extname(parsedUrl.pathname || '') || '.jpg';
  } catch {
    return path.extname(normalizedInput) || '.jpg';
  }
}

async function downloadImageToFile(imageUrl = '', config = getWhatsappPlaywrightConfig()) {
  const normalizedImageUrl = cleanText(imageUrl);
  if (!normalizedImageUrl) {
    return '';
  }

  if (/^data:image\//i.test(normalizedImageUrl)) {
    const base64Match = normalizedImageUrl.match(/^data:image\/[a-z0-9.+-]+;base64,(.+)$/i);
    if (!base64Match?.[1]) {
      return '';
    }

    const tempPath = path.join(getTempDirectory(config), `${crypto.randomUUID()}${resolveTempImageExtension(normalizedImageUrl)}`);
    fs.writeFileSync(tempPath, Buffer.from(base64Match[1], 'base64'));
    return tempPath;
  }

  if (!/^https?:\/\//i.test(normalizedImageUrl)) {
    return fs.existsSync(normalizedImageUrl) ? normalizedImageUrl : '';
  }

  const response = await fetch(normalizedImageUrl);
  if (!response.ok) {
    throw buildWhatsappPlaywrightError(`Bild konnte nicht geladen werden (${response.status}).`, {
      code: 'WHATSAPP_IMAGE_DOWNLOAD_ERROR'
    });
  }

  const arrayBuffer = await response.arrayBuffer();
  const extension = resolveTempImageExtension(normalizedImageUrl);
  const tempPath = path.join(getTempDirectory(config), `${crypto.randomUUID()}${extension}`);
  fs.writeFileSync(tempPath, Buffer.from(arrayBuffer));
  return tempPath;
}

async function waitForMediaComposerSurface(page, timeoutMs) {
  const startedAt = Date.now();
  const mediaSurfaceSelectors = [
    '[data-testid="media-caption-input"]',
    '[data-testid="media-viewer"]',
    '[data-testid="media-preview"]',
    'div[role="dialog"] [role="textbox"][contenteditable="true"]',
    '[data-animate-modal-popup="true"] [role="textbox"][contenteditable="true"]',
    'div[role="dialog"] img',
    'div[role="dialog"] video',
    'div[role="dialog"] canvas'
  ];

  while (Date.now() - startedAt <= timeoutMs) {
    const mediaSurface = await locateFirstVisible(page, mediaSurfaceSelectors, 400).catch(() => null);
    if (mediaSurface) {
      return true;
    }

    await page.waitForTimeout(250).catch(() => null);
  }

  return false;
}

async function locateMediaCaptionBox(page, timeoutMs) {
  return await locateFirstVisible(
    page,
    [
      '[data-testid="media-caption-input"]',
      'div[role="dialog"] [data-testid="media-caption-input"]',
      'div[role="dialog"] [role="textbox"][contenteditable="true"]',
      '[data-animate-modal-popup="true"] [role="textbox"][contenteditable="true"]',
      '[contenteditable="true"][aria-label*="caption"]',
      '[contenteditable="true"][aria-label*="Caption"]',
      '[contenteditable="true"][aria-label*="Beschriftung"]',
      '[contenteditable="true"][aria-label*="Bildunterschrift"]'
    ],
    timeoutMs
  );
}

async function locateWhatsappImageFileInput(page, timeoutMs) {
  const footerFileInput = page.locator('footer input[type="file"][accept*="image"]').last();
  if ((await footerFileInput.count().catch(() => 0)) > 0) {
    return footerFileInput;
  }

  const genericFileInput = page.locator('input[type="file"][accept*="image"]').last();
  if ((await genericFileInput.count().catch(() => 0)) > 0) {
    return genericFileInput;
  }

  const attachButton = await locateFirstVisible(
    page,
    [
      'button[aria-label*="Anh\u00e4ngen"]',
      'button[aria-label*="Attach"]',
      'button[title*="Anh\u00e4ngen"]',
      'button[title*="Attach"]',
      '[data-testid="attach-button"]',
      'span[data-icon="clip"]'
    ],
    timeoutMs
  );

  if (!attachButton) {
    return null;
  }

  await attachButton.click().catch(() => null);
  await page.waitForTimeout(250).catch(() => null);

  const revealedFileInput = page.locator('input[type="file"][accept*="image"]').last();
  if ((await revealedFileInput.count().catch(() => 0)) > 0) {
    return revealedFileInput;
  }

  return null;
}

async function waitForMediaSendCompletion(page, timeoutMs) {
  const startedAt = Date.now();

  while (Date.now() - startedAt <= timeoutMs) {
    const mediaStillOpen = await locateFirstVisible(
      page,
      [
        '[data-testid="media-caption-input"]',
        '[data-testid="media-viewer"]',
        '[data-testid="media-preview"]',
        'div[role="dialog"] img',
        'div[role="dialog"] video',
        'div[role="dialog"] canvas'
      ],
      400
    ).catch(() => null);

    if (!mediaStillOpen) {
      const composerVisible = await locateFirstVisible(
        page,
        ['footer [data-testid="conversation-compose-box-input"]'],
        400
      ).catch(() => null);
      if (composerVisible) {
        return true;
      }
    }

    await page.waitForTimeout(250).catch(() => null);
  }

  return false;
}

async function sendTextThroughComposer(page, text, timeoutMs) {
  const composerMatch = await locateComposer(page, timeoutMs, {
    captureArtifacts: false
  });
  const composer = composerMatch?.locator || null;
  if (!composer) {
    throw buildWhatsappPlaywrightError('Kein WhatsApp Channel-Composer verfuegbar.', {
      code: 'WHATSAPP_CHANNEL_COMPOSER_NOT_FOUND',
      retryable: false
    });
  }
  await clearComposerAndType(page, composer, text);
  const sendTriggered = await triggerSendWithoutPointer(page, timeoutMs, {
    allowKeyboardFallback: true
  });
  if (!sendTriggered) {
    throw buildWhatsappPlaywrightError('WhatsApp Text konnte nicht abgesendet werden.', {
      code: 'WHATSAPP_SEND_BUTTON_MISSING'
    });
  }
}

async function sendImageWithCaption(page, imageUrl, caption, config, timeoutMs) {
  const downloadedImagePath = await downloadImageToFile(imageUrl, config);
  if (!downloadedImagePath) {
    throw buildWhatsappPlaywrightError('Bild fuer den WhatsApp Versand fehlt.', {
      code: 'WHATSAPP_IMAGE_MISSING',
      retryable: false
    });
  }

  try {
    logPlaywrightWorkerEvent(
      'whatsapp.image.upload.start',
      '[WHATSAPP_IMAGE_UPLOAD_START] WhatsApp Bild-Upload wird vorbereitet.',
      {
        imageInput: cleanText(imageUrl),
        downloadedImagePath
      }
    );

    let fileInput = await locateWhatsappImageFileInput(page, timeoutMs);
    if (!fileInput) {
      throw buildWhatsappPlaywrightError('Kein WhatsApp Upload-Feld gefunden.', {
        code: 'WHATSAPP_FILE_INPUT_MISSING'
      });
    }
    await fileInput.setInputFiles(downloadedImagePath);
    const mediaSurfaceVisible = await waitForMediaComposerSurface(page, timeoutMs);
    if (!mediaSurfaceVisible) {
      const attachButton = await locateFirstVisible(
        page,
        [
          'button[aria-label*="Anh\u00e4ngen"]',
          'button[aria-label*="Attach"]',
          'button[title*="Anh\u00e4ngen"]',
          'button[title*="Attach"]',
          '[data-testid="attach-button"]',
          'span[data-icon="clip"]'
        ],
        Math.max(1500, Math.floor(timeoutMs / 2))
      ).catch(() => null);

      if (attachButton) {
        await attachButton.click().catch(() => null);
        await page.waitForTimeout(250).catch(() => null);
        fileInput = await locateWhatsappImageFileInput(page, Math.max(1500, Math.floor(timeoutMs / 2)));
      }

      if (!fileInput) {
        logPlaywrightWorkerEvent(
          'whatsapp.image.upload.failed',
          '[WHATSAPP_IMAGE_UPLOAD_FAILED] WhatsApp Bild-Upload konnte nicht initialisiert werden.',
          {
            imageInput: cleanText(imageUrl)
          },
          'warning'
        );
        throw buildWhatsappPlaywrightError('Kein WhatsApp Upload-Feld fuer das Bild gefunden.', {
          code: 'WHATSAPP_IMAGE_UPLOAD_FAILED'
        });
      }

      await fileInput.setInputFiles(downloadedImagePath);
      const retriedMediaSurfaceVisible = await waitForMediaComposerSurface(page, Math.max(1500, Math.floor(timeoutMs / 2)));
      if (!retriedMediaSurfaceVisible) {
        logPlaywrightWorkerEvent(
          'whatsapp.image.upload.failed',
          '[WHATSAPP_IMAGE_UPLOAD_FAILED] WhatsApp Medienoberflaeche erschien nach dem Upload nicht.',
          {
            imageInput: cleanText(imageUrl),
            downloadedImagePath
          },
          'warning'
        );
        throw buildWhatsappPlaywrightError('WhatsApp Medienoberflaeche fuer den Bild-Upload wurde nicht sichtbar.', {
          code: 'WHATSAPP_IMAGE_UPLOAD_FAILED'
        });
      }
    }

    if (cleanText(caption)) {
      const captionBox = await locateMediaCaptionBox(page, timeoutMs);

      if (!captionBox) {
        logPlaywrightWorkerEvent(
          'whatsapp.image.upload.failed',
          '[WHATSAPP_IMAGE_UPLOAD_FAILED] Kein Caption-Feld fuer den WhatsApp Bild-Upload gefunden.',
          {
            imageInput: cleanText(imageUrl),
            downloadedImagePath
          },
          'warning'
        );
        throw buildWhatsappPlaywrightError('Kein Caption-Feld fuer das WhatsApp Bild gefunden.', {
          code: 'WHATSAPP_CAPTION_MISSING'
        });
      }

      await clearComposerAndType(page, captionBox, caption);
    }

    const sendTriggered = await triggerSendWithoutPointer(page, timeoutMs, {
      allowKeyboardFallback: true
    });
    if (!sendTriggered) {
      throw buildWhatsappPlaywrightError('WhatsApp Bild konnte nicht abgesendet werden.', {
        code: 'WHATSAPP_SEND_BUTTON_MISSING'
      });
    }

    const sendCompleted = await waitForMediaSendCompletion(page, timeoutMs);
    if (!sendCompleted) {
      logPlaywrightWorkerEvent(
        'whatsapp.image.upload.failed',
        '[WHATSAPP_IMAGE_UPLOAD_FAILED] WhatsApp Bildversand wurde nicht bestaetigt.',
        {
          imageInput: cleanText(imageUrl),
          downloadedImagePath
        },
        'warning'
      );
      throw buildWhatsappPlaywrightError('WhatsApp Bildversand wurde nicht bestaetigt.', {
        code: 'WHATSAPP_IMAGE_UPLOAD_FAILED'
      });
    }

    logPlaywrightWorkerEvent(
      'whatsapp.image.upload.success',
      '[WHATSAPP_IMAGE_UPLOAD_SUCCESS] WhatsApp Bild-Upload wurde sichtbar abgeschlossen.',
      {
        imageInput: cleanText(imageUrl),
        downloadedImagePath
      }
    );

    return {
      imageUploadVerified: true,
      imageInput: cleanText(imageUrl)
    };
  } finally {
    if (downloadedImagePath && (/^https?:\/\//i.test(cleanText(imageUrl)) || /^data:image\//i.test(cleanText(imageUrl)))) {
      try {
        fs.unlinkSync(downloadedImagePath);
      } catch {
        // Intentionally ignored. Temp file cleanup is best-effort only.
      }
    }
  }
}

async function defaultStartSession(providerInfo) {
  const playwright = await loadPlaywrightModule();
  const chromium = playwright.chromium;
  if (!chromium?.launchPersistentContext) {
    throw buildWhatsappPlaywrightError('Chromium-Launcher von Playwright ist nicht verfuegbar.', {
      code: 'WHATSAPP_PLAYWRIGHT_CHROMIUM_MISSING',
      retryable: false
    });
  }

  const executablePath = cleanText(providerInfo.executablePath);
  if (!executablePath || !fs.existsSync(executablePath)) {
    throw buildWhatsappPlaywrightError('Kein lokaler Chromium/Edge Browser fuer WhatsApp gefunden.', {
      code: 'WHATSAPP_PLAYWRIGHT_BROWSER_MISSING',
      retryable: false
    });
  }

  const context = await chromium.launchPersistentContext(getProfileDirectory(providerInfo.config), {
    executablePath,
    headless: providerInfo.config.headless === true,
    viewport: DESKTOP_VIEWPORT,
    screen: DESKTOP_VIEWPORT,
    deviceScaleFactor: 1,
    isMobile: false,
    hasTouch: false,
    args: [
      '--disable-dev-shm-usage',
      '--disable-notifications',
      '--no-default-browser-check',
      '--start-maximized',
      `--window-size=${DESKTOP_VIEWPORT.width},${DESKTOP_VIEWPORT.height}`
    ]
  });
  await installWebOnlyModeGuards(context, providerInfo);
  const page = context.pages()[0] || (await context.newPage());
  await page.setViewportSize(DESKTOP_VIEWPORT).catch(() => null);
  await page.bringToFront().catch(() => null);
  await navigateTo(page, cleanText(providerInfo.config.webUrl) || WHATSAPP_WEB_URL, providerInfo.config.startupTimeoutMs);
  await waitForSettledUi(page);

  return {
    context,
    page,
    providerInfo,
    createdAt: nowIso()
  };
}

async function defaultStopSession(session) {
  await session?.context?.close?.();
}

function buildRuntimeResult(snapshot = {}, providerInfo = null, extras = {}) {
  const effectiveProviderInfo = providerInfo || runtimeState.session?.providerInfo || buildProviderInfo();
  const profileDir = getProfileDirectory(effectiveProviderInfo.config);

  return {
    instanceId: 'primary',
    browserStatus: runtimeState.browserStatus === 'error' ? 'error' : cleanText(snapshot.browserStatus) || runtimeState.browserStatus,
    channelReachable: snapshot.channelReachable === true,
    workerPid: process.pid,
    browserChannel: cleanText(effectiveProviderInfo.browserChannel),
    browserProfileDir: profileDir,
    browserExecutablePath: cleanText(effectiveProviderInfo.executablePath),
    profileWritable: ensureProfileWritable(profileDir),
    loginTimeoutMs: resolveLoginTimeoutMs(effectiveProviderInfo),
    ...snapshot,
    ...extras
  };
}

function persistSnapshotState(snapshot = {}, providerInfo = null, extras = {}) {
  const runtime = buildRuntimeResult(snapshot, providerInfo, extras);
  persistRuntimePatch(
    {
      connectionStatus: cleanText(runtime.connectionStatus) || 'not_connected',
      workerStatus: cleanText(runtime.workerStatus) || 'stopped',
      sessionValid: runtime.sessionValid === true,
      qrRequired: runtime.qrRequired === true,
      qrCodeDataUrl: cleanText(runtime.qrCodeDataUrl),
      browserStatus: cleanText(runtime.browserStatus) || 'unknown',
      channelReachable: runtime.channelReachable === true,
      workerPid: process.pid,
      browserChannel: cleanText(runtime.browserChannel),
      browserProfileDir: cleanText(runtime.browserProfileDir),
      browserExecutablePath: cleanText(runtime.browserExecutablePath),
      profileWritable: runtime.profileWritable === true,
      loginTimeoutMs: Number(runtime.loginTimeoutMs || 120000),
      ...extras
    },
    cleanText(runtime.connectionStatus) || 'not_connected'
  );
  return runtime;
}

function markBrowserClosedTooEarly(providerInfo = buildProviderInfo()) {
  const message = 'Der sichtbare WhatsApp Browser wurde geschlossen, bevor die Session stabil verbunden war.';
  runtimeState.session = null;
  runtimeState.sessionId = '';
  runtimeState.browserStatus = 'stopped';
  releaseWorkerOwnership();
  logPlaywrightWorkerEvent(
    'whatsapp.worker.stop.reason',
    '[WHATSAPP_WORKER_STOP_REASON] browser_closed_too_early',
    {
      workerPid: process.pid,
      profileDir: getProfileDirectory(providerInfo.config)
    },
    'warning'
  );
  logPlaywrightWorkerEvent(
    'whatsapp.browser.closed_too_early',
    `[WHATSAPP_BROWSER_CLOSED_TOO_EARLY] ${message}`,
    {
      workerPid: process.pid,
      profileDir: getProfileDirectory(providerInfo.config)
    },
    'warning'
  );
  persistRuntimePatch(
    {
      connectionStatus: 'error',
      workerStatus: 'error',
      sessionValid: false,
      qrRequired: false,
      qrCodeDataUrl: '',
      browserStatus: 'stopped',
      channelReachable: false,
      lastError: message,
      lastErrorAt: nowIso(),
      workerPid: process.pid,
      loginMonitorActive: false
    },
    'error'
  );
}

async function persistSessionArtifacts(session, providerInfo) {
  const profileDir = getProfileDirectory(providerInfo.config);
  const storageStatePath = getStorageStatePath(providerInfo.config);
  const sessionMetaPath = path.join(cleanText(providerInfo.config.sessionDir), 'session-meta.json');
  const connectedAt = nowIso();
  const profileWritable = ensureProfileWritable(profileDir);

  logPlaywrightWorkerEvent(
    'whatsapp.session.save.start',
    `[WHATSAPP_SESSION_SAVE_START] Persistiere WhatsApp Session im Profil ${profileDir}.`,
    {
      profileDir,
      storageStatePath
    }
  );

  ensureDirectory(path.dirname(storageStatePath));
  ensureDirectory(path.dirname(sessionMetaPath));

  if (typeof session?.context?.storageState === 'function') {
    await session.context.storageState({ path: storageStatePath });
  }

  fs.writeFileSync(
    sessionMetaPath,
    JSON.stringify(
      {
        connectedAt,
        storageStatePath,
        profileDir,
        workerPid: process.pid
      },
      null,
      2
    ),
    'utf8'
  );

  logPlaywrightWorkerEvent(
    'whatsapp.session.save.ok',
    `[WHATSAPP_SESSION_SAVE_OK] WhatsApp Session wurde im persistenten Profil gespeichert.`,
    {
      profileDir,
      storageStatePath,
      profileWritable
    }
  );

  persistRuntimePatch(
    {
      connectionStatus: 'connected',
      workerStatus: 'running',
      sessionValid: true,
      qrRequired: false,
      qrCodeDataUrl: '',
      browserStatus: 'running',
      channelReachable: true,
      lastConnectedAt: connectedAt,
      sessionSavedAt: connectedAt,
      browserChannel: cleanText(providerInfo.browserChannel),
      browserProfileDir: profileDir,
      browserExecutablePath: cleanText(providerInfo.executablePath),
      profileWritable,
      workerPid: process.pid,
      lastError: '',
      lastErrorAt: null,
      loginMonitorActive: false
    },
    'connected'
  );

  return {
    lastConnectedAt: connectedAt,
    sessionSavedAt: connectedAt,
    storageStatePath,
    profileWritable
  };
}

function isPendingLoginSnapshot(snapshot = {}) {
  const status = cleanText(snapshot.connectionStatus);
  return ['qr_required', 'not_connected'].includes(status) && cleanText(snapshot.workerStatus) !== 'stopped';
}

async function startLoginMonitor(previousState = null) {
  if (runtimeState.loginMonitorPromise || !runtimeState.session) {
    return runtimeState.loginMonitorPromise;
  }

  const session = runtimeState.session;
  const providerInfo = session.providerInfo;
  const timeoutMs = resolveLoginTimeoutMs(providerInfo);
  const pollIntervalMs = Math.max(250, Number(providerInfo.config?.loginPollIntervalMs || 1500));
  runtimeState.loginMonitorAbort = false;

  runtimeState.loginMonitorPromise = (async () => {
    let snapshot = previousState;
    let qrLogged = false;
    let waitingLogged = false;
    const startedAt = Date.now();

    persistRuntimePatch(
      {
        loginMonitorActive: true,
        loginTimeoutMs: timeoutMs
      },
      cleanText(previousState?.connectionStatus) || 'not_connected'
    );

    while (!runtimeState.loginMonitorAbort && runtimeState.session === session) {
      if (hasClosedSession(session)) {
        markBrowserClosedTooEarly(providerInfo);
        return null;
      }

      snapshot = await getAutomationAdapter().inspectSession(session, providerInfo, {
        previousState: snapshot || previousState,
        preserveLocation: true
      });

      const runtime = persistSnapshotState(snapshot, providerInfo, {
        loginMonitorActive: true
      });

      if (runtime.qrRequired === true && !qrLogged) {
        qrLogged = true;
        logPlaywrightWorkerEvent(
          'whatsapp.qr.visible',
          '[WHATSAPP_QR_VISIBLE] WhatsApp QR Login ist sichtbar.',
          {
            workerPid: process.pid,
            profileDir: runtime.browserProfileDir
          }
        );
      }

      if (isPendingLoginSnapshot(runtime) && !waitingLogged) {
        waitingLogged = true;
        logPlaywrightWorkerEvent(
          'whatsapp.qr.waiting',
          `[WHATSAPP_QR_SCAN_WAITING] Warte bis zu ${timeoutMs}ms auf den echten QR Scan.`,
          {
            timeoutMs,
            pollIntervalMs
          }
        );
      }

      if (runtime.connectionStatus === 'connected' && runtime.sessionValid === true) {
        logPlaywrightWorkerEvent(
          'whatsapp.login.success_detected',
          '[WHATSAPP_LOGIN_SUCCESS_DETECTED] WhatsApp Hauptoberflaeche wurde nach dem QR Scan erkannt.',
          {
            currentUrl: runtime.currentUrl || '',
            currentTitle: runtime.currentTitle || ''
          }
        );
        const sessionInfo = await persistSessionArtifacts(session, providerInfo);
        return {
          ...runtime,
          ...sessionInfo,
          loginMonitorActive: false
        };
      }

      if (Date.now() - startedAt >= timeoutMs) {
        const timeoutMessage = `WhatsApp QR Login Timeout nach ${timeoutMs}ms.`;
        logPlaywrightWorkerEvent(
          'whatsapp.login.timeout',
          `[WHATSAPP_LOGIN_TIMEOUT] ${timeoutMessage}`,
          {
            timeoutMs,
            connectionStatus: runtime.connectionStatus
          },
          'warning'
        );
        persistRuntimePatch(
          {
            lastError: timeoutMessage,
            lastErrorAt: nowIso(),
            loginMonitorActive: false
          },
          cleanText(runtime.connectionStatus) || 'qr_required'
        );
        return runtime;
      }

      await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
    }

    return snapshot;
  })().finally(() => {
    runtimeState.loginMonitorPromise = null;
    persistRuntimePatch(
      {
        loginMonitorActive: false
      },
      cleanText(readRuntimeMeta().connectionStatus) || 'not_connected'
    );
  });

  return runtimeState.loginMonitorPromise;
}

async function defaultInspectSession(session, providerInfo, options = {}) {
  if (!options.preserveLocation) {
    await navigateTo(
      session.page,
      cleanText(providerInfo.config.webUrl) || WHATSAPP_WEB_URL,
      providerInfo.config.actionTimeoutMs
    );
    await waitForSettledUi(session.page);
  }

  const domState = await extractDomState(session.page);
  const qrCodeDataUrl = await buildQrDataUrl(session.page, domState);
  const snapshot = buildSnapshotFromDom(domState, qrCodeDataUrl, options.previousState || null);

  return {
    ...snapshot,
    inspectedAt: nowIso(),
    currentUrl: cleanText(domState.url) || cleanText(providerInfo.config.webUrl) || WHATSAPP_WEB_URL,
    currentTitle: cleanText(domState.title)
  };
}

async function defaultRefreshSession(session, providerInfo) {
  await navigateTo(
    session.page,
    cleanText(providerInfo.config.webUrl) || WHATSAPP_WEB_URL,
    providerInfo.config.startupTimeoutMs
  );
  await waitForSettledUi(session.page);
  return await defaultInspectSession(session, providerInfo, {
    preserveLocation: true
  });
}

async function defaultTestChannel(session, providerInfo, input = {}) {
  const navigation = await ensureChannelSurface(session, providerInfo, input, {
    debug: false
  });

  return {
    channelReachable: true,
    targetUrl: navigation.targetUrl,
    currentUrl: navigation.currentUrl,
    currentTitle: navigation.currentTitle,
    channelNavigationStatus: navigation.channelNavigationStatus,
    channelAdminStatus: navigation.channelAdminStatus,
    debugArtifacts: navigation.debugArtifacts
  };
}

async function defaultDebugChannel(session, providerInfo, input = {}) {
  const navigation = await ensureChannelSurface(session, providerInfo, input, {
    debug: true
  });

  return {
    success: true,
    channelReachable: true,
    targetUrl: navigation.targetUrl,
    currentUrl: navigation.currentUrl,
    currentTitle: navigation.currentTitle,
    channelNavigationStatus: navigation.channelNavigationStatus,
    channelAdminStatus: navigation.channelAdminStatus,
    debugArtifacts: navigation.debugArtifacts
  };
}

async function defaultManualChannelDebugCapture(session, providerInfo, input = {}) {
  return await buildManualChannelDebugCapture(session, providerInfo, input, {
    waitForChannel: false,
    prefix: 'manual-channel-debug-capture'
  });
}

async function defaultManualChannelDebugWait(session, providerInfo, input = {}) {
  return await buildManualChannelDebugCapture(session, providerInfo, input, {
    waitForChannel: true,
    prefix: 'manual-channel-debug-wait'
  });
}

async function defaultSendPhase(session, providerInfo, input = {}) {
  const targetMeta = input.targetMeta && typeof input.targetMeta === 'object' ? input.targetMeta : {};
  const navigation = await ensureChannelSurface(
    session,
    providerInfo,
    {
      ...input,
      targetMeta
    },
    {
      debug: false
    }
  );
  const targetLabel = cleanText(targetMeta?.targetLabel || targetMeta?.name || input.targetRef) || navigation.targetUrl;

  logPlaywrightWorkerEvent(
    'whatsapp.channel.send.start',
    `[WHATSAPP_CHANNEL_SEND_START] Versandvorbereitung fuer ${targetLabel}.`,
    {
      sendId: cleanText(input.sendId),
      targetRef: cleanText(input.targetRef),
      currentUrl: navigation.currentUrl
    }
  );

  let sendMetadata = {
    messageKind: cleanText(input.imageUrl) ? 'image_caption' : 'text',
    imageUploadVerified: false,
    imageInput: cleanText(input.imageUrl)
  };

  try {
    if (cleanText(input.imageUrl)) {
      sendMetadata = {
        ...sendMetadata,
        ...(await sendImageWithCaption(
          session.page,
          cleanText(input.imageUrl),
          cleanText(input.text),
          providerInfo.config,
          providerInfo.config.actionTimeoutMs
        ))
      };
    } else {
      await sendTextThroughComposer(session.page, cleanText(input.text), providerInfo.config.actionTimeoutMs);
    }
  } catch (error) {
    const sendErrorArtifacts = await captureComposerDebugArtifacts(session.page, {
      prefix: 'channel-send-error',
      label: cleanText(input.phase) || 'send'
    }).catch(() => null);

    if (sendErrorArtifacts?.jsonPath || sendErrorArtifacts?.screenshotPath) {
      persistRuntimePatch(
        {
          lastChannelDebugAt: nowIso(),
          lastChannelDebugMessage: error instanceof Error ? error.message : 'WhatsApp Versandfehler.',
          lastChannelScreenshotPath: cleanText(sendErrorArtifacts.screenshotPath) || readRuntimeMeta().lastChannelScreenshotPath,
          lastChannelDomSnapshotPath: cleanText(sendErrorArtifacts.jsonPath) || readRuntimeMeta().lastChannelDomSnapshotPath
        },
        cleanText(readRuntimeMeta().connectionStatus) || 'connected'
      );
    }
    throw error;
  }

  logPlaywrightWorkerEvent(
    'whatsapp.channel.send.success',
    `[WHATSAPP_CHANNEL_SEND_SUCCESS] Versandoberflaeche fuer ${targetLabel} erfolgreich genutzt.`,
    {
      sendId: cleanText(input.sendId),
      targetRef: cleanText(input.targetRef),
      currentUrl: session.page.url()
    }
  );

  return {
    status: 'sent',
    duplicatePrevented: false,
    messageId: `wa-web-${Date.now()}`,
    deliveryId: cleanText(input.sendId) || `wa-web-${Date.now()}`,
    targetUrl: navigation.targetUrl,
    currentUrl: session.page.url(),
    channelNavigationStatus: navigation.channelNavigationStatus,
    channelAdminStatus: navigation.channelAdminStatus,
    response: sendMetadata
  };
}

function buildProviderInfo() {
  const config = getWhatsappPlaywrightConfig();
  const executablePath = resolveBrowserPath(config);
  const browserChannel = cleanText(config.browserChannel) || (executablePath.toLowerCase().includes('edge') ? 'msedge' : 'chrome');
  const available = Boolean(automationOverride || executablePath);

  return {
    mode: 'playwright',
    available,
    executablePath,
    browserChannel,
    profileDir: getProfileDirectory(config),
    config: {
      ...config,
      executablePath,
      browserChannel,
      webUrl: cleanText(config.webUrl) || WHATSAPP_WEB_URL
    }
  };
}

export function getWhatsappPlaywrightProviderInfo() {
  return buildProviderInfo();
}

export function __classifyWhatsappWebOnlyTargetForTests(candidate = {}) {
  return classifyWhatsappWebOnlyTarget(candidate);
}

async function ensureRuntimeSession({ forceRestart = false } = {}) {
  const providerInfo = buildProviderInfo();
  if (!providerInfo.available) {
    throw buildWhatsappPlaywrightError('Kein lokaler Browser fuer den WhatsApp Playwright Worker gefunden.', {
      code: 'WHATSAPP_PLAYWRIGHT_BROWSER_MISSING',
      retryable: false
    });
  }

  if (runtimeState.session && hasClosedSession(runtimeState.session)) {
    markBrowserClosedTooEarly(providerInfo);
  }

  if (forceRestart && runtimeState.session) {
    await stopWhatsappPlaywrightWorker({
      reason: 'force_restart',
      forceClose: true
    }).catch(() => null);
  }

  if (runtimeState.session) {
    return runtimeState.session;
  }

  if (runtimeState.launchPromise) {
    return await runtimeState.launchPromise;
  }

  runtimeState.browserStatus = 'launching';
  runtimeState.loginMonitorAbort = false;
  claimWorkerOwnership(providerInfo);
  runtimeState.launchPromise = (async () => {
    const nextSession = await getAutomationAdapter().startSession(providerInfo);
    runtimeState.session = nextSession;
    runtimeState.browserStatus = 'running';
    runtimeState.sessionId = crypto.randomUUID();
    logPlaywrightWorkerEvent(
      'whatsapp.browser.opened',
      '[WHATSAPP_BROWSER_OPENED] Sichtbarer WhatsApp Playwright Browser wurde gestartet.',
      {
        workerPid: process.pid,
        browserChannel: cleanText(providerInfo.browserChannel),
        executablePath: cleanText(providerInfo.executablePath),
        profileDir: getProfileDirectory(providerInfo.config),
        keepBrowserOpen: isKeepBrowserOpenEnabled(providerInfo)
      }
    );
    persistRuntimePatch(
      {
        workerStatus: 'running',
        browserStatus: 'running',
        browserChannel: cleanText(providerInfo.browserChannel),
        browserProfileDir: getProfileDirectory(providerInfo.config),
        browserExecutablePath: cleanText(providerInfo.executablePath),
        workerPid: process.pid,
        profileWritable: ensureProfileWritable(getProfileDirectory(providerInfo.config)),
        loginMonitorActive: false
      },
      'not_connected'
    );
    return nextSession;
  })();

  try {
    return await runtimeState.launchPromise;
  } catch (error) {
    runtimeState.browserStatus = 'error';
    runtimeState.session = null;
    runtimeState.sessionId = '';
    releaseWorkerOwnership();
    persistRuntimePatch(
      {
        workerStatus: 'error',
        browserStatus: 'error',
        lastError: error instanceof Error ? error.message : 'WhatsApp Browser konnte nicht gestartet werden.',
        lastErrorAt: nowIso(),
        loginMonitorActive: false
      },
      'error'
    );
    throw error;
  } finally {
    runtimeState.launchPromise = null;
  }
}

async function inspectRuntime(previousState = null, options = {}) {
  const session = await ensureRuntimeSession({
    forceRestart: options.forceRestart === true
  });
  if (hasClosedSession(session)) {
    markBrowserClosedTooEarly(session.providerInfo);
    throw buildWhatsappPlaywrightError('Der WhatsApp Browser wurde vorzeitig geschlossen.', {
      code: 'WHATSAPP_BROWSER_CLOSED_TOO_EARLY'
    });
  }
  const snapshot = await getAutomationAdapter().inspectSession(session, session.providerInfo, {
    previousState,
    preserveLocation: options.preserveLocation === true
  });
  const runtime = persistSnapshotState(snapshot, session.providerInfo);

  if (runtime.connectionStatus === 'connected' && runtime.sessionValid === true) {
    const sessionInfo = await persistSessionArtifacts(session, session.providerInfo);
    return {
      ...runtime,
      ...sessionInfo,
      loginMonitorActive: false
    };
  }

  if (options.startLoginMonitor !== false && isPendingLoginSnapshot(runtime)) {
    void startLoginMonitor(runtime).catch((error) => {
      persistRuntimePatch(
        {
          lastError: error instanceof Error ? error.message : 'WhatsApp Login Monitor fehlgeschlagen.',
          lastErrorAt: nowIso(),
          loginMonitorActive: false
        },
        cleanText(readRuntimeMeta().connectionStatus) || 'error'
      );
    });
  }

  return runtime;
}

export async function startWhatsappPlaywrightWorker(previousState = null) {
  return await inspectRuntime(previousState, {
    preserveLocation: false,
    startLoginMonitor: true
  });
}

export async function stopWhatsappPlaywrightWorker(options = {}) {
  const providerInfo = runtimeState.session?.providerInfo || buildProviderInfo();
  const reason = cleanText(options.reason) || 'manual_stop';
  const forceClose = options.forceClose === true;
  const keepBrowserOpen = isKeepBrowserOpenEnabled(providerInfo);
  const profileDir = getProfileDirectory(providerInfo.config);

  logPlaywrightWorkerEvent(
    'whatsapp.browser.close.requested',
    '[WHATSAPP_BROWSER_CLOSE_REQUESTED] WhatsApp Browser soll geschlossen werden.',
    {
      reason,
      forceClose,
      keepBrowserOpen,
      workerPid: process.pid,
      profileDir
    }
  );
  logPlaywrightWorkerEvent(
    'whatsapp.worker.stop.reason',
    `[WHATSAPP_WORKER_STOP_REASON] ${reason}`,
    {
      reason,
      forceClose,
      keepBrowserOpen,
      workerPid: process.pid
    },
    forceClose ? 'warning' : 'info'
  );

  if (keepBrowserOpen && forceClose !== true && runtimeState.session) {
    logPlaywrightWorkerEvent(
      'whatsapp.browser.close.skipped_debug',
      '[WHATSAPP_BROWSER_CLOSE_SKIPPED_DEBUG] Debug-Modus haelt den WhatsApp Browser offen.',
      {
        reason,
        workerPid: process.pid,
        profileDir
      },
      'warning'
    );
    const snapshot = await getAutomationAdapter()
      .inspectSession(runtimeState.session, providerInfo, {
        previousState: readRuntimeMeta(),
        preserveLocation: true
      })
      .catch(() => null);

    if (snapshot) {
      return persistSnapshotState(snapshot, providerInfo, {
        loginMonitorActive: false
      });
    }

    return buildRuntimeResult(
      {
        connectionStatus: cleanText(readRuntimeMeta().connectionStatus) || 'not_connected',
        workerStatus: 'running',
        sessionValid: parseBool(readRuntimeMeta().sessionValid, false),
        qrRequired: parseBool(readRuntimeMeta().qrRequired, false),
        browserStatus: cleanText(readRuntimeMeta().browserStatus) || 'running',
        channelReachable: parseBool(readRuntimeMeta().channelReachable, false)
      },
      providerInfo,
      {
        loginMonitorActive: false
      }
    );
  }

  runtimeState.loginMonitorAbort = true;
  if (runtimeState.session) {
    await getAutomationAdapter().stopSession(runtimeState.session).catch(() => null);
  }

  runtimeState.session = null;
  runtimeState.sessionId = '';
  runtimeState.browserStatus = 'stopped';
  releaseWorkerOwnership();
  persistRuntimePatch(
    {
      connectionStatus: 'not_connected',
      workerStatus: 'stopped',
      sessionValid: false,
      qrRequired: false,
      qrCodeDataUrl: '',
      browserStatus: 'stopped',
      channelReachable: false,
      loginMonitorActive: false
    },
    'not_connected'
  );

  return {
    instanceId: 'primary',
    connectionStatus: 'not_connected',
    workerStatus: 'stopped',
    sessionValid: false,
    qrRequired: false,
    qrCodeDataUrl: '',
    browserStatus: 'stopped',
    channelReachable: false,
    inspectedAt: nowIso()
  };
}

export async function connectWhatsappPlaywrightWorker(previousState = null, input = {}) {
  return await inspectRuntime(previousState, {
    preserveLocation: input.preserveLocation !== false,
    startLoginMonitor: true
  });
}

export async function refreshWhatsappPlaywrightSession(previousState = null) {
  const session = await ensureRuntimeSession({
    forceRestart: true
  });
  await getAutomationAdapter().refreshSession(session, session.providerInfo, previousState);
  const snapshot = await getAutomationAdapter().inspectSession(session, session.providerInfo, {
    previousState,
    preserveLocation: true
  });
  const runtime = persistSnapshotState(snapshot, session.providerInfo);
  if (isPendingLoginSnapshot(runtime)) {
    void startLoginMonitor(runtime).catch(() => null);
  }
  return runtime;
}

export async function testWhatsappPlaywrightConnection(input = {}, previousState = null) {
  return await inspectRuntime(previousState, {
    preserveLocation: input.preserveLocation !== false,
    startLoginMonitor: true
  });
}

export async function testWhatsappPlaywrightChannel(input = {}, previousState = null) {
  const session = await ensureRuntimeSession();
  await ensureConnectedSnapshot(session, previousState);
  const channelResult = await getAutomationAdapter().testChannel(session, session.providerInfo, input);
  if (channelResult.channelReachable !== true) {
    throw buildWhatsappPlaywrightError('WhatsApp Kanal ist nicht erreichbar oder nicht schreibbar.', {
      code: 'WHATSAPP_CHANNEL_UNREACHABLE'
    });
  }
  const snapshot = await getAutomationAdapter().inspectSession(session, session.providerInfo, {
    previousState,
    preserveLocation: true
  });

  return {
    ...snapshot,
    channelReachable: channelResult.channelReachable === true,
    targetUrl: cleanText(channelResult.targetUrl),
    currentUrl:
      cleanText(channelResult.currentUrl) ||
      snapshot.currentUrl ||
      session?.page?.url?.() ||
      cleanText(channelResult.targetUrl),
    currentTitle: cleanText(channelResult.currentTitle) || snapshot.currentTitle || '',
    channelNavigationStatus: cleanText(channelResult.channelNavigationStatus) || 'WHATSAPP_CHANNEL_FOUND',
    channelAdminStatus: cleanText(channelResult.channelAdminStatus) || '',
    debugArtifacts: Array.isArray(channelResult.debugArtifacts) ? channelResult.debugArtifacts : []
  };
}

export async function debugWhatsappPlaywrightChannel(input = {}, previousState = null) {
  const session = await ensureRuntimeSession();
  await ensureConnectedSnapshot(session, previousState);
  const channelResult = await getAutomationAdapter().debugChannel(session, session.providerInfo, input);
  const snapshot = await getAutomationAdapter().inspectSession(session, session.providerInfo, {
    previousState,
    preserveLocation: true
  });

  return {
    ...snapshot,
    success: channelResult?.success !== false,
    channelReachable: channelResult?.channelReachable === true,
    targetUrl: cleanText(channelResult?.targetUrl),
    currentUrl:
      cleanText(channelResult?.currentUrl) ||
      snapshot.currentUrl ||
      session?.page?.url?.() ||
      cleanText(channelResult?.targetUrl),
    currentTitle: cleanText(channelResult?.currentTitle) || snapshot.currentTitle || '',
    channelNavigationStatus: cleanText(channelResult?.channelNavigationStatus) || 'WHATSAPP_CHANNEL_FOUND',
    channelAdminStatus: cleanText(channelResult?.channelAdminStatus) || '',
    debugArtifacts: Array.isArray(channelResult?.debugArtifacts) ? channelResult.debugArtifacts : []
  };
}

export async function captureWhatsappPlaywrightManualChannelDebug(input = {}, previousState = null) {
  const session = await ensureRuntimeSession();
  await ensureConnectedSnapshot(session, previousState);
  return await getAutomationAdapter().manualChannelDebugCapture(session, session.providerInfo, input);
}

export async function waitForWhatsappPlaywrightManualChannelDebug(input = {}, previousState = null) {
  const session = await ensureRuntimeSession();
  await ensureConnectedSnapshot(session, previousState);
  return await getAutomationAdapter().manualChannelDebugWait(session, session.providerInfo, input);
}

export async function runWhatsappPlaywrightHealthCheck(previousState = null) {
  return await inspectRuntime(previousState, {
    preserveLocation: true,
    startLoginMonitor: true
  });
}

export async function recoverWhatsappPlaywrightWorker(previousState = null) {
  const stopped = await stopWhatsappPlaywrightWorker({
    reason: 'recover_restart',
    forceClose: true
  });
  const started = await startWhatsappPlaywrightWorker(previousState);
  return {
    ...started,
    lastRestartAt: nowIso(),
    previousState: stopped.connectionStatus
  };
}

export async function resetWhatsappPlaywrightSession(previousState = null) {
  const providerInfo = buildProviderInfo();
  const profileDir = getProfileDirectory(providerInfo.config);
  const storageStatePath = getStorageStatePath(providerInfo.config);
  const profileBackupDir = getProfileResetBackupPath(providerInfo.config);

  await stopWhatsappPlaywrightWorker({
    reason: 'reset_session',
    forceClose: true
  }).catch(() => null);

  if (fs.existsSync(profileDir)) {
    fs.renameSync(profileDir, profileBackupDir);
  }
  ensureDirectory(profileDir);

  if (fs.existsSync(storageStatePath)) {
    fs.unlinkSync(storageStatePath);
  }

  logPlaywrightWorkerEvent(
    'whatsapp.session.reset',
    '[WHATSAPP_SESSION_RESET] WhatsApp Session wurde zurueckgesetzt und ein neues Profil vorbereitet.',
    {
      profileDir,
      profileBackupDir
    }
  );

  persistRuntimePatch(
    {
      connectionStatus: 'not_connected',
      workerStatus: 'stopped',
      sessionValid: false,
      qrRequired: false,
      qrCodeDataUrl: '',
      browserStatus: 'stopped',
      channelReachable: false,
      lastError: '',
      lastErrorAt: null,
      profileBackupDir,
      sessionResetAt: nowIso(),
      loginMonitorActive: false
    },
    'not_connected'
  );

  return {
    instanceId: 'primary',
    connectionStatus: 'not_connected',
    workerStatus: 'stopped',
    sessionValid: false,
    qrRequired: false,
    qrCodeDataUrl: '',
    browserStatus: 'stopped',
    channelReachable: false,
    inspectedAt: nowIso(),
    profileBackupDir,
    sessionResetAt: nowIso()
  };
}

export async function sendWhatsappPlaywrightPhase(input = {}, previousState = null) {
  const session = await ensureRuntimeSession();
  await ensureConnectedSnapshot(session, previousState);
  return await getAutomationAdapter().sendPhase(session, session.providerInfo, input);
}

export function __buildWhatsappChannelPlanForTests(targetRef = '', targetMeta = {}) {
  return buildWhatsappChannelPlan(targetRef, targetMeta);
}

export function __setWhatsappAutomationOverrideForTests(adapter = null) {
  automationOverride = adapter;
}

export async function __resetWhatsappPlaywrightWorkerForTests() {
  automationOverride = null;
  await stopWhatsappPlaywrightWorker().catch(() => null);
}
