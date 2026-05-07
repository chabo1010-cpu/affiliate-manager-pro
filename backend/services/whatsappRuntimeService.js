import crypto from 'node:crypto';
import { getDb } from '../db.js';
import { getStorageConfig, getWhatsappControlConfig, getWhatsappDeliveryConfig } from '../env.js';
import { upsertAppSession } from './databaseService.js';
import {
  WHATSAPP_ALERT_TARGET_TYPE,
  getWhatsappAlertSettings,
  saveWhatsappAlertSettings,
  sendWhatsappAlert,
  sendWhatsappAlertTest
} from './systemAlertService.js';
import {
  captureWhatsappPlaywrightManualChannelDebug,
  connectWhatsappPlaywrightWorker,
  debugWhatsappPlaywrightChannel,
  getWhatsappPlaywrightProviderInfo,
  recoverWhatsappPlaywrightWorker,
  resetWhatsappPlaywrightSession,
  refreshWhatsappPlaywrightSession,
  runWhatsappPlaywrightHealthCheck,
  startWhatsappPlaywrightWorker,
  stopWhatsappPlaywrightWorker,
  testWhatsappPlaywrightChannel,
  testWhatsappPlaywrightConnection,
  waitForWhatsappPlaywrightManualChannelDebug
} from './whatsappPlaywrightWorkerService.js';

const db = getDb();
const WHATSAPP_RUNTIME_SESSION_KEY = 'whatsapp_output:session:default';
const WHATSAPP_HEALTH_INTERVAL_MS = 60 * 1000;
let healthMonitorStarted = false;

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

function normalizeConnectionStatus(value = '', fallback = 'not_connected') {
  const normalized = cleanText(value).toLowerCase();
  if (['connected', 'not_connected', 'qr_required', 'session_expired', 'error', 'recovering'].includes(normalized)) {
    return normalized;
  }
  if (['ready', 'online', 'connected_ok'].includes(normalized)) {
    return 'connected';
  }
  if (['disconnected', 'offline', 'inactive'].includes(normalized)) {
    return 'not_connected';
  }
  if (['expired', 'invalid'].includes(normalized)) {
    return 'session_expired';
  }
  if (['qr', 'qr-needed', 'qr_needed'].includes(normalized)) {
    return 'qr_required';
  }
  return fallback;
}

function normalizeWorkerStatus(value = '', fallback = 'stopped') {
  const normalized = cleanText(value).toLowerCase();
  if (['running', 'stopped', 'error', 'recovering'].includes(normalized)) {
    return normalized;
  }
  if (['active', 'started'].includes(normalized)) {
    return 'running';
  }
  if (['failed'].includes(normalized)) {
    return 'error';
  }
  return fallback;
}

function deriveHealthStatus(config = {}, meta = {}) {
  if (!config.deliveryEnabled) {
    return 'disabled';
  }
  if (!config.providerConfigured) {
    return 'not_configured';
  }
  if (meta.workerStatus === 'recovering') {
    return 'recovering';
  }
  if (meta.connectionStatus === 'session_expired') {
    return 'session_expired';
  }
  if (meta.connectionStatus === 'qr_required') {
    return 'qr_required';
  }
  if (meta.connectionStatus === 'error' || meta.workerStatus === 'error') {
    return 'error';
  }
  if (meta.workerStatus === 'stopped' || config.workerEnabled !== true) {
    return 'stopped';
  }
  if (meta.connectionStatus === 'connected' && meta.sessionValid === true) {
    return 'healthy';
  }
  return 'waiting';
}

function logWhatsappRuntimeEvent(eventType = '', message = '', payload = null, level = 'info') {
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
  ).run(level, cleanText(eventType) || 'whatsapp.runtime', cleanText(message) || 'WhatsApp Runtime Event', payload ? JSON.stringify(payload) : null, nowIso());
}

function readSettingsRow() {
  return db.prepare(`SELECT * FROM app_settings WHERE id = 1`).get() || null;
}

function readRuntimeRow() {
  return db.prepare(`SELECT * FROM app_sessions WHERE session_key = ? LIMIT 1`).get(WHATSAPP_RUNTIME_SESSION_KEY) || null;
}

function getOpenWhatsappQueueCount() {
  const row =
    db
      .prepare(
        `
          SELECT COUNT(*) AS count
          FROM publishing_targets
          WHERE channel_type = 'whatsapp'
            AND status IN ('pending', 'retry', 'sending')
        `
      )
      .get() || {};

  return Number(row.count || 0);
}

function getWhatsappQueueBreakdown() {
  const row =
    db
      .prepare(
        `
          SELECT
            SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending_count,
            SUM(CASE WHEN status = 'retry' THEN 1 ELSE 0 END) AS retry_count,
            SUM(CASE WHEN status = 'sending' THEN 1 ELSE 0 END) AS sending_count,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_count,
            SUM(CASE WHEN status = 'sent' THEN 1 ELSE 0 END) AS sent_count
          FROM publishing_targets
          WHERE channel_type = 'whatsapp'
        `
      )
      .get() || {};

  return {
    pending: Number(row.pending_count || 0),
    retry: Number(row.retry_count || 0),
    sending: Number(row.sending_count || 0),
    failed: Number(row.failed_count || 0),
    sent: Number(row.sent_count || 0)
  };
}

function buildRuntimeConfig() {
  const delivery = getWhatsappDeliveryConfig();
  const control = getWhatsappControlConfig();
  const storage = getStorageConfig();
  const settings = readSettingsRow();
  const alerts = getWhatsappAlertSettings();
  const playwright = getWhatsappPlaywrightProviderInfo();
  const providerMode = control.endpoint?.trim() ? 'control_endpoint' : playwright.available ? 'playwright' : delivery.endpoint?.trim() ? 'delivery_gateway' : 'playwright';
  const providerConfigured =
    providerMode === 'control_endpoint'
      ? Boolean(cleanText(control.endpoint))
      : providerMode === 'playwright'
        ? playwright.available === true
        : Boolean(cleanText(delivery.endpoint));

  return {
    deliveryEnabled: delivery.enabled === true,
    deliveryEndpointConfigured: Boolean(cleanText(delivery.endpoint)),
    deliveryEndpoint: cleanText(delivery.endpoint),
    deliveryToken: cleanText(delivery.token),
    senderConfigured: Boolean(cleanText(delivery.sender)),
    sender: cleanText(delivery.sender),
    retryLimit: Number(delivery.retryLimit || 3),
    controlEndpointConfigured: Boolean(cleanText(control.endpoint)),
    controlEndpoint: cleanText(control.endpoint),
    controlToken: cleanText(control.token),
    instanceId: cleanText(control.instanceId) || 'primary',
    sessionDir: cleanText(control.sessionDir) || cleanText(storage.whatsappSessionDir),
    providerMode,
    providerConfigured,
    providerLabel:
      providerMode === 'control_endpoint'
        ? 'Control Endpoint'
        : providerMode === 'delivery_gateway'
          ? 'Delivery Gateway'
          : 'Playwright Worker',
    playwrightAvailable: playwright.available === true,
    browserChannel: cleanText(playwright.browserChannel),
    browserExecutablePath: cleanText(playwright.executablePath),
    browserProfileDir: cleanText(playwright.profileDir),
    loginTimeoutMs: Number(playwright.config?.loginTimeoutMs || 120000),
    keepBrowserOpen: playwright.config?.keepBrowserOpen === true,
    workerEnabled: settings?.whatsappWorkerEnabled === 1,
    sendCooldownMs: Math.min(60_000, Math.max(500, Number(settings?.whatsappSendCooldownMs || 4000))),
    alertsEnabled: alerts.enabled,
    alertTargetRef: alerts.targetRef,
    alertTargetType: alerts.targetType || WHATSAPP_ALERT_TARGET_TYPE
  };
}

function buildDefaultRuntimeMeta(config = buildRuntimeConfig()) {
  return {
    instanceId: config.instanceId,
    connectionStatus: 'not_connected',
    workerStatus: config.workerEnabled ? 'running' : 'stopped',
    sessionValid: false,
    qrRequired: false,
    qrCodeDataUrl: '',
    healthStatus: deriveHealthStatus(config, {
      connectionStatus: 'not_connected',
      workerStatus: config.workerEnabled ? 'running' : 'stopped',
      sessionValid: false
    }),
    browserStatus: 'unknown',
    browserChannel: config.browserChannel,
    browserExecutablePath: config.browserExecutablePath,
    browserProfileDir: config.browserProfileDir,
    keepBrowserOpen: config.keepBrowserOpen === true,
    channelReachable: false,
    lastHealthCheckAt: null,
    lastRestartAt: null,
    lastSuccessfulPostAt: null,
    lastError: '',
    lastErrorAt: null,
    errorCount: 0,
    currentJob: '',
    lastSendId: '',
    lastDispatchAt: null,
    lastRecoveryAt: null,
    lastAction: '',
    lastActionAt: null,
    lastConnectedAt: null,
    sessionSavedAt: null,
    profileWritable: false,
    workerPid: null,
    loginMonitorActive: false,
    loginTimeoutMs: Number(config.loginTimeoutMs || 120000),
    profileBackupDir: '',
    sessionResetAt: null,
    channelNavigationStatus: '',
    channelAdminStatus: '',
    lastChannelTargetRef: '',
    lastChannelDebugAt: null,
    lastChannelDebugMessage: '',
    lastChannelDebugArtifacts: [],
    lastChannelPreferredSelector: '',
    lastChannelComposerCandidates: [],
    lastChannelDomSnapshotPath: '',
    lastChannelHtmlSnapshotPath: '',
    lastChannelScreenshotPath: '',
    currentUrl: '',
    currentTitle: ''
  };
}

function persistRuntimeMeta(patch = {}, options = {}) {
  const config = buildRuntimeConfig();
  const row = readRuntimeRow();
  const currentMeta = row ? parseJson(row.meta_json, {}) : buildDefaultRuntimeMeta(config);
  const nextMeta = {
    ...buildDefaultRuntimeMeta(config),
    ...currentMeta,
    ...patch
  };

  const connectionStatus = normalizeConnectionStatus(options.connectionStatus || nextMeta.connectionStatus, nextMeta.connectionStatus);
  const workerStatus = normalizeWorkerStatus(options.workerStatus || nextMeta.workerStatus, nextMeta.workerStatus);
  const sessionValid =
    patch.sessionValid !== undefined
      ? parseBool(patch.sessionValid, false)
      : connectionStatus === 'connected'
        ? true
        : parseBool(nextMeta.sessionValid, false);
  const qrRequired =
    patch.qrRequired !== undefined
      ? parseBool(patch.qrRequired, false)
      : connectionStatus === 'qr_required'
        ? true
        : parseBool(nextMeta.qrRequired, false);

  nextMeta.connectionStatus = connectionStatus;
  nextMeta.workerStatus = workerStatus;
  nextMeta.sessionValid = sessionValid;
  nextMeta.qrRequired = qrRequired;
  nextMeta.healthStatus = cleanText(patch.healthStatus) || deriveHealthStatus(config, nextMeta);

  return upsertAppSession({
    sessionKey: WHATSAPP_RUNTIME_SESSION_KEY,
    module: 'whatsapp_output',
    sessionType: 'worker',
    status: connectionStatus,
    storagePath: config.sessionDir,
    externalRef: config.instanceId,
    meta: nextMeta,
    lastSeenAt: patch.lastSeenAt || nowIso()
  });
}

function ensureRuntimeSession() {
  const existing = readRuntimeRow();
  if (existing) {
    return existing;
  }

  persistRuntimeMeta({}, {});
  return readRuntimeRow();
}

function mapRuntimeState(row, config = buildRuntimeConfig()) {
  const runtimeRow = row || ensureRuntimeSession();
  const meta = {
    ...buildDefaultRuntimeMeta(config),
    ...(parseJson(runtimeRow?.meta_json, {}) || {})
  };
  const breakdown = getWhatsappQueueBreakdown();

  const latestSuccess =
    db
      .prepare(
        `
          SELECT created_at
          FROM publishing_logs
          WHERE worker_type = 'whatsapp'
            AND event_type IN ('target.sent', 'whatsapp.send.success')
          ORDER BY created_at DESC
          LIMIT 1
        `
      )
      .get() || null;
  const latestError =
    db
      .prepare(
        `
          SELECT created_at, message
          FROM publishing_logs
          WHERE worker_type = 'whatsapp'
            AND level IN ('warning', 'error')
          ORDER BY created_at DESC
          LIMIT 1
        `
      )
      .get() || null;

  return {
    instanceId: cleanText(meta.instanceId) || config.instanceId,
    enabled: config.deliveryEnabled,
    endpointConfigured: config.deliveryEndpointConfigured,
    providerConfigured: config.providerConfigured,
    providerMode: config.providerMode,
    providerLabel: config.providerLabel,
    senderConfigured: config.senderConfigured,
    sender: config.sender,
    retryLimit: config.retryLimit,
    controlEndpointConfigured: config.controlEndpointConfigured,
    workerEnabled: config.workerEnabled,
    workerStatus: normalizeWorkerStatus(meta.workerStatus, config.workerEnabled ? 'running' : 'stopped'),
    connectionStatus: normalizeConnectionStatus(meta.connectionStatus),
    sessionValid: parseBool(meta.sessionValid, false),
    qrRequired: parseBool(meta.qrRequired, false),
    qrCodeDataUrl: cleanText(meta.qrCodeDataUrl),
    healthStatus: cleanText(meta.healthStatus) || deriveHealthStatus(config, meta),
    browserStatus: cleanText(meta.browserStatus) || 'unknown',
    browserChannel: cleanText(meta.browserChannel) || config.browserChannel,
    channelReachable: parseBool(meta.channelReachable, false),
    lastHealthCheckAt: meta.lastHealthCheckAt || null,
    lastRestartAt: meta.lastRestartAt || null,
    lastSuccessfulPostAt: meta.lastSuccessfulPostAt || latestSuccess?.created_at || null,
    lastError: cleanText(meta.lastError) || cleanText(meta.lastChannelDebugMessage) || cleanText(latestError?.message),
    lastErrorAt: meta.lastErrorAt || meta.lastChannelDebugAt || latestError?.created_at || null,
    errorCount: Number(meta.errorCount || 0),
    currentJob: cleanText(meta.currentJob),
    lastSendId: cleanText(meta.lastSendId),
    lastDispatchAt: meta.lastDispatchAt || null,
    lastRecoveryAt: meta.lastRecoveryAt || null,
    lastAction: cleanText(meta.lastAction),
    lastActionAt: meta.lastActionAt || null,
    lastConnectedAt: meta.lastConnectedAt || null,
    sessionSavedAt: meta.sessionSavedAt || null,
    profileWritable: parseBool(meta.profileWritable, false),
    workerPid: Number(meta.workerPid || 0) || null,
    loginMonitorActive: parseBool(meta.loginMonitorActive, false),
    loginTimeoutMs: Number(meta.loginTimeoutMs || config.loginTimeoutMs || 120000),
    profileBackupDir: cleanText(meta.profileBackupDir),
    sessionResetAt: meta.sessionResetAt || null,
    channelNavigationStatus: cleanText(meta.channelNavigationStatus),
    channelAdminStatus: cleanText(meta.channelAdminStatus),
    lastChannelTargetRef: cleanText(meta.lastChannelTargetRef),
    lastChannelDebugAt: meta.lastChannelDebugAt || null,
    lastChannelDebugMessage: cleanText(meta.lastChannelDebugMessage),
    lastChannelDebugArtifacts: Array.isArray(meta.lastChannelDebugArtifacts) ? meta.lastChannelDebugArtifacts : [],
    lastChannelPreferredSelector: cleanText(meta.lastChannelPreferredSelector),
    lastChannelComposerCandidates: Array.isArray(meta.lastChannelComposerCandidates) ? meta.lastChannelComposerCandidates : [],
    lastChannelDomSnapshotPath: cleanText(meta.lastChannelDomSnapshotPath),
    lastChannelHtmlSnapshotPath: cleanText(meta.lastChannelHtmlSnapshotPath),
    lastChannelScreenshotPath: cleanText(meta.lastChannelScreenshotPath),
    currentUrl: cleanText(meta.currentUrl),
    currentTitle: cleanText(meta.currentTitle),
    browserExecutablePath: cleanText(meta.browserExecutablePath) || config.browserExecutablePath,
    browserProfileDir: cleanText(meta.browserProfileDir) || config.browserProfileDir,
    keepBrowserOpen: config.keepBrowserOpen === true,
    sessionDir: config.sessionDir,
    alertsEnabled: config.alertsEnabled,
    alertTargetRef: config.alertTargetRef,
    alertTargetType: config.alertTargetType,
    sendCooldownMs: config.sendCooldownMs,
    queue: {
      open: getOpenWhatsappQueueCount(),
      ...breakdown
    }
  };
}

function buildControlError(message, options = {}) {
  const error = new Error(message);
  if (options.code) {
    error.code = options.code;
  }
  return error;
}

function isWhatsappChannelNavigationCode(code = '') {
  return [
    'WHATSAPP_CHANNEL_NAVIGATION_NOT_IMPLEMENTED',
    'WHATSAPP_CHANNEL_COMPOSER_NOT_FOUND',
    'WHATSAPP_CHANNEL_NO_ADMIN_RIGHTS'
  ].includes(cleanText(code).toUpperCase());
}

function resolveWhatsappTargetDeliveryStatus(success = false, code = '', fallback = '') {
  if (success) {
    return cleanText(fallback) || 'tested';
  }

  const normalizedCode = cleanText(code).toUpperCase();
  if (normalizedCode === 'WHATSAPP_CHANNEL_NO_ADMIN_RIGHTS') {
    return 'no_admin_rights';
  }
  if (normalizedCode === 'WHATSAPP_CHANNEL_NAVIGATION_NOT_IMPLEMENTED') {
    return 'navigation_failed';
  }
  if (normalizedCode === 'WHATSAPP_CHANNEL_COMPOSER_NOT_FOUND') {
    return 'composer_missing';
  }

  return cleanText(fallback) || 'test_failed';
}

async function callLocalWhatsappControl(action = '', payload = {}) {
  const current = getWhatsappRuntimeState();
  const normalizedAction = cleanText(action).toLowerCase();

  if (normalizedAction === 'start_worker') {
    return await startWhatsappPlaywrightWorker(current);
  }

  if (normalizedAction === 'stop_worker') {
    return await stopWhatsappPlaywrightWorker({
      reason: cleanText(payload.reason) || 'manual_stop',
      forceClose: payload.forceClose !== false
    });
  }

  if (normalizedAction === 'connect') {
    return await connectWhatsappPlaywrightWorker(current, payload);
  }

  if (normalizedAction === 'refresh_session' || normalizedAction === 'restart_worker') {
    return await refreshWhatsappPlaywrightSession(current);
  }

  if (normalizedAction === 'test_connection') {
    return await testWhatsappPlaywrightConnection(payload, current);
  }

  if (normalizedAction === 'test_channel') {
    return await testWhatsappPlaywrightChannel(payload, current);
  }

  if (normalizedAction === 'channel_debug') {
    return await debugWhatsappPlaywrightChannel(payload, current);
  }

  if (normalizedAction === 'manual_channel_debug_capture') {
    return await captureWhatsappPlaywrightManualChannelDebug(payload, current);
  }

  if (normalizedAction === 'manual_channel_debug_wait') {
    return await waitForWhatsappPlaywrightManualChannelDebug(payload, current);
  }

  if (normalizedAction === 'health_check') {
    return await runWhatsappPlaywrightHealthCheck(current);
  }

  if (normalizedAction === 'recover') {
    return await recoverWhatsappPlaywrightWorker(current);
  }

  if (normalizedAction === 'reset_session') {
    return await resetWhatsappPlaywrightSession(current);
  }

  return current;
}

async function callWhatsappControl(action = '', payload = {}) {
  const config = buildRuntimeConfig();
  if (!config.controlEndpointConfigured) {
    return await callLocalWhatsappControl(action, payload);
  }

  const response = await fetch(config.controlEndpoint, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...(cleanText(config.controlToken) ? { Authorization: `Bearer ${config.controlToken}` } : {})
    },
    body: JSON.stringify({
      action: cleanText(action),
      instanceId: config.instanceId,
      payload
    })
  });
  const rawBody = await response.text();
  const body = parseJson(rawBody, null);

  if (!response.ok || (body && body.ok === false)) {
    throw buildControlError(
      cleanText(body?.error) || cleanText(body?.message) || `WhatsApp Control Fehler (${response.status}).`,
      {
        code: cleanText(body?.code) || 'WHATSAPP_CONTROL_ERROR'
      }
    );
  }

  return body?.item || body || {};
}

function applyControlState(action = '', result = {}) {
  const current = getWhatsappRuntimeState();
  const connectionStatus = normalizeConnectionStatus(
    result.connectionStatus || result.sessionStatus || result.status,
    current.connectionStatus
  );
  const workerStatus = normalizeWorkerStatus(result.workerStatus || result.runtimeStatus, current.workerStatus);
  const patch = {
    instanceId: cleanText(result.instanceId) || current.instanceId,
    connectionStatus,
    workerStatus,
    sessionValid:
      typeof result.sessionValid === 'boolean' ? result.sessionValid : connectionStatus === 'connected' ? true : current.sessionValid,
    qrRequired:
      typeof result.qrRequired === 'boolean' ? result.qrRequired : connectionStatus === 'qr_required' ? true : current.qrRequired,
    qrCodeDataUrl:
      result.qrCodeDataUrl !== undefined ? cleanText(result.qrCodeDataUrl) : cleanText(current.qrCodeDataUrl),
    browserStatus: cleanText(result.browserStatus) || current.browserStatus,
    browserChannel: cleanText(result.browserChannel) || cleanText(current.browserChannel),
    channelReachable:
      typeof result.channelReachable === 'boolean' ? result.channelReachable : current.channelReachable,
    lastHealthCheckAt: result.lastHealthCheckAt || current.lastHealthCheckAt || nowIso(),
    lastRestartAt: result.lastRestartAt || current.lastRestartAt || (action.includes('restart') ? nowIso() : null),
    lastSuccessfulPostAt: result.lastSuccessfulPostAt || current.lastSuccessfulPostAt,
    lastError: cleanText(result.lastError) || cleanText(result.error) || '',
    lastErrorAt: cleanText(result.lastError) || cleanText(result.error) ? nowIso() : current.lastErrorAt,
    errorCount:
      result.errorCount !== undefined ? parseInteger(result.errorCount, current.errorCount) : Number(current.errorCount || 0),
    currentJob: cleanText(result.currentJob) || '',
    lastConnectedAt: result.lastConnectedAt || current.lastConnectedAt || null,
    sessionSavedAt: result.sessionSavedAt || current.sessionSavedAt || null,
    profileWritable:
      typeof result.profileWritable === 'boolean' ? result.profileWritable : parseBool(current.profileWritable, false),
    workerPid: result.workerPid !== undefined ? parseInteger(result.workerPid, current.workerPid) : current.workerPid,
    loginMonitorActive:
      typeof result.loginMonitorActive === 'boolean'
        ? result.loginMonitorActive
        : parseBool(current.loginMonitorActive, false),
    loginTimeoutMs: result.loginTimeoutMs !== undefined ? parseInteger(result.loginTimeoutMs, current.loginTimeoutMs) : current.loginTimeoutMs,
    profileBackupDir: cleanText(result.profileBackupDir) || cleanText(current.profileBackupDir),
    sessionResetAt: result.sessionResetAt || current.sessionResetAt || null,
    channelNavigationStatus: cleanText(result.channelNavigationStatus) || cleanText(current.channelNavigationStatus),
    channelAdminStatus: cleanText(result.channelAdminStatus) || cleanText(current.channelAdminStatus),
    lastChannelTargetRef: cleanText(result.lastChannelTargetRef) || cleanText(current.lastChannelTargetRef),
    lastChannelDebugAt: result.lastChannelDebugAt || current.lastChannelDebugAt || null,
    lastChannelDebugMessage: cleanText(result.lastChannelDebugMessage) || cleanText(current.lastChannelDebugMessage),
    lastChannelDebugArtifacts: Array.isArray(result.debugArtifacts)
      ? result.debugArtifacts
      : Array.isArray(result.lastChannelDebugArtifacts)
        ? result.lastChannelDebugArtifacts
        : Array.isArray(current.lastChannelDebugArtifacts)
          ? current.lastChannelDebugArtifacts
          : [],
    lastChannelPreferredSelector: cleanText(result.preferredSelector) || cleanText(result.lastChannelPreferredSelector) || cleanText(current.lastChannelPreferredSelector),
    lastChannelComposerCandidates: Array.isArray(result.composerCandidates)
      ? result.composerCandidates
      : Array.isArray(result.lastChannelComposerCandidates)
        ? result.lastChannelComposerCandidates
        : Array.isArray(current.lastChannelComposerCandidates)
          ? current.lastChannelComposerCandidates
          : [],
    lastChannelDomSnapshotPath: cleanText(result.domSnapshotPath) || cleanText(result.lastChannelDomSnapshotPath) || cleanText(current.lastChannelDomSnapshotPath),
    lastChannelHtmlSnapshotPath: cleanText(result.htmlSnapshotPath) || cleanText(result.lastChannelHtmlSnapshotPath) || cleanText(current.lastChannelHtmlSnapshotPath),
    lastChannelScreenshotPath: cleanText(result.screenshotPath) || cleanText(result.lastChannelScreenshotPath) || cleanText(current.lastChannelScreenshotPath),
    currentUrl: cleanText(result.currentUrl) || cleanText(current.currentUrl),
    currentTitle: cleanText(result.currentTitle) || cleanText(current.currentTitle),
    lastAction: cleanText(action),
    lastActionAt: nowIso()
  };

  persistRuntimeMeta(patch, {
    connectionStatus,
    workerStatus
  });

  return getWhatsappRuntimeState();
}

function buildReadinessError(message, code, retryable = true) {
  const error = new Error(message);
  error.code = code;
  error.retryable = retryable;
  error.retryLimit = buildRuntimeConfig().retryLimit;
  return error;
}

export function getWhatsappRuntimeConfig() {
  return {
    ...buildRuntimeConfig()
  };
}

export function getWhatsappRuntimeState() {
  const config = buildRuntimeConfig();
  const row = ensureRuntimeSession();
  return mapRuntimeState(row, config);
}

export function saveWhatsappRuntimeSettings(input = {}) {
  const current = getWhatsappRuntimeConfig();
  db.prepare(
    `
      UPDATE app_settings
      SET whatsappWorkerEnabled = ?,
          whatsappSendCooldownMs = ?
      WHERE id = 1
    `
  ).run(
    input.workerEnabled === undefined ? (current.workerEnabled ? 1 : 0) : input.workerEnabled ? 1 : 0,
    input.sendCooldownMs === undefined
      ? current.sendCooldownMs
      : Math.min(60_000, Math.max(500, Number(input.sendCooldownMs || current.sendCooldownMs || 4000)))
  );

  if (Object.prototype.hasOwnProperty.call(input, 'alertsEnabled') || Object.prototype.hasOwnProperty.call(input, 'alertTargetRef')) {
    saveWhatsappAlertSettings({
      enabled: input.alertsEnabled,
      targetRef: input.alertTargetRef
    });
  }

  persistRuntimeMeta(
    {
      workerStatus: input.workerEnabled === false ? 'stopped' : input.workerEnabled === true ? 'running' : undefined,
      lastAction: 'settings_saved',
      lastActionAt: nowIso()
    },
    {
      workerStatus: input.workerEnabled === false ? 'stopped' : input.workerEnabled === true ? 'running' : undefined
    }
  );

  return getWhatsappRuntimeState();
}

export async function performWhatsappRuntimeAction(action = '', input = {}) {
  const normalizedAction = cleanText(action).toLowerCase();

  if (normalizedAction === 'start_worker') {
    saveWhatsappRuntimeSettings({ workerEnabled: true });
    try {
      const result = await callWhatsappControl('start_worker', input);
      return {
        runtime: applyControlState('start_worker', result),
        result
      };
    } catch (error) {
      persistRuntimeMeta(
        {
          workerStatus: 'error',
          lastError: error instanceof Error ? error.message : 'WhatsApp Worker konnte nicht gestartet werden.',
          lastErrorAt: nowIso(),
          lastAction: 'start_worker',
          lastActionAt: nowIso()
        },
        {
          workerStatus: 'error'
        }
      );
      throw error;
    }
  }

  if (normalizedAction === 'stop_worker') {
    saveWhatsappRuntimeSettings({ workerEnabled: false });
    try {
      const result = await callWhatsappControl('stop_worker', input);
      return {
        runtime: applyControlState('stop_worker', result),
        result
      };
    } catch (error) {
      persistRuntimeMeta({
        lastError: error instanceof Error ? error.message : 'WhatsApp Worker konnte nicht gestoppt werden.',
        lastErrorAt: nowIso(),
        lastAction: 'stop_worker',
        lastActionAt: nowIso()
      });
      throw error;
    }
  }

  if (normalizedAction === 'alert_test') {
    const result = await sendWhatsappAlertTest();
    if (result?.skipped) {
      throw buildControlError('Telegram Alert Kanal ist nicht konfiguriert oder Alerts sind deaktiviert.', {
        code: 'WHATSAPP_ALERT_NOT_READY'
      });
    }
    return {
      runtime: getWhatsappRuntimeState(),
      result
    };
  }

  if (normalizedAction === 'health_check') {
    return {
      runtime: await runWhatsappHealthCheck({ manual: true }),
      result: null
    };
  }

  try {
    const result = await callWhatsappControl(normalizedAction, input);
    if (normalizedAction === 'test_channel') {
      recordWhatsappTargetTest({
        targetId: input.targetId,
        success: result?.success !== false && result?.channelReachable !== false,
        errorMessage: cleanText(result?.error || result?.message),
        code: cleanText(result?.code),
        deliveryStatus: resolveWhatsappTargetDeliveryStatus(
          result?.success !== false && result?.channelReachable !== false,
          cleanText(result?.code),
          result?.channelReachable === true ? 'tested' : ''
        )
      });
      if (result?.success === false || result?.channelReachable === false) {
        await sendWhatsappChannelUnavailableAlert({
          problem: cleanText(result?.error || result?.message) || 'WhatsApp Kanal ist aktuell nicht erreichbar.'
        }).catch(() => null);
      }
    }
    if (normalizedAction === 'manual_channel_debug_capture' || normalizedAction === 'manual_channel_debug_wait') {
      persistRuntimeMeta({
        lastChannelDebugAt: nowIso(),
        lastChannelDebugMessage:
          cleanText(result?.preferredSelector)
            ? `Manueller Channel-Debug gespeichert: ${cleanText(result.preferredSelector)}`
            : cleanText(result?.lastChannelDebugMessage) || 'Manueller Channel-Debug gespeichert.',
        lastAction: normalizedAction,
        lastActionAt: nowIso()
      });
    }
    return {
      runtime: applyControlState(normalizedAction, result),
      result
    };
  } catch (error) {
    if (normalizedAction === 'test_channel') {
      recordWhatsappTargetTest({
        targetId: input.targetId,
        success: false,
        errorMessage: error instanceof Error ? error.message : 'WhatsApp Kanaltest fehlgeschlagen.',
        code: error instanceof Error ? error.code || '' : '',
        deliveryStatus: resolveWhatsappTargetDeliveryStatus(
          false,
          error instanceof Error ? error.code || '' : '',
          'test_failed'
        )
      });
      await sendWhatsappChannelUnavailableAlert({
        problem: error instanceof Error ? error.message : 'WhatsApp Kanal ist aktuell nicht erreichbar.'
      }).catch(() => null);
    }

    if (
      normalizedAction === 'channel_debug' ||
      normalizedAction === 'manual_channel_debug_capture' ||
      normalizedAction === 'manual_channel_debug_wait'
    ) {
      persistRuntimeMeta({
        lastChannelDebugAt: nowIso(),
        lastChannelDebugMessage: error instanceof Error ? error.message : 'WhatsApp Channel Debug fehlgeschlagen.',
        lastAction: normalizedAction,
        lastActionAt: nowIso()
      });
    }

    throw error;
  }
}

export async function runWhatsappHealthCheck({ manual = false } = {}) {
  const current = getWhatsappRuntimeState();
  const config = buildRuntimeConfig();

  if (!config.providerConfigured) {
    persistRuntimeMeta({
      lastHealthCheckAt: nowIso(),
      lastAction: manual ? 'health_check_manual' : 'health_check_auto',
      lastActionAt: nowIso(),
      healthStatus: deriveHealthStatus(config, current)
    });
    return getWhatsappRuntimeState();
  }

  if (manual !== true && config.workerEnabled !== true) {
    logWhatsappRuntimeEvent(
      'whatsapp.worker.stop.reason',
      '[WHATSAPP_WORKER_STOP_REASON] Auto-Healthcheck startet keinen Browser, weil whatsappWorkerEnabled=false ist.',
      {
        workerEnabled: config.workerEnabled,
        keepBrowserOpen: config.keepBrowserOpen === true,
        queueOpen: current.queue.open
      }
    );
    persistRuntimeMeta({
      lastHealthCheckAt: nowIso(),
      lastAction: 'health_check_auto',
      lastActionAt: nowIso(),
      healthStatus: deriveHealthStatus(config, current)
    });
    return getWhatsappRuntimeState();
  }

  try {
    const health = await callWhatsappControl('health_check', {
      openQueueCount: current.queue.open
    });
    const nextRuntime = applyControlState(manual ? 'health_check_manual' : 'health_check_auto', health);
    const degradedHealth =
      nextRuntime.connectionStatus === 'session_expired' ||
      nextRuntime.connectionStatus === 'error' ||
      nextRuntime.workerStatus === 'error';

    if (degradedHealth) {
      logWhatsappRuntimeEvent(
        'whatsapp.health.error',
        `[WHATSAPP_HEALTH_ERROR] ${nextRuntime.lastError || 'WhatsApp Health Check hat einen Fehlerzustand erkannt.'}`,
        {
          connectionStatus: nextRuntime.connectionStatus,
          workerStatus: nextRuntime.workerStatus,
          queueOpen: nextRuntime.queue.open
        },
        'warning'
      );

      if (config.keepBrowserOpen === true) {
        logWhatsappRuntimeEvent(
          'whatsapp.browser.close.skipped_debug',
          '[WHATSAPP_BROWSER_CLOSE_SKIPPED_DEBUG] Debug-Modus aktiv. Healthcheck laesst den Browser offen und ueberspringt Recovery.',
          {
            connectionStatus: nextRuntime.connectionStatus,
            workerStatus: nextRuntime.workerStatus,
            queueOpen: nextRuntime.queue.open
          },
          'warning'
        );
        logWhatsappRuntimeEvent(
          'whatsapp.worker.stop.reason',
          '[WHATSAPP_WORKER_STOP_REASON] Debug-Modus ueberspringt automatischen Recovery-Neustart nach Healthcheck-Fehler.',
          {
            connectionStatus: nextRuntime.connectionStatus,
            workerStatus: nextRuntime.workerStatus,
            queueOpen: nextRuntime.queue.open
          },
          'warning'
        );
        return nextRuntime;
      }

      await sendWhatsappAlert({
        headline: 'WhatsApp Output Stoerung',
        problem:
          nextRuntime.connectionStatus === 'session_expired'
            ? 'Session ist abgelaufen.'
            : nextRuntime.lastError || 'Health Check hat einen Fehlerzustand erkannt.',
        actionText: 'Neustart wird versucht.',
        openItems: nextRuntime.queue.open,
        statusLabel: 'Recovery laeuft',
        fingerprint: 'whatsapp-health-error-state'
      }).catch(() => null);

      const recovery = await callWhatsappControl('recover', {
        openQueueCount: nextRuntime.queue.open
      });
      const recoveredRuntime = applyControlState('recover', recovery);
      persistRuntimeMeta({
        lastRecoveryAt: nowIso(),
        lastRestartAt: nowIso()
      });
      logWhatsappRuntimeEvent(
        'whatsapp.worker.restart',
        '[WHATSAPP_WORKER_RESTART] WhatsApp Worker wurde nach Health Check neu gestartet.',
        {
          queueOpen: recoveredRuntime.queue.open,
          connectionStatus: recoveredRuntime.connectionStatus
        }
      );
      await sendWhatsappAlert({
        headline: 'WhatsApp Output wieder online',
        actionText: 'Neustart erfolgreich.',
        openItems: recoveredRuntime.queue.open,
        statusLabel: 'Recovery laeuft',
        fingerprint: 'whatsapp-recovered'
      }).catch(() => null);
      return getWhatsappRuntimeState();
    }

    if (nextRuntime.connectionStatus === 'qr_required') {
      logWhatsappRuntimeEvent(
        'whatsapp.health.pending_login',
        '[WHATSAPP_HEALTH_OK] WhatsApp Worker laeuft, wartet aber noch auf QR Login.',
        {
          connectionStatus: nextRuntime.connectionStatus,
          queueOpen: nextRuntime.queue.open
        }
      );
      await sendWhatsappLoginRequiredAlert({
        code: 'WHATSAPP_QR_REQUIRED',
        openItems: nextRuntime.queue.open
      }).catch(() => null);
    } else {
      logWhatsappRuntimeEvent(
        'whatsapp.health.ok',
        '[WHATSAPP_HEALTH_OK] WhatsApp Worker ist gesund.',
        {
          connectionStatus: nextRuntime.connectionStatus,
          workerStatus: nextRuntime.workerStatus,
          queueOpen: nextRuntime.queue.open
        }
      );
    }

    if (nextRuntime.connectionStatus === 'connected' && nextRuntime.channelReachable === false) {
      await sendWhatsappChannelUnavailableAlert({
        openItems: nextRuntime.queue.open
      }).catch(() => null);
    }

    if (
      ['healthy', 'connected'].includes(cleanText(health.healthStatus).toLowerCase()) &&
      ['error', 'session_expired', 'qr_required', 'recovering'].includes(cleanText(current.healthStatus).toLowerCase())
    ) {
      await sendWhatsappAlert({
        headline: 'WhatsApp Output wieder online',
        actionText: 'Neustart erfolgreich.',
        openItems: nextRuntime.queue.open,
        statusLabel: 'Recovery laeuft',
        fingerprint: 'whatsapp-online'
      }).catch(() => null);
    }

    return nextRuntime;
  } catch (error) {
    persistRuntimeMeta(
      {
        connectionStatus: cleanText(error.code) === 'WHATSAPP_CONTROL_MISSING' ? current.connectionStatus : 'error',
        workerStatus: config.workerEnabled ? 'error' : current.workerStatus,
        healthStatus: 'error',
        lastHealthCheckAt: nowIso(),
        lastError: error instanceof Error ? error.message : 'WhatsApp Health Check fehlgeschlagen.',
        lastErrorAt: nowIso(),
        errorCount: Number(current.errorCount || 0) + 1,
        lastAction: manual ? 'health_check_manual' : 'health_check_auto',
        lastActionAt: nowIso()
      },
      {
        connectionStatus: cleanText(error.code) === 'WHATSAPP_CONTROL_MISSING' ? current.connectionStatus : 'error',
        workerStatus: config.workerEnabled ? 'error' : current.workerStatus
      }
    );

    if (config.keepBrowserOpen === true) {
      logWhatsappRuntimeEvent(
        'whatsapp.browser.close.skipped_debug',
        '[WHATSAPP_BROWSER_CLOSE_SKIPPED_DEBUG] Debug-Modus aktiv. Healthcheck schliesst den Browser bei Fehlern nicht automatisch.',
        {
          error: error instanceof Error ? error.message : 'WhatsApp Health Check fehlgeschlagen.',
          queueOpen: current.queue.open
        },
        'warning'
      );
      logWhatsappRuntimeEvent(
        'whatsapp.worker.stop.reason',
        '[WHATSAPP_WORKER_STOP_REASON] Debug-Modus ueberspringt automatischen Recovery-Neustart nach Control/Launch-Fehler.',
        {
          error: error instanceof Error ? error.message : 'WhatsApp Health Check fehlgeschlagen.',
          queueOpen: current.queue.open
        },
        'warning'
      );
      return getWhatsappRuntimeState();
    }

    await sendWhatsappAlert({
      headline: 'WhatsApp Output Stoerung',
      problem: error instanceof Error ? error.message : 'Health Check fehlgeschlagen.',
      actionText: 'Neustart wird versucht.',
      openItems: current.queue.open,
      statusLabel: 'Recovery laeuft',
      fingerprint: 'whatsapp-health-error'
    }).catch(() => null);

    try {
      const recovery = await callWhatsappControl('recover', {
        openQueueCount: current.queue.open
      });
      const recoveredRuntime = applyControlState('recover', recovery);
      persistRuntimeMeta({
        lastRecoveryAt: nowIso(),
        lastRestartAt: nowIso()
      });
      logWhatsappRuntimeEvent(
        'whatsapp.worker.restart',
        '[WHATSAPP_WORKER_RESTART] WhatsApp Worker wurde nach Fehler neu gestartet.',
        {
          queueOpen: recoveredRuntime.queue.open,
          problem: error instanceof Error ? error.message : 'Health Check Fehler'
        }
      );
      await sendWhatsappAlert({
        headline: 'WhatsApp Output wieder online',
        actionText: 'Neustart erfolgreich.',
        openItems: recoveredRuntime.queue.open,
        statusLabel: 'Recovery laeuft',
        fingerprint: 'whatsapp-recovered'
      }).catch(() => null);
      return getWhatsappRuntimeState();
    } catch (recoveryError) {
      await sendWhatsappAlert({
        headline: 'WhatsApp Output ausgefallen',
        problem: recoveryError instanceof Error ? recoveryError.message : 'Session konnte nicht repariert werden.',
        actionText: 'Manuelle Pruefung noetig.',
        affectedItems: current.queue.open,
        fingerprint: 'whatsapp-final-failure'
      }).catch(() => null);
      logWhatsappRuntimeEvent(
        'whatsapp.health.error',
        `[WHATSAPP_HEALTH_ERROR] ${recoveryError instanceof Error ? recoveryError.message : 'Session konnte nicht repariert werden.'}`,
        {
          queueOpen: current.queue.open
        },
        'error'
      );
      return getWhatsappRuntimeState();
    }
  }
}

export function startWhatsappHealthMonitor() {
  if (healthMonitorStarted) {
    return;
  }

  healthMonitorStarted = true;
  void runWhatsappHealthCheck({ manual: false }).catch(() => null);
  const timer = setInterval(() => {
    void runWhatsappHealthCheck({ manual: false }).catch(() => null);
  }, WHATSAPP_HEALTH_INTERVAL_MS);
  timer.unref?.();
}

export function assertWhatsappRuntimeReady() {
  const config = buildRuntimeConfig();
  const runtime = getWhatsappRuntimeState();

  if (!config.deliveryEnabled) {
    throw buildReadinessError('WhatsApp Output ist deaktiviert.', 'WHATSAPP_OUTPUT_DISABLED', false);
  }

  if (!config.providerConfigured) {
    throw buildReadinessError(
      config.providerMode === 'playwright'
        ? 'Kein lokaler Browser fuer den WhatsApp Playwright Worker gefunden.'
        : 'WHATSAPP_DELIVERY_ENDPOINT fehlt im Backend.',
      'WHATSAPP_ENDPOINT_MISSING',
      false
    );
  }

  if (config.workerEnabled !== true) {
    throw buildReadinessError('WhatsApp Worker ist gestoppt.', 'WHATSAPP_WORKER_STOPPED', true);
  }

  if (runtime.connectionStatus === 'session_expired') {
    throw buildReadinessError('WhatsApp Session ist abgelaufen.', 'WHATSAPP_SESSION_EXPIRED', true);
  }

  if (runtime.connectionStatus === 'qr_required') {
    throw buildReadinessError('WhatsApp QR Login ist erforderlich.', 'WHATSAPP_QR_REQUIRED', true);
  }

  if (runtime.connectionStatus !== 'connected' || runtime.sessionValid !== true) {
    throw buildReadinessError('WhatsApp ist nicht verbunden.', 'WHATSAPP_NOT_CONNECTED', true);
  }

  if (runtime.workerStatus === 'error') {
    throw buildReadinessError('WhatsApp Worker meldet einen Fehler.', 'WHATSAPP_WORKER_ERROR', true);
  }

  return {
    config,
    runtime
  };
}

export async function waitForWhatsappSendCooldown() {
  const runtime = getWhatsappRuntimeState();
  const cooldownMs = Number(runtime.sendCooldownMs || 4000);
  const lastDispatchAt = cleanText(runtime.lastDispatchAt);
  if (!lastDispatchAt) {
    return;
  }

  const elapsedMs = Date.now() - new Date(lastDispatchAt).getTime();
  if (!Number.isFinite(elapsedMs) || elapsedMs >= cooldownMs) {
    return;
  }

  await new Promise((resolve) => setTimeout(resolve, cooldownMs - elapsedMs));
}

export function allocateWhatsappSendId(target = {}) {
  const existing = cleanText(target.send_id);
  if (existing) {
    return existing;
  }

  const nextSendId = `wa-${Number(target.queue_id || 0)}-${Number(target.id || 0)}-${crypto.randomUUID()}`;
  db.prepare(`UPDATE publishing_targets SET send_id = ?, updated_at = ? WHERE id = ?`).run(nextSendId, nowIso(), target.id);
  return nextSendId;
}

export function markWhatsappSendStart(input = {}) {
  persistRuntimeMeta({
    currentJob: `queue:${Number(input.queueId || 0)} target:${Number(input.targetId || 0)}`,
    lastSendId: cleanText(input.sendId),
    lastDispatchAt: nowIso(),
    lastAction: 'send_start',
    lastActionAt: nowIso()
  });
}

export function recordWhatsappSendSuccess(input = {}) {
  const targetId = Number(input.targetId || 0);
  if (targetId > 0) {
    db.prepare(
      `
        UPDATE publishing_targets
        SET delivery_ref = COALESCE(?, delivery_ref),
            updated_at = ?
        WHERE id = ?
      `
    ).run(cleanText(input.deliveryRef) || null, nowIso(), targetId);
  }

  const outputTargetId = Number(input.outputTargetId || 0);
  if (outputTargetId > 0) {
    db.prepare(
      `
        UPDATE whatsapp_output_targets
        SET last_sent_at = ?,
            last_error = '',
            last_error_at = NULL,
            last_delivery_status = 'sent',
            updated_at = ?
        WHERE id = ?
      `
    ).run(nowIso(), nowIso(), outputTargetId);
  }

  persistRuntimeMeta({
    connectionStatus: 'connected',
    sessionValid: true,
    qrRequired: false,
    currentJob: '',
    lastSuccessfulPostAt: nowIso(),
    lastError: '',
    lastErrorAt: null,
    lastAction: 'send_success',
    lastActionAt: nowIso(),
    healthStatus: 'healthy'
  });
}

export function recordWhatsappSendError(input = {}) {
  const currentRuntime = getWhatsappRuntimeState();
  const outputTargetId = Number(input.outputTargetId || 0);
  const normalizedCode = cleanText(input.code).toUpperCase();
  const isChannelNavigationError = isWhatsappChannelNavigationCode(normalizedCode);
  const deliveryStatus = cleanText(input.deliveryStatus);
  if (outputTargetId > 0) {
    db.prepare(
      `
        UPDATE whatsapp_output_targets
        SET last_error = ?,
            last_error_at = ?,
            last_delivery_status = ?,
            updated_at = ?
        WHERE id = ?
      `
    ).run(
      cleanText(input.errorMessage),
      nowIso(),
      deliveryStatus ||
        (isChannelNavigationError
          ? resolveWhatsappTargetDeliveryStatus(false, normalizedCode, 'navigation_failed')
          : input.finalFailure === true
            ? 'failed'
            : 'retry'),
      nowIso(),
      outputTargetId
    );
  }

  const nextConnectionStatus =
    normalizedCode === 'WHATSAPP_SESSION_EXPIRED'
      ? 'session_expired'
      : normalizedCode === 'WHATSAPP_QR_REQUIRED'
        ? 'qr_required'
        : isChannelNavigationError
          ? cleanText(currentRuntime.connectionStatus) || 'connected'
          : input.retryable === false
          ? 'error'
          : cleanText(currentRuntime.connectionStatus) || 'not_connected';

  persistRuntimeMeta(
    {
      connectionStatus: nextConnectionStatus || undefined,
      currentJob: '',
      lastError: cleanText(input.errorMessage),
      lastErrorAt: nowIso(),
      errorCount:
        input.incrementErrorCount === false ? Number(currentRuntime.errorCount || 0) : Number(currentRuntime.errorCount || 0) + 1,
      lastAction: input.finalFailure === true ? 'send_failed_final' : 'send_retry',
      lastActionAt: nowIso(),
      healthStatus:
        normalizedCode === 'WHATSAPP_SESSION_EXPIRED'
          ? 'session_expired'
          : normalizedCode === 'WHATSAPP_QR_REQUIRED'
            ? 'qr_required'
            : isChannelNavigationError
              ? cleanText(currentRuntime.healthStatus) || 'healthy'
            : input.finalFailure === true
              ? 'error'
              : cleanText(currentRuntime.healthStatus) || 'healthy',
      channelNavigationStatus: isChannelNavigationError
        ? normalizedCode
        : cleanText(currentRuntime.channelNavigationStatus),
      channelAdminStatus:
        normalizedCode === 'WHATSAPP_CHANNEL_NO_ADMIN_RIGHTS'
          ? normalizedCode
          : cleanText(currentRuntime.channelAdminStatus)
    },
    {
      connectionStatus: nextConnectionStatus || undefined
    }
  );
}

export function recordWhatsappTargetTest(input = {}, successArg, errorMessageArg = '') {
  const normalizedInput =
    typeof input === 'object' && input !== null
      ? input
      : {
          targetId: input,
          success: successArg,
          errorMessage: errorMessageArg
        };
  const numericId = Number(normalizedInput.targetId || 0);
  if (!numericId) {
    return;
  }

  const success = normalizedInput.success === true;
  const errorMessage = cleanText(normalizedInput.errorMessage);
  const deliveryStatus = resolveWhatsappTargetDeliveryStatus(success, cleanText(normalizedInput.code), normalizedInput.deliveryStatus);

  db.prepare(
    `
      UPDATE whatsapp_output_targets
      SET last_tested_at = ?,
          last_error = CASE WHEN ? = 1 THEN '' ELSE ? END,
          last_error_at = CASE WHEN ? = 1 THEN last_error_at ELSE ? END,
          last_delivery_status = ?,
          updated_at = ?
      WHERE id = ?
    `
  ).run(
    nowIso(),
    success ? 1 : 0,
    errorMessage,
    success ? 1 : 0,
    nowIso(),
    deliveryStatus,
    nowIso(),
    numericId
  );
}

export async function sendWhatsappRetryAlert(input = {}) {
  return await sendWhatsappAlert({
    headline: 'WhatsApp Output Stoerung',
    problem: cleanText(input.problem) || 'WhatsApp Versand fehlgeschlagen.',
    actionText: 'Retry wird eingeplant.',
    openItems: Number(input.openItems || 0),
    statusLabel: 'Retry geplant',
    fingerprint: `whatsapp-retry-${cleanText(input.code) || 'unknown'}`
  });
}

export async function sendWhatsappFinalFailureAlert(input = {}) {
  return await sendWhatsappAlert({
    headline: 'WhatsApp Output ausgefallen',
    problem: cleanText(input.problem) || 'WhatsApp Versand konnte nicht abgeschlossen werden.',
    actionText: 'Manuelle Pruefung noetig.',
    affectedItems: Number(input.affectedItems || 0),
    fingerprint: `whatsapp-final-${cleanText(input.code) || 'unknown'}`
  });
}

export async function sendWhatsappRecoveryAlert(input = {}) {
  return await sendWhatsappAlert({
    headline: 'WhatsApp Output wieder online',
    actionText: 'Offene Beitraege werden weiter verarbeitet.',
    openItems: Number(input.openItems || 0),
    statusLabel: 'Recovery laeuft',
    fingerprint: 'whatsapp-recovery-online'
  });
}

export async function sendWhatsappLoginRequiredAlert(input = {}) {
  const code = cleanText(input.code);
  const sessionExpired = code === 'WHATSAPP_SESSION_EXPIRED';
  return await sendWhatsappAlert({
    headline: 'WhatsApp Output Stoerung',
    problem: cleanText(input.problem) || (sessionExpired ? 'Session ist abgelaufen.' : 'QR Login ist erforderlich.'),
    actionText: sessionExpired ? 'Session wird erneuert.' : 'Bitte QR Login durchfuehren.',
    openItems: Number(input.openItems || 0),
    statusLabel: 'Login erforderlich',
    fingerprint: `whatsapp-login-${code || 'required'}`
  });
}

export async function sendWhatsappChannelUnavailableAlert(input = {}) {
  return await sendWhatsappAlert({
    headline: 'WhatsApp Output Stoerung',
    problem: cleanText(input.problem) || 'WhatsApp Kanal ist aktuell nicht erreichbar.',
    actionText: 'Kanal wird erneut geprueft.',
    openItems: Number(input.openItems || 0),
    statusLabel: 'Kanal pruefen',
    fingerprint: 'whatsapp-channel-unreachable'
  });
}

export async function sendWhatsappDuplicatePreventedAlert(input = {}) {
  return await sendWhatsappAlert({
    headline: 'WhatsApp Output Stoerung',
    problem: cleanText(input.problem) || 'Duplicate-Schutz hat einen Doppelpost verhindert.',
    actionText: 'Kein Eingriff noetig.',
    openItems: Number(input.openItems || 0),
    statusLabel: 'Duplikat blockiert',
    fingerprint: 'whatsapp-duplicate-prevented'
  });
}
