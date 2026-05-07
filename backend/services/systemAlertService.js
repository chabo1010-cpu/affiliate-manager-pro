import { getDb } from '../db.js';
import { upsertAppSession } from './databaseService.js';
import { sendTelegramPost } from './telegramSenderService.js';

const db = getDb();
const ALERT_SESSION_KEY = 'whatsapp_output:alert_state:default';
const ALERT_COOLDOWN_MS = 2 * 60 * 1000;
export const DEFAULT_WHATSAPP_ALERT_TARGET = '@WhatsappStatusFehler';
export const DEFAULT_WHATSAPP_ALERTS_ENABLED = true;
export const WHATSAPP_ALERT_TARGET_TYPE = 'SYSTEM_ALERT_CHANNEL';

function nowIso() {
  return new Date().toISOString();
}

function cleanText(value = '') {
  return typeof value === 'string' ? value.trim() : '';
}

function normalizeWhatsappAlertTargetRef(value = '') {
  const trimmed = cleanText(value);
  if (!trimmed) {
    return '';
  }

  let normalized = trimmed.replace(/^https?:\/\/(?:www\.)?t\.me\//i, '').replace(/^t\.me\//i, '');
  normalized = normalized.split(/[/?#]/, 1)[0]?.trim() || '';
  if (!normalized) {
    return '';
  }

  if (/^-?\d+$/.test(normalized) || normalized.startsWith('-')) {
    return normalized;
  }

  return `@${normalized.replace(/^@+/, '')}`;
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

function readSettingsRow() {
  return db.prepare(`SELECT * FROM app_settings WHERE id = 1`).get() || null;
}

function readAlertState() {
  const row = db.prepare(`SELECT * FROM app_sessions WHERE session_key = ? LIMIT 1`).get(ALERT_SESSION_KEY) || null;
  return row
    ? {
        status: cleanText(row.status) || 'idle',
        meta: parseJson(row.meta_json, {})
      }
    : {
        status: 'idle',
        meta: {}
      };
}

function saveAlertState(meta = {}) {
  const current = readAlertState();
  return upsertAppSession({
    sessionKey: ALERT_SESSION_KEY,
    module: 'whatsapp_output',
    sessionType: 'alert_state',
    status: cleanText(meta.status) || current.status || 'idle',
    meta: {
      ...(current.meta || {}),
      ...meta
    },
    lastSeenAt: nowIso()
  });
}

function formatAlertTime(value = new Date()) {
  try {
    return new Intl.DateTimeFormat('de-DE', {
      hour: '2-digit',
      minute: '2-digit',
      timeZone: 'Europe/Berlin'
    }).format(value);
  } catch {
    return value instanceof Date ? value.toISOString().slice(11, 16) : cleanText(value);
  }
}

function resolveWhatsappAlertVariant(headline = '') {
  const normalized = cleanText(headline).toLowerCase();
  if (normalized.includes('verbunden')) {
    return {
      headline: cleanText(headline) || 'WhatsApp Test Output verbunden',
      headlineEmoji: '✅',
      counterLabel: 'Offene Beitraege',
      actionEmoji: '🔁'
    };
  }

  if (normalized.includes('wieder online')) {
    return {
      headline: 'WhatsApp Output wieder online',
      headlineEmoji: '✅',
      counterLabel: 'Offene Beitraege',
      actionEmoji: '🔁'
    };
  }

  if (normalized.includes('ausgefallen')) {
    return {
      headline: 'WhatsApp Output ausgefallen',
      headlineEmoji: '❌',
      counterLabel: 'Betroffene Beitraege',
      actionEmoji: '🔧'
    };
  }

  return {
    headline: 'WhatsApp Output Stoerung',
    headlineEmoji: '🚨',
    counterLabel: 'Offene Beitraege',
    actionEmoji: '🔁'
  };
}

function resolveWhatsappStatusEmoji(statusLabel = '') {
  const normalized = cleanText(statusLabel).toLowerCase();
  if (!normalized) {
    return '';
  }
  if (normalized.includes('erfolgreich') || normalized.includes('online') || normalized.includes('aktiv') || normalized.includes('verbunden')) {
    return '🟢';
  }
  if (normalized.includes('retry') || normalized.includes('recovery') || normalized.includes('login')) {
    return '🟡';
  }
  if (normalized.includes('fehler') || normalized.includes('ausgefallen')) {
    return '🔴';
  }
  return 'ℹ️';
}

function buildWhatsappAlertText({
  headline = 'WhatsApp Output Stoerung',
  area = 'WhatsApp Worker',
  problem = '',
  actionText = '',
  openItems = 0,
  statusLabel = '',
  affectedItems = 0
} = {}) {
  const timestamp = formatAlertTime(new Date());
  const variant = resolveWhatsappAlertVariant(headline);
  const lines = [`${variant.headlineEmoji} ${variant.headline}`, '', `📍 Bereich: ${cleanText(area) || 'WhatsApp Worker'}`];

  if (problem) {
    lines.push(`⚠️ Problem: ${problem}`);
  }
  if (actionText) {
    lines.push(`${variant.actionEmoji} Aktion: ${actionText}`);
  }

  const hasAffectedItems = Number.isFinite(Number(affectedItems)) && Number(affectedItems) > 0;
  const queueCount = hasAffectedItems ? Number(affectedItems) : Number.isFinite(Number(openItems)) ? Number(openItems) : 0;
  if (queueCount > 0) {
    lines.push(`📦 ${hasAffectedItems ? 'Betroffene Beitraege' : variant.counterLabel}: ${queueCount}`);
  }

  lines.push(`🕒 Zeit: ${timestamp}`);

  if (statusLabel) {
    const statusEmoji = resolveWhatsappStatusEmoji(statusLabel);
    lines.push('', 'Status:', `${statusEmoji ? `${statusEmoji} ` : ''}${statusLabel}`);
  }

  return lines.join('\n');
}

function shouldDispatchAlert(fingerprint = '', force = false) {
  if (force) {
    return true;
  }

  const current = readAlertState();
  const lastFingerprint = cleanText(current.meta?.lastAlertFingerprint);
  const lastAlertAt = cleanText(current.meta?.lastAlertAt);
  if (!fingerprint || !lastFingerprint || fingerprint !== lastFingerprint || !lastAlertAt) {
    return true;
  }

  const elapsedMs = Date.now() - new Date(lastAlertAt).getTime();
  return !Number.isFinite(elapsedMs) || elapsedMs >= ALERT_COOLDOWN_MS;
}

export function getWhatsappAlertSettings() {
  const settings = readSettingsRow();
  const targetRef = normalizeWhatsappAlertTargetRef(settings?.whatsappAlertTelegramTarget) || DEFAULT_WHATSAPP_ALERT_TARGET;
  const enabled =
    settings?.whatsappAlertTelegramEnabled === 1 ||
    (!normalizeWhatsappAlertTargetRef(settings?.whatsappAlertTelegramTarget) && DEFAULT_WHATSAPP_ALERTS_ENABLED);

  return {
    enabled,
    targetRef,
    targetType: WHATSAPP_ALERT_TARGET_TYPE,
    cooldownMs: ALERT_COOLDOWN_MS
  };
}

export function saveWhatsappAlertSettings(input = {}) {
  const current = getWhatsappAlertSettings();
  const nextTargetRef =
    input.targetRef === undefined ? current.targetRef || DEFAULT_WHATSAPP_ALERT_TARGET : normalizeWhatsappAlertTargetRef(input.targetRef);

  db.prepare(
    `
      UPDATE app_settings
      SET whatsappAlertTelegramEnabled = ?,
          whatsappAlertTelegramTarget = ?
      WHERE id = 1
    `
  ).run(
    input.enabled === undefined ? (current.enabled ? 1 : 0) : input.enabled ? 1 : 0,
    nextTargetRef || DEFAULT_WHATSAPP_ALERT_TARGET
  );

  return getWhatsappAlertSettings();
}

export async function sendWhatsappAlert(input = {}) {
  const settings = getWhatsappAlertSettings();
  const fingerprint = cleanText(input.fingerprint);

  if (!settings.enabled || !settings.targetRef) {
    return {
      sent: false,
      skipped: true,
      reason: 'alerts_disabled'
    };
  }

  if (!shouldDispatchAlert(fingerprint, input.force === true)) {
    return {
      sent: false,
      skipped: true,
      reason: 'cooldown_active'
    };
  }

  const text = buildWhatsappAlertText(input);
  const result = await sendTelegramPost({
    chatId: settings.targetRef,
    text,
    disableWebPagePreview: true,
    titlePreview: 'WhatsApp Alert',
    hasAffiliateLink: false,
    postContext: 'system_alert_whatsapp'
  });

  saveAlertState({
    status: 'sent',
    lastAlertAt: nowIso(),
    lastAlertFingerprint: fingerprint || cleanText(input.headline),
    lastAlertTextPreview: text.slice(0, 280),
    lastTargetRef: settings.targetRef
  });

  return {
    sent: true,
    targetRef: settings.targetRef,
    result
  };
}

export async function sendWhatsappAlertTest() {
  return await sendWhatsappAlert({
    headline: 'WhatsApp Test Output verbunden',
    statusLabel: 'Test-Kanal aktiv',
    force: true,
    fingerprint: 'whatsapp-alert-test'
  });
}
