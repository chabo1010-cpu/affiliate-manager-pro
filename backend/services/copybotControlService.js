import { getDb } from '../db.js';

const db = getDb();

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

function parseEnabledFlag(value, fallback = false) {
  if (value === undefined || value === null || value === '') {
    return fallback;
  }

  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    if (['1', 'true', 'yes', 'on'].includes(normalized)) {
      return true;
    }
    if (['0', 'false', 'no', 'off'].includes(normalized)) {
      return false;
    }
  }

  return value === true || value === 1;
}

function getCopybotSettingsRow() {
  return db.prepare(`SELECT copybotEnabled FROM app_settings WHERE id = 1`).get() || null;
}

export function isCopybotControlledSourceType(sourceType = '') {
  const normalized = String(sourceType || '').trim().toLowerCase();
  return normalized === 'copybot' || normalized === 'telegram_reader';
}

export function getCopybotRuntimeState() {
  const envOverrideRaw = process.env.COPYBOT_ENABLED;
  const hasEnvOverride = envOverrideRaw !== undefined && String(envOverrideRaw).trim() !== '';
  const envEnabled = hasEnvOverride ? parseEnabledFlag(envOverrideRaw, true) : true;
  const row = getCopybotSettingsRow();
  const settingEnabled = parseEnabledFlag(row?.copybotEnabled, false);
  const enabled = envEnabled && settingEnabled;

  return {
    enabled,
    envEnabled,
    settingEnabled,
    hasEnvOverride,
    reason: !envEnabled ? 'env_disabled' : !settingEnabled ? 'setting_disabled' : 'enabled'
  };
}

export function isCopybotRuntimeEnabled() {
  return getCopybotRuntimeState().enabled === true;
}

export function logCopybotStatusChange({ enabled = false, actor = '', source = '', runtime = null } = {}) {
  const safeRuntime = runtime && typeof runtime === 'object' ? runtime : getCopybotRuntimeState();
  const normalizedEnabled = enabled === true;
  const normalizedActor = cleanText(actor) || 'system';
  const normalizedSource = cleanText(source) || 'settings_toggle';

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
      ) VALUES (?, ?, NULL, NULL, ?, ?, ?)
    `
  ).run(
    'info',
    normalizedEnabled ? 'copybot.enabled' : 'copybot.disabled',
    normalizedEnabled ? 'Copybot wurde aktiviert.' : 'Copybot wurde deaktiviert.',
    JSON.stringify({
      actor: normalizedActor,
      source: normalizedSource,
      enabled: normalizedEnabled,
      runtime: safeRuntime
    }),
    nowIso()
  );
}

export function getCopybotStatusAudit() {
  const row =
    db
      .prepare(
        `
          SELECT *
          FROM copybot_logs
          WHERE event_type IN ('copybot.enabled', 'copybot.disabled')
          ORDER BY created_at DESC, id DESC
          LIMIT 1
        `
      )
      .get() || null;

  const payload = parseJson(row?.payload_json, {}) || {};

  return {
    lastChangedAt: row?.created_at || null,
    changedBy: cleanText(payload.actor) || 'unbekannt',
    source: cleanText(payload.source) || '',
    eventType: cleanText(row?.event_type),
    message: cleanText(row?.message),
    enabled: payload.enabled === true,
    runtime: payload.runtime && typeof payload.runtime === 'object' ? payload.runtime : null
  };
}
