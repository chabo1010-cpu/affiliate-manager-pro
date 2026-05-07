import { getDb } from '../db.js';

const db = getDb();
export const DEFAULT_WHATSAPP_TEST_CHANNEL_URL = 'https://whatsapp.com/channel/0029VbCsyVY7NoZryjRrBU2P';
export const DEFAULT_WHATSAPP_LIVE_CHANNEL_URL = 'https://whatsapp.com/channel/0029Va8EEIFHLHQgQlvNdx1y';
export const DEFAULT_WHATSAPP_TEST_CHANNEL_NAME = 'WhatsApp Test Output';
export const DEFAULT_WHATSAPP_LIVE_CHANNEL_NAME = 'Code & Couponing WhatsApp';
export const DEFAULT_WHATSAPP_TARGET_TYPE = 'WHATSAPP_CHANNEL';
export const DEFAULT_WHATSAPP_TEST_TARGET_TYPE = 'WHATSAPP_TEST_CHANNEL';

function nowIso() {
  return new Date().toISOString();
}

function cleanText(value = '') {
  return typeof value === 'string' ? value.trim() : '';
}

function parseEnabledFlag(value, fallback = false) {
  if (value === undefined || value === null || value === '') {
    return fallback;
  }

  return value === true || value === 1 || value === '1';
}

function isWhatsappTestTargetType(value = '') {
  return cleanText(value).toUpperCase() === DEFAULT_WHATSAPP_TEST_TARGET_TYPE;
}

function getDefaultSystemTargets() {
  return [
    {
      name: DEFAULT_WHATSAPP_TEST_CHANNEL_NAME,
      targetRef: DEFAULT_WHATSAPP_TEST_CHANNEL_URL,
      targetLabel: DEFAULT_WHATSAPP_TEST_CHANNEL_NAME,
      targetType: DEFAULT_WHATSAPP_TEST_TARGET_TYPE,
      channelUrl: DEFAULT_WHATSAPP_TEST_CHANNEL_URL,
      isActive: true,
      useForPublishing: true,
      isSystem: true,
      requiresManualActivation: false,
      sortOrder: 5
    },
    {
      name: DEFAULT_WHATSAPP_LIVE_CHANNEL_NAME,
      targetRef: DEFAULT_WHATSAPP_LIVE_CHANNEL_URL,
      targetLabel: DEFAULT_WHATSAPP_LIVE_CHANNEL_NAME,
      targetType: DEFAULT_WHATSAPP_TARGET_TYPE,
      channelUrl: DEFAULT_WHATSAPP_LIVE_CHANNEL_URL,
      isActive: false,
      useForPublishing: true,
      isSystem: true,
      requiresManualActivation: true,
      sortOrder: 10
    }
  ];
}

function mapTargetRow(row) {
  return {
    id: Number(row.id),
    name: cleanText(row.name) || `WhatsApp Ziel ${row.id}`,
    targetRef: cleanText(row.target_ref),
    targetLabel: cleanText(row.target_label) || cleanText(row.name) || `WhatsApp Ziel ${row.id}`,
    targetType: cleanText(row.target_type).toUpperCase() || DEFAULT_WHATSAPP_TARGET_TYPE,
    channelUrl: cleanText(row.channel_url) || cleanText(row.target_ref),
    isActive: row.is_active === 1,
    useForPublishing: row.use_for_publishing === 1,
    isSystem: row.is_system === 1,
    requiresManualActivation: row.requires_manual_activation === 1,
    sortOrder: Number(row.sort_order || 100),
    lastSentAt: row.last_sent_at || null,
    lastError: cleanText(row.last_error),
    lastErrorAt: row.last_error_at || null,
    lastDeliveryStatus: cleanText(row.last_delivery_status) || 'idle',
    lastTestedAt: row.last_tested_at || null,
    createdAt: row.created_at,
    updatedAt: row.updated_at
  };
}

function normalizeTargetInput(target = {}, index = 0) {
  const targetRef = cleanText(target.targetRef || target.target_ref);
  if (!targetRef) {
    return null;
  }

  const numericId = Number.parseInt(String(target.id ?? ''), 10);
  const targetLabel = cleanText(target.targetLabel || target.target_label);

  return {
    id: Number.isFinite(numericId) ? numericId : null,
    name: cleanText(target.name) || `WhatsApp Ziel ${index + 1}`,
    targetRef,
    targetLabel: targetLabel || cleanText(target.name) || `WhatsApp Ziel ${index + 1}`,
    targetType: cleanText(target.targetType || target.target_type).toUpperCase() || DEFAULT_WHATSAPP_TARGET_TYPE,
    channelUrl: cleanText(target.channelUrl || target.channel_url) || targetRef,
    isActive: parseEnabledFlag(target.isActive ?? target.is_active, true),
    useForPublishing: parseEnabledFlag(target.useForPublishing ?? target.use_for_publishing, true),
    isSystem: parseEnabledFlag(target.isSystem ?? target.is_system, false),
    requiresManualActivation: parseEnabledFlag(
      target.requiresManualActivation ?? target.requires_manual_activation,
      false
    ),
    sortOrder: Number.isFinite(Number(target.sortOrder ?? target.sort_order))
      ? Number(target.sortOrder ?? target.sort_order)
      : 100,
    lastSentAt: target.lastSentAt || target.last_sent_at || null,
    lastError: cleanText(target.lastError || target.last_error),
    lastErrorAt: target.lastErrorAt || target.last_error_at || null,
    lastDeliveryStatus: cleanText(target.lastDeliveryStatus || target.last_delivery_status) || 'idle',
    lastTestedAt: target.lastTestedAt || target.last_tested_at || null
  };
}

function dedupeTargets(targets = []) {
  const byTargetRef = new Map();

  for (const target of targets) {
    if (!target?.targetRef || byTargetRef.has(target.targetRef)) {
      continue;
    }

    byTargetRef.set(target.targetRef, target);
  }

  return Array.from(byTargetRef.values());
}

function listPersistentTargets() {
  ensureDefaultWhatsappTargets();

  return db
    .prepare(
      `
        SELECT *
        FROM whatsapp_output_targets
        ORDER BY sort_order ASC, use_for_publishing DESC, is_active DESC, id ASC
      `
    )
    .all()
    .map(mapTargetRow);
}

function ensureDefaultWhatsappTargets() {
  const timestamp = nowIso();
  const defaults = getDefaultSystemTargets();

  defaults.forEach((target) => {
    const existing = db.prepare(`SELECT id, is_active FROM whatsapp_output_targets WHERE target_ref = ? LIMIT 1`).get(target.targetRef) || null;
    if (existing) {
      db.prepare(
        `
          UPDATE whatsapp_output_targets
          SET name = ?,
              target_label = ?,
              target_type = ?,
              channel_url = ?,
              use_for_publishing = 1,
              is_system = 1,
              requires_manual_activation = ?,
              sort_order = ?,
              updated_at = ?
          WHERE id = ?
        `
      ).run(
        target.name,
        target.targetLabel,
        target.targetType,
        target.channelUrl,
        target.requiresManualActivation ? 1 : 0,
        target.sortOrder,
        timestamp,
        existing.id
      );
      return;
    }

    db.prepare(
      `
        INSERT INTO whatsapp_output_targets (
          name,
          target_ref,
          target_label,
          target_type,
          channel_url,
          is_active,
          use_for_publishing,
          is_system,
          requires_manual_activation,
          sort_order,
          last_sent_at,
          last_error,
          last_error_at,
          last_delivery_status,
          last_tested_at,
          created_at,
          updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, 1, 1, ?, ?, NULL, '', NULL, 'idle', NULL, ?, ?)
      `
    ).run(
      target.name,
      target.targetRef,
      target.targetLabel,
      target.targetType,
      target.channelUrl,
      target.isActive ? 1 : 0,
      target.requiresManualActivation ? 1 : 0,
      target.sortOrder,
      timestamp,
      timestamp
    );
  });
}

function normalizeTargetRefList(values = []) {
  return Array.from(
    new Set(
      values
        .map((value) => cleanText(value))
        .filter(Boolean)
    )
  );
}

function normalizeIdList(values = []) {
  return Array.from(
    new Set(
      values
        .map((value) => Number.parseInt(String(value ?? ''), 10))
        .filter((value) => Number.isFinite(value))
    )
  );
}

function resolveTargetsFromPayload(queuePayload = {}, config = getWhatsappOutputTargetConfig()) {
  const requestedTargetIds = normalizeIdList(
    Array.isArray(queuePayload.whatsappTargetIds) ? queuePayload.whatsappTargetIds : []
  );
  const requestedTargetRefs = normalizeTargetRefList(
    Array.isArray(queuePayload.whatsappTargetRefs) ? queuePayload.whatsappTargetRefs : []
  );

  if (requestedTargetIds.length) {
    const selectedTargets = config.targets.filter((target) => requestedTargetIds.includes(target.id));
    if (!selectedTargets.length) {
      return [];
    }

    return selectedTargets;
  }

  if (requestedTargetRefs.length) {
    return requestedTargetRefs.map((targetRef) => {
      const persistedTarget = config.targets.find((target) => target.targetRef === targetRef);

      return (
        persistedTarget || {
          id: null,
          name: targetRef,
          targetRef,
          targetLabel: targetRef,
          targetType: DEFAULT_WHATSAPP_TARGET_TYPE,
          channelUrl: targetRef,
          isActive: true,
          useForPublishing: true,
          isSystem: false,
          requiresManualActivation: false,
          sortOrder: 100,
          lastSentAt: null,
          lastError: '',
          lastErrorAt: null,
          lastDeliveryStatus: 'idle',
          lastTestedAt: null
        }
      );
    });
  }

  return config.effectiveTargets;
}

export function getWhatsappOutputTargetConfig() {
  const targets = listPersistentTargets();
  const publishTargets = targets.filter((target) => target.isActive && target.useForPublishing);

  return {
    targets,
    effectiveTargets: publishTargets,
    defaultTargetRef: DEFAULT_WHATSAPP_TEST_CHANNEL_URL
  };
}

export function saveWhatsappOutputTargetConfig(input = {}) {
  const currentConfig = getWhatsappOutputTargetConfig();
  const hasTargetsInput = Object.prototype.hasOwnProperty.call(input, 'targets');
  const normalizedTargets = hasTargetsInput
    ? dedupeTargets(
        (Array.isArray(input.targets) ? input.targets : [])
          .map((target, index) => normalizeTargetInput(target, index))
          .filter(Boolean)
      )
    : currentConfig.targets;
  const timestamp = nowIso();

  const persist = db.transaction(() => {
    if (!hasTargetsInput) {
      return;
    }

    const existingIds = new Set(
      db
        .prepare(`SELECT id FROM whatsapp_output_targets`)
        .all()
        .map((row) => Number(row.id))
    );
    const keepIds = [];

    for (const target of normalizedTargets) {
      if (target.id && existingIds.has(target.id)) {
        db.prepare(
          `
            UPDATE whatsapp_output_targets
            SET name = ?,
                target_ref = ?,
                target_label = ?,
                target_type = ?,
                channel_url = ?,
                is_active = ?,
                use_for_publishing = ?,
                is_system = ?,
                requires_manual_activation = ?,
                sort_order = ?,
                last_sent_at = ?,
                last_error = ?,
                last_error_at = ?,
                last_delivery_status = ?,
                last_tested_at = ?,
                updated_at = ?
            WHERE id = ?
          `
        ).run(
          target.name,
          target.targetRef,
          target.targetLabel,
          target.targetType,
          target.channelUrl || target.targetRef,
          target.isActive ? 1 : 0,
          target.useForPublishing ? 1 : 0,
          target.isSystem ? 1 : 0,
          target.requiresManualActivation ? 1 : 0,
          target.sortOrder,
          target.lastSentAt || null,
          target.lastError || '',
          target.lastErrorAt || null,
          target.lastDeliveryStatus || 'idle',
          target.lastTestedAt || null,
          timestamp,
          target.id
        );
        keepIds.push(target.id);
        continue;
      }

      const insertResult = db
        .prepare(
          `
            INSERT INTO whatsapp_output_targets (
              name,
              target_ref,
              target_label,
              target_type,
              channel_url,
              is_active,
              use_for_publishing,
              is_system,
              requires_manual_activation,
              sort_order,
              last_sent_at,
              last_error,
              last_error_at,
              last_delivery_status,
              last_tested_at,
              created_at,
              updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          `
        )
        .run(
          target.name,
          target.targetRef,
          target.targetLabel,
          target.targetType,
          target.channelUrl || target.targetRef,
          target.isActive ? 1 : 0,
          target.useForPublishing ? 1 : 0,
          target.isSystem ? 1 : 0,
          target.requiresManualActivation ? 1 : 0,
          target.sortOrder,
          target.lastSentAt || null,
          target.lastError || '',
          target.lastErrorAt || null,
          target.lastDeliveryStatus || 'idle',
          target.lastTestedAt || null,
          timestamp,
          timestamp
        );

      keepIds.push(Number(insertResult.lastInsertRowid));
    }

    if (!keepIds.length) {
      db.prepare(`DELETE FROM whatsapp_output_targets`).run();
      return;
    }

    const placeholders = keepIds.map(() => '?').join(', ');
    db.prepare(`DELETE FROM whatsapp_output_targets WHERE id NOT IN (${placeholders})`).run(...keepIds);
  });

  persist();

  return getWhatsappOutputTargetConfig();
}

export function expandWhatsappPublishingTargets(baseTarget = {}, queuePayload = {}) {
  const baseTargetMeta =
    baseTarget && typeof baseTarget.targetMeta === 'object' && baseTarget.targetMeta ? baseTarget.targetMeta : {};
  const hasExplicitTargetSelection =
    Boolean(cleanText(baseTarget.targetRef || baseTarget.target_ref)) ||
    Number.isFinite(Number.parseInt(String(baseTargetMeta.targetId ?? ''), 10));

  if (hasExplicitTargetSelection) {
    return [baseTarget];
  }

  const resolvedTargets = resolveTargetsFromPayload(queuePayload);
  if (!resolvedTargets.length) {
    return [baseTarget];
  }

  return resolvedTargets.map((resolvedTarget) => ({
    ...baseTarget,
    targetRef: resolvedTarget.targetRef,
    targetLabel: resolvedTarget.targetLabel || resolvedTarget.name,
    targetMeta: {
      ...((baseTarget && typeof baseTarget.targetMeta === 'object' && baseTarget.targetMeta) || {}),
      targetId: resolvedTarget.id,
      name: resolvedTarget.name,
      targetRef: resolvedTarget.targetRef,
      targetType: resolvedTarget.targetType || DEFAULT_WHATSAPP_TARGET_TYPE,
      channelUrl: resolvedTarget.channelUrl || resolvedTarget.targetRef,
      requiresManualActivation: resolvedTarget.requiresManualActivation === true,
      isSystem: resolvedTarget.isSystem === true
    }
  }));
}
