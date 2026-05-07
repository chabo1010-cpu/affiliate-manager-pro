import { getDb } from '../db.js';
import { getTelegramConfig, getTelegramTestGroupConfig } from '../env.js';
import { cleanText } from './dealHistoryService.js';
import { OUTPUT_DISABLED_SKIP } from './outputChannelService.js';
import { sendTelegramDealPost, sendTelegramPost } from './telegramSenderService.js';

const db = getDb();
const DEFAULT_RETRY_LIMIT = 3;
const TARGET_KIND_VALUES = new Set(['test', 'live', 'review', 'standard', 'custom']);
export const DEFAULT_LIVE_CHANNEL_CHAT_ID = '@codeundcouponing';

function nowIso() {
  return new Date().toISOString();
}

function parseEnabledFlag(value, fallback = false) {
  if (value === undefined || value === null || value === '') {
    return fallback;
  }

  return value === true || value === 1 || value === '1';
}

function parseRetryLimit(value, fallback = DEFAULT_RETRY_LIMIT) {
  const parsed = Number.parseInt(String(value ?? ''), 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback;
}

function normalizeTargetKind(value, fallback = 'test') {
  const normalized = cleanText(value).toLowerCase();
  return TARGET_KIND_VALUES.has(normalized) ? normalized : fallback;
}

function normalizeChatIdKey(value = '') {
  return cleanText(value).toLowerCase();
}

function normalizeIdList(values = []) {
  return Array.from(
    new Set(
      values
        .map((value) => Number.parseInt(String(value ?? ''), 10))
        .filter((value) => Number.isFinite(value) && value > 0)
    )
  );
}

function normalizeChatIdList(values = []) {
  return Array.from(new Set(values.map((value) => cleanText(value)).filter(Boolean)));
}

function buildTelegramBotError(message, options = {}) {
  const error = new Error(message);
  error.retryable = options.retryable !== false;
  if (options.code) {
    error.code = options.code;
  }
  if (options.details) {
    error.details = options.details;
  }
  if (options.deliveredTargets) {
    error.deliveredTargets = options.deliveredTargets;
  }
  if (options.skippedTargets) {
    error.skippedTargets = options.skippedTargets;
  }
  return error;
}

function ensureAppSettingsRow() {
  const row = db.prepare(`SELECT id FROM app_settings WHERE id = 1`).get();
  if (row) {
    return;
  }

  db.prepare(
    `
      INSERT INTO app_settings (
        id,
        repostCooldownEnabled,
        repostCooldownHours,
        telegramCopyButtonText,
        copybotEnabled,
        outputQueueEnabled,
        telegramBotEnabled,
        telegramBotDefaultRetryLimit,
        facebookEnabled,
        facebookSessionMode,
        facebookDefaultRetryLimit,
        facebookDefaultTarget,
        telegramReaderGroupSlotCount,
        schedulerBootstrapVersion
      ) VALUES (1, 1, 12, 'Zum Kopieren hier klicken', 0, 1, 1, 3, 0, 'persistent', 3, NULL, 10, 0)
    `
  ).run();
}

function readTelegramBotSettingsRow() {
  ensureAppSettingsRow();
  return (
    db
      .prepare(
        `
          SELECT
            id,
            telegramBotEnabled,
            telegramBotDefaultRetryLimit
          FROM app_settings
          WHERE id = 1
        `
      )
      .get() || null
  );
}

function mapTargetRow(row = {}) {
  const targetKind = normalizeTargetKind(row.target_kind ?? row.channel_kind, 'test');
  const testGroupChatId = cleanText(getTelegramTestGroupConfig().chatId);
  return {
    id: Number(row.id),
    name: cleanText(row.name) || `Telegram Ziel ${row.id}`,
    chatId: cleanText(row.chat_id),
    isActive: row.is_active === 1,
    useForPublishing: row.use_for_publishing === 1,
    channelKind: targetKind,
    targetKind,
    isSystem:
      cleanText(row.chat_id) === DEFAULT_LIVE_CHANNEL_CHAT_ID ||
      (Boolean(testGroupChatId) && cleanText(row.chat_id) === testGroupChatId),
    requiresManualActivation: targetKind === 'live',
    lastSentAt: row.last_sent_at || null,
    lastError: cleanText(row.last_error),
    lastErrorAt: row.last_error_at || null,
    lastDeliveryStatus: cleanText(row.last_delivery_status) || 'idle',
    lastTestedAt: row.last_tested_at || null,
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null
  };
}

function normalizeTargetInput(target = {}, index = 0) {
  const chatId = cleanText(target.chatId || target.chat_id);
  if (!chatId) {
    return null;
  }

  const normalizedKind = normalizeTargetKind(
    target.targetKind ?? target.channelKind ?? target.target_kind ?? target.channel_kind,
    'test'
  );
  const numericId = Number.parseInt(String(target.id ?? ''), 10);

  return {
    id: Number.isFinite(numericId) ? numericId : null,
    name: cleanText(target.name) || `Telegram Ziel ${index + 1}`,
    chatId,
    isActive: parseEnabledFlag(target.isActive ?? target.is_active, true),
    useForPublishing: parseEnabledFlag(target.useForPublishing ?? target.use_for_publishing, true),
    channelKind: normalizedKind,
    targetKind: normalizedKind,
    isSystem: parseEnabledFlag(target.isSystem ?? target.is_system, false),
    requiresManualActivation: parseEnabledFlag(
      target.requiresManualActivation ?? target.requires_manual_activation,
      normalizedKind === 'live'
    ),
    lastSentAt: target.lastSentAt || target.last_sent_at || null,
    lastError: cleanText(target.lastError || target.last_error),
    lastErrorAt: target.lastErrorAt || target.last_error_at || null,
    lastDeliveryStatus: cleanText(target.lastDeliveryStatus || target.last_delivery_status) || 'idle',
    lastTestedAt: target.lastTestedAt || target.last_tested_at || null
  };
}

function dedupeTargets(targets = []) {
  const seen = new Set();
  const deduped = [];

  for (const target of targets) {
    if (!target) {
      continue;
    }

    const key = target.id ? `id:${target.id}` : `chat:${normalizeChatIdKey(target.chatId)}`;
    if (!key || seen.has(key)) {
      continue;
    }

    seen.add(key);
    deduped.push(target);
  }

  return deduped;
}

function ensureDefaultTelegramTargets() {
  const timestamp = nowIso();
  const testGroupChatId = cleanText(getTelegramTestGroupConfig().chatId);
  const upsertTarget = db.prepare(
    `
      INSERT INTO telegram_bot_targets (
        name,
        chat_id,
        is_active,
        use_for_publishing,
        channel_kind,
        last_sent_at,
        last_error,
        last_error_at,
        last_delivery_status,
        last_tested_at,
        created_at,
        updated_at
      ) VALUES (?, ?, ?, ?, ?, NULL, '', NULL, 'idle', NULL, ?, ?)
      ON CONFLICT(chat_id) DO UPDATE SET
        name = excluded.name,
        channel_kind = excluded.channel_kind
    `
  );

  const persist = db.transaction(() => {
    if (testGroupChatId) {
      upsertTarget.run('Telegram Testgruppe', testGroupChatId, 1, 1, 'test', timestamp, timestamp);
    }

    upsertTarget.run('LIVE KANAL @codeundcouponing', DEFAULT_LIVE_CHANNEL_CHAT_ID, 0, 1, 'live', timestamp, timestamp);
  });

  persist();
}

function listPersistentTargets() {
  ensureDefaultTelegramTargets();

  return db
    .prepare(
      `
        SELECT *
        FROM telegram_bot_targets
        ORDER BY
          CASE channel_kind
            WHEN 'test' THEN 0
            WHEN 'live' THEN 1
            ELSE 2
          END ASC,
          use_for_publishing DESC,
          is_active DESC,
          id ASC
      `
    )
    .all()
    .map(mapTargetRow);
}

function buildEnvFallbackTarget(defaultRetryLimit = DEFAULT_RETRY_LIMIT) {
  const telegramConfig = getTelegramConfig();
  const chatId = cleanText(telegramConfig.chatId);

  if (!chatId) {
    return null;
  }

  return {
    id: null,
    name: 'ENV Default Chat',
    chatId,
    isActive: true,
    useForPublishing: true,
    channelKind: 'test',
    targetKind: 'test',
    isSystem: false,
    requiresManualActivation: false,
    lastSentAt: null,
    lastError: '',
    lastErrorAt: null,
    lastDeliveryStatus: 'idle',
    lastTestedAt: null,
    isFallback: true,
    retryLimit: defaultRetryLimit
  };
}

function findTargetByChatId(targets = [], chatId = '') {
  const normalized = normalizeChatIdKey(chatId);
  return targets.find((target) => normalizeChatIdKey(target.chatId) === normalized) || null;
}

function isTargetPublishEnabled(target = {}) {
  return target.isActive === true && target.useForPublishing === true;
}

function buildDisabledSkipMessage(target = {}) {
  if (target.targetKind === 'live' || target.channelKind === 'live') {
    return `LIVE KANAL ${target.chatId || target.name} ist deaktiviert. Erst manuell aktivieren.`;
  }

  return `Telegram-Ziel ${target.chatId || target.name} ist deaktiviert. Versand wurde uebersprungen.`;
}

function buildSkippedTarget(target = {}) {
  return {
    targetId: target.id ?? null,
    targetName: target.name || '',
    targetChatId: target.chatId || '',
    channelKind: target.channelKind || target.targetKind || 'test',
    targetKind: target.targetKind || target.channelKind || 'test',
    reasonCode: OUTPUT_DISABLED_SKIP,
    reason: buildDisabledSkipMessage(target)
  };
}

function splitResolvedTargets(targets = []) {
  const enabledTargets = [];
  const skippedTargets = [];

  for (const target of dedupeTargets(targets)) {
    if (isTargetPublishEnabled(target)) {
      enabledTargets.push(target);
      continue;
    }

    skippedTargets.push(buildSkippedTarget(target));
  }

  return {
    targets: enabledTargets,
    skippedTargets
  };
}

function computeEffectiveTargets(targets = [], defaultRetryLimit = DEFAULT_RETRY_LIMIT) {
  if (targets.length) {
    return targets.filter(isTargetPublishEnabled);
  }

  const envFallbackTarget = buildEnvFallbackTarget(defaultRetryLimit);
  return envFallbackTarget ? [envFallbackTarget] : [];
}

function createAdHocTarget(chatId = '') {
  return {
    id: null,
    name: `Telegram ${chatId}`,
    chatId,
    isActive: true,
    useForPublishing: true,
    channelKind: 'standard',
    targetKind: 'custom',
    isSystem: false,
    requiresManualActivation: false,
    lastSentAt: null,
    lastError: '',
    lastErrorAt: null,
    lastDeliveryStatus: 'idle',
    lastTestedAt: null,
    isFallback: false
  };
}

export function getTelegramBotClientConfig() {
  const settingsRow = readTelegramBotSettingsRow();
  const defaultRetryLimit = parseRetryLimit(settingsRow?.telegramBotDefaultRetryLimit, DEFAULT_RETRY_LIMIT);
  const telegramConfig = getTelegramConfig();
  const targets = listPersistentTargets();

  return {
    enabled: parseEnabledFlag(settingsRow?.telegramBotEnabled, true),
    defaultRetryLimit,
    tokenConfigured: Boolean(cleanText(telegramConfig.token)),
    fallbackChatConfigured: Boolean(cleanText(telegramConfig.chatId)),
    targets,
    effectiveTargets: computeEffectiveTargets(targets, defaultRetryLimit)
  };
}

export function getTelegramBotRetryLimit() {
  return getTelegramBotClientConfig().defaultRetryLimit;
}

export function resolveTargetsFromPayload(queuePayload = {}, config = getTelegramBotClientConfig()) {
  const requestedTargetIds = normalizeIdList(
    Array.isArray(queuePayload.telegramTargetIds) ? queuePayload.telegramTargetIds : []
  );
  const requestedChatIds = normalizeChatIdList(
    Array.isArray(queuePayload.telegramChatIds) ? queuePayload.telegramChatIds : []
  );

  if (requestedTargetIds.length) {
    const matchedTargets = config.targets.filter((target) => requestedTargetIds.includes(target.id));
    if (!matchedTargets.length) {
      throw buildTelegramBotError('Keine Telegram-Zielgruppe fuer die uebergebenen Target-IDs gefunden.', {
        retryable: false
      });
    }

    return {
      targetMode: 'target_ids',
      ...splitResolvedTargets(matchedTargets)
    };
  }

  if (requestedChatIds.length) {
    const selectedTargets = requestedChatIds.map((chatId) => findTargetByChatId(config.targets, chatId) || createAdHocTarget(chatId));

    return {
      targetMode: 'chat_ids',
      ...splitResolvedTargets(selectedTargets)
    };
  }

  if (config.targets.length) {
    return {
      targetMode: 'persistent_targets',
      ...splitResolvedTargets(config.targets)
    };
  }

  const envFallbackTarget = buildEnvFallbackTarget(config.defaultRetryLimit);
  return {
    targetMode: 'env_fallback',
    targets: envFallbackTarget ? [envFallbackTarget] : [],
    skippedTargets: []
  };
}

function resolveExpansionTargets(queuePayload = {}, config = getTelegramBotClientConfig()) {
  const requestedTargetIds = normalizeIdList(
    Array.isArray(queuePayload.telegramTargetIds) ? queuePayload.telegramTargetIds : []
  );
  const requestedChatIds = normalizeChatIdList(
    Array.isArray(queuePayload.telegramChatIds) ? queuePayload.telegramChatIds : []
  );

  if (requestedTargetIds.length) {
    const matchedTargets = config.targets.filter((target) => requestedTargetIds.includes(target.id));
    if (!matchedTargets.length) {
      throw buildTelegramBotError('Keine Telegram-Zielgruppe fuer die uebergebenen Target-IDs gefunden.', {
        retryable: false
      });
    }

    return dedupeTargets(matchedTargets);
  }

  if (requestedChatIds.length) {
    return dedupeTargets(requestedChatIds.map((chatId) => findTargetByChatId(config.targets, chatId) || createAdHocTarget(chatId)));
  }

  if (config.targets.length) {
    return config.targets;
  }

  const envFallbackTarget = buildEnvFallbackTarget(config.defaultRetryLimit);
  return envFallbackTarget ? [envFallbackTarget] : [];
}

function recordTelegramTargetState({ targetId = null, status = 'idle', errorMessage = '', sentAt = null, testedAt = null } = {}) {
  const numericTargetId = Number.parseInt(String(targetId ?? ''), 10);
  if (!Number.isFinite(numericTargetId) || numericTargetId <= 0) {
    return;
  }

  const timestamp = nowIso();
  const normalizedStatus = cleanText(status) || 'idle';
  const normalizedError = cleanText(errorMessage);
  const nextSentAt = sentAt || null;
  const nextTestedAt = testedAt || null;
  const nextErrorAt = normalizedError ? timestamp : null;

  db.prepare(
    `
      UPDATE telegram_bot_targets
      SET last_delivery_status = ?,
          last_sent_at = CASE WHEN ? IS NULL THEN last_sent_at ELSE ? END,
          last_tested_at = CASE WHEN ? IS NULL THEN last_tested_at ELSE ? END,
          last_error = ?,
          last_error_at = ?,
          updated_at = ?
      WHERE id = ?
    `
  ).run(normalizedStatus, nextSentAt, nextSentAt, nextTestedAt, nextTestedAt, normalizedError, nextErrorAt, timestamp, numericTargetId);
}

export function getTelegramBotTargetState({ targetId = null, chatId = '', isFallback = false, allowAdHoc = false } = {}) {
  const config = getTelegramBotClientConfig();

  if (config.enabled !== true) {
    return {
      shouldSkip: true,
      reasonCode: 'publishing_disabled',
      target: null
    };
  }

  if (isFallback === true) {
    return {
      shouldSkip: false,
      reasonCode: '',
      target: buildEnvFallbackTarget(config.defaultRetryLimit)
    };
  }

  const numericTargetId = Number.parseInt(String(targetId ?? ''), 10);
  const hasExplicitTargetRef = Number.isFinite(numericTargetId) || Boolean(cleanText(chatId));
  const resolvedTarget =
    (Number.isFinite(numericTargetId) ? config.targets.find((item) => item.id === numericTargetId) : null) ||
    findTargetByChatId(config.targets, chatId) ||
    null;

  if (!resolvedTarget) {
    if (!hasExplicitTargetRef) {
      return {
        shouldSkip: false,
        reasonCode: '',
        target: null
      };
    }

    if (allowAdHoc === true && cleanText(chatId)) {
      return {
        shouldSkip: false,
        reasonCode: '',
        target: createAdHocTarget(chatId)
      };
    }

    return {
      shouldSkip: true,
      reasonCode: 'target_missing',
      target: null
    };
  }

  if (!resolvedTarget.useForPublishing) {
    return {
      shouldSkip: true,
      reasonCode: 'publishing_disabled',
      target: resolvedTarget
    };
  }

  if (!resolvedTarget.isActive) {
    return {
      shouldSkip: true,
      reasonCode: 'target_disabled',
      target: resolvedTarget
    };
  }

  return {
    shouldSkip: false,
    reasonCode: '',
    target: resolvedTarget
  };
}

export function recordTelegramBotTargetDelivery({
  targetId = null,
  chatId = '',
  success = false,
  errorMessage = '',
  deliveryStatus = '',
  sentAt = null
} = {}) {
  const numericTargetId = Number.parseInt(String(targetId ?? ''), 10);
  const resolvedTargetId = Number.isFinite(numericTargetId)
    ? numericTargetId
    : findTargetByChatId(getTelegramBotClientConfig().targets, chatId)?.id ?? null;
  const normalizedStatus = cleanText(deliveryStatus);

  if (!resolvedTargetId) {
    return;
  }

  if (success === true) {
    const effectiveSentAt = sentAt || nowIso();
    recordTelegramTargetState({
      targetId: resolvedTargetId,
      status: normalizedStatus || 'sent',
      sentAt: effectiveSentAt,
      testedAt: normalizedStatus === 'test_sent' ? effectiveSentAt : null
    });
    return;
  }

  recordTelegramTargetState({
    targetId: resolvedTargetId,
    status: normalizedStatus || 'error',
    errorMessage
  });
}

export function saveTelegramBotClientConfig(input = {}) {
  const currentConfig = getTelegramBotClientConfig();
  const nextEnabled =
    input.enabled === undefined ? currentConfig.enabled : parseEnabledFlag(input.enabled, currentConfig.enabled);
  const nextRetryLimit = parseRetryLimit(input.defaultRetryLimit, currentConfig.defaultRetryLimit);
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
    db.prepare(
      `
        UPDATE app_settings
        SET telegramBotEnabled = ?,
            telegramBotDefaultRetryLimit = ?
        WHERE id = 1
      `
    ).run(nextEnabled ? 1 : 0, nextRetryLimit);

    if (!hasTargetsInput) {
      return;
    }

    const existingIds = new Set(
      db
        .prepare(`SELECT id FROM telegram_bot_targets`)
        .all()
        .map((row) => Number(row.id))
    );
    const keepIds = [];

    for (const target of normalizedTargets) {
      const targetKind = normalizeTargetKind(target.targetKind ?? target.channelKind, 'test');
      const requiresManualActivation = target.requiresManualActivation === true || targetKind === 'live';

      if (target.id && existingIds.has(target.id)) {
        db.prepare(
          `
            UPDATE telegram_bot_targets
            SET name = ?,
                chat_id = ?,
                is_active = ?,
                use_for_publishing = ?,
                channel_kind = ?,
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
          target.chatId,
          target.isActive ? 1 : 0,
          target.useForPublishing ? 1 : 0,
          targetKind,
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
            INSERT INTO telegram_bot_targets (
              name,
              chat_id,
              is_active,
              use_for_publishing,
              channel_kind,
              last_sent_at,
              last_error,
              last_error_at,
              last_delivery_status,
              last_tested_at,
              created_at,
              updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          `
        )
        .run(
          target.name,
          target.chatId,
          target.isActive ? 1 : 0,
          target.useForPublishing ? 1 : 0,
          targetKind,
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
      db.prepare(`DELETE FROM telegram_bot_targets`).run();
      return;
    }

    const placeholders = keepIds.map(() => '?').join(', ');
    db.prepare(`DELETE FROM telegram_bot_targets WHERE id NOT IN (${placeholders})`).run(...keepIds);
  });

  persist();
  return getTelegramBotClientConfig();
}

export function expandTelegramPublishingTargets(baseTarget = {}, queuePayload = {}) {
  const baseTargetMeta =
    baseTarget && typeof baseTarget.targetMeta === 'object' && baseTarget.targetMeta ? baseTarget.targetMeta : {};
  const hasExplicitTargetSelection =
    normalizeIdList(Array.isArray(queuePayload.telegramTargetIds) ? queuePayload.telegramTargetIds : []).length > 0 ||
    normalizeChatIdList(Array.isArray(queuePayload.telegramChatIds) ? queuePayload.telegramChatIds : []).length > 0 ||
    Boolean(cleanText(baseTarget.targetRef || baseTarget.target_ref)) ||
    Number.isFinite(Number.parseInt(String(baseTargetMeta.targetId ?? ''), 10)) ||
    Boolean(cleanText(baseTargetMeta.chatId));

  if (!hasExplicitTargetSelection) {
    return [baseTarget];
  }

  const config = getTelegramBotClientConfig();
  const targets = resolveExpansionTargets(queuePayload, config);

  if (!targets.length) {
    return [baseTarget];
  }

  return targets.map((resolvedTarget) => ({
    ...baseTarget,
    targetRef: resolvedTarget.chatId,
    targetLabel: resolvedTarget.name,
    targetMeta: {
      ...baseTargetMeta,
      targetId: resolvedTarget.id,
      name: resolvedTarget.name,
      chatId: resolvedTarget.chatId,
      isFallback: Boolean(resolvedTarget.isFallback),
      channelKind: resolvedTarget.channelKind || resolvedTarget.targetKind || 'standard',
      targetKind: resolvedTarget.targetKind || resolvedTarget.channelKind || 'standard',
      isLive: resolvedTarget.targetKind === 'live' || resolvedTarget.channelKind === 'live'
    }
  }));
}

export async function testTelegramBotTarget({ targetId = null, requestedBy = '' } = {}) {
  const numericTargetId = Number.parseInt(String(targetId ?? ''), 10);
  const config = getTelegramBotClientConfig();
  const target = config.targets.find((item) => item.id === numericTargetId) || null;

  if (!target) {
    throw buildTelegramBotError('Telegram-Ziel konnte nicht gefunden werden.', {
      retryable: false,
      code: 'TARGET_NOT_FOUND'
    });
  }

  if (!config.enabled) {
    throw buildTelegramBotError('Telegram Bot Client ist deaktiviert.', {
      retryable: false,
      code: 'OUTPUT_DISABLED'
    });
  }

  if (!config.tokenConfigured) {
    throw buildTelegramBotError('TELEGRAM_BOT_TOKEN fehlt im Backend.', {
      retryable: false,
      code: 'TOKEN_MISSING'
    });
  }

  if (!target.isActive || !target.useForPublishing) {
    const skipMessage = buildDisabledSkipMessage(target);
    console.warn('[OUTPUT_DISABLED_SKIP]', {
      source: 'telegram_target_test',
      targetId: target.id,
      targetName: target.name,
      targetChatId: target.chatId,
      channelKind: target.channelKind,
      requestedBy: cleanText(requestedBy) || 'unknown'
    });
    recordTelegramTargetState({
      targetId: target.id,
      status: 'skipped',
      errorMessage: skipMessage
    });
    throw buildTelegramBotError(skipMessage, {
      retryable: false,
      code: OUTPUT_DISABLED_SKIP
    });
  }

  const sentAt = nowIso();
  const result = await sendTelegramPost({
    text: `TEST SENDUNG\n${target.name}\n${sentAt}`,
    disableWebPagePreview: true,
    chatId: target.chatId,
    titlePreview: target.name,
    postContext: 'telegram_target_test'
  });

  recordTelegramTargetState({
    targetId: target.id,
    status: 'test_sent',
    sentAt,
    testedAt: sentAt
  });

  return {
    target,
    delivery: {
      ...result,
      sentAt
    }
  };
}

export async function sendTelegramBotTargetTest(targetId, input = {}) {
  return testTelegramBotTarget({
    targetId,
    requestedBy: input?.requestedBy || ''
  });
}

function isRetryableTelegramError(message = '') {
  const normalized = cleanText(message).toLowerCase();
  return (
    normalized.includes('timeout') ||
    normalized.includes('tempor') ||
    normalized.includes('network') ||
    normalized.includes('429') ||
    normalized.includes('too many requests') ||
    normalized.includes('retry')
  );
}

export async function sendTelegramDealToTargets(input = {}) {
  const config = getTelegramBotClientConfig();
  const requestedChatIds = normalizeChatIdList(
    Array.isArray(input.telegramTargetChatIds) ? input.telegramTargetChatIds : []
  );
  const resolution = requestedChatIds.length
    ? resolveTargetsFromPayload({ telegramChatIds: requestedChatIds }, config)
    : resolveTargetsFromPayload(input.queuePayload ?? {}, config);
  const text = cleanText(input.text);
  const queuePayload = input.queuePayload && typeof input.queuePayload === 'object' ? input.queuePayload : {};
  const hasStructuredDealPayload = Boolean(
    cleanText(queuePayload.title) || cleanText(queuePayload.currentPrice) || cleanText(queuePayload.link)
  );
  const deliveryContext = cleanText(input.deliveryContext) || 'publish';
  const skippedTargets = [];

  if (!config.enabled) {
    throw buildTelegramBotError('Telegram Bot Client ist deaktiviert.', { retryable: false });
  }

  if (!config.tokenConfigured) {
    throw buildTelegramBotError('TELEGRAM_BOT_TOKEN fehlt im Backend.', { retryable: false });
  }

  if (!text && !hasStructuredDealPayload) {
    throw buildTelegramBotError('Telegram-Text fuer den Versand fehlt.', { retryable: false });
  }

  for (const skippedTarget of resolution.skippedTargets || []) {
    console.warn('[OUTPUT_DISABLED_SKIP]', {
      source: 'telegram_bot_client',
      targetId: skippedTarget.targetId,
      targetName: skippedTarget.targetName,
      targetChatId: skippedTarget.targetChatId,
      channelKind: skippedTarget.channelKind
    });
    recordTelegramTargetState({
      targetId: skippedTarget.targetId,
      status: 'skipped',
      errorMessage: skippedTarget.reason
    });
    skippedTargets.push(skippedTarget);
  }

  if (!resolution.targets.length) {
    if (skippedTargets.length) {
      return {
        targetMode: resolution.targetMode,
        targets: [],
        skippedTargets
      };
    }

    throw buildTelegramBotError('Keine Telegram-Zielgruppe fuer den Publisher konfiguriert.', { retryable: false });
  }

  const deliveredTargets = [];
  const failedTargets = [];

  for (const target of resolution.targets) {
    try {
      const sentAt = nowIso();
      const result = await sendTelegramDealPost({
        title: cleanText(queuePayload.title),
        price: cleanText(queuePayload.currentPrice),
        affiliateLink: cleanText(queuePayload.link),
        asin: cleanText(queuePayload.asin).toUpperCase(),
        debugInfo: cleanText(queuePayload.debugInfoByChannel?.telegram || ''),
        testMode: queuePayload.testMode === true,
        uploadedFile: input.uploadedFile,
        uploadedImage: input.uploadedImage,
        imageUrl: input.imageUrl,
        chatId: target.chatId,
        fallbackText: text,
        rabattgutscheinCode: input.rabattgutscheinCode,
        duplicateContext: {
          channelType: cleanText(queuePayload.telegramDuplicateChannelType || queuePayload.meta?.telegramRoutingChannel || 'telegram'),
          targetRef: target.chatId,
          asin: cleanText(queuePayload.asin).toUpperCase(),
          title: cleanText(queuePayload.title),
          price: cleanText(queuePayload.currentPrice),
          url: cleanText(queuePayload.normalizedUrl || queuePayload.link),
          originalUrl: cleanText(queuePayload.link),
          postContext: cleanText(queuePayload.telegramDuplicatePostContext || '')
        }
      });

      recordTelegramTargetState({
        targetId: target.id,
        status: deliveryContext === 'test' ? 'test_sent' : 'sent',
        sentAt,
        testedAt: deliveryContext === 'test' ? sentAt : null
      });

      deliveredTargets.push({
        ...result,
        targetId: target.id ?? null,
        targetName: target.name,
        targetChatId: target.chatId,
        isFallback: Boolean(target.isFallback),
        sentAt
      });
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Telegram-Versand fehlgeschlagen.';
      recordTelegramTargetState({
        targetId: target.id,
        status: 'error',
        errorMessage
      });

      failedTargets.push({
        targetId: target.id ?? null,
        targetName: target.name,
        targetChatId: target.chatId,
        error: errorMessage
      });
    }
  }

  if (failedTargets.length) {
    throw buildTelegramBotError(
      `Telegram Bot Client konnte ${failedTargets.length} Zielgruppe(n) nicht beliefern: ${failedTargets
        .map((target) => `${target.targetName || target.targetChatId} (${target.error})`)
        .join('; ')}`,
      {
        retryable: failedTargets.every((target) => isRetryableTelegramError(target.error)),
        details: failedTargets,
        deliveredTargets,
        skippedTargets
      }
    );
  }

  return {
    targetMode: resolution.targetMode,
    targets: deliveredTargets,
    skippedTargets
  };
}

export const __testablesTelegramBotClient = {
  DEFAULT_LIVE_CHANNEL_CHAT_ID,
  buildDisabledSkipMessage,
  computeEffectiveTargets,
  expandTelegramPublishingTargets,
  getTelegramBotTargetState,
  normalizeChatIdKey,
  resolveTargetsFromPayload,
  splitResolvedTargets
};
