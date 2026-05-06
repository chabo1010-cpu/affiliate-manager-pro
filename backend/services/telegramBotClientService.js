import { getDb } from '../db.js';
import { getTelegramConfig } from '../env.js';
import { cleanText } from './dealHistoryService.js';
import { sendTelegramDealPost } from './telegramSenderService.js';

const db = getDb();
const DEFAULT_RETRY_LIMIT = 3;

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

function buildTelegramBotError(message, options = {}) {
  const error = new Error(message);
  error.retryable = options.retryable !== false;
  if (options.details) {
    error.details = options.details;
  }
  if (options.deliveredTargets) {
    error.deliveredTargets = options.deliveredTargets;
  }
  return error;
}

function mapTargetRow(row) {
  return {
    id: Number(row.id),
    name: cleanText(row.name) || `Telegram Ziel ${row.id}`,
    chatId: cleanText(row.chat_id),
    isActive: row.is_active === 1,
    useForPublishing: row.use_for_publishing === 1,
    createdAt: row.created_at,
    updatedAt: row.updated_at
  };
}

function normalizeTargetInput(target = {}, index = 0) {
  const chatId = cleanText(target.chatId || target.chat_id);
  if (!chatId) {
    return null;
  }

  const numericId = Number.parseInt(String(target.id ?? ''), 10);

  return {
    id: Number.isFinite(numericId) ? numericId : null,
    name: cleanText(target.name) || `Telegram Ziel ${index + 1}`,
    chatId,
    isActive: parseEnabledFlag(target.isActive ?? target.is_active, true),
    useForPublishing: parseEnabledFlag(target.useForPublishing ?? target.use_for_publishing, true)
  };
}

function readTelegramBotSettingsRow() {
  const row = db
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
    .get();

  if (row) {
    return row;
  }

  db.prepare(
    `
      INSERT INTO app_settings (
        id,
        repostCooldownEnabled,
        repostCooldownHours,
        telegramCopyButtonText,
        copybotEnabled,
        telegramBotEnabled,
        telegramBotDefaultRetryLimit,
        facebookEnabled,
        facebookSessionMode,
        facebookDefaultRetryLimit,
        facebookDefaultTarget
      ) VALUES (1, 1, 12, 'Zum Kopieren hier klicken', 0, 1, 3, 0, 'persistent', 3, NULL)
    `
  ).run();

  return db
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
    .get();
}

function listPersistentTargets() {
  return db
    .prepare(
      `
        SELECT *
        FROM telegram_bot_targets
        ORDER BY use_for_publishing DESC, is_active DESC, id ASC
      `
    )
    .all()
    .map(mapTargetRow);
}

function buildEnvFallbackTarget(defaultRetryLimit) {
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
    isFallback: true,
    retryLimit: defaultRetryLimit
  };
}

function normalizeChatIdList(values = []) {
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

function dedupeTargets(targets = []) {
  const byChatId = new Map();

  for (const target of targets) {
    const chatId = cleanText(target.chatId);
    if (!chatId || byChatId.has(chatId)) {
      continue;
    }

    byChatId.set(chatId, target);
  }

  return Array.from(byChatId.values());
}

function isRetryableTelegramError(message = '') {
  const normalized = cleanText(message).toLowerCase();
  if (!normalized) {
    return true;
  }

  const nonRetryablePatterns = [
    'telegram_bot_token',
    'telegram_bot_token fehlt',
    'telegram_chat_id fehlt',
    'chat not found',
    'bot was blocked',
    'bot is not a member',
    'forbidden',
    'unauthorized',
    'have no rights to send',
    'group chat was upgraded',
    'chat_write_forbidden'
  ];

  return !nonRetryablePatterns.some((pattern) => normalized.includes(pattern));
}

function resolveTargetsFromPayload(queuePayload = {}, config = getTelegramBotClientConfig()) {
  const requestedTargetIds = normalizeIdList(
    Array.isArray(queuePayload.telegramTargetIds) ? queuePayload.telegramTargetIds : []
  );
  const requestedChatIds = normalizeChatIdList(
    Array.isArray(queuePayload.telegramChatIds) ? queuePayload.telegramChatIds : []
  );

  if (requestedTargetIds.length) {
    const selectedTargets = config.targets.filter((target) => requestedTargetIds.includes(target.id) && target.isActive);
    if (!selectedTargets.length) {
      throw buildTelegramBotError('Keine aktive Telegram-Zielgruppe fuer die uebergebenen Target-IDs gefunden.', {
        retryable: false
      });
    }

    return {
      targetMode: 'target_ids',
      targets: selectedTargets
    };
  }

  if (requestedChatIds.length) {
    const selectedTargets = dedupeTargets(
      requestedChatIds.map((chatId) => {
        const persistedTarget = config.targets.find((target) => target.chatId === chatId);

        return (
          persistedTarget || {
            id: null,
            name: `Telegram ${chatId}`,
            chatId,
            isActive: true,
            useForPublishing: true,
            isFallback: false
          }
        );
      })
    );

    return {
      targetMode: 'chat_ids',
      targets: selectedTargets
    };
  }

  return {
    targetMode: config.targets.some((target) => target.isActive && target.useForPublishing)
      ? 'persistent_targets'
      : 'env_fallback',
    targets: config.effectiveTargets
  };
}

export function getTelegramBotClientConfig() {
  const settingsRow = readTelegramBotSettingsRow();
  const defaultRetryLimit = parseRetryLimit(settingsRow?.telegramBotDefaultRetryLimit, DEFAULT_RETRY_LIMIT);
  const telegramConfig = getTelegramConfig();
  const targets = listPersistentTargets();
  const publishTargets = targets.filter((target) => target.isActive && target.useForPublishing);
  const envFallbackTarget = buildEnvFallbackTarget(defaultRetryLimit);

  return {
    enabled: parseEnabledFlag(settingsRow?.telegramBotEnabled, true),
    defaultRetryLimit,
    tokenConfigured: Boolean(cleanText(telegramConfig.token)),
    fallbackChatConfigured: Boolean(cleanText(telegramConfig.chatId)),
    targets,
    effectiveTargets: publishTargets.length ? publishTargets : envFallbackTarget ? [envFallbackTarget] : []
  };
}

export function getTelegramBotRetryLimit() {
  return getTelegramBotClientConfig().defaultRetryLimit;
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
      if (target.id && existingIds.has(target.id)) {
        db.prepare(
          `
            UPDATE telegram_bot_targets
            SET name = ?,
                chat_id = ?,
                is_active = ?,
                use_for_publishing = ?,
                updated_at = ?
            WHERE id = ?
          `
        ).run(target.name, target.chatId, target.isActive ? 1 : 0, target.useForPublishing ? 1 : 0, timestamp, target.id);
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
              created_at,
              updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
          `
        )
        .run(target.name, target.chatId, target.isActive ? 1 : 0, target.useForPublishing ? 1 : 0, timestamp, timestamp);

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
  try {
    const resolution = resolveTargetsFromPayload(queuePayload);
    if (!resolution.targets.length) {
      return [baseTarget];
    }

    return resolution.targets.map((resolvedTarget) => ({
      ...baseTarget,
      targetRef: resolvedTarget.chatId,
      targetLabel: resolvedTarget.name,
      targetMeta: {
        targetId: resolvedTarget.id,
        name: resolvedTarget.name,
        chatId: resolvedTarget.chatId,
        isFallback: Boolean(resolvedTarget.isFallback)
      }
    }));
  } catch {
    return [baseTarget];
  }
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

  if (!config.enabled) {
    throw buildTelegramBotError('Telegram Bot Client ist deaktiviert.', { retryable: false });
  }

  if (!config.tokenConfigured) {
    throw buildTelegramBotError('TELEGRAM_BOT_TOKEN fehlt im Backend.', { retryable: false });
  }

  if (!text && !hasStructuredDealPayload) {
    throw buildTelegramBotError('Telegram-Text fuer den Versand fehlt.', { retryable: false });
  }

  if (!resolution.targets.length) {
    throw buildTelegramBotError('Keine Telegram-Zielgruppe fuer den Publisher konfiguriert.', { retryable: false });
  }

  console.info('[OUTPUT_CONFIG]', {
    configSource: 'telegram_bot_client',
    enabled: config.enabled,
    tokenConfigured: config.tokenConfigured,
    fallbackChatConfigured: config.fallbackChatConfigured,
    targetMode: resolution.targetMode,
    requestedChatIds,
    resolvedTargetCount: resolution.targets.length,
    resolvedChatIds: resolution.targets.map((target) => target.chatId)
  });
  console.info('[OUTPUT_PAYLOAD]', {
    configSource: 'telegram_bot_client',
    textLength: text.length,
    textPreview: text.slice(0, 160),
    hasUploadedFile: Boolean(input.uploadedFile),
    hasUploadedImage: Boolean(cleanText(input.uploadedImage)),
    hasImageUrl: Boolean(cleanText(input.imageUrl)),
    disableWebPagePreview: input.disableWebPagePreview === true,
    hasCouponCode: Boolean(cleanText(input.rabattgutscheinCode)),
    hasStructuredDealPayload,
    titlePreview: cleanText(queuePayload.title).slice(0, 120) || null
  });

  const deliveredTargets = [];
  const failedTargets = [];

  for (const target of resolution.targets) {
    try {
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

      deliveredTargets.push({
        ...result,
        targetId: target.id ?? null,
        targetName: target.name,
        targetChatId: target.chatId,
        isFallback: Boolean(target.isFallback)
      });
    } catch (error) {
      failedTargets.push({
        targetId: target.id ?? null,
        targetName: target.name,
        targetChatId: target.chatId,
        error: error instanceof Error ? error.message : 'Telegram-Versand fehlgeschlagen.'
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
        deliveredTargets
      }
    );
  }

  return {
    targetMode: resolution.targetMode,
    targets: deliveredTargets
  };
}
