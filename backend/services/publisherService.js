import { getDb } from '../db.js';
import { getReaderRuntimeConfig } from '../env.js';
import { assertDealNotLocked, cleanText } from './dealHistoryService.js';
import { buildDealStatusKey, syncDealStatusWithQueue } from './databaseService.js';
import {
  PUBLISHING_QUEUE_STATUS,
  isFailedPublishingQueueStatus,
  isRetryPublishingQueueStatus,
  isSendingPublishingQueueStatus,
  isSentPublishingQueueStatus,
  isWaitingPublishingQueueStatus,
  normalizePublishingQueueStatus
} from './publishingQueueStateService.js';
import { expandTelegramPublishingTargets, getTelegramBotClientConfig, getTelegramBotRetryLimit } from './telegramBotClientService.js';
import { processTelegramPublishingTarget } from './telegramWorkerService.js';
import { processWhatsappPublishingTarget } from './whatsappWorkerService.js';
import { getWhatsappClientConfig, getWhatsappClientRetryLimit } from './whatsappClientService.js';
import { processFacebookPublishingTarget } from './facebookWorkerService.js';
import { getCopybotRuntimeState, isCopybotControlledSourceType } from './copybotControlService.js';

const db = getDb();
const PUBLISHER_LOOP_INTERVAL_MS = 15 * 1000;
const SQLITE_RETRY_ATTEMPTS = 6;
const SQLITE_RETRY_BASE_DELAY_MS = 40;
let publishingWorkerLoopStarted = false;
let publishingWorkerLoopRunning = false;

function getDealLockBypassMeta(explicitSkipDealLock = false) {
  const runtimeConfig = getReaderRuntimeConfig();
  return {
    active: explicitSkipDealLock === true || runtimeConfig.dealLockBypass,
    explicitSkipDealLock: explicitSkipDealLock === true,
    readerTestMode: runtimeConfig.readerTestMode,
    readerDebugMode: runtimeConfig.readerDebugMode
  };
}

function nowIso() {
  return new Date().toISOString();
}

function stringifyJson(value) {
  return JSON.stringify(value ?? null);
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

function sleepSync(delayMs) {
  if (!Number.isFinite(delayMs) || delayMs <= 0) {
    return;
  }

  const sharedArray = new SharedArrayBuffer(4);
  const view = new Int32Array(sharedArray);
  Atomics.wait(view, 0, 0, delayMs);
}

function isSqliteBusyError(error) {
  const message = error instanceof Error ? error.message : String(error || '');
  return /SQLITE_BUSY|database is locked|database schema is locked/i.test(message);
}

function runWithSqliteWriteRetry(operation, { attempts = SQLITE_RETRY_ATTEMPTS, baseDelayMs = SQLITE_RETRY_BASE_DELAY_MS } = {}) {
  let lastError;

  for (let attempt = 0; attempt < attempts; attempt += 1) {
    try {
      return operation();
    } catch (error) {
      lastError = error;
      if (!isSqliteBusyError(error) || attempt === attempts - 1) {
        throw error;
      }

      const delayMs = Math.min(300, baseDelayMs * 2 ** attempt);
      sleepSync(delayMs);
    }
  }

  throw lastError;
}

function getAppSettings() {
  return db.prepare(`SELECT * FROM app_settings WHERE id = 1`).get();
}

function getPublishingOrigin(sourceType = '', originOverride = '') {
  const normalizedOverride = cleanText(originOverride).toLowerCase();
  if (normalizedOverride) {
    return normalizedOverride;
  }

  return ['generator', 'generator_direct', 'manual_post'].includes(cleanText(sourceType).toLowerCase())
    ? 'manual'
    : 'automatic';
}

function buildDealLockError(message = 'Deal-Lock aktiv: Der Deal ist bereits in Queue oder Verarbeitung.', meta = null) {
  const error = new Error(message);
  error.code = 'DEAL_LOCK_ACTIVE_QUEUE';
  error.retryable = false;
  if (meta && typeof meta === 'object') {
    error.meta = meta;
  }
  return error;
}

function isActiveDealKeyConstraintError(error) {
  const message = error instanceof Error ? error.message : String(error || '');
  return /idx_publishing_queue_active_deal_key|UNIQUE constraint failed: publishing_queue\.deal_key/i.test(message);
}

function logPublishing({ queueId = null, targetId = null, workerType = null, level = 'info', eventType, message, payload = null }) {
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
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `
  ).run(queueId, targetId, workerType, level, eventType, message, payload ? stringifyJson(payload) : null, nowIso());
}

function safeLogPublishing(input = {}) {
  try {
    runWithSqliteWriteRetry(() => logPublishing(input));
  } catch (error) {
    console.error('PUBLISHING LOG WRITE FAILED', error instanceof Error ? error.message : error);
  }
}

function safeSyncDealStatusWithQueue(input = {}) {
  try {
    return runWithSqliteWriteRetry(() => syncDealStatusWithQueue(input));
  } catch (error) {
    console.error('DEAL STATUS SYNC FAILED', error instanceof Error ? error.message : error);
    return null;
  }
}

function normalizeTarget(target = {}) {
  return {
    channelType: cleanText(target.channelType || target.channel_type).toLowerCase(),
    isEnabled: target.isEnabled === undefined ? true : Boolean(target.isEnabled),
    imageSource: cleanText(target.imageSource || target.image_source).toLowerCase() || 'none',
    targetRef: cleanText(target.targetRef || target.target_ref),
    targetLabel: cleanText(target.targetLabel || target.target_label),
    targetMeta: target.targetMeta || target.target_meta || null
  };
}

function mapPublishingTargetRow(row = {}) {
  return {
    ...row,
    status: normalizePublishingQueueStatus(row.status, row.status)
  };
}

function mapPublishingQueueRow(queue, targets = []) {
  if (!queue) {
    return null;
  }

  return {
    ...queue,
    status: normalizePublishingQueueStatus(queue.status, queue.status),
    dealKey: queue.deal_key || '',
    attemptCount: Number(queue.attempt_count || 0),
    retryCount: Number(queue.retry_count || 0),
    payload: parseJson(queue.payload_json, {}),
    targets: targets.map(mapPublishingTargetRow)
  };
}

function expandPublishingTargets(payload, targets = []) {
  return targets.flatMap((target) => {
    if (!target.isEnabled) {
      return [target];
    }

    if (target.channelType === 'telegram') {
      return expandTelegramPublishingTargets(target, payload);
    }

    return [target];
  });
}

function getSelectedImage(payload, imageSource) {
  const variants = payload.imageVariants || {};

  if (imageSource === 'upload') {
    return variants.upload || '';
  }

  if (imageSource === 'standard') {
    return variants.standard || '';
  }

  return '';
}

function buildChannelPayload(payload, channelType) {
  const texts = payload.textByChannel || {};

  return {
    text: texts[channelType] || texts.telegram || payload.title || '',
    imageUrl: getSelectedImage(payload, payload.targetImageSources?.[channelType] || 'none'),
    link: cleanText(payload.link),
    couponCode: cleanText(payload.couponCode)
  };
}

const insertPublishingEntryTransaction = db.transaction(
  ({ sourceType, sourceId, payload, targets, originOverride = '', skipDealLock = false }) => {
    const timestamp = nowIso();
    const origin = getPublishingOrigin(sourceType, originOverride);
    const dealLockBypass = getDealLockBypassMeta(
      skipDealLock === true || (payload && typeof payload === 'object' && payload.skipDealLock === true)
    );
    const enrichedPayload = {
      ...(payload && typeof payload === 'object' ? payload : {}),
      sourceId: sourceId ?? payload?.sourceId ?? null,
      databaseSourceType: sourceType,
      databaseOrigin: origin,
      ...(dealLockBypass.active ? { skipDealLock: true } : {})
    };
    const normalizedTargets = expandPublishingTargets(
      enrichedPayload,
      (Array.isArray(targets) ? targets : []).map(normalizeTarget).filter((item) => item.channelType)
    );
    const baseDealKey = buildDealStatusKey({
      asin: enrichedPayload.asin,
      normalizedUrl: enrichedPayload.normalizedUrl || enrichedPayload.link,
      originalUrl: enrichedPayload.link,
      queueId: null
    });
    const dealKey = dealLockBypass.active ? null : baseDealKey;

    if (dealLockBypass.active) {
      console.info('[DEAL_LOCK_BYPASSED]', {
        phase: 'queue_create',
        sourceType: cleanText(sourceType),
        sourceId: sourceId ?? null,
        asin: cleanText(enrichedPayload.asin).toUpperCase() || '',
        normalizedUrl: cleanText(enrichedPayload.normalizedUrl || enrichedPayload.link) || '',
        explicitSkipDealLock: dealLockBypass.explicitSkipDealLock,
        readerTestMode: dealLockBypass.readerTestMode,
        readerDebugMode: dealLockBypass.readerDebugMode
      });
    } else {
      try {
        assertDealNotLocked({
          asin: enrichedPayload.asin,
          url: enrichedPayload.link,
          normalizedUrl: enrichedPayload.normalizedUrl || enrichedPayload.link,
          sourceType,
          origin
        });
      } catch (error) {
        console.error('[DEAL_LOCK_BLOCKED]', {
          phase: 'queue_create',
          sourceType: cleanText(sourceType),
          sourceId: sourceId ?? null,
          asin: cleanText(enrichedPayload.asin).toUpperCase() || '',
          normalizedUrl: cleanText(enrichedPayload.normalizedUrl || enrichedPayload.link) || '',
          reason: error instanceof Error ? error.message : 'Deal-Lock aktiv.',
          blockCode: error instanceof Error ? error.code || error.dealLock?.blockCode || '' : '',
          readerTestMode: dealLockBypass.readerTestMode,
          readerDebugMode: dealLockBypass.readerDebugMode
        });
        throw error;
      }
    }

    const queueResult = db
      .prepare(
        `
          INSERT INTO publishing_queue (
            source_type,
            source_id,
            status,
            payload_json,
            deal_key,
            attempt_count,
            retry_count,
            next_retry_at,
            created_at,
            updated_at
          ) VALUES (?, ?, ?, ?, ?, 0, 0, NULL, ?, ?)
        `
      )
      .run(
        sourceType,
        sourceId,
        PUBLISHING_QUEUE_STATUS.pending,
        stringifyJson(enrichedPayload),
        dealKey || null,
        timestamp,
        timestamp
      );

    const queueId = Number(queueResult.lastInsertRowid);
    const targetStatement = db.prepare(
      `
        INSERT INTO publishing_targets (
          queue_id,
          channel_type,
          is_enabled,
          image_source,
          target_ref,
          target_label,
          target_meta_json,
          status,
          posted_at,
          error_message,
          created_at,
          updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)
      `
    );

    normalizedTargets.forEach((target) => {
      targetStatement.run(
        queueId,
        target.channelType,
        target.isEnabled ? 1 : 0,
        target.imageSource,
        target.targetRef || null,
        target.targetLabel || null,
        target.targetMeta ? stringifyJson(target.targetMeta) : null,
        PUBLISHING_QUEUE_STATUS.pending,
        timestamp,
        timestamp
      );
    });

    logPublishing({
      queueId,
      workerType: 'publisher',
      eventType: 'queue.created',
      message: `Publishing-Eintrag aus ${sourceType} erstellt.`,
      payload: {
        sourceId,
        dealKey,
        targets: normalizedTargets
      }
    });

    syncDealStatusWithQueue({
      queueId,
      queueStatus: PUBLISHING_QUEUE_STATUS.pending,
      payload: enrichedPayload,
      sourceType,
      sourceId,
      message: `Queue fuer ${sourceType} erstellt.`,
      origin,
      meta: {
        dealKey,
        targetCount: normalizedTargets.length
      }
    });

    return queueId;
  }
);

export function createPublishingEntry(input = {}) {
  try {
    const queueId = runWithSqliteWriteRetry(() => insertPublishingEntryTransaction(input));
    return getPublishingQueueEntry(queueId);
  } catch (error) {
    if (isActiveDealKeyConstraintError(error)) {
      console.error('[DEAL_LOCK_BLOCKED]', {
        phase: 'queue_create_constraint',
        sourceType: cleanText(input.sourceType),
        sourceId: input.sourceId ?? null,
        asin: cleanText(input.payload?.asin).toUpperCase() || '',
        normalizedUrl: cleanText(input.payload?.normalizedUrl || input.payload?.link) || '',
        reason: error instanceof Error ? error.message : 'Aktiver Queue-Deal-Key blockiert.'
      });
      throw buildDealLockError(undefined, {
        sourceType: cleanText(input.sourceType),
        asin: cleanText(input.payload?.asin),
        normalizedUrl: cleanText(input.payload?.normalizedUrl || input.payload?.link)
      });
    }

    throw error;
  }
}

export function createGeneratorPublishingEntry(input = {}) {
  const timestamp = nowIso();
  const generatorResult = runWithSqliteWriteRetry(() =>
    db
      .prepare(
        `
          INSERT INTO generator_posts (
            title,
            product_link,
            asin,
            normalized_url,
            seller_type,
            telegram_text,
            whatsapp_text,
            facebook_text,
            generated_image_path,
            uploaded_image_path,
            telegram_image_source,
            whatsapp_image_source,
            facebook_image_source,
            created_at,
            updated_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `
      )
      .run(
        cleanText(input.title),
        cleanText(input.link),
        cleanText(input.asin).toUpperCase(),
        cleanText(input.normalizedUrl),
        cleanText(input.sellerType) || 'FBM',
        cleanText(input.textByChannel?.telegram),
        cleanText(input.textByChannel?.whatsapp),
        cleanText(input.textByChannel?.facebook),
        cleanText(input.generatedImagePath),
        cleanText(input.uploadedImagePath),
        cleanText(input.telegramImageSource) || 'standard',
        cleanText(input.whatsappImageSource) || 'standard',
        cleanText(input.facebookImageSource) || 'link_preview',
        timestamp,
        timestamp
      )
  );

  return createPublishingEntry({
    sourceType: 'generator',
    sourceId: generatorResult.lastInsertRowid,
    payload: {
      ...input,
      generatorPostId: generatorResult.lastInsertRowid
    },
    targets: [
      { channelType: 'telegram', isEnabled: input.enableTelegram !== false, imageSource: input.telegramImageSource },
      { channelType: 'whatsapp', isEnabled: input.enableWhatsapp !== false, imageSource: input.whatsappImageSource },
      { channelType: 'facebook', isEnabled: input.enableFacebook !== false, imageSource: input.facebookImageSource }
    ]
  });
}

export function enqueueCopybotPublishing(input = {}) {
  const copybotState = getCopybotRuntimeState();
  if (copybotState.enabled !== true) {
    console.warn('[COPYBOT_SKIP_PIPELINE_DISABLED]', {
      sourceType: 'copybot',
      sourceId: input.sourceId ?? null,
      reason: copybotState.reason,
      envEnabled: copybotState.envEnabled,
      settingEnabled: copybotState.settingEnabled
    });
    const error = new Error('Copybot ist deaktiviert. Queue-Eintrag wurde nicht erstellt.');
    error.retryable = false;
    throw error;
  }

  return createPublishingEntry({
    sourceType: 'copybot',
    sourceId: input.sourceId ?? null,
    payload: input.payload,
    targets: input.targets
  });
}

export function getPublishingQueueEntry(queueId) {
  const queue = db.prepare(`SELECT * FROM publishing_queue WHERE id = ?`).get(queueId);
  if (!queue) {
    return null;
  }

  const targets = db.prepare(`SELECT * FROM publishing_targets WHERE queue_id = ? ORDER BY id ASC`).all(queueId);
  return mapPublishingQueueRow(queue, targets);
}

function buildPublishingQueueQuery({ sourceType = '', limit = null } = {}) {
  const clauses = [];
  const params = [];

  if (cleanText(sourceType)) {
    clauses.push(`source_type = ?`);
    params.push(cleanText(sourceType));
  }

  let sql = `SELECT * FROM publishing_queue`;
  if (clauses.length) {
    sql += ` WHERE ${clauses.join(' AND ')}`;
  }

  sql += ` ORDER BY created_at DESC`;

  const normalizedLimit = Number(limit);
  if (Number.isFinite(normalizedLimit) && normalizedLimit > 0) {
    sql += ` LIMIT ${Math.max(1, Math.trunc(normalizedLimit))}`;
  }

  return { sql, params };
}

export function listPublishingQueue(options = {}) {
  const { sql, params } = buildPublishingQueueQuery(options);
  const queueRows = db.prepare(sql).all(...params);
  if (!queueRows.length) {
    return [];
  }

  const queueIds = queueRows.map((queue) => Number(queue.id)).filter((id) => Number.isFinite(id) && id > 0);
  const placeholders = queueIds.map(() => '?').join(', ');
  const targetRows = placeholders
    ? db
        .prepare(`SELECT * FROM publishing_targets WHERE queue_id IN (${placeholders}) ORDER BY queue_id ASC, id ASC`)
        .all(...queueIds)
    : [];
  const targetsByQueueId = new Map();

  for (const row of targetRows) {
    const queueId = Number(row.queue_id);
    if (!targetsByQueueId.has(queueId)) {
      targetsByQueueId.set(queueId, []);
    }
    targetsByQueueId.get(queueId).push(row);
  }

  return queueRows.map((queue) => mapPublishingQueueRow(queue, targetsByQueueId.get(Number(queue.id)) || []));
}

export function getPublishingQueueCounts({ sourceType = '' } = {}) {
  const clauses = [];
  const params = [];

  if (cleanText(sourceType)) {
    clauses.push(`source_type = ?`);
    params.push(cleanText(sourceType));
  }

  let sql = `
    SELECT
      COUNT(*) AS total_count,
      SUM(CASE WHEN status IN ('pending', 'queued', 'retry', 'sending', 'processing') THEN 1 ELSE 0 END) AS open_count
    FROM publishing_queue
  `;

  if (clauses.length) {
    sql += ` WHERE ${clauses.join(' AND ')}`;
  }

  const row = db.prepare(sql).get(...params) || {};

  return {
    totalCount: Number(row.total_count || 0),
    openCount: Number(row.open_count || 0)
  };
}

export function listPublishingLogs(options = {}) {
  const normalizedLimit = Number(options?.limit);
  const limit = Number.isFinite(normalizedLimit) && normalizedLimit > 0 ? Math.max(1, Math.trunc(normalizedLimit)) : 200;

  return db
    .prepare(`SELECT * FROM publishing_logs ORDER BY created_at DESC LIMIT ${limit}`)
    .all()
    .map((row) => ({
      ...row,
      payload: parseJson(row.payload_json, null)
    }));
}

export function getWorkerStatus() {
  const stats = db
    .prepare(
      `
        SELECT
          channel_type,
          SUM(CASE WHEN status IN ('pending', 'queued') THEN 1 ELSE 0 END) AS pending,
          SUM(CASE WHEN status IN ('sending', 'processing') THEN 1 ELSE 0 END) AS sending,
          SUM(CASE WHEN status IN ('sent', 'posted') THEN 1 ELSE 0 END) AS sent,
          SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
          SUM(CASE WHEN status = 'retry' THEN 1 ELSE 0 END) AS retry
        FROM publishing_targets
        WHERE is_enabled = 1
        GROUP BY channel_type
      `
    )
    .all()
    .map((item) => ({
      ...item,
      waiting: Number(item.pending || 0) + Number(item.retry || 0),
      processing: Number(item.sending || 0),
      posted: Number(item.sent || 0)
    }));
  const settings = getAppSettings();
  const telegramBot = getTelegramBotClientConfig();
  const whatsappClient = getWhatsappClientConfig();

  return {
    channels: stats,
    telegramBot: {
      enabled: telegramBot.enabled,
      retryLimit: telegramBot.defaultRetryLimit,
      configuredTargets: telegramBot.targets.length,
      publishTargets: telegramBot.effectiveTargets.length,
      tokenConfigured: telegramBot.tokenConfigured,
      fallbackChatConfigured: telegramBot.fallbackChatConfigured
    },
    whatsapp: {
      enabled: whatsappClient.enabled,
      endpointConfigured: whatsappClient.endpointConfigured,
      senderConfigured: whatsappClient.senderConfigured,
      sender: whatsappClient.sender,
      retryLimit: whatsappClient.retryLimit
    },
    facebook: {
      enabled: settings?.facebookEnabled === 1,
      sessionMode: settings?.facebookSessionMode || 'persistent',
      retryLimit: Number(settings?.facebookDefaultRetryLimit ?? 3),
      defaultTarget: settings?.facebookDefaultTarget || ''
    }
  };
}

export function saveFacebookWorkerSettings(input = {}) {
  runWithSqliteWriteRetry(() =>
    db
      .prepare(
        `
          UPDATE app_settings
          SET facebookEnabled = ?,
              facebookSessionMode = ?,
              facebookDefaultRetryLimit = ?,
              facebookDefaultTarget = ?
          WHERE id = 1
        `
      )
      .run(
        input.facebookEnabled ? 1 : 0,
        cleanText(input.facebookSessionMode) || 'persistent',
        Number(input.facebookDefaultRetryLimit ?? 3),
        cleanText(input.facebookDefaultTarget) || null
      )
  );

  return getWorkerStatus().facebook;
}

function updateQueueStatus(queueId) {
  const queue = db.prepare(`SELECT status, next_retry_at FROM publishing_queue WHERE id = ?`).get(queueId);
  if (!queue) {
    return null;
  }

  const targets = db.prepare(`SELECT status FROM publishing_targets WHERE queue_id = ? AND is_enabled = 1`).all(queueId);
  const normalizedStatuses = targets.map((item) => normalizePublishingQueueStatus(item.status, item.status));
  const nextStatus = normalizedStatuses.every((status) => isSentPublishingQueueStatus(status))
    ? PUBLISHING_QUEUE_STATUS.sent
    : normalizedStatuses.some((status) => isSendingPublishingQueueStatus(status))
      ? PUBLISHING_QUEUE_STATUS.sending
      : normalizedStatuses.some((status) => isRetryPublishingQueueStatus(status))
        ? PUBLISHING_QUEUE_STATUS.retry
        : normalizedStatuses.some((status) => isWaitingPublishingQueueStatus(status))
          ? PUBLISHING_QUEUE_STATUS.pending
          : normalizedStatuses.some((status) => isFailedPublishingQueueStatus(status))
            ? PUBLISHING_QUEUE_STATUS.failed
            : PUBLISHING_QUEUE_STATUS.pending;
  const nextRetryAt = nextStatus === PUBLISHING_QUEUE_STATUS.retry ? queue.next_retry_at || null : null;

  runWithSqliteWriteRetry(() =>
    db.prepare(`UPDATE publishing_queue SET status = ?, next_retry_at = ?, updated_at = ? WHERE id = ?`).run(
      nextStatus,
      nextRetryAt,
      nowIso(),
      queueId
    )
  );

  return nextStatus;
}

function resolveRetryLimitForTarget(target, settings = getAppSettings()) {
  if (target?.channel_type === 'telegram') {
    return getTelegramBotRetryLimit();
  }

  if (target?.channel_type === 'whatsapp') {
    return getWhatsappClientRetryLimit();
  }

  if (target?.channel_type === 'facebook') {
    return Number(settings?.facebookDefaultRetryLimit ?? 3);
  }

  return Number(settings?.facebookDefaultRetryLimit ?? 3);
}

function markTargetProcessing(target) {
  const timestamp = nowIso();

  runWithSqliteWriteRetry(() => {
    db.prepare(`UPDATE publishing_targets SET status = ?, updated_at = ? WHERE id = ?`).run(
      PUBLISHING_QUEUE_STATUS.sending,
      timestamp,
      target.id
    );
    db.prepare(
      `
        UPDATE publishing_queue
        SET status = ?,
            attempt_count = COALESCE(attempt_count, 0) + 1,
            updated_at = ?
        WHERE id = ?
      `
    ).run(PUBLISHING_QUEUE_STATUS.sending, timestamp, target.queue_id);
  });
}

function markTargetFailed(target, errorMessage, retry = true, retryLimitOverride = null) {
  const settings = getAppSettings();
  const retryLimit = Number.isFinite(Number(retryLimitOverride))
    ? Number(retryLimitOverride)
    : resolveRetryLimitForTarget(target, settings);
  const queue = db.prepare(`SELECT retry_count FROM publishing_queue WHERE id = ?`).get(target.queue_id);
  const nextRetryCountCandidate = Number(queue?.retry_count ?? 0) + 1;
  const canRetry = retry && nextRetryCountCandidate <= retryLimit;
  const nextRetryAt = canRetry ? new Date(Date.now() + 15 * 60 * 1000).toISOString() : null;

  runWithSqliteWriteRetry(() => {
    db.prepare(
      `
        UPDATE publishing_targets
        SET status = ?, error_message = ?, updated_at = ?
        WHERE id = ?
      `
    ).run(canRetry ? PUBLISHING_QUEUE_STATUS.retry : PUBLISHING_QUEUE_STATUS.failed, errorMessage, nowIso(), target.id);

    db.prepare(
      `
        UPDATE publishing_queue
        SET status = ?,
            retry_count = ?,
            next_retry_at = ?,
            updated_at = ?
        WHERE id = ?
      `
    ).run(
      canRetry ? PUBLISHING_QUEUE_STATUS.retry : PUBLISHING_QUEUE_STATUS.failed,
      canRetry ? nextRetryCountCandidate : Number(queue?.retry_count ?? 0),
      nextRetryAt,
      nowIso(),
      target.queue_id
    );
  });

  safeLogPublishing({
    queueId: target.queue_id,
    targetId: target.id,
    workerType: target.channel_type,
    level: 'warning',
    eventType: 'target.failed',
    message: errorMessage,
    payload: {
      retryLimit,
      retryScheduled: canRetry,
      nextRetryAt
    }
  });
}

function markTargetSent(target, workerType, workerResult = {}) {
  const timestamp = nowIso();

  runWithSqliteWriteRetry(() =>
    db.prepare(
      `
        UPDATE publishing_targets
        SET status = ?,
            posted_at = ?,
            error_message = NULL,
            updated_at = ?
        WHERE id = ?
      `
    ).run(PUBLISHING_QUEUE_STATUS.sent, timestamp, timestamp, target.id)
  );

  safeLogPublishing({
    queueId: target.queue_id,
    targetId: target.id,
    workerType,
    eventType: 'target.sent',
    message: `${workerType} Worker hat den Beitrag erfolgreich verarbeitet.`,
    payload: workerResult
  });
}

function deferTargetDueToCopybotDisabled(target, reason = 'Copybot deaktiviert.') {
  const nextRetryAt = new Date(Date.now() + 10 * 60 * 1000).toISOString();
  const timestamp = nowIso();

  runWithSqliteWriteRetry(() => {
    db.prepare(
      `
        UPDATE publishing_targets
        SET status = ?,
            error_message = ?,
            updated_at = ?
        WHERE id = ?
      `
    ).run(PUBLISHING_QUEUE_STATUS.retry, reason, timestamp, target.id);

    db.prepare(
      `
        UPDATE publishing_queue
        SET status = ?,
            next_retry_at = ?,
            updated_at = ?
        WHERE id = ?
      `
    ).run(PUBLISHING_QUEUE_STATUS.retry, nextRetryAt, timestamp, target.queue_id);
  });

  safeLogPublishing({
    queueId: target.queue_id,
    targetId: target.id,
    workerType: target.channel_type,
    level: 'warning',
    eventType: 'copybot.disabled.defer',
    message: reason,
    payload: {
      nextRetryAt
    }
  });

  return nextRetryAt;
}

function getTargetProcessor(target, processorOverrides = {}) {
  const processors = {
    telegram: processTelegramPublishingTarget,
    whatsapp: processWhatsappPublishingTarget,
    facebook: processFacebookPublishingTarget,
    ...(processorOverrides && typeof processorOverrides === 'object' ? processorOverrides : {})
  };

  return processors[target.channel_type] || null;
}

async function processTarget(target, processorOverrides = {}) {
  const queue = db.prepare(`SELECT * FROM publishing_queue WHERE id = ?`).get(target.queue_id);
  const payload = parseJson(queue?.payload_json, {});
  const origin = getPublishingOrigin(queue?.source_type || '', payload.databaseOrigin || '');
  const dealLockBypass = getDealLockBypassMeta(payload.skipDealLock === true);
  const channelPayload = buildChannelPayload(payload, target.channel_type);
  const workerPayload = {
    ...payload,
    textByChannel: {
      ...(payload.textByChannel || {}),
      [target.channel_type]: channelPayload.text
    }
  };

  if (isCopybotControlledSourceType(queue?.source_type || '')) {
    const copybotState = getCopybotRuntimeState();
    if (copybotState.enabled !== true) {
      const reason = `Copybot deaktiviert (${copybotState.reason}). Versand pausiert.`;
      console.warn('[COPYBOT_SKIP_SEND_DISABLED]', {
        queueId: target.queue_id,
        targetId: target.id,
        sourceType: queue?.source_type || '',
        channelType: target.channel_type,
        reason: copybotState.reason,
        envEnabled: copybotState.envEnabled,
        settingEnabled: copybotState.settingEnabled
      });
      const nextRetryAt = deferTargetDueToCopybotDisabled(target, reason);
      safeSyncDealStatusWithQueue({
        queueId: target.queue_id,
        queueStatus: PUBLISHING_QUEUE_STATUS.retry,
        payload,
        sourceType: queue?.source_type || '',
        sourceId: queue?.source_id ?? null,
        target,
        message: reason,
        origin,
        meta: {
          nextRetryAt,
          copybotState
        }
      });

      return {
        targetId: target.id,
        channelType: target.channel_type,
        status: PUBLISHING_QUEUE_STATUS.retry,
        skipped: true,
        reason
      };
    }
  }

  markTargetProcessing(target);

  try {
    if (dealLockBypass.active) {
      console.info('[DEAL_LOCK_BYPASSED]', {
        phase: 'publisher_worker',
        queueId: target.queue_id,
        targetId: target.id,
        channelType: target.channel_type,
        asin: cleanText(payload.asin).toUpperCase() || '',
        normalizedUrl: cleanText(payload.normalizedUrl || payload.link) || '',
        explicitSkipDealLock: dealLockBypass.explicitSkipDealLock,
        readerTestMode: dealLockBypass.readerTestMode,
        readerDebugMode: dealLockBypass.readerDebugMode
      });
    } else {
      try {
        assertDealNotLocked({
          asin: payload.asin,
          url: payload.link,
          normalizedUrl: payload.normalizedUrl || payload.link,
          queueId: target.queue_id,
          sourceType: queue?.source_type || '',
          origin
        });
      } catch (error) {
        console.error('[DEAL_LOCK_BLOCKED]', {
          phase: 'publisher_worker',
          queueId: target.queue_id,
          targetId: target.id,
          channelType: target.channel_type,
          asin: cleanText(payload.asin).toUpperCase() || '',
          normalizedUrl: cleanText(payload.normalizedUrl || payload.link) || '',
          reason: error instanceof Error ? error.message : 'Deal-Lock aktiv.',
          blockCode: error instanceof Error ? error.code || error.dealLock?.blockCode || '' : '',
          readerTestMode: dealLockBypass.readerTestMode,
          readerDebugMode: dealLockBypass.readerDebugMode
        });
        throw error;
      }
    }

    const processor = getTargetProcessor(target, processorOverrides);
    if (!processor) {
      throw new Error(`Unbekannter Channel ${target.channel_type}`);
    }

    const workerResult = await processor(target, workerPayload, queue);
    markTargetSent(target, target.channel_type, workerResult);
    const queueStatus = updateQueueStatus(target.queue_id) || PUBLISHING_QUEUE_STATUS.sent;
    const updatedQueue = db.prepare(`SELECT * FROM publishing_queue WHERE id = ?`).get(target.queue_id);
    safeSyncDealStatusWithQueue({
      queueId: target.queue_id,
      queueStatus: normalizePublishingQueueStatus(queueStatus, PUBLISHING_QUEUE_STATUS.sent),
      payload,
      sourceType: queue?.source_type || '',
      sourceId: queue?.source_id ?? null,
      target,
      message: `${target.channel_type} Target erfolgreich verarbeitet.`,
      origin,
      meta: {
        workerResult,
        dealKey: updatedQueue?.deal_key || ''
      }
    });

    return {
      targetId: target.id,
      channelType: target.channel_type,
      status: PUBLISHING_QUEUE_STATUS.sent,
      workerResult
    };
  } catch (error) {
    console.error('[PUBLISHER_ERROR]', {
      queueId: target.queue_id,
      targetId: target.id,
      channelType: target.channel_type,
      error: error instanceof Error ? error.message : 'Worker-Fehler'
    });
    console.error('[ERROR_REASON]', {
      reason: error instanceof Error ? error.message : 'Worker-Fehler',
      queueId: target.queue_id,
      targetId: target.id,
      channelType: target.channel_type
    });
    try {
      markTargetFailed(
        target,
        error instanceof Error ? error.message : 'Worker-Fehler',
        !(error instanceof Error && error.retryable === false),
        error instanceof Error ? error.retryLimit : null
      );
      const queueStatus = updateQueueStatus(target.queue_id) || PUBLISHING_QUEUE_STATUS.failed;
      const updatedQueue = db.prepare(`SELECT * FROM publishing_queue WHERE id = ?`).get(target.queue_id);
      safeSyncDealStatusWithQueue({
        queueId: target.queue_id,
        queueStatus: normalizePublishingQueueStatus(queueStatus, PUBLISHING_QUEUE_STATUS.failed),
        payload,
        sourceType: queue?.source_type || '',
        sourceId: queue?.source_id ?? null,
        target,
        message: `${target.channel_type} Target fehlgeschlagen.`,
        errorMessage: error instanceof Error ? error.message : 'Worker-Fehler',
        origin,
        meta: {
          dealKey: updatedQueue?.deal_key || ''
        }
      });
    } catch (stateError) {
      safeLogPublishing({
        queueId: target.queue_id,
        targetId: target.id,
        workerType: target.channel_type,
        level: 'error',
        eventType: 'target.state.error',
        message: stateError instanceof Error ? stateError.message : 'Queue-Status konnte nach Fehler nicht aktualisiert werden.',
        payload: {
          originalError: error instanceof Error ? error.message : 'Worker-Fehler'
        }
      });
    }

    return {
      targetId: target.id,
      channelType: target.channel_type,
      status: PUBLISHING_QUEUE_STATUS.failed,
      error: error instanceof Error ? error.message : 'Worker-Fehler'
    };
  }
}

function listRunnableTargets({ channelType = null, queueId = null, limit = 20 } = {}) {
  const now = nowIso();
  const sql = `
    SELECT *
    FROM publishing_targets
    WHERE is_enabled = 1
      AND status IN (@pendingStatus, @retryStatus)
      AND (@channelType = '' OR channel_type = @channelType)
      AND (@queueId = 0 OR queue_id = @queueId)
      AND (
        status = @pendingStatus
        OR EXISTS (
          SELECT 1
          FROM publishing_queue q
          WHERE q.id = publishing_targets.queue_id
            AND (q.next_retry_at IS NULL OR q.next_retry_at <= @now)
        )
      )
    ORDER BY id ASC
    LIMIT @limit
  `;

  return db.prepare(sql).all({
    channelType: cleanText(channelType),
    queueId: Number(queueId || 0),
    now,
    limit: Number(limit || 20),
    pendingStatus: PUBLISHING_QUEUE_STATUS.pending,
    retryStatus: PUBLISHING_QUEUE_STATUS.retry
  });
}

function recoverInterruptedPublishingTargets() {
  const timestamp = nowIso();
  const recoveredTargets = runWithSqliteWriteRetry(() =>
    db
      .prepare(
        `
          UPDATE publishing_targets
          SET status = ?, updated_at = ?
          WHERE status IN (?, ?)
        `
      )
      .run(PUBLISHING_QUEUE_STATUS.retry, timestamp, PUBLISHING_QUEUE_STATUS.sending, 'processing').changes
  );
  const recoveredQueues = runWithSqliteWriteRetry(() =>
    db
      .prepare(
        `
          UPDATE publishing_queue
          SET status = ?,
              next_retry_at = NULL,
              updated_at = ?
          WHERE status IN (?, ?)
        `
      )
      .run(PUBLISHING_QUEUE_STATUS.retry, timestamp, PUBLISHING_QUEUE_STATUS.sending, 'processing').changes
  );

  if (recoveredTargets || recoveredQueues) {
    console.info('QUEUE RECOVERY APPLIED', {
      recoveredQueues,
      recoveredTargets
    });
    safeLogPublishing({
      workerType: 'publisher',
      eventType: 'queue.recovered',
      message: `${recoveredTargets} Target(s) und ${recoveredQueues} Queue(s) nach Neustart auf Retry gesetzt.`,
      payload: {
        recoveredQueues,
        recoveredTargets
      }
    });
  }

  return {
    recoveredQueues,
    recoveredTargets
  };
}

export function recoverPublishingQueueState() {
  return recoverInterruptedPublishingTargets();
}

export async function runPublishingWorkers(channelType = null, options = {}) {
  const targets = listRunnableTargets({
    channelType,
    queueId: options?.queueId || null,
    limit: options?.limit || (channelType ? 10 : 20)
  });

  const results = [];
  for (const target of targets) {
    results.push(await processTarget(target, options?.processors || {}));
  }
  return results;
}

export async function processPublishingQueueEntry(queueId, options = {}) {
  const queue = db.prepare(`SELECT * FROM publishing_queue WHERE id = ?`).get(queueId);
  console.info('[PUBLISHER_TRIGGERED]', {
    queueId,
    sourceType: queue?.source_type || '',
    sourceId: queue?.source_id ?? null,
    status: queue?.status || ''
  });

  const results = [];

  while (true) {
    const batch = listRunnableTargets({
      queueId,
      limit: 20
    });

    if (!batch.length) {
      break;
    }

    for (const target of batch) {
      results.push(await processTarget(target, options?.processors || {}));
    }
  }

  return {
    queue: getPublishingQueueEntry(queueId),
    results
  };
}

async function drainPublishingQueue() {
  if (publishingWorkerLoopRunning) {
    return;
  }

  publishingWorkerLoopRunning = true;

  try {
    const processedTargets = await runPublishingWorkers();

    if (processedTargets.length) {
      console.info('QUEUE JOBS RESUMED', {
        processedTargets: processedTargets.length
      });
    }
  } catch (error) {
    safeLogPublishing({
      workerType: 'publisher',
      level: 'error',
      eventType: 'queue.resume.error',
      message: error instanceof Error ? error.message : 'Publishing Queue konnte nach Neustart nicht fortgesetzt werden.'
    });
  } finally {
    publishingWorkerLoopRunning = false;
  }
}

export function startPublishingWorkerLoop() {
  if (publishingWorkerLoopStarted) {
    return;
  }

  publishingWorkerLoopStarted = true;
  recoverInterruptedPublishingTargets();
  console.info('Queue Worker aktiv', {
    intervalMs: PUBLISHER_LOOP_INTERVAL_MS,
    handles: ['pending', 'retry', 'recovery']
  });

  setInterval(() => {
    void drainPublishingQueue();
  }, PUBLISHER_LOOP_INTERVAL_MS);

  void drainPublishingQueue();
}

export function getPublishingWorkerRuntimeStatus() {
  return {
    started: publishingWorkerLoopStarted,
    running: publishingWorkerLoopRunning,
    intervalMs: PUBLISHER_LOOP_INTERVAL_MS
  };
}

export function retryPublishingQueue(queueId) {
  runWithSqliteWriteRetry(() => {
    db.prepare(
      `
        UPDATE publishing_queue
        SET status = ?,
            next_retry_at = NULL,
            updated_at = ?
        WHERE id = ?
      `
    ).run(PUBLISHING_QUEUE_STATUS.retry, nowIso(), queueId);
    db.prepare(
      `
        UPDATE publishing_targets
        SET status = CASE WHEN status = ? THEN ? ELSE status END,
            updated_at = ?
        WHERE queue_id = ?
      `
    ).run(PUBLISHING_QUEUE_STATUS.failed, PUBLISHING_QUEUE_STATUS.retry, nowIso(), queueId);
  });

  const queueStatus = updateQueueStatus(queueId) || PUBLISHING_QUEUE_STATUS.retry;
  const queue = db.prepare(`SELECT * FROM publishing_queue WHERE id = ?`).get(queueId);
  safeSyncDealStatusWithQueue({
    queueId,
    queueStatus,
    payload: parseJson(queue?.payload_json, {}),
    sourceType: queue?.source_type || '',
    sourceId: queue?.source_id ?? null,
    message: 'Queue manuell auf Retry gesetzt.',
    origin: getPublishingOrigin(queue?.source_type || '')
  });
  return getPublishingQueueEntry(queueId);
}
