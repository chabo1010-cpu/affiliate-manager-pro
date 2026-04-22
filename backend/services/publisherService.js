import { getDb } from '../db.js';
import { assertDealNotLocked, cleanText } from './dealHistoryService.js';
import { syncDealStatusWithQueue } from './databaseService.js';
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

const db = getDb();
const PUBLISHER_LOOP_INTERVAL_MS = 15 * 1000;
let publishingWorkerLoopStarted = false;
let publishingWorkerLoopRunning = false;

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

  if (imageSource === 'link_preview' || imageSource === 'none') {
    return '';
  }

  return '';
}

function buildChannelPayload(payload, channelType) {
  const texts = payload.textByChannel || {};
  const couponCode = cleanText(payload.couponCode);

  return {
    text: texts[channelType] || texts.telegram || payload.title || '',
    imageUrl: getSelectedImage(payload, payload.targetImageSources?.[channelType] || 'none'),
    link: cleanText(payload.link),
    couponCode
  };
}

export function createPublishingEntry({ sourceType, sourceId = null, payload, targets = [], originOverride = '' }) {
  const timestamp = nowIso();
  const origin = getPublishingOrigin(sourceType, originOverride);
  const enrichedPayload = {
    ...(payload && typeof payload === 'object' ? payload : {}),
    sourceId: sourceId ?? payload?.sourceId ?? null,
    databaseSourceType: sourceType,
    databaseOrigin: origin
  };
  const normalizedTargets = expandPublishingTargets(
    enrichedPayload,
    targets.map(normalizeTarget).filter((item) => item.channelType)
  );

  assertDealNotLocked({
    asin: enrichedPayload.asin,
    url: enrichedPayload.link,
    normalizedUrl: enrichedPayload.normalizedUrl || enrichedPayload.link,
    sourceType,
    origin
  });

  const queueResult = db
    .prepare(
      `
        INSERT INTO publishing_queue (
          source_type,
          source_id,
          status,
          payload_json,
          retry_count,
          next_retry_at,
          created_at,
          updated_at
        ) VALUES (?, ?, ?, ?, 0, NULL, ?, ?)
      `
    )
    .run(sourceType, sourceId, PUBLISHING_QUEUE_STATUS.pending, stringifyJson(enrichedPayload), timestamp, timestamp);

  const queueId = queueResult.lastInsertRowid;
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
      targetCount: normalizedTargets.length
    }
  });

  return getPublishingQueueEntry(queueId);
}

export function createGeneratorPublishingEntry(input = {}) {
  const timestamp = nowIso();
  const generatorResult = db
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

export function listPublishingQueue() {
  return db
    .prepare(`SELECT * FROM publishing_queue ORDER BY created_at DESC`)
    .all()
    .map((queue) =>
      mapPublishingQueueRow(
        queue,
        db.prepare(`SELECT * FROM publishing_targets WHERE queue_id = ? ORDER BY id ASC`).all(queue.id)
      )
    );
}

export function listPublishingLogs() {
  return db
    .prepare(`SELECT * FROM publishing_logs ORDER BY created_at DESC LIMIT 200`)
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
  db.prepare(
    `
      UPDATE app_settings
      SET facebookEnabled = ?,
          facebookSessionMode = ?,
          facebookDefaultRetryLimit = ?,
          facebookDefaultTarget = ?
      WHERE id = 1
    `
  ).run(
    input.facebookEnabled ? 1 : 0,
    cleanText(input.facebookSessionMode) || 'persistent',
    Number(input.facebookDefaultRetryLimit ?? 3),
    cleanText(input.facebookDefaultTarget) || null
  );

  return getWorkerStatus().facebook;
}

function updateQueueStatus(queueId) {
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

  db.prepare(`UPDATE publishing_queue SET status = ?, updated_at = ? WHERE id = ?`).run(nextStatus, nowIso(), queueId);
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

function markTargetFailed(target, errorMessage, retry = true, retryLimitOverride = null) {
  const settings = getAppSettings();
  const retryLimit = Number.isFinite(Number(retryLimitOverride))
    ? Number(retryLimitOverride)
    : resolveRetryLimitForTarget(target, settings);
  const queue = db.prepare(`SELECT retry_count FROM publishing_queue WHERE id = ?`).get(target.queue_id);
  const nextRetryCount = Number(queue?.retry_count ?? 0) + 1;
  const canRetry = retry && nextRetryCount <= retryLimit;
  const nextRetryAt = canRetry ? new Date(Date.now() + 15 * 60 * 1000).toISOString() : null;

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
      SET status = ?, retry_count = ?, next_retry_at = ?, updated_at = ?
      WHERE id = ?
    `
  ).run(
    canRetry ? PUBLISHING_QUEUE_STATUS.retry : PUBLISHING_QUEUE_STATUS.failed,
    nextRetryCount,
    nextRetryAt,
    nowIso(),
    target.queue_id
  );

  logPublishing({
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
  db.prepare(
    `
      UPDATE publishing_targets
      SET status = ?,
          posted_at = ?,
          error_message = NULL,
          updated_at = ?
      WHERE id = ?
    `
  ).run(PUBLISHING_QUEUE_STATUS.sent, timestamp, timestamp, target.id);

  logPublishing({
    queueId: target.queue_id,
    targetId: target.id,
    workerType,
    eventType: 'target.sent',
    message: `${workerType} Worker hat den Beitrag erfolgreich verarbeitet.`,
    payload: workerResult
  });
}

async function processTarget(target) {
  const queue = db.prepare(`SELECT * FROM publishing_queue WHERE id = ?`).get(target.queue_id);
  const payload = parseJson(queue?.payload_json, {});
  db.prepare(`UPDATE publishing_targets SET status = ?, updated_at = ? WHERE id = ?`).run(
    PUBLISHING_QUEUE_STATUS.sending,
    nowIso(),
    target.id
  );
  db.prepare(`UPDATE publishing_queue SET status = ?, updated_at = ? WHERE id = ?`).run(
    PUBLISHING_QUEUE_STATUS.sending,
    nowIso(),
    target.queue_id
  );

  try {
    assertDealNotLocked({
      asin: payload.asin,
      url: payload.link,
      normalizedUrl: payload.normalizedUrl || payload.link,
      queueId: target.queue_id,
      sourceType: queue?.source_type || '',
      origin: getPublishingOrigin(queue?.source_type || '')
    });

    let workerResult;
    if (target.channel_type === 'telegram') {
      workerResult = await processTelegramPublishingTarget(target, payload);
    } else if (target.channel_type === 'whatsapp') {
      workerResult = await processWhatsappPublishingTarget(target, payload);
    } else if (target.channel_type === 'facebook') {
      workerResult = await processFacebookPublishingTarget(target, payload);
    } else {
      throw new Error(`Unbekannter Channel ${target.channel_type}`);
    }

    markTargetSent(target, target.channel_type, workerResult);
    updateQueueStatus(target.queue_id);
    const updatedQueue = db.prepare(`SELECT * FROM publishing_queue WHERE id = ?`).get(target.queue_id);
    syncDealStatusWithQueue({
      queueId: target.queue_id,
      queueStatus: normalizePublishingQueueStatus(updatedQueue?.status, PUBLISHING_QUEUE_STATUS.sent),
      payload,
      sourceType: queue?.source_type || '',
      sourceId: queue?.source_id ?? null,
      target,
      message: `${target.channel_type} Target erfolgreich verarbeitet.`,
      origin: getPublishingOrigin(queue?.source_type || ''),
      meta: {
        workerResult
      }
    });
    return {
      targetId: target.id,
      channelType: target.channel_type,
      status: PUBLISHING_QUEUE_STATUS.sent,
      workerResult
    };
  } catch (error) {
    markTargetFailed(
      target,
      error instanceof Error ? error.message : 'Worker-Fehler',
      !(error instanceof Error && error.retryable === false),
      error instanceof Error ? error.retryLimit : null
    );
    updateQueueStatus(target.queue_id);
    const updatedQueue = db.prepare(`SELECT * FROM publishing_queue WHERE id = ?`).get(target.queue_id);
    syncDealStatusWithQueue({
      queueId: target.queue_id,
      queueStatus: normalizePublishingQueueStatus(updatedQueue?.status, PUBLISHING_QUEUE_STATUS.failed),
      payload,
      sourceType: queue?.source_type || '',
      sourceId: queue?.source_id ?? null,
      target,
      message: `${target.channel_type} Target fehlgeschlagen.`,
      errorMessage: error instanceof Error ? error.message : 'Worker-Fehler',
      origin: getPublishingOrigin(queue?.source_type || '')
    });
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
  const recoveredTargets = db
    .prepare(
      `
        UPDATE publishing_targets
        SET status = ?, updated_at = ?
        WHERE status IN (?, ?)
      `
    )
    .run(
      PUBLISHING_QUEUE_STATUS.retry,
      timestamp,
      PUBLISHING_QUEUE_STATUS.sending,
      'processing'
    ).changes;
  const recoveredQueues = db
    .prepare(
      `
        UPDATE publishing_queue
        SET status = ?,
            next_retry_at = NULL,
            updated_at = ?
        WHERE status IN (?, ?)
      `
    )
    .run(
      PUBLISHING_QUEUE_STATUS.retry,
      timestamp,
      PUBLISHING_QUEUE_STATUS.sending,
      'processing'
    ).changes;

  if (recoveredTargets || recoveredQueues) {
    console.info('QUEUE RECOVERY APPLIED', {
      recoveredQueues,
      recoveredTargets
    });
    logPublishing({
      workerType: 'publisher',
      eventType: 'queue.recovered',
      message: `${recoveredTargets} Target(s) und ${recoveredQueues} Queue(s) nach Neustart auf Retry gesetzt.`,
      payload: {
        recoveredQueues,
        recoveredTargets
      }
    });
  }
}

export async function runPublishingWorkers(channelType = null, options = {}) {
  const targets = listRunnableTargets({
    channelType,
    queueId: options?.queueId || null,
    limit: options?.limit || (channelType ? 10 : 20)
  });

  const results = [];
  for (const target of targets) {
    results.push(await processTarget(target));
  }
  return results;
}

export async function processPublishingQueueEntry(queueId) {
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
      results.push(await processTarget(target));
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
    logPublishing({
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
  console.info('QUEUE RESUME ACTIVE', {
    intervalMs: PUBLISHER_LOOP_INTERVAL_MS
  });

  setInterval(() => {
    void drainPublishingQueue();
  }, PUBLISHER_LOOP_INTERVAL_MS);

  void drainPublishingQueue();
}

export function retryPublishingQueue(queueId) {
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
  updateQueueStatus(queueId);
  const queue = db.prepare(`SELECT * FROM publishing_queue WHERE id = ?`).get(queueId);
  syncDealStatusWithQueue({
    queueId,
    queueStatus: PUBLISHING_QUEUE_STATUS.retry,
    payload: parseJson(queue?.payload_json, {}),
    sourceType: queue?.source_type || '',
    sourceId: queue?.source_id ?? null,
    message: 'Queue manuell auf Retry gesetzt.',
    origin: getPublishingOrigin(queue?.source_type || '')
  });
  return getPublishingQueueEntry(queueId);
}
