import { getDb } from '../db.js';
import { cleanText } from './dealHistoryService.js';
import { processTelegramPublishingTarget } from './telegramWorkerService.js';
import { processWhatsappPublishingTarget } from './whatsappWorkerService.js';
import { processFacebookPublishingTarget } from './facebookWorkerService.js';

const db = getDb();

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
    imageSource: cleanText(target.imageSource || target.image_source).toLowerCase() || 'none'
  };
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

export function createPublishingEntry({ sourceType, sourceId = null, payload, targets = [] }) {
  const timestamp = nowIso();
  const normalizedTargets = targets.map(normalizeTarget).filter((item) => item.channelType);

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
        ) VALUES (?, ?, 'queued', ?, 0, NULL, ?, ?)
      `
    )
    .run(sourceType, sourceId, stringifyJson(payload), timestamp, timestamp);

  const queueId = queueResult.lastInsertRowid;
  const targetStatement = db.prepare(
    `
      INSERT INTO publishing_targets (
        queue_id,
        channel_type,
        is_enabled,
        image_source,
        status,
        posted_at,
        error_message,
        created_at,
        updated_at
      ) VALUES (?, ?, ?, ?, 'pending', NULL, NULL, ?, ?)
    `
  );

  normalizedTargets.forEach((target) => {
    targetStatement.run(queueId, target.channelType, target.isEnabled ? 1 : 0, target.imageSource, timestamp, timestamp);
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
  return {
    ...queue,
    payload: parseJson(queue.payload_json, {}),
    targets
  };
}

export function listPublishingQueue() {
  return db
    .prepare(`SELECT * FROM publishing_queue ORDER BY created_at DESC`)
    .all()
    .map((queue) => ({
      ...queue,
      payload: parseJson(queue.payload_json, {}),
      targets: db.prepare(`SELECT * FROM publishing_targets WHERE queue_id = ? ORDER BY id ASC`).all(queue.id)
    }));
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
          SUM(CASE WHEN status IN ('pending', 'retry') THEN 1 ELSE 0 END) AS waiting,
          SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) AS processing,
          SUM(CASE WHEN status = 'posted' THEN 1 ELSE 0 END) AS posted,
          SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed
        FROM publishing_targets
        GROUP BY channel_type
      `
    )
    .all();
  const settings = getAppSettings();

  return {
    channels: stats,
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
  const targets = db.prepare(`SELECT status FROM publishing_targets WHERE queue_id = ?`).all(queueId);
  const nextStatus = targets.every((item) => item.status === 'posted')
    ? 'posted'
    : targets.some((item) => item.status === 'processing')
      ? 'processing'
      : targets.some((item) => item.status === 'failed')
        ? 'failed'
        : targets.some((item) => item.status === 'retry')
          ? 'retry'
          : 'queued';

  db.prepare(`UPDATE publishing_queue SET status = ?, updated_at = ? WHERE id = ?`).run(nextStatus, nowIso(), queueId);
}

function markTargetFailed(target, errorMessage, retry = true) {
  const settings = getAppSettings();
  const retryLimit = Number(settings?.facebookDefaultRetryLimit ?? 3);
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
  ).run(canRetry ? 'retry' : 'failed', errorMessage, nowIso(), target.id);

  db.prepare(
    `
      UPDATE publishing_queue
      SET status = ?, retry_count = ?, next_retry_at = ?, updated_at = ?
      WHERE id = ?
    `
  ).run(canRetry ? 'retry' : 'failed', nextRetryCount, nextRetryAt, nowIso(), target.queue_id);

  logPublishing({
    queueId: target.queue_id,
    targetId: target.id,
    workerType: target.channel_type,
    level: 'warning',
    eventType: 'target.failed',
    message: errorMessage,
    payload: {
      retryScheduled: canRetry,
      nextRetryAt
    }
  });
}

function markTargetPosted(target, workerType, workerResult = {}) {
  const timestamp = nowIso();
  db.prepare(
    `
      UPDATE publishing_targets
      SET status = 'posted',
          posted_at = ?,
          error_message = NULL,
          updated_at = ?
      WHERE id = ?
    `
  ).run(timestamp, timestamp, target.id);

  logPublishing({
    queueId: target.queue_id,
    targetId: target.id,
    workerType,
    eventType: 'target.posted',
    message: `${workerType} Worker hat den Beitrag erfolgreich verarbeitet.`,
    payload: workerResult
  });
}

async function processTarget(target) {
  const queue = db.prepare(`SELECT * FROM publishing_queue WHERE id = ?`).get(target.queue_id);
  const payload = parseJson(queue?.payload_json, {});
  db.prepare(`UPDATE publishing_targets SET status = 'processing', updated_at = ? WHERE id = ?`).run(nowIso(), target.id);
  db.prepare(`UPDATE publishing_queue SET status = 'processing', updated_at = ? WHERE id = ?`).run(nowIso(), target.queue_id);

  try {
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

    markTargetPosted(target, target.channel_type, workerResult);
    updateQueueStatus(target.queue_id);
    return {
      targetId: target.id,
      channelType: target.channel_type,
      status: 'posted',
      workerResult
    };
  } catch (error) {
    markTargetFailed(target, error instanceof Error ? error.message : 'Worker-Fehler');
    updateQueueStatus(target.queue_id);
    return {
      targetId: target.id,
      channelType: target.channel_type,
      status: 'failed',
      error: error instanceof Error ? error.message : 'Worker-Fehler'
    };
  }
}

export async function runPublishingWorkers(channelType = null) {
  const now = nowIso();
  const targets = channelType
    ? db
        .prepare(
          `
            SELECT *
            FROM publishing_targets
            WHERE channel_type = ?
              AND is_enabled = 1
              AND status IN ('pending', 'retry')
              AND (
                status = 'pending'
                OR EXISTS (
                  SELECT 1
                  FROM publishing_queue q
                  WHERE q.id = publishing_targets.queue_id
                    AND (q.next_retry_at IS NULL OR q.next_retry_at <= ?)
                )
              )
            ORDER BY id ASC
            LIMIT 10
          `
        )
        .all(channelType, now)
    : db
        .prepare(
          `
            SELECT *
            FROM publishing_targets
            WHERE is_enabled = 1
              AND status IN ('pending', 'retry')
              AND (
                status = 'pending'
                OR EXISTS (
                  SELECT 1
                  FROM publishing_queue q
                  WHERE q.id = publishing_targets.queue_id
                    AND (q.next_retry_at IS NULL OR q.next_retry_at <= ?)
                )
              )
            ORDER BY id ASC
            LIMIT 20
          `
        )
        .all(now);

  const results = [];
  for (const target of targets) {
    results.push(await processTarget(target));
  }
  return results;
}

export function retryPublishingQueue(queueId) {
  db.prepare(
    `
      UPDATE publishing_queue
      SET status = 'retry',
          next_retry_at = NULL,
          updated_at = ?
      WHERE id = ?
    `
  ).run(nowIso(), queueId);
  db.prepare(
    `
      UPDATE publishing_targets
      SET status = CASE WHEN status = 'failed' THEN 'retry' ELSE status END,
          updated_at = ?
      WHERE queue_id = ?
    `
  ).run(nowIso(), queueId);
  updateQueueStatus(queueId);
  return getPublishingQueueEntry(queueId);
}
