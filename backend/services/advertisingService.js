import { getDb } from '../db.js';
import { createPublishingEntry, listPublishingLogs, processPublishingQueueEntry } from './publisherService.js';
import { getTelegramBotClientConfig } from './telegramBotClientService.js';
import { getWhatsappClientConfig } from './whatsappClientService.js';
import {
  ADVERTISING_PRIORITY_ORDER,
  formatDateKey,
  getPriorityWeight,
  isSameMinute,
  listOccurrencesForDay,
  listOccurrencesForRange,
  normalizeAdvertisingFrequencyMode,
  normalizeAdvertisingPriority,
  normalizeAdvertisingStatus,
  normalizeDateValue,
  normalizeTimeEntries,
  normalizeWeekdayEntries,
  nowIso,
  parseLocalDate,
  startOfLocalDay
} from './advertisingScheduleService.js';

const db = getDb();
const ADVERTISING_SCHEDULER_INTERVAL_MS = 30 * 1000;
const ADVERTISING_MODULE_DEFAULTS = Array.from({ length: 5 }, (_, index) => ({
  slotNumber: index + 1,
  moduleName: `Werbemodul ${index + 1}`
}));
let advertisingSchedulerStarted = false;
let advertisingSchedulerRunning = false;

function cleanText(value) {
  return typeof value === 'string' ? value.trim() : '';
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

function parseJson(value, fallback) {
  try {
    if (!value) {
      return fallback;
    }

    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function stringifyJson(value) {
  return JSON.stringify(value ?? null);
}

function ensureAdvertisingModuleCatalog() {
  const timestamp = nowIso();
  const startDate = formatDateKey(new Date());

  ADVERTISING_MODULE_DEFAULTS.forEach((item) => {
    db.prepare(
      `
        INSERT OR IGNORE INTO advertising_modules (
          slot_number,
          module_name,
          status,
          priority,
          start_date,
          end_date,
          frequency_mode,
          times_json,
          weekdays_json,
          interval_hours,
          interval_days,
          max_per_day,
          main_text,
          extra_text,
          image_data_url,
          image_filename,
          telegram_enabled,
          telegram_target_ids_json,
          whatsapp_enabled,
          whatsapp_targets_json,
          last_scheduled_at,
          last_success_at,
          last_failure_at,
          last_error,
          created_at,
          updated_at
        ) VALUES (
          @slotNumber,
          @moduleName,
          'paused',
          'medium',
          @startDate,
          NULL,
          'daily',
          '["09:00"]',
          '[]',
          6,
          1,
          1,
          '',
          '',
          NULL,
          NULL,
          1,
          '[]',
          0,
          '[]',
          NULL,
          NULL,
          NULL,
          '',
          @createdAt,
          @updatedAt
        )
      `
    ).run({
      slotNumber: item.slotNumber,
      moduleName: item.moduleName,
      startDate,
      createdAt: timestamp,
      updatedAt: timestamp
    });
  });
}

function normalizeWhatsappTargets(value) {
  if (Array.isArray(value)) {
    return value
      .map((item) => {
        if (!item || typeof item !== 'object') {
          return null;
        }

        const ref = cleanText(item.ref || item.targetRef);
        const label = cleanText(item.label || item.targetLabel);
        const meta = item.meta && typeof item.meta === 'object' ? item.meta : null;
        return ref || label ? { ref, label, meta } : null;
      })
      .filter(Boolean);
  }

  if (typeof value === 'string') {
    return value
      .split(/\n+/)
      .map((line) => cleanText(line))
      .filter(Boolean)
      .map((line) => {
        const [refPart, labelPart] = line.split('|');
        const ref = cleanText(refPart);
        const label = cleanText(labelPart);
        return ref || label ? { ref, label } : null;
      })
      .filter(Boolean);
  }

  return [];
}

function normalizeTelegramTargetIds(value) {
  if (!Array.isArray(value)) {
    return [];
  }

  return Array.from(
    new Set(
      value
        .map((item) => parseInteger(item, 0))
        .filter((item) => item > 0)
    )
  );
}

function mapAdvertisingModuleRow(row = {}) {
  return {
    id: Number(row.id),
    slotNumber: Number(row.slot_number || row.id),
    moduleName: cleanText(row.module_name) || `Werbemodul ${row.slot_number || row.id}`,
    status: normalizeAdvertisingStatus(row.status),
    priority: normalizeAdvertisingPriority(row.priority),
    startDate: cleanText(row.start_date) || '',
    endDate: cleanText(row.end_date) || '',
    frequencyMode: normalizeAdvertisingFrequencyMode(row.frequency_mode),
    times: normalizeTimeEntries(parseJson(row.times_json, ['09:00'])),
    weekdays: normalizeWeekdayEntries(parseJson(row.weekdays_json, [])),
    intervalHours: Math.max(1, parseInteger(row.interval_hours, 6)),
    intervalDays: Math.max(1, parseInteger(row.interval_days, 1)),
    maxPerDay: Math.max(1, parseInteger(row.max_per_day, 1)),
    mainText: row.main_text || '',
    extraText: row.extra_text || '',
    imageDataUrl: row.image_data_url || '',
    imageFilename: row.image_filename || '',
    telegramEnabled: row.telegram_enabled === 1,
    telegramTargetIds: normalizeTelegramTargetIds(parseJson(row.telegram_target_ids_json, [])),
    whatsappEnabled: row.whatsapp_enabled === 1,
    whatsappTargets: normalizeWhatsappTargets(parseJson(row.whatsapp_targets_json, [])),
    lastScheduledAt: row.last_scheduled_at || null,
    lastSuccessAt: row.last_success_at || null,
    lastFailureAt: row.last_failure_at || null,
    lastError: row.last_error || '',
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null,
    priorityWeight: getPriorityWeight(row.priority)
  };
}

function mapAdvertisingJobRow(row = {}) {
  return {
    id: Number(row.id),
    moduleId: Number(row.module_id),
    moduleName: row.module_name || '',
    jobType: row.job_type || 'scheduled',
    scheduledFor: row.scheduled_for || null,
    scheduledDateKey: row.scheduled_date_key || '',
    priority: normalizeAdvertisingPriority(row.priority),
    status: row.status || 'queued',
    queueId: row.queue_id ?? null,
    queueStatus: row.queue_status || '',
    retryCount: parseInteger(row.retry_count, 0),
    nextRetryAt: row.next_retry_at || null,
    lastError: row.last_error || '',
    deliveredChannels: parseJson(row.delivered_channels_json, []),
    targetSnapshot: parseJson(row.target_snapshot_json, []),
    payloadSnapshot: parseJson(row.payload_snapshot_json, null),
    sentAt: row.sent_at || null,
    failedAt: row.failed_at || null,
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null
  };
}

function listAdvertisingModulesInternal() {
  ensureAdvertisingModuleCatalog();
  return db
    .prepare(`SELECT * FROM advertising_modules ORDER BY slot_number ASC, id ASC`)
    .all()
    .map(mapAdvertisingModuleRow);
}

function getAdvertisingModuleRowById(id) {
  ensureAdvertisingModuleCatalog();
  const row = db.prepare(`SELECT * FROM advertising_modules WHERE id = ?`).get(id) || null;
  return row ? mapAdvertisingModuleRow(row) : null;
}

function updateAdvertisingModuleExecution(moduleId, patch = {}) {
  const payload = {
    moduleId,
    lastScheduledAt: patch.lastScheduledAt ?? null,
    lastSuccessAt: patch.lastSuccessAt ?? null,
    lastFailureAt: patch.lastFailureAt ?? null,
    lastError: patch.lastError ?? null,
    updatedAt: nowIso()
  };

  db.prepare(
    `
      UPDATE advertising_modules
      SET last_scheduled_at = COALESCE(@lastScheduledAt, last_scheduled_at),
          last_success_at = CASE WHEN @lastSuccessAt IS NOT NULL THEN @lastSuccessAt ELSE last_success_at END,
          last_failure_at = CASE WHEN @lastFailureAt IS NOT NULL THEN @lastFailureAt ELSE last_failure_at END,
          last_error = CASE
            WHEN @lastError IS NOT NULL THEN @lastError
            ELSE last_error
          END,
          updated_at = @updatedAt
      WHERE id = @moduleId
    `
  ).run(payload);
}

function buildAdvertisingJobDedupeKey(moduleId, scheduledForIso, jobType = 'scheduled') {
  return `${moduleId}:${jobType}:${scheduledForIso}`;
}

function getAdvertisingJobCountForDate(moduleId, dateKey, includeTests = false) {
  const row = db
    .prepare(
      `
        SELECT COUNT(*) AS count
        FROM advertising_jobs
        WHERE module_id = @moduleId
          AND scheduled_date_key = @dateKey
          AND (@includeTests = 1 OR job_type != 'test')
      `
    )
    .get({
      moduleId,
      dateKey,
      includeTests: includeTests ? 1 : 0
    });

  return parseInteger(row?.count, 0);
}

function listAdvertisingJobsInternal(limit = 40) {
  return db
    .prepare(`SELECT * FROM advertising_jobs ORDER BY scheduled_for DESC, id DESC LIMIT ?`)
    .all(limit)
    .map(mapAdvertisingJobRow);
}

function getAdvertisingJobById(id) {
  const row = db.prepare(`SELECT * FROM advertising_jobs WHERE id = ?`).get(id) || null;
  return row ? mapAdvertisingJobRow(row) : null;
}

function getAdvertisingJobByDedupeKey(dedupeKey) {
  const row = db.prepare(`SELECT * FROM advertising_jobs WHERE dedupe_key = ?`).get(dedupeKey) || null;
  return row ? mapAdvertisingJobRow(row) : null;
}

function insertAdvertisingJob(module, scheduledForIso, jobType = 'scheduled') {
  const timestamp = nowIso();
  const dedupeKey = buildAdvertisingJobDedupeKey(module.id, scheduledForIso, jobType);
  const existing = getAdvertisingJobByDedupeKey(dedupeKey);
  if (existing) {
    return existing;
  }

  const result = db
    .prepare(
      `
        INSERT INTO advertising_jobs (
          module_id,
          module_name,
          job_type,
          dedupe_key,
          scheduled_for,
          scheduled_date_key,
          priority,
          status,
          queue_id,
          queue_status,
          retry_count,
          next_retry_at,
          last_error,
          delivered_channels_json,
          target_snapshot_json,
          payload_snapshot_json,
          sent_at,
          failed_at,
          created_at,
          updated_at
        ) VALUES (
          @moduleId,
          @moduleName,
          @jobType,
          @dedupeKey,
          @scheduledFor,
          @scheduledDateKey,
          @priority,
          'creating',
          NULL,
          '',
          0,
          NULL,
          '',
          '[]',
          '[]',
          NULL,
          NULL,
          NULL,
          @createdAt,
          @updatedAt
        )
      `
    )
    .run({
      moduleId: module.id,
      moduleName: module.moduleName,
      jobType,
      dedupeKey,
      scheduledFor: scheduledForIso,
      scheduledDateKey: formatDateKey(scheduledForIso),
      priority: module.priority,
      createdAt: timestamp,
      updatedAt: timestamp
    });

  return getAdvertisingJobById(result.lastInsertRowid);
}

function updateAdvertisingJob(jobId, patch = {}) {
  const existing = getAdvertisingJobById(jobId);
  if (!existing) {
    return null;
  }

  const payload = {
    id: jobId,
    status: cleanText(patch.status) || existing.status,
    queueId: patch.queueId === undefined ? existing.queueId : patch.queueId,
    queueStatus: patch.queueStatus === undefined ? existing.queueStatus : cleanText(patch.queueStatus),
    retryCount: patch.retryCount === undefined ? existing.retryCount : parseInteger(patch.retryCount, existing.retryCount),
    nextRetryAt: patch.nextRetryAt === undefined ? existing.nextRetryAt : patch.nextRetryAt,
    lastError: patch.lastError === undefined ? existing.lastError : cleanText(patch.lastError),
    deliveredChannelsJson:
      patch.deliveredChannels === undefined ? stringifyJson(existing.deliveredChannels) : stringifyJson(patch.deliveredChannels),
    targetSnapshotJson:
      patch.targetSnapshot === undefined ? stringifyJson(existing.targetSnapshot) : stringifyJson(patch.targetSnapshot),
    payloadSnapshotJson:
      patch.payloadSnapshot === undefined ? stringifyJson(existing.payloadSnapshot) : stringifyJson(patch.payloadSnapshot),
    sentAt: patch.sentAt === undefined ? existing.sentAt : patch.sentAt,
    failedAt: patch.failedAt === undefined ? existing.failedAt : patch.failedAt,
    updatedAt: nowIso()
  };

  db.prepare(
    `
      UPDATE advertising_jobs
      SET status = @status,
          queue_id = @queueId,
          queue_status = @queueStatus,
          retry_count = @retryCount,
          next_retry_at = @nextRetryAt,
          last_error = @lastError,
          delivered_channels_json = @deliveredChannelsJson,
          target_snapshot_json = @targetSnapshotJson,
          payload_snapshot_json = @payloadSnapshotJson,
          sent_at = @sentAt,
          failed_at = @failedAt,
          updated_at = @updatedAt
      WHERE id = @id
    `
  ).run(payload);

  return getAdvertisingJobById(jobId);
}

function buildAdvertisingText(module) {
  return [module.mainText, module.extraText].map((item) => cleanText(item)).filter(Boolean).join('\n\n');
}

function buildAdvertisingTargets(module) {
  const targets = [];
  const imageSource = module.imageDataUrl ? 'upload' : 'none';

  if (module.telegramEnabled) {
    targets.push({
      channelType: 'telegram',
      isEnabled: true,
      imageSource
    });
  }

  if (module.whatsappEnabled) {
    if (module.whatsappTargets.length) {
      module.whatsappTargets.forEach((target) => {
        targets.push({
          channelType: 'whatsapp',
          isEnabled: true,
          imageSource,
          targetRef: target.ref || '',
          targetLabel: target.label || module.moduleName,
          targetMeta: target.meta || null
        });
      });
    } else {
      targets.push({
        channelType: 'whatsapp',
        isEnabled: true,
        imageSource,
        targetLabel: module.moduleName
      });
    }
  }

  return targets;
}

function buildAdvertisingPayload(module, job, scheduledForIso) {
  const text = buildAdvertisingText(module);
  return {
    title: module.moduleName,
    link: '',
    normalizedUrl: '',
    asin: '',
    sellerType: 'FBM',
    currentPrice: '',
    oldPrice: '',
    couponCode: '',
    textByChannel: {
      telegram: text || module.moduleName,
      whatsapp: text || module.moduleName
    },
    telegramTargetIds: module.telegramTargetIds,
    imageVariants: {
      upload: module.imageDataUrl || '',
      standard: ''
    },
    targetImageSources: {
      telegram: module.imageDataUrl ? 'upload' : 'none',
      whatsapp: module.imageDataUrl ? 'upload' : 'none'
    },
    skipDealLock: true,
    skipPostedDealHistory: true,
    advertisingModuleId: module.id,
    advertisingJobId: job.id,
    advertisingScheduledFor: scheduledForIso,
    advertisingPriority: module.priority,
    databaseSourceType: 'advertising',
    databaseOrigin: 'automatic'
  };
}

function createAdvertisingQueueJob(module, scheduledForIso, options = {}) {
  const jobType = options.jobType || 'scheduled';
  let job = insertAdvertisingJob(module, scheduledForIso, jobType);
  const targets = buildAdvertisingTargets(module);
  const payload = buildAdvertisingPayload(module, job, scheduledForIso);

  try {
    const queue = createPublishingEntry({
      sourceType: 'advertising',
      sourceId: job.id,
      payload,
      targets,
      originOverride: 'automatic',
      skipDealLock: true
    });

    job = updateAdvertisingJob(job.id, {
      status: 'queued',
      queueId: queue?.id ?? null,
      queueStatus: queue?.status || 'pending',
      targetSnapshot: queue?.targets || targets,
      payloadSnapshot: payload
    });
    updateAdvertisingModuleExecution(module.id, {
      lastScheduledAt: scheduledForIso,
      lastError: ''
    });

    return job;
  } catch (error) {
    job = updateAdvertisingJob(job.id, {
      status: 'failed',
      lastError: error instanceof Error ? error.message : 'Werbejob konnte nicht in die Queue geschrieben werden.',
      failedAt: nowIso(),
      targetSnapshot: targets,
      payloadSnapshot: payload
    });
    updateAdvertisingModuleExecution(module.id, {
      lastFailureAt: nowIso(),
      lastError: error instanceof Error ? error.message : 'Werbejob konnte nicht in die Queue geschrieben werden.'
    });
    throw error;
  }
}

function mapQueueStatusToAdvertisingStatus(queueStatus) {
  const normalized = cleanText(queueStatus).toLowerCase();
  if (normalized === 'sent') {
    return 'sent';
  }

  if (normalized === 'failed') {
    return 'failed';
  }

  if (normalized === 'retry') {
    return 'retry';
  }

  if (normalized === 'sending') {
    return 'sending';
  }

  return 'queued';
}

function readQueueSnapshot(queueId) {
  if (!queueId) {
    return null;
  }

  const queue = db.prepare(`SELECT * FROM publishing_queue WHERE id = ?`).get(queueId);
  if (!queue) {
    return null;
  }

  const targets = db.prepare(`SELECT * FROM publishing_targets WHERE queue_id = ? ORDER BY id ASC`).all(queueId);
  return {
    queue,
    targets
  };
}

export function syncAdvertisingJobsFromQueue() {
  const jobs = db
    .prepare(`SELECT * FROM advertising_jobs WHERE queue_id IS NOT NULL ORDER BY updated_at DESC LIMIT 300`)
    .all()
    .map(mapAdvertisingJobRow);

  jobs.forEach((job) => {
    const snapshot = readQueueSnapshot(job.queueId);
    if (!snapshot) {
      return;
    }

    const queueStatus = cleanText(snapshot.queue.status).toLowerCase();
    const nextStatus = mapQueueStatusToAdvertisingStatus(queueStatus);
    const retryCount = parseInteger(snapshot.queue.retry_count, 0);
    const nextRetryAt = snapshot.queue.next_retry_at || null;
    const deliveredChannels = snapshot.targets
      .filter((target) => cleanText(target.status).toLowerCase() === 'sent')
      .map((target) => `${target.channel_type}${target.target_label ? `:${target.target_label}` : ''}`);
    const failedTarget = snapshot.targets.find((target) => ['failed', 'retry'].includes(cleanText(target.status).toLowerCase()));
    const lastError = cleanText(failedTarget?.error_message) || job.lastError || '';
    const patch = {
      status: nextStatus,
      queueStatus: snapshot.queue.status,
      retryCount,
      nextRetryAt,
      lastError,
      deliveredChannels,
      targetSnapshot: snapshot.targets
    };

    if (nextStatus === 'sent' && !job.sentAt) {
      patch.sentAt = snapshot.queue.updated_at || nowIso();
      updateAdvertisingModuleExecution(job.moduleId, {
        lastSuccessAt: patch.sentAt,
        lastError: ''
      });
    }

    if (nextStatus === 'failed' && !job.failedAt) {
      patch.failedAt = snapshot.queue.updated_at || nowIso();
      updateAdvertisingModuleExecution(job.moduleId, {
        lastFailureAt: patch.failedAt,
        lastError
      });
    }

    if (nextStatus === 'retry') {
      updateAdvertisingModuleExecution(job.moduleId, {
        lastError
      });
    }

    updateAdvertisingJob(job.id, patch);
  });

  return listAdvertisingJobsInternal(40);
}

function listActiveAdvertisingModules() {
  return listAdvertisingModulesInternal().filter((module) => module.status === 'active');
}

function buildScheduledUsageMap(jobs = []) {
  const usageMap = new Map();

  jobs.forEach((job) => {
    const key = `${job.moduleId}:${job.scheduledDateKey}`;
    const currentCount = usageMap.get(key) || 0;
    if (job.jobType !== 'test') {
      usageMap.set(key, currentCount + 1);
    }
  });

  return usageMap;
}

function listDueAdvertisingOccurrences(modules, currentDate = new Date()) {
  const minuteDate = new Date(currentDate);
  minuteDate.setSeconds(0, 0);

  return modules
    .flatMap((module) =>
      listOccurrencesForDay(module, minuteDate).map((scheduledFor) => ({
        module,
        scheduledFor
      }))
    )
    .filter((entry) => isSameMinute(entry.scheduledFor, minuteDate))
    .sort((left, right) => {
      if (left.scheduledFor.getTime() !== right.scheduledFor.getTime()) {
        return left.scheduledFor.getTime() - right.scheduledFor.getTime();
      }

      if (left.module.priorityWeight !== right.module.priorityWeight) {
        return right.module.priorityWeight - left.module.priorityWeight;
      }

      return left.module.slotNumber - right.module.slotNumber;
    });
}

export function runAdvertisingSchedulerTick() {
  syncAdvertisingJobsFromQueue();
  const modules = listActiveAdvertisingModules();
  const dueOccurrences = listDueAdvertisingOccurrences(modules, new Date());

  dueOccurrences.forEach(({ module, scheduledFor }) => {
    try {
      const dateKey = formatDateKey(scheduledFor);
      const scheduledCount = getAdvertisingJobCountForDate(module.id, dateKey, false);
      if (scheduledCount >= module.maxPerDay) {
        return;
      }

      const scheduledForIso = scheduledFor.toISOString();
      createAdvertisingQueueJob(module, scheduledForIso, {
        jobType: 'scheduled'
      });
    } catch (error) {
      updateAdvertisingModuleExecution(module.id, {
        lastFailureAt: nowIso(),
        lastError: error instanceof Error ? error.message : 'Scheduler-Fehler im Werbemodul.'
      });
    }
  });
}

export function startAdvertisingScheduler() {
  if (advertisingSchedulerStarted) {
    return;
  }

  advertisingSchedulerStarted = true;
  console.info('Werbemodule aktiv', {
    intervalMs: ADVERTISING_SCHEDULER_INTERVAL_MS,
    scope: 'advertising'
  });
  setInterval(() => {
    if (advertisingSchedulerRunning) {
      return;
    }

    advertisingSchedulerRunning = true;
    try {
      runAdvertisingSchedulerTick();
    } finally {
      advertisingSchedulerRunning = false;
    }
  }, ADVERTISING_SCHEDULER_INTERVAL_MS);

  runAdvertisingSchedulerTick();
}

export function getAdvertisingSchedulerRuntimeStatus() {
  return {
    started: advertisingSchedulerStarted,
    running: advertisingSchedulerRunning,
    intervalMs: ADVERTISING_SCHEDULER_INTERVAL_MS
  };
}

export function getAdvertisingChannelCatalog() {
  const telegram = getTelegramBotClientConfig();
  const whatsapp = getWhatsappClientConfig();

  return {
    telegram: {
      enabled: telegram.enabled,
      tokenConfigured: telegram.tokenConfigured,
      fallbackChatConfigured: telegram.fallbackChatConfigured,
      targets: telegram.targets.filter((target) => target.isActive),
      effectiveTargets: telegram.effectiveTargets
    },
    whatsapp: {
      enabled: whatsapp.enabled,
      endpointConfigured: whatsapp.providerConfigured,
      senderConfigured: whatsapp.senderConfigured,
      sender: whatsapp.sender,
      providerMode: whatsapp.providerMode,
      retryLimit: whatsapp.retryLimit
    }
  };
}

export function getAdvertisingModules() {
  return listAdvertisingModulesInternal();
}

export function saveAdvertisingModule(input = {}, moduleId) {
  const existing = getAdvertisingModuleRowById(moduleId);
  if (!existing) {
    throw new Error('Werbemodul nicht gefunden.');
  }

  const nextStatus = normalizeAdvertisingStatus(input.status ?? existing.status);
  const nextPriority = normalizeAdvertisingPriority(input.priority ?? existing.priority);
  const nextStartDate = normalizeDateValue(input.startDate ?? existing.startDate) || existing.startDate;
  const nextEndDate = normalizeDateValue(input.endDate ?? existing.endDate);
  const nextFrequencyMode = normalizeAdvertisingFrequencyMode(input.frequencyMode ?? existing.frequencyMode);
  const nextTimes = normalizeTimeEntries(input.times ?? existing.times, existing.times);
  const nextWeekdays = normalizeWeekdayEntries(input.weekdays ?? existing.weekdays, existing.weekdays);
  const nextIntervalHours = Math.max(1, parseInteger(input.intervalHours ?? existing.intervalHours, existing.intervalHours));
  const nextIntervalDays = Math.max(1, parseInteger(input.intervalDays ?? existing.intervalDays, existing.intervalDays));
  const nextMaxPerDay = Math.max(1, parseInteger(input.maxPerDay ?? existing.maxPerDay, existing.maxPerDay));
  const nextTelegramEnabled =
    input.telegramEnabled === undefined ? existing.telegramEnabled : parseBool(input.telegramEnabled, existing.telegramEnabled);
  const nextWhatsappEnabled =
    input.whatsappEnabled === undefined ? existing.whatsappEnabled : parseBool(input.whatsappEnabled, existing.whatsappEnabled);
  const nextTelegramTargetIds =
    input.telegramTargetIds === undefined ? existing.telegramTargetIds : normalizeTelegramTargetIds(input.telegramTargetIds);
  const nextWhatsappTargets =
    input.whatsappTargets === undefined ? existing.whatsappTargets : normalizeWhatsappTargets(input.whatsappTargets);
  const nextImageDataUrl = input.imageDataUrl === undefined ? existing.imageDataUrl : cleanText(input.imageDataUrl);
  const nextImageFilename = input.imageFilename === undefined ? existing.imageFilename : cleanText(input.imageFilename);

  db.prepare(
    `
      UPDATE advertising_modules
      SET module_name = @moduleName,
          status = @status,
          priority = @priority,
          start_date = @startDate,
          end_date = @endDate,
          frequency_mode = @frequencyMode,
          times_json = @timesJson,
          weekdays_json = @weekdaysJson,
          interval_hours = @intervalHours,
          interval_days = @intervalDays,
          max_per_day = @maxPerDay,
          main_text = @mainText,
          extra_text = @extraText,
          image_data_url = @imageDataUrl,
          image_filename = @imageFilename,
          telegram_enabled = @telegramEnabled,
          telegram_target_ids_json = @telegramTargetIdsJson,
          whatsapp_enabled = @whatsappEnabled,
          whatsapp_targets_json = @whatsappTargetsJson,
          updated_at = @updatedAt
      WHERE id = @id
    `
  ).run({
    id: moduleId,
    moduleName: cleanText(input.moduleName || input.name) || existing.moduleName,
    status: nextStatus,
    priority: nextPriority,
    startDate: nextStartDate,
    endDate: nextEndDate || null,
    frequencyMode: nextFrequencyMode,
    timesJson: stringifyJson(nextTimes),
    weekdaysJson: stringifyJson(nextWeekdays),
    intervalHours: nextIntervalHours,
    intervalDays: nextIntervalDays,
    maxPerDay: nextMaxPerDay,
    mainText: input.mainText === undefined ? existing.mainText : String(input.mainText ?? ''),
    extraText: input.extraText === undefined ? existing.extraText : String(input.extraText ?? ''),
    imageDataUrl: nextImageDataUrl || null,
    imageFilename: nextImageFilename || null,
    telegramEnabled: nextTelegramEnabled ? 1 : 0,
    telegramTargetIdsJson: stringifyJson(nextTelegramTargetIds),
    whatsappEnabled: nextWhatsappEnabled ? 1 : 0,
    whatsappTargetsJson: stringifyJson(nextWhatsappTargets),
    updatedAt: nowIso()
  });

  return getAdvertisingModuleRowById(moduleId);
}

export function pauseAdvertisingModule(moduleId, paused = true) {
  const module = getAdvertisingModuleRowById(moduleId);
  if (!module) {
    throw new Error('Werbemodul nicht gefunden.');
  }

  db.prepare(`UPDATE advertising_modules SET status = ?, updated_at = ? WHERE id = ?`).run(
    paused ? 'paused' : 'active',
    nowIso(),
    moduleId
  );

  return getAdvertisingModuleRowById(moduleId);
}

export async function triggerAdvertisingModuleTest(moduleId) {
  const module = getAdvertisingModuleRowById(moduleId);
  if (!module) {
    throw new Error('Werbemodul nicht gefunden.');
  }

  const scheduledForIso = new Date().toISOString();
  const job = createAdvertisingQueueJob(module, scheduledForIso, {
    jobType: 'test'
  });

  if (job?.queueId) {
    try {
      await processPublishingQueueEntry(job.queueId);
    } catch (error) {
      updateAdvertisingJob(job.id, {
        status: 'failed',
        lastError: error instanceof Error ? error.message : 'Testsendung fehlgeschlagen.',
        failedAt: nowIso()
      });
      updateAdvertisingModuleExecution(module.id, {
        lastFailureAt: nowIso(),
        lastError: error instanceof Error ? error.message : 'Testsendung fehlgeschlagen.'
      });
    }
  }

  syncAdvertisingJobsFromQueue();
  return getAdvertisingJobById(job.id);
}

export function getAdvertisingDashboard() {
  syncAdvertisingJobsFromQueue();
  const modules = listAdvertisingModulesInternal();
  const jobs = listAdvertisingJobsInternal(40);
  const activeModules = modules.filter((module) => module.status === 'active');
  const usageMap = buildScheduledUsageMap(jobs);
  const allOccurrences = listOccurrencesForRange(activeModules, new Date(), 21);
  const upcoming = [];

  for (const occurrence of allOccurrences) {
    const key = `${occurrence.moduleId}:${occurrence.scheduledDateKey}`;
    const module = modules.find((item) => item.id === occurrence.moduleId);
    const usedSlots = usageMap.get(key) || 0;
    if (!module || usedSlots >= module.maxPerDay) {
      continue;
    }

    usageMap.set(key, usedSlots + 1);
    upcoming.push({
      moduleId: occurrence.moduleId,
      moduleName: occurrence.moduleName,
      priority: occurrence.priority,
      scheduledFor: occurrence.scheduledFor.toISOString(),
      scheduledDateKey: occurrence.scheduledDateKey
    });

    if (upcoming.length >= 20) {
      break;
    }
  }

  const todayKey = formatDateKey(new Date());
  const lastSuccess = [...jobs].find((job) => job.status === 'sent') || null;
  const lastFailure = [...jobs].find((job) => ['failed', 'retry'].includes(job.status)) || null;
  const nextPlanned = upcoming[0] || null;
  const publishingLogs = listPublishingLogs()
    .filter((entry) => entry.payload?.sourceType === 'advertising' || entry.payload?.databaseSourceType === 'advertising')
    .slice(0, 10);

  return {
    overview: {
      activeModuleCount: activeModules.length,
      plannedTodayCount: upcoming.filter((item) => item.scheduledDateKey === todayKey).length,
      nextPlannedPost: nextPlanned,
      lastSuccess,
      lastFailure
    },
    modules,
    upcoming,
    history: jobs,
    logs: publishingLogs,
    channelCatalog: getAdvertisingChannelCatalog(),
    publishing: {
      queueCount: jobs.filter((job) => ['queued', 'retry', 'sending'].includes(job.status)).length,
      failedCount: jobs.filter((job) => job.status === 'failed').length,
      sentCount: jobs.filter((job) => job.status === 'sent').length
    }
  };
}

export function listAdvertisingHistory(limit = 30) {
  syncAdvertisingJobsFromQueue();
  return {
    items: listAdvertisingJobsInternal(limit)
  };
}

export function runAdvertisingAdminSync() {
  runAdvertisingSchedulerTick();
  return getAdvertisingDashboard();
}
