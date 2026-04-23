import { getDb } from '../db.js';
import { isSentPublishingQueueStatus, normalizePublishingQueueStatus } from './publishingQueueStateService.js';

const db = getDb();

export const CENTRAL_DATABASE_COLLECTIONS = {
  sessions: ['app_sessions', 'telegram_reader_sessions', 'telegram_reader_channels'],
  sperrmodul: ['deals_history'],
  queue: ['publishing_queue', 'publishing_targets'],
  advertising: ['advertising_modules', 'advertising_jobs'],
  logs: ['publishing_logs', 'copybot_logs', 'keepa_usage_logs', 'amazon_api_logs'],
  dealStatus: ['deal_status_registry']
};

function cleanText(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function nowIso() {
  return new Date().toISOString();
}

function parseInteger(value, fallback = 0) {
  const parsed = Number.parseInt(String(value ?? ''), 10);
  return Number.isFinite(parsed) ? parsed : fallback;
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

function stringifyJson(value) {
  return JSON.stringify(value ?? null);
}

function normalizeSellerType(value) {
  const normalized = cleanText(value).toUpperCase();
  return ['AMAZON', 'FBA', 'FBM'].includes(normalized) ? normalized : 'FBM';
}

function normalizeOrigin(value) {
  const normalized = cleanText(value).toLowerCase();

  if (['manual', 'generator', 'generator_direct', 'direct', 'manual_post'].includes(normalized)) {
    return 'manual';
  }

  if (
    [
      'automatic',
      'auto',
      'copybot',
      'scrapper',
      'publisher_queue',
      'queue',
      'keepa_auto',
      'keepa',
      'automation'
    ].includes(normalized)
  ) {
    return 'automatic';
  }

  return normalized || 'system';
}

function buildSessionKey(input = {}) {
  const explicitKey = cleanText(input.sessionKey);
  if (explicitKey) {
    return explicitKey;
  }

  const moduleName = cleanText(input.module) || 'app';
  const sessionType = cleanText(input.sessionType) || 'runtime';
  const externalRef = cleanText(input.externalRef || input.name || input.storagePath) || 'default';

  return `${moduleName}:${sessionType}:${externalRef}`;
}

export function buildDealStatusKey(input = {}) {
  const explicitKey = cleanText(input.dealKey || input.fallbackKey);
  if (explicitKey) {
    return explicitKey;
  }

  const asin = cleanText(input.asin).toUpperCase();
  const normalizedUrl = cleanText(input.normalizedUrl || input.normalized_url);
  const originalUrl = cleanText(input.originalUrl || input.original_url || input.url);
  const queueId = parseInteger(input.queueId, 0);

  if (asin) {
    return `asin:${asin}`;
  }

  if (normalizedUrl) {
    return `url:${normalizedUrl}`;
  }

  if (originalUrl) {
    return `url:${originalUrl}`;
  }

  if (queueId > 0) {
    return `queue:${queueId}`;
  }

  return '';
}

export function buildPublishingChannelLabel(channelType = '', targetLabel = '') {
  const normalizedChannel = cleanText(channelType).toUpperCase();
  const normalizedTargetLabel = cleanText(targetLabel);

  if (!normalizedChannel) {
    return normalizedTargetLabel;
  }

  return normalizedTargetLabel ? `${normalizedChannel}:${normalizedTargetLabel}` : normalizedChannel;
}

function readAppSessionByKey(sessionKey) {
  return db.prepare(`SELECT * FROM app_sessions WHERE session_key = ? LIMIT 1`).get(sessionKey) || null;
}

function mapAppSessionRow(row) {
  if (!row) {
    return null;
  }

  return {
    id: row.id,
    sessionKey: row.session_key,
    module: row.module,
    sessionType: row.session_type,
    status: row.status,
    storagePath: row.storage_path || '',
    externalRef: row.external_ref || '',
    meta: parseJson(row.meta_json, null),
    lastSeenAt: row.last_seen_at || null,
    createdAt: row.created_at,
    updatedAt: row.updated_at
  };
}

function readDealStatusRowByKey(dealKey) {
  return db.prepare(`SELECT * FROM deal_status_registry WHERE deal_key = ? LIMIT 1`).get(dealKey) || null;
}

function mapDealStatusRow(row) {
  if (!row) {
    return null;
  }

  return {
    id: row.id,
    dealKey: row.deal_key,
    asin: row.asin || '',
    normalizedUrl: row.normalized_url || '',
    originalUrl: row.original_url || '',
    title: row.title || '',
    sellerType: row.seller_type || 'FBM',
    sourceType: row.source_type || '',
    sourceId: row.source_id || '',
    status: row.status,
    decisionReason: row.decision_reason || '',
    queueId: row.queue_id || null,
    lastQueueStatus: row.last_queue_status || '',
    lastChannel: row.last_channel || '',
    postedChannels: parseJson(row.posted_channels_json, []),
    lastPostedAt: row.last_posted_at || null,
    lastError: row.last_error || '',
    manualPostCount: parseInteger(row.manual_post_count, 0),
    automaticPostCount: parseInteger(row.automatic_post_count, 0),
    lastOrigin: row.last_origin || '',
    meta: parseJson(row.meta_json, null),
    createdAt: row.created_at,
    updatedAt: row.updated_at
  };
}

export function upsertAppSession(input = {}) {
  const sessionKey = buildSessionKey(input);
  if (!sessionKey) {
    throw new Error('Session-Key fehlt fuer die zentrale Session-Persistenz.');
  }

  const timestamp = nowIso();
  const existing = readAppSessionByKey(sessionKey);
  const payload = {
    sessionKey,
    module: cleanText(input.module) || existing?.module || 'app',
    sessionType: cleanText(input.sessionType) || existing?.session_type || 'runtime',
    status: cleanText(input.status) || existing?.status || 'inactive',
    storagePath: cleanText(input.storagePath) || existing?.storage_path || null,
    externalRef: cleanText(input.externalRef) || existing?.external_ref || null,
    metaJson:
      input.meta !== undefined ? stringifyJson(input.meta) : existing?.meta_json || null,
    lastSeenAt: input.lastSeenAt || timestamp,
    createdAt: existing?.created_at || timestamp,
    updatedAt: timestamp
  };

  if (existing) {
    db.prepare(
      `
        UPDATE app_sessions
        SET module = @module,
            session_type = @sessionType,
            status = @status,
            storage_path = @storagePath,
            external_ref = @externalRef,
            meta_json = @metaJson,
            last_seen_at = @lastSeenAt,
            updated_at = @updatedAt
        WHERE session_key = @sessionKey
      `
    ).run(payload);
  } else {
    db.prepare(
      `
        INSERT INTO app_sessions (
          session_key,
          module,
          session_type,
          status,
          storage_path,
          external_ref,
          meta_json,
          last_seen_at,
          created_at,
          updated_at
        ) VALUES (
          @sessionKey,
          @module,
          @sessionType,
          @status,
          @storagePath,
          @externalRef,
          @metaJson,
          @lastSeenAt,
          @createdAt,
          @updatedAt
        )
      `
    ).run(payload);
  }

  return mapAppSessionRow(readAppSessionByKey(sessionKey));
}

export function upsertDealStatusState(input = {}) {
  const dealKey = buildDealStatusKey(input);
  if (!dealKey) {
    return null;
  }

  const timestamp = nowIso();
  const existing = readDealStatusRowByKey(dealKey);
  const channel = cleanText(input.channel);
  const postedChannels = Array.from(
    new Set([
      ...parseJson(existing?.posted_channels_json, []),
      ...(channel && input.registerPost === true ? [channel] : [])
    ].filter(Boolean))
  );
  const origin = normalizeOrigin(input.origin || input.sourceType);
  const manualPostCount = parseInteger(existing?.manual_post_count, 0) + (input.registerPost === true && origin === 'manual' ? 1 : 0);
  const automaticPostCount =
    parseInteger(existing?.automatic_post_count, 0) + (input.registerPost === true && origin === 'automatic' ? 1 : 0);
  const status = cleanText(input.status) || existing?.status || 'detected';
  const normalizedStatus = normalizePublishingQueueStatus(status, status);
  const queueStatus = cleanText(input.lastQueueStatus)
    ? normalizePublishingQueueStatus(input.lastQueueStatus, cleanText(input.lastQueueStatus))
    : input.queueId
      ? normalizedStatus
      : existing?.last_queue_status || null;
  const payload = {
    dealKey,
    asin: cleanText(input.asin).toUpperCase() || existing?.asin || null,
    normalizedUrl: cleanText(input.normalizedUrl || input.normalized_url) || existing?.normalized_url || null,
    originalUrl: cleanText(input.originalUrl || input.original_url || input.url) || existing?.original_url || null,
    title: cleanText(input.title) || existing?.title || null,
    sellerType: normalizeSellerType(input.sellerType || input.seller_type || existing?.seller_type),
    sourceType: cleanText(input.sourceType) || existing?.source_type || null,
    sourceId:
      input.sourceId !== undefined && input.sourceId !== null && input.sourceId !== ''
        ? String(input.sourceId)
        : existing?.source_id || null,
    status: normalizedStatus,
    decisionReason: cleanText(input.decisionReason) || existing?.decision_reason || null,
    queueId: parseInteger(input.queueId, 0) || existing?.queue_id || null,
    lastQueueStatus: queueStatus,
    lastChannel: channel || existing?.last_channel || null,
    postedChannelsJson: JSON.stringify(postedChannels),
    lastPostedAt: input.postedAt || (input.registerPost === true ? timestamp : existing?.last_posted_at || null),
    lastError: isSentPublishingQueueStatus(normalizedStatus) ? null : cleanText(input.lastError) || existing?.last_error || null,
    manualPostCount,
    automaticPostCount,
    lastOrigin: origin,
    metaJson: input.meta !== undefined ? stringifyJson(input.meta) : existing?.meta_json || null,
    createdAt: existing?.created_at || timestamp,
    updatedAt: timestamp
  };

  if (existing) {
    db.prepare(
      `
        UPDATE deal_status_registry
        SET asin = @asin,
            normalized_url = @normalizedUrl,
            original_url = @originalUrl,
            title = @title,
            seller_type = @sellerType,
            source_type = @sourceType,
            source_id = @sourceId,
            status = @status,
            decision_reason = @decisionReason,
            queue_id = @queueId,
            last_queue_status = @lastQueueStatus,
            last_channel = @lastChannel,
            posted_channels_json = @postedChannelsJson,
            last_posted_at = @lastPostedAt,
            last_error = @lastError,
            manual_post_count = @manualPostCount,
            automatic_post_count = @automaticPostCount,
            last_origin = @lastOrigin,
            meta_json = @metaJson,
            updated_at = @updatedAt
        WHERE deal_key = @dealKey
      `
    ).run(payload);
  } else {
    db.prepare(
      `
        INSERT INTO deal_status_registry (
          deal_key,
          asin,
          normalized_url,
          original_url,
          title,
          seller_type,
          source_type,
          source_id,
          status,
          decision_reason,
          queue_id,
          last_queue_status,
          last_channel,
          posted_channels_json,
          last_posted_at,
          last_error,
          manual_post_count,
          automatic_post_count,
          last_origin,
          meta_json,
          created_at,
          updated_at
        ) VALUES (
          @dealKey,
          @asin,
          @normalizedUrl,
          @originalUrl,
          @title,
          @sellerType,
          @sourceType,
          @sourceId,
          @status,
          @decisionReason,
          @queueId,
          @lastQueueStatus,
          @lastChannel,
          @postedChannelsJson,
          @lastPostedAt,
          @lastError,
          @manualPostCount,
          @automaticPostCount,
          @lastOrigin,
          @metaJson,
          @createdAt,
          @updatedAt
        )
      `
    ).run(payload);
  }

  return mapDealStatusRow(readDealStatusRowByKey(dealKey));
}

export function syncDealStatusWithQueue(input = {}) {
  const payload = input.payload && typeof input.payload === 'object' ? input.payload : {};
  const target = input.target && typeof input.target === 'object' ? input.target : null;
  const channelType = cleanText(target?.channel_type || target?.channelType).toUpperCase();
  const targetLabel = cleanText(target?.target_label || target?.targetLabel);
  const channel = buildPublishingChannelLabel(channelType, targetLabel);

  return upsertDealStatusState({
    asin: cleanText(payload.asin),
    normalizedUrl: cleanText(payload.normalizedUrl || payload.normalized_url),
    originalUrl: cleanText(payload.link || payload.originalUrl || payload.original_url),
    title: cleanText(payload.title),
    sellerType: cleanText(payload.sellerType || payload.seller_type),
    sourceType: cleanText(input.sourceType),
    sourceId: input.sourceId,
    queueId: input.queueId,
    status: cleanText(input.queueStatus),
    lastQueueStatus: cleanText(input.queueStatus),
    channel,
    decisionReason: cleanText(input.message),
    lastError: cleanText(input.errorMessage),
    origin: cleanText(input.origin || input.sourceType),
    meta: {
      targetId: target?.id || null,
      targetRef: cleanText(target?.target_ref || target?.targetRef),
      targetMeta: target?.target_meta_json ? parseJson(target.target_meta_json, null) : target?.targetMeta || null,
      ...(input.meta && typeof input.meta === 'object' ? input.meta : {})
    }
  });
}

export function syncImportedDealState(input = {}) {
  return upsertDealStatusState({
    dealKey: buildDealStatusKey({
      asin: input.asin,
      normalizedUrl: input.normalizedUrl,
      originalUrl: input.originalUrl
    }),
    asin: input.asin,
    normalizedUrl: input.normalizedUrl,
    originalUrl: input.originalUrl,
    title: input.title,
    sellerType: input.sellerType,
    sourceType: input.sourceType || 'copybot',
    sourceId: input.sourceId,
    status: input.status,
    decisionReason: input.decisionReason,
    queueId: input.queueId,
    lastQueueStatus: input.queueId ? 'pending' : null,
    origin: input.origin || 'automatic',
    meta: input.meta
  });
}

export function getCentralDatabaseStructure() {
  return Object.entries(CENTRAL_DATABASE_COLLECTIONS).map(([collection, tables]) => ({
    collection,
    tables: tables.map((tableName) => ({
      name: tableName,
      columns: db.prepare(`PRAGMA table_info(${tableName})`).all().map((column) => ({
        name: column.name,
        type: column.type,
        notNull: column.notnull === 1,
        primaryKey: column.pk === 1,
        defaultValue: column.dflt_value
      }))
    }))
  }));
}
