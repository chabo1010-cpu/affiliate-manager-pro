import crypto from 'crypto';
import { DEFAULT_TELEGRAM_COPY_BUTTON_TEXT, getDb } from '../db.js';
import { upsertDealStatusState } from './databaseService.js';
import { logGeneratorDebug } from './generatorFlowService.js';
import { isActivePublishingQueueStatus } from './publishingQueueStateService.js';

const db = getDb();
const AMAZON_AFFILIATE_TAG = 'codeundcoup08-21';
const AMAZON_FALLBACK_HOST = 'amazon.de';
const AMAZON_SHORT_HOSTS = new Set(['amzn.to']);
const AMAZON_HOST_PATTERN = /(^|\.)amazon\.[a-z.]+$/i;
const AMAZON_ASIN_PATTERNS = [
  /\/dp\/([A-Z0-9]{10})(?:[/?]|$)/i,
  /\/gp\/product\/([A-Z0-9]{10})(?:[/?]|$)/i,
  /\/exec\/obidos\/ASIN\/([A-Z0-9]{10})(?:[/?]|$)/i,
  /\/product\/([A-Z0-9]{10})(?:[/?]|$)/i,
  /[?&](?:asin|ASIN)=([A-Z0-9]{10})(?:[&#]|$)/i
];

function cleanText(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function parseEnabledFlag(value) {
  return value === 1 || value === '1' || value === true;
}

function parseNumber(value) {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }

  const trimmed = cleanText(value);
  if (!trimmed) {
    return null;
  }

  const normalized = trimmed
    .replace(/[^\d,.-]/g, '')
    .replace(/\.(?=\d{3}(?:\D|$))/g, '')
    .replace(',', '.');
  const parsed = Number.parseFloat(normalized);

  return Number.isFinite(parsed) ? parsed : null;
}

function parseInteger(value, fallback = 0) {
  const parsed = Number.parseInt(String(value ?? ''), 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function normalizeOriginType(value = '') {
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

  return normalized || 'manual';
}

function safeUrl(value) {
  try {
    return new URL(value);
  } catch {
    return null;
  }
}

function normalizeHostname(value = '') {
  return cleanText(value).toLowerCase().replace(/^www\./, '');
}

function isAmazonHostname(value = '') {
  const hostname = normalizeHostname(value);
  return AMAZON_SHORT_HOSTS.has(hostname) || AMAZON_HOST_PATTERN.test(hostname);
}

function findDealsByField(field, value) {
  const cleanValue = cleanText(value);
  if (!cleanValue) {
    return [];
  }

  return db
    .prepare(
      `
        SELECT
          id,
          asin,
          dealHash,
          url,
          originalUrl,
          normalizedUrl,
          title,
          productTitle,
          price,
          currentPrice,
          oldPrice,
          sellerType,
          sourceType,
          originType,
          queueId,
          postedAt,
          channel,
          couponCode
        FROM deals_history
        WHERE ${field} = ?
        ORDER BY postedAt DESC
      `
    )
    .all(cleanValue);
}

function buildHistorySummary(rows = []) {
  const sixMonthsAgo = new Date();
  sixMonthsAgo.setMonth(sixMonthsAgo.getMonth() - 6);

  const prices = rows
    .filter((row) => {
      const postedAt = new Date(row.postedAt).getTime();
      return Number.isFinite(postedAt) && postedAt >= sixMonthsAgo.getTime();
    })
    .map((row) => parseNumber(row.currentPrice ?? row.price))
    .filter((value) => typeof value === 'number');

  return {
    latest: rows[0] || null,
    minPrice: prices.length ? Math.min(...prices) : null,
    maxPrice: prices.length ? Math.max(...prices) : null,
    count: rows.length
  };
}

function buildDealLockHash({ asin = '', normalizedUrl = '', rawUrl = '' } = {}) {
  const hashBase = asin
    ? `asin:${cleanText(asin).toUpperCase()}`
    : normalizedUrl
      ? `url:${cleanText(normalizedUrl)}`
      : cleanText(rawUrl)
        ? `url:${cleanText(rawUrl)}`
        : '';

  return hashBase ? crypto.createHash('sha1').update(hashBase).digest('hex') : '';
}

export function buildDealLockIdentity(input = {}) {
  const rawUrl = cleanText(input.url || input.originalUrl || '');
  const finalUrl = cleanText(input.finalUrl || input.url || input.normalizedUrl || input.originalUrl || '');
  const asin =
    cleanText(input.asin).toUpperCase() ||
    extractAsin(input.finalUrl || input.url || input.normalizedUrl || input.originalUrl || '');
  const normalizedUrl = normalizeAmazonLink(input.normalizedUrl || finalUrl || rawUrl || '');
  const dealHash = cleanText(input.dealHash) || buildDealLockHash({ asin, normalizedUrl, rawUrl: finalUrl || rawUrl });
  const queueId = parseInteger(input.queueId, 0) || null;

  return {
    asin,
    normalizedUrl,
    rawUrl,
    finalUrl,
    dealHash,
    queueId
  };
}

function findDealsByIdentity(identity = {}) {
  if (!identity.dealHash && !identity.asin && !identity.normalizedUrl && !identity.rawUrl) {
    return [];
  }

  return db
    .prepare(
      `
        SELECT
          id,
          asin,
          dealHash,
          url,
          originalUrl,
          normalizedUrl,
          title,
          productTitle,
          price,
          currentPrice,
          oldPrice,
          sellerType,
          sourceType,
          originType,
          queueId,
          postedAt,
          channel,
          couponCode
        FROM deals_history
        WHERE (@dealHash != '' AND dealHash = @dealHash)
           OR (@asin != '' AND asin = @asin)
           OR (@normalizedUrl != '' AND normalizedUrl = @normalizedUrl)
           OR (@rawUrl != '' AND (url = @rawUrl OR originalUrl = @rawUrl))
        ORDER BY postedAt DESC
      `
    )
    .all({
      dealHash: identity.dealHash || '',
      asin: identity.asin || '',
      normalizedUrl: identity.normalizedUrl || '',
      rawUrl: identity.rawUrl || ''
    });
}

function isActiveQueueState(value = '') {
  return isActivePublishingQueueStatus(value) || cleanText(value).toLowerCase() === 'posting';
}

function findActiveDealRegistryEntry(identity = {}) {
  if (!identity.asin && !identity.normalizedUrl && !identity.rawUrl) {
    return null;
  }

  const rows = db
    .prepare(
      `
        SELECT
          id,
          deal_key,
          asin,
          normalized_url,
          original_url,
          status,
          queue_id,
          last_queue_status,
          last_channel,
          decision_reason,
          last_posted_at,
          last_origin,
          updated_at
        FROM deal_status_registry
        WHERE (@asin != '' AND asin = @asin)
           OR (@normalizedUrl != '' AND normalized_url = @normalizedUrl)
           OR (@rawUrl != '' AND original_url = @rawUrl)
        ORDER BY updated_at DESC
      `
    )
    .all({
      asin: identity.asin || '',
      normalizedUrl: identity.normalizedUrl || '',
      rawUrl: identity.rawUrl || ''
    });

  return (
    rows.find((row) => {
      const queueId = parseInteger(row.queue_id, 0);
      if (identity.queueId && queueId && identity.queueId === queueId) {
        return false;
      }

      return isActiveQueueState(row.status) || isActiveQueueState(row.last_queue_status);
    }) || null
  );
}

function getDealMatchClause(asin, normalizedUrl) {
  if (asin) {
    return {
      clause: `asin = @asin`,
      params: { asin }
    };
  }

  if (normalizedUrl) {
    return {
      clause: `normalizedUrl = @normalizedUrl`,
      params: { normalizedUrl }
    };
  }

  return {
    clause: `1 = 0`,
    params: {}
  };
}

export function extractAsin(value = '') {
  const trimmed = cleanText(value);
  if (!trimmed) {
    return '';
  }

  if (/^[A-Z0-9]{10}$/i.test(trimmed)) {
    return trimmed.toUpperCase();
  }

  for (const pattern of AMAZON_ASIN_PATTERNS) {
    const match = trimmed.match(pattern);
    if (match?.[1]) {
      return match[1].toUpperCase();
    }
  }

  return '';
}

export function normalizeAmazonLink(value = '') {
  const trimmed = cleanText(value);
  if (!trimmed) {
    return '';
  }

  const parsed = safeUrl(trimmed);
  if (!parsed) {
    return trimmed;
  }

  const hostname = parsed.hostname.toLowerCase().replace(/^www\./, '');
  const asin = extractAsin(trimmed);
  if (asin && /amazon\./i.test(hostname)) {
    return `https://${hostname}/dp/${asin}`;
  }

  return `${parsed.protocol}//${hostname}${parsed.pathname.replace(/\/+$/, '') || '/'}`;
}

export function buildAmazonAffiliateLinkRecord(value = '', options = {}) {
  const originalUrl = cleanText(value);
  const resolvedUrl = cleanText(options.resolvedUrl);
  const providedAsin = cleanText(options.asin).toUpperCase();
  const lookupValue = resolvedUrl || originalUrl;
  const parsedUrl = safeUrl(lookupValue);
  const hostname = normalizeHostname(parsedUrl?.hostname || '');
  const isAmazonSource = hostname ? isAmazonHostname(hostname) : false;

  logGeneratorDebug('RAW LINK RECEIVED', {
    originalUrl: originalUrl || null,
    resolvedUrl: resolvedUrl || null
  });

  const directAsin = extractAsin(lookupValue) || extractAsin(originalUrl);
  const asin =
    directAsin ||
    (!originalUrl || isAmazonSource || /^[A-Z0-9]{10}$/i.test(originalUrl) ? providedAsin : '');

  if (asin) {
    logGeneratorDebug('ASIN EXTRACTED', {
      asin,
      sourceHost: hostname || null
    });
  }

  if (!asin) {
    logGeneratorDebug('INVALID LINK', {
      originalUrl: originalUrl || null,
      resolvedUrl: resolvedUrl || null,
      reason: 'ASIN konnte nicht erkannt werden.'
    });

    return {
      originalUrl,
      asin: '',
      normalizedUrl: '',
      affiliateUrl: '',
      valid: false
    };
  }

  if (parsedUrl && !isAmazonSource) {
    logGeneratorDebug('INVALID LINK', {
      originalUrl: originalUrl || null,
      resolvedUrl: resolvedUrl || null,
      reason: 'Link stammt nicht von Amazon.'
    });

    return {
      originalUrl,
      asin: '',
      normalizedUrl: '',
      affiliateUrl: '',
      valid: false
    };
  }

  const finalHost = AMAZON_HOST_PATTERN.test(hostname) ? hostname : normalizeHostname(options.defaultHost || AMAZON_FALLBACK_HOST);
  const normalizedUrl = `https://www.${finalHost}/dp/${asin}`;
  const affiliateUrl = `${normalizedUrl}?tag=${AMAZON_AFFILIATE_TAG}`;

  logGeneratorDebug('LINK NORMALIZED', {
    asin,
    normalizedUrl
  });
  logGeneratorDebug('AFFILIATE LINK BUILT', {
    asin,
    affiliateUrl
  });

  return {
    originalUrl,
    asin,
    normalizedUrl,
    affiliateUrl,
    valid: true
  };
}

export function classifySellerType({ soldByAmazon, shippedByAmazon }) {
  if (soldByAmazon && shippedByAmazon) {
    return 'AMAZON';
  }

  if (!soldByAmazon && shippedByAmazon) {
    return 'FBA';
  }

  return 'FBM';
}

export function normalizeSellerType(value) {
  const normalized = cleanText(value).toUpperCase();
  if (normalized === 'AMAZON' || normalized === 'FBA' || normalized === 'FBM') {
    return normalized;
  }

  return 'FBM';
}

export function getRepostSettingsRow() {
  let row = db
    .prepare(
      `
        SELECT
          id,
          repostCooldownEnabled,
          repostCooldownHours,
          telegramCopyButtonText,
          copybotEnabled
        FROM app_settings
        WHERE id = 1
      `
    )
    .get();

  if (!row) {
    db.prepare(
      `
        INSERT INTO app_settings (
          id,
          repostCooldownEnabled,
          repostCooldownHours,
          telegramCopyButtonText,
          copybotEnabled
        ) VALUES (1, 1, 12, ?, 0)
      `
    ).run(DEFAULT_TELEGRAM_COPY_BUTTON_TEXT);

    row = db
      .prepare(
        `
          SELECT
            id,
            repostCooldownEnabled,
            repostCooldownHours,
            telegramCopyButtonText,
            copybotEnabled
          FROM app_settings
          WHERE id = 1
        `
      )
      .get();
  }

  return row ?? null;
}

export function getRepostCooldownHours() {
  const row = getRepostSettingsRow();
  const hours = Number(row?.repostCooldownHours);
  return Number.isFinite(hours) && hours >= 0 ? hours : 12;
}

export function getRepostSettings() {
  const row = getRepostSettingsRow();
  const repostCooldownHours = Number(row?.repostCooldownHours);

  return {
    repostCooldownEnabled: parseEnabledFlag(row?.repostCooldownEnabled),
    repostCooldownHours: Number.isFinite(repostCooldownHours) && repostCooldownHours >= 0 ? repostCooldownHours : 12,
    telegramCopyButtonText: cleanText(row?.telegramCopyButtonText) || DEFAULT_TELEGRAM_COPY_BUTTON_TEXT,
    copybotEnabled: parseEnabledFlag(row?.copybotEnabled)
  };
}

export function saveRepostSettings({
  repostCooldownEnabled,
  repostCooldownHours,
  telegramCopyButtonText,
  copybotEnabled
}) {
  const current = getRepostSettings();
  const enabled =
    repostCooldownEnabled === undefined ? current.repostCooldownEnabled : parseEnabledFlag(repostCooldownEnabled);
  const hours = Number(repostCooldownHours);
  const nextHours = Number.isFinite(hours) && hours >= 0 ? Math.round(hours) : current.repostCooldownHours;
  const nextTelegramCopyButtonText =
    telegramCopyButtonText === undefined
      ? current.telegramCopyButtonText
      : cleanText(telegramCopyButtonText) || DEFAULT_TELEGRAM_COPY_BUTTON_TEXT;
  const nextCopybotEnabled =
    copybotEnabled === undefined ? current.copybotEnabled : parseEnabledFlag(copybotEnabled);

  const updateResult = db
    .prepare(
      `
        UPDATE app_settings
        SET repostCooldownEnabled = ?,
            repostCooldownHours = ?,
            telegramCopyButtonText = ?,
            copybotEnabled = ?
        WHERE id = 1
      `
    )
    .run(enabled ? 1 : 0, nextHours, nextTelegramCopyButtonText, nextCopybotEnabled ? 1 : 0);

  if (!updateResult.changes) {
    db.prepare(
      `
        INSERT INTO app_settings (
          id,
          repostCooldownEnabled,
          repostCooldownHours,
          telegramCopyButtonText,
          copybotEnabled
        ) VALUES (1, ?, ?, ?, ?)
      `
    ).run(enabled ? 1 : 0, nextHours, nextTelegramCopyButtonText, nextCopybotEnabled ? 1 : 0);
  }

  return getRepostSettings();
}

export function getTelegramCopyButtonText() {
  return getRepostSettings().telegramCopyButtonText;
}

export function getDealHistorySummary({ asin = '', normalizedUrl = '' } = {}) {
  const cleanAsin = cleanText(asin).toUpperCase();
  const cleanNormalizedUrl = cleanText(normalizedUrl);
  const { clause, params } = getDealMatchClause(cleanAsin, cleanNormalizedUrl);

  const rows = db
    .prepare(
      `
        SELECT
          id,
          asin,
          dealHash,
          url,
          originalUrl,
          normalizedUrl,
          title,
          productTitle,
          price,
          currentPrice,
          oldPrice,
          sellerType,
          sourceType,
          originType,
          queueId,
          postedAt,
          channel,
          couponCode
        FROM deals_history
        WHERE ${clause}
        ORDER BY postedAt DESC
      `
    )
    .all(params);

  return buildHistorySummary(rows);
}

export function checkDealLockStatus(input = {}) {
  const identity = buildDealLockIdentity(input);
  const historyRows = findDealsByIdentity(identity);
  const summary = buildHistorySummary(historyRows);
  const settings = getRepostSettings();
  const activeRegistryLock = findActiveDealRegistryEntry(identity);
  const lastPostedAt = summary.latest?.postedAt || null;
  const sameQueuePublication =
    Boolean(identity.queueId) && Boolean(parseInteger(summary.latest?.queueId, 0)) && identity.queueId === parseInteger(summary.latest?.queueId, 0);
  let blocked = false;
  let remainingSeconds = 0;
  let blockCode = '';
  let blockReason = '';

  if (activeRegistryLock) {
    blocked = true;
    blockCode = 'DEAL_LOCK_ACTIVE_QUEUE';
    blockReason = 'Deal-Lock aktiv: Der Deal ist bereits in Queue oder Verarbeitung.';
  }

  if (!blocked && settings.repostCooldownEnabled && lastPostedAt && !sameQueuePublication) {
    const nowMs = Date.now();
    const lastPostedMs = new Date(lastPostedAt).getTime();
    const cooldownMs = settings.repostCooldownHours * 60 * 60 * 1000;

    if (Number.isFinite(lastPostedMs) && nowMs - lastPostedMs < cooldownMs) {
      blocked = true;
      remainingSeconds = Math.ceil((cooldownMs - (nowMs - lastPostedMs)) / 1000);
      blockCode = 'DEAL_LOCK_COOLDOWN';
      blockReason = 'Deal-Lock aktiv: Sperrzeit fuer diesen Deal laeuft noch.';
    }
  }

  return {
    ...identity,
    blocked,
    blockCode,
    blockReason,
    remainingSeconds,
    repostCooldownEnabled: settings.repostCooldownEnabled,
    repostCooldownHours: settings.repostCooldownHours,
    lastDeal: summary.latest,
    minPrice: summary.minPrice,
    maxPrice: summary.maxPrice,
    postingCount: summary.count,
    activeRegistryLock: activeRegistryLock
      ? {
          id: activeRegistryLock.id,
          status: activeRegistryLock.status,
          queueId: activeRegistryLock.queue_id || null,
          lastQueueStatus: activeRegistryLock.last_queue_status || '',
          channel: activeRegistryLock.last_channel || '',
          decisionReason: activeRegistryLock.decision_reason || '',
          updatedAt: activeRegistryLock.updated_at || null
        }
      : null
  };
}

export function checkDealCooldown(input = {}) {
  return checkDealLockStatus(input);
}

export function assertDealNotLocked(input = {}) {
  const lockStatus = checkDealLockStatus(input);

  if (!lockStatus.blocked) {
    return lockStatus;
  }

  const error = new Error(lockStatus.blockReason || 'Deal-Lock aktiv.');
  error.code = lockStatus.blockCode || 'DEAL_LOCK_ACTIVE';
  error.retryable = false;
  error.dealLock = lockStatus;
  throw error;
}

export function savePostedDeal(input = {}) {
  const identity = buildDealLockIdentity(input);
  const originalUrl = cleanText(input.originalUrl || input.url || input.finalUrl || input.normalizedUrl);
  const url = cleanText(input.finalUrl || input.url || input.normalizedUrl || input.originalUrl);
  const productTitle = cleanText(input.productTitle || input.title);
  const currentPrice = cleanText(input.currentPrice || input.price);
  const oldPrice = cleanText(input.oldPrice);
  const sellerType = normalizeSellerType(input.sellerType);
  const postedAt = cleanText(input.postedAt) || new Date().toISOString();
  const sourceType = cleanText(input.sourceType) || 'publication';
  const originType = normalizeOriginType(input.origin || input.originType || sourceType);
  const queueId = parseInteger(input.queueId, 0) || null;
  const payload = {
    asin: identity.asin,
    dealHash: identity.dealHash,
    url,
    originalUrl,
    normalizedUrl: identity.normalizedUrl,
    title: productTitle,
    productTitle,
    price: currentPrice,
    currentPrice,
    oldPrice,
    sellerType,
    sourceType,
    originType,
    queueId,
    postedAt,
    channel: cleanText(input.channel),
    couponCode: cleanText(input.couponCode)
  };

  const result = db
    .prepare(
      `
        INSERT INTO deals_history (
          asin,
          dealHash,
          url,
          originalUrl,
          normalizedUrl,
          title,
          productTitle,
          price,
          currentPrice,
          oldPrice,
          sellerType,
          sourceType,
          originType,
          queueId,
          postedAt,
          channel,
          couponCode
        ) VALUES (
          @asin,
          @dealHash,
          @url,
          @originalUrl,
          @normalizedUrl,
          @title,
          @productTitle,
          @price,
          @currentPrice,
          @oldPrice,
          @sellerType,
          @sourceType,
          @originType,
          @queueId,
          @postedAt,
          @channel,
          @couponCode
        )
      `
    )
    .run(payload);

  upsertDealStatusState({
    asin: identity.asin,
    normalizedUrl: identity.normalizedUrl,
    originalUrl,
    title: productTitle,
    sellerType,
    sourceType,
    sourceId: input.sourceId ?? result.lastInsertRowid,
    status: 'sent',
    decisionReason: cleanText(input.decisionReason) || 'Deal veroeffentlicht.',
    queueId,
    channel: payload.channel,
    postedAt,
    origin: originType,
    registerPost: true,
    meta: {
      dealHash: identity.dealHash,
      sourceType,
      originType,
      ...(input.meta && typeof input.meta === 'object' ? input.meta : {})
    }
  });

  return {
    id: result.lastInsertRowid,
    asin: identity.asin,
    normalizedUrl: identity.normalizedUrl,
    dealHash: identity.dealHash,
    postedAt
  };
}

export function listDealsHistory({
  sellerType = '',
  startDate = '',
  endDate = '',
  asin = '',
  url = '',
  title = ''
} = {}) {
  const normalizedSellerType = cleanText(sellerType).toUpperCase();
  const whereClauses = [];
  const params = {};

  if (normalizedSellerType) {
    whereClauses.push(`sellerType = @sellerType`);
    params.sellerType = normalizedSellerType;
  }

  const cleanAsin = cleanText(asin).toUpperCase();
  if (cleanAsin) {
    whereClauses.push(`asin LIKE @asin`);
    params.asin = `%${cleanAsin}%`;
  }

  const cleanUrlValue = cleanText(url);
  if (cleanUrlValue) {
    whereClauses.push(`(url LIKE @url OR normalizedUrl LIKE @url OR originalUrl LIKE @url)`);
    params.url = `%${cleanUrlValue}%`;
  }

  const cleanTitle = cleanText(title);
  if (cleanTitle) {
    whereClauses.push(`(title LIKE @title OR productTitle LIKE @title)`);
    params.title = `%${cleanTitle}%`;
  }

  const parsedStartDate = cleanText(startDate) ? new Date(`${cleanText(startDate)}T00:00:00.000`) : null;
  if (parsedStartDate && !Number.isNaN(parsedStartDate.getTime())) {
    whereClauses.push(`postedAt >= @startDate`);
    params.startDate = parsedStartDate.toISOString();
  }

  const parsedEndDate = cleanText(endDate) ? new Date(`${cleanText(endDate)}T23:59:59.999`) : null;
  if (parsedEndDate && !Number.isNaN(parsedEndDate.getTime())) {
    whereClauses.push(`postedAt <= @endDate`);
    params.endDate = parsedEndDate.toISOString();
  }

  const whereSql = whereClauses.length ? `WHERE ${whereClauses.join(' AND ')}` : '';

  return db
    .prepare(
      `
        SELECT
          id,
          asin,
          dealHash,
          url,
          originalUrl,
          normalizedUrl,
          title,
          productTitle,
          price,
          currentPrice,
          oldPrice,
          sellerType,
          sourceType,
          originType,
          queueId,
          postedAt,
          channel,
          couponCode
        FROM deals_history
        ${whereSql}
        ORDER BY postedAt DESC
      `
    )
    .all(params);
}

export { cleanText, normalizeOriginType, parseEnabledFlag, parseNumber };
