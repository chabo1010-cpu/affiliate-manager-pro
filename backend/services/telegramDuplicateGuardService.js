import { getDb } from '../db.js';
import { cleanText, extractAsin, normalizeAmazonLink, parseNumber } from './dealHistoryService.js';

const db = getDb();

const DEFAULT_DUPLICATE_WINDOW_HOURS = 24;
const DUPLICATE_TITLE_LIMIT = 100;
const DUPLICATE_PENDING_TTL_MS = 15 * 60 * 1000;
const pendingDuplicateReservations = new Map();

function nowIso() {
  return new Date().toISOString();
}

function resolveDuplicateWindowHours() {
  const rawValue = Number.parseFloat(String(process.env.DUPLICATE_WINDOW_HOURS ?? ''));
  return Number.isFinite(rawValue) && rawValue > 0 ? rawValue : DEFAULT_DUPLICATE_WINDOW_HOURS;
}

function normalizeDuplicateChannelType(channelType = '', targetRef = '') {
  const normalizedChannelType = cleanText(channelType).toLowerCase() || 'telegram';
  const normalizedTargetRef = cleanText(targetRef);
  return normalizedTargetRef ? `${normalizedChannelType}:${normalizedTargetRef}` : normalizedChannelType;
}

export function normalizeTelegramDuplicateTitle(title = '') {
  const normalized = cleanText(title)
    .normalize('NFKD')
    .replace(/\p{M}+/gu, '')
    .replace(/[\p{Extended_Pictographic}\p{Emoji_Presentation}]/gu, ' ')
    .replace(/[^\p{L}\p{N}]+/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();

  return normalized.slice(0, DUPLICATE_TITLE_LIMIT);
}

export function normalizeTelegramDuplicatePrice(price = '') {
  const parsedPrice = parseNumber(price);
  return Number.isFinite(parsedPrice) ? parsedPrice.toFixed(2) : '';
}

function cleanupExpiredPendingReservations(nowMs = Date.now()) {
  for (const [duplicateKey, reservation] of pendingDuplicateReservations.entries()) {
    const reservedAtMs = new Date(reservation?.reservedAt || '').getTime();
    if (!Number.isFinite(reservedAtMs) || nowMs - reservedAtMs > DUPLICATE_PENDING_TTL_MS) {
      pendingDuplicateReservations.delete(duplicateKey);
    }
  }
}

function findRecentDuplicateRow(descriptor = {}, sinceIso = '') {
  const hasNormalizedUrl = Boolean(descriptor.normalizedUrl);

  return db
    .prepare(
      `
        SELECT
          id,
          duplicate_key,
          channel_type,
          target_ref,
          asin,
          normalized_title,
          normalized_price,
          normalized_url,
          last_sent_at,
          last_message_id,
          last_post_context
        FROM telegram_post_duplicates
        WHERE channel_type = @channelType
          AND last_sent_at IS NOT NULL
          AND last_sent_at >= @sinceIso
          AND (
            (@duplicateKey != '' AND duplicate_key = @duplicateKey)
            OR (
              @hasNormalizedUrl = 1
              AND normalized_url = @normalizedUrl
              AND normalized_price = @normalizedPrice
            )
          )
        ORDER BY last_sent_at DESC
        LIMIT 1
      `
    )
    .get({
      channelType: descriptor.channelType || '',
      duplicateKey: descriptor.duplicateKey || '',
      normalizedUrl: descriptor.normalizedUrl || '',
      normalizedPrice: descriptor.normalizedPrice || '',
      sinceIso,
      hasNormalizedUrl: hasNormalizedUrl ? 1 : 0
    });
}

export function buildTelegramDuplicateDescriptor(input = {}) {
  const targetRef = cleanText(input.targetRef || input.chatId);
  const channelType = normalizeDuplicateChannelType(input.channelType || 'telegram', targetRef);
  const asin =
    cleanText(input.asin).toUpperCase() ||
    extractAsin(input.normalizedUrl || input.url || input.originalUrl || input.affiliateLink || '');
  const normalizedUrl = normalizeAmazonLink(
    input.normalizedUrl || input.url || input.originalUrl || input.affiliateLink || ''
  );
  const normalizedTitle = normalizeTelegramDuplicateTitle(
    input.title || input.titlePreview || input.normalizedTitle || ''
  );
  const normalizedPrice =
    cleanText(input.normalizedPrice) || normalizeTelegramDuplicatePrice(input.price || input.currentPrice || '');
  const priceToken = normalizedPrice || 'na';
  const duplicateKey = asin
    ? `${channelType}:${asin}:${priceToken}`
    : normalizedTitle
      ? `${channelType}:${normalizedTitle}:${priceToken}`
      : '';

  return {
    channelType,
    targetRef,
    asin,
    normalizedUrl,
    normalizedTitle,
    normalizedPrice: priceToken,
    rawNormalizedPrice: normalizedPrice,
    duplicateKey,
    title: cleanText(input.title || input.titlePreview),
    price: cleanText(input.price || input.currentPrice),
    postContext: cleanText(input.postContext),
    reason: duplicateKey ? '' : 'missing_duplicate_key'
  };
}

export function checkAndReserveTelegramDuplicate(input = {}) {
  const descriptor = buildTelegramDuplicateDescriptor(input);
  const windowHours = resolveDuplicateWindowHours();
  const now = nowIso();
  const nowMs = new Date(now).getTime();
  cleanupExpiredPendingReservations(nowMs);

  console.info('[DUPLICATE_CHECK_START]', {
    channelType: descriptor.channelType || null,
    asin: descriptor.asin || '',
    price: descriptor.rawNormalizedPrice || descriptor.price || '',
    targetRef: descriptor.targetRef || null,
    postContext: descriptor.postContext || null,
    windowHours
  });
  console.info('[DUPLICATE_CHECK_KEY]', {
    channelType: descriptor.channelType || null,
    asin: descriptor.asin || '',
    price: descriptor.rawNormalizedPrice || descriptor.price || '',
    duplicateKey: descriptor.duplicateKey || '',
    normalizedTitle: descriptor.normalizedTitle || '',
    normalizedUrl: descriptor.normalizedUrl || ''
  });

  if (!descriptor.duplicateKey) {
    console.info('[DUPLICATE_NOT_FOUND]', {
      channelType: descriptor.channelType || null,
      asin: descriptor.asin || '',
      price: descriptor.rawNormalizedPrice || descriptor.price || '',
      duplicateKey: '',
      reason: descriptor.reason || 'missing_duplicate_key'
    });
    return {
      blocked: false,
      descriptor,
      reservationTaken: false,
      windowHours
    };
  }

  const pendingReservation = pendingDuplicateReservations.get(descriptor.duplicateKey);
  if (pendingReservation) {
    console.warn('[DUPLICATE_FOUND_BLOCKED]', {
      channelType: descriptor.channelType || null,
      asin: descriptor.asin || '',
      price: descriptor.rawNormalizedPrice || descriptor.price || '',
      duplicateKey: descriptor.duplicateKey,
      lastSentAt: pendingReservation.lastSentAt || pendingReservation.reservedAt || null,
      reason: 'PENDING_SEND'
    });
    return {
      blocked: true,
      descriptor,
      reservationTaken: false,
      windowHours,
      reason: 'PENDING_SEND',
      lastSentAt: pendingReservation.lastSentAt || pendingReservation.reservedAt || null,
      previousMessageId: pendingReservation.lastMessageId || null
    };
  }

  const sinceIso = new Date(nowMs - windowHours * 60 * 60 * 1000).toISOString();
  const existingRow = findRecentDuplicateRow(descriptor, sinceIso);
  if (existingRow) {
    console.warn('[DUPLICATE_FOUND_BLOCKED]', {
      channelType: descriptor.channelType || null,
      asin: descriptor.asin || '',
      price: descriptor.rawNormalizedPrice || descriptor.price || '',
      duplicateKey: descriptor.duplicateKey,
      lastSentAt: existingRow.last_sent_at || null,
      reason: 'DUPLICATE_WINDOW_ACTIVE'
    });
    return {
      blocked: true,
      descriptor,
      reservationTaken: false,
      windowHours,
      reason: 'DUPLICATE_WINDOW_ACTIVE',
      lastSentAt: existingRow.last_sent_at || null,
      previousMessageId: existingRow.last_message_id || null
    };
  }

  pendingDuplicateReservations.set(descriptor.duplicateKey, {
    reservedAt: now,
    channelType: descriptor.channelType,
    targetRef: descriptor.targetRef,
    asin: descriptor.asin,
    normalizedPrice: descriptor.normalizedPrice
  });

  console.info('[DUPLICATE_NOT_FOUND]', {
    channelType: descriptor.channelType || null,
    asin: descriptor.asin || '',
    price: descriptor.rawNormalizedPrice || descriptor.price || '',
    duplicateKey: descriptor.duplicateKey,
    reason: 'not_found'
  });

  return {
    blocked: false,
    descriptor,
    reservationTaken: true,
    windowHours
  };
}

export function releaseTelegramDuplicateReservation(descriptorOrKey = '') {
  const duplicateKey =
    typeof descriptorOrKey === 'string' ? cleanText(descriptorOrKey) : cleanText(descriptorOrKey?.duplicateKey);
  if (!duplicateKey) {
    return;
  }

  pendingDuplicateReservations.delete(duplicateKey);
}

export function saveTelegramDuplicateAfterSend(input = {}) {
  const descriptor =
    input.descriptor && typeof input.descriptor === 'object'
      ? input.descriptor
      : buildTelegramDuplicateDescriptor(input);
  if (!descriptor.duplicateKey) {
    return null;
  }

  const timestamp = cleanText(input.sentAt) || nowIso();
  const messageId = cleanText(String(input.messageId ?? ''));
  const postContext = cleanText(input.postContext || descriptor.postContext);

  db.prepare(
    `
      INSERT INTO telegram_post_duplicates (
        duplicate_key,
        channel_type,
        target_ref,
        asin,
        normalized_title,
        normalized_price,
        normalized_url,
        last_sent_at,
        last_message_id,
        last_post_context,
        created_at,
        updated_at
      ) VALUES (
        @duplicateKey,
        @channelType,
        @targetRef,
        @asin,
        @normalizedTitle,
        @normalizedPrice,
        @normalizedUrl,
        @lastSentAt,
        @lastMessageId,
        @lastPostContext,
        @createdAt,
        @updatedAt
      )
      ON CONFLICT(duplicate_key) DO UPDATE SET
        channel_type = excluded.channel_type,
        target_ref = excluded.target_ref,
        asin = excluded.asin,
        normalized_title = excluded.normalized_title,
        normalized_price = excluded.normalized_price,
        normalized_url = excluded.normalized_url,
        last_sent_at = excluded.last_sent_at,
        last_message_id = excluded.last_message_id,
        last_post_context = excluded.last_post_context,
        updated_at = excluded.updated_at
    `
  ).run({
    duplicateKey: descriptor.duplicateKey,
    channelType: descriptor.channelType || 'telegram',
    targetRef: descriptor.targetRef || '',
    asin: descriptor.asin || '',
    normalizedTitle: descriptor.normalizedTitle || '',
    normalizedPrice: descriptor.normalizedPrice || 'na',
    normalizedUrl: descriptor.normalizedUrl || '',
    lastSentAt: timestamp,
    lastMessageId: messageId,
    lastPostContext: postContext,
    createdAt: timestamp,
    updatedAt: timestamp
  });

  pendingDuplicateReservations.delete(descriptor.duplicateKey);

  console.info('[DUPLICATE_SAVED_AFTER_SEND]', {
    channelType: descriptor.channelType || null,
    asin: descriptor.asin || '',
    price: descriptor.rawNormalizedPrice || descriptor.price || '',
    duplicateKey: descriptor.duplicateKey,
    lastSentAt: timestamp,
    postContext: postContext || null
  });

  return {
    ...descriptor,
    lastSentAt: timestamp,
    messageId: messageId || null,
    postContext
  };
}

export const __testablesTelegramDuplicateGuard = {
  pendingDuplicateReservations,
  resolveDuplicateWindowHours
};
