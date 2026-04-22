export const PUBLISHING_QUEUE_STATUS = Object.freeze({
  pending: 'pending',
  sending: 'sending',
  sent: 'sent',
  failed: 'failed',
  retry: 'retry'
});

const LEGACY_STATUS_MAP = Object.freeze({
  queued: PUBLISHING_QUEUE_STATUS.pending,
  processing: PUBLISHING_QUEUE_STATUS.sending,
  posted: PUBLISHING_QUEUE_STATUS.sent
});

function cleanText(value) {
  return typeof value === 'string' ? value.trim().toLowerCase() : '';
}

export function normalizePublishingQueueStatus(value, fallback = PUBLISHING_QUEUE_STATUS.pending) {
  const normalized = cleanText(value);

  if (normalized in LEGACY_STATUS_MAP) {
    return LEGACY_STATUS_MAP[normalized];
  }

  if (Object.values(PUBLISHING_QUEUE_STATUS).includes(normalized)) {
    return normalized;
  }

  return fallback;
}

export function isPendingPublishingQueueStatus(value) {
  return normalizePublishingQueueStatus(value) === PUBLISHING_QUEUE_STATUS.pending;
}

export function isSendingPublishingQueueStatus(value) {
  return normalizePublishingQueueStatus(value) === PUBLISHING_QUEUE_STATUS.sending;
}

export function isSentPublishingQueueStatus(value) {
  return normalizePublishingQueueStatus(value) === PUBLISHING_QUEUE_STATUS.sent;
}

export function isFailedPublishingQueueStatus(value) {
  return normalizePublishingQueueStatus(value) === PUBLISHING_QUEUE_STATUS.failed;
}

export function isRetryPublishingQueueStatus(value) {
  return normalizePublishingQueueStatus(value) === PUBLISHING_QUEUE_STATUS.retry;
}

export function isWaitingPublishingQueueStatus(value) {
  const normalized = normalizePublishingQueueStatus(value);
  return normalized === PUBLISHING_QUEUE_STATUS.pending || normalized === PUBLISHING_QUEUE_STATUS.retry;
}

export function isActivePublishingQueueStatus(value) {
  const normalized = normalizePublishingQueueStatus(value);
  return (
    normalized === PUBLISHING_QUEUE_STATUS.pending ||
    normalized === PUBLISHING_QUEUE_STATUS.sending ||
    normalized === PUBLISHING_QUEUE_STATUS.retry
  );
}
