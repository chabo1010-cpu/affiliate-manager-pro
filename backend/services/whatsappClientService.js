import { getWhatsappDeliveryConfig } from '../env.js';
import { cleanText } from './dealHistoryService.js';

const DEFAULT_TIMEOUT_MS = 12_000;

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

function buildWhatsappClientError(message, options = {}) {
  const error = new Error(message);
  error.retryable = options.retryable !== false;

  if (Number.isFinite(options.retryLimit)) {
    error.retryLimit = Number(options.retryLimit);
  }

  if (options.details !== undefined) {
    error.details = options.details;
  }

  return error;
}

function isRetryableStatusCode(statusCode) {
  if (!Number.isFinite(statusCode)) {
    return true;
  }

  if ([408, 425, 429].includes(statusCode)) {
    return true;
  }

  if (statusCode >= 500) {
    return true;
  }

  return false;
}

function resolveProviderMessage(payload, fallbackMessage) {
  if (!payload || typeof payload !== 'object') {
    return fallbackMessage;
  }

  return (
    cleanText(payload.error) ||
    cleanText(payload.message) ||
    cleanText(payload.detail) ||
    cleanText(payload.reason) ||
    fallbackMessage
  );
}

function normalizeTargetMeta(value) {
  if (!value) {
    return null;
  }

  if (typeof value === 'object') {
    return value;
  }

  if (typeof value === 'string') {
    return parseJson(value, null);
  }

  return null;
}

function buildRequestBody(input = {}, config = getWhatsappClientConfig()) {
  const queuePayload = input.queuePayload && typeof input.queuePayload === 'object' ? input.queuePayload : {};
  const imageUrl = cleanText(input.imageUrl);
  const targetMeta = normalizeTargetMeta(input.targetMeta);

  return {
    channel: 'whatsapp',
    sender: cleanText(input.sender) || cleanText(config.sender) || null,
    targetRef: cleanText(input.targetRef) || null,
    targetLabel: cleanText(input.targetLabel) || null,
    targetMeta,
    text: cleanText(input.text),
    imageUrl: imageUrl || null,
    imageSource: cleanText(input.imageSource) || 'none',
    link: cleanText(input.link || queuePayload.link) || null,
    couponCode: cleanText(input.couponCode) || null,
    asin: cleanText(input.asin || queuePayload.asin).toUpperCase() || null,
    normalizedUrl: cleanText(input.normalizedUrl || queuePayload.normalizedUrl) || null,
    title: cleanText(input.title || queuePayload.title) || null,
    queueId: Number.isFinite(Number(input.queueId)) ? Number(input.queueId) : null,
    sourceType: cleanText(input.sourceType || queuePayload.databaseSourceType) || 'publisher_queue',
    origin: cleanText(input.origin || queuePayload.databaseOrigin) || 'automatic',
    payloadVersion: 1
  };
}

async function sendWhatsappRequest(input = {}, config = getWhatsappClientConfig()) {
  const controller = new AbortController();
  const timeoutHandle = setTimeout(() => controller.abort(), DEFAULT_TIMEOUT_MS);

  try {
    return await fetch(config.endpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...(config.tokenConfigured ? { Authorization: `Bearer ${config.token}` } : {})
      },
      body: JSON.stringify(buildRequestBody(input, config)),
      signal: controller.signal
    });
  } catch (error) {
    if (error?.name === 'AbortError') {
      throw buildWhatsappClientError('WhatsApp Delivery Gateway hat nicht rechtzeitig geantwortet.', {
        retryable: true,
        retryLimit: config.retryLimit
      });
    }

    throw buildWhatsappClientError(
      `WhatsApp Delivery Gateway nicht erreichbar: ${error instanceof Error ? error.message : 'Unbekannter Fehler'}`,
      {
        retryable: true,
        retryLimit: config.retryLimit
      }
    );
  } finally {
    clearTimeout(timeoutHandle);
  }
}

export function getWhatsappClientConfig() {
  const config = getWhatsappDeliveryConfig();
  const endpoint = cleanText(config.endpoint);
  const token = cleanText(config.token);
  const sender = cleanText(config.sender);

  return {
    enabled: config.enabled === true,
    endpointConfigured: Boolean(endpoint),
    endpoint,
    tokenConfigured: Boolean(token),
    token,
    senderConfigured: Boolean(sender),
    sender,
    retryLimit: Number(config.retryLimit || 0)
  };
}

export function getWhatsappClientRetryLimit() {
  return getWhatsappClientConfig().retryLimit;
}

export async function sendWhatsappDeal(input = {}) {
  const config = getWhatsappClientConfig();
  const text = cleanText(input.text);

  if (!config.enabled) {
    throw buildWhatsappClientError('WhatsApp Client ist deaktiviert.', {
      retryable: false
    });
  }

  if (!config.endpointConfigured) {
    throw buildWhatsappClientError('WHATSAPP_DELIVERY_ENDPOINT fehlt im Backend.', {
      retryable: false
    });
  }

  if (!text) {
    throw buildWhatsappClientError('WhatsApp-Text fuer den Versand fehlt.', {
      retryable: false
    });
  }

  const requestBody = buildRequestBody(input, config);
  const response = await sendWhatsappRequest(input, config);
  const rawResponse = await response.text();
  const parsedResponse = parseJson(rawResponse, null);

  if (!response.ok) {
    throw buildWhatsappClientError(
      resolveProviderMessage(parsedResponse, `WhatsApp Delivery Gateway antwortete mit ${response.status}.`),
      {
        retryable: isRetryableStatusCode(response.status),
        retryLimit: config.retryLimit,
        details: {
          status: response.status,
          body: parsedResponse || rawResponse || null
        }
      }
    );
  }

  if (
    parsedResponse &&
    typeof parsedResponse === 'object' &&
    (parsedResponse.ok === false ||
      parsedResponse.success === false ||
      cleanText(parsedResponse.status).toLowerCase() === 'error')
  ) {
    const retryable =
      typeof parsedResponse.retryable === 'boolean'
        ? parsedResponse.retryable
        : isRetryableStatusCode(Number(parsedResponse.statusCode || parsedResponse.status_code || 500));

    throw buildWhatsappClientError(
      resolveProviderMessage(parsedResponse, 'WhatsApp Gateway hat den Versand abgelehnt.'),
      {
        retryable,
        retryLimit: config.retryLimit,
        details: parsedResponse
      }
    );
  }

  return {
    status: cleanText(parsedResponse?.status) || 'sent',
    messageId: cleanText(parsedResponse?.messageId || parsedResponse?.message_id),
    deliveryId: cleanText(parsedResponse?.deliveryId || parsedResponse?.delivery_id),
    sender: requestBody.sender,
    targetRef: requestBody.targetRef,
    targetLabel: requestBody.targetLabel,
    endpoint: config.endpoint,
    response: parsedResponse || (rawResponse ? { raw: rawResponse } : null)
  };
}
