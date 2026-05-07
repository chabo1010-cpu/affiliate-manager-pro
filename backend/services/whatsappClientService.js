import { getWhatsappDeliveryConfig } from '../env.js';
import { cleanText } from './dealHistoryService.js';
import { getWhatsappOutputTargetConfig } from './whatsappOutputTargetService.js';
import { assertWhatsappRuntimeReady, getWhatsappRuntimeConfig, getWhatsappRuntimeState, waitForWhatsappSendCooldown } from './whatsappRuntimeService.js';
import {
  getRememberedWhatsappPhaseDelivery,
  rememberWhatsappPhaseDelivery,
  sendWhatsappPlaywrightPhase
} from './whatsappPlaywrightWorkerService.js';

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

  if (cleanText(options.code)) {
    error.code = cleanText(options.code);
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
  const steps = Array.isArray(input.steps)
    ? input.steps.map((step, index) => ({
        kind: cleanText(step.kind) || `step_${index + 1}`,
        text: cleanText(step.text),
        imageUrl: cleanText(step.imageUrl) || null,
        sendId: cleanText(step.sendId) || null,
        phaseIndex: Number.isFinite(Number(step.phaseIndex)) ? Number(step.phaseIndex) : index + 1
      }))
    : [];

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
    payloadVersion: 2,
    sendId: cleanText(input.sendId) || null,
    phase: cleanText(input.phase) || null,
    phaseIndex: Number.isFinite(Number(input.phaseIndex)) ? Number(input.phaseIndex) : null,
    phaseCount: Number.isFinite(Number(input.phaseCount)) ? Number(input.phaseCount) : steps.length || null,
    steps
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
  const runtime = getWhatsappRuntimeState();
  const runtimeConfig = getWhatsappRuntimeConfig();
  const targetConfig = getWhatsappOutputTargetConfig();
  const endpoint = cleanText(config.endpoint);
  const token = cleanText(config.token);
  const sender = cleanText(config.sender) || cleanText(targetConfig.targets?.[0]?.name);
  const runtimeProviderMode = cleanText(runtime.providerMode || runtimeConfig.providerMode);
  const providerMode =
    runtimeProviderMode === 'playwright'
      ? 'playwright'
      : endpoint
        ? 'delivery_gateway'
        : runtimeConfig.playwrightAvailable === true
          ? 'playwright'
          : 'delivery_gateway';
  const providerConfigured =
    providerMode === 'playwright'
      ? runtimeConfig.playwrightAvailable === true || runtime.providerConfigured === true
      : Boolean(endpoint);

  return {
    enabled: config.enabled === true,
    endpointConfigured: providerConfigured,
    providerConfigured,
    providerMode,
    providerLabel:
      providerMode === 'playwright'
        ? 'Playwright Worker'
        : cleanText(runtime.providerLabel || runtimeConfig.providerLabel || 'Delivery Gateway'),
    endpoint,
    tokenConfigured: Boolean(token),
    token,
    senderConfigured: Boolean(sender),
    sender,
    retryLimit: Number(config.retryLimit || 0),
    workerEnabled: runtime.workerEnabled === true,
    controlEndpointConfigured: runtime.controlEndpointConfigured === true,
    browserChannel: cleanText(runtime.browserChannel || runtimeConfig.browserChannel),
    browserExecutablePath: cleanText(runtime.browserExecutablePath || runtimeConfig.browserExecutablePath)
  };
}

export function getWhatsappClientRetryLimit() {
  return getWhatsappClientConfig().retryLimit;
}

function buildWhatsappPhases(input = {}) {
  const text = cleanText(input.text);
  const imageUrl = cleanText(input.imageUrl);
  const couponCode = cleanText(input.couponCode);
  const sendId = cleanText(input.sendId);
  const phases = [];

  phases.push({
    kind: 'main',
    text,
    imageUrl: imageUrl || null,
    sendId: sendId ? `${sendId}:main` : '',
    phaseIndex: 1
  });

  if (couponCode) {
    phases.push({
      kind: 'coupon',
      text: couponCode,
      imageUrl: null,
      sendId: sendId ? `${sendId}:coupon` : '',
      phaseIndex: 2
    });
  }

  return phases;
}

async function sendWhatsappPhase(input = {}, config = getWhatsappClientConfig()) {
  const requestBody = buildRequestBody(input, config);
  const rememberedDelivery = getRememberedWhatsappPhaseDelivery(requestBody.sendId);
  if (rememberedDelivery?.status === 'sent') {
    return {
      status: 'sent',
      duplicatePrevented: true,
      messageId: cleanText(rememberedDelivery.messageId),
      deliveryId: cleanText(rememberedDelivery.deliveryId || rememberedDelivery.messageId),
      sender: requestBody.sender,
      targetRef: requestBody.targetRef,
      targetLabel: requestBody.targetLabel,
      phase: cleanText(requestBody.phase) || 'main',
      phaseIndex: Number(requestBody.phaseIndex || 0),
      endpoint: config.endpoint,
      response: rememberedDelivery.response || null
    };
  }

  let parsedResponse = null;
  let rawResponse = '';
  let providerResult = null;

  if (config.providerMode === 'playwright') {
    try {
      providerResult = await sendWhatsappPlaywrightPhase({
        ...input,
        ...requestBody
      });
    } catch (error) {
      throw buildWhatsappClientError(
        error instanceof Error ? error.message : 'WhatsApp Playwright Worker hat den Versand abgebrochen.',
        {
          retryable: !(error instanceof Error && error.retryable === false),
          retryLimit:
            error instanceof Error && Number.isFinite(Number(error.retryLimit))
              ? Number(error.retryLimit)
              : config.retryLimit,
          code: error instanceof Error ? error.code || '' : '',
          details: error instanceof Error ? error.details || null : null
        }
      );
    }
  } else {
    const response = await sendWhatsappRequest(input, config);
    rawResponse = await response.text();
    parsedResponse = parseJson(rawResponse, null);

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

    providerResult = {
      status: cleanText(parsedResponse?.status) || 'sent',
      duplicatePrevented: parsedResponse?.duplicatePrevented === true || parsedResponse?.duplicate_prevented === true,
      messageId: cleanText(parsedResponse?.messageId || parsedResponse?.message_id),
      deliveryId: cleanText(parsedResponse?.deliveryId || parsedResponse?.delivery_id),
      response: parsedResponse || (rawResponse ? { raw: rawResponse } : null)
    };
  }

  const normalizedResult = {
    status: cleanText(providerResult?.status) || 'sent',
    duplicatePrevented: providerResult?.duplicatePrevented === true,
    messageId: cleanText(providerResult?.messageId),
    deliveryId: cleanText(providerResult?.deliveryId || providerResult?.messageId),
    sender: requestBody.sender,
    targetRef: requestBody.targetRef,
    targetLabel: requestBody.targetLabel,
    phase: cleanText(requestBody.phase) || 'main',
    phaseIndex: Number(requestBody.phaseIndex || 0),
    endpoint: config.endpoint,
    response: providerResult?.response || parsedResponse || (rawResponse ? { raw: rawResponse } : null)
  };

  if (cleanText(requestBody.sendId)) {
    rememberWhatsappPhaseDelivery(requestBody.sendId, normalizedResult);
  }

  return normalizedResult;
}

export async function sendWhatsappDeal(input = {}) {
  const config = getWhatsappClientConfig();
  const text = cleanText(input.text);

  if (!config.enabled) {
    throw buildWhatsappClientError('WhatsApp Client ist deaktiviert.', {
      retryable: false
    });
  }

  if (!config.providerConfigured) {
    throw buildWhatsappClientError(
      config.providerMode === 'playwright'
        ? 'WhatsApp Playwright Worker ist nicht vorbereitet.'
        : 'WHATSAPP_DELIVERY_ENDPOINT fehlt im Backend.',
      {
        retryable: false
      }
    );
  }

  if (!text) {
    throw buildWhatsappClientError('WhatsApp-Text fuer den Versand fehlt.', {
      retryable: false
    });
  }

  assertWhatsappRuntimeReady();
  await waitForWhatsappSendCooldown();

  const phases = buildWhatsappPhases(input);
  const phaseResults = [];
  for (const phase of phases) {
    phaseResults.push(
      await sendWhatsappPhase(
        {
          ...input,
          text: phase.text,
          imageUrl: phase.imageUrl,
          sendId: phase.sendId,
          phase: phase.kind,
          phaseIndex: phase.phaseIndex,
          phaseCount: phases.length,
          steps: phases
        },
        config
      )
    );
    await waitForWhatsappSendCooldown();
  }

  const primaryResult = phaseResults[0] || {};

  return {
    status: primaryResult.status || 'sent',
    duplicatePrevented: phaseResults.some((result) => result.duplicatePrevented === true),
    messageId: primaryResult.messageId,
    deliveryId: primaryResult.deliveryId,
    sender: primaryResult.sender,
    targetRef: primaryResult.targetRef,
    targetLabel: primaryResult.targetLabel,
    endpoint: config.endpoint,
    phases: phaseResults,
    response: primaryResult.response || null
  };
}
