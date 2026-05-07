import { cleanText, savePostedDeal } from './dealHistoryService.js';
import { getDb } from '../db.js';
import { buildPublishingChannelLabel } from './databaseService.js';
import { sendWhatsappDeal } from './whatsappClientService.js';
import {
  allocateWhatsappSendId,
  getWhatsappRuntimeState,
  markWhatsappSendStart,
  recordWhatsappSendError,
  recordWhatsappSendSuccess,
  sendWhatsappDuplicatePreventedAlert,
  sendWhatsappLoginRequiredAlert
} from './whatsappRuntimeService.js';

const db = getDb();

function nowIso() {
  return new Date().toISOString();
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

function logWhatsappEvent(target, eventType, message, payload = null, level = 'info') {
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
      ) VALUES (?, ?, 'whatsapp', ?, ?, ?, ?, ?)
    `
  ).run(
    target.queue_id,
    target.id,
    level,
    eventType,
    message,
    payload ? JSON.stringify(payload) : null,
    nowIso()
  );
}

function isWhatsappPublishMirrorTarget(targetMeta = {}, queuePayload = {}) {
  const mirrorRouteKey = cleanText(targetMeta?.mirrorRouteKey || queuePayload?.meta?.whatsappMirrorRouteKey).toLowerCase();
  const sourceType = cleanText(queuePayload?.databaseSourceType).toLowerCase();
  return mirrorRouteKey === 'approved' || sourceType.includes('approved_route');
}

function mapWhatsappMirrorSkipReasonFromError(error) {
  const code = cleanText(error instanceof Error ? error.code || '' : '').toLowerCase();

  if (code === 'whatsapp_output_disabled') {
    return 'whatsapp_disabled';
  }

  if (code === 'whatsapp_worker_stopped') {
    return 'worker_not_running';
  }

  if (['whatsapp_not_connected', 'whatsapp_qr_required', 'whatsapp_session_expired'].includes(code)) {
    return 'session_not_connected';
  }

  if (code === 'whatsapp_duplicate_prevented') {
    return 'duplicate';
  }

  if (code === 'whatsapp_missing_caption') {
    return 'missing_caption';
  }

  if (code === 'whatsapp_missing_image') {
    return 'missing_image';
  }

  return code || 'send_error';
}

function normalizeWhatsappImageSource(value = '') {
  const normalized = cleanText(value).toLowerCase();
  if (normalized === 'standard_upload') {
    return 'standard_upload';
  }
  if (normalized === 'upload') {
    return 'upload';
  }
  if (normalized === 'standard') {
    return 'standard';
  }
  return '';
}

function resolveWhatsappImageInput(queuePayload = {}, target = {}) {
  const imageVariants = queuePayload.imageVariants && typeof queuePayload.imageVariants === 'object' ? queuePayload.imageVariants : {};
  const requestedSource =
    normalizeWhatsappImageSource(target.image_source) ||
    normalizeWhatsappImageSource(queuePayload.targetImageSources?.whatsapp) ||
    'none';
  const candidates = [];
  const seenValues = new Set();

  const pushCandidate = (source, value) => {
    const normalizedValue = cleanText(value);
    if (!normalizedValue || seenValues.has(normalizedValue)) {
      return;
    }
    seenValues.add(normalizedValue);
    candidates.push({
      source,
      value: normalizedValue
    });
  };

  const standardImage = cleanText(imageVariants.standard);
  const uploadImage = cleanText(imageVariants.upload);
  const standardVariantSource = standardImage.startsWith('data:image') ? 'standard_upload' : 'standard';

  if (requestedSource === 'upload') {
    pushCandidate('upload', uploadImage);
    pushCandidate(standardVariantSource, standardImage);
  } else if (requestedSource === 'standard_upload') {
    pushCandidate('standard_upload', standardImage);
    pushCandidate('upload', uploadImage);
  } else {
    pushCandidate(standardVariantSource, standardImage);
    pushCandidate('upload', uploadImage);
  }

  const resolved = candidates[0] || {
    source: requestedSource || 'none',
    value: ''
  };

  return {
    requestedSource,
    resolvedSource: resolved.source || requestedSource || 'none',
    value: resolved.value || '',
    hasImage: Boolean(resolved.value)
  };
}

export async function processWhatsappPublishingTarget(target, queuePayload) {
  const text = queuePayload.textByChannel?.whatsapp || queuePayload.title || '';
  const resolvedImage = resolveWhatsappImageInput(queuePayload, target);
  const imageUrl = resolvedImage.value;
  const targetMeta = parseJson(target.target_meta_json, {});
  const sendId = allocateWhatsappSendId(target);
  const runtime = getWhatsappRuntimeState();
  const isMirrorPublish = isWhatsappPublishMirrorTarget(targetMeta, queuePayload);
  const imageExpected = Boolean(
    resolvedImage.requestedSource && resolvedImage.requestedSource !== 'none'
      ? true
      : cleanText(queuePayload.imageVariants?.standard) || cleanText(queuePayload.imageVariants?.upload)
  );

  if (!text.trim()) {
    if (isMirrorPublish) {
      logWhatsappEvent(
        target,
        'whatsapp.mirror.real_flow.skip_reason',
        '[WHATSAPP_MIRROR_REAL_FLOW_SKIP_REASON] missing_caption',
        {
          sendId,
          reason: 'missing_caption',
          queueId: target.queue_id,
          targetRef: target.target_ref || '',
          targetLabel: target.target_label || ''
        },
        'warning'
      );
    }
    const error = new Error('WhatsApp Payload ohne Text kann nicht verarbeitet werden.');
    error.code = 'WHATSAPP_MISSING_CAPTION';
    error.retryable = false;
    throw error;
  }

  if (imageUrl) {
    logWhatsappEvent(
      target,
      'whatsapp.image.path.found',
      `[WHATSAPP_IMAGE_PATH_FOUND] ${resolvedImage.resolvedSource}`,
      {
        sendId,
        requestedSource: resolvedImage.requestedSource,
        resolvedSource: resolvedImage.resolvedSource,
        imageInput: imageUrl,
        isRemoteUrl: /^https?:\/\//i.test(imageUrl),
        isDataUrl: /^data:image\//i.test(imageUrl)
      }
    );
  } else if (imageExpected) {
    logWhatsappEvent(
      target,
      'whatsapp.image.path.missing',
      `[WHATSAPP_IMAGE_PATH_MISSING] ${resolvedImage.requestedSource || 'none'}`,
      {
        sendId,
        requestedSource: resolvedImage.requestedSource,
        resolvedSource: resolvedImage.resolvedSource,
        hasStandardVariant: Boolean(cleanText(queuePayload.imageVariants?.standard)),
        hasUploadVariant: Boolean(cleanText(queuePayload.imageVariants?.upload))
      },
      'warning'
    );
  }

  if (isMirrorPublish && !cleanText(imageUrl)) {
    logWhatsappEvent(
      target,
      'whatsapp.post.skipped',
      '[WHATSAPP_POST_SKIPPED] missing_image',
      {
        sendId,
        reason: 'missing_image',
        requestedSource: resolvedImage.requestedSource,
        resolvedSource: resolvedImage.resolvedSource
      },
      'warning'
    );
    logWhatsappEvent(
      target,
      'whatsapp.mirror.real_flow.skip_reason',
      '[WHATSAPP_MIRROR_REAL_FLOW_SKIP_REASON] missing_image',
      {
        sendId,
        reason: 'missing_image',
        queueId: target.queue_id,
        targetRef: target.target_ref || '',
        targetLabel: target.target_label || ''
      },
      'warning'
    );
    const error = new Error('WhatsApp Spiegelung benoetigt ein Bild aus dem Veroeffentlicht-Output.');
    error.code = 'WHATSAPP_MISSING_IMAGE';
    error.retryable = false;
    throw error;
  }

  if (isMirrorPublish) {
    logWhatsappEvent(
      target,
      'whatsapp.publish.mirror.start',
      '[WHATSAPP_PUBLISH_MIRROR_START] WhatsApp Spiegelung fuer Veroeffentlicht startet.',
      {
        sendId,
        queueId: target.queue_id,
        targetRef: target.target_ref || '',
        targetLabel: target.target_label || '',
        outputTargetId: targetMeta?.targetId || null
      }
    );
    logWhatsappEvent(
      target,
      'whatsapp.publish.mirror.target.selected',
      `[WHATSAPP_PUBLISH_MIRROR_TARGET_SELECTED] ${target.target_label || target.target_ref || 'WhatsApp Output'}`,
      {
        sendId,
        queueId: target.queue_id,
        targetRef: target.target_ref || '',
        targetLabel: target.target_label || '',
        outputTargetId: targetMeta?.targetId || null
      }
    );
    if (imageUrl) {
      logWhatsappEvent(
        target,
        'whatsapp.publish.mirror.image.start',
      '[WHATSAPP_PUBLISH_MIRROR_IMAGE_START] Bildphase fuer den WhatsApp Spiegelpost startet.',
      {
        sendId,
        imageSource: resolvedImage.resolvedSource,
        hasImage: true
      }
    );
    }
    logWhatsappEvent(
      target,
      'whatsapp.publish.mirror.text.start',
      '[WHATSAPP_PUBLISH_MIRROR_TEXT_START] Textphase fuer den WhatsApp Spiegelpost startet.',
      {
        sendId,
        textLength: text.length
      }
    );
    if (cleanText(queuePayload.couponCode)) {
      logWhatsappEvent(
        target,
        'whatsapp.publish.mirror.coupon.start',
        '[WHATSAPP_PUBLISH_MIRROR_COUPON_START] Coupon-Folgepost fuer den WhatsApp Spiegelpost wird vorbereitet.',
        {
          sendId,
          couponCode: cleanText(queuePayload.couponCode)
        }
      );
    }
    logWhatsappEvent(
      target,
      'whatsapp.mirror.real_flow.send_start',
      '[WHATSAPP_MIRROR_REAL_FLOW_SEND_START] WhatsApp Real-Flow-Spiegelung startet.',
      {
        sendId,
        queueId: target.queue_id,
        targetRef: target.target_ref || '',
        targetLabel: target.target_label || '',
        outputTargetId: targetMeta?.targetId || null
      }
    );
  }

  if (runtime.sessionValid === true && runtime.connectionStatus === 'connected') {
    logWhatsappEvent(target, 'whatsapp.session.ok', '[WHATSAPP_SESSION_OK] WhatsApp Session ist fuer den Versand vorbereitet.', {
      connectionStatus: runtime.connectionStatus,
      workerStatus: runtime.workerStatus,
      sessionValid: runtime.sessionValid
    });
  }
  logWhatsappEvent(target, 'whatsapp.send.start', '[WHATSAPP_SEND_START] WhatsApp Versand startet.', {
    sendId,
    targetRef: target.target_ref || '',
    targetLabel: target.target_label || '',
    outputTargetId: targetMeta?.targetId || null
  });
  markWhatsappSendStart({
    queueId: target.queue_id,
    targetId: target.id,
    sendId
  });

  const postedAt = new Date().toISOString();
  let result;

  try {
    result = await sendWhatsappDeal({
      queuePayload,
      text,
      imageUrl,
      imageSource: resolvedImage.resolvedSource,
      link: queuePayload.link,
      couponCode: queuePayload.couponCode,
      asin: queuePayload.asin,
      normalizedUrl: queuePayload.normalizedUrl || queuePayload.link,
      title: queuePayload.title,
      queueId: target.queue_id,
      targetId: target.id,
      sourceType: cleanText(queuePayload.databaseSourceType) || 'publisher_queue',
      origin: cleanText(queuePayload.databaseOrigin) || 'automatic',
      targetRef: target.target_ref || '',
      targetLabel: target.target_label || '',
      targetMeta: target.target_meta_json || null,
      sendId
    });
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : 'WhatsApp Versand fehlgeschlagen.';
    const code = error instanceof Error ? error.code || '' : '';
    const retryable = !(error instanceof Error && error.retryable === false);

    if (isMirrorPublish) {
      logWhatsappEvent(
        target,
        'whatsapp.publish.mirror.error',
        `[WHATSAPP_PUBLISH_MIRROR_ERROR] ${errorMessage}`,
        {
          sendId,
          code,
          retryable
        },
        'warning'
      );
      logWhatsappEvent(
        target,
        'whatsapp.mirror.real_flow.skip_reason',
        `[WHATSAPP_MIRROR_REAL_FLOW_SKIP_REASON] ${mapWhatsappMirrorSkipReasonFromError(error)}`,
        {
          sendId,
          reason: mapWhatsappMirrorSkipReasonFromError(error),
          code,
          retryable,
          queueId: target.queue_id,
          targetRef: target.target_ref || '',
          targetLabel: target.target_label || ''
        },
        'warning'
      );
    }

    if (code === 'WHATSAPP_IMAGE_UPLOAD_FAILED' || code === 'WHATSAPP_IMAGE_MISSING') {
      logWhatsappEvent(
        target,
        'whatsapp.image.upload.failed',
        `[WHATSAPP_IMAGE_UPLOAD_FAILED] ${errorMessage}`,
        {
          sendId,
          requestedSource: resolvedImage.requestedSource,
          resolvedSource: resolvedImage.resolvedSource,
          imageInput: imageUrl
        },
        'warning'
      );
    }
    logWhatsappEvent(
      target,
      'whatsapp.send.error',
      `[WHATSAPP_SEND_ERROR] ${errorMessage}`,
      {
        sendId,
        code,
        retryable
      },
      'warning'
    );
    if (code === 'WHATSAPP_SESSION_EXPIRED') {
      logWhatsappEvent(
        target,
        'whatsapp.session.expired',
        '[WHATSAPP_SESSION_EXPIRED] WhatsApp Session ist abgelaufen.',
        {
          sendId
        },
        'warning'
      );
    }
    if (code === 'WHATSAPP_SESSION_EXPIRED' || code === 'WHATSAPP_QR_REQUIRED') {
      void sendWhatsappLoginRequiredAlert({
        code,
        openItems: getWhatsappRuntimeState().queue.open
      }).catch(() => null);
    }
    recordWhatsappSendError({
      outputTargetId: targetMeta?.targetId ?? null,
      errorMessage,
      code,
      retryable,
      finalFailure: retryable === false
    });
    throw error;
  }

  const primaryPhase = Array.isArray(result?.phases) ? result.phases[0] || null : null;
  const primaryResponse = primaryPhase?.response && typeof primaryPhase.response === 'object' ? primaryPhase.response : {};
  const imageUploadVerified = primaryResponse.imageUploadVerified === true || primaryResponse.messageKind === 'image_caption';

  if (imageUrl && primaryResponse.imageUploadVerified === false) {
    logWhatsappEvent(
      target,
      'whatsapp.text.sent.without.image.warning',
      '[WHATSAPP_TEXT_SENT_WITHOUT_IMAGE_WARNING] Text wurde ohne bestaetigten Bild-Upload verarbeitet.',
      {
        sendId,
        requestedSource: resolvedImage.requestedSource,
        resolvedSource: resolvedImage.resolvedSource,
        response: primaryResponse
      },
      'warning'
    );
  }

  if (imageUrl && imageUploadVerified) {
    logWhatsappEvent(
      target,
      'whatsapp.image.upload.success',
      '[WHATSAPP_IMAGE_UPLOAD_SUCCESS] WhatsApp Bild-Upload wurde bestaetigt.',
      {
        sendId,
        requestedSource: resolvedImage.requestedSource,
        resolvedSource: resolvedImage.resolvedSource,
        imageInput: imageUrl,
        response: primaryResponse
      }
    );
  }

  if (result.duplicatePrevented === true) {
    if (isMirrorPublish) {
      logWhatsappEvent(
        target,
        'whatsapp.publish.mirror.duplicate.skip',
        '[WHATSAPP_PUBLISH_MIRROR_DUPLICATE_SKIP] Duplicate-Schutz hat den WhatsApp Spiegelpost uebersprungen.',
        {
          sendId,
          phases: result.phases || []
        }
      );
      logWhatsappEvent(
        target,
        'whatsapp.mirror.real_flow.skip_reason',
        '[WHATSAPP_MIRROR_REAL_FLOW_SKIP_REASON] duplicate',
        {
          sendId,
          reason: 'duplicate',
          phases: result.phases || [],
          queueId: target.queue_id,
          targetRef: target.target_ref || '',
          targetLabel: target.target_label || ''
        }
      );
    }
    logWhatsappEvent(target, 'whatsapp.duplicate.prevented', '[WHATSAPP_DUPLICATE_PREVENTED] Provider hat einen Doppelpost verhindert.', {
      sendId,
      phases: result.phases || []
    });
    void sendWhatsappDuplicatePreventedAlert({
      openItems: getWhatsappRuntimeState().queue.open
    }).catch(() => null);
  }
  if (isMirrorPublish) {
    logWhatsappEvent(
      target,
      'whatsapp.publish.mirror.success',
      '[WHATSAPP_PUBLISH_MIRROR_SUCCESS] WhatsApp Spiegelung fuer Veroeffentlicht erfolgreich abgeschlossen.',
      {
        sendId,
        phases: result.phases || []
      }
    );
    logWhatsappEvent(
      target,
      'whatsapp.mirror.real_flow.success',
      '[WHATSAPP_MIRROR_REAL_FLOW_SUCCESS] WhatsApp Real-Flow-Spiegelung erfolgreich abgeschlossen.',
      {
        sendId,
        phases: result.phases || [],
        queueId: target.queue_id,
        targetRef: target.target_ref || '',
        targetLabel: target.target_label || ''
      }
    );
  }
  logWhatsappEvent(target, 'whatsapp.send.success', '[WHATSAPP_SEND_SUCCESS] WhatsApp Versand abgeschlossen.', {
    sendId,
    phases: result.phases || []
  });
  if (imageUrl && imageUploadVerified) {
    logWhatsappEvent(
      target,
      'whatsapp.full.post.success',
      '[WHATSAPP_FULL_POST_SUCCESS] WhatsApp Vollpost mit Bild und Text erfolgreich abgeschlossen.',
      {
        sendId,
        phases: result.phases || []
      }
    );
  }
  recordWhatsappSendSuccess({
    targetId: target.id,
    outputTargetId: targetMeta?.targetId ?? null,
    deliveryRef: result.deliveryId || result.messageId || ''
  });

  if (queuePayload.skipPostedDealHistory !== true) {
    savePostedDeal({
      asin: queuePayload.asin || '',
      originalUrl: queuePayload.link,
      normalizedUrl: queuePayload.normalizedUrl || queuePayload.link,
      title: queuePayload.title,
      currentPrice: queuePayload.currentPrice || '',
      oldPrice: queuePayload.oldPrice || '',
      sellerType: queuePayload.sellerType || 'FBM',
      postedAt,
      channel: buildPublishingChannelLabel('whatsapp', target.target_label || result.targetLabel || target.target_ref || ''),
      couponCode: queuePayload.couponCode || '',
      sourceType: cleanText(queuePayload.databaseSourceType) || 'publisher_queue',
      sourceId: queuePayload.generatorPostId || queuePayload.sourceId || null,
      queueId: target.queue_id,
      origin: cleanText(queuePayload.databaseOrigin) || 'automatic',
      decisionReason: 'WhatsApp Queue-Target erfolgreich verarbeitet.',
      meta: {
        targetId: target.id,
        sendId,
        delivery: result
      }
    });
  }

  return {
    ...result,
    sendId
  };
}
