import { getDb } from '../db.js';
import { getReaderRuntimeConfig, getTelegramTestGroupConfig } from '../env.js';
import { assertDealNotLocked, cleanText } from './dealHistoryService.js';
import { buildGeneratorDealContext } from './generatorDealScoringService.js';
import { logGeneratorDebug } from './generatorFlowService.js';
import { createPublishingEntry, processPublishingQueueEntry } from './publisherService.js';
import { isFailedPublishingQueueStatus, normalizePublishingQueueStatus } from './publishingQueueStateService.js';

const db = getDb();
const DEBUG_QUEUE_ID_PLACEHOLDER = '__QUEUE_ID__';

function getDealLockBypassMeta(explicitSkipDealLock = false) {
  const runtimeConfig = getReaderRuntimeConfig();
  return {
    active: explicitSkipDealLock === true || runtimeConfig.dealLockBypass,
    explicitSkipDealLock: explicitSkipDealLock === true,
    readerTestMode: runtimeConfig.readerTestMode,
    readerDebugMode: runtimeConfig.readerDebugMode
  };
}

function nowIso() {
  return new Date().toISOString();
}

function insertGeneratorPost(input = {}) {
  const timestamp = nowIso();
  const result = db
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
          keepa_result_id,
          generator_context_json,
          created_at,
          updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
      input.generatorContext?.keepa?.keepaResultId || null,
      input.generatorContext ? JSON.stringify(input.generatorContext) : null,
      timestamp,
      timestamp
    );

  return result.lastInsertRowid;
}

function updateGeneratorPostMeta(generatorPostId, meta = {}) {
  db.prepare(
    `
      UPDATE generator_posts
      SET keepa_result_id = @keepaResultId,
          generator_context_json = @generatorContextJson,
          telegram_message_id = @telegramMessageId,
          posted_channels_json = @postedChannelsJson,
          updated_at = @updatedAt
      WHERE id = @id
    `
  ).run({
    id: generatorPostId,
    keepaResultId: meta.keepaResultId ?? null,
    generatorContextJson: meta.generatorContext ? JSON.stringify(meta.generatorContext) : null,
    telegramMessageId: meta.telegramMessageId ?? null,
    postedChannelsJson: JSON.stringify(meta.postedChannels || null),
    updatedAt: nowIso()
  });
}

function serializeUploadedFileAsDataUrl(uploadedFile) {
  if (!uploadedFile?.buffer || !Buffer.isBuffer(uploadedFile.buffer) || uploadedFile.buffer.length === 0) {
    return '';
  }

  const mimeType =
    typeof uploadedFile.mimetype === 'string' && uploadedFile.mimetype.trim()
      ? uploadedFile.mimetype.trim()
      : 'image/jpeg';

  return `data:${mimeType};base64,${uploadedFile.buffer.toString('base64')}`;
}

function parsePublishingPriceValue(value = '') {
  const raw = cleanText(String(value || '')).replace(/[^0-9.,-]/g, '');
  if (!raw) {
    return null;
  }

  let normalized = raw;
  if (raw.includes(',') && raw.includes('.')) {
    normalized =
      raw.lastIndexOf(',') > raw.lastIndexOf('.')
        ? raw.replace(/\./g, '').replace(',', '.')
        : raw.replace(/,/g, '');
  } else if (raw.includes(',')) {
    normalized = raw.replace(',', '.');
  }

  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : null;
}

function validatePublishingPrice(input = {}) {
  const parsedPrice = parsePublishingPriceValue(input.currentPrice);
  if (parsedPrice !== null && parsedPrice > 0) {
    return {
      valid: true,
      parsedPrice,
      reason: ''
    };
  }

  return {
    valid: false,
    parsedPrice,
    reason: cleanText(input.currentPrice) ? 'Preis ist 0,00€ oder ungueltig.' : 'Preis fehlt oder ist ungueltig.'
  };
}

function buildDirectPublishingPayload(input = {}, generatorPostId, generatorContext) {
  const testGroupConfig = getTelegramTestGroupConfig();
  const uploadedImageDataUrl = serializeUploadedFileAsDataUrl(input.uploadedImageFile);

  return {
    generatorPostId,
    generatorContext,
    link: cleanText(input.link),
    normalizedUrl: cleanText(input.normalizedUrl || input.link),
    asin: cleanText(input.asin).toUpperCase(),
    sellerType: cleanText(input.sellerType) || 'FBM',
    sellerClass: cleanText(input.sellerClass) || '',
    soldByAmazon: input.soldByAmazon ?? null,
    shippedByAmazon: input.shippedByAmazon ?? null,
    title: cleanText(input.title),
    currentPrice: cleanText(input.currentPrice),
    oldPrice: cleanText(input.oldPrice),
    couponCode: cleanText(input.couponCode),
    textByChannel: input.textByChannel && typeof input.textByChannel === 'object' ? input.textByChannel : {},
    telegramChatIds: testGroupConfig.chatId ? [String(testGroupConfig.chatId)] : [],
    imageVariants: {
      standard: cleanText(input.generatedImagePath),
      upload: uploadedImageDataUrl || cleanText(input.uploadedImagePath)
    },
    targetImageSources: {
      telegram: cleanText(input.telegramImageSource) || 'standard',
      whatsapp: cleanText(input.whatsappImageSource) || 'standard',
      facebook: cleanText(input.facebookImageSource) || 'link_preview'
    }
  };
}

function replaceQueueIdPlaceholder(value, queueId) {
  if (typeof value !== 'string' || !value.includes(DEBUG_QUEUE_ID_PLACEHOLDER)) {
    return value;
  }

  return value.replaceAll(DEBUG_QUEUE_ID_PLACEHOLDER, queueId ? String(queueId) : 'n/a');
}

function applyQueueIdPlaceholderToPayload(payload = {}, queueId) {
  const nextTextByChannel =
    payload.textByChannel && typeof payload.textByChannel === 'object'
      ? Object.fromEntries(
          Object.entries(payload.textByChannel).map(([channel, text]) => [channel, replaceQueueIdPlaceholder(text, queueId)])
        )
      : payload.textByChannel;

  return {
    ...payload,
    textByChannel: nextTextByChannel
  };
}

function persistPublishingPayload(queueId, generatorPostId, payload = {}) {
  db.prepare(`UPDATE publishing_queue SET payload_json = ?, updated_at = ? WHERE id = ?`).run(
    JSON.stringify(payload ?? {}),
    nowIso(),
    queueId
  );

  db.prepare(
    `
      UPDATE generator_posts
      SET telegram_text = ?,
          whatsapp_text = ?,
          facebook_text = ?,
          updated_at = ?
      WHERE id = ?
    `
  ).run(
    cleanText(payload?.textByChannel?.telegram),
    cleanText(payload?.textByChannel?.whatsapp),
    cleanText(payload?.textByChannel?.facebook),
    nowIso(),
    generatorPostId
  );
}

function buildDirectPublishingTargets(input = {}) {
  return [
    { channelType: 'telegram', isEnabled: input.enableTelegram !== false, imageSource: input.telegramImageSource },
    { channelType: 'whatsapp', isEnabled: input.enableWhatsapp === true, imageSource: input.whatsappImageSource },
    { channelType: 'facebook', isEnabled: input.enableFacebook === true, imageSource: input.facebookImageSource }
  ];
}

function buildEmptyChannelResult(channelType = '', imageSource = '') {
  return {
    channelType: cleanText(channelType).toLowerCase(),
    status: 'pending',
    imageSource: cleanText(imageSource) || '',
    deliveries: [],
    messageId: null,
    chatId: null
  };
}

function buildChannelResult(base = null, nextResult = {}) {
  const safeBase =
    base && typeof base === 'object'
      ? {
          ...buildEmptyChannelResult(base.channelType, base.imageSource),
          ...base,
          deliveries: Array.isArray(base.deliveries) ? base.deliveries : []
        }
      : buildEmptyChannelResult(nextResult.channelType, nextResult.imageSource);
  const deliveries = Array.isArray(nextResult.deliveries) ? nextResult.deliveries : [];
  return {
    channelType: cleanText(nextResult.channelType || safeBase.channelType).toLowerCase(),
    status: nextResult.status || safeBase.status || 'pending',
    imageSource: nextResult.imageSource || safeBase.imageSource || '',
    deliveries: [...safeBase.deliveries, ...deliveries],
    messageId: nextResult.messageId || safeBase.messageId || null,
    chatId: nextResult.chatId || safeBase.chatId || null
  };
}

function summarizeQueueResults(queueProcessingResult = {}, input = {}, generatorPostId) {
  const queue = queueProcessingResult.queue || null;
  const queueTargets = Array.isArray(queue?.targets) ? queue.targets : [];
  const processingResults = Array.isArray(queueProcessingResult.results) ? queueProcessingResult.results : [];
  const results = {
    generatorPostId,
    telegram: buildEmptyChannelResult('telegram', cleanText(input.telegramImageSource) || 'standard'),
    whatsapp: buildEmptyChannelResult('whatsapp', cleanText(input.whatsappImageSource) || 'standard'),
    facebook: buildEmptyChannelResult('facebook', cleanText(input.facebookImageSource) || 'link_preview')
  };

  processingResults.forEach((entry) => {
    if (entry.channelType === 'telegram') {
      const deliveries = Array.isArray(entry.workerResult?.targets) ? entry.workerResult.targets : [];
      results.telegram = buildChannelResult(results.telegram, {
        channelType: 'telegram',
        status: entry.status,
        imageSource: cleanText(input.telegramImageSource) || 'standard',
        deliveries,
        messageId: deliveries[0]?.messageId || null,
        chatId: deliveries[0]?.chatId || deliveries[0]?.targetChatId || null
      });
      return;
    }

    if (entry.channelType === 'whatsapp') {
      results.whatsapp = buildChannelResult(results.whatsapp, {
        channelType: 'whatsapp',
        status: entry.status,
        imageSource: cleanText(input.whatsappImageSource) || 'standard'
      });
      return;
    }

    if (entry.channelType === 'facebook') {
      results.facebook = buildChannelResult(results.facebook, {
        channelType: 'facebook',
        status: entry.status,
        imageSource: cleanText(input.facebookImageSource) || 'link_preview'
      });
    }
  });

  queueTargets.forEach((target) => {
    const normalizedStatus = normalizePublishingQueueStatus(target?.status);
    const channelType = cleanText(target?.channel_type).toLowerCase();
    if (!channelType || !results[channelType] || !normalizedStatus) {
      return;
    }

    results[channelType] = buildChannelResult(results[channelType], {
      channelType,
      status: normalizedStatus,
      imageSource: results[channelType].imageSource
    });
  });

  const sentTarget = queueTargets.find((target) => normalizePublishingQueueStatus(target.status) === 'sent') || null;
  const deliveries = {
    telegram: Array.isArray(results.telegram?.deliveries) ? results.telegram.deliveries : [],
    whatsapp: Array.isArray(results.whatsapp?.deliveries) ? results.whatsapp.deliveries : [],
    facebook: Array.isArray(results.facebook?.deliveries) ? results.facebook.deliveries : []
  };

  return {
    queue,
    results,
    deliveries,
    postedAt: sentTarget?.posted_at || null,
    telegramMessageId: results.telegram?.messageId || null
  };
}

function buildQueueFailureError(summary = {}) {
  const failedTarget = (summary.queue?.targets || []).find((target) => normalizePublishingQueueStatus(target.status) === 'failed') || null;
  const error = new Error(failedTarget?.error_message || 'Publishing Queue konnte den Deal nicht versenden.');
  error.code = 'PUBLISHING_QUEUE_FAILED';
  error.retryable = false;
  error.queue = summary.queue || null;
  return error;
}

function assertDirectPublishingTargets(input = {}, payload = {}) {
  const targets = buildDirectPublishingTargets(input).filter((target) => target.isEnabled);

  if (!targets.length) {
    const error = new Error('Keine aktiven Ziele fuer den manuellen Test-Post ausgewaehlt.');
    error.code = 'NO_PUBLISH_TARGETS_SELECTED';
    error.retryable = false;
    throw error;
  }

  const hasTelegramTarget =
    targets.some((target) => cleanText(target.channelType).toLowerCase() === 'telegram') &&
    Array.isArray(payload.telegramChatIds) &&
    payload.telegramChatIds.length > 0;

  if (!hasTelegramTarget) {
    const error = new Error('Keine Telegram-Zielgruppe fuer den manuellen Test-Post verfuegbar.');
    error.code = 'NO_TELEGRAM_PUBLISH_TARGET';
    error.retryable = false;
    throw error;
  }

  return targets;
}

export async function publishGeneratorPostDirect(input = {}) {
  const generatorContext =
    input.generatorContext ||
    (await buildGeneratorDealContext({
      asin: input.asin,
      sellerType: input.sellerType,
      sellerClass: input.sellerClass,
      soldByAmazon: input.soldByAmazon,
      shippedByAmazon: input.shippedByAmazon,
      currentPrice: input.currentPrice,
      title: input.title,
      productUrl: input.normalizedUrl || input.link,
      imageUrl: input.generatedImagePath,
      source: cleanText(input.contextSource) || 'generator_direct_publish',
      origin: cleanText(input.originOverride) || 'manual'
    }));
  const preparedInput = {
    ...input,
    generatorContext
  };
  const priceValidation = validatePublishingPrice(preparedInput);

  if (!priceValidation.valid) {
    if (input.allowInvalidPriceTestPost === true) {
      console.info('[TEST_POST_INVALID_PRICE_ONLY]', {
        asin: cleanText(input.asin).toUpperCase(),
        sourceType: cleanText(input.queueSourceType) || 'generator_direct',
        reason: priceValidation.reason
      });
    } else {
      console.error('[POST_BLOCKED_INVALID_PRICE]', {
        asin: cleanText(input.asin).toUpperCase(),
        sourceType: cleanText(input.queueSourceType) || 'generator_direct',
        reason: priceValidation.reason
      });
      const invalidPriceError = new Error(priceValidation.reason);
      invalidPriceError.code = 'INVALID_PRICE_BLOCKED';
      invalidPriceError.retryable = false;
      throw invalidPriceError;
    }
  }

  const generatorPostId = insertGeneratorPost(preparedInput);

  logGeneratorDebug('GENERATOR DIRECT TEST POST', {
    generatorPostId,
    asin: cleanText(input.asin).toUpperCase(),
    sellerType: cleanText(input.sellerType) || 'FBM',
    decision: generatorContext?.learning?.routingDecision || generatorContext?.evaluation?.decision || 'manual_review',
    testGroupApproved: generatorContext?.learning?.routingDecision === 'test_group',
    internetStatus: generatorContext?.internet?.status || 'missing',
    keepaStatus: generatorContext?.keepa?.status || 'missing'
  });

  const dealLockBypass = getDealLockBypassMeta(input.skipDealLock === true);
  const skipDealLock = dealLockBypass.active;
  const dealLock = skipDealLock
    ? {
        blocked: false,
        dealHash: null
      }
    : (() => {
        try {
          return assertDealNotLocked({
            asin: input.asin,
            url: input.link,
            normalizedUrl: input.normalizedUrl || input.link,
            sourceType: cleanText(input.queueSourceType) || 'generator_direct',
            origin: cleanText(input.originOverride) || 'manual'
          });
        } catch (error) {
          console.error('[DEAL_LOCK_BLOCKED]', {
            phase: 'direct_publish_pre_queue',
            sourceType: cleanText(input.queueSourceType) || 'generator_direct',
            sourceId: generatorPostId,
            asin: cleanText(input.asin).toUpperCase() || '',
            normalizedUrl: cleanText(input.normalizedUrl || input.link) || '',
            reason: error instanceof Error ? error.message : 'Deal-Lock aktiv.',
            blockCode: error instanceof Error ? error.code || error.dealLock?.blockCode || '' : '',
            readerTestMode: dealLockBypass.readerTestMode,
            readerDebugMode: dealLockBypass.readerDebugMode
          });
          throw error;
        }
      })();

  if (skipDealLock) {
    console.info('[DEAL_LOCK_BYPASSED]', {
      phase: 'direct_publish_pre_queue',
      sourceType: cleanText(input.queueSourceType) || 'generator_direct',
      sourceId: generatorPostId,
      asin: cleanText(input.asin).toUpperCase() || '',
      normalizedUrl: cleanText(input.normalizedUrl || input.link) || '',
      explicitSkipDealLock: dealLockBypass.explicitSkipDealLock,
      readerTestMode: dealLockBypass.readerTestMode,
      readerDebugMode: dealLockBypass.readerDebugMode
    });
    console.info('[DEAL_LOCK_FORCE_DISABLED]', {
      phase: 'direct_publish_pre_queue',
      sourceType: cleanText(input.queueSourceType) || 'generator_direct',
      sourceId: generatorPostId,
      asin: cleanText(input.asin).toUpperCase() || ''
    });
  }

  logGeneratorDebug('DEAL LOCK CHECK BEFORE DIRECT POST', {
    generatorPostId,
    asin: cleanText(input.asin).toUpperCase(),
    blocked: dealLock.blocked,
    dealHash: dealLock.dealHash || null,
    skipped: skipDealLock
  });

  const publishingPayload = buildDirectPublishingPayload(input, generatorPostId, generatorContext);
  let publishingTargets;
  try {
    publishingTargets = assertDirectPublishingTargets(input, publishingPayload);
  } catch (error) {
    const queuePreparationError =
      error instanceof Error ? error.message : 'Publishing-Ziele konnten nicht vorbereitet werden.';
    console.error('[QUEUE_ERROR]', {
      sourceType: cleanText(input.queueSourceType) || 'generator_direct',
      sourceId: generatorPostId,
      asin: cleanText(input.asin).toUpperCase() || '',
      error: queuePreparationError
    });
    console.error('[ERROR_REASON]', {
      reason: queuePreparationError,
      sourceType: cleanText(input.queueSourceType) || 'generator_direct',
      sourceId: generatorPostId
    });
    throw error;
  }

  let queueEntry;
  try {
    queueEntry = createPublishingEntry({
      sourceType: cleanText(input.queueSourceType) || 'generator_direct',
      sourceId: generatorPostId,
      originOverride: cleanText(input.originOverride) || 'manual',
      skipDealLock,
      payload: {
        ...publishingPayload,
        ...(skipDealLock ? { skipDealLock: true } : {})
      },
      targets: publishingTargets
    });
  } catch (error) {
    const queueErrorMessage = error instanceof Error ? error.message : 'Queue-Eintrag konnte nicht erstellt werden.';
    console.error('[QUEUE_ERROR]', {
      sourceType: cleanText(input.queueSourceType) || 'generator_direct',
      sourceId: generatorPostId,
      asin: cleanText(input.asin).toUpperCase() || '',
      error: queueErrorMessage
    });
    console.error('[ERROR_REASON]', {
      reason: queueErrorMessage,
      sourceType: cleanText(input.queueSourceType) || 'generator_direct',
      sourceId: generatorPostId
    });
    throw error;
  }

  const finalizedPublishingPayload = applyQueueIdPlaceholderToPayload(publishingPayload, queueEntry?.id || null);
  if (finalizedPublishingPayload.textByChannel !== publishingPayload.textByChannel) {
    persistPublishingPayload(queueEntry.id, generatorPostId, finalizedPublishingPayload);
  }

  console.info('[QUEUE_JOB_CREATED]', {
    queueId: queueEntry?.id || null,
    sourceType: cleanText(input.queueSourceType) || 'generator_direct',
    sourceId: generatorPostId,
    asin: cleanText(input.asin).toUpperCase() || ''
  });

  logGeneratorDebug('MANUAL POST SAVED TO QUEUE', {
    generatorPostId,
    queueId: queueEntry?.id || null,
    asin: cleanText(input.asin).toUpperCase()
  });

  let queueProcessingResult;
  try {
    console.info('[PUBLISHER_FORCE_SEND]', {
      queueId: queueEntry?.id || null,
      sourceType: cleanText(input.queueSourceType) || 'generator_direct',
      sourceId: generatorPostId,
      asin: cleanText(input.asin).toUpperCase() || ''
    });
    queueProcessingResult = await processPublishingQueueEntry(queueEntry.id);
  } catch (error) {
    const publisherErrorMessage =
      error instanceof Error ? error.message : 'Publisher konnte den Queue-Eintrag nicht verarbeiten.';
    console.error('[PUBLISHER_ERROR]', {
      queueId: queueEntry?.id || null,
      sourceType: cleanText(input.queueSourceType) || 'generator_direct',
      sourceId: generatorPostId,
      asin: cleanText(input.asin).toUpperCase() || '',
      error: publisherErrorMessage
    });
    console.error('[ERROR_REASON]', {
      reason: publisherErrorMessage,
      queueId: queueEntry?.id || null,
      sourceType: cleanText(input.queueSourceType) || 'generator_direct'
    });
    console.error('[PUBLISHER_FORCE_ERROR]', {
      queueId: queueEntry?.id || null,
      sourceType: cleanText(input.queueSourceType) || 'generator_direct',
      sourceId: generatorPostId,
      asin: cleanText(input.asin).toUpperCase() || '',
      reason: publisherErrorMessage
    });
    throw error;
  }
  const summary = summarizeQueueResults(queueProcessingResult, input, generatorPostId);

  updateGeneratorPostMeta(generatorPostId, {
    keepaResultId: generatorContext?.keepa?.keepaResultId || null,
    generatorContext,
    telegramMessageId: summary.telegramMessageId,
    postedChannels: {
      ...summary.results,
      queue: {
        id: summary.queue?.id || queueEntry?.id || null,
        status: summary.queue?.status || queueEntry?.status || 'pending'
      }
    }
  });

  if (isFailedPublishingQueueStatus(summary.queue?.status)) {
    const queueFailureError = buildQueueFailureError(summary);
    console.error('[QUEUE_ERROR]', {
      queueId: summary.queue?.id || queueEntry?.id || null,
      sourceType: cleanText(input.queueSourceType) || 'generator_direct',
      sourceId: generatorPostId,
      asin: cleanText(input.asin).toUpperCase() || '',
      error: queueFailureError.message
    });
    console.error('[ERROR_REASON]', {
      reason: queueFailureError.message,
      queueId: summary.queue?.id || queueEntry?.id || null
    });
    throw queueFailureError;
  }

  if (summary.results.telegram?.messageId) {
    console.info('[PUBLISHER_FORCE_SUCCESS]', {
      queueId: summary.queue?.id || queueEntry?.id || null,
      sourceType: cleanText(input.queueSourceType) || 'generator_direct',
      sourceId: generatorPostId,
      asin: cleanText(input.asin).toUpperCase() || '',
      telegramMessageId: summary.results.telegram.messageId
    });
    logGeneratorDebug('TEST GROUP POST SENT', {
      generatorPostId,
      asin: cleanText(input.asin).toUpperCase(),
      messageId: summary.results.telegram.messageId,
      chatId: summary.results.telegram.chatId || null,
      imageSource: input.telegramImageSource || 'standard'
    });
  }

  return {
    success: true,
    postedAt: summary.postedAt || null,
    queue: summary.queue || queueEntry,
    results: summary.results,
    deliveries: summary.deliveries,
    generatorContext
  };
}

export const __testablesDirectPublisher = {
  buildEmptyChannelResult,
  buildChannelResult,
  summarizeQueueResults,
  assertDirectPublishingTargets,
  validatePublishingPrice
};
