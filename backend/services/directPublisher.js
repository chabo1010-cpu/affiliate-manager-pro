import { getDb } from '../db.js';
import { getTelegramTestGroupConfig } from '../env.js';
import { assertDealNotLocked, cleanText } from './dealHistoryService.js';
import { buildGeneratorDealContext } from './generatorDealScoringService.js';
import { logGeneratorDebug } from './generatorFlowService.js';
import { createPublishingEntry, processPublishingQueueEntry } from './publisherService.js';
import { isFailedPublishingQueueStatus, normalizePublishingQueueStatus } from './publishingQueueStateService.js';

const db = getDb();

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
  const generatorContext = await buildGeneratorDealContext({
    asin: input.asin,
    sellerType: input.sellerType,
    currentPrice: input.currentPrice,
    title: input.title,
    productUrl: input.normalizedUrl || input.link,
    imageUrl: input.generatedImagePath,
    source: 'generator_direct_publish'
  });
  const preparedInput = {
    ...input,
    generatorContext
  };
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

  const dealLock = assertDealNotLocked({
    asin: input.asin,
    url: input.link,
    normalizedUrl: input.normalizedUrl || input.link,
    sourceType: 'generator_direct',
    origin: 'manual'
  });

  logGeneratorDebug('DEAL LOCK CHECK BEFORE DIRECT POST', {
    generatorPostId,
    asin: cleanText(input.asin).toUpperCase(),
    blocked: dealLock.blocked,
    dealHash: dealLock.dealHash || null
  });

  const publishingPayload = buildDirectPublishingPayload(input, generatorPostId, generatorContext);
  const publishingTargets = assertDirectPublishingTargets(input, publishingPayload);

  const queueEntry = createPublishingEntry({
    sourceType: 'generator_direct',
    sourceId: generatorPostId,
    originOverride: 'manual',
    payload: publishingPayload,
    targets: publishingTargets
  });

  logGeneratorDebug('MANUAL POST SAVED TO QUEUE', {
    generatorPostId,
    queueId: queueEntry?.id || null,
    asin: cleanText(input.asin).toUpperCase()
  });

  const queueProcessingResult = await processPublishingQueueEntry(queueEntry.id);
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
    throw buildQueueFailureError(summary);
  }

  if (summary.results.telegram?.messageId) {
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
  assertDirectPublishingTargets
};
