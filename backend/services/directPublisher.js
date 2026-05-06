import { getDb } from '../db.js';
import { getReaderRuntimeConfig, getTelegramConfig, getTelegramTestGroupConfig } from '../env.js';
import { assertDealNotLocked, cleanText } from './dealHistoryService.js';
import { buildGeneratorDealContext } from './generatorDealScoringService.js';
import { logGeneratorDebug } from './generatorFlowService.js';
import { evaluateProductRules } from './productRulesService.js';
import { createPublishingEntry, processPublishingQueueEntry } from './publisherService.js';
import { isFailedPublishingQueueStatus, normalizePublishingQueueStatus } from './publishingQueueStateService.js';
import { sendTelegramCouponFollowUp, sendTelegramPost } from './telegramSenderService.js';

const db = getDb();
const DEBUG_QUEUE_ID_PLACEHOLDER = '__QUEUE_ID__';
const APPROVED_ENABLED = process.env.TELEGRAM_APPROVED_CHANNEL_ENABLED === '1';
const REJECTED_ENABLED = process.env.TELEGRAM_REJECTED_CHANNEL_ENABLED === '1';
const APPROVED_CHANNEL = cleanText(process.env.TELEGRAM_APPROVED_CHANNEL_ID);
const REJECTED_CHANNEL = cleanText(process.env.TELEGRAM_REJECTED_CHANNEL_ID);
const APPROVED_CHANNEL_USERNAME = normalizeTelegramChannelUsername(process.env.TELEGRAM_APPROVED_CHANNEL_USERNAME);
const REJECTED_CHANNEL_USERNAME = normalizeTelegramChannelUsername(process.env.TELEGRAM_REJECTED_CHANNEL_USERNAME);
const ROUTING_TEST_FORCE_APPROVE = process.env.ROUTING_TEST_FORCE_APPROVE === '1';
const telegramRoutingChannelCache = {
  approved: {
    chatId: APPROVED_CHANNEL,
    resolvedFromUsername: false,
    resolveAttempted: false,
    error: ''
  },
  rejected: {
    chatId: REJECTED_CHANNEL,
    resolvedFromUsername: false,
    resolveAttempted: false,
    error: ''
  }
};

console.info('[APPROVED_CHANNEL_READY]', {
  enabled: APPROVED_ENABLED,
  configured: Boolean(APPROVED_CHANNEL || APPROVED_CHANNEL_USERNAME),
  chatIdConfigured: Boolean(APPROVED_CHANNEL),
  usernameConfigured: Boolean(APPROVED_CHANNEL_USERNAME),
  ready: APPROVED_ENABLED && Boolean(APPROVED_CHANNEL || APPROVED_CHANNEL_USERNAME)
});
console.info('[REJECTED_CHANNEL_READY]', {
  enabled: REJECTED_ENABLED,
  configured: Boolean(REJECTED_CHANNEL || REJECTED_CHANNEL_USERNAME),
  chatIdConfigured: Boolean(REJECTED_CHANNEL),
  usernameConfigured: Boolean(REJECTED_CHANNEL_USERNAME),
  ready: REJECTED_ENABLED && Boolean(REJECTED_CHANNEL || REJECTED_CHANNEL_USERNAME)
});

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

function normalizeTelegramChannelUsername(value = '') {
  const username = cleanText(value).replace(/^@+/, '');
  return username ? `@${username}` : '';
}

async function resolveTelegramRoutingChannel({
  routeKey = '',
  username = '',
  chatId = '',
  resolvedLogTag = '',
  failedLogTag = ''
} = {}) {
  const cache = telegramRoutingChannelCache[routeKey] || {
    chatId: '',
    resolvedFromUsername: false,
    resolveAttempted: false,
    error: ''
  };
  const cleanChatId = cleanText(chatId);
  const cleanUsername = normalizeTelegramChannelUsername(username);

  if (cleanChatId) {
    cache.chatId = cleanChatId;
    cache.error = '';
    telegramRoutingChannelCache[routeKey] = cache;
    return cleanChatId;
  }

  if (cache.chatId) {
    return cache.chatId;
  }

  if (!cleanUsername) {
    return '';
  }

  if (cache.resolveAttempted) {
    return cache.chatId || '';
  }

  cache.resolveAttempted = true;

  try {
    const telegramConfig = getTelegramConfig();
    const token = cleanText(telegramConfig.token);

    if (!token) {
      throw new Error('TELEGRAM_BOT_TOKEN fehlt im Backend.');
    }

    const response = await fetch(
      `https://api.telegram.org/bot${token}/getChat?chat_id=${encodeURIComponent(cleanUsername)}`
    );
    const data = await response.json().catch(() => null);

    if (!response.ok || data?.ok !== true) {
      throw new Error(data?.description || `Telegram getChat fehlgeschlagen (${response.status}).`);
    }

    const resolvedChatId = data?.result?.id === undefined || data?.result?.id === null ? '' : String(data.result.id);
    if (!resolvedChatId) {
      throw new Error('Telegram getChat hat keine chat.id geliefert.');
    }

    cache.chatId = resolvedChatId;
    cache.resolvedFromUsername = true;
    cache.error = '';
    console.info(resolvedLogTag, {
      username: cleanUsername,
      chatId: resolvedChatId
    });
    console.log(`TELEGRAM_${routeKey.toUpperCase()}_CHANNEL_ID=${resolvedChatId}`);
  } catch (error) {
    cache.error = error instanceof Error ? error.message : 'Telegram Channel Username konnte nicht aufgeloest werden.';
    console.warn(failedLogTag, {
      username: cleanUsername,
      error: cache.error
    });
  }

  telegramRoutingChannelCache[routeKey] = cache;
  return cache.chatId || '';
}

async function getTelegramRoutingConfig() {
  const approvedChannel = APPROVED_ENABLED
    ? await resolveTelegramRoutingChannel({
        routeKey: 'approved',
        username: APPROVED_CHANNEL_USERNAME,
        chatId: APPROVED_CHANNEL,
        resolvedLogTag: '[APPROVED_CHANNEL_USERNAME_RESOLVED]',
        failedLogTag: '[APPROVED_CHANNEL_USERNAME_RESOLVE_FAILED]'
      })
    : APPROVED_CHANNEL;
  const rejectedChannel = REJECTED_ENABLED
    ? await resolveTelegramRoutingChannel({
        routeKey: 'rejected',
        username: REJECTED_CHANNEL_USERNAME,
        chatId: REJECTED_CHANNEL,
        resolvedLogTag: '[REJECTED_CHANNEL_USERNAME_RESOLVED]',
        failedLogTag: '[REJECTED_CHANNEL_USERNAME_RESOLVE_FAILED]'
      })
    : REJECTED_CHANNEL;

  return {
    approved: {
      enabled: APPROVED_ENABLED && Boolean(approvedChannel),
      configured: Boolean(approvedChannel),
      flagEnabled: APPROVED_ENABLED,
      chatId: approvedChannel,
      username: APPROVED_CHANNEL_USERNAME,
      resolveError: telegramRoutingChannelCache.approved.error
    },
    rejected: {
      enabled: REJECTED_ENABLED && Boolean(rejectedChannel),
      configured: Boolean(rejectedChannel),
      flagEnabled: REJECTED_ENABLED,
      chatId: rejectedChannel,
      username: REJECTED_CHANNEL_USERNAME,
      resolveError: telegramRoutingChannelCache.rejected.error
    }
  };
}

function buildRoutingSkipReason(config = {}, inactiveDecisionReason = 'decision_not_matching') {
  if (config.flagEnabled !== true) {
    return 'enabled_flag_off';
  }

  if (!config.configured) {
    return 'channel_id_missing';
  }

  return inactiveDecisionReason;
}

function resolveTelegramRoutingReason(input = {}, generatorContext = {}) {
  return (
    cleanText(input.telegramRoutingReason) ||
    cleanText(generatorContext?.learning?.reason) ||
    cleanText(generatorContext?.evaluation?.reason) ||
    (Array.isArray(generatorContext?.evaluation?.reasons) ? cleanText(generatorContext.evaluation.reasons.join(' | ')) : '') ||
    'Keine Begruendung vorhanden.'
  );
}

function resolveTelegramRoutingSourceLabel(input = {}) {
  return cleanText(input.sourceName) || cleanText(input.contextSource) || cleanText(input.queueSourceType) || 'System';
}

function isReaderTestModeFinalRoutingActive() {
  const runtimeConfig = getReaderRuntimeConfig();
  return runtimeConfig.readerTestMode === true || process.env.READER_TEST_MODE === '1';
}

function resolveFinalRoutingSellerClass(input = {}, generatorContext = {}) {
  return cleanText(
    input.sellerClass ||
      generatorContext?.seller?.sellerClass ||
      generatorContext?.learning?.sellerClass ||
      generatorContext?.evaluation?.sellerClass ||
      generatorContext?.decisionPolicy?.seller?.sellerClass
  ).toUpperCase();
}

function resolveFinalRoutingSellerType(input = {}, generatorContext = {}) {
  return cleanText(
    input.sellerType ||
      generatorContext?.seller?.sellerType ||
      generatorContext?.learning?.sellerType ||
      generatorContext?.evaluation?.sellerType ||
      generatorContext?.decisionPolicy?.seller?.sellerType
  ).toUpperCase();
}

function isFbmFinalRoutingSeller(input = {}, generatorContext = {}) {
  const sellerClass = resolveFinalRoutingSellerClass(input, generatorContext);
  const sellerType = resolveFinalRoutingSellerType(input, generatorContext);

  return sellerClass === 'FBM' || sellerClass.includes('FBM') || sellerType === 'FBM' || sellerType.includes('FBM');
}

function isApprovedFinalRoutingSellerAllowed(input = {}, generatorContext = {}) {
  const sellerClass = resolveFinalRoutingSellerClass(input, generatorContext);
  const sellerType = resolveFinalRoutingSellerType(input, generatorContext);

  if (sellerClass) {
    return sellerClass === 'AMAZON_DIRECT' || sellerClass === 'FBA' || sellerClass === 'FBA_THIRDPARTY';
  }

  return (
    sellerType === 'AMAZON' ||
    sellerType === 'FBA'
  );
}

function resolveFinalRoutingMerchantName(input = {}, generatorContext = {}) {
  return cleanText(
    input.amazonMerchantName ||
      input.offerMerchantInfo ||
      input.paapiMerchantInfo ||
      generatorContext?.seller?.details?.merchantText ||
      generatorContext?.seller?.details?.sellerName ||
      generatorContext?.evaluation?.merchantName ||
      generatorContext?.learning?.merchantName
  );
}

function resolveFinalRoutingProductRuleEvaluation(input = {}, generatorContext = {}) {
  const storedEvaluation =
    input.productRuleEvaluation ||
    generatorContext?.productRuleEvaluation ||
    generatorContext?.evaluation?.productRuleEvaluation ||
    generatorContext?.learning?.productRuleEvaluation;

  if (storedEvaluation && typeof storedEvaluation === 'object') {
    return storedEvaluation;
  }

  return evaluateProductRules({
    title: cleanText(input.title),
    brand: cleanText(
      input.brand ||
        generatorContext?.product?.brand ||
        generatorContext?.evaluation?.brand ||
        generatorContext?.keepa?.brand
    ),
    finalPrice: input.currentPrice,
    rating:
      input.rating ??
      generatorContext?.product?.rating ??
      generatorContext?.evaluation?.rating ??
      generatorContext?.keepa?.rating ??
      null,
    reviewCount:
      input.reviewCount ??
      input.totalReviews ??
      generatorContext?.product?.reviewCount ??
      generatorContext?.evaluation?.reviewCount ??
      generatorContext?.keepa?.reviewCount ??
      null,
    category: cleanText(input.category || generatorContext?.product?.category),
    features: input.features || generatorContext?.product?.features || [],
    sellerClass: resolveFinalRoutingSellerClass(input, generatorContext),
    isBrandProduct: input.isBrandProduct ?? generatorContext?.product?.isBrandProduct,
    isNoName: input.isNoName ?? generatorContext?.product?.isNoName,
    isChinaProduct: input.isChinaProduct ?? generatorContext?.product?.isChinaProduct,
    merchantName: resolveFinalRoutingMerchantName(input, generatorContext),
    marketComparisonAvailable:
      generatorContext?.learning?.marketComparisonUsed === true ||
      cleanText(generatorContext?.learning?.marketComparisonStatus).toLowerCase() === 'success',
    marketComparisonStatus:
      cleanText(generatorContext?.learning?.marketComparisonStatus || generatorContext?.internet?.status) || 'missing',
    scope: 'approved_routing'
  });
}

function normalizeFinalRoutingDecisionToken(value = '') {
  return cleanText(value).toLowerCase();
}

function isApproveFinalRoutingDecision(value = '') {
  return ['approve', 'approved', 'approved_for_test_group', 'test_group'].includes(
    normalizeFinalRoutingDecisionToken(value)
  );
}

function isReviewFinalRoutingDecision(value = '') {
  return ['review', 'manual_review'].includes(normalizeFinalRoutingDecisionToken(value));
}

function isBlockFinalRoutingDecision(value = '') {
  return ['block', 'blocked', 'reject', 'rejected', 'hold'].includes(normalizeFinalRoutingDecisionToken(value));
}

function resolveFinalRoutingExplicitDecision(input = {}) {
  return normalizeFinalRoutingDecisionToken(
    input.telegramRoutingDecision ||
      input.decisionStatus ||
      input.routingDecision
  );
}

function hasApprovedFinalRoutingSignal(input = {}, generatorContext = {}) {
  const evaluationDecision = normalizeFinalRoutingDecisionToken(generatorContext?.evaluation?.decision);
  const hasValidGeneratorPost =
    Boolean(cleanText(input.title)) &&
    Boolean(cleanText(input.link || input.normalizedUrl)) &&
    Boolean(cleanText(input.currentPrice));

  return (
    (ROUTING_TEST_FORCE_APPROVE === true && hasValidGeneratorPost) ||
    isApproveFinalRoutingDecision(resolveFinalRoutingExplicitDecision(input)) ||
    isApproveFinalRoutingDecision(input.decision) ||
    isApproveFinalRoutingDecision(generatorContext?.learning?.routingDecision) ||
    ['approve', 'approved', 'approved_for_test_group'].includes(evaluationDecision) ||
    input.wouldPostNormally === true ||
    generatorContext?.learning?.wouldPostNormally === true ||
    generatorContext?.learning?.canReachTestGroup === true ||
    generatorContext?.evaluation?.testGroupApproved === true
  );
}

function hasRouteLevelRejectedFinalRoutingSignal(input = {}, generatorContext = {}) {
  const routeSignals = [
    resolveFinalRoutingExplicitDecision(input),
    normalizeFinalRoutingDecisionToken(generatorContext?.learning?.routingDecision)
  ].filter(Boolean);

  return routeSignals.some((token) => isBlockFinalRoutingDecision(token) || isReviewFinalRoutingDecision(token));
}

function buildFinalRoutingInputState(input = {}, generatorContext = {}) {
  const finalInput = { ...input };
  const finalGeneratorContext = {
    ...generatorContext,
    learning: { ...(generatorContext?.learning || {}) },
    evaluation: { ...(generatorContext?.evaluation || {}) },
    seller: { ...(generatorContext?.seller || {}) }
  };
  const sellerClass = resolveFinalRoutingSellerClass(finalInput, finalGeneratorContext);
  const shouldHardBlockFbm = isFbmFinalRoutingSeller(finalInput, finalGeneratorContext);
  const approvedSellerAllowed = isApprovedFinalRoutingSellerAllowed(finalInput, finalGeneratorContext);
  const merchantName = resolveFinalRoutingMerchantName(finalInput, finalGeneratorContext);
  const hasApproveLikeState = hasApprovedFinalRoutingSignal(finalInput, finalGeneratorContext);
  const shouldForceApprove =
    shouldHardBlockFbm !== true &&
    approvedSellerAllowed === true &&
    isReaderTestModeFinalRoutingActive() === true &&
    sellerClass === 'FBA_OR_AMAZON_UNKNOWN';

  if (shouldHardBlockFbm) {
    const reason = 'FBM_NOT_ALLOWED';
    const reasonDetail = 'FBM/Drittanbieter darf nicht automatisch veroeffentlicht werden.';

    finalInput.sellerClass = 'FBM';
    finalInput.sellerType = 'FBM';
    finalInput.decision = 'BLOCK';
    finalInput.decisionDisplay = 'BLOCK';
    finalInput.telegramRoutingDecision = 'block';
    finalInput.decisionStatus = 'block';
    finalInput.routingDecision = 'block';
    finalInput.normalDecision = 'block';
    finalInput.wouldPostNormally = false;
    finalInput.testGroupApproved = false;
    finalInput.accepted = false;
    finalInput.telegramRoutingReason = reasonDetail;
    finalInput.reasonCode = reason;
    finalInput.telegramRoutingReasonCode = reason;

    finalGeneratorContext.seller = {
      ...finalGeneratorContext.seller,
      sellerClass: 'FBM',
      sellerType: 'FBM'
    };
    finalGeneratorContext.learning = {
      ...finalGeneratorContext.learning,
      routingDecision: 'block',
      normalDecision: 'block',
      wouldPostNormally: false,
      testGroupApproved: false,
      canReachTestGroup: false,
      accepted: false,
      blocked: true,
      reason: reasonDetail,
      reasonCode: reason
    };
    finalGeneratorContext.evaluation = {
      ...finalGeneratorContext.evaluation,
      decision: 'BLOCK',
      decisionLabel: 'BLOCK',
      testGroupApproved: false,
      accepted: false,
      blocked: true,
      reason: reasonDetail,
      reasonCode: reason
    };

    console.info('[SELLER_HARD_BLOCK_FBM]', {
      asin: cleanText(finalInput.asin).toUpperCase() || '',
      merchant: merchantName || '',
      routeBlocked: true
    });
    console.info('[FBM_HARD_BLOCK_ACTIVE]', {
      sellerClass: finalInput.sellerClass,
      sellerType: finalInput.sellerType,
      routingDecision: finalInput.routingDecision,
      reason: reasonDetail
    });
    console.info('[FBM_BLOCK_OVERRIDES_TEST_APPROVE]', {
      sellerClass: finalInput.sellerClass,
      sellerType: finalInput.sellerType,
      wouldPostNormally: false,
      testGroupApproved: false
    });
  }

  if (shouldHardBlockFbm !== true && approvedSellerAllowed !== true && hasApproveLikeState) {
    const reason = 'Seller ist fuer den Kanal Veroeffentlicht nicht erlaubt.';

    finalInput.decision = cleanText(finalInput.decision).toUpperCase() === 'BLOCK' ? 'BLOCK' : 'REVIEW';
    finalInput.decisionDisplay = finalInput.decision;
    finalInput.telegramRoutingDecision = 'review';
    finalInput.decisionStatus = 'review';
    finalInput.routingDecision = 'review';
    finalInput.normalDecision = 'review';
    finalInput.wouldPostNormally = false;
    finalInput.testGroupApproved = false;
    finalInput.accepted = false;
    finalInput.telegramRoutingReason = cleanText(finalInput.telegramRoutingReason) || reason;

    finalGeneratorContext.learning = {
      ...finalGeneratorContext.learning,
      routingDecision: 'review',
      normalDecision: 'review',
      wouldPostNormally: false,
      testGroupApproved: false,
      canReachTestGroup: false,
      accepted: false,
      blocked: false,
      reason: cleanText(finalGeneratorContext.learning.reason) || reason
    };
    finalGeneratorContext.evaluation = {
      ...finalGeneratorContext.evaluation,
      decision: cleanText(finalGeneratorContext.evaluation.decision).toUpperCase() === 'BLOCK' ? 'BLOCK' : 'REVIEW',
      decisionLabel: cleanText(finalGeneratorContext.evaluation.decisionLabel).toUpperCase() === 'BLOCK' ? 'BLOCK' : 'REVIEW',
      testGroupApproved: false,
      accepted: false,
      blocked: false,
      reason: cleanText(finalGeneratorContext.evaluation.reason) || reason
    };
  }

  const productRuleEvaluation = resolveFinalRoutingProductRuleEvaluation(finalInput, finalGeneratorContext);
  finalInput.productRuleEvaluation = productRuleEvaluation;
  finalGeneratorContext.productRuleEvaluation = productRuleEvaluation;
  finalGeneratorContext.learning = {
    ...finalGeneratorContext.learning,
    productRuleEvaluation
  };
  finalGeneratorContext.evaluation = {
    ...finalGeneratorContext.evaluation,
    productRuleEvaluation
  };

  if (
    shouldHardBlockFbm !== true &&
    approvedSellerAllowed === true &&
    hasApproveLikeState &&
    productRuleEvaluation?.matchedRule &&
    productRuleEvaluation.allowed !== true
  ) {
    const ruleReasonCode = cleanText(productRuleEvaluation.reasonCode) || 'PRODUCT_RULE_BLOCKED';
    const routingDecision = productRuleEvaluation.decision === 'review' ? 'review' : 'block';
    const decisionLabel = routingDecision === 'block' ? 'BLOCK' : 'REVIEW';
    const ruleReason = `Produkt-Regel "${productRuleEvaluation.matchedRuleName || 'Unbekannt'}": ${
      productRuleEvaluation.reason || 'Nicht fuer Veroeffentlicht erlaubt.'
    }`;

    finalInput.decision = decisionLabel;
    finalInput.decisionDisplay = decisionLabel;
    finalInput.telegramRoutingDecision = routingDecision;
    finalInput.decisionStatus = routingDecision;
    finalInput.routingDecision = routingDecision;
    finalInput.normalDecision = routingDecision;
    finalInput.wouldPostNormally = false;
    finalInput.testGroupApproved = false;
    finalInput.accepted = false;
    finalInput.telegramRoutingReason = ruleReason;
    finalInput.reasonCode = ruleReasonCode;
    finalInput.telegramRoutingReasonCode = ruleReasonCode;

    finalGeneratorContext.learning = {
      ...finalGeneratorContext.learning,
      routingDecision,
      normalDecision: routingDecision,
      wouldPostNormally: false,
      testGroupApproved: false,
      canReachTestGroup: false,
      accepted: false,
      blocked: routingDecision === 'block',
      reason: ruleReason,
      reasonCode: ruleReasonCode,
      productRuleEvaluation
    };
    finalGeneratorContext.evaluation = {
      ...finalGeneratorContext.evaluation,
      decision: decisionLabel,
      decisionLabel,
      testGroupApproved: false,
      accepted: false,
      blocked: routingDecision === 'block',
      reason: ruleReason,
      reasonCode: ruleReasonCode,
      productRuleEvaluation
    };
  }

  if (
    shouldHardBlockFbm !== true &&
    approvedSellerAllowed === true &&
    hasApprovedFinalRoutingSignal(finalInput, finalGeneratorContext) === true &&
    hasRouteLevelRejectedFinalRoutingSignal(finalInput, finalGeneratorContext) !== true &&
    !(productRuleEvaluation?.matchedRule && productRuleEvaluation.allowed !== true)
  ) {
    const reason =
      cleanText(finalInput.telegramRoutingReason) ||
      cleanText(finalGeneratorContext?.learning?.reason) ||
      cleanText(finalGeneratorContext?.evaluation?.reason) ||
      'Approved-Route erlaubt: Seller und Produkt-Regeln sind gueltig.';
    const testGroupApproved =
      finalInput.testGroupApproved === true ||
      finalGeneratorContext?.learning?.testGroupApproved === true ||
      finalGeneratorContext?.evaluation?.testGroupApproved === true ||
      finalGeneratorContext?.learning?.canReachTestGroup === true;

    finalInput.decision = 'APPROVE';
    finalInput.decisionDisplay = 'APPROVE';
    finalInput.telegramRoutingDecision = 'approve';
    finalInput.decisionStatus = 'approve';
    finalInput.routingDecision = 'approve';
    finalInput.normalDecision = 'approve';
    finalInput.wouldPostNormally = true;
    finalInput.testGroupApproved = testGroupApproved;
    finalInput.accepted = true;
    finalInput.telegramRoutingReason = reason;

    finalGeneratorContext.learning = {
      ...finalGeneratorContext.learning,
      routingDecision: 'approve',
      normalDecision: 'approve',
      wouldPostNormally: true,
      testGroupApproved,
      canReachTestGroup: testGroupApproved,
      accepted: true,
      blocked: false,
      reason
    };
    finalGeneratorContext.evaluation = {
      ...finalGeneratorContext.evaluation,
      decision: 'APPROVE',
      decisionLabel: 'APPROVE',
      testGroupApproved,
      accepted: true,
      blocked: false,
      reason
    };
  }

  if (shouldForceApprove && !(productRuleEvaluation?.matchedRule && productRuleEvaluation.allowed !== true)) {
    const reason = 'Testmodus: Amazon Produktdaten verifiziert, Seller noch nicht eindeutig.';

    finalInput.sellerClass = 'FBA_OR_AMAZON_UNKNOWN';
    finalInput.decision = 'APPROVE';
    finalInput.decisionDisplay = 'APPROVE';
    finalInput.telegramRoutingDecision = 'approve';
    finalInput.decisionStatus = 'approve';
    finalInput.routingDecision = 'approve';
    finalInput.normalDecision = 'approve';
    finalInput.wouldPostNormally = true;
    finalInput.testGroupApproved = true;
    finalInput.accepted = true;
    finalInput.telegramRoutingReason = cleanText(finalInput.telegramRoutingReason) || reason;

    finalGeneratorContext.seller.sellerClass = 'FBA_OR_AMAZON_UNKNOWN';
    finalGeneratorContext.learning = {
      ...finalGeneratorContext.learning,
      routingDecision: 'approve',
      normalDecision: 'approve',
      wouldPostNormally: true,
      testGroupApproved: true,
      canReachTestGroup: true,
      accepted: true,
      blocked: false,
      reason: cleanText(finalGeneratorContext.learning.reason) || reason
    };
    finalGeneratorContext.evaluation = {
      ...finalGeneratorContext.evaluation,
      decision: 'APPROVE',
      decisionLabel: 'APPROVE',
      testGroupApproved: true,
      accepted: true,
      blocked: false,
      reason: cleanText(finalGeneratorContext.evaluation.reason) || reason
    };

    console.info('[FINAL_FORCE_APPROVE_FOR_VERIFIED_AMAZON_UNKNOWN]', {
      sellerClass: finalInput.sellerClass,
      decision: finalInput.decision,
      routingDecision: finalInput.routingDecision,
      reason
    });
  }

  return {
    input: finalInput,
    generatorContext: finalGeneratorContext,
    sellerClass: resolveFinalRoutingSellerClass(finalInput, finalGeneratorContext),
    forcedApprove: shouldForceApprove,
    hardBlockedFbm: shouldHardBlockFbm
  };
}

function buildFinalRoutingInputLog(finalRoutingState = {}, routingDecision = {}) {
  const routingInput = finalRoutingState.input || {};
  const routingGeneratorContext = finalRoutingState.generatorContext || {};
  const learning = routingGeneratorContext.learning || {};
  const evaluation = routingGeneratorContext.evaluation || {};

  return {
    sellerClass: finalRoutingState.sellerClass || resolveFinalRoutingSellerClass(routingInput, routingGeneratorContext),
    sellerType: resolveFinalRoutingSellerType(routingInput, routingGeneratorContext),
    decision: cleanText(routingInput.decision || evaluation.decision) || routingDecision.label || '',
    decisionDisplay: cleanText(routingInput.decisionDisplay || evaluation.decisionLabel) || routingDecision.label || '',
    routingDecision:
      cleanText(routingInput.routingDecision || routingInput.telegramRoutingDecision || learning.routingDecision) ||
      routingDecision.bucket ||
      '',
    normalDecision: cleanText(routingInput.normalDecision || learning.normalDecision) || '',
    wouldPostNormally:
      routingInput.wouldPostNormally === true ||
      learning.wouldPostNormally === true ||
      routingDecision.bucket === 'approved',
    testGroupApproved:
      routingInput.testGroupApproved === true ||
      learning.testGroupApproved === true ||
      evaluation.testGroupApproved === true,
    fbmProtection: finalRoutingState.hardBlockedFbm === true ? 'AKTIV' : 'nicht noetig',
    productRule:
      routingInput.productRuleEvaluation?.matchedRuleName ||
      routingGeneratorContext?.productRuleEvaluation?.matchedRuleName ||
      ''
  };
}

function resolveTelegramRoutingDecision(input = {}, generatorContext = {}) {
  const explicitDecision = resolveFinalRoutingExplicitDecision(input);
  const learningDecision = normalizeFinalRoutingDecisionToken(generatorContext?.learning?.routingDecision);
  const evaluationDecision = normalizeFinalRoutingDecisionToken(generatorContext?.evaluation?.decision);
  const evaluationApproved = generatorContext?.evaluation?.testGroupApproved === true;
  const approvedSellerAllowed = isApprovedFinalRoutingSellerAllowed(input, generatorContext);
  const hasValidGeneratorPost =
    Boolean(cleanText(input.title)) &&
    Boolean(cleanText(input.link || input.normalizedUrl)) &&
    Boolean(cleanText(input.currentPrice));
  const hasApproveSignal =
    hasApprovedFinalRoutingSignal(input, generatorContext) ||
    ((ROUTING_TEST_FORCE_APPROVE === true && hasValidGeneratorPost) || evaluationApproved === true);

  if (isFbmFinalRoutingSeller(input, generatorContext)) {
    return { bucket: 'rejected', label: 'BLOCK' };
  }

  if (ROUTING_TEST_FORCE_APPROVE === true && hasValidGeneratorPost && approvedSellerAllowed === true) {
    console.info('[ROUTING_TEST_FORCE_APPROVE_ACTIVE]', {
      asin: cleanText(input.asin).toUpperCase() || '',
      titlePreview: cleanText(input.title).slice(0, 120),
      hasPrice: Boolean(cleanText(input.currentPrice)),
      hasLink: Boolean(cleanText(input.link || input.normalizedUrl))
    });
    return { bucket: 'approved', label: 'APPROVE' };
  }

  if (isBlockFinalRoutingDecision(explicitDecision)) {
    return { bucket: 'rejected', label: 'BLOCK' };
  }
  if (isReviewFinalRoutingDecision(explicitDecision)) {
    return { bucket: 'rejected', label: 'REVIEW' };
  }
  if (isBlockFinalRoutingDecision(learningDecision)) {
    return { bucket: 'rejected', label: 'BLOCK' };
  }
  if (isReviewFinalRoutingDecision(learningDecision)) {
    return { bucket: 'rejected', label: 'REVIEW' };
  }
  if (hasApproveSignal && approvedSellerAllowed !== true) {
    return { bucket: 'rejected', label: 'REVIEW' };
  }
  if (hasApproveSignal) {
    return { bucket: 'approved', label: 'APPROVE' };
  }
  if (isBlockFinalRoutingDecision(evaluationDecision)) {
    return { bucket: 'rejected', label: 'BLOCK' };
  }
  if (isReviewFinalRoutingDecision(evaluationDecision)) {
    return { bucket: 'rejected', label: 'REVIEW' };
  }

  return { bucket: 'neutral', label: 'NEUTRAL' };
}

function shortenRoutingText(value = '', maxLength = 140) {
  const normalized = cleanText(String(value || '').replace(/\s+/g, ' '));
  if (!normalized || normalized.length <= maxLength) {
    return normalized;
  }

  return `${normalized.slice(0, maxLength - 3).trim()}...`;
}

function formatRoutingValue(value, fallback = 'n/a') {
  if (value === null || value === undefined || value === '') {
    return fallback;
  }

  if (typeof value === 'number' && Number.isFinite(value)) {
    return String(Math.round(value * 10) / 10);
  }

  return cleanText(String(value)) || fallback;
}

function formatRoutingPercent(value, fallback = 'n/a') {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return fallback;
  }

  return `${Math.round(numeric * 10) / 10}%`;
}

function formatRoutingPriceText(value, fallback = 'n/a') {
  const text = cleanText(value);
  if (!text) {
    return fallback;
  }

  if (/\u20AC|eur/i.test(text)) {
    return text;
  }

  const numeric = parsePublishingPriceValue(text);
  if (numeric !== null) {
    const formatted = new Intl.NumberFormat('de-DE', {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
    }).format(numeric);
    return `${formatted}\u20AC`;
  }

  return text;
}

function resolveRoutingPriceSourceLabel(input = {}, generatorContext = {}) {
  const rawSource = cleanText(
    input.rawPriceSource ||
      input.priceRawSource ||
      input.priceSource ||
      generatorContext?.pricing?.rawPriceSource ||
      generatorContext?.pricing?.priceSource
  ).toLowerCase();

  if (rawSource.includes('paapi')) {
    return { label: 'PAAPI', logTag: '[PRICE_SOURCE_PAAPI]', rawSource };
  }

  if (rawSource.includes('creator')) {
    return { label: 'Creator API', logTag: '', rawSource };
  }

  if (rawSource.includes('scrape') || rawSource.includes('scraped') || rawSource.includes('amazonfinal') || rawSource.includes('amazonbuybox')) {
    return { label: 'Scrape', logTag: '[PRICE_SOURCE_SCRAPE]', rawSource };
  }

  if (rawSource.includes('keepa')) {
    return { label: 'Keepa', logTag: '[PRICE_SOURCE_KEEPA]', rawSource };
  }

  if (rawSource.includes('telegram')) {
    return { label: 'Telegram', logTag: '', rawSource };
  }

  return { label: 'Fallback', logTag: '', rawSource: rawSource || 'fallback' };
}

function parseRoutingDiscountDescriptor(value = '') {
  const text = cleanText(value);
  if (!text) {
    return null;
  }

  const numeric = parsePublishingPriceValue(text);
  if (numeric === null) {
    return null;
  }

  if (/%/.test(text)) {
    return { type: 'percent', value: numeric };
  }

  if (/\u20AC|eur/i.test(text)) {
    return { type: 'amount', value: numeric };
  }

  return null;
}

function calculateRoutingFinalPrice(basePriceText = '', couponValue = '', subscribeDiscount = '') {
  const basePrice = parsePublishingPriceValue(basePriceText);
  if (basePrice === null) {
    return {
      calculated: false,
      finalPrice: '',
      finalPriceValue: null
    };
  }

  const couponDescriptor = parseRoutingDiscountDescriptor(couponValue);
  const subscribeDescriptor = parseRoutingDiscountDescriptor(subscribeDiscount);
  let finalPriceValue = basePrice;
  let appliedDiscount = false;

  if (couponDescriptor) {
    appliedDiscount = true;
    finalPriceValue =
      couponDescriptor.type === 'percent'
        ? finalPriceValue * (1 - couponDescriptor.value / 100)
        : finalPriceValue - couponDescriptor.value;
  }

  if (subscribeDescriptor) {
    appliedDiscount = true;
    finalPriceValue =
      subscribeDescriptor.type === 'percent'
        ? finalPriceValue * (1 - subscribeDescriptor.value / 100)
        : finalPriceValue - subscribeDescriptor.value;
  }

  if (!appliedDiscount || !Number.isFinite(finalPriceValue) || finalPriceValue <= 0) {
    return {
      calculated: false,
      finalPrice: '',
      finalPriceValue: null
    };
  }

  return {
    calculated: true,
    finalPrice: formatRoutingPriceText(String(finalPriceValue)),
    finalPriceValue
  };
}

function formatRoutingDiscountLabel(value = '', fallback = 'Coupon') {
  const text = cleanText(value);
  if (!text) {
    return fallback;
  }

  const numeric = parsePublishingPriceValue(text);
  if (numeric === null) {
    return text;
  }

  if (/%/.test(text)) {
    return `${Math.round(numeric * 10) / 10}%`;
  }

  if (/\u20AC|eur/i.test(text)) {
    return formatRoutingPriceText(text);
  }

  return text;
}

function buildRoutingPriceCalculation({
  couponDetected = false,
  subscribeDetected = false,
  basePrice = '',
  couponValue = '',
  subscribeDiscount = '',
  finalPrice = ''
} = {}) {
  if (couponDetected) {
    const basePriceText = formatRoutingPriceText(basePrice);
    const couponText = formatRoutingDiscountLabel(couponValue, 'unbekannter');
    const finalPriceText = formatRoutingPriceText(finalPrice);

    if (subscribeDetected) {
      const subscribeText = formatRoutingDiscountLabel(subscribeDiscount, 'Sparabo');
      return `${basePriceText} minus ${couponText} Coupon minus ${subscribeText} Sparabo = ${finalPriceText}`;
    }

    return `${basePriceText} minus ${couponText} Coupon = ${finalPriceText}`;
  }

  return 'keine Coupon-Berechnung';
}

function resolveRoutingPriceType({
  couponDetected = false,
  subscribeDetected = false,
  finalPriceCalculated = false
} = {}) {
  if (couponDetected && subscribeDetected) {
    return 'Coupon + Sparabo';
  }

  if (couponDetected) {
    return 'Coupon';
  }

  if (subscribeDetected) {
    return 'Sparabo';
  }

  if (finalPriceCalculated) {
    return 'Rabatt';
  }

  return 'Standard';
}

function resolveRoutingPriceInfo(input = {}, generatorContext = {}, sourceLabel = '') {
  const source = resolveRoutingPriceSourceLabel(input, generatorContext);
  const couponValue = cleanText(input.couponValue);
  const couponLabelValue = couponValue || cleanText(input.couponCode);
  const subscribeDiscount = cleanText(input.subscribeDiscount);
  const couponDetected = input.couponDetected === true || Boolean(couponValue);
  const subscribeDetected = input.subscribeDetected === true || Boolean(subscribeDiscount);
  const explicitFinalPrice = cleanText(input.finalPrice);
  const hasExplicitFinalPrice = input.finalPriceCalculated === true && Boolean(explicitFinalPrice);
  const basePrice = cleanText(input.basePrice || input.currentPrice || generatorContext?.keepa?.currentPrice);
  const calculatedFinalPrice =
    hasExplicitFinalPrice || couponDetected || subscribeDetected
      ? calculateRoutingFinalPrice(basePrice, couponValue, subscribeDiscount)
      : { calculated: false, finalPrice: '', finalPriceValue: null };
  const finalPriceCalculated = hasExplicitFinalPrice || calculatedFinalPrice.calculated === true;
  const displayPrice = hasExplicitFinalPrice
    ? explicitFinalPrice
    : calculatedFinalPrice.finalPrice || basePrice || generatorContext?.keepa?.currentPrice;
  const priceType = resolveRoutingPriceType({
    couponDetected,
    subscribeDetected,
    finalPriceCalculated
  });
  const visibleSource = couponDetected ? 'Amazon + Coupon' : source.label;
  const visiblePrice = formatRoutingPriceText(displayPrice);
  const calculation = buildRoutingPriceCalculation({
    couponDetected,
    subscribeDetected,
    basePrice,
    couponValue: couponLabelValue,
    subscribeDiscount,
    finalPrice: visiblePrice
  });

  if (source.logTag) {
    console.info(source.logTag, {
      asin: cleanText(input.asin).toUpperCase() || '',
      rawSource: source.rawSource,
      price: visiblePrice,
      priceType
    });
  }

  if (finalPriceCalculated) {
    console.info('[PRICE_FINAL_CALCULATED]', {
      asin: cleanText(input.asin).toUpperCase() || '',
      basePrice: formatRoutingPriceText(basePrice),
      couponDetected,
      couponValue: couponLabelValue,
      subscribeDetected,
      subscribeDiscount,
      finalPrice: visiblePrice,
      priceType,
      providedBySource: hasExplicitFinalPrice
    });
  }

  console.info('[PRICE_SOURCE_VISIBLE]', {
    asin: cleanText(input.asin).toUpperCase() || '',
    context: 'routing_shortcheck',
    sourceGroup: sourceLabel || resolveRoutingSourceLabel(input),
    price: visiblePrice,
    priceSource: visibleSource,
    calculation
  });

  if (couponDetected) {
    console.info('[PRICE_COUPON_CALCULATION_VISIBLE]', {
      asin: cleanText(input.asin).toUpperCase() || '',
      context: 'routing_shortcheck',
      basePrice: formatRoutingPriceText(basePrice),
      couponValue: couponLabelValue,
      subscribeDiscount,
      finalPrice: visiblePrice,
      calculation
    });
  }

  return {
    price: visiblePrice,
    source: visibleSource,
    type: priceType,
    calculation
  };
}

function buildRejectedRoutingSolution(reason = '') {
  const normalized = cleanText(reason).toLowerCase();

  if (normalized.includes('affiliate') || normalized.includes('partnerlink')) {
    return 'Amazon-Link oder PAAPI-Daten pruefen.';
  }
  if (normalized.includes('amazon-link') || normalized.includes('amazon link')) {
    return 'Amazon-Link validieren.';
  }
  if (normalized.includes('preis')) {
    return 'Preis mit Amazon und Keepa abgleichen.';
  }
  if (normalized.includes('produktregel') || normalized.includes('powerbank') || normalized.includes('kopfho')) {
    return 'Produktregel und Preislimit pruefen.';
  }
  if (normalized.includes('review') || normalized.includes('unknown')) {
    return 'Produktdaten manuell verifizieren.';
  }

  return 'Kurze manuelle Pruefung noetig.';
}

function resolveRoutingSourceLabel(input = {}) {
  return (
    cleanText(input.channelRef) ||
    cleanText(input.channelTitle) ||
    cleanText(input.group) ||
    cleanText(input.sourceName) ||
    cleanText(input.contextSource) ||
    cleanText(input.queueSourceType) ||
    'Unbekannt'
  );
}

function getGeneratorPostTextFromPayload(payload = {}) {
  return cleanText(payload.textByChannel?.telegram || payload.textByChannel?.whatsapp || payload.title);
}

function resolveGeneratorPostImagePayload(payload = {}, preferredSource = 'standard') {
  const imageVariants = payload.imageVariants && typeof payload.imageVariants === 'object' ? payload.imageVariants : {};
  const preferred = cleanText(preferredSource);
  const uploadImage = cleanText(imageVariants.upload);
  const standardImage = cleanText(imageVariants.standard);

  if (preferred === 'upload' && uploadImage) {
    return { uploadedImage: uploadImage, imageUrl: '', imageSource: 'upload' };
  }

  if (preferred === 'standard' && standardImage) {
    return standardImage.startsWith('data:image')
      ? { uploadedImage: standardImage, imageUrl: '', imageSource: 'standard_upload' }
      : { uploadedImage: '', imageUrl: standardImage, imageSource: 'standard' };
  }

  if (uploadImage) {
    return { uploadedImage: uploadImage, imageUrl: '', imageSource: 'upload' };
  }

  if (standardImage) {
    return standardImage.startsWith('data:image')
      ? { uploadedImage: standardImage, imageUrl: '', imageSource: 'standard_upload' }
      : { uploadedImage: '', imageUrl: standardImage, imageSource: 'standard' };
  }

  return { uploadedImage: '', imageUrl: '', imageSource: 'none' };
}

function buildRejectedShortcheck({
  input = {},
  generatorContext = {},
  decisionLabel = 'REVIEW',
  reason = ''
} = {}) {
  const metrics = generatorContext?.evaluation?.metrics || {};
  const learning = generatorContext?.learning || {};
  const seller = generatorContext?.seller || {};
  const sellerClass = cleanText(input.sellerClass || seller.sellerClass || seller.sellerType) || 'UNKNOWN';
  const sellerSource =
    cleanText(input.sellerDetectionSource || seller.details?.detectionSource || seller.detectionSource || learning.sellerDetectionSource) ||
    'unknown';
  const routingLabel =
    decisionLabel === 'BLOCK' || decisionLabel === 'REJECT'
      ? 'BLOCK'
      : decisionLabel === 'APPROVE'
        ? 'APPROVE'
        : 'REVIEW';
  const fbmProtection = isFbmFinalRoutingSeller(input, generatorContext) ? 'AKTIV' : 'nicht noetig';
  const sourceLabel = resolveRoutingSourceLabel(input);
  const displayDecision = decisionLabel === 'BLOCK' || decisionLabel === 'REJECT' ? 'ABGELEHNT' : 'PR\u00DCFEN';
  const keepaDiscount = metrics.keepaDiscount ?? generatorContext?.keepa?.keepaDiscount ?? null;
  const score = metrics.finalScore ?? metrics.keepaDealScore ?? null;
  const fakeRisk = metrics.fakeDropRisk ?? null;
  const marketStatus = cleanText(learning.marketComparisonStatus || generatorContext?.internet?.status) || 'skipped';
  const shortReason = shortenRoutingText(reason || learning.reason || 'Keine Begruendung vorhanden.', 150);
  const solution = buildRejectedRoutingSolution(reason || learning.reason);
  const priceInfo = resolveRoutingPriceInfo(input, generatorContext, sourceLabel);
  const productRuleEvaluation =
    input.productRuleEvaluation ||
    generatorContext?.productRuleEvaluation ||
    generatorContext?.evaluation?.productRuleEvaluation ||
    generatorContext?.learning?.productRuleEvaluation ||
    null;
  const productRuleLines =
    productRuleEvaluation?.matchedRule
      ? [
          `\u{1F4CF} Produkt-Regel: ${productRuleEvaluation.matchedRuleName || '-'}`,
          `\u{1F4B6} Maximalpreis: ${
            productRuleEvaluation.maxPrice === null ? '-' : formatRoutingPriceText(String(productRuleEvaluation.maxPrice))
          }`,
          `\u{1F4B0} Aktueller Preis: ${
            productRuleEvaluation.actualPrice === null ? priceInfo.price : formatRoutingPriceText(String(productRuleEvaluation.actualPrice))
          }`,
          `\u{1F4CC} Regel-Ergebnis: ${productRuleEvaluation.allowed === true ? 'OK' : productRuleEvaluation.decision === 'review' ? 'REVIEW' : 'BLOCK'}`,
          `\u{1F4CC} Grund: ${shortenRoutingText(productRuleEvaluation.reason || '-', 150)}`
        ]
      : [];

  return [
    '\u{1F4CA} KURZCHECK',
    `\u{1F4E2} Quellgruppe: ${sourceLabel}`,
    `\u{1F4CC} Entscheidung: ${displayDecision}`,
    `\u{1F6D2} Seller: ${sellerClass}`,
    `\u{1F50E} Seller Quelle: ${sellerSource}`,
    `\u{1F4CC} Routing: ${routingLabel}`,
    `\u{1F6E1} FBM Schutz: ${fbmProtection}`,
    `\u{1F4B6} Preis: ${priceInfo.price}`,
    `\u{1F50E} Preisquelle: ${priceInfo.source}`,
    `\u{1F4CC} Preis-Typ: ${priceInfo.type}`,
    `\u{1F4CC} Berechnung: ${priceInfo.calculation}`,
    `\u{1F4C8} Keepa: ${formatRoutingPercent(keepaDiscount)} / Score ${formatRoutingValue(score)}`,
    `\u{1F30D} Marktvergleich: ${marketStatus}`,
    `\u26A0\uFE0F Fake-Risiko: ${formatRoutingPercent(fakeRisk)}`,
    ...productRuleLines,
    `\u{1F4CC} Grund: ${shortReason}`,
    `\u{1F6E0} L\u00F6sung: ${solution}`
  ].join('\n');
}

function buildRejectedCombinedPost({ generatorPostText = '', shortcheckText = '' } = {}) {
  const safeGeneratorPost = cleanText(generatorPostText);
  const safeShortcheck = cleanText(shortcheckText);

  if (!safeGeneratorPost) {
    return ['\u26A0\uFE0F NICHT VER\u00D6FFENTLICHT', safeShortcheck].filter(Boolean).join('\n\n');
  }

  return [safeGeneratorPost, '\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501', '\u26A0\uFE0F NICHT VER\u00D6FFENTLICHT', safeShortcheck]
    .filter(Boolean)
    .join('\n\n');
}

function buildRejectedRoutingMessage({ sourceLabel = '', reason = '' } = {}) {
  const shortReason = shortenRoutingText(reason || 'Keine Begruendung vorhanden.', 110);
  const solution = buildRejectedRoutingSolution(reason);

  return [
    '\u26A0\uFE0F NICHT VER\u00D6FFENTLICHT',
    `\u{1F4E2} Quelle: ${sourceLabel || 'System'}`,
    `\u{1F4CC} Grund: ${shortReason}`,
    `\u{1F6E0} L\u00F6sung: ${solution}`
  ].join('\n');
}

async function publishSecondaryTelegramRoute({
  routeKey = '',
  targetChatId = '',
  queueSourceType = '',
  generatorPostId = null,
  payload = {},
  imageSource = 'none'
} = {}) {
  const queueEntry = createPublishingEntry({
    sourceType: queueSourceType,
    sourceId: generatorPostId,
    originOverride: 'automatic',
    skipDealLock: true,
    payload: {
      ...payload,
      skipDealLock: true,
      skipPostedDealHistory: true
    },
    targets: [{ channelType: 'telegram', isEnabled: true, imageSource }]
  });

  const queueProcessingResult = await processPublishingQueueEntry(queueEntry.id);
  const summary = summarizeQueueResults(
    queueProcessingResult,
    {
      telegramImageSource: imageSource
    },
    generatorPostId
  );

  return {
    routeKey,
    queueId: summary.queue?.id || queueEntry?.id || null,
    queueStatus: summary.queue?.status || queueEntry?.status || 'pending',
    messageId: summary.results.telegram?.messageId || null,
    chatId: summary.results.telegram?.chatId || targetChatId || null,
    duplicateBlocked: summary.results.telegram?.duplicateBlocked === true,
    duplicateKey: summary.results.telegram?.duplicateKey || null,
    lastSentAt: summary.results.telegram?.lastSentAt || null
  };
}

async function sendRouteCouponFollowUp({
  couponCode = '',
  chatId = '',
  titlePreview = '',
  postContext = 'routing_coupon_follow_up',
  logTag = '[ROUTING_CODE_SEND_FAILED]',
  routeKey = '',
  generatorPostId = null
} = {}) {
  const normalizedCouponCode = cleanText(couponCode);
  const normalizedChatId = cleanText(chatId);

  if (!normalizedCouponCode || !normalizedChatId) {
    return null;
  }

  try {
    return await sendTelegramCouponFollowUp({
      couponCode: normalizedCouponCode,
      chatId: normalizedChatId,
      titlePreview,
      postContext
    });
  } catch (error) {
    console.warn(logTag, {
      routeKey: routeKey || null,
      generatorPostId,
      chatId: normalizedChatId,
      error: error instanceof Error ? error.message : 'CODE-Folgepost konnte nicht gesendet werden.'
    });
    return null;
  }
}

async function publishTelegramRoutingOutputs({
  input = {},
  generatorContext = {},
  publishingPayload = {},
  generatorPostId = null
} = {}) {
  const routingConfig = await getTelegramRoutingConfig();
  const finalRoutingState = buildFinalRoutingInputState(input, generatorContext);
  const routingInput = finalRoutingState.input;
  const routingGeneratorContext = finalRoutingState.generatorContext;
  const routingDecision = resolveTelegramRoutingDecision(routingInput, routingGeneratorContext);
  const routingReason = resolveTelegramRoutingReason(routingInput, routingGeneratorContext);
  const baseImageSource = cleanText(routingInput.telegramImageSource) || 'standard';
  const generatorPostText = getGeneratorPostTextFromPayload(publishingPayload);
  const generatorImagePayload = resolveGeneratorPostImagePayload(publishingPayload, baseImageSource);
  const hasGeneratorPost = Boolean(
    generatorPostText || generatorImagePayload.uploadedImage || generatorImagePayload.imageUrl
  );
  const results = {
    decision: routingDecision.label,
    reason: routingReason,
    approved: {
      enabled: routingConfig.approved.enabled,
      chatId: routingConfig.approved.chatId || null,
      status: 'skipped'
    },
    rejected: {
      enabled: routingConfig.rejected.enabled,
      chatId: routingConfig.rejected.chatId || null,
      status: 'skipped'
    }
  };
  const approvedRouteLog = {
    generatorPostId,
    sellerClass: finalRoutingState.sellerClass || resolveFinalRoutingSellerClass(routingInput, routingGeneratorContext),
    sellerType: resolveFinalRoutingSellerType(routingInput, routingGeneratorContext),
    decision: cleanText(routingInput.decision || routingGeneratorContext?.evaluation?.decision) || '',
    routingDecision: routingDecision.label,
    bucket: routingDecision.bucket,
    wouldPostNormally:
      routingInput.wouldPostNormally === true || routingGeneratorContext?.learning?.wouldPostNormally === true,
    approvedEnabled: routingConfig.approved.enabled,
    approvedChatId: routingConfig.approved.chatId || '',
    productRule: routingInput.productRuleEvaluation?.matchedRuleName || '',
    reason: routingReason
  };

  console.info('[FINAL_ROUTING_INPUT]', buildFinalRoutingInputLog(finalRoutingState, routingDecision));
  console.info('[TELEGRAM_ROUTING_DECISION]', {
    generatorPostId,
    queueSourceType: cleanText(routingInput.queueSourceType) || 'generator_direct',
    bucket: routingDecision.bucket,
    label: routingDecision.label,
    approvedEnabled: routingConfig.approved.enabled,
    rejectedEnabled: routingConfig.rejected.enabled
  });
  console.info('[APPROVED_ROUTE_CHECK]', approvedRouteLog);
  if (routingDecision.bucket === 'approved' && routingConfig.approved.enabled) {
    try {
      console.info('[APPROVED_ALLOWED]', {
        ...approvedRouteLog,
        routeKey: 'approved'
      });
      if (hasGeneratorPost) {
        console.info('[ROUTING_USE_GENERATOR_POST]', {
          routeKey: 'approved',
          generatorPostId,
          hasText: Boolean(generatorPostText),
          imageSource: generatorImagePayload.imageSource
        });
      }
      const approvedPayload = {
        ...publishingPayload,
        telegramChatIds: [routingConfig.approved.chatId],
        debugInfoByChannel: {
          ...(publishingPayload.debugInfoByChannel || {}),
          telegram: ''
        },
        testMode: false,
        meta: {
          ...(publishingPayload.meta || {}),
          telegramRoutingChannel: 'approved',
          telegramRoutingDecision: routingDecision.label
        }
      };
      console.info('[APPROVED_SEND_START]', {
        generatorPostId,
        chatId: routingConfig.approved.chatId,
        decision: routingDecision.label,
        sellerClass: approvedRouteLog.sellerClass,
        wouldPostNormally: approvedRouteLog.wouldPostNormally
      });
      console.info('[APPROVED_CHANNEL_SEND]', {
        generatorPostId,
        decision: routingDecision.label,
        debugSuppressed: true
      });
      console.info('[APPROVED_CHANNEL_MAIN_ONLY]', {
        generatorPostId,
        decision: routingDecision.label,
        debugSuppressed: true,
        extraAnalysisMessage: false
      });
      const approvedResult = await publishSecondaryTelegramRoute({
        routeKey: 'approved',
        targetChatId: routingConfig.approved.chatId,
        queueSourceType: `${cleanText(routingInput.queueSourceType) || 'generator_direct'}_approved_route`,
        generatorPostId,
        payload: approvedPayload,
        imageSource: baseImageSource
      });
      const approvedCouponResult =
        approvedResult.duplicateBlocked === true
          ? null
          : await sendRouteCouponFollowUp({
              couponCode: cleanText(approvedPayload.couponCode),
              chatId: routingConfig.approved.chatId,
              titlePreview: cleanText(routingInput.title).slice(0, 120) || 'Veroeffentlicht Rabattcode',
              postContext: 'approved_coupon_follow_up',
              logTag: '[APPROVED_CODE_SEND_FAILED]',
              routeKey: 'approved',
              generatorPostId
            });
      results.approved = {
        ...results.approved,
        status: normalizePublishingQueueStatus(approvedResult.queueStatus, approvedResult.queueStatus || 'sent'),
        queueId: approvedResult.queueId,
        messageId: approvedResult.messageId,
        couponCodeMessageId: approvedCouponResult?.messageId || null,
        duplicateBlocked: approvedResult.duplicateBlocked === true
      };
      console.info('[APPROVED_SEND_OK]', {
        generatorPostId,
        chatId: routingConfig.approved.chatId,
        queueId: approvedResult.queueId,
        messageId: approvedResult.messageId,
        couponCodeMessageId: approvedCouponResult?.messageId || null,
        status: results.approved.status
      });
      console.info('[TELEGRAM_ROUTING_CHANNEL_SENT]', {
        routeKey: 'approved',
        generatorPostId,
        queueId: approvedResult.queueId,
        chatId: routingConfig.approved.chatId
      });
    } catch (error) {
      results.approved = {
        ...results.approved,
        status: 'failed',
        error: error instanceof Error ? error.message : 'Approved-Route fehlgeschlagen.'
      };
      console.error('[APPROVED_SEND_ERROR]', {
        generatorPostId,
        chatId: routingConfig.approved.chatId,
        error: results.approved.error
      });
      console.error('[TELEGRAM_ROUTING_CHANNEL_FAILED]', {
        routeKey: 'approved',
        generatorPostId,
        error: results.approved.error
      });
    }
  } else {
    const reason = buildRoutingSkipReason(routingConfig.approved, 'decision_not_approved');
    if (finalRoutingState.hardBlockedFbm === true || isFbmFinalRoutingSeller(routingInput, routingGeneratorContext)) {
      console.info('[APPROVED_CHANNEL_REJECT_FBM]', {
        asin: cleanText(routingInput.asin).toUpperCase() || '',
        reason: 'FBM_NOT_ALLOWED'
      });
      console.info('[APPROVED_CHANNEL_SKIP_FBM]', {
        generatorPostId,
        decision: routingDecision.label,
        sellerClass: finalRoutingState.sellerClass || resolveFinalRoutingSellerClass(routingInput, routingGeneratorContext),
        sellerType: resolveFinalRoutingSellerType(routingInput, routingGeneratorContext),
        reason: 'fbm_hard_block'
      });
    }
    console.info('[APPROVED_BLOCKED_REASON]', {
      ...approvedRouteLog,
      reason
    });
    console.info('[APPROVED_CHANNEL_SKIP]', {
      generatorPostId,
      decision: routingDecision.label,
      reason
    });
  }

  if (routingDecision.bucket === 'rejected' && routingConfig.rejected.enabled) {
    try {
      const shortcheckText = buildRejectedShortcheck({
        input: routingInput,
        generatorContext: routingGeneratorContext,
        decisionLabel: routingDecision.label,
        reason: routingReason
      });
      const rejectedText = buildRejectedCombinedPost({
        generatorPostText,
        shortcheckText
      });
      if (hasGeneratorPost) {
        console.info('[ROUTING_USE_GENERATOR_POST]', {
          routeKey: 'rejected',
          generatorPostId,
          hasText: Boolean(generatorPostText),
          imageSource: generatorImagePayload.imageSource
        });
        console.info('[REJECTED_CHANNEL_WITH_MAIN_POST]', {
          generatorPostId,
          decision: routingDecision.label,
          imageSource: generatorImagePayload.imageSource
        });
        console.info('[DEBUG_MERGED_INTO_CAPTION]', {
          generatorPostId,
          decision: routingDecision.label,
          shortcheckLength: shortcheckText.length
        });
      }
      console.info('[REJECTED_OLD_TEMPLATE_DISABLED]', {
        generatorPostId,
        textOnlyTemplate: false,
        combinedCaption: hasGeneratorPost
      });
      console.info('[REJECTED_CHANNEL_SEND]', {
        generatorPostId,
        decision: routingDecision.label,
        reasonPreview: shortenRoutingText(routingReason, 96)
      });
      const rejectedResult = await sendTelegramPost({
        text: rejectedText,
        uploadedImage: hasGeneratorPost ? generatorImagePayload.uploadedImage : '',
        imageUrl: hasGeneratorPost ? generatorImagePayload.imageUrl : '',
        disableWebPagePreview: !hasGeneratorPost,
        chatId: routingConfig.rejected.chatId,
        titlePreview: cleanText(routingInput.title) || 'NICHT VER\u00D6FFENTLICHT',
        hasAffiliateLink: hasGeneratorPost && Boolean(cleanText(publishingPayload.link)),
        postContext: 'deal_routing_rejected_combined',
        duplicateContext: {
          channelType: 'rejected',
          targetRef: routingConfig.rejected.chatId,
          asin: cleanText(routingInput.asin).toUpperCase(),
          title: cleanText(routingInput.title) || 'NICHT VER\u00D6FFENTLICHT',
          price: cleanText(publishingPayload.currentPrice || routingInput.currentPrice),
          url: cleanText(publishingPayload.normalizedUrl || publishingPayload.link || routingInput.normalizedUrl),
          originalUrl: cleanText(publishingPayload.link || routingInput.originalUrl)
        }
      });
      const rejectedCouponResult =
        rejectedResult?.duplicateBlocked === true
          ? null
          : await sendRouteCouponFollowUp({
              couponCode: cleanText(publishingPayload.couponCode),
              chatId: routingConfig.rejected.chatId,
              titlePreview: cleanText(routingInput.title).slice(0, 120) || 'Geblockt Rabattcode',
              postContext: 'rejected_coupon_follow_up',
              logTag: '[REJECTED_CODE_SEND_FAILED]',
              routeKey: 'rejected',
              generatorPostId
            });
      results.rejected = {
        ...results.rejected,
        status: 'sent',
        messageId: rejectedResult.messageId,
        couponCodeMessageId: rejectedCouponResult?.messageId || null,
        duplicateBlocked: rejectedResult?.duplicateBlocked === true
      };
      console.info('[TELEGRAM_ROUTING_CHANNEL_SENT]', {
        routeKey: 'rejected',
        generatorPostId,
        chatId: routingConfig.rejected.chatId
      });
    } catch (error) {
      results.rejected = {
        ...results.rejected,
        status: 'failed',
        error: error instanceof Error ? error.message : 'Rejected-Route fehlgeschlagen.'
      };
      console.error('[TELEGRAM_ROUTING_CHANNEL_FAILED]', {
        routeKey: 'rejected',
        generatorPostId,
        error: results.rejected.error
      });
    }
  } else {
    const reason = buildRoutingSkipReason(routingConfig.rejected, 'decision_not_rejected_or_review');
    console.info('[REJECTED_CHANNEL_SKIP]', {
      generatorPostId,
      decision: routingDecision.label,
      reason
    });
  }

  return results;
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
    debugInfoByChannel:
      input.debugInfoByChannel && typeof input.debugInfoByChannel === 'object' ? input.debugInfoByChannel : {},
    testMode: input.testMode === true,
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
    chatId: null,
    duplicateBlocked: false,
    duplicateKey: null,
    lastSentAt: null
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
    chatId: nextResult.chatId || safeBase.chatId || null,
    duplicateBlocked: nextResult.duplicateBlocked === true || safeBase.duplicateBlocked === true,
    duplicateKey: nextResult.duplicateKey || safeBase.duplicateKey || null,
    lastSentAt: nextResult.lastSentAt || safeBase.lastSentAt || null
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
        chatId: deliveries[0]?.chatId || deliveries[0]?.targetChatId || null,
        duplicateBlocked: deliveries[0]?.duplicateBlocked === true,
        duplicateKey: cleanText(deliveries[0]?.duplicateKey),
        lastSentAt: deliveries[0]?.lastSentAt || null
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

  const primaryCouponResult =
    summary.results.telegram?.duplicateBlocked === true
      ? null
      : await sendRouteCouponFollowUp({
          couponCode: cleanText(finalizedPublishingPayload.couponCode || input.couponCode),
          chatId: cleanText(summary.results.telegram?.chatId),
          titlePreview: cleanText(input.title).slice(0, 120) || 'Telegram Rabattcode',
          postContext: 'primary_coupon_follow_up',
          logTag: '[PRIMARY_CODE_SEND_FAILED]',
          routeKey: 'primary',
          generatorPostId
        });

  if (summary.results.telegram && primaryCouponResult?.messageId) {
    summary.results.telegram.couponCodeMessageId = primaryCouponResult.messageId;
  }

  const routingOutputs = await publishTelegramRoutingOutputs({
    input,
    generatorContext,
    publishingPayload: finalizedPublishingPayload,
    generatorPostId
  });

  return {
    success: true,
    postedAt: summary.postedAt || null,
    queue: summary.queue || queueEntry,
    results: summary.results,
    deliveries: summary.deliveries,
    generatorContext,
    routingOutputs
  };
}

export const __testablesDirectPublisher = {
  buildEmptyChannelResult,
  buildChannelResult,
  buildFinalRoutingInputState,
  buildRejectedCombinedPost,
  resolveTelegramRoutingDecision,
  isApprovedFinalRoutingSellerAllowed,
  summarizeQueueResults,
  assertDirectPublishingTargets,
  validatePublishingPrice
};
