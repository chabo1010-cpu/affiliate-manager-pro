import { getDb } from '../db.js';
import { normalizeSellerType } from './dealHistoryService.js';
import { logGeneratorDebug } from './generatorFlowService.js';
import { SELLER_TYPE_LOGIC, evaluateSellerTypeDeal, getSellerTypeFeedbackSummary, getSellerTypeLogicConfig } from './sellerTypeLogicService.js';
import { getSimilarCaseSignals } from './keepaFakeDropService.js';

const db = getDb();

function cleanText(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function parseNumber(value, fallback = null) {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }

  if (typeof value !== 'string') {
    return fallback;
  }

  const trimmed = value.trim();
  if (!trimmed) {
    return fallback;
  }

  const parsed = Number.parseFloat(trimmed.replace(/[^\d,.-]/g, '').replace(',', '.'));
  return Number.isFinite(parsed) ? parsed : fallback;
}

function parseInteger(value, fallback = null) {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return Math.trunc(value);
  }

  if (typeof value !== 'string') {
    return fallback;
  }

  const trimmed = value.trim();
  if (!trimmed) {
    return fallback;
  }

  const parsed = Number.parseInt(trimmed, 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function nowIso() {
  return new Date().toISOString();
}

function buildKeepaPreview(context = {}) {
  const result = context.result || null;
  const keepaClient = context.client && typeof context.client === 'object' ? context.client : null;
  const fakeDropFeatures = result?.fakeDrop?.features && typeof result.fakeDrop.features === 'object' ? result.fakeDrop.features : {};
  const avg90 = parseNumber(keepaClient?.avg90 ?? fakeDropFeatures.avg90, null);
  const avg180 = parseNumber(keepaClient?.avg180 ?? fakeDropFeatures.avg180, null);
  const min90 = parseNumber(keepaClient?.min90, null);
  const isLowest90 = keepaClient?.isLowest90 === true;
  if (!result) {
    return {
      available: false,
      status: context.status || 'missing',
      reason: context.reason || 'Keepa-Daten fehlen.',
      requestedAt: context.requestedAt || null,
      avg90,
      avg180,
      min90,
      isLowest90,
      keepaClient
    };
  }

  return {
    available: true,
    status: context.status || 'loaded',
    cached: context.cached === true,
    requestedAt: context.requestedAt || null,
    lastSyncedAt: result.lastSyncedAt || result.updatedAt || null,
    keepaResultId: result.id || null,
    currentPrice: result.currentPrice ?? null,
    referencePrice: result.referencePrice ?? null,
    keepaDiscount: result.keepaDiscount ?? null,
    dealScore: result.dealScore ?? null,
    dealStrength: result.dealStrength || null,
    strengthReason: result.strengthReason || '',
    avg90,
    avg180,
    min90,
    isLowest90,
    keepaClient,
    sellerType: result.sellerType || null,
    categoryName: result.categoryName || '',
    comparisonSource: result.comparisonSource || null,
    comparisonStatus: result.comparisonStatus || null,
    comparisonPrice: result.comparisonPrice ?? null,
    priceDifferenceAbs: result.priceDifferenceAbs ?? null,
    priceDifferencePct: result.priceDifferencePct ?? null,
    comparisonCheckedAt: result.comparisonCheckedAt || null,
    fakeDrop: result.fakeDrop
      ? {
          reviewItemId: result.fakeDrop.reviewItemId || null,
          classification: result.fakeDrop.classification || '',
          classificationLabel: result.fakeDrop.classificationLabel || '',
          fakeDropRisk: result.fakeDrop.fakeDropRisk ?? null,
          analysisReason: result.fakeDrop.analysisReason || '',
          chartPoints: Array.isArray(result.fakeDrop.chartPoints) ? result.fakeDrop.chartPoints : [],
          features: result.fakeDrop.features && typeof result.fakeDrop.features === 'object' ? result.fakeDrop.features : {},
          currentLabel: result.fakeDrop.currentLabel || null,
          tags: Array.isArray(result.fakeDrop.tags) ? result.fakeDrop.tags : []
        }
      : null
  };
}

function buildInternetPreview(context = {}, keepaPreview = null) {
  const result = context.result || null;
  const fallback = keepaPreview && typeof keepaPreview === 'object' ? keepaPreview : {};
  const requestedAt = context.requestedAt || fallback.comparisonCheckedAt || nowIso();
  const comparisonSource = cleanText(result?.comparisonSource || context.comparisonSource || fallback.comparisonSource);
  const comparisonStatus =
    cleanText(result?.comparisonStatus || context.comparisonStatus || fallback.comparisonStatus) || 'not_connected';
  const comparisonPrice = parseNumber(result?.comparisonPrice ?? context.comparisonPrice ?? fallback.comparisonPrice, null);
  const priceDifferenceAbs = parseNumber(result?.priceDifferenceAbs ?? context.priceDifferenceAbs ?? fallback.priceDifferenceAbs, null);
  const priceDifferencePct = parseNumber(result?.priceDifferencePct ?? context.priceDifferencePct ?? fallback.priceDifferencePct, null);
  const comparisonCheckedAt = result?.comparisonCheckedAt || context.comparisonCheckedAt || fallback.comparisonCheckedAt || null;
  const available = Boolean(comparisonSource || comparisonPrice !== null || priceDifferencePct !== null) && comparisonStatus !== 'not_connected';

  if (!available) {
    return {
      available: false,
      marketAvailable: false,
      status: cleanText(context.status) || comparisonStatus || 'missing',
      reason: cleanText(context.reason) || 'Kein Marktvergleich verfuegbar.',
      requestedAt,
      comparisonSource: comparisonSource || null,
      comparisonStatus,
      comparisonPrice,
      priceDifferenceAbs,
      priceDifferencePct,
      comparisonCheckedAt
    };
  }

  return {
    available: true,
    marketAvailable: true,
    status: cleanText(context.status) || comparisonStatus || 'available',
    reason: '',
    requestedAt,
    comparisonSource: comparisonSource || null,
    comparisonStatus,
    comparisonPrice,
    priceDifferenceAbs,
    priceDifferencePct,
    comparisonCheckedAt
  };
}

function normalizeKeepaContext(input = {}) {
  if (input.keepaPreview) {
    return input.keepaPreview;
  }

  if (input.keepaContext && typeof input.keepaContext === 'object') {
    return buildKeepaPreview(input.keepaContext);
  }

  if (input.keepaResultRecord && typeof input.keepaResultRecord === 'object') {
    return buildKeepaPreview({
      result: input.keepaResultRecord,
      status: 'provided',
      cached: true,
      requestedAt: nowIso()
    });
  }

  if (input.keepaResult && typeof input.keepaResult === 'object') {
    if (input.keepaResult.result) {
      return buildKeepaPreview(input.keepaResult);
    }

    if (input.keepaResult.resultRecord) {
      return buildKeepaPreview({
        result: input.keepaResult.resultRecord,
        status: cleanText(input.keepaResult.status) || 'provided',
        cached: true,
        requestedAt: nowIso()
      });
    }

    return {
      available: input.keepaResult.ok === true,
      status: cleanText(input.keepaResult.status) || 'missing',
      reason: cleanText(input.keepaResult.reason) || 'Keepa-Daten fehlen.',
      requestedAt: nowIso()
    };
  }

  return {
    available: false,
    status: input.keepaRequired ? 'required_missing' : 'missing',
    reason: input.keepaRequired ? 'Keepa-Pruefung ist erforderlich.' : 'Keepa-Daten fehlen.',
    requestedAt: nowIso()
  };
}

function normalizeInternetContext(input = {}, keepaPreview = null) {
  if (input.internetPreview) {
    return input.internetPreview;
  }

  if (input.internetContext && typeof input.internetContext === 'object') {
    return buildInternetPreview(input.internetContext, keepaPreview);
  }

  return buildInternetPreview({}, keepaPreview);
}

function buildAmazonPreview(context = {}) {
  const result = context.result || null;
  if (!result) {
    return {
      available: false,
      status: context.status || 'missing',
      reason: context.reason || 'Amazon-Affiliate-Daten fehlen.',
      requestedAt: context.requestedAt || null
    };
  }

  return {
    available: true,
    status: context.status || 'loaded',
    requestedAt: context.requestedAt || null,
    asin: result.asin || null,
    title: result.title || '',
    brand: result.brand || '',
    categoryName: result.categoryName || '',
    imageUrl: result.imageUrl || '',
    affiliateUrl: result.affiliateUrl || '',
    detailPageUrl: result.detailPageUrl || '',
    priceDisplay: result.priceDisplay || '',
    availability: result.availability || ''
  };
}

function normalizeAmazonContext(input = {}) {
  if (input.amazonPreview) {
    return input.amazonPreview;
  }

  if (input.amazonContext && typeof input.amazonContext === 'object') {
    return buildAmazonPreview(input.amazonContext);
  }

  if (input.amazonResult && typeof input.amazonResult === 'object') {
    return buildAmazonPreview({
      result: input.amazonResult,
      status: 'provided',
      requestedAt: nowIso()
    });
  }

  return {
    available: false,
    status: 'missing',
    reason: 'Amazon-Affiliate-Daten wurden noch nicht geladen.',
    requestedAt: nowIso()
  };
}

function normalizeList(values = []) {
  return [...new Set((Array.isArray(values) ? values : [values]).map((item) => cleanText(String(item || '')).toLowerCase()).filter(Boolean))];
}

function getDefaultQueueChannels(sourceType = '') {
  const normalizedSourceType = cleanText(sourceType).toLowerCase();

  if (['generator', 'generator_direct', 'manual_post'].includes(normalizedSourceType)) {
    return ['telegram', 'whatsapp', 'facebook'];
  }

  if (['scrapper', 'copybot'].includes(normalizedSourceType)) {
    return ['telegram', 'whatsapp'];
  }

  if (normalizedSourceType === 'auto_deals') {
    return ['telegram'];
  }

  return ['telegram'];
}

function buildDealLockPreview(input = {}) {
  const source =
    input.dealLockStatus && typeof input.dealLockStatus === 'object'
      ? input.dealLockStatus
      : input.dealLock && typeof input.dealLock === 'object'
        ? input.dealLock
        : {};
  const lastDeal = source.lastDeal && typeof source.lastDeal === 'object' ? source.lastDeal : null;
  const activeRegistryLock =
    source.activeRegistryLock && typeof source.activeRegistryLock === 'object' ? source.activeRegistryLock : null;

  return {
    integrated: true,
    checked: Object.keys(source).length > 0,
    blocked: source.blocked === true,
    blockCode: cleanText(source.blockCode),
    blockReason: cleanText(source.blockReason),
    remainingSeconds: parseInteger(source.remainingSeconds, 0) ?? 0,
    repostCooldownEnabled: source.repostCooldownEnabled === true,
    repostCooldownHours: parseNumber(source.repostCooldownHours, null),
    dealHash: cleanText(source.dealHash),
    asin: cleanText(source.asin).toUpperCase(),
    normalizedUrl: cleanText(source.normalizedUrl),
    postingCount: parseInteger(source.postingCount, 0) ?? 0,
    lastPostedAt: lastDeal?.postedAt || null,
    lastChannel: cleanText(lastDeal?.channel),
    activeQueueLock: activeRegistryLock
      ? {
          status: cleanText(activeRegistryLock.status),
          queueId: parseInteger(activeRegistryLock.queueId, 0) || null,
          lastQueueStatus: cleanText(activeRegistryLock.lastQueueStatus),
          channel: cleanText(activeRegistryLock.channel),
          decisionReason: cleanText(activeRegistryLock.decisionReason),
          updatedAt: activeRegistryLock.updatedAt || null
        }
      : null
  };
}

function buildReglerPreview(input = {}, evaluation = {}) {
  const context = input.reglerContext && typeof input.reglerContext === 'object' ? input.reglerContext : {};
  const config = evaluation.config && typeof evaluation.config === 'object' ? evaluation.config : {};
  const metrics = evaluation.metrics && typeof evaluation.metrics === 'object' ? evaluation.metrics : {};
  const checks = evaluation.checks && typeof evaluation.checks === 'object' ? evaluation.checks : {};
  const pricingRule = context.pricingRule && typeof context.pricingRule === 'object' ? context.pricingRule : null;
  const samplingRule = context.samplingRule && typeof context.samplingRule === 'object' ? context.samplingRule : null;

  return {
    integrated: true,
    mode: cleanText(context.mode) || (pricingRule || samplingRule ? 'pricing_sampling_and_seller_type' : 'seller_type_logic'),
    stage: cleanText(context.stage) || 'decision',
    decisionStatus: cleanText(context.decisionStatus || context.status) || cleanText(evaluation.decision) || 'manual_review',
    decisionReason:
      cleanText(context.decisionReason || context.reason) || (Array.isArray(evaluation.reasons) ? evaluation.reasons.join(' | ') : ''),
    score: parseNumber(context.score ?? metrics.finalScore, null),
    discount: parseNumber(context.discount ?? metrics.keepaDiscount, null),
    sellerRating: parseNumber(context.sellerRating, null),
    sampleValue: parseNumber(context.sampleValue, null),
    keepaRating: cleanText(evaluation.keepaRating),
    sellerTypeConfig: {
      minDiscount: parseNumber(config.minDiscount, null),
      minScore: parseNumber(config.minScore, null),
      maxFakeDropRisk: parseNumber(config.maxFakeDropRisk, null),
      allowTestGroup: config.allowTestGroup === true
    },
    checks: {
      keepaAvailable: checks.keepaAvailable === true,
      minDiscountPassed: checks.minDiscountPassed === true,
      minScorePassed: checks.minScorePassed === true,
      fakeDropPassed: checks.fakeDropPassed === true,
      classificationPassed: checks.classificationPassed === true,
      keepaOk: context.keepaOk === undefined ? null : context.keepaOk === true,
      internetOk: context.internetOk === undefined ? null : context.internetOk === true
    },
    source: context.source && typeof context.source === 'object' ? context.source : null,
    pricingRule,
    samplingRule
  };
}

function buildQueuePreview(input = {}, routing = {}) {
  const context = input.queueContext && typeof input.queueContext === 'object' ? input.queueContext : {};
  const channels = normalizeList(context.channels?.length ? context.channels : getDefaultQueueChannels(input.sourceType));

  return {
    integrated: true,
    required: context.required !== false,
    mode: cleanText(context.mode) || 'publisher_queue',
    currentStatus: cleanText(context.currentStatus || context.status) || 'not_enqueued',
    queueId: parseInteger(context.queueId, 0) || null,
    splitByPlatform: context.splitByPlatform !== false,
    preSendPersistence: context.preSendPersistence !== false,
    recoveryEnabled: context.recoveryEnabled !== false,
    routeType: cleanText(context.routeType) || (cleanText(input.sourceType).toLowerCase() === 'generator' ? 'manual' : 'automatic'),
    channels,
    nextStep:
      routing.decision === 'test_group'
        ? 'enqueue_before_send'
        : routing.decision === 'block'
          ? 'skip_send'
          : 'hold_for_review'
  };
}

function buildDecisionEnginePreview({ internetPreview, keepaPreview, dealLock, regler, queue, routing }) {
  return {
    primaryLogic: 'internet',
    fallbackLogic: 'keepa',
    aiOptional: true,
    worksWithoutAi: true,
    modules: {
      internet: internetPreview.available ? 'primary' : 'waiting_for_fallback',
      keepa: routing.strategy === 'keepa_fallback' ? (keepaPreview.available ? 'active_fallback' : 'fallback_missing') : 'standby',
      sperrmodul: dealLock.blocked ? 'blocked' : dealLock.checked ? 'passed' : 'not_checked',
      regler: regler.decisionStatus || 'active',
      queue: queue.currentStatus || 'not_enqueued'
    },
    pipeline: [
      {
        id: 'sperrmodul',
        label: 'Sperrcheck',
        status: dealLock.blocked ? 'blocked' : dealLock.checked ? 'passed' : 'not_checked'
      },
      {
        id: 'internet',
        label: 'Internetvergleich',
        status: internetPreview.available ? 'primary' : 'missing'
      },
      {
        id: 'keepa',
        label: 'Keepa-Fallback',
        status: routing.strategy === 'keepa_fallback' ? (keepaPreview.available ? 'used' : 'missing') : 'standby'
      },
      {
        id: 'regler',
        label: 'Regler',
        status: regler.decisionStatus || 'active'
      },
      {
        id: 'queue',
        label: 'Queue',
        status: queue.currentStatus || 'not_enqueued'
      }
    ]
  };
}

function getSourceLabel(sourceType) {
  if (sourceType === 'generator') {
    return 'Generator';
  }

  if (sourceType === 'scrapper') {
    return 'Scrapper';
  }

  if (sourceType === 'copybot') {
    return 'Copybot';
  }

  if (sourceType === 'auto_deals') {
    return 'Auto-Deals';
  }

  return 'Automatik';
}

function getRoutingLabel(decision) {
  if (decision === 'test_group') {
    return 'Testgruppe';
  }

  if (decision === 'block') {
    return 'Blockieren';
  }

  return 'Review';
}

function resolveInternetGapPct(internetPreview, currentPrice) {
  if (internetPreview.priceDifferencePct !== null && internetPreview.priceDifferencePct !== undefined) {
    return internetPreview.priceDifferencePct;
  }

  const currentPriceValue = parseNumber(currentPrice, null);
  const comparisonPriceValue = parseNumber(internetPreview.comparisonPrice, null);

  if (currentPriceValue !== null && comparisonPriceValue !== null && currentPriceValue > 0) {
    return Math.round(((comparisonPriceValue - currentPriceValue) / currentPriceValue) * 1000) / 10;
  }

  return null;
}

function buildInternetPrimaryDecision(input = {}, internetPreview) {
  if (!internetPreview.available || !internetPreview.marketAvailable) {
    return null;
  }

  const minGapPct = Math.max(0, parseNumber(input.marketMinGapPct, 10) ?? 10);
  const marketGapPct = resolveInternetGapPct(internetPreview, input.currentPrice);
  const comparisonSourceLabel = internetPreview.comparisonSource || 'Marktvergleich';

  if (marketGapPct !== null && marketGapPct >= minGapPct) {
    return {
      decision: 'test_group',
      reason: `${comparisonSourceLabel} bestaetigt den Deal mit +${marketGapPct.toFixed(1)}% Marktvorteil.`,
      strategy: 'internet_primary'
    };
  }

  if (marketGapPct !== null && marketGapPct <= 0) {
    return {
      decision: 'block',
      reason: `${comparisonSourceLabel} zeigt keinen echten Marktdeal (${marketGapPct.toFixed(1)}%).`,
      strategy: 'internet_primary'
    };
  }

  return {
    decision: 'review',
    reason:
      marketGapPct !== null
        ? `${comparisonSourceLabel} ist vorhanden, aber mit +${marketGapPct.toFixed(1)}% nicht eindeutig genug.`
        : `${comparisonSourceLabel} ist vorhanden, liefert aber keinen belastbaren Marktabstand.`,
    strategy: 'internet_primary'
  };
}

function buildKeepaFallbackDecision(input = {}, keepaPreview, evaluation) {
  if (evaluation.decision === 'hold') {
    return {
      decision: 'block',
      reason: evaluation.reasons.join(' | '),
      strategy: 'keepa_fallback'
    };
  }

  if (!keepaPreview.available && (input.enforceDecision || input.keepaRequired)) {
    return {
      decision: 'review',
      reason: keepaPreview.reason || 'Keepa-Pruefung fehlt.',
      strategy: 'keepa_fallback'
    };
  }

  if (evaluation.testGroupApproved) {
    return {
      decision: 'test_group',
      reason: evaluation.reasons.join(' | '),
      strategy: 'keepa_fallback'
    };
  }

  return {
    decision: 'review',
    reason: evaluation.reasons.join(' | '),
    strategy: 'keepa_fallback'
  };
}

function buildRoutingDecision(input = {}, internetPreview, keepaPreview, evaluation) {
  const internetDecision = buildInternetPrimaryDecision(input, internetPreview);
  return internetDecision || buildKeepaFallbackDecision(input, keepaPreview, evaluation);
}

export function evaluateLearningRoute(input = {}) {
  const sellerType = normalizeSellerType(input.sellerType);
  const keepaPreview = normalizeKeepaContext(input);
  const internetPreview = normalizeInternetContext(input, keepaPreview);
  const amazonPreview = normalizeAmazonContext(input);
  const patternSupportEnabled = input.patternSupportEnabled !== false;

  logGeneratorDebug('LEARNING LOGIC LOADED', {
    sourceType: input.sourceType || 'generator',
    sellerType,
    enforceDecision: input.enforceDecision === true,
    internetStatus: internetPreview.status,
    keepaStatus: keepaPreview.status,
    amazonStatus: amazonPreview.status
  });

  const sellerTypeConfig = getSellerTypeLogicConfig(sellerType);
  const feedbackSummary =
    input.feedbackSummary?.sellerType === sellerType ? input.feedbackSummary : getSellerTypeFeedbackSummary(sellerType);
  const similarCaseSignals =
    patternSupportEnabled && keepaPreview.available && keepaPreview.fakeDrop
      ? getSimilarCaseSignals(
          {
            reviewItemId: keepaPreview.fakeDrop.reviewItemId || 0,
            keepaResultId: keepaPreview.keepaResultId || 0,
            asin: input.asin || '',
            sellerType,
            categoryName: keepaPreview.categoryName || input.categoryName || '',
            sourceType: input.sourceType || 'generator',
            currentPrice: keepaPreview.currentPrice,
            keepaDiscount: keepaPreview.keepaDiscount,
            fakeDropRisk: keepaPreview.fakeDrop.fakeDropRisk ?? null,
            classification: keepaPreview.fakeDrop.classification || '',
            features: keepaPreview.fakeDrop.features || {}
          },
          {
            limit: 4,
            minSimilarityScore: 58,
            scanLimit: 60
          }
        )
      : {
          sellerType,
          consideredCount: 0,
          matchedCount: 0,
          cases: [],
          summary: {
            total: 0,
            positiveCount: 0,
            negativeCount: 0,
            uncertainCount: 0,
            dominantLabel: null,
            dominantLabelLabel: null,
            riskAdjustment: 0,
            scoreAdjustment: 0
          }
        };
  const evaluation = evaluateSellerTypeDeal({
    sellerType,
    keepaAvailable: keepaPreview.available,
    keepaDiscount: keepaPreview.keepaDiscount,
    keepaDealScore: keepaPreview.dealScore,
    fakeDropRisk: keepaPreview.fakeDrop?.fakeDropRisk ?? null,
    fakeDropClassification: keepaPreview.fakeDrop?.classification || '',
    feedbackSummary,
    similarCaseSummary: similarCaseSignals.summary
  });
  const routing = buildRoutingDecision(input, internetPreview, keepaPreview, evaluation);
  const dealLock = buildDealLockPreview(input);
  const regler = buildReglerPreview(input, evaluation);
  const queue = buildQueuePreview(input, routing);
  const decisionEngine = buildDecisionEnginePreview({
    internetPreview,
    keepaPreview,
    dealLock,
    regler,
    queue,
    routing
  });
  const sourceLabel = getSourceLabel(input.sourceType || 'generator');
  const learning = {
    sourceType: input.sourceType || 'generator',
    sourceLabel,
    enforced: input.enforceDecision === true,
    keepaRequired: input.keepaRequired === true,
    primaryDecisionSource: routing.strategy === 'internet_primary' ? 'internetvergleich' : 'keepa_fallback',
    fallbackUsed: routing.strategy === 'keepa_fallback',
    internetPrimary: routing.strategy === 'internet_primary',
    keepaFallbackUsed: routing.strategy === 'keepa_fallback',
    aiRequired: false,
    worksWithoutAi: true,
    queueRequired: true,
    queueIntegrated: true,
    reglerIntegrated: true,
    sperrmodulIntegrated: true,
    routingDecision: routing.decision,
    routingLabel: getRoutingLabel(routing.decision),
    canReachTestGroup: routing.decision === 'test_group',
    shouldReview: routing.decision === 'review',
    blocked: routing.decision === 'block',
    dealLockBlocked: dealLock.blocked,
    queueMode: queue.mode,
    queueStatus: queue.currentStatus,
    reglerMode: regler.mode,
    patternSupportEnabled,
    reason: routing.reason,
    decisionEngine,
    workflow:
      routing.strategy === 'internet_primary'
        ? routing.decision === 'test_group'
          ? 'Deal -> Sperrcheck -> Internetvergleich -> Marktentscheidung -> Queue'
          : routing.decision === 'block'
            ? 'Deal -> Sperrcheck -> Internetvergleich -> Marktentscheidung -> Block'
            : 'Deal -> Sperrcheck -> Internetvergleich -> Review'
        : routing.decision === 'test_group'
          ? 'Deal -> Sperrcheck -> Keepa-Fallback -> Regler -> Queue'
          : routing.decision === 'block'
            ? 'Deal -> Sperrcheck -> Keepa-Fallback -> Regler -> Block'
            : 'Deal -> Sperrcheck -> Keepa-Fallback -> Review'
  };

  logGeneratorDebug('SELLER TYPE LOGIC ACTIVE', {
    sourceType: input.sourceType || 'generator',
    sellerType,
    minDiscount: sellerTypeConfig.minDiscount,
    minScore: sellerTypeConfig.minScore,
    maxFakeDropRisk: sellerTypeConfig.maxFakeDropRisk
  });
  logGeneratorDebug(`PATTERN SUPPORT ACTIVE: ${sellerType}`, {
    sourceType: input.sourceType || 'generator',
    enabled: patternSupportEnabled,
    matchedSimilarCases: similarCaseSignals.matchedCount
  });
  logGeneratorDebug('SELLER TYPE LEARNING APPLIED', {
    sourceType: input.sourceType || 'generator',
    sellerType,
    feedbackRiskAdjustment: feedbackSummary.riskAdjustment,
    feedbackScoreAdjustment: feedbackSummary.scoreAdjustment,
    similarCasesMatched: similarCaseSignals.matchedCount,
    similarRiskAdjustment: similarCaseSignals.summary.riskAdjustment,
    similarScoreAdjustment: similarCaseSignals.summary.scoreAdjustment
  });
  logGeneratorDebug('LEARNING LOGIC DECISION UPDATED', {
    sourceType: input.sourceType || 'generator',
    sellerType,
    matchedSimilarCases: similarCaseSignals.matchedCount,
    finalScore: evaluation.metrics.finalScore,
    routingDecision: routing.decision,
    strategy: routing.strategy
  });

  logGeneratorDebug(routing.strategy === 'internet_primary' ? 'INTERNET COMPARISON PRIMARY' : 'KEEPA FALLBACK USED', {
    sourceType: input.sourceType || 'generator',
    sellerType,
    internetStatus: internetPreview.status,
    keepaStatus: keepaPreview.status,
    routingDecision: routing.decision
  });
  logGeneratorDebug('DECISION ENGINE INTEGRATED', {
    sourceType: input.sourceType || 'generator',
    sellerType,
    primaryLogic: decisionEngine.primaryLogic,
    fallbackLogic: decisionEngine.fallbackLogic,
    dealLockBlocked: dealLock.blocked,
    reglerMode: regler.mode,
    queueMode: queue.mode
  });

  logGeneratorDebug('DEAL DECISION RESULT', {
    sourceType: input.sourceType || 'generator',
    sellerType,
    routingDecision: learning.routingDecision,
    finalScore: evaluation.metrics.finalScore,
    internetStatus: internetPreview.status,
    keepaStatus: keepaPreview.status
  });

  return {
    asin: cleanText(input.asin).toUpperCase(),
    sellerType,
    currentPrice: parseNumber(input.currentPrice, null),
    sperrmodul: dealLock,
    dealLock,
    internet: internetPreview,
    keepa: keepaPreview,
    amazon: amazonPreview,
    regler,
    queue,
    decisionEngine,
    evaluation,
    learning,
    similarCases: similarCaseSignals.cases,
    similarCaseSummary: similarCaseSignals.summary,
    review: keepaPreview.fakeDrop
      ? {
          reviewItemId: keepaPreview.fakeDrop.reviewItemId || null,
          currentLabel: keepaPreview.fakeDrop.currentLabel || null,
          tags: keepaPreview.fakeDrop.tags || []
        }
      : null
  };
}

function getSellerTypeActivity(sellerType) {
  const lastResult = db
    .prepare(
      `
        SELECT asin, title, workflow_status, deal_strength, last_synced_at, updated_at
        FROM keepa_results
        WHERE seller_type = ?
        ORDER BY COALESCE(last_synced_at, updated_at, created_at) DESC
        LIMIT 1
      `
    )
    .get(sellerType);
  const lastAlert = db
    .prepare(
      `
        SELECT ka.status, ka.created_at, ka.error_message
        FROM keepa_alerts ka
        JOIN keepa_results kr ON kr.id = ka.keepa_result_id
        WHERE kr.seller_type = ?
        ORDER BY ka.created_at DESC
        LIMIT 1
      `
    )
    .get(sellerType);

  return {
    lastRunAt: lastResult?.last_synced_at || lastResult?.updated_at || null,
    lastDecision: lastAlert?.status || lastResult?.workflow_status || 'noch_keine',
    lastDecisionDetail: lastAlert?.error_message || lastResult?.title || '',
    lastAsin: lastResult?.asin || '',
    lastStrength: lastResult?.deal_strength || ''
  };
}

export function getLearningLogicOverview() {
  const generatorPostsWithLearning =
    db.prepare(`SELECT COUNT(*) AS count FROM generator_posts WHERE generator_context_json IS NOT NULL AND generator_context_json != ''`).get()?.count || 0;
  const scrapperReviewCount = db.prepare(`SELECT COUNT(*) AS count FROM imported_deals WHERE status = 'review'`).get()?.count || 0;
  const automaticBlockedCount =
    db.prepare(`SELECT COUNT(*) AS count FROM keepa_review_items WHERE review_status = 'open'`).get()?.count || 0;

  return {
    requestedAt: nowIso(),
    pipeline: [
      {
        id: 'generator',
        label: 'Generator',
        mode: 'manueller Arbeitsbereich',
        integrationMode: 'unterstuetzend',
        connected: true,
        detail: `${generatorPostsWithLearning} Generator-Posts mit Lern-Kontext gespeichert.`
      },
      {
        id: 'scrapper',
        label: 'Scrapper',
        mode: 'Rohdeal-Eingang',
        integrationMode: 'Pflichtpruefung',
        connected: true,
        detail: `${scrapperReviewCount} Scrapper-/Copybot-Faelle aktuell in Review.`
      },
      {
        id: 'auto_deals',
        label: 'Automatische Deals',
        mode: 'Keepa / Amazon API / spaeter Auto-Deals',
        integrationMode: 'erzwungen ueber Lern-Logik',
        connected: true,
        detail: `${automaticBlockedCount} Keepa-Review-Faelle offen oder wartend.`
      }
    ],
    sellerTypes: Object.values(SELLER_TYPE_LOGIC).map((item) => ({
      id: item.id,
      keepaRating: item.keepaRating,
      minDiscount: item.minDiscount,
      minScore: item.minScore,
      maxFakeDropRisk: item.maxFakeDropRisk,
      learningLabels: item.learningLabels,
      feedback: getSellerTypeFeedbackSummary(item.id),
      activity: getSellerTypeActivity(item.id)
    }))
  };
}
