import { extractKeepaFallbackMetrics } from '../keepaFakeDropService.js';
import { logGeneratorDebug } from '../generatorFlowService.js';
import { evaluateSellerDecisionPolicy, resolveSellerIdentity } from '../sellerClassificationService.js';
import { getDealEngineSettings, getRequiredMarketAdvantagePct, resolveDealEngineDayPart } from './configService.js';
import { resolveAiAssistance } from './aiResolverService.js';
import { evaluateFakePatterns } from './fakePatternService.js';
import { evaluateKeepaFallback } from './keepaFallbackService.js';
import { evaluateMarketComparison } from './marketService.js';
import { enqueueApprovedDeal } from './publisherService.js';
import { createDealEngineRun } from './repositoryService.js';
import {
  cleanText,
  extractAsinFromAmazonUrl,
  isAmazonLink,
  normalizeDayPart,
  normalizeSellerArea,
  parseBool,
  parseNumber,
  round,
  summarizeReasons
} from './shared.js';

function createReasonDetail(code, message, severity = 'info', meta = null) {
  return {
    code,
    message,
    severity,
    ...(meta && typeof meta === 'object' ? { meta } : {})
  };
}

function normalizeReasonDetails(reasons = [], fallbackCode = 'info', fallbackSeverity = 'info') {
  const items = Array.isArray(reasons) ? reasons : [reasons];

  return items
    .map((item) => {
      if (item && typeof item === 'object' && !Array.isArray(item)) {
        const code = cleanText(item.code) || fallbackCode;
        const message = cleanText(item.message || item.reason || item.detail || item.label);
        if (!message) {
          return null;
        }

        return {
          code,
          message,
          severity: cleanText(item.severity) || fallbackSeverity,
          ...(item.meta && typeof item.meta === 'object' ? { meta: item.meta } : {})
        };
      }

      const message = cleanText(String(item || ''));
      if (!message) {
        return null;
      }

      return createReasonDetail(fallbackCode, message, fallbackSeverity);
    })
    .filter(Boolean);
}

function mergeReasonDetails(...collections) {
  return collections
    .flatMap((collection) => normalizeReasonDetails(collection))
    .filter((item, index, source) => source.findIndex((candidate) => candidate.code === item.code && candidate.message === item.message) === index);
}

function mapReasonMessages(reasonDetails = []) {
  return normalizeReasonDetails(reasonDetails).map((item) => item.message);
}

function buildMarketComparisonAlias(marketResult = {}) {
  return {
    available: marketResult.available === true,
    blocked: marketResult.blocked === true,
    blockedReason: cleanText(marketResult.blockedReason),
    validOfferCount: Number(marketResult.validOfferCount || 0),
    invalidOfferCount: Number(marketResult.invalidOfferCount || 0),
    lowestPrice: marketResult.marketPrice ?? null,
    marketPrice: marketResult.marketPrice ?? null,
    marketAdvantagePct: marketResult.marketAdvantagePct ?? null,
    cheapestOffer: marketResult.cheapestOffer || null,
    contradictoryPrices: marketResult.contradictoryPrices === true
  };
}

function logDecisionFlow(tag, payload = {}) {
  try {
    console.info(`[${tag}]`, payload);
  } catch {
    console.info(`[${tag}]`);
  }

  logGeneratorDebug(tag, payload);
}

function buildSellerAnalysis(policy = {}) {
  const seller = policy.seller || {};

  return {
    sellerClass: seller.sellerClass || 'UNKNOWN',
    sellerType: seller.sellerType || 'UNKNOWN',
    soldByAmazon: seller.soldByAmazon ?? null,
    shippedByAmazon: seller.shippedByAmazon ?? null,
    soldByAmazonLabel: seller.details?.soldByAmazonLabel || 'unbekannt',
    shippedByAmazonLabel: seller.details?.shippedByAmazonLabel || 'unbekannt',
    detectionSource: seller.details?.detectionSource || 'unknown',
    detectionSources: seller.details?.detectionSources || [],
    marketCompareAllowed: policy.marketComparison?.allowed === true,
    marketCompareBlockedReason: policy.marketComparison?.allowed === true ? '' : cleanText(policy.marketComparison?.reason),
    aiAllowed: policy.ai?.allowed === true,
    aiBlockedReason: policy.ai?.allowed === true ? '' : cleanText(policy.ai?.reason),
    unknownSellerMode: cleanText(policy.unknownSellerMode) || 'review',
    unknownSellerAction: cleanText(policy.unknownSellerAction) || 'pass'
  };
}

function attachAnalysisAliases(analysis = {}, marketResult = {}, aiResolution = null) {
  const marketComparison = buildMarketComparisonAlias(marketResult || analysis.market || {});

  return {
    ...analysis,
    keepaFallbackUsed: analysis.fallbackUsed === true,
    aiNeeded: aiResolution?.needed === true,
    aiUsed: aiResolution?.used === true,
    aiEscalation: aiResolution?.status || analysis.aiStatus || 'not_needed',
    lowestPrice: analysis.marketPrice ?? marketComparison.lowestPrice ?? null,
    marketComparison,
    reasons: mapReasonMessages(analysis.reasonDetails),
    decisionReason: summarizeReasons(analysis.reasonDetails)
  };
}

function normalizeDealEngineInput(input = {}) {
  const source = typeof input.source === 'object' && input.source ? input.source : {};
  const deal = typeof input.deal === 'object' && input.deal ? input.deal : {};
  const market = typeof input.market === 'object' && input.market ? input.market : {};
  const keepa = typeof input.keepa === 'object' && input.keepa ? input.keepa : {};
  const ai = typeof input.ai === 'object' && input.ai ? input.ai : {};
  const amazonPrice = parseNumber(deal.amazonPrice ?? deal.price, null);
  const referencePrice = parseNumber(deal.referencePrice, null);
  const sellerIdentity = resolveSellerIdentity({
    sellerType: deal.sellerType || deal.sellerArea,
    sellerClass: deal.sellerClass,
    soldByAmazon: deal.soldByAmazon,
    shippedByAmazon: deal.shippedByAmazon
  });
  const normalizedSellerArea = normalizeSellerArea(sellerIdentity.sellerType || deal.sellerArea);
  const keepaPayload =
    keepa.payload && typeof keepa.payload === 'object'
      ? keepa.payload
      : keepa.keepaPayload && typeof keepa.keepaPayload === 'object'
        ? keepa.keepaPayload
        : {};
  const derivedKeepaMetrics = extractKeepaFallbackMetrics({
    title: cleanText(deal.title),
    sellerType: normalizedSellerArea,
    currentPrice: amazonPrice,
    referencePrice,
    keepaPayload
  });

  return {
    source: {
      name: cleanText(source.name) || 'Manueller Input',
      platform: cleanText(source.platform) || 'internal',
      type: cleanText(source.type || source.sourceType) || 'manual'
    },
    deal: {
      title: cleanText(deal.title),
      amazonUrl: cleanText(deal.amazonUrl || deal.url),
      amazonPrice,
      sellerArea: normalizedSellerArea,
      sellerType: sellerIdentity.sellerType,
      sellerClass: sellerIdentity.sellerClass,
      soldByAmazon: sellerIdentity.soldByAmazon,
      shippedByAmazon: sellerIdentity.shippedByAmazon,
      brand: cleanText(deal.brand),
      category: cleanText(deal.category),
      variantKey: cleanText(deal.variantKey || deal.variant),
      quantityKey: cleanText(deal.quantityKey || deal.quantity),
      isBrandProduct: parseBool(deal.isBrandProduct, false),
      isNoName: parseBool(deal.isNoName, false),
      isChinaProduct: parseBool(deal.isChinaProduct, false),
      referencePrice,
      highPriceNoHistory: parseBool(deal.highPriceNoHistory, false),
      suddenDropWithoutHistory: parseBool(deal.suddenDropWithoutHistory, false),
      fakeAnchorPrice: parseBool(deal.fakeAnchorPrice, false),
      illogicalChart: parseBool(deal.illogicalChart, false)
    },
    market: {
      offers: Array.isArray(market.offers) ? market.offers : []
    },
    keepa: {
      avg90: parseNumber(keepa.avg90 ?? keepa.average90, derivedKeepaMetrics.avg90),
      avg180: parseNumber(keepa.avg180 ?? keepa.average180, derivedKeepaMetrics.avg180),
      min90: parseNumber(keepa.min90 ?? keepa.lowest90, derivedKeepaMetrics.min90),
      isLowest90: parseBool(keepa.isLowest90, derivedKeepaMetrics.isLowest90),
      nearLow:
        parseBool(keepa.nearLow, false) ||
        (derivedKeepaMetrics.min90 !== null &&
          amazonPrice !== null &&
          amazonPrice <= derivedKeepaMetrics.min90 * 1.05),
      highPriceNoHistory: parseBool(keepa.highPriceNoHistory, false),
      suddenDropWithoutHistory: parseBool(keepa.suddenDropWithoutHistory, false),
      anchorPriceSuspect: parseBool(keepa.anchorPriceSuspect, false),
      fakeAnchorPrice: parseBool(keepa.fakeAnchorPrice, false),
      illogicalChart: parseBool(keepa.illogicalChart, false),
      payload: keepaPayload,
      historyPointCount: Number(derivedKeepaMetrics.historyPointCount || 0),
      historySource: derivedKeepaMetrics.historySource || 'empty'
    },
    ai: {
      variantUnclear: parseBool(ai.variantUnclear, false),
      multipleSimilarHits: parseBool(ai.multipleSimilarHits, false),
      conflictingPrices: parseBool(ai.conflictingPrices, false),
      mappingUncertain: parseBool(ai.mappingUncertain, false),
      resolvedOfferIds: Array.isArray(ai.resolvedOfferIds) ? ai.resolvedOfferIds : [],
      resolvedVariant: cleanText(ai.resolvedVariant),
      confidence: parseNumber(ai.confidence, null)
    },
    meta: {
      overrideDayPart: normalizeDayPart(input?.meta?.overrideDayPart)
    }
  };
}

function buildRejectAnalysis(
  normalized,
  settings,
  dayPart,
  reasons,
  decisionSource = 'validation',
  aiResolution = null,
  marketResult = {},
  sellerPolicy = null
) {
  const reasonDetails = normalizeReasonDetails(reasons, 'validation_reject', 'high');

  return attachAnalysisAliases(
    {
      title: normalized.deal.title,
      amazonUrl: normalized.deal.amazonUrl,
      asin: extractAsinFromAmazonUrl(normalized.deal.amazonUrl),
      sellerArea: normalized.deal.sellerArea,
      amazonPrice: normalized.deal.amazonPrice,
      dayPart,
      decision: 'REJECT',
      decisionSource,
      decisionSourceLabel: decisionSource === 'validation' ? 'Validierung' : 'Fake Pattern',
      thresholdPct: getRequiredMarketAdvantagePct(settings, normalized.deal.sellerArea, dayPart),
      marketPrice: null,
      marketAdvantagePct: null,
      marketOfferCount: 0,
      keepaScore: null,
      keepaDiscount90: null,
      keepaDiscount180: null,
      fallbackUsed: false,
      aiStatus: aiResolution?.status || 'not_needed',
      fakePatternStatus: decisionSource === 'fake_pattern' ? 'reject' : 'clear',
      seller: buildSellerAnalysis(sellerPolicy || evaluateSellerDecisionPolicy(settings, normalized.deal)),
      reasonDetails,
      flow: ['Deal kommt rein', 'Amazon-Link pruefen', 'Reject'],
      output: {
        status: 'not_sent',
        queueId: null,
        targetCount: 0
      }
    },
    marketResult,
    aiResolution
  );
}

function applyFakePatternDecision({ decision, settings, fakePatterns = {}, reasonDetails = [] }) {
  const nextReasonDetails = mergeReasonDetails(reasonDetails, fakePatterns.reasonDetails || []);
  let nextDecision = decision;
  let overrideSource = '';

  if (fakePatterns.reject) {
    nextDecision = 'REJECT';
    overrideSource = 'fake_pattern';
    nextReasonDetails.push(
      createReasonDetail('fake_pattern_reject', 'Fake-Pattern-Erkennung erzwingt REJECT.', 'critical', {
        classification: fakePatterns.engine?.classification || 'unknown',
        fakeDropRisk: fakePatterns.engine?.fakeDropRisk ?? null
      })
    );
  } else if (fakePatterns.forceQueue && decision === 'APPROVE') {
    nextDecision = settings.global.queueEnabled ? 'QUEUE' : 'REJECT';
    overrideSource = 'fake_pattern';
    nextReasonDetails.push(
      createReasonDetail(
        nextDecision === 'QUEUE' ? 'fake_pattern_queue' : 'fake_pattern_reject_without_queue',
        nextDecision === 'QUEUE'
          ? 'Unsicheres Fake-Pattern stuft den Deal intern auf QUEUE herunter.'
          : 'Unsicheres Fake-Pattern erzwingt REJECT, weil Queue deaktiviert ist.',
        'high',
        {
          classification: fakePatterns.engine?.classification || 'unknown',
          fakeDropRisk: fakePatterns.engine?.fakeDropRisk ?? null
        }
      )
    );
  }

  return {
    decision: nextDecision,
    reasonDetails: normalizeReasonDetails(nextReasonDetails),
    overrideSource
  };
}

function applyUnknownSellerPolicy({ analysis, settings, sellerPolicy }) {
  if (!analysis || sellerPolicy?.seller?.isUnknown !== true) {
    return analysis;
  }

  if (sellerPolicy.unknownSellerAction === 'block') {
    const reasonDetails = mergeReasonDetails(analysis.reasonDetails, [
      createReasonDetail('seller_unknown_block', 'Unbekannter Verkaeufer erzwingt REJECT.', 'critical')
    ]);
    logDecisionFlow('SELLER_UNKNOWN_REVIEW', {
      sellerClass: sellerPolicy.seller?.sellerClass || 'UNKNOWN',
      action: 'block'
    });

    return attachAnalysisAliases(
      {
        ...analysis,
        decision: 'REJECT',
        decisionOverrideSource: 'seller_policy',
        reasonDetails
      },
      analysis.market || {},
      {
        needed: analysis.aiNeeded === true,
        used: analysis.aiUsed === true,
        status: analysis.aiStatus || 'not_needed'
      }
    );
  }

  if (analysis.decision === 'APPROVE') {
    const reasonDetails = mergeReasonDetails(analysis.reasonDetails, [
      createReasonDetail(
        'seller_unknown_review',
        settings.global.queueEnabled
          ? 'Unbekannter Verkaeufer zwingt den Deal in REVIEW/QUEUE.'
          : 'Unbekannter Verkaeufer verhindert APPROVE ohne aktive Queue.',
        'warning'
      )
    ]);
    const downgradedDecision = settings.global.queueEnabled ? 'QUEUE' : 'REJECT';

    logDecisionFlow('SELLER_UNKNOWN_REVIEW', {
      sellerClass: sellerPolicy.seller?.sellerClass || 'UNKNOWN',
      downgradedDecision
    });

    return attachAnalysisAliases(
      {
        ...analysis,
        decision: downgradedDecision,
        decisionOverrideSource: 'seller_policy',
        reasonDetails
      },
      analysis.market || {},
      {
        needed: analysis.aiNeeded === true,
        used: analysis.aiUsed === true,
        status: analysis.aiStatus || 'not_needed'
      }
    );
  }

  logDecisionFlow('SELLER_UNKNOWN_REVIEW', {
    sellerClass: sellerPolicy.seller?.sellerClass || 'UNKNOWN',
    decision: analysis.decision
  });

  return analysis;
}

function finalizeMarketDecision({ normalized, settings, dayPart, marketResult, fakePatterns, aiResolution, sellerPolicy }) {
  const thresholdPct = getRequiredMarketAdvantagePct(settings, normalized.deal.sellerArea, dayPart);
  const reasonDetails = [
    createReasonDetail(
      'market_available',
      `Internetvergleich aktiv mit ${marketResult.validOfferCount} gueltigen Marktpreisen.`,
      'info'
    ),
    createReasonDetail(
      'market_threshold',
      `Marktschwelle ${thresholdPct}% fuer ${normalized.deal.sellerArea} im ${dayPart === 'night' ? 'Nacht' : 'Tag'}modus.`,
      'info'
    )
  ];
  let decision = 'REJECT';

  if (marketResult.marketAdvantagePct !== null && marketResult.marketAdvantagePct >= thresholdPct) {
    decision = 'APPROVE';
    reasonDetails.push(
      createReasonDetail(
        'market_approve',
        `Marktvorteil ${marketResult.marketAdvantagePct}% liegt ueber der Schwelle.`,
        'success'
      )
    );
  } else if (
    settings.global.queueEnabled &&
    marketResult.marketAdvantagePct !== null &&
    marketResult.marketAdvantagePct > 0 &&
    marketResult.marketAdvantagePct >= thresholdPct - settings.global.queueMarginPct
  ) {
    decision = 'QUEUE';
    reasonDetails.push(
      createReasonDetail(
        'market_queue',
        `Marktvorteil ${marketResult.marketAdvantagePct}% liegt knapp unter der Schwelle und geht intern in Queue.`,
        'warning'
      )
    );
  } else if (marketResult.marketAdvantagePct !== null && marketResult.marketAdvantagePct <= 0) {
    reasonDetails.push(
      createReasonDetail('market_reject_non_positive', 'Amazonpreis ist nicht besser als der gueltige Marktpreis.', 'high')
    );
  } else {
    reasonDetails.push(
      createReasonDetail('market_reject_threshold', 'Marktvorteil reicht fuer APPROVE oder QUEUE nicht aus.', 'high')
    );
  }

  const fakeDecision = applyFakePatternDecision({
    decision,
    settings,
    fakePatterns,
    reasonDetails
  });

  return attachAnalysisAliases(
    {
      title: normalized.deal.title,
      amazonUrl: normalized.deal.amazonUrl,
      asin: extractAsinFromAmazonUrl(normalized.deal.amazonUrl),
      sellerArea: normalized.deal.sellerArea,
      amazonPrice: normalized.deal.amazonPrice,
      dayPart,
      thresholdPct,
      decision: fakeDecision.decision,
      decisionSource: 'market',
      decisionSourceLabel: 'Internetvergleich',
      decisionOverrideSource: fakeDecision.overrideSource,
      marketPrice: marketResult.marketPrice,
      marketAdvantagePct: marketResult.marketAdvantagePct,
      marketOfferCount: marketResult.validOfferCount,
      keepaScore: null,
      keepaDiscount90: null,
      keepaDiscount180: null,
      fallbackUsed: false,
      aiStatus: aiResolution.status,
      fakePatternStatus: fakePatterns.status,
      seller: buildSellerAnalysis(sellerPolicy),
      reasonDetails: fakeDecision.reasonDetails,
      market: marketResult,
      fakePatterns,
      flow: [
        'Deal kommt rein',
        'Amazon-Link vorhanden',
        'Internetvergleich',
        'Markt entscheidet',
        'Seller pruefen',
        'Fake-Pattern pruefen',
        `Final: ${fakeDecision.decision}`
      ]
    },
    marketResult,
    aiResolution
  );
}

function finalizeKeepaDecision({ normalized, settings, dayPart, marketResult, fakePatterns, aiResolution, keepaResult, sellerPolicy }) {
  const reasonDetails = mergeReasonDetails(
    [
      createReasonDetail(
        'keepa_fallback',
        marketResult.blocked === true
          ? 'Marktvergleich ist fuer diesen Seller blockiert. Keepa Fallback greift.'
          : 'Kein brauchbarer Marktpreis vorhanden. Keepa Fallback greift.',
        'warning'
      ),
      marketResult.blocked === true && marketResult.blockedReason
        ? createReasonDetail('market_compare_blocked', marketResult.blockedReason, 'warning')
        : null
    ],
    keepaResult.reasons.map((message) => createReasonDetail('keepa_reason', message, 'info'))
  );
  const fakeDecision = applyFakePatternDecision({
    decision: keepaResult.decision,
    settings,
    fakePatterns,
    reasonDetails
  });

  return attachAnalysisAliases(
    {
      title: normalized.deal.title,
      amazonUrl: normalized.deal.amazonUrl,
      asin: extractAsinFromAmazonUrl(normalized.deal.amazonUrl),
      sellerArea: normalized.deal.sellerArea,
      amazonPrice: normalized.deal.amazonPrice,
      dayPart,
      thresholdPct: getRequiredMarketAdvantagePct(settings, normalized.deal.sellerArea, dayPart),
      decision: fakeDecision.decision,
      decisionSource: 'keepa',
      decisionSourceLabel: 'Keepa Fallback',
      decisionOverrideSource: fakeDecision.overrideSource,
      marketPrice: marketResult.marketPrice,
      marketAdvantagePct: marketResult.marketAdvantagePct,
      marketOfferCount: marketResult.validOfferCount,
      keepaScore: keepaResult.score,
      keepaDiscount90: keepaResult.discount90,
      keepaDiscount180: keepaResult.discount180,
      fallbackUsed: true,
      aiStatus: aiResolution.status,
      fakePatternStatus: fakePatterns.status,
      seller: buildSellerAnalysis(sellerPolicy),
      reasonDetails: fakeDecision.reasonDetails,
      market: marketResult,
      keepa: keepaResult,
      fakePatterns,
      flow: [
        'Deal kommt rein',
        'Amazon-Link vorhanden',
        'Internetvergleich',
        'Keine brauchbaren Marktpreise',
        'Keepa entscheidet',
        'Seller pruefen',
        'Fake-Pattern pruefen',
        `Final: ${fakeDecision.decision}`
      ]
    },
    marketResult,
    aiResolution
  );
}

export function getDealEngineSamplePayload() {
  return {
    source: {
      name: 'Demo Quelle',
      platform: 'telegram',
      type: 'manual'
    },
    deal: {
      title: 'Bosch Professional Akku-Schrauber 18V Solo',
      amazonUrl: 'https://www.amazon.de/dp/B0DDKZBYK6',
      amazonPrice: 79.99,
      sellerType: 'AMAZON',
      sellerClass: 'AMAZON_DIRECT',
      soldByAmazon: true,
      shippedByAmazon: true,
      brand: 'Bosch Professional',
      category: 'Werkzeug',
      variantKey: '18v solo',
      quantityKey: '1 stueck',
      isBrandProduct: true,
      isNoName: false,
      isChinaProduct: false
    },
    market: {
      offers: [
        {
          id: 'idealo-1',
          shopName: 'WerkzeugHub',
          price: 104.99,
          shippingPrice: 0,
          variantKey: '18v solo',
          quantityKey: '1 stueck',
          isRealShop: true
        },
        {
          id: 'idealo-2',
          shopName: 'ToolStar',
          price: 99.99,
          shippingPrice: 4.95,
          variantKey: '18v solo',
          quantityKey: '1 stueck',
          isRealShop: true
        },
        {
          id: 'bad-1',
          shopName: 'FlashMegaDiscount',
          price: 39.99,
          shippingPrice: 0,
          variantKey: '12v set',
          quantityKey: '1 stueck',
          isRealShop: false
        }
      ]
    },
    keepa: {
      avg90: 109.99,
      avg180: 114.99,
      min90: 78.99,
      isLowest90: false,
      nearLow: true
    },
    ai: {
      variantUnclear: false,
      multipleSimilarHits: false,
      conflictingPrices: false,
      mappingUncertain: false,
      resolvedOfferIds: []
    },
    meta: {
      overrideDayPart: 'day'
    }
  };
}

export async function analyzeDealWithEngine(input = {}) {
  const settings = getDealEngineSettings();
  const normalized = normalizeDealEngineInput(input);
  const dayPart = resolveDealEngineDayPart(settings, normalized.meta.overrideDayPart);
  const sellerPolicy = evaluateSellerDecisionPolicy(settings, normalized.deal);
  let analysis;
  let aiResolution = {
    needed: false,
    used: false,
    status: 'not_needed'
  };
  let marketResult = {
    available: false,
    validOfferCount: 0,
    invalidOfferCount: 0,
    marketPrice: null,
    marketAdvantagePct: null
  };

  logDecisionFlow('DECISION_FLOW_START', {
    title: normalized.deal.title || null,
    asin: extractAsinFromAmazonUrl(normalized.deal.amazonUrl),
    sellerType: sellerPolicy.seller?.sellerType || 'UNKNOWN',
    sellerClass: sellerPolicy.seller?.sellerClass || 'UNKNOWN'
  });
  logDecisionFlow('SELLER_DETAILS', {
    sellerType: sellerPolicy.seller?.sellerType || 'UNKNOWN',
    sellerClass: sellerPolicy.seller?.sellerClass || 'UNKNOWN',
    soldByAmazon: sellerPolicy.seller?.soldByAmazon,
    shippedByAmazon: sellerPolicy.seller?.shippedByAmazon,
    soldByAmazonLabel: sellerPolicy.seller?.details?.soldByAmazonLabel || 'unbekannt',
    shippedByAmazonLabel: sellerPolicy.seller?.details?.shippedByAmazonLabel || 'unbekannt',
    detectionSource: sellerPolicy.seller?.details?.detectionSource || 'unknown'
  });
  logDecisionFlow('SELLER_TYPE_DETECTED', {
    sellerType: sellerPolicy.seller?.sellerType || 'UNKNOWN'
  });
  logDecisionFlow('SELLER_CLASS_DETECTED', {
    sellerClass: sellerPolicy.seller?.sellerClass || 'UNKNOWN'
  });

  if (sellerPolicy.seller?.isAmazonDirect === true) {
    logDecisionFlow('AMAZON_DIRECT_CONFIRMED', {
      sellerClass: sellerPolicy.seller?.sellerClass || 'AMAZON_DIRECT'
    });
  }

  if (!normalized.deal.amazonUrl || !isAmazonLink(normalized.deal.amazonUrl)) {
    analysis = buildRejectAnalysis(
      normalized,
      settings,
      dayPart,
      [createReasonDetail('invalid_amazon_link', 'Kein gueltiger Amazon-Link vorhanden.', 'critical')],
      'validation',
      aiResolution,
      marketResult,
      sellerPolicy
    );
  } else if (normalized.deal.amazonPrice === null || normalized.deal.amazonPrice <= 0) {
    analysis = buildRejectAnalysis(
      normalized,
      settings,
      dayPart,
      [createReasonDetail('invalid_amazon_price', 'Amazonpreis fehlt oder ist ungueltig.', 'critical')],
      'validation',
      aiResolution,
      marketResult,
      sellerPolicy
    );
  } else {
    const marketComparisonAllowed = sellerPolicy.marketComparison?.allowed === true;
    let preliminaryMarketResult = {
      status: 'market_missing',
      available: false,
      blocked: false,
      blockedReason: '',
      validOfferCount: 0,
      invalidOfferCount: 0,
      marketPrice: null,
      marketAdvantagePct: null
    };

    logDecisionFlow('MARKET_COMPARE_TRIGGER', {
      sellerClass: sellerPolicy.seller?.sellerClass || 'UNKNOWN',
      allowed: marketComparisonAllowed,
      reason: sellerPolicy.marketComparison?.reason || ''
    });

    if (marketComparisonAllowed) {
      logDecisionFlow('MARKET_COMPARE_ALLOWED', {
        sellerClass: sellerPolicy.seller?.sellerClass || 'UNKNOWN',
        reason: sellerPolicy.marketComparison?.reason || ''
      });
      preliminaryMarketResult = evaluateMarketComparison({
        deal: normalized.deal,
        market: normalized.market,
        aiResolution: { resolvedOfferIds: [] }
      });
    } else {
      marketResult = {
        status: 'blocked_by_seller_policy',
        available: false,
        blocked: true,
        blockedReason: sellerPolicy.marketComparison?.reason || 'Marktvergleich blockiert.',
        validOfferCount: 0,
        invalidOfferCount: 0,
        marketPrice: null,
        marketAdvantagePct: null,
        contradictoryPrices: false,
        uncertaintyHints: {
          multipleSimilarHits: false,
          contradictoryPrices: false,
          invalidVariantCount: 0
        }
      };
      preliminaryMarketResult = marketResult;
      logDecisionFlow('MARKET_COMPARE_BLOCKED', {
        sellerClass: sellerPolicy.seller?.sellerClass || 'UNKNOWN',
        reason: sellerPolicy.marketComparison?.reason || ''
      });
    }

    if (marketComparisonAllowed) {
      if (sellerPolicy.ai?.allowed === true) {
        logDecisionFlow('AI_ALLOWED', {
          sellerClass: sellerPolicy.seller?.sellerClass || 'UNKNOWN',
          reason: sellerPolicy.ai?.reason || ''
        });
        aiResolution = resolveAiAssistance({
          ai: normalized.ai,
          market: preliminaryMarketResult,
          settings
        });
      } else {
        aiResolution = {
          needed: false,
          used: false,
          status: 'blocked_by_seller_policy',
          reason: sellerPolicy.ai?.reason || 'KI blockiert.'
        };
        logDecisionFlow('AI_BLOCKED', {
          sellerClass: sellerPolicy.seller?.sellerClass || 'UNKNOWN',
          reason: sellerPolicy.ai?.reason || ''
        });
      }
    } else {
      aiResolution = {
        needed: false,
        used: false,
        status: 'blocked_by_seller_policy',
        reason: 'KI blockiert, weil Marktvergleich fuer diesen Seller nicht laufen darf.'
      };
      logDecisionFlow('AI_BLOCKED', {
        sellerClass: sellerPolicy.seller?.sellerClass || 'UNKNOWN',
        reason: aiResolution.reason
      });
    }

    logDecisionFlow('AI_TRIGGER_REASON', {
      sellerClass: sellerPolicy.seller?.sellerClass || 'UNKNOWN',
      needed: aiResolution.needed === true,
      used: aiResolution.used === true,
      status: aiResolution.status,
      reason: aiResolution.reason || ''
    });

    marketResult =
      marketComparisonAllowed && (aiResolution.used || aiResolution.needed)
        ? evaluateMarketComparison({
            deal: normalized.deal,
            market: normalized.market,
            aiResolution
          })
        : preliminaryMarketResult;
    const fakePatterns = evaluateFakePatterns({
      deal: normalized.deal,
      keepa: normalized.keepa,
      market: marketResult
    });

    if (marketResult.available) {
      analysis = finalizeMarketDecision({
        normalized,
        settings,
        dayPart,
        marketResult,
        fakePatterns,
        aiResolution,
        sellerPolicy
      });
    } else {
      const strictMarketRequired =
        marketComparisonAllowed &&
        ((settings.global.requireMarketForCheapProducts &&
          normalized.deal.amazonPrice < settings.global.cheapProductLimit) ||
          (settings.global.requireMarketForNoNameProducts && (normalized.deal.isNoName || normalized.deal.isChinaProduct)));

      if (strictMarketRequired) {
        const baseReason =
          normalized.deal.amazonPrice < settings.global.cheapProductLimit
            ? createReasonDetail(
                'market_required_cheap_product',
                'Billige Produkte benoetigen zwingend einen brauchbaren Internetvergleich.',
                'critical'
              )
            : createReasonDetail(
                'market_required_no_name',
                'No-Name- oder China-Produkte benoetigen zwingend einen brauchbaren Internetvergleich.',
                'critical'
              );
        analysis = buildRejectAnalysis(normalized, settings, dayPart, [baseReason], 'validation', aiResolution, marketResult, sellerPolicy);
        analysis.fakePatternStatus = fakePatterns.status;
        analysis.market = marketResult;
        analysis.fakePatterns = fakePatterns;
        analysis.reasonDetails = mergeReasonDetails(analysis.reasonDetails, fakePatterns.reasonDetails || []);
        analysis = attachAnalysisAliases(analysis, marketResult, aiResolution);
      } else {
        logDecisionFlow('KEEPA_FALLBACK_TRIGGER', {
          sellerClass: sellerPolicy.seller?.sellerClass || 'UNKNOWN',
          reason:
            marketResult.blocked === true
              ? marketResult.blockedReason || 'Marktvergleich blockiert.'
              : 'Kein brauchbarer Marktpreis vorhanden.'
        });
        const keepaResult = evaluateKeepaFallback({
          deal: normalized.deal,
          keepa: normalized.keepa,
          sellerArea: normalized.deal.sellerArea,
          fakePatterns,
          settings
        });
        analysis = finalizeKeepaDecision({
          normalized,
          settings,
          dayPart,
          marketResult,
          fakePatterns,
          aiResolution,
          keepaResult,
          sellerPolicy
        });
      }
    }
  }

  analysis = applyUnknownSellerPolicy({
    analysis,
    settings,
    sellerPolicy
  });

  let output;
  try {
    output = enqueueApprovedDeal(analysis, settings);
  } catch (error) {
    output = {
      status: 'output_error',
      queueId: null,
      targetCount: 0,
      reason: error instanceof Error ? error.message : 'Output konnte nicht uebergeben werden.'
    };
    analysis.reasonDetails = mergeReasonDetails(
      analysis.reasonDetails,
      [createReasonDetail('output_error', output.reason, 'high')]
    );
    analysis = attachAnalysisAliases(analysis, marketResult, aiResolution);
  }
  analysis.output = output;
  analysis.marketAdvantagePct =
    analysis.marketAdvantagePct === null || analysis.marketAdvantagePct === undefined
      ? null
      : round(analysis.marketAdvantagePct, 2);
  analysis.marketComparison = buildMarketComparisonAlias(analysis.market || marketResult);
  analysis.lowestPrice = analysis.marketPrice ?? analysis.marketComparison.lowestPrice ?? null;

  const run = createDealEngineRun({
    source: normalized.source,
    amazonUrl: analysis.amazonUrl,
    asin: analysis.asin,
    title: analysis.title,
    sellerArea: analysis.sellerArea,
    amazonPrice: analysis.amazonPrice,
    marketPrice: analysis.marketPrice,
    lowestPrice: analysis.lowestPrice,
    marketAdvantagePct: analysis.marketAdvantagePct,
    marketOfferCount: analysis.marketOfferCount,
    keepaScore: analysis.keepaScore,
    keepaDiscount90: analysis.keepaDiscount90,
    keepaDiscount180: analysis.keepaDiscount180,
    fallbackUsed: analysis.fallbackUsed,
    keepaFallbackUsed: analysis.keepaFallbackUsed,
    aiStatus: analysis.aiStatus,
    aiNeeded: analysis.aiNeeded,
    aiUsed: analysis.aiUsed,
    aiEscalation: analysis.aiEscalation,
    fakePatternStatus: analysis.fakePatternStatus,
    dayPart: analysis.dayPart,
    decision: analysis.decision,
    decisionReason: analysis.decisionReason,
    marketComparison: analysis.marketComparison,
    reasonDetails: analysis.reasonDetails,
    outputStatus: output.status,
    outputQueueId: output.queueId,
    outputTargetCount: output.targetCount,
    payload: input,
    analysis
  });

  return {
    settings,
    item: run
  };
}
