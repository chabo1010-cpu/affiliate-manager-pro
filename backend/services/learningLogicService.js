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

function nowIso() {
  return new Date().toISOString();
}

function buildKeepaPreview(context = {}) {
  const result = context.result || null;
  if (!result) {
    return {
      available: false,
      status: context.status || 'missing',
      reason: context.reason || 'Keepa-Daten fehlen.',
      requestedAt: context.requestedAt || null
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
    sellerType: result.sellerType || null,
    categoryName: result.categoryName || '',
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

function getSourceLabel(sourceType) {
  if (sourceType === 'generator') {
    return 'Generator';
  }

  if (sourceType === 'scrapper') {
    return 'Scrapper';
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

function buildRoutingDecision(input = {}, keepaPreview, evaluation) {
  if (evaluation.decision === 'hold') {
    return {
      decision: 'block',
      reason: evaluation.reasons.join(' | ')
    };
  }

  if (!keepaPreview.available && (input.enforceDecision || input.keepaRequired)) {
    return {
      decision: 'review',
      reason: keepaPreview.reason || 'Keepa-Pruefung fehlt.'
    };
  }

  if (evaluation.testGroupApproved) {
    return {
      decision: 'test_group',
      reason: evaluation.reasons.join(' | ')
    };
  }

  return {
    decision: 'review',
    reason: evaluation.reasons.join(' | ')
  };
}

export function evaluateLearningRoute(input = {}) {
  const sellerType = normalizeSellerType(input.sellerType);
  const keepaPreview = normalizeKeepaContext(input);
  const amazonPreview = normalizeAmazonContext(input);
  const patternSupportEnabled = input.patternSupportEnabled !== false;

  logGeneratorDebug('LEARNING LOGIC LOADED', {
    sourceType: input.sourceType || 'generator',
    sellerType,
    enforceDecision: input.enforceDecision === true,
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
  const routing = buildRoutingDecision(input, keepaPreview, evaluation);
  const sourceLabel = getSourceLabel(input.sourceType || 'generator');
  const learning = {
    sourceType: input.sourceType || 'generator',
    sourceLabel,
    enforced: input.enforceDecision === true,
    keepaRequired: input.keepaRequired === true,
    routingDecision: routing.decision,
    routingLabel: getRoutingLabel(routing.decision),
    canReachTestGroup: routing.decision === 'test_group',
    shouldReview: routing.decision === 'review',
    blocked: routing.decision === 'block',
    patternSupportEnabled,
    reason: routing.reason,
    workflow:
      routing.decision === 'test_group'
        ? 'Quelle -> Lern-Logik -> Testgruppe'
        : routing.decision === 'block'
          ? 'Quelle -> Lern-Logik -> Block'
          : 'Quelle -> Lern-Logik -> Review'
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
    routingDecision: routing.decision
  });

  logGeneratorDebug('DEAL DECISION RESULT', {
    sourceType: input.sourceType || 'generator',
    sellerType,
    routingDecision: learning.routingDecision,
    finalScore: evaluation.metrics.finalScore,
    keepaStatus: keepaPreview.status
  });

  return {
    asin: cleanText(input.asin).toUpperCase(),
    sellerType,
    currentPrice: parseNumber(input.currentPrice, null),
    keepa: keepaPreview,
    amazon: amazonPreview,
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
