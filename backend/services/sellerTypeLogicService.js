import { getDb } from '../db.js';
import { normalizeSellerType } from './dealHistoryService.js';

const db = getDb();

const POSITIVE_LABELS = new Set(['approved', 'strong_deal', 'ja']);
const NEGATIVE_LABELS = new Set(['rejected', 'fake_drop', 'weak_deal', 'nein']);
const UNCERTAIN_LABELS = new Set(['eventuell_gut', 'ueberspringen']);
const POSITIVE_TAGS = new Set(['amazon_ok', 'fba_ok', 'testgruppe_freigabe']);
const NEGATIVE_TAGS = new Set(['fake_drop', 'fbm_bad', 'coupon_verdacht', 'fba_fbm_trick']);
const KNOWN_CLASSIFICATIONS = new Set([
  'echter_deal',
  'verdaechtig',
  'wahrscheinlicher_fake_drop',
  'manuelle_pruefung',
  'amazon_stabil'
]);

export const SELLER_TYPE_LOGIC = {
  AMAZON: {
    id: 'AMAZON',
    minDiscount: 12,
    minScore: 58,
    maxFakeDropRisk: 72,
    keepaRating: 'vertrauenswuerdig',
    allowTestGroup: true,
    learningLabels: ['approved', 'strong_deal', 'rejected', 'fake_drop', 'weak_deal'],
    learningTags: ['amazon_ok', 'testgruppe_freigabe', 'echter_deal']
  },
  FBA: {
    id: 'FBA',
    minDiscount: 18,
    minScore: 66,
    maxFakeDropRisk: 58,
    keepaRating: 'mittel',
    allowTestGroup: true,
    learningLabels: ['approved', 'strong_deal', 'rejected', 'fake_drop', 'weak_deal'],
    learningTags: ['fba_ok', 'testgruppe_freigabe', 'echter_deal', 'fba_fbm_trick']
  },
  FBM: {
    id: 'FBM',
    minDiscount: 24,
    minScore: 74,
    maxFakeDropRisk: 44,
    keepaRating: 'streng',
    allowTestGroup: true,
    learningLabels: ['approved', 'strong_deal', 'rejected', 'fake_drop', 'weak_deal'],
    learningTags: ['fbm_bad', 'fake_drop', 'coupon_verdacht', 'fba_fbm_trick']
  }
};

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

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function fromJson(value, fallback) {
  try {
    if (!value) {
      return fallback;
    }

    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function normalizeClassification(value) {
  const normalized = cleanText(String(value || '')).toLowerCase();
  return KNOWN_CLASSIFICATIONS.has(normalized) ? normalized : 'manuelle_pruefung';
}

function countTags(rows = []) {
  const tagCounts = {};

  rows.forEach((row) => {
    const tags = fromJson(row.tags_json, []);
    if (!Array.isArray(tags)) {
      return;
    }

    tags.forEach((tag) => {
      const normalizedTag = cleanText(String(tag || '')).toLowerCase();
      if (!normalizedTag) {
        return;
      }

      tagCounts[normalizedTag] = (tagCounts[normalizedTag] || 0) + 1;
    });
  });

  return tagCounts;
}

function buildUniqueList(values = []) {
  return [...new Set(values.filter(Boolean))];
}

function getDecisionLabel(decision) {
  if (decision === 'ready_for_test_group') {
    return 'Bereit fuer Testgruppe';
  }

  if (decision === 'hold') {
    return 'Vorlaeufig halten';
  }

  return 'Manuelle Pruefung';
}

export function getSellerTypeLogicConfig(value) {
  const sellerType = normalizeSellerType(value);
  return SELLER_TYPE_LOGIC[sellerType] || SELLER_TYPE_LOGIC.FBM;
}

export function getSellerTypeFeedbackSummary(inputSellerType = 'FBM') {
  const sellerType = normalizeSellerType(inputSellerType);
  const rows = db
    .prepare(
      `
        SELECT label, tags_json
        FROM keepa_review_labels
        WHERE seller_type = ?
        ORDER BY created_at DESC
        LIMIT 240
      `
    )
    .all(sellerType);

  const labelCounts = rows.reduce((accumulator, row) => {
    const label = cleanText(String(row.label || '')).toLowerCase();
    if (!label) {
      return accumulator;
    }

    accumulator[label] = (accumulator[label] || 0) + 1;
    return accumulator;
  }, {});
  const tagCounts = countTags(rows);
  const positiveCount =
    rows.filter((row) => POSITIVE_LABELS.has(cleanText(String(row.label || '')).toLowerCase())).length +
    Object.entries(tagCounts)
      .filter(([tag]) => POSITIVE_TAGS.has(tag))
      .reduce((sum, [, count]) => sum + count, 0);
  const negativeCount =
    rows.filter((row) => NEGATIVE_LABELS.has(cleanText(String(row.label || '')).toLowerCase())).length +
    Object.entries(tagCounts)
      .filter(([tag]) => NEGATIVE_TAGS.has(tag))
      .reduce((sum, [, count]) => sum + count, 0);
  const uncertainCount = rows.filter((row) => UNCERTAIN_LABELS.has(cleanText(String(row.label || '')).toLowerCase())).length;
  const total = positiveCount + negativeCount + uncertainCount;
  const rawRiskAdjustment = total >= 3 ? ((negativeCount - positiveCount) / total) * 10 : 0;
  const riskAdjustment = Math.round(clamp(rawRiskAdjustment, -8, 8) * 10) / 10;
  const scoreAdjustment = Math.round(clamp(-riskAdjustment * 0.8, -10, 10) * 10) / 10;

  return {
    sellerType,
    total,
    positiveCount,
    negativeCount,
    uncertainCount,
    riskAdjustment,
    scoreAdjustment,
    labelCounts,
    tagCounts
  };
}

export function evaluateSellerTypeDeal(input = {}) {
  const sellerType = normalizeSellerType(input.sellerType);
  const config = getSellerTypeLogicConfig(sellerType);
  const feedbackSummary =
    input.feedbackSummary?.sellerType === sellerType ? input.feedbackSummary : getSellerTypeFeedbackSummary(sellerType);
  const similarCaseSummary =
    input.similarCaseSummary && typeof input.similarCaseSummary === 'object'
      ? {
          total: parseNumber(input.similarCaseSummary.total, 0) ?? 0,
          positiveCount: parseNumber(input.similarCaseSummary.positiveCount, 0) ?? 0,
          negativeCount: parseNumber(input.similarCaseSummary.negativeCount, 0) ?? 0,
          uncertainCount: parseNumber(input.similarCaseSummary.uncertainCount, 0) ?? 0,
          dominantLabel: cleanText(input.similarCaseSummary.dominantLabel),
          riskAdjustment: parseNumber(input.similarCaseSummary.riskAdjustment, 0) ?? 0,
          scoreAdjustment: parseNumber(input.similarCaseSummary.scoreAdjustment, 0) ?? 0
        }
      : {
          total: 0,
          positiveCount: 0,
          negativeCount: 0,
          uncertainCount: 0,
          dominantLabel: '',
          riskAdjustment: 0,
          scoreAdjustment: 0
        };
  const keepaAvailable = input.keepaAvailable === true;
  const keepaDiscount = parseNumber(input.keepaDiscount, 0) ?? 0;
  const keepaDealScore = parseNumber(input.keepaDealScore ?? input.dealScore, 0) ?? 0;
  const fakeDropRiskRaw = parseNumber(input.fakeDropRisk, null);
  const combinedRiskAdjustment = feedbackSummary.riskAdjustment + similarCaseSummary.riskAdjustment;
  const combinedScoreAdjustment = feedbackSummary.scoreAdjustment + similarCaseSummary.scoreAdjustment;
  const fakeDropRisk =
    fakeDropRiskRaw === null ? null : Math.round(clamp(fakeDropRiskRaw + combinedRiskAdjustment, 0, 100) * 10) / 10;
  const fakeDropClassification = normalizeClassification(input.fakeDropClassification);
  const riskPenalty = fakeDropRisk === null ? 0 : Math.max(0, fakeDropRisk - config.maxFakeDropRisk) * 0.7;
  const finalScore = clamp(Math.round(keepaDealScore + combinedScoreAdjustment - riskPenalty), 0, 100);
  const checks = {
    keepaAvailable,
    minDiscountPassed: keepaDiscount >= config.minDiscount,
    minScorePassed: finalScore >= config.minScore,
    fakeDropPassed: fakeDropRisk === null ? false : fakeDropRisk <= config.maxFakeDropRisk,
    classificationPassed: !['wahrscheinlicher_fake_drop'].includes(fakeDropClassification)
  };
  const reasons = [];

  if (!keepaAvailable) {
    reasons.push('Kein Keepa-Kontext verfuegbar');
  } else {
    reasons.push(`Keepa Rabatt ${keepaDiscount.toFixed(1)}%`);
    reasons.push(`Seller-Type Score ${finalScore}/${config.minScore}`);
  }

  reasons.push(`Seller-Typ Logik ${sellerType}`);
  reasons.push(`Keepa Bewertung ${config.keepaRating}`);

  if (fakeDropRisk !== null) {
    reasons.push(`Fake-Drop Risiko ${fakeDropRisk.toFixed(1)}/${config.maxFakeDropRisk}`);
  } else {
    reasons.push('Fake-Drop Risiko noch offen');
  }

  if (feedbackSummary.total >= 3) {
    reasons.push(`Feedback Anpassung Score ${feedbackSummary.scoreAdjustment >= 0 ? '+' : ''}${feedbackSummary.scoreAdjustment}`);
  }

  if (similarCaseSummary.total >= 2) {
    reasons.push(
      `Aehnliche Faelle ${similarCaseSummary.total} (${similarCaseSummary.positiveCount} good / ${similarCaseSummary.negativeCount} kritisch)`
    );
    reasons.push(`Case-Abgleich Score ${similarCaseSummary.scoreAdjustment >= 0 ? '+' : ''}${similarCaseSummary.scoreAdjustment}`);
  }

  let decision = 'manual_review';
  if (
    keepaAvailable &&
    checks.minDiscountPassed &&
    checks.minScorePassed &&
    checks.fakeDropPassed &&
    checks.classificationPassed &&
    config.allowTestGroup
  ) {
    decision = 'ready_for_test_group';
  } else if (
    keepaAvailable &&
    (fakeDropClassification === 'wahrscheinlicher_fake_drop' || (fakeDropRisk !== null && fakeDropRisk >= config.maxFakeDropRisk + 18))
  ) {
    decision = 'hold';
  }

  const automationReady = decision === 'ready_for_test_group' && finalScore >= Math.min(100, config.minScore + 8);
  const recommendedLabel =
    decision === 'ready_for_test_group'
      ? finalScore >= Math.min(100, config.minScore + 10) || keepaDiscount >= config.minDiscount + 8
        ? 'strong_deal'
        : 'approved'
      : decision === 'hold'
        ? fakeDropClassification === 'wahrscheinlicher_fake_drop'
          ? 'fake_drop'
          : 'rejected'
        : keepaAvailable && !checks.minScorePassed
          ? 'weak_deal'
          : 'eventuell_gut';
  const recommendedTags = buildUniqueList([
    ...config.learningTags,
    decision === 'ready_for_test_group' && sellerType === 'AMAZON' ? 'amazon_ok' : '',
    decision === 'ready_for_test_group' && sellerType === 'FBA' ? 'fba_ok' : '',
    decision !== 'ready_for_test_group' && sellerType === 'FBM' ? 'fbm_bad' : '',
    fakeDropClassification === 'wahrscheinlicher_fake_drop' ? 'fake_drop' : '',
    decision === 'ready_for_test_group' ? 'testgruppe_freigabe' : ''
  ]);

  return {
    sellerType,
    decision,
    decisionLabel: getDecisionLabel(decision),
    testGroupApproved: decision === 'ready_for_test_group',
    automationReady,
    recommendedLabel,
    recommendedTags,
    keepaRating: config.keepaRating,
    config: {
      minDiscount: config.minDiscount,
      minScore: config.minScore,
      maxFakeDropRisk: config.maxFakeDropRisk,
      allowTestGroup: config.allowTestGroup,
      learningLabels: config.learningLabels
    },
    metrics: {
      keepaDiscount,
      keepaDealScore,
      fakeDropRisk,
      fakeDropClassification,
      finalScore,
      feedbackScoreAdjustment: feedbackSummary.scoreAdjustment,
      similarCaseScoreAdjustment: similarCaseSummary.scoreAdjustment,
      combinedScoreAdjustment,
      combinedRiskAdjustment
    },
    feedbackSummary,
    similarCaseSummary,
    checks,
    reasons
  };
}
