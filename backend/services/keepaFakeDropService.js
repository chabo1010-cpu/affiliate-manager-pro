import { getDb } from '../db.js';
import { logGeneratorDebug } from './generatorFlowService.js';

const db = getDb();

const ENGINE_VERSION = 'keepa-fake-drop-v1';
const KEEPA_EPOCH_MS = Date.UTC(2011, 0, 1, 0, 0, 0, 0);
const REVIEW_LABEL_CATALOG = [
  { id: 'approved', label: 'Good' },
  { id: 'strong_deal', label: 'Strong Deal' },
  { id: 'rejected', label: 'Rejected' },
  { id: 'fake_drop', label: 'Fake' },
  { id: 'weak_deal', label: 'Weak' },
  { id: 'eventuell_gut', label: 'Review' },
  { id: 'ueberspringen', label: 'Skip' },
  { id: 'ja', label: 'Ja (Legacy)' },
  { id: 'nein', label: 'Nein (Legacy)' }
];
const REVIEW_LABEL_ALIASES = {
  good: 'approved',
  fake: 'fake_drop',
  weak: 'weak_deal',
  review: 'eventuell_gut'
};
const REVIEW_TAG_CATALOG = [
  { id: 'echter_deal', label: 'echter Deal' },
  { id: 'fake_drop', label: 'Fake-Drop' },
  { id: 'strong_deal', label: 'Strong Deal' },
  { id: 'weak_deal', label: 'Weak Deal' },
  { id: 'amazon_ok', label: 'Amazon ok' },
  { id: 'fba_ok', label: 'FBA ok' },
  { id: 'fbm_bad', label: 'FBM schlecht' },
  { id: 'testgruppe_freigabe', label: 'Testgruppe freigegeben' },
  { id: 'coupon_verdacht', label: 'Coupon-Verdacht' },
  { id: 'fba_fbm_trick', label: 'FBA/FBM-Trick' },
  { id: 'amazon_sauber', label: 'Amazon sauber' },
  { id: 'unsicher', label: 'unsicher' }
];
const CLASSIFICATION_LABELS = {
  echter_deal: 'ECHTER DEAL',
  verdaechtig: 'VERDAECHTIG',
  wahrscheinlicher_fake_drop: 'WAHRSCHEINLICHER FAKE-DROP',
  manuelle_pruefung: 'MANUELLE PRUEFUNG',
  amazon_stabil: 'AMAZON-STABIL / STARKER PREISVERFALL'
};
const EXAMPLE_BUCKET_LABELS = {
  positive: 'Positive Beispiele',
  negative: 'Negative Beispiele',
  unsicher: 'Unsichere Beispiele'
};
const DEFAULT_WEIGHTS = {
  stability: 1,
  manipulation: 1,
  amazon: 1,
  feedback: 1
};
const DEFAULT_SETTINGS = {
  engineEnabled: true,
  lowRiskThreshold: 32,
  highRiskThreshold: 72,
  reviewPriorityThreshold: 58,
  amazonConfidenceStrong: 72,
  stabilityStrong: 66,
  referenceInflationThreshold: 22,
  volatilityWarningThreshold: 18,
  shortPeakMaxDays: 3,
  spikeSensitivity: 16,
  reboundWindowDays: 7,
  weights: DEFAULT_WEIGHTS,
  engineVersion: ENGINE_VERSION
};

function cleanText(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function parseBool(value, fallback = false) {
  if (value === true || value === false) {
    return value;
  }

  if (value === 1 || value === '1' || value === 'true') {
    return true;
  }

  if (value === 0 || value === '0' || value === 'false') {
    return false;
  }

  return fallback;
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

function parseInteger(value, fallback = 0) {
  if (value === null || value === undefined || value === '') {
    return fallback;
  }

  const parsed = Number.parseInt(String(value), 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function toJson(value) {
  return JSON.stringify(value ?? null);
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

function nowIso() {
  return new Date().toISOString();
}

function normalizeSellerType(value) {
  const normalized = cleanText(String(value || '')).toUpperCase();
  return ['AMAZON', 'FBA', 'FBM'].includes(normalized) ? normalized : 'UNKNOWN';
}

function normalizeClassification(value) {
  const normalized = cleanText(String(value || '')).toLowerCase();
  return Object.prototype.hasOwnProperty.call(CLASSIFICATION_LABELS, normalized) ? normalized : 'manuelle_pruefung';
}

function normalizeReviewLabel(value) {
  const normalized = cleanText(String(value || '')).toLowerCase();
  const mapped = REVIEW_LABEL_ALIASES[normalized] || normalized;
  return REVIEW_LABEL_CATALOG.some((item) => item.id === mapped) ? mapped : 'ueberspringen';
}

function normalizeReviewStatus(value) {
  const normalized = cleanText(String(value || '')).toLowerCase();
  return ['open', 'reviewed', 'skipped', 'closed'].includes(normalized) ? normalized : 'open';
}

function normalizeTags(values) {
  const sourceValues = Array.isArray(values) ? values : [];
  return [...new Set(sourceValues.map((item) => cleanText(item).toLowerCase()))].filter((item) =>
    REVIEW_TAG_CATALOG.some((tag) => tag.id === item)
  );
}

function normalizeLearningSource(value) {
  const normalized = cleanText(String(value || '')).toLowerCase();

  if (!normalized) {
    return 'keepa';
  }

  if (normalized.includes('generator')) {
    return 'generator';
  }

  if (normalized.includes('scrapp') || normalized.includes('copybot') || normalized.includes('import')) {
    return 'scrapper';
  }

  if (normalized.includes('amazon')) {
    return 'amazon';
  }

  return 'keepa';
}

function getLearningSourceLabel(value) {
  const sourceType = normalizeLearningSource(value);

  if (sourceType === 'generator') {
    return 'Generator';
  }

  if (sourceType === 'scrapper') {
    return 'Scrapper';
  }

  if (sourceType === 'amazon') {
    return 'Amazon API';
  }

  return 'Keepa';
}

function normalizeWeights(input) {
  const source = input && typeof input === 'object' ? input : {};
  return {
    stability: clamp(parseNumber(source.stability, DEFAULT_WEIGHTS.stability), 0.4, 2.5),
    manipulation: clamp(parseNumber(source.manipulation, DEFAULT_WEIGHTS.manipulation), 0.4, 2.5),
    amazon: clamp(parseNumber(source.amazon, DEFAULT_WEIGHTS.amazon), 0.4, 2.5),
    feedback: clamp(parseNumber(source.feedback, DEFAULT_WEIGHTS.feedback), 0.2, 2.5)
  };
}

function getSettingsRow() {
  return (
    db.prepare(`SELECT * FROM keepa_fake_drop_settings WHERE id = 1`).get() || {
      id: 1
    }
  );
}

function normalizeSettingsRow(row) {
  return {
    engineEnabled: parseBool(row.engine_enabled, DEFAULT_SETTINGS.engineEnabled),
    lowRiskThreshold: clamp(parseNumber(row.low_risk_threshold, DEFAULT_SETTINGS.lowRiskThreshold), 0, 100),
    highRiskThreshold: clamp(parseNumber(row.high_risk_threshold, DEFAULT_SETTINGS.highRiskThreshold), 0, 100),
    reviewPriorityThreshold: clamp(
      parseNumber(row.review_priority_threshold, DEFAULT_SETTINGS.reviewPriorityThreshold),
      0,
      100
    ),
    amazonConfidenceStrong: clamp(
      parseNumber(row.amazon_confidence_strong, DEFAULT_SETTINGS.amazonConfidenceStrong),
      0,
      100
    ),
    stabilityStrong: clamp(parseNumber(row.stability_strong, DEFAULT_SETTINGS.stabilityStrong), 0, 100),
    referenceInflationThreshold: clamp(
      parseNumber(row.reference_inflation_threshold, DEFAULT_SETTINGS.referenceInflationThreshold),
      0,
      200
    ),
    volatilityWarningThreshold: clamp(
      parseNumber(row.volatility_warning_threshold, DEFAULT_SETTINGS.volatilityWarningThreshold),
      0,
      200
    ),
    shortPeakMaxDays: clamp(parseNumber(row.short_peak_max_days, DEFAULT_SETTINGS.shortPeakMaxDays), 1, 14),
    spikeSensitivity: clamp(parseNumber(row.spike_sensitivity, DEFAULT_SETTINGS.spikeSensitivity), 4, 60),
    reboundWindowDays: clamp(parseNumber(row.rebound_window_days, DEFAULT_SETTINGS.reboundWindowDays), 1, 21),
    weights: normalizeWeights(fromJson(row.weights_json, DEFAULT_WEIGHTS)),
    engineVersion: cleanText(row.engine_version) || ENGINE_VERSION
  };
}

function toLocalDateKey(value = new Date()) {
  const date = value instanceof Date ? new Date(value) : new Date(value);
  if (Number.isNaN(date.getTime())) {
    return new Date().toISOString().slice(0, 10);
  }

  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

function shiftLocalDays(date, days) {
  const nextDate = new Date(date);
  nextDate.setDate(nextDate.getDate() + days);
  return nextDate;
}

function getLocalDayStart(date = new Date()) {
  const nextDate = new Date(date);
  nextDate.setHours(0, 0, 0, 0);
  return nextDate;
}

function normalizeResultInput(input = {}) {
  return {
    id: parseInteger(input.id, null),
    asin: cleanText(input.asin),
    title: cleanText(input.title),
    imageUrl: cleanText(input.imageUrl || input.image_url),
    productUrl: cleanText(input.productUrl || input.product_url),
    sellerType: normalizeSellerType(input.sellerType || input.seller_type),
    categoryName: cleanText(input.categoryName || input.category_name),
    currentPrice: parseNumber(input.currentPrice ?? input.current_price, null),
    referencePrice: parseNumber(input.referencePrice ?? input.reference_price, null),
    keepaDiscount: parseNumber(input.keepaDiscount ?? input.keepa_discount, 0) || 0,
    dealScore: parseNumber(input.dealScore ?? input.deal_score, 0) || 0,
    note: cleanText(input.note),
    comparisonSource: cleanText(input.comparisonSource || input.comparison_source),
    keepaPayload:
      typeof input.keepaPayload === 'object' && input.keepaPayload
        ? input.keepaPayload
        : fromJson(input.keepa_payload_json, null),
    searchPayload:
      typeof input.searchPayload === 'object' && input.searchPayload
        ? input.searchPayload
        : fromJson(input.search_payload_json, null),
    createdAt: cleanText(input.createdAt || input.created_at) || nowIso(),
    updatedAt: cleanText(input.updatedAt || input.updated_at) || nowIso()
  };
}

function isFiniteNumber(value) {
  return typeof value === 'number' && Number.isFinite(value);
}

function normalizePriceCandidate(rawValue, currentPrice = null) {
  const numeric = parseNumber(rawValue, null);
  if (numeric === null || numeric <= 0) {
    return null;
  }

  if (currentPrice && numeric > currentPrice * 20) {
    return Math.round((numeric / 100) * 100) / 100;
  }

  if (!currentPrice && numeric > 5000) {
    return Math.round((numeric / 100) * 100) / 100;
  }

  return Math.round(numeric * 100) / 100;
}

function normalizeTimestamp(rawValue) {
  const numeric = parseNumber(rawValue, null);
  if (numeric === null) {
    return null;
  }

  if (numeric > 1000000000000) {
    return new Date(numeric).toISOString();
  }

  if (numeric > 1000000000) {
    return new Date(numeric * 1000).toISOString();
  }

  if (numeric > 0) {
    return new Date(KEEPA_EPOCH_MS + numeric * 60 * 1000).toISOString();
  }

  return null;
}

function buildSeriesFromFlatPairs(values, currentPrice) {
  if (!Array.isArray(values) || values.length < 4) {
    return [];
  }

  const series = [];
  for (let index = 0; index < values.length - 1; index += 2) {
    const timestamp = normalizeTimestamp(values[index]);
    const price = normalizePriceCandidate(values[index + 1], currentPrice);

    if (!timestamp || price === null) {
      continue;
    }

    series.push({
      timestamp,
      price
    });
  }

  return series;
}

function scoreSeriesCandidate(series, currentPrice) {
  if (!Array.isArray(series) || series.length < 3) {
    return -1;
  }

  const lastPoint = series[series.length - 1];
  const lastTimestamp = new Date(lastPoint.timestamp).getTime();
  const recencyScore = Number.isFinite(lastTimestamp) ? Math.max(0, 1 - (Date.now() - lastTimestamp) / (180 * 24 * 60 * 60 * 1000)) : 0;
  const closenessScore =
    currentPrice && lastPoint?.price
      ? Math.max(0, 1 - Math.abs(lastPoint.price - currentPrice) / Math.max(currentPrice, 1))
      : 0.5;
  const densityScore = Math.min(1, series.length / 40);

  return recencyScore * 0.45 + closenessScore * 0.4 + densityScore * 0.15;
}

function collectCandidateSeries(payload, currentPrice) {
  const sources = [];
  const productCandidates = [
    payload?.raw?.product?.csv,
    payload?.product?.csv,
    payload?.rawProduct?.csv,
    payload?.priceHistory,
    payload?.raw?.product?.priceHistory
  ];

  productCandidates.forEach((candidate) => {
    if (Array.isArray(candidate) && candidate.length && Array.isArray(candidate[0])) {
      candidate.forEach((entry) => sources.push(entry));
      return;
    }

    if (candidate && typeof candidate === 'object' && !Array.isArray(candidate)) {
      Object.values(candidate).forEach((entry) => sources.push(entry));
      return;
    }

    if (Array.isArray(candidate)) {
      sources.push(candidate);
    }
  });

  const candidates = sources
    .map((entry) => buildSeriesFromFlatPairs(entry, currentPrice))
    .filter((entry) => entry.length >= 3)
    .sort((left, right) => scoreSeriesCandidate(right, currentPrice) - scoreSeriesCandidate(left, currentPrice));

  return candidates[0] || [];
}

function extractAverage(stats, keys) {
  if (!stats || typeof stats !== 'object') {
    return null;
  }

  for (const key of keys) {
    const value = stats[key];
    if (Array.isArray(value)) {
      const numeric = value.map((entry) => normalizePriceCandidate(entry)).find((entry) => entry !== null);
      if (numeric !== null) {
        return numeric;
      }
    }

    const numeric = normalizePriceCandidate(value);
    if (numeric !== null) {
      return numeric;
    }
  }

  return null;
}

function buildFallbackSeries(result) {
  const currentPrice = parseNumber(result.currentPrice, null);
  const referencePrice = parseNumber(result.referencePrice, null);
  const discount = parseNumber(result.keepaDiscount, 0) || 0;

  if (currentPrice === null && referencePrice === null) {
    return [];
  }

  const now = new Date();
  const inferredReference =
    referencePrice !== null
      ? referencePrice
      : currentPrice !== null && discount > 0 && discount < 100
        ? Math.round((currentPrice / (1 - discount / 100)) * 100) / 100
        : currentPrice;

  return [
    { timestamp: shiftLocalDays(now, -180).toISOString(), price: inferredReference },
    { timestamp: shiftLocalDays(now, -90).toISOString(), price: inferredReference },
    {
      timestamp: shiftLocalDays(now, -30).toISOString(),
      price: currentPrice !== null && inferredReference !== null ? Math.round(((inferredReference + currentPrice) / 2) * 100) / 100 : inferredReference
    },
    { timestamp: now.toISOString(), price: currentPrice ?? inferredReference }
  ].filter((entry) => entry.price !== null);
}

function compressSeries(series, maxPoints = 32) {
  const safeSeries = Array.isArray(series) ? series : [];
  if (safeSeries.length <= maxPoints) {
    return safeSeries;
  }

  const output = [];
  const step = Math.max(1, Math.floor(safeSeries.length / maxPoints));
  for (let index = 0; index < safeSeries.length; index += step) {
    output.push(safeSeries[index]);
  }

  const lastPoint = safeSeries[safeSeries.length - 1];
  if (output[output.length - 1] !== lastPoint) {
    output.push(lastPoint);
  }

  return output.slice(-maxPoints);
}

function countTransitions(values) {
  const safeValues = (Array.isArray(values) ? values : []).filter((value) => value !== null && value !== undefined && value !== '');
  if (safeValues.length < 2) {
    return 0;
  }

  let count = 0;
  let lastValue = safeValues[0];
  for (let index = 1; index < safeValues.length; index += 1) {
    if (safeValues[index] !== lastValue) {
      count += 1;
      lastValue = safeValues[index];
    }
  }

  return count;
}

function extractOfferSignals(payload) {
  const product = payload?.raw?.product || payload?.product || payload?.rawProduct || {};
  const sellerSignals = [];

  Object.entries(product).forEach(([key, value]) => {
    if (!/seller|offer/i.test(key)) {
      return;
    }

    if (Array.isArray(value)) {
      sellerSignals.push(...value.slice(0, 60));
      return;
    }

    if (value && typeof value === 'object') {
      sellerSignals.push(...Object.values(value).slice(0, 60));
    }
  });

  return countTransitions(sellerSignals);
}

function hasCouponLikeMarkers(payload) {
  const snapshot = cleanText(JSON.stringify(payload || {}).toLowerCase());
  return /coupon|voucher|lightning|promo|promotion/.test(snapshot);
}

function getSeriesWindow(series, days) {
  const since = Date.now() - days * 24 * 60 * 60 * 1000;
  return series.filter((item) => new Date(item.timestamp).getTime() >= since);
}

function average(values) {
  const safeValues = values.filter((value) => isFiniteNumber(value));
  if (!safeValues.length) {
    return null;
  }

  return safeValues.reduce((sum, value) => sum + value, 0) / safeValues.length;
}

function standardDeviation(values) {
  const mean = average(values);
  if (mean === null) {
    return null;
  }

  const safeValues = values.filter((value) => isFiniteNumber(value));
  if (!safeValues.length) {
    return null;
  }

  const variance =
    safeValues.reduce((sum, value) => sum + (value - mean) ** 2, 0) / safeValues.length;
  return Math.sqrt(variance);
}

function daysBetween(start, end) {
  const startMs = new Date(start).getTime();
  const endMs = new Date(end).getTime();
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs)) {
    return 0;
  }

  return Math.max(0, (endMs - startMs) / (24 * 60 * 60 * 1000));
}

function buildChartPoints(series) {
  return compressSeries(series, 18).map((item) => ({
    timestamp: item.timestamp,
    label: new Intl.DateTimeFormat('de-DE', { day: '2-digit', month: '2-digit' }).format(new Date(item.timestamp)),
    price: item.price
  }));
}

function buildRelevantPricePoints(series, result) {
  const safeSeries = Array.isArray(series) ? series.filter((item) => item?.timestamp && isFiniteNumber(item?.price)) : [];
  if (!safeSeries.length) {
    return [];
  }

  const currentPoint = safeSeries[safeSeries.length - 1] || null;
  const lowestPoint = safeSeries.reduce((lowest, item) => (!lowest || item.price < lowest.price ? item : lowest), null);
  const highestPoint = safeSeries.reduce((highest, item) => (!highest || item.price > highest.price ? item : highest), null);
  const referencePrice = parseNumber(result.referencePrice, null);

  return [
    currentPoint
      ? {
          key: 'current',
          label: 'Aktuell',
          price: currentPoint.price,
          timestamp: currentPoint.timestamp
        }
      : null,
    lowestPoint
      ? {
          key: 'low',
          label: 'Tief',
          price: lowestPoint.price,
          timestamp: lowestPoint.timestamp
        }
      : null,
    highestPoint
      ? {
          key: 'high',
          label: 'Hoch',
          price: highestPoint.price,
          timestamp: highestPoint.timestamp
        }
      : null,
    referencePrice !== null
      ? {
          key: 'reference',
          label: 'Referenz',
          price: referencePrice,
          timestamp: currentPoint?.timestamp || result.updatedAt || result.createdAt || nowIso()
        }
      : null
  ].filter(Boolean);
}

export function buildKeepaChartSnapshot(resultInput = {}) {
  const result = normalizeResultInput(resultInput);
  const rawSeries = collectCandidateSeries(result.keepaPayload || {}, result.currentPrice);
  const fallbackSeries = buildFallbackSeries(result);
  const analysisSeries = compressSeries(rawSeries.length ? rawSeries : fallbackSeries, 120).slice(-120);

  return {
    chartSource: rawSeries.length ? 'keepa-history' : fallbackSeries.length ? 'derived-fallback' : 'empty',
    historyPointCount: analysisSeries.length,
    priceSeries: compressSeries(analysisSeries, 48),
    chartPoints: buildChartPoints(analysisSeries),
    relevantPoints: buildRelevantPricePoints(analysisSeries, result)
  };
}

function getFeedbackAdjustments() {
  const rows = db
    .prepare(
      `
        SELECT
          seller_type,
          SUM(CASE WHEN label IN ('ja', 'approved', 'strong_deal') THEN 1 ELSE 0 END) AS positive_count,
          SUM(CASE WHEN label IN ('nein', 'rejected', 'fake_drop', 'weak_deal') THEN 1 ELSE 0 END) AS negative_count,
          SUM(CASE WHEN label IN ('eventuell_gut', 'ueberspringen') THEN 1 ELSE 0 END) AS uncertain_count
        FROM keepa_review_labels
        GROUP BY seller_type
      `
    )
    .all();

  const feedback = {};
  rows.forEach((row) => {
    const sellerType = normalizeSellerType(row.seller_type);
    const positiveCount = parseInteger(row.positive_count, 0);
    const negativeCount = parseInteger(row.negative_count, 0);
    const uncertainCount = parseInteger(row.uncertain_count, 0);
    const total = positiveCount + negativeCount + uncertainCount;
    const rawAdjustment = total >= 3 ? ((negativeCount - positiveCount) / total) * 12 : 0;
    feedback[sellerType] = {
      sellerType,
      positiveCount,
      negativeCount,
      uncertainCount,
      total,
      riskAdjustment: Math.round(clamp(rawAdjustment, -8, 8) * 10) / 10
    };
  });

  return feedback;
}

function analyzeTimeSeries(result, settings, feedbackAdjustments) {
  const payload = result.keepaPayload || {};
  const stats = payload?.raw?.product?.stats || payload?.product?.stats || {};
  const rawSeries = collectCandidateSeries(payload, result.currentPrice);
  const fallbackSeries = buildFallbackSeries(result);
  const fullSeries = compressSeries(rawSeries.length ? rawSeries : fallbackSeries, 120);
  const analysisSeries = fullSeries.slice(-120);
  const prices = analysisSeries.map((item) => item.price).filter((item) => isFiniteNumber(item) && item > 0);
  const latestPoint = analysisSeries[analysisSeries.length - 1] || null;
  const avg30 = average(getSeriesWindow(analysisSeries, 30).map((item) => item.price)) ?? extractAverage(stats, ['avg30', 'avg90']);
  const avg90 = average(getSeriesWindow(analysisSeries, 90).map((item) => item.price)) ?? extractAverage(stats, ['avg90', 'avg']);
  const avg180 = average(getSeriesWindow(analysisSeries, 180).map((item) => item.price)) ?? extractAverage(stats, ['avg180', 'avg90', 'avg']);
  const historicalLow = prices.length ? Math.min(...prices) : null;
  const historySpanDays =
    analysisSeries.length >= 2 ? Math.round(daysBetween(analysisSeries[0].timestamp, latestPoint.timestamp)) : 0;
  const priceChangeCount = analysisSeries.reduce((count, item, index, source) => {
    if (index === 0) {
      return count;
    }

    const previous = source[index - 1];
    return Math.abs(item.price - previous.price) >= Math.max(0.5, previous.price * 0.03) ? count + 1 : count;
  }, 0);
  const deltas = analysisSeries.slice(1).map((item, index) => {
    const previous = analysisSeries[index];
    return previous.price > 0 ? ((item.price - previous.price) / previous.price) * 100 : 0;
  });
  const zigZagCount = deltas.reduce((count, delta, index, source) => {
    if (index === 0) {
      return count;
    }

    const previous = source[index - 1];
    if (Math.abs(delta) < 3 || Math.abs(previous) < 3) {
      return count;
    }

    return Math.sign(delta) !== Math.sign(previous) ? count + 1 : count;
  }, 0);
  const spikeCount = analysisSeries.reduce((count, item, index, source) => {
    if (index === 0 || index === source.length - 1) {
      return count;
    }

    const prev = source[index - 1];
    const next = source[index + 1];
    const localMedian = (prev.price + next.price) / 2;
    const durationDays = daysBetween(prev.timestamp, next.timestamp);

    if (
      localMedian > 0 &&
      item.price >= localMedian * (1 + settings.spikeSensitivity / 100) &&
      durationDays <= settings.shortPeakMaxDays
    ) {
      return count + 1;
    }

    return count;
  }, 0);
  const reboundCount = analysisSeries.reduce((count, item, index, source) => {
    if (index === 0 || index >= source.length - 2) {
      return count;
    }

    const previous = source[index - 1];
    const lookAhead = source.slice(index + 1, index + 3);
    const rebound = lookAhead.some((candidate) => candidate.price >= item.price * 1.12);
    const windowDays = lookAhead.length ? daysBetween(item.timestamp, lookAhead[lookAhead.length - 1].timestamp) : 0;

    if (previous.price > 0 && item.price <= previous.price * 0.88 && rebound && windowDays <= settings.reboundWindowDays) {
      return count + 1;
    }

    return count;
  }, 0);

  let stableTailDays = 0;
  if (analysisSeries.length >= 2 && latestPoint) {
    for (let index = analysisSeries.length - 2; index >= 0; index -= 1) {
      const candidate = analysisSeries[index];
      const pctDiff = latestPoint.price > 0 ? Math.abs(candidate.price - latestPoint.price) / latestPoint.price : 0;
      if (pctDiff <= 0.05) {
        stableTailDays = Math.round(daysBetween(candidate.timestamp, latestPoint.timestamp));
        continue;
      }

      break;
    }
  }

  let currentDropDurationDays = 0;
  if (latestPoint) {
    for (let index = analysisSeries.length - 2; index >= 0; index -= 1) {
      if (analysisSeries[index].price >= latestPoint.price * 1.08) {
        currentDropDurationDays = Math.round(daysBetween(analysisSeries[index].timestamp, latestPoint.timestamp));
        break;
      }
    }
  }

  const volatility = standardDeviation(prices);
  const volatilityPct = prices.length && volatility !== null && average(prices)
    ? Math.round(((volatility / average(prices)) * 100) * 10) / 10
    : 0;
  const referenceGapPct =
    result.referencePrice !== null && avg90
      ? Math.round((((result.referencePrice - avg90) / Math.max(avg90, 1)) * 100) * 10) / 10
      : result.referencePrice !== null && result.currentPrice
        ? Math.round((((result.referencePrice - result.currentPrice) / Math.max(result.currentPrice, 1)) * 100) * 10) / 10
        : 0;
  const distanceToHistoricalLowPct =
    historicalLow !== null && result.currentPrice !== null
      ? Math.round((((result.currentPrice - historicalLow) / Math.max(historicalLow, 1)) * 100) * 10) / 10
      : null;
  const offerSwitchCount = extractOfferSignals(payload);
  const couponUncertainty = hasCouponLikeMarkers(payload);
  const historySparse = analysisSeries.length < 5 || historySpanDays < 21;
  const organicDowntrend =
    avg90 !== null && avg30 !== null && result.currentPrice !== null && avg90 > avg30 && avg30 > result.currentPrice;

  const flags = [];
  const positives = [];

  if (spikeCount >= 2) {
    flags.push({ id: 'short_spikes', label: 'Mehrere kurze Preisspitzen nach oben', severity: 'high' });
  }
  if (referenceGapPct >= settings.referenceInflationThreshold) {
    flags.push({ id: 'reference_inflated', label: 'Referenzpreis wirkt kuenstlich aufgeblasen', severity: 'high' });
  }
  if (reboundCount >= 1) {
    flags.push({ id: 'rebound_pattern', label: 'Preis springt nach Drops schnell wieder hoch', severity: 'medium' });
  }
  if (volatilityPct >= settings.volatilityWarningThreshold) {
    flags.push({ id: 'volatile_curve', label: 'Hohe Volatilitaet in kurzer Zeit', severity: 'medium' });
  }
  if (zigZagCount >= 3) {
    flags.push({ id: 'zigzag_curve', label: 'Unruhiger Zick-Zack-Verlauf', severity: 'medium' });
  }
  if (offerSwitchCount >= 3) {
    flags.push({ id: 'seller_mix', label: 'Viele Seller- oder Offer-Wechsel', severity: 'medium' });
  }
  if (couponUncertainty) {
    flags.push({ id: 'coupon_unclear', label: 'Moeglicher Coupon- oder Werbeeffekt', severity: 'low' });
  }
  if (historySparse) {
    flags.push({ id: 'history_sparse', label: 'Zu wenig Verlauf fuer eine harte Entscheidung', severity: 'medium' });
  }

  if (stableTailDays >= 7) {
    positives.push({ id: 'drop_holds', label: 'Preis bleibt nach dem Drop stabil' });
  }
  if (organicDowntrend) {
    positives.push({ id: 'organic_downtrend', label: 'Preisverfall wirkt organisch und nachvollziehbar' });
  }
  if (result.sellerType === 'AMAZON') {
    positives.push({ id: 'amazon_offer', label: 'Verkauf direkt ueber Amazon' });
  }
  if (referenceGapPct <= 10 && avg90 !== null) {
    positives.push({ id: 'stable_baseline', label: 'Referenzpreis basiert auf stabilem Verlauf' });
  }

  const weights = settings.weights || DEFAULT_WEIGHTS;
  const feedback = feedbackAdjustments[result.sellerType] || {
    sellerType: result.sellerType,
    positiveCount: 0,
    negativeCount: 0,
    uncertainCount: 0,
    total: 0,
    riskAdjustment: 0
  };
  const sellerRiskBias = result.sellerType === 'FBM' ? 8 : result.sellerType === 'FBA' ? 4 : -6;
  const sellerTrustBias = result.sellerType === 'AMAZON' ? 10 : result.sellerType === 'FBA' ? 3 : -4;

  const stabilityScore = clamp(
    Math.round(
      58 +
        stableTailDays * 1.8 +
        positives.length * 5 +
        sellerTrustBias -
        volatilityPct * 1.1 * weights.stability -
        spikeCount * 9 -
        zigZagCount * 3 -
        offerSwitchCount * 2 -
        (historySparse ? 12 : 0)
    ),
    0,
    100
  );
  const manipulationScore = clamp(
    Math.round(
      14 +
        sellerRiskBias +
        spikeCount * 14 * weights.manipulation +
        reboundCount * 10 +
        Math.max(0, volatilityPct - settings.volatilityWarningThreshold) * 1.9 +
        Math.max(0, referenceGapPct - settings.referenceInflationThreshold) * 1.4 +
        offerSwitchCount * 4 +
        (couponUncertainty ? 10 : 0) +
        (historySparse ? 10 : 0)
    ),
    0,
    100
  );
  const amazonConfidence = clamp(
    Math.round(
      (result.sellerType === 'AMAZON' ? 55 : result.sellerType === 'FBA' ? 30 : 12) +
        stabilityScore * 0.35 * weights.amazon -
        manipulationScore * 0.2 +
        (organicDowntrend ? 8 : 0) +
        (stableTailDays >= 10 ? 6 : 0)
    ),
    0,
    100
  );
  const trustScore = clamp(
    Math.round(
      50 +
        stabilityScore * 0.3 +
        amazonConfidence * 0.18 -
        manipulationScore * 0.22 +
        (historySparse ? -6 : 0)
    ),
    0,
    100
  );
  const fakeDropRisk = clamp(
    Math.round(
      18 +
        manipulationScore * 0.62 +
        Math.max(0, 55 - stabilityScore) * 0.55 -
        amazonConfidence * 0.14 -
        positives.length * 3 +
        flags.length * 3 +
        feedback.riskAdjustment * weights.feedback
    ),
    0,
    100
  );
  const reviewPriority = clamp(
    Math.round(
      fakeDropRisk * 0.58 +
        result.keepaDiscount * 0.28 +
        (historySparse ? 12 : 0) +
        (result.sellerType === 'FBM' ? 10 : result.sellerType === 'FBA' ? 5 : 0) +
        (result.dealScore >= 75 ? 6 : 0)
    ),
    0,
    100
  );

  let classification = 'manuelle_pruefung';
  if (
    result.sellerType === 'AMAZON' &&
    amazonConfidence >= settings.amazonConfidenceStrong &&
    stabilityScore >= settings.stabilityStrong &&
    fakeDropRisk <= settings.lowRiskThreshold
  ) {
    classification = 'amazon_stabil';
  } else if (
    fakeDropRisk >= settings.highRiskThreshold ||
    (spikeCount >= 2 && referenceGapPct >= settings.referenceInflationThreshold)
  ) {
    classification = 'wahrscheinlicher_fake_drop';
  } else if (
    !historySparse &&
    result.keepaDiscount >= 15 &&
    stabilityScore >= settings.stabilityStrong &&
    fakeDropRisk <= settings.lowRiskThreshold
  ) {
    classification = 'echter_deal';
  } else if (fakeDropRisk >= settings.lowRiskThreshold || flags.length) {
    classification = 'verdaechtig';
  }

  const reviewRecommended =
    classification === 'wahrscheinlicher_fake_drop' ||
    classification === 'verdaechtig' ||
    classification === 'manuelle_pruefung' ||
    reviewPriority >= settings.reviewPriorityThreshold ||
    historySparse;

  const reasons = [
    `Stability Score ${stabilityScore}`,
    `Fake-Drop Risk ${fakeDropRisk}`,
    `Amazon Confidence ${amazonConfidence}`,
    `Review Priority ${reviewPriority}`
  ];
  flags.slice(0, 4).forEach((flag) => reasons.push(flag.label));
  positives.slice(0, 3).forEach((positive) => reasons.push(positive.label));
  if (feedback.total >= 3 && feedback.riskAdjustment !== 0) {
    reasons.push(`Feedback-Anpassung ${feedback.riskAdjustment > 0 ? '+' : ''}${feedback.riskAdjustment} Risiko aus ${feedback.total} Reviews`);
  }

  return {
    engineVersion: settings.engineVersion || ENGINE_VERSION,
    classification,
    classificationLabel: CLASSIFICATION_LABELS[classification],
    reviewRecommended,
    stabilityScore,
    manipulationScore,
    trustScore,
    amazonConfidence,
    fakeDropRisk,
    reviewPriority,
    analysisReason: reasons.join(' | '),
    features: {
      currentPrice: result.currentPrice,
      avg30,
      avg90,
      avg180,
      historicalLow,
      distanceToHistoricalLowPct,
      priceChangeCount,
      volatilityPct,
      stableTailDays,
      currentDropDurationDays,
      spikeCount,
      reboundCount,
      zigZagCount,
      offerSwitchCount,
      sellerType: result.sellerType,
      historySpanDays,
      historySparse,
      referenceGapPct,
      couponUncertainty,
      feedbackAdjustment: feedback.riskAdjustment
    },
    flags,
    positives,
    chartPoints: buildChartPoints(analysisSeries),
    priceSeries: compressSeries(analysisSeries, 48),
    offerSeries: [],
    reasoning: {
      reasons,
      flags,
      positives,
      scoreBreakdown: {
        stabilityScore,
        manipulationScore,
        trustScore,
        amazonConfidence,
        fakeDropRisk,
        reviewPriority
      },
      feedback
    }
  };
}

function getExistingReviewItem(resultId) {
  return db.prepare(`SELECT * FROM keepa_review_items WHERE keepa_result_id = ?`).get(resultId);
}

function upsertFeatureSnapshot(result, analysis) {
  const existing = db
    .prepare(`SELECT id, created_at FROM keepa_feature_snapshots WHERE keepa_result_id = ?`)
    .get(result.id);
  const timestamp = nowIso();
  const payload = {
    keepaResultId: result.id,
    asin: result.asin,
    sellerType: result.sellerType,
    featureJson: toJson(analysis.features),
    priceSeriesJson: toJson(analysis.priceSeries),
    offerSeriesJson: toJson(analysis.offerSeries),
    chartPointsJson: toJson(analysis.chartPoints),
    engineVersion: analysis.engineVersion,
    createdAt: existing?.created_at || timestamp,
    updatedAt: timestamp
  };

  if (existing) {
    db.prepare(
      `
        UPDATE keepa_feature_snapshots
        SET feature_json = @featureJson,
            price_series_json = @priceSeriesJson,
            offer_series_json = @offerSeriesJson,
            chart_points_json = @chartPointsJson,
            engine_version = @engineVersion,
            updated_at = @updatedAt
        WHERE keepa_result_id = @keepaResultId
      `
    ).run(payload);
    return existing.id;
  }

  const inserted = db
    .prepare(
      `
        INSERT INTO keepa_feature_snapshots (
          keepa_result_id,
          asin,
          seller_type,
          feature_json,
          price_series_json,
          offer_series_json,
          chart_points_json,
          engine_version,
          created_at,
          updated_at
        ) VALUES (
          @keepaResultId,
          @asin,
          @sellerType,
          @featureJson,
          @priceSeriesJson,
          @offerSeriesJson,
          @chartPointsJson,
          @engineVersion,
          @createdAt,
          @updatedAt
        )
      `
    )
    .run(payload);

  return Number(inserted.lastInsertRowid);
}

function upsertScore(result, analysis) {
  const existing = db
    .prepare(`SELECT id, created_at FROM keepa_fake_drop_scores WHERE keepa_result_id = ?`)
    .get(result.id);
  const timestamp = nowIso();
  const payload = {
    keepaResultId: result.id,
    asin: result.asin,
    sellerType: result.sellerType,
    classification: analysis.classification,
    stabilityScore: analysis.stabilityScore,
    manipulationScore: analysis.manipulationScore,
    trustScore: analysis.trustScore,
    amazonConfidence: analysis.amazonConfidence,
    fakeDropRisk: analysis.fakeDropRisk,
    reviewPriority: analysis.reviewPriority,
    reasoningJson: toJson(analysis.reasoning),
    engineVersion: analysis.engineVersion,
    createdAt: existing?.created_at || timestamp,
    updatedAt: timestamp
  };

  if (existing) {
    db.prepare(
      `
        UPDATE keepa_fake_drop_scores
        SET asin = @asin,
            seller_type = @sellerType,
            classification = @classification,
            stability_score = @stabilityScore,
            manipulation_score = @manipulationScore,
            trust_score = @trustScore,
            amazon_confidence = @amazonConfidence,
            fake_drop_risk = @fakeDropRisk,
            review_priority = @reviewPriority,
            reasoning_json = @reasoningJson,
            engine_version = @engineVersion,
            updated_at = @updatedAt
        WHERE keepa_result_id = @keepaResultId
      `
    ).run(payload);
    return existing.id;
  }

  const inserted = db
    .prepare(
      `
        INSERT INTO keepa_fake_drop_scores (
          keepa_result_id,
          asin,
          seller_type,
          classification,
          stability_score,
          manipulation_score,
          trust_score,
          amazon_confidence,
          fake_drop_risk,
          review_priority,
          reasoning_json,
          engine_version,
          created_at,
          updated_at
        ) VALUES (
          @keepaResultId,
          @asin,
          @sellerType,
          @classification,
          @stabilityScore,
          @manipulationScore,
          @trustScore,
          @amazonConfidence,
          @fakeDropRisk,
          @reviewPriority,
          @reasoningJson,
          @engineVersion,
          @createdAt,
          @updatedAt
        )
      `
    )
    .run(payload);

  return Number(inserted.lastInsertRowid);
}

function upsertReviewItem(result, analysis, scoreId, snapshotId) {
  const existing = getExistingReviewItem(result.id);
  const timestamp = nowIso();
  const currentLabel = cleanText(existing?.current_label);
  const reviewStatus = analysis.reviewRecommended
    ? currentLabel
      ? 'reviewed'
      : 'open'
    : currentLabel
      ? 'reviewed'
      : 'closed';
  const payload = {
    keepaResultId: result.id,
    fakeDropScoreId: scoreId,
    featureSnapshotId: snapshotId,
    asin: result.asin,
    sellerType: result.sellerType,
    categoryName: result.categoryName || null,
    classification: analysis.classification,
    reviewStatus,
    reviewPriority: analysis.reviewPriority,
    analysisReason: analysis.analysisReason,
    currentLabel: currentLabel || null,
    tagsJson: existing?.tags_json || '[]',
    note: existing?.note || result.note || '',
    chartSnapshotJson: toJson(analysis.chartPoints),
    exampleBucket: existing?.example_bucket || null,
    labelCount: parseInteger(existing?.label_count, 0),
    lastReviewedAt: existing?.last_reviewed_at || null,
    createdAt: existing?.created_at || timestamp,
    updatedAt: timestamp
  };

  if (existing) {
    db.prepare(
      `
        UPDATE keepa_review_items
        SET fake_drop_score_id = @fakeDropScoreId,
            feature_snapshot_id = @featureSnapshotId,
            seller_type = @sellerType,
            category_name = @categoryName,
            classification = @classification,
            review_status = @reviewStatus,
            review_priority = @reviewPriority,
            analysis_reason = @analysisReason,
            chart_snapshot_json = @chartSnapshotJson,
            updated_at = @updatedAt
        WHERE keepa_result_id = @keepaResultId
      `
    ).run(payload);
    return existing.id;
  }

  const inserted = db
    .prepare(
      `
        INSERT INTO keepa_review_items (
          keepa_result_id,
          fake_drop_score_id,
          feature_snapshot_id,
          asin,
          seller_type,
          category_name,
          classification,
          review_status,
          review_priority,
          analysis_reason,
          current_label,
          tags_json,
          note,
          chart_snapshot_json,
          example_bucket,
          label_count,
          last_reviewed_at,
          created_at,
          updated_at
        ) VALUES (
          @keepaResultId,
          @fakeDropScoreId,
          @featureSnapshotId,
          @asin,
          @sellerType,
          @categoryName,
          @classification,
          @reviewStatus,
          @reviewPriority,
          @analysisReason,
          @currentLabel,
          @tagsJson,
          @note,
          @chartSnapshotJson,
          @exampleBucket,
          @labelCount,
          @lastReviewedAt,
          @createdAt,
          @updatedAt
        )
      `
    )
    .run(payload);

  return Number(inserted.lastInsertRowid);
}

function mapReviewLabel(row) {
  return row
    ? {
        id: row.id,
        label: normalizeReviewLabel(row.label),
        labelLabel: REVIEW_LABEL_CATALOG.find((item) => item.id === normalizeReviewLabel(row.label))?.label || row.label,
        tags: fromJson(row.tags_json, []),
        note: row.note || '',
        createdAt: row.created_at
      }
    : null;
}

function mapFakeDropSnapshot(scoreRow, featureRow, reviewRow) {
  if (!scoreRow) {
    return null;
  }

  const classification = normalizeClassification(scoreRow.classification);
  const reasoning = fromJson(scoreRow.reasoning_json, {});
  const features = fromJson(featureRow?.feature_json, {});
  const chartPoints = fromJson(featureRow?.chart_points_json, fromJson(reviewRow?.chart_snapshot_json, [])) || [];

  return {
    scoreId: scoreRow.id,
    reviewItemId: reviewRow?.id || null,
    classification,
    classificationLabel: CLASSIFICATION_LABELS[classification],
    stabilityScore: Math.round((parseNumber(scoreRow.stability_score, 0) || 0) * 10) / 10,
    manipulationScore: Math.round((parseNumber(scoreRow.manipulation_score, 0) || 0) * 10) / 10,
    trustScore: Math.round((parseNumber(scoreRow.trust_score, 0) || 0) * 10) / 10,
    amazonConfidence: Math.round((parseNumber(scoreRow.amazon_confidence, 0) || 0) * 10) / 10,
    fakeDropRisk: Math.round((parseNumber(scoreRow.fake_drop_risk, 0) || 0) * 10) / 10,
    reviewPriority: Math.round((parseNumber(scoreRow.review_priority, 0) || 0) * 10) / 10,
    analysisReason: reviewRow?.analysis_reason || reasoning?.reasons?.join(' | ') || '',
    flags: Array.isArray(reasoning?.flags) ? reasoning.flags : [],
    positives: Array.isArray(reasoning?.positives) ? reasoning.positives : [],
    scoreBreakdown: reasoning?.scoreBreakdown || {},
    features,
    chartPoints,
    reviewStatus: normalizeReviewStatus(reviewRow?.review_status),
    currentLabel: cleanText(reviewRow?.current_label) || null,
    tags: fromJson(reviewRow?.tags_json, []),
    note: reviewRow?.note || '',
    labelCount: parseInteger(reviewRow?.label_count, 0),
    lastReviewedAt: reviewRow?.last_reviewed_at || null,
    reviewRecommended: normalizeReviewStatus(reviewRow?.review_status) === 'open',
    engineVersion: cleanText(scoreRow.engine_version) || ENGINE_VERSION
  };
}

export function getFakeDropSnapshotForResult(resultId) {
  const scoreRow = db.prepare(`SELECT * FROM keepa_fake_drop_scores WHERE keepa_result_id = ?`).get(resultId);
  if (!scoreRow) {
    return null;
  }

  const featureRow = db.prepare(`SELECT * FROM keepa_feature_snapshots WHERE keepa_result_id = ?`).get(resultId);
  const reviewRow = db.prepare(`SELECT * FROM keepa_review_items WHERE keepa_result_id = ?`).get(resultId);
  return mapFakeDropSnapshot(scoreRow, featureRow, reviewRow);
}

export function persistFakeDropAnalysis(resultInput, options = {}) {
  const result = normalizeResultInput(resultInput);
  if (!result.id || !result.asin) {
    return null;
  }

  const settings = getFakeDropSettingsView();
  if (!settings.engineEnabled) {
    return null;
  }

  const feedbackAdjustments = getFeedbackAdjustments();
  const analysis = analyzeTimeSeries(result, settings, feedbackAdjustments);
  const snapshotId = upsertFeatureSnapshot(result, analysis);
  const scoreId = upsertScore(result, analysis);
  const reviewItemId = upsertReviewItem(result, analysis, scoreId, snapshotId);
  const storedSnapshot = getFakeDropSnapshotForResult(result.id);

  logGeneratorDebug('LEARNING CASE STORED', {
    keepaResultId: result.id,
    reviewItemId,
    asin: result.asin,
    sellerType: result.sellerType,
    classification: analysis.classification,
    sourceType: normalizeLearningSource(resultInput.origin || resultInput.sourceType)
  });
  logGeneratorDebug('PRICE HISTORY ATTACHED TO CASE', {
    keepaResultId: result.id,
    reviewItemId,
    asin: result.asin,
    sellerType: result.sellerType,
    chartPointCount: Array.isArray(analysis.chartPoints) ? analysis.chartPoints.length : 0,
    priceSeriesCount: Array.isArray(analysis.priceSeries) ? analysis.priceSeries.length : 0
  });

  return {
    reviewItemId,
    ...storedSnapshot
  };
}

export function getFakeDropSettingsView() {
  const settings = normalizeSettingsRow(getSettingsRow());
  const feedbackAdjustments = Object.values(getFeedbackAdjustments())
    .sort((left, right) => right.total - left.total)
    .map((item) => ({
      ...item,
      note:
        item.total >= 3
          ? `${item.total} gelabelte Beispiele fuer ${item.sellerType}, Anpassung ${item.riskAdjustment > 0 ? '+' : ''}${item.riskAdjustment}.`
          : `Noch zu wenig Feedback fuer ${item.sellerType}.`
    }));

  return {
    ...settings,
    reviewLabelCatalog: REVIEW_LABEL_CATALOG,
    reviewTagCatalog: REVIEW_TAG_CATALOG,
    classificationCatalog: Object.entries(CLASSIFICATION_LABELS).map(([id, label]) => ({ id, label })),
    feedbackAdjustments
  };
}

export function saveFakeDropSettings(input = {}) {
  const current = getFakeDropSettingsView();
  const next = {
    engineEnabled: input.engineEnabled === undefined ? current.engineEnabled : parseBool(input.engineEnabled),
    lowRiskThreshold:
      input.lowRiskThreshold === undefined
        ? current.lowRiskThreshold
        : clamp(parseNumber(input.lowRiskThreshold, current.lowRiskThreshold), 0, 100),
    highRiskThreshold:
      input.highRiskThreshold === undefined
        ? current.highRiskThreshold
        : clamp(parseNumber(input.highRiskThreshold, current.highRiskThreshold), 0, 100),
    reviewPriorityThreshold:
      input.reviewPriorityThreshold === undefined
        ? current.reviewPriorityThreshold
        : clamp(parseNumber(input.reviewPriorityThreshold, current.reviewPriorityThreshold), 0, 100),
    amazonConfidenceStrong:
      input.amazonConfidenceStrong === undefined
        ? current.amazonConfidenceStrong
        : clamp(parseNumber(input.amazonConfidenceStrong, current.amazonConfidenceStrong), 0, 100),
    stabilityStrong:
      input.stabilityStrong === undefined
        ? current.stabilityStrong
        : clamp(parseNumber(input.stabilityStrong, current.stabilityStrong), 0, 100),
    referenceInflationThreshold:
      input.referenceInflationThreshold === undefined
        ? current.referenceInflationThreshold
        : clamp(parseNumber(input.referenceInflationThreshold, current.referenceInflationThreshold), 0, 200),
    volatilityWarningThreshold:
      input.volatilityWarningThreshold === undefined
        ? current.volatilityWarningThreshold
        : clamp(parseNumber(input.volatilityWarningThreshold, current.volatilityWarningThreshold), 0, 200),
    shortPeakMaxDays:
      input.shortPeakMaxDays === undefined
        ? current.shortPeakMaxDays
        : clamp(parseNumber(input.shortPeakMaxDays, current.shortPeakMaxDays), 1, 14),
    spikeSensitivity:
      input.spikeSensitivity === undefined
        ? current.spikeSensitivity
        : clamp(parseNumber(input.spikeSensitivity, current.spikeSensitivity), 4, 60),
    reboundWindowDays:
      input.reboundWindowDays === undefined
        ? current.reboundWindowDays
        : clamp(parseNumber(input.reboundWindowDays, current.reboundWindowDays), 1, 21),
    weights: input.weights === undefined ? current.weights : normalizeWeights(input.weights)
  };

  if (next.lowRiskThreshold >= next.highRiskThreshold) {
    throw new Error('Die Low-Risk-Schwelle muss kleiner als die High-Risk-Schwelle sein.');
  }

  db.prepare(
    `
      UPDATE keepa_fake_drop_settings
      SET engine_enabled = @engineEnabled,
          low_risk_threshold = @lowRiskThreshold,
          high_risk_threshold = @highRiskThreshold,
          review_priority_threshold = @reviewPriorityThreshold,
          amazon_confidence_strong = @amazonConfidenceStrong,
          stability_strong = @stabilityStrong,
          reference_inflation_threshold = @referenceInflationThreshold,
          volatility_warning_threshold = @volatilityWarningThreshold,
          short_peak_max_days = @shortPeakMaxDays,
          spike_sensitivity = @spikeSensitivity,
          rebound_window_days = @reboundWindowDays,
          weights_json = @weightsJson,
          engine_version = @engineVersion,
          updated_at = @updatedAt
      WHERE id = 1
    `
  ).run({
    engineEnabled: next.engineEnabled ? 1 : 0,
    lowRiskThreshold: next.lowRiskThreshold,
    highRiskThreshold: next.highRiskThreshold,
    reviewPriorityThreshold: next.reviewPriorityThreshold,
    amazonConfidenceStrong: next.amazonConfidenceStrong,
    stabilityStrong: next.stabilityStrong,
    referenceInflationThreshold: next.referenceInflationThreshold,
    volatilityWarningThreshold: next.volatilityWarningThreshold,
    shortPeakMaxDays: next.shortPeakMaxDays,
    spikeSensitivity: next.spikeSensitivity,
    reboundWindowDays: next.reboundWindowDays,
    weightsJson: toJson(next.weights),
    engineVersion: ENGINE_VERSION,
    updatedAt: nowIso()
  });

  return getFakeDropSettingsView();
}

function buildPatternBreakdown(limit = 120) {
  const rows = db
    .prepare(`SELECT reasoning_json FROM keepa_fake_drop_scores ORDER BY updated_at DESC LIMIT ?`)
    .all(limit);
  const counts = new Map();

  rows.forEach((row) => {
    const flags = fromJson(row.reasoning_json, {})?.flags || [];
    flags.forEach((flag) => {
      counts.set(flag.id, {
        id: flag.id,
        label: flag.label,
        count: (counts.get(flag.id)?.count || 0) + 1
      });
    });
  });

  return [...counts.values()].sort((left, right) => right.count - left.count).slice(0, 8);
}

export function getFakeDropSummary() {
  const countRows = db
    .prepare(
      `
        SELECT classification, COUNT(*) AS count
        FROM keepa_fake_drop_scores
        GROUP BY classification
      `
    )
    .all();
  const counts = countRows.reduce(
    (accumulator, row) => ({
      ...accumulator,
      [normalizeClassification(row.classification)]: parseInteger(row.count, 0)
    }),
    {
      echter_deal: 0,
      verdaechtig: 0,
      wahrscheinlicher_fake_drop: 0,
      manuelle_pruefung: 0,
      amazon_stabil: 0
    }
  );
  const reviewStats =
    db
      .prepare(
        `
          SELECT
            SUM(CASE WHEN review_status = 'open' THEN 1 ELSE 0 END) AS open_count,
            SUM(CASE WHEN current_label IS NULL OR current_label = '' THEN 1 ELSE 0 END) AS unlabeled_count,
            COUNT(*) AS total_count
          FROM keepa_review_items
        `
      )
      .get() || {};
  const recentReviews = db
    .prepare(
      `
        SELECT
          rl.*,
          kr.title,
          kr.image_url,
          kri.classification
        FROM keepa_review_labels rl
        JOIN keepa_review_items kri ON kri.id = rl.review_item_id
        JOIN keepa_results kr ON kr.id = rl.keepa_result_id
        ORDER BY rl.created_at DESC
        LIMIT 8
      `
    )
    .all()
    .map((row) => ({
      id: row.id,
      asin: row.asin,
      title: row.title,
      imageUrl: row.image_url,
      sellerType: row.seller_type,
      label: normalizeReviewLabel(row.label),
      labelLabel: REVIEW_LABEL_CATALOG.find((item) => item.id === normalizeReviewLabel(row.label))?.label || row.label,
      classification: normalizeClassification(row.classification),
      classificationLabel: CLASSIFICATION_LABELS[normalizeClassification(row.classification)],
      tags: fromJson(row.tags_json, []),
      note: row.note || '',
      createdAt: row.created_at
    }));
  const recentDetections = db
    .prepare(
      `
        SELECT
          kr.id,
          kr.asin,
          kr.title,
          kr.image_url,
          kfds.classification,
          kfds.fake_drop_risk,
          kfds.reasoning_json,
          kfds.updated_at
        FROM keepa_fake_drop_scores kfds
        JOIN keepa_results kr ON kr.id = kfds.keepa_result_id
        ORDER BY kfds.updated_at DESC
        LIMIT 8
      `
    )
    .all()
    .map((row) => ({
      id: row.id,
      asin: row.asin,
      title: row.title,
      imageUrl: row.image_url,
      classification: normalizeClassification(row.classification),
      classificationLabel: CLASSIFICATION_LABELS[normalizeClassification(row.classification)],
      fakeDropRisk: Math.round((parseNumber(row.fake_drop_risk, 0) || 0) * 10) / 10,
      flags: fromJson(row.reasoning_json, {})?.flags || [],
      updatedAt: row.updated_at
    }));
  const feedback = getFakeDropSettingsView().feedbackAdjustments;

  return {
    requestedAt: nowIso(),
    engineVersion: getFakeDropSettingsView().engineVersion,
    kpis: {
      echterDealCount: counts.echter_deal,
      suspiciousCount: counts.verdaechtig,
      fakeDropCount: counts.wahrscheinlicher_fake_drop,
      amazonStableCount: counts.amazon_stabil,
      openReviewCount: parseInteger(reviewStats.open_count, 0),
      unlabeledCount: parseInteger(reviewStats.unlabeled_count, 0),
      totalScored: Object.values(counts).reduce((sum, value) => sum + value, 0)
    },
    distribution: Object.entries(CLASSIFICATION_LABELS).map(([id, label]) => ({
      id,
      label,
      count: counts[id] || 0
    })),
    patternBreakdown: buildPatternBreakdown(),
    recentReviews,
    recentDetections,
    feedback
  };
}

export function getFakeDropHistory(filters = {}) {
  const days = clamp(parseInteger(filters.days, 30), 7, 90);
  const endDate = getLocalDayStart(new Date());
  const startDate = getLocalDayStart(shiftLocalDays(endDate, -(days - 1)));
  const rows = db
    .prepare(
      `
        SELECT
          DATE(updated_at) AS date_key,
          SUM(CASE WHEN classification IN ('echter_deal', 'amazon_stabil') THEN 1 ELSE 0 END) AS clean_count,
          SUM(CASE WHEN classification IN ('verdaechtig', 'manuelle_pruefung') THEN 1 ELSE 0 END) AS suspicious_count,
          SUM(CASE WHEN classification = 'wahrscheinlicher_fake_drop' THEN 1 ELSE 0 END) AS fake_count
        FROM keepa_fake_drop_scores
        WHERE updated_at >= ?
        GROUP BY DATE(updated_at)
      `
    )
    .all(startDate.toISOString());
  const reviewRows = db
    .prepare(
      `
        SELECT
          DATE(created_at) AS date_key,
          COUNT(*) AS review_count
        FROM keepa_review_labels
        WHERE created_at >= ?
        GROUP BY DATE(created_at)
      `
    )
    .all(startDate.toISOString());
  const sellerBreakdown = db
    .prepare(
      `
        SELECT seller_type, COUNT(*) AS count
        FROM keepa_fake_drop_scores
        GROUP BY seller_type
        ORDER BY count DESC
      `
    )
    .all()
    .map((row) => ({
      id: normalizeSellerType(row.seller_type),
      label: normalizeSellerType(row.seller_type),
      count: parseInteger(row.count, 0)
    }));
  const rowMap = new Map(rows.map((row) => [row.date_key, row]));
  const reviewMap = new Map(reviewRows.map((row) => [row.date_key, row]));
  const series = [];

  for (let index = 0; index < days; index += 1) {
    const date = shiftLocalDays(startDate, index);
    const key = toLocalDateKey(date);
    const row = rowMap.get(key) || {};
    const reviewRow = reviewMap.get(key) || {};

    series.push({
      date: key,
      label: new Intl.DateTimeFormat('de-DE', { day: '2-digit', month: '2-digit' }).format(date),
      cleanCount: parseInteger(row.clean_count, 0),
      suspiciousCount: parseInteger(row.suspicious_count, 0),
      fakeCount: parseInteger(row.fake_count, 0),
      reviewCount: parseInteger(reviewRow.review_count, 0)
    });
  }

  return {
    range: {
      days
    },
    series,
    sellerBreakdown,
    patternBreakdown: buildPatternBreakdown(200)
  };
}

function getReviewLabelPolarity(label) {
  const normalized = normalizeReviewLabel(label);

  if (['approved', 'strong_deal', 'ja'].includes(normalized)) {
    return 'positive';
  }

  if (['rejected', 'fake_drop', 'weak_deal', 'nein'].includes(normalized)) {
    return 'negative';
  }

  return 'uncertain';
}

function normalizeSimilarityFeatures(value) {
  const source = value && typeof value === 'object' ? value : {};

  return {
    volatilityPct: parseNumber(source.volatilityPct, null),
    referenceGapPct: parseNumber(source.referenceGapPct, null),
    stableTailDays: parseNumber(source.stableTailDays, null),
    historySpanDays: parseNumber(source.historySpanDays, null),
    distanceToHistoricalLowPct: parseNumber(source.distanceToHistoricalLowPct, null),
    spikeCount: parseInteger(source.spikeCount, 0),
    reboundCount: parseInteger(source.reboundCount, 0),
    zigZagCount: parseInteger(source.zigZagCount, 0),
    offerSwitchCount: parseInteger(source.offerSwitchCount, 0),
    historySparse: parseBool(source.historySparse, false)
  };
}

function buildSimilarityReference(input = {}) {
  return {
    reviewItemId: parseInteger(input.reviewItemId ?? input.id, 0),
    keepaResultId: parseInteger(input.keepaResultId, 0),
    asin: cleanText(input.asin).toUpperCase(),
    sellerType: normalizeSellerType(input.sellerType),
    categoryName: cleanText(input.categoryName),
    sourceType: normalizeLearningSource(input.sourceType || input.origin),
    currentPrice: parseNumber(input.currentPrice, null),
    keepaDiscount: parseNumber(input.keepaDiscount, null),
    fakeDropRisk: parseNumber(input.fakeDropRisk, null),
    classification: normalizeClassification(input.classification),
    features: normalizeSimilarityFeatures(input.features)
  };
}

function buildSimilarityCandidate(row) {
  const features = normalizeSimilarityFeatures(fromJson(row.feature_json, {}));
  const label = normalizeReviewLabel(row.current_label);

  return {
    reviewItemId: parseInteger(row.review_item_id, 0),
    keepaResultId: parseInteger(row.keepa_result_id, 0),
    asin: cleanText(row.asin).toUpperCase(),
    title: row.title || '',
    sellerType: normalizeSellerType(row.seller_type),
    categoryName: cleanText(row.category_name),
    sourceType: normalizeLearningSource(row.origin),
    sourceLabel: getLearningSourceLabel(row.origin),
    currentPrice: parseNumber(row.current_price, null),
    keepaDiscount: parseNumber(row.keepa_discount, null),
    fakeDropRisk: parseNumber(row.fake_drop_risk, null),
    classification: normalizeClassification(row.classification),
    label,
    labelLabel: REVIEW_LABEL_CATALOG.find((item) => item.id === label)?.label || label,
    note: row.note || '',
    tags: fromJson(row.tags_json, []),
    lastReviewedAt: row.last_reviewed_at || null,
    features
  };
}

function measureNumericDistance(left, right, weight, cap) {
  if (left === null || right === null) {
    return 0;
  }

  return Math.min(cap, Math.abs(left - right) * weight);
}

function measureRelativeDistance(left, right, weight, cap) {
  if (left === null || right === null) {
    return 0;
  }

  const reference = Math.max(Math.abs(left), Math.abs(right), 1);
  return Math.min(cap, (Math.abs(left - right) / reference) * 100 * weight);
}

function computeSimilarityScore(reference, candidate) {
  let score = 100;

  score -= measureRelativeDistance(reference.currentPrice, candidate.currentPrice, 0.45, 24);
  score -= measureNumericDistance(reference.keepaDiscount, candidate.keepaDiscount, 0.8, 18);
  score -= measureNumericDistance(reference.fakeDropRisk, candidate.fakeDropRisk, 0.45, 22);
  score -= measureNumericDistance(reference.features.volatilityPct, candidate.features.volatilityPct, 0.35, 12);
  score -= measureNumericDistance(reference.features.referenceGapPct, candidate.features.referenceGapPct, 0.3, 10);
  score -= measureNumericDistance(reference.features.distanceToHistoricalLowPct, candidate.features.distanceToHistoricalLowPct, 0.2, 10);
  score -= measureNumericDistance(reference.features.stableTailDays, candidate.features.stableTailDays, 0.35, 9);
  score -= measureNumericDistance(reference.features.historySpanDays, candidate.features.historySpanDays, 0.08, 8);
  score -= measureNumericDistance(reference.features.spikeCount, candidate.features.spikeCount, 7, 10);
  score -= measureNumericDistance(reference.features.reboundCount, candidate.features.reboundCount, 8, 10);
  score -= measureNumericDistance(reference.features.zigZagCount, candidate.features.zigZagCount, 3.5, 8);
  score -= measureNumericDistance(reference.features.offerSwitchCount, candidate.features.offerSwitchCount, 3.5, 8);

  if (reference.classification === candidate.classification) {
    score += 6;
  }

  if (reference.categoryName && candidate.categoryName && reference.categoryName === candidate.categoryName) {
    score += 4;
  }

  if (reference.sourceType === candidate.sourceType) {
    score += 2;
  }

  if (reference.features.historySparse === candidate.features.historySparse) {
    score += 2;
  }

  return Math.round(clamp(score, 0, 100));
}

function buildSimilarCaseSummary(cases = []) {
  const summary = cases.reduce(
    (accumulator, item) => {
      const bucket = getReviewLabelPolarity(item.label);
      accumulator[`${bucket}Count`] += 1;
      accumulator.total += 1;
      accumulator.labelCounts[item.label] = (accumulator.labelCounts[item.label] || 0) + 1;
      return accumulator;
    },
    {
      total: 0,
      positiveCount: 0,
      negativeCount: 0,
      uncertainCount: 0,
      labelCounts: {}
    }
  );

  const dominantLabel =
    Object.entries(summary.labelCounts).sort((left, right) => right[1] - left[1])[0]?.[0] || null;
  const rawRiskAdjustment =
    summary.total >= 2 ? ((summary.negativeCount - summary.positiveCount) / Math.max(summary.total, 1)) * 12 : 0;
  const riskAdjustment = Math.round(clamp(rawRiskAdjustment, -10, 10) * 10) / 10;
  const scoreAdjustment = Math.round(clamp(-riskAdjustment * 0.9, -12, 12) * 10) / 10;

  return {
    ...summary,
    dominantLabel,
    dominantLabelLabel: dominantLabel
      ? REVIEW_LABEL_CATALOG.find((item) => item.id === dominantLabel)?.label || dominantLabel
      : null,
    riskAdjustment,
    scoreAdjustment
  };
}

export function getSimilarCaseSignals(input = {}, options = {}) {
  const reference = buildSimilarityReference(input);
  if (!reference.sellerType || reference.sellerType === 'UNKNOWN') {
    return {
      sellerType: 'UNKNOWN',
      consideredCount: 0,
      matchedCount: 0,
      cases: [],
      summary: buildSimilarCaseSummary([])
    };
  }

  const minSimilarityScore = clamp(parseInteger(options.minSimilarityScore, 56), 30, 95);
  const limit = clamp(parseInteger(options.limit, 4), 1, 8);
  const scanLimit = clamp(parseInteger(options.scanLimit, 60), 8, 120);
  const rows = db
    .prepare(
      `
        SELECT
          kri.id AS review_item_id,
          kri.keepa_result_id,
          kri.current_label,
          kri.tags_json,
          kri.note,
          kri.last_reviewed_at,
          kr.asin,
          kr.title,
          kr.seller_type,
          kr.category_name,
          kr.current_price,
          kr.keepa_discount,
          kr.origin,
          kfds.fake_drop_risk,
          kfds.classification,
          kfs.feature_json
        FROM keepa_review_items kri
        JOIN keepa_results kr ON kr.id = kri.keepa_result_id
        LEFT JOIN keepa_fake_drop_scores kfds ON kfds.keepa_result_id = kri.keepa_result_id
        LEFT JOIN keepa_feature_snapshots kfs ON kfs.keepa_result_id = kri.keepa_result_id
        WHERE kr.seller_type = @sellerType
          AND kri.current_label IS NOT NULL
          AND kri.current_label != ''
          AND (@excludeReviewItemId = 0 OR kri.id != @excludeReviewItemId)
        ORDER BY COALESCE(kri.last_reviewed_at, kri.updated_at, kri.created_at) DESC
        LIMIT @scanLimit
      `
    )
    .all({
      sellerType: reference.sellerType,
      excludeReviewItemId: reference.reviewItemId || 0,
      scanLimit
    });
  const similarCases = rows
    .map((row) => {
      const candidate = buildSimilarityCandidate(row);
      return {
        ...candidate,
        similarityScore: computeSimilarityScore(reference, candidate)
      };
    })
    .filter((item) => item.similarityScore >= minSimilarityScore)
    .sort(
      (left, right) =>
        right.similarityScore - left.similarityScore || String(right.lastReviewedAt || '').localeCompare(String(left.lastReviewedAt || ''))
    )
    .slice(0, limit);
  const summary = buildSimilarCaseSummary(similarCases);

  if (options.skipLog !== true) {
    logGeneratorDebug('SIMILAR CASE CHECKED', {
      sellerType: reference.sellerType,
      sourceType: reference.sourceType,
      referenceAsin: reference.asin,
      consideredCount: rows.length,
      matchedCount: similarCases.length,
      positiveCount: summary.positiveCount,
      negativeCount: summary.negativeCount,
      uncertainCount: summary.uncertainCount
    });
    logGeneratorDebug('SIMILAR CASES CHECKED', {
      sellerType: reference.sellerType,
      sourceType: reference.sourceType,
      referenceAsin: reference.asin,
      consideredCount: rows.length,
      matchedCount: similarCases.length,
      positiveCount: summary.positiveCount,
      negativeCount: summary.negativeCount,
      uncertainCount: summary.uncertainCount
    });
  }

  return {
    sellerType: reference.sellerType,
    consideredCount: rows.length,
    matchedCount: similarCases.length,
    cases: similarCases,
    summary
  };
}

function buildReviewQueueWhere(filters) {
  const clauses = [];
  const params = {};

  if (filters.onlyOpen !== false) {
    clauses.push(`kri.review_status = 'open'`);
  }

  if (filters.onlyUnlabeled) {
    clauses.push(`(kri.current_label IS NULL OR kri.current_label = '')`);
  }

  if (filters.sellerType && filters.sellerType !== 'ALL') {
    clauses.push(`kr.seller_type = @sellerType`);
    params.sellerType = normalizeSellerType(filters.sellerType);
  }

  if (filters.classification) {
    const classification = normalizeClassification(filters.classification);
    clauses.push(`kri.classification = @classification`);
    params.classification = classification;
  }

  return {
    whereSql: clauses.length ? `WHERE ${clauses.join(' AND ')}` : '',
    params
  };
}

function mapReviewQueueItem(row) {
  const fakeDrop = mapFakeDropSnapshot(
    {
      id: row.score_id,
      classification: row.classification,
      stability_score: row.stability_score,
      manipulation_score: row.manipulation_score,
      trust_score: row.trust_score,
      amazon_confidence: row.amazon_confidence,
      fake_drop_risk: row.fake_drop_risk,
      review_priority: row.review_priority,
      reasoning_json: row.reasoning_json,
      engine_version: row.engine_version
    },
    {
      feature_json: row.feature_json,
      chart_points_json: row.chart_points_json
    },
    {
      id: row.review_item_id,
      review_status: row.review_status,
      current_label: row.current_label,
      tags_json: row.tags_json,
      note: row.note,
      label_count: row.label_count,
      last_reviewed_at: row.last_reviewed_at,
      analysis_reason: row.analysis_reason,
      chart_snapshot_json: row.chart_snapshot_json
    }
  );
  const similarCaseSignals = getSimilarCaseSignals(
    {
      reviewItemId: row.review_item_id,
      keepaResultId: row.keepa_result_id,
      asin: row.asin,
      sellerType: row.seller_type,
      categoryName: row.category_name,
      sourceType: row.origin,
      currentPrice: row.current_price,
      keepaDiscount: row.keepa_discount,
      fakeDropRisk: fakeDrop?.fakeDropRisk ?? row.fake_drop_risk,
      classification: fakeDrop?.classification || row.classification,
      features: fakeDrop?.features || fromJson(row.feature_json, {})
    },
    {
      limit: 3,
      minSimilarityScore: 58,
      scanLimit: 48,
      skipLog: true
    }
  );

  return {
    id: row.review_item_id,
    keepaResultId: row.keepa_result_id,
    asin: row.asin,
    title: row.title,
    imageUrl: row.image_url,
    sellerType: normalizeSellerType(row.seller_type),
    sourceType: normalizeLearningSource(row.origin),
    sourceLabel: getLearningSourceLabel(row.origin),
    categoryName: row.category_name,
    currentPrice: parseNumber(row.current_price, null),
    keepaDiscount: parseNumber(row.keepa_discount, null),
    dealScore: parseNumber(row.deal_score, null),
    fakeDrop,
    similarCaseSummary: similarCaseSignals.summary,
    similarCases: similarCaseSignals.cases,
    lastLabel: mapReviewLabel({
      id: row.last_label_id,
      label: row.last_label,
      tags_json: row.last_label_tags_json,
      note: row.last_label_note,
      created_at: row.last_label_created_at
    })
  };
}

function getMappedReviewQueueItemById(id) {
  const row = db
    .prepare(
      `
        SELECT
          kri.id AS review_item_id,
          kri.keepa_result_id,
          kri.review_status,
          kri.current_label,
          kri.tags_json,
          kri.note,
          kri.label_count,
          kri.last_reviewed_at,
          kri.analysis_reason,
          kri.chart_snapshot_json,
          kr.asin,
          kr.title,
          kr.image_url,
          kr.seller_type,
          kr.category_name,
          kr.origin,
          kr.current_price,
          kr.keepa_discount,
          kr.deal_score,
          kfds.id AS score_id,
          kfds.classification,
          kfds.stability_score,
          kfds.manipulation_score,
          kfds.trust_score,
          kfds.amazon_confidence,
          kfds.fake_drop_risk,
          kfds.review_priority,
          kfds.reasoning_json,
          kfds.engine_version,
          kfs.feature_json,
          kfs.chart_points_json,
          last_label.id AS last_label_id,
          last_label.label AS last_label,
          last_label.tags_json AS last_label_tags_json,
          last_label.note AS last_label_note,
          last_label.created_at AS last_label_created_at
        FROM keepa_review_items kri
        JOIN keepa_results kr ON kr.id = kri.keepa_result_id
        LEFT JOIN keepa_fake_drop_scores kfds ON kfds.keepa_result_id = kri.keepa_result_id
        LEFT JOIN keepa_feature_snapshots kfs ON kfs.keepa_result_id = kri.keepa_result_id
        LEFT JOIN keepa_review_labels last_label ON last_label.id = (
          SELECT rl.id
          FROM keepa_review_labels rl
          WHERE rl.review_item_id = kri.id
          ORDER BY rl.created_at DESC
          LIMIT 1
        )
        WHERE kri.id = ?
        LIMIT 1
      `
    )
    .get(id);

  return row ? mapReviewQueueItem(row) : null;
}

export function listFakeDropReviewQueue(filters = {}) {
  const page = clamp(parseInteger(filters.page, 1), 1, 50);
  const limit = clamp(parseInteger(filters.limit, 12), 1, 40);
  const offset = (page - 1) * limit;
  const normalizedFilters = {
    sellerType: cleanText(filters.sellerType || 'ALL'),
    classification: cleanText(filters.classification),
    onlyUnlabeled: parseBool(filters.onlyUnlabeled, false),
    onlyOpen: filters.onlyOpen === undefined ? true : parseBool(filters.onlyOpen)
  };
  const { whereSql, params } = buildReviewQueueWhere(normalizedFilters);
  const total =
    db.prepare(
      `
        SELECT COUNT(*) AS count
        FROM keepa_review_items kri
        JOIN keepa_results kr ON kr.id = kri.keepa_result_id
        ${whereSql}
      `
    ).get(params)?.count || 0;
  const rows = db
    .prepare(
      `
        SELECT
          kri.id AS review_item_id,
          kri.keepa_result_id,
          kri.review_status,
          kri.current_label,
          kri.tags_json,
          kri.note,
          kri.label_count,
          kri.last_reviewed_at,
          kri.analysis_reason,
          kri.chart_snapshot_json,
          kr.asin,
          kr.title,
          kr.image_url,
          kr.seller_type,
          kr.category_name,
          kr.origin,
          kr.current_price,
          kr.keepa_discount,
          kr.deal_score,
          kfds.id AS score_id,
          kfds.classification,
          kfds.stability_score,
          kfds.manipulation_score,
          kfds.trust_score,
          kfds.amazon_confidence,
          kfds.fake_drop_risk,
          kfds.review_priority,
          kfds.reasoning_json,
          kfds.engine_version,
          kfs.feature_json,
          kfs.chart_points_json,
          last_label.id AS last_label_id,
          last_label.label AS last_label,
          last_label.tags_json AS last_label_tags_json,
          last_label.note AS last_label_note,
          last_label.created_at AS last_label_created_at
        FROM keepa_review_items kri
        JOIN keepa_results kr ON kr.id = kri.keepa_result_id
        LEFT JOIN keepa_fake_drop_scores kfds ON kfds.keepa_result_id = kri.keepa_result_id
        LEFT JOIN keepa_feature_snapshots kfs ON kfs.keepa_result_id = kri.keepa_result_id
        LEFT JOIN keepa_review_labels last_label ON last_label.id = (
          SELECT rl.id
          FROM keepa_review_labels rl
          WHERE rl.review_item_id = kri.id
          ORDER BY rl.created_at DESC
          LIMIT 1
        )
        ${whereSql}
        ORDER BY kri.review_priority DESC, kri.updated_at DESC
        LIMIT @limit OFFSET @offset
      `
    )
    .all({
      ...params,
      limit,
      offset
    });

  return {
    items: rows.map(mapReviewQueueItem),
    pagination: {
      page,
      limit,
      total,
      totalPages: Math.max(1, Math.ceil(total / limit))
    },
    filters: normalizedFilters
  };
}

function mapExampleBucket(label, explicitBucket = '') {
  if (explicitBucket && Object.prototype.hasOwnProperty.call(EXAMPLE_BUCKET_LABELS, explicitBucket)) {
    return explicitBucket;
  }

  if (['ja', 'approved', 'strong_deal'].includes(label)) {
    return 'positive';
  }

  if (['nein', 'rejected', 'fake_drop', 'weak_deal'].includes(label)) {
    return 'negative';
  }

  return 'unsicher';
}

function buildExampleSnapshot(reviewItem, resultRow, fakeDropSnapshot, label, tags, note) {
  const keepaPayload = fromJson(resultRow?.keepa_payload_json, {});
  const history = keepaPayload?.history && typeof keepaPayload.history === 'object' ? keepaPayload.history : {};

  return {
    asin: resultRow.asin,
    title: resultRow.title,
    productUrl: resultRow.product_url || '',
    imageUrl: resultRow.image_url || '',
    sellerType: resultRow.seller_type,
    sourceType: normalizeLearningSource(resultRow.origin),
    sourceLabel: getLearningSourceLabel(resultRow.origin),
    categoryName: resultRow.category_name,
    currentPrice: parseNumber(resultRow.current_price, null),
    referencePrice: parseNumber(resultRow.reference_price, null),
    keepaDiscount: parseNumber(resultRow.keepa_discount, null),
    priceHistory: Array.isArray(history.priceSeries) ? history.priceSeries : [],
    chartPoints: Array.isArray(fakeDropSnapshot?.chartPoints)
      ? fakeDropSnapshot.chartPoints
      : Array.isArray(history.chartPoints)
        ? history.chartPoints
        : [],
    label,
    tags,
    note,
    reviewedAt: nowIso(),
    fakeDrop: fakeDropSnapshot,
    reviewItem: {
      id: reviewItem.id,
      analysisReason: reviewItem.analysis_reason
    }
  };
}

function getReviewItemJoined(id) {
  return db
    .prepare(
      `
        SELECT
          kri.*,
          kr.title,
          kr.image_url,
          kr.product_url,
          kr.current_price,
          kr.reference_price,
          kr.keepa_discount,
          kr.deal_score,
          kr.seller_type,
          kr.category_name,
          kr.asin,
          kr.origin,
          kr.keepa_payload_json
        FROM keepa_review_items kri
        JOIN keepa_results kr ON kr.id = kri.keepa_result_id
        WHERE kri.id = ?
        LIMIT 1
      `
    )
    .get(id);
}

export function submitFakeDropReview(id, input = {}) {
  const reviewItem = getReviewItemJoined(id);
  if (!reviewItem) {
    throw new Error('Review-Eintrag wurde nicht gefunden.');
  }

  const label = normalizeReviewLabel(input.label);
  const tags = normalizeTags(input.tags);
  const note = cleanText(input.note).slice(0, 1500);
  const saveAsExample = parseBool(input.saveAsExample, false);
  const exampleBucket = mapExampleBucket(label, cleanText(input.exampleBucket).toLowerCase());
  const timestamp = nowIso();

  db.prepare(
    `
      INSERT INTO keepa_review_labels (
        review_item_id,
        keepa_result_id,
        asin,
        seller_type,
        label,
        tags_json,
        note,
        engine_version,
        created_at
      ) VALUES (
        @reviewItemId,
        @keepaResultId,
        @asin,
        @sellerType,
        @label,
        @tagsJson,
        @note,
        @engineVersion,
        @createdAt
      )
    `
  ).run({
    reviewItemId: reviewItem.id,
    keepaResultId: reviewItem.keepa_result_id,
    asin: reviewItem.asin,
    sellerType: reviewItem.seller_type,
    label,
    tagsJson: toJson(tags),
    note,
    engineVersion: ENGINE_VERSION,
    createdAt: timestamp
  });

  db.prepare(
    `
      UPDATE keepa_review_items
      SET review_status = @reviewStatus,
          current_label = @currentLabel,
          tags_json = @tagsJson,
          note = @note,
          example_bucket = CASE WHEN @saveAsExample = 1 THEN @exampleBucket ELSE example_bucket END,
          label_count = COALESCE(label_count, 0) + 1,
          last_reviewed_at = @lastReviewedAt,
          updated_at = @updatedAt
      WHERE id = @id
    `
  ).run({
    id: reviewItem.id,
    reviewStatus: label === 'ueberspringen' ? 'skipped' : 'reviewed',
    currentLabel: label,
    tagsJson: toJson(tags),
    note,
    saveAsExample: saveAsExample ? 1 : 0,
    exampleBucket,
    lastReviewedAt: timestamp,
    updatedAt: timestamp
  });

  if (saveAsExample) {
    const resultRow = db.prepare(`SELECT * FROM keepa_results WHERE id = ?`).get(reviewItem.keepa_result_id);
    const fakeDropSnapshot = getFakeDropSnapshotForResult(reviewItem.keepa_result_id);
    db.prepare(
      `
        INSERT INTO keepa_example_library (
          review_item_id,
          keepa_result_id,
          asin,
          seller_type,
          category_name,
          bucket,
          label,
          tags_json,
          note,
          snapshot_json,
          created_at
        ) VALUES (
          @reviewItemId,
          @keepaResultId,
          @asin,
          @sellerType,
          @categoryName,
          @bucket,
          @label,
          @tagsJson,
          @note,
          @snapshotJson,
          @createdAt
        )
      `
    ).run({
      reviewItemId: reviewItem.id,
      keepaResultId: reviewItem.keepa_result_id,
      asin: reviewItem.asin,
      sellerType: reviewItem.seller_type,
      categoryName: reviewItem.category_name,
      bucket: exampleBucket,
      label,
      tagsJson: toJson(tags),
      note,
      snapshotJson: toJson(buildExampleSnapshot(reviewItem, resultRow, fakeDropSnapshot, label, tags, note)),
      createdAt: timestamp
    });

    logGeneratorDebug('LEARNING CASE STORED', {
      reviewItemId: reviewItem.id,
      keepaResultId: reviewItem.keepa_result_id,
      asin: reviewItem.asin,
      sellerType: reviewItem.seller_type,
      label,
      bucket: exampleBucket
    });
  }

  logGeneratorDebug('USER RATING APPLIED', {
    reviewItemId: reviewItem.id,
    keepaResultId: reviewItem.keepa_result_id,
    asin: reviewItem.asin,
    sellerType: reviewItem.seller_type,
    label,
    tags,
    saveAsExample
  });
  logGeneratorDebug('FEEDBACK LABEL SAVED', {
    reviewItemId: reviewItem.id,
    keepaResultId: reviewItem.keepa_result_id,
    asin: reviewItem.asin,
    sellerType: reviewItem.seller_type,
    label,
    tags,
    saveAsExample
  });
  logGeneratorDebug('LEARNING FEEDBACK SAVED', {
    reviewItemId: reviewItem.id,
    keepaResultId: reviewItem.keepa_result_id,
    asin: reviewItem.asin,
    sellerType: reviewItem.seller_type,
    label,
    sourceType: normalizeLearningSource(reviewItem.origin),
    saveAsExample
  });

  return getMappedReviewQueueItemById(id);
}

function buildExamplesWhere(filters) {
  const clauses = [];
  const params = {};

  if (filters.bucket && Object.prototype.hasOwnProperty.call(EXAMPLE_BUCKET_LABELS, filters.bucket)) {
    clauses.push(`kel.bucket = @bucket`);
    params.bucket = filters.bucket;
  }

  if (filters.label) {
    clauses.push(`kel.label = @label`);
    params.label = normalizeReviewLabel(filters.label);
  }

  if (filters.sellerType && filters.sellerType !== 'ALL') {
    clauses.push(`kel.seller_type = @sellerType`);
    params.sellerType = normalizeSellerType(filters.sellerType);
  }

  if (filters.search) {
    clauses.push(`(kr.title LIKE @search OR kel.asin LIKE @search OR COALESCE(kr.category_name, '') LIKE @search)`);
    params.search = `%${cleanText(filters.search).slice(0, 60)}%`;
  }

  return {
    whereSql: clauses.length ? `WHERE ${clauses.join(' AND ')}` : '',
    params
  };
}

function findSimilarExamples(referenceInput = {}) {
  return getSimilarCaseSignals(referenceInput, {
    limit: 3,
    minSimilarityScore: 58,
    scanLimit: 48,
    skipLog: true
  }).cases.map((row) => ({
    id: row.reviewItemId,
    asin: row.asin,
    title: row.title,
    label: row.label,
    labelLabel: row.labelLabel,
    categoryName: row.categoryName,
    sourceLabel: row.sourceLabel,
    fakeDropRisk: row.fakeDropRisk === null ? null : Math.round(row.fakeDropRisk * 10) / 10,
    classification: row.classification,
    classificationLabel: CLASSIFICATION_LABELS[normalizeClassification(row.classification)],
    similarityScore: row.similarityScore
  }));
}

export function listFakeDropExamples(filters = {}) {
  const page = clamp(parseInteger(filters.page, 1), 1, 50);
  const limit = clamp(parseInteger(filters.limit, 12), 1, 40);
  const offset = (page - 1) * limit;
  const normalizedFilters = {
    bucket: cleanText(filters.bucket).toLowerCase(),
    label: cleanText(filters.label).toLowerCase(),
    sellerType: cleanText(filters.sellerType || 'ALL'),
    search: cleanText(filters.search)
  };
  const { whereSql, params } = buildExamplesWhere(normalizedFilters);
  const total =
    db.prepare(
      `
        SELECT COUNT(*) AS count
        FROM keepa_example_library kel
        JOIN keepa_results kr ON kr.id = kel.keepa_result_id
        ${whereSql}
      `
    ).get(params)?.count || 0;
  const rows = db
    .prepare(
      `
        SELECT
          kel.*,
          kr.title,
          kr.image_url,
          kr.current_price,
          kr.keepa_discount,
          kr.origin,
          kfds.classification,
          kfds.fake_drop_risk,
          kfds.stability_score,
          kfs.chart_points_json
        FROM keepa_example_library kel
        JOIN keepa_results kr ON kr.id = kel.keepa_result_id
        LEFT JOIN keepa_fake_drop_scores kfds ON kfds.keepa_result_id = kel.keepa_result_id
        LEFT JOIN keepa_feature_snapshots kfs ON kfs.keepa_result_id = kel.keepa_result_id
        ${whereSql}
        ORDER BY kel.created_at DESC
        LIMIT @limit OFFSET @offset
      `
    )
    .all({
      ...params,
      limit,
      offset
    });
  const counts = db
    .prepare(
      `
        SELECT bucket, COUNT(*) AS count
        FROM keepa_example_library
        GROUP BY bucket
      `
    )
    .all()
    .reduce(
      (accumulator, row) => ({
        ...accumulator,
        [row.bucket]: parseInteger(row.count, 0)
      }),
      { positive: 0, negative: 0, unsicher: 0 }
    );

  return {
    items: rows.map((row) => {
      const snapshot = fromJson(row.snapshot_json, {});
      const similarCases = findSimilarExamples({
        reviewItemId: row.review_item_id,
        keepaResultId: row.keepa_result_id,
        asin: row.asin,
        sellerType: row.seller_type,
        categoryName: row.category_name,
        sourceType: row.origin || snapshot.sourceType,
        currentPrice: row.current_price,
        keepaDiscount: row.keepa_discount,
        fakeDropRisk: row.fake_drop_risk,
        classification: row.classification,
        features: snapshot.fakeDrop?.features || {}
      });

      return {
        id: row.id,
        reviewItemId: row.review_item_id,
        keepaResultId: row.keepa_result_id,
        asin: row.asin,
        title: row.title,
        imageUrl: row.image_url,
        sellerType: normalizeSellerType(row.seller_type),
        sourceType: normalizeLearningSource(row.origin || snapshot.sourceType),
        sourceLabel: getLearningSourceLabel(row.origin || snapshot.sourceType),
        categoryName: row.category_name,
        bucket: row.bucket,
        bucketLabel: EXAMPLE_BUCKET_LABELS[row.bucket] || row.bucket,
        label: normalizeReviewLabel(row.label),
        labelLabel: REVIEW_LABEL_CATALOG.find((item) => item.id === normalizeReviewLabel(row.label))?.label || row.label,
        tags: fromJson(row.tags_json, []),
        note: row.note || '',
        currentPrice: parseNumber(row.current_price, null),
        keepaDiscount: parseNumber(row.keepa_discount, null),
        classification: normalizeClassification(row.classification),
        classificationLabel: CLASSIFICATION_LABELS[normalizeClassification(row.classification)],
        fakeDropRisk: Math.round((parseNumber(row.fake_drop_risk, 0) || 0) * 10) / 10,
        stabilityScore: Math.round((parseNumber(row.stability_score, 0) || 0) * 10) / 10,
        chartPoints: fromJson(row.chart_points_json, []),
        createdAt: row.created_at,
        similarCases
      };
    }),
    pagination: {
      page,
      limit,
      total,
      totalPages: Math.max(1, Math.ceil(total / limit))
    },
    filters: normalizedFilters,
    counts
  };
}

export function recalculateFakeDropScores() {
  const rows = db.prepare(`SELECT * FROM keepa_results ORDER BY updated_at DESC`).all();
  const counts = {
    echter_deal: 0,
    verdaechtig: 0,
    wahrscheinlicher_fake_drop: 0,
    manuelle_pruefung: 0,
    amazon_stabil: 0
  };

  rows.forEach((row) => {
    const snapshot = persistFakeDropAnalysis(row, {
      recalculate: true
    });
    if (snapshot?.classification) {
      counts[snapshot.classification] = (counts[snapshot.classification] || 0) + 1;
    }
  });

  return {
    recalculatedAt: nowIso(),
    processedCount: rows.length,
    counts
  };
}

export function evaluateFakeDropAlertEligibility(resultInput, rule = null) {
  const result = normalizeResultInput(resultInput);
  const fakeDrop = result.id ? getFakeDropSnapshotForResult(result.id) : null;
  const settings = getFakeDropSettingsView();

  if (!fakeDrop) {
    return {
      allowed: false,
      reason: 'Keine Fake-Drop-Analyse vorhanden.',
      reviewQueue: true
    };
  }

  const minDealScore = clamp(parseNumber(rule?.minDealScore, result.dealScore), 0, 100);
  const enoughDealScore = result.dealScore >= minDealScore;
  const lowRisk = fakeDrop.fakeDropRisk <= settings.lowRiskThreshold;
  const strongAmazon = fakeDrop.amazonConfidence >= settings.amazonConfidenceStrong;

  if (enoughDealScore && (lowRisk || strongAmazon)) {
    return {
      allowed: true,
      reason: lowRisk ? 'Niedriges Fake-Drop-Risiko.' : 'Hohe Amazon-Confidence.'
    };
  }

  return {
    allowed: false,
    reason: !enoughDealScore
      ? 'Deal-Score unterschreitet die Alert-Schwelle.'
      : 'Treffer wurde wegen Fake-Drop-Risiko in die Review Queue verschoben.',
    reviewQueue: true,
    fakeDrop
  };
}
