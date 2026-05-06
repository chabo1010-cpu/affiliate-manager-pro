import { getDb } from '../db.js';
import {
  buildAmazonAffiliateLinkRecord,
  checkDealCooldown,
  classifySellerType,
  cleanText,
  extractAsin,
  isAmazonShortLink,
  normalizeAmazonLink,
  normalizeSellerType,
  parseNumber
} from './dealHistoryService.js';
import { syncImportedDealState } from './databaseService.js';
import { logGeneratorDebug } from './generatorFlowService.js';
import { loadKeepaClientByAsin } from './keepaClientService.js';
import {
  getKeepaDrawerControlConfig,
  getKeepaSettings,
  loadStoredInternetComparisonContext
} from './keepaService.js';
import { evaluateLearningRoute } from './learningLogicService.js';
import { enqueueCopybotPublishing } from './publisherService.js';
import { getCopybotRuntimeState, getCopybotStatusAudit } from './copybotControlService.js';

const db = getDb();

function nowIso() {
  return new Date().toISOString();
}

function parseBoolean(value, fallback = false) {
  if (value === undefined) {
    return fallback;
  }

  return value === true || value === 1 || value === '1';
}

function clamp(value, min, max, fallback) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }

  return Math.min(max, Math.max(min, parsed));
}

function parseJsonArray(value) {
  if (Array.isArray(value)) {
    return value.map((item) => cleanText(String(item))).filter(Boolean);
  }

  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (!trimmed) {
      return [];
    }

    try {
      const parsed = JSON.parse(trimmed);
      if (Array.isArray(parsed)) {
        return parsed.map((item) => cleanText(String(item))).filter(Boolean);
      }
    } catch {
      return trimmed
        .split(',')
        .map((item) => cleanText(item))
        .filter(Boolean);
    }
  }

  return [];
}

function stringifyJson(value) {
  return JSON.stringify(value ?? null);
}

function loadPricingRuleById(id) {
  return db.prepare(`SELECT * FROM pricing_rules WHERE id = ?`).get(id) || null;
}

function loadSamplingRuleById(id) {
  return db.prepare(`SELECT * FROM sampling_rules WHERE id = ?`).get(id) || null;
}

function loadSourceById(id) {
  return (
    db
      .prepare(
        `
          SELECT
            s.*,
            pr.name AS pricing_rule_name,
            sr.name AS sampling_rule_name
          FROM sources s
          JOIN pricing_rules pr ON pr.id = s.pricing_rule_id
          LEFT JOIN sampling_rules sr ON sr.id = s.sampling_rule_id
          WHERE s.id = ?
        `
      )
      .get(id) || null
  );
}

function getSellerTypeConfig(rule, sellerType) {
  if (sellerType === 'AMAZON') {
    return {
      allow: rule.allow_amazon === 1,
      minDiscount: Number(rule.min_discount_amazon ?? 0),
      minScore: Number(rule.min_score_amazon ?? 0),
      sampling: Number(rule.sampling_amazon ?? 100)
    };
  }

  if (sellerType === 'FBA') {
    return {
      allow: rule.allow_fba === 1,
      minDiscount: Number(rule.min_discount_fba ?? 0),
      minScore: Number(rule.min_score_fba ?? 0),
      sampling: Number(rule.sampling_fba ?? 100)
    };
  }

  return {
    allow: rule.allow_fbm === 1,
    minDiscount: Number(rule.min_discount_fbm ?? 0),
    minScore: Number(rule.min_score_fbm ?? 0),
    sampling: Number(rule.sampling_fbm ?? 100)
  };
}

function getSamplingThreshold(samplingRule, sellerType) {
  if (!samplingRule) {
    return 100;
  }

  if (sellerType === 'AMAZON') {
    return Number(samplingRule.sampling_amazon ?? samplingRule.default_sampling ?? 100);
  }

  if (sellerType === 'FBA') {
    return Number(samplingRule.sampling_fba ?? samplingRule.default_sampling ?? 100);
  }

  return Number(samplingRule.sampling_fbm ?? samplingRule.default_sampling ?? 100);
}

function getSourceDailyProcessedCount(sourceId) {
  const startOfDay = new Date();
  startOfDay.setHours(0, 0, 0, 0);

  const row = db
    .prepare(
      `
        SELECT COUNT(*) AS count
        FROM imported_deals
        WHERE source_id = ?
          AND created_at >= ?
      `
    )
    .get(sourceId, startOfDay.toISOString());

  return Number(row?.count ?? 0);
}

function createDeterministicSampleValue(seed) {
  let hash = 0;
  const input = cleanText(seed) || `${Date.now()}`;

  for (let index = 0; index < input.length; index += 1) {
    hash = (hash * 31 + input.charCodeAt(index)) % 100000;
  }

  return (hash % 10000) / 100;
}

function logEvent({ level = 'info', eventType, sourceId = null, importedDealId = null, message, payload = null }) {
  db.prepare(
    `
      INSERT INTO copybot_logs (
        level,
        event_type,
        source_id,
        imported_deal_id,
        message,
        payload_json,
        created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?)
    `
  ).run(level, eventType, sourceId, importedDealId, message, payload ? stringifyJson(payload) : null, nowIso());
}

async function resolveAmazonShortLink(value = '') {
  const trimmed = cleanText(value);
  if (!trimmed || !isAmazonShortLink(trimmed)) {
    return trimmed;
  }

  for (const method of ['HEAD', 'GET']) {
    try {
      const response = await fetch(trimmed, {
        method,
        redirect: 'follow'
      });

      if (response?.url) {
        return response.url;
      }
    } catch {
      // Fallback keeps the raw short URL if the redirect cannot be resolved.
    }
  }

  return trimmed;
}

function mapPricingRuleInput(input = {}) {
  return {
    name: cleanText(input.name),
    is_active: parseBoolean(input.is_active ?? input.isActive, true) ? 1 : 0,
    keepa_required: parseBoolean(input.keepa_required ?? input.keepaRequired, false) ? 1 : 0,
    idealo_required: parseBoolean(input.idealo_required ?? input.idealoRequired, false) ? 1 : 0,
    autopost_above_score: clamp(input.autopost_above_score ?? input.autopostAboveScore, 0, 100, 85),
    manual_review_below_score: clamp(input.manual_review_below_score ?? input.manualReviewBelowScore, 0, 100, 45),
    allow_amazon: parseBoolean(input.allow_amazon ?? input.allowAmazon, true) ? 1 : 0,
    min_discount_amazon: clamp(input.min_discount_amazon ?? input.minDiscountAmazon, 0, 1000, 15),
    min_score_amazon: clamp(input.min_score_amazon ?? input.minScoreAmazon, 0, 100, 70),
    sampling_amazon: clamp(input.sampling_amazon ?? input.samplingAmazon, 0, 100, 100),
    max_price_gap_idealo_amazon: input.max_price_gap_idealo_amazon ?? input.maxPriceGapIdealoAmazon ?? null,
    allow_fba: parseBoolean(input.allow_fba ?? input.allowFba, true) ? 1 : 0,
    min_discount_fba: clamp(input.min_discount_fba ?? input.minDiscountFba, 0, 1000, 20),
    min_score_fba: clamp(input.min_score_fba ?? input.minScoreFba, 0, 100, 75),
    sampling_fba: clamp(input.sampling_fba ?? input.samplingFba, 0, 100, 60),
    max_price_gap_idealo_fba: input.max_price_gap_idealo_fba ?? input.maxPriceGapIdealoFba ?? null,
    allow_fbm: parseBoolean(input.allow_fbm ?? input.allowFbm, true) ? 1 : 0,
    min_discount_fbm: clamp(input.min_discount_fbm ?? input.minDiscountFbm, 0, 1000, 40),
    min_score_fbm: clamp(input.min_score_fbm ?? input.minScoreFbm, 0, 100, 82),
    sampling_fbm: clamp(input.sampling_fbm ?? input.samplingFbm, 0, 100, 20),
    max_price_gap_idealo_fbm: input.max_price_gap_idealo_fbm ?? input.maxPriceGapIdealoFbm ?? null,
    fbm_requires_manual_review:
      parseBoolean(input.fbm_requires_manual_review ?? input.fbmRequiresManualReview, true) ? 1 : 0,
    min_seller_rating_fbm: input.min_seller_rating_fbm ?? input.minSellerRatingFbm ?? null,
    fake_drop_filter_enabled:
      parseBoolean(input.fake_drop_filter_enabled ?? input.fakeDropFilterEnabled, false) ? 1 : 0,
    coupon_only_penalty: clamp(input.coupon_only_penalty ?? input.couponOnlyPenalty, 0, 100, 0),
    variant_switch_penalty: clamp(input.variant_switch_penalty ?? input.variantSwitchPenalty, 0, 100, 0),
    marketplace_switch_penalty:
      clamp(input.marketplace_switch_penalty ?? input.marketplaceSwitchPenalty, 0, 100, 0),
    manual_blacklist_keywords: stringifyJson(
      parseJsonArray(input.manual_blacklist_keywords ?? input.manualBlacklistKeywords)
    ),
    manual_whitelist_brands: stringifyJson(
      parseJsonArray(input.manual_whitelist_brands ?? input.manualWhitelistBrands)
    )
  };
}

function mapSamplingRuleInput(input = {}) {
  return {
    name: cleanText(input.name),
    is_active: parseBoolean(input.is_active ?? input.isActive, true) ? 1 : 0,
    default_sampling: clamp(input.default_sampling ?? input.defaultSampling, 0, 100, 100),
    sampling_amazon: clamp(input.sampling_amazon ?? input.samplingAmazon, 0, 100, 100),
    sampling_fba: clamp(input.sampling_fba ?? input.samplingFba, 0, 100, 100),
    sampling_fbm: clamp(input.sampling_fbm ?? input.samplingFbm, 0, 100, 100),
    daily_limit:
      input.daily_limit === '' || input.daily_limit === undefined ? null : Number(input.daily_limit),
    min_score: input.min_score === '' || input.min_score === undefined ? null : Number(input.min_score),
    min_discount:
      input.min_discount === '' || input.min_discount === undefined ? null : Number(input.min_discount),
    notes: cleanText(input.notes)
  };
}

function mapSourceInput(input = {}) {
  return {
    name: cleanText(input.name),
    platform: cleanText(input.platform).toLowerCase() === 'whatsapp' ? 'whatsapp' : 'telegram',
    source_type: cleanText(input.source_type ?? input.sourceType) || 'manual',
    is_active: parseBoolean(input.is_active ?? input.isActive, true) ? 1 : 0,
    priority: Number.isFinite(Number(input.priority)) ? Number(input.priority) : 100,
    pricing_rule_id: Number(input.pricing_rule_id ?? input.pricingRuleId ?? 1),
    sampling_rule_id:
      input.sampling_rule_id === null || input.samplingRuleId === null
        ? null
        : Number(input.sampling_rule_id ?? input.samplingRuleId ?? 1),
    success_rate:
      input.success_rate === '' || input.success_rate === undefined ? null : Number(input.success_rate),
    notes: cleanText(input.notes)
  };
}

export function listPricingRules() {
  return db
    .prepare(`SELECT * FROM pricing_rules ORDER BY is_active DESC, name ASC`)
    .all()
    .map((row) => ({
      ...row,
      manual_blacklist_keywords: parseJsonArray(row.manual_blacklist_keywords),
      manual_whitelist_brands: parseJsonArray(row.manual_whitelist_brands)
    }));
}

export function savePricingRule(input = {}, id = null) {
  const payload = mapPricingRuleInput(input);
  if (!payload.name) {
    throw new Error('Name fuer Preispruef-Logik fehlt.');
  }

  const timestamp = nowIso();

  if (id) {
    db.prepare(
      `
        UPDATE pricing_rules
        SET name = @name,
            is_active = @is_active,
            keepa_required = @keepa_required,
            idealo_required = @idealo_required,
            autopost_above_score = @autopost_above_score,
            manual_review_below_score = @manual_review_below_score,
            allow_amazon = @allow_amazon,
            min_discount_amazon = @min_discount_amazon,
            min_score_amazon = @min_score_amazon,
            sampling_amazon = @sampling_amazon,
            max_price_gap_idealo_amazon = @max_price_gap_idealo_amazon,
            allow_fba = @allow_fba,
            min_discount_fba = @min_discount_fba,
            min_score_fba = @min_score_fba,
            sampling_fba = @sampling_fba,
            max_price_gap_idealo_fba = @max_price_gap_idealo_fba,
            allow_fbm = @allow_fbm,
            min_discount_fbm = @min_discount_fbm,
            min_score_fbm = @min_score_fbm,
            sampling_fbm = @sampling_fbm,
            max_price_gap_idealo_fbm = @max_price_gap_idealo_fbm,
            fbm_requires_manual_review = @fbm_requires_manual_review,
            min_seller_rating_fbm = @min_seller_rating_fbm,
            fake_drop_filter_enabled = @fake_drop_filter_enabled,
            coupon_only_penalty = @coupon_only_penalty,
            variant_switch_penalty = @variant_switch_penalty,
            marketplace_switch_penalty = @marketplace_switch_penalty,
            manual_blacklist_keywords = @manual_blacklist_keywords,
            manual_whitelist_brands = @manual_whitelist_brands,
            updated_at = @updated_at
        WHERE id = @id
      `
    ).run({ ...payload, id, updated_at: timestamp });
  } else {
    db.prepare(
      `
        INSERT INTO pricing_rules (
          name, is_active, keepa_required, idealo_required, autopost_above_score, manual_review_below_score,
          allow_amazon, min_discount_amazon, min_score_amazon, sampling_amazon, max_price_gap_idealo_amazon,
          allow_fba, min_discount_fba, min_score_fba, sampling_fba, max_price_gap_idealo_fba,
          allow_fbm, min_discount_fbm, min_score_fbm, sampling_fbm, max_price_gap_idealo_fbm,
          fbm_requires_manual_review, min_seller_rating_fbm, fake_drop_filter_enabled,
          coupon_only_penalty, variant_switch_penalty, marketplace_switch_penalty,
          manual_blacklist_keywords, manual_whitelist_brands, created_at, updated_at
        ) VALUES (
          @name, @is_active, @keepa_required, @idealo_required, @autopost_above_score, @manual_review_below_score,
          @allow_amazon, @min_discount_amazon, @min_score_amazon, @sampling_amazon, @max_price_gap_idealo_amazon,
          @allow_fba, @min_discount_fba, @min_score_fba, @sampling_fba, @max_price_gap_idealo_fba,
          @allow_fbm, @min_discount_fbm, @min_score_fbm, @sampling_fbm, @max_price_gap_idealo_fbm,
          @fbm_requires_manual_review, @min_seller_rating_fbm, @fake_drop_filter_enabled,
          @coupon_only_penalty, @variant_switch_penalty, @marketplace_switch_penalty,
          @manual_blacklist_keywords, @manual_whitelist_brands, @created_at, @updated_at
        )
      `
    ).run({ ...payload, created_at: timestamp, updated_at: timestamp });
  }

  return listPricingRules();
}

export function listSamplingRules() {
  return db.prepare(`SELECT * FROM sampling_rules ORDER BY is_active DESC, name ASC`).all();
}

export function saveSamplingRule(input = {}, id = null) {
  const payload = mapSamplingRuleInput(input);
  if (!payload.name) {
    throw new Error('Name fuer Sampling-Regel fehlt.');
  }

  const timestamp = nowIso();

  if (id) {
    db.prepare(
      `
        UPDATE sampling_rules
        SET name = @name,
            is_active = @is_active,
            default_sampling = @default_sampling,
            sampling_amazon = @sampling_amazon,
            sampling_fba = @sampling_fba,
            sampling_fbm = @sampling_fbm,
            daily_limit = @daily_limit,
            min_score = @min_score,
            min_discount = @min_discount,
            notes = @notes,
            updated_at = @updated_at
        WHERE id = @id
      `
    ).run({ ...payload, id, updated_at: timestamp });
  } else {
    db.prepare(
      `
        INSERT INTO sampling_rules (
          name, is_active, default_sampling, sampling_amazon, sampling_fba, sampling_fbm,
          daily_limit, min_score, min_discount, notes, created_at, updated_at
        ) VALUES (
          @name, @is_active, @default_sampling, @sampling_amazon, @sampling_fba, @sampling_fbm,
          @daily_limit, @min_score, @min_discount, @notes, @created_at, @updated_at
        )
      `
    ).run({ ...payload, created_at: timestamp, updated_at: timestamp });
  }

  return listSamplingRules();
}

export function listSources(platform = null) {
  const rows = platform
    ? db
        .prepare(
          `
            SELECT
              s.*,
              pr.name AS pricing_rule_name,
              sr.name AS sampling_rule_name
            FROM sources s
            JOIN pricing_rules pr ON pr.id = s.pricing_rule_id
            LEFT JOIN sampling_rules sr ON sr.id = s.sampling_rule_id
            WHERE s.platform = ?
            ORDER BY s.is_active DESC, s.priority ASC, s.name ASC
          `
        )
        .all(platform)
    : db
        .prepare(
          `
            SELECT
              s.*,
              pr.name AS pricing_rule_name,
              sr.name AS sampling_rule_name
            FROM sources s
            JOIN pricing_rules pr ON pr.id = s.pricing_rule_id
            LEFT JOIN sampling_rules sr ON sr.id = s.sampling_rule_id
            ORDER BY s.platform ASC, s.is_active DESC, s.priority ASC, s.name ASC
          `
        )
        .all();

  return rows;
}

export function saveSource(input = {}, id = null) {
  const payload = mapSourceInput(input);
  if (!payload.name) {
    throw new Error('Name fuer Quelle fehlt.');
  }

  if (!loadPricingRuleById(payload.pricing_rule_id)) {
    throw new Error('Preispruef-Logik existiert nicht.');
  }

  if (payload.sampling_rule_id && !loadSamplingRuleById(payload.sampling_rule_id)) {
    throw new Error('Sampling-Regel existiert nicht.');
  }

  const timestamp = nowIso();

  if (id) {
    db.prepare(
      `
        UPDATE sources
        SET name = @name,
            platform = @platform,
            source_type = @source_type,
            is_active = @is_active,
            priority = @priority,
            pricing_rule_id = @pricing_rule_id,
            sampling_rule_id = @sampling_rule_id,
            success_rate = @success_rate,
            notes = @notes,
            updated_at = @updated_at
        WHERE id = @id
      `
    ).run({ ...payload, id, updated_at: timestamp });
  } else {
    db.prepare(
      `
        INSERT INTO sources (
          name, platform, source_type, is_active, priority, pricing_rule_id, sampling_rule_id,
          success_rate, notes, created_at, updated_at
        ) VALUES (
          @name, @platform, @source_type, @is_active, @priority, @pricing_rule_id, @sampling_rule_id,
          @success_rate, @notes, @created_at, @updated_at
        )
      `
    ).run({ ...payload, created_at: timestamp, updated_at: timestamp });
  }

  return listSources(payload.platform);
}

export function deleteSource(id) {
  db.prepare(`UPDATE sources SET is_active = 0, updated_at = ? WHERE id = ?`).run(nowIso(), id);
  return loadSourceById(id);
}

export function setSourceActiveState(id, isActive) {
  db.prepare(`UPDATE sources SET is_active = ?, updated_at = ? WHERE id = ?`).run(isActive ? 1 : 0, nowIso(), id);
  return loadSourceById(id);
}

function detectSellerType(input = {}) {
  if (cleanText(input.sellerType)) {
    return normalizeSellerType(input.sellerType);
  }

  return classifySellerType({
    soldByAmazon: parseBoolean(input.soldByAmazon, false),
    shippedByAmazon: parseBoolean(input.shippedByAmazon, false)
  });
}

function buildReviewStatuses(rule, keepaResult, comparisonResult) {
  const keepaRequired = rule.keepa_required === 1;
  const idealoRequired = rule.idealo_required === 1;
  const keepaOk = parseBoolean(keepaResult?.ok, !keepaRequired);
  const idealoOk = parseBoolean(comparisonResult?.ok, !idealoRequired);

  return {
    keepaRequired,
    idealoRequired,
    keepaOk,
    idealoOk
  };
}

function applyPenalties(baseScore, input, rule) {
  let score = baseScore;

  if (parseBoolean(input.isCouponOnly, false)) {
    score -= Number(rule.coupon_only_penalty ?? 0);
  }

  if (parseBoolean(input.variantSwitchDetected, false)) {
    score -= Number(rule.variant_switch_penalty ?? 0);
  }

  if (parseBoolean(input.marketplaceSwitchDetected, false)) {
    score -= Number(rule.marketplace_switch_penalty ?? 0);
  }

  return clamp(score, 0, 100, 0);
}

function decideStatus({
  score,
  discount,
  source,
  pricingRule,
  samplingRule,
  sellerType,
  keepaStatus,
  idealoStatus,
  sellerRating,
  title
}) {
  const sellerConfig = getSellerTypeConfig(pricingRule, sellerType);
  const reasons = [];

  if (!sellerConfig.allow) {
    return { status: 'rejected', reason: `${sellerType} ist fuer diese Preislogik deaktiviert.` };
  }

  if (pricingRule.keepa_required === 1 && !keepaStatus) {
    return { status: 'review', reason: 'Keepa-Pruefung erforderlich.' };
  }

  if (pricingRule.idealo_required === 1 && !idealoStatus) {
    return { status: 'review', reason: 'Idealo-/Vergleichspruefung erforderlich.' };
  }

  const keywords = parseJsonArray(pricingRule.manual_blacklist_keywords);
  if (keywords.some((keyword) => title.toLowerCase().includes(keyword.toLowerCase()))) {
    return { status: 'rejected', reason: 'Deal enthaelt ein Blacklist-Keyword.' };
  }

  if (discount < sellerConfig.minDiscount) {
    reasons.push(`Rabatt ${discount}% liegt unter Minimum ${sellerConfig.minDiscount}% fuer ${sellerType}.`);
  }

  if (score < sellerConfig.minScore) {
    reasons.push(`Score ${score} liegt unter Minimum ${sellerConfig.minScore} fuer ${sellerType}.`);
  }

  if (
    sellerType === 'FBM' &&
    pricingRule.min_seller_rating_fbm !== null &&
    pricingRule.min_seller_rating_fbm !== undefined &&
    sellerRating !== null &&
    sellerRating < Number(pricingRule.min_seller_rating_fbm)
  ) {
    reasons.push(`FBM Seller-Rating ${sellerRating} liegt unter ${pricingRule.min_seller_rating_fbm}.`);
  }

  if (samplingRule?.min_score !== null && samplingRule?.min_score !== undefined && score < Number(samplingRule.min_score)) {
    reasons.push(`Score ${score} liegt unter Quellen-Minimum ${samplingRule.min_score}.`);
  }

  if (
    samplingRule?.min_discount !== null &&
    samplingRule?.min_discount !== undefined &&
    discount < Number(samplingRule.min_discount)
  ) {
    reasons.push(`Rabatt ${discount}% liegt unter Quellen-Minimum ${samplingRule.min_discount}%.`);
  }

  if (samplingRule?.daily_limit !== null && samplingRule?.daily_limit !== undefined) {
    const processedToday = getSourceDailyProcessedCount(source.id);
    if (processedToday >= Number(samplingRule.daily_limit)) {
      reasons.push(`Tageslimit ${samplingRule.daily_limit} fuer Quelle erreicht.`);
    }
  }

  const sourceSampling = getSamplingThreshold(samplingRule, sellerType);
  const ruleSampling = Number(sellerConfig.sampling ?? 100);
  const finalSampling = Math.min(sourceSampling, ruleSampling);
  const sampleValue = createDeterministicSampleValue(`${source.id}:${title}:${score}:${discount}:${sellerType}`);

  if (sampleValue > finalSampling) {
    reasons.push(`Sampling blockiert (${sampleValue.toFixed(2)} > ${finalSampling}).`);
  }

  if (sellerType === 'FBM' && pricingRule.fbm_requires_manual_review === 1) {
    return { status: 'review', reason: 'FBM erfordert manuelle Review.', samplingValue: sampleValue };
  }

  if (score >= Number(pricingRule.autopost_above_score) && reasons.length === 0) {
    return { status: 'posted', reason: 'Auto-Post freigegeben.', samplingValue: sampleValue };
  }

  if (score >= Number(pricingRule.manual_review_below_score)) {
    return {
      status: 'review',
      reason: reasons.join(' ') || 'Deal liegt zwischen Review- und Auto-Post-Schwelle.',
      samplingValue: sampleValue
    };
  }

  return {
    status: reasons.length ? 'rejected' : 'review',
    reason: reasons.join(' ') || 'Deal wurde vorsichtshalber zur Review gestellt.',
    samplingValue: sampleValue
  };
}

function buildCopybotReglerContext({
  source,
  pricingRule,
  samplingRule,
  sellerType,
  score,
  discount,
  sellerRating,
  baseDecision,
  reviewStatuses
}) {
  const sellerConfig = getSellerTypeConfig(pricingRule, sellerType);
  const sourceSampling = getSamplingThreshold(samplingRule, sellerType);
  const ruleSampling = Number(sellerConfig.sampling ?? 100);

  return {
    mode: 'pricing_sampling_and_seller_type',
    stage: 'pre_queue',
    decisionStatus: baseDecision?.status || 'review',
    decisionReason: baseDecision?.reason || '',
    score,
    discount,
    sellerRating,
    sampleValue: baseDecision?.samplingValue ?? null,
    keepaOk: reviewStatuses?.keepaOk === true,
    internetOk: reviewStatuses?.idealoOk === true,
    source: source
      ? {
          id: source.id ?? null,
          name: cleanText(source.name),
          platform: cleanText(source.platform),
          sourceType: cleanText(source.source_type)
        }
      : null,
    pricingRule: pricingRule
      ? {
          id: pricingRule.id ?? null,
          name: cleanText(pricingRule.name),
          keepaRequired: pricingRule.keepa_required === 1,
          internetRequired: pricingRule.idealo_required === 1,
          autopostAboveScore: Number(pricingRule.autopost_above_score ?? 0),
          manualReviewBelowScore: Number(pricingRule.manual_review_below_score ?? 0),
          sellerThresholds: {
            allow: sellerConfig.allow === true,
            minDiscount: Number(sellerConfig.minDiscount ?? 0),
            minScore: Number(sellerConfig.minScore ?? 0),
            sampling: ruleSampling
          },
          fbmRequiresManualReview: sellerType === 'FBM' && pricingRule.fbm_requires_manual_review === 1,
          minSellerRating:
            sellerType === 'FBM' && pricingRule.min_seller_rating_fbm !== null && pricingRule.min_seller_rating_fbm !== undefined
              ? Number(pricingRule.min_seller_rating_fbm)
              : null
        }
      : null,
    samplingRule: samplingRule
      ? {
          id: samplingRule.id ?? null,
          name: cleanText(samplingRule.name),
          defaultSampling: Number(samplingRule.default_sampling ?? 100),
          sourceSampling,
          finalSampling: Math.min(sourceSampling, ruleSampling),
          minScore: samplingRule.min_score === null || samplingRule.min_score === undefined ? null : Number(samplingRule.min_score),
          minDiscount:
            samplingRule.min_discount === null || samplingRule.min_discount === undefined ? null : Number(samplingRule.min_discount),
          dailyLimit: samplingRule.daily_limit === null || samplingRule.daily_limit === undefined ? null : Number(samplingRule.daily_limit)
        }
      : null
  };
}

function buildCopybotQueueContext(source) {
  return {
    required: true,
    mode: 'publisher_queue',
    currentStatus: 'not_enqueued',
    splitByPlatform: true,
    preSendPersistence: true,
    recoveryEnabled: true,
    routeType: 'automatic',
    channels: source?.platform ? [source.platform] : ['telegram']
  };
}

export async function processImportedDeal(sourceId, input = {}) {
  const source = loadSourceById(sourceId);
  if (!source) {
    throw new Error('Quelle nicht gefunden.');
  }

  console.info('[PIPELINE_RECEIVED]', {
    sourceId,
    sourceName: cleanText(source.name),
    originalUrl: cleanText(input.originalUrl || input.url),
    title: cleanText(input.title).slice(0, 120),
    hasTelegramText: Boolean(cleanText(input.telegramText)),
    hasImageUrl: Boolean(cleanText(input.imageUrl))
  });

  const copybotState = getCopybotRuntimeState();
  if (copybotState.enabled !== true) {
    console.warn('[COPYBOT_SKIP_PIPELINE_DISABLED]', {
      sourceId,
      sourceName: cleanText(source.name),
      reason: copybotState.reason,
      envEnabled: copybotState.envEnabled,
      settingEnabled: copybotState.settingEnabled
    });
    logEvent({
      level: 'warning',
      eventType: 'copybot.disabled',
      sourceId,
      message: 'Deal verworfen, weil der globale Copybot deaktiviert ist.',
      payload: {
        ...input,
        copybotState
      }
    });

    return {
      blocked: true,
      reason: 'Copybot ist global deaktiviert.',
      status: 'blocked',
      queueEntryId: null
    };
  }

  if (source.is_active !== 1) {
    return {
      blocked: true,
      reason: 'Quelle ist deaktiviert.',
      status: 'blocked'
    };
  }

  const pricingRule = loadPricingRuleById(source.pricing_rule_id);
  const samplingRule = source.sampling_rule_id ? loadSamplingRuleById(source.sampling_rule_id) : null;
  if (!pricingRule) {
    throw new Error('Preispruef-Logik der Quelle fehlt.');
  }

  const originalUrl = cleanText(input.originalUrl || input.url);
  if (isAmazonShortLink(originalUrl)) {
    console.info('[AUTOMATION_SHORTLINK_BLOCKED]', {
      sourceId,
      sourceName: cleanText(source.name),
      originalUrl
    });
  }
  const resolvedOriginalUrl = await resolveAmazonShortLink(originalUrl);
  const linkRecord = buildAmazonAffiliateLinkRecord(input.normalizedUrl || resolvedOriginalUrl || originalUrl || input.asin, {
    resolvedUrl: resolvedOriginalUrl,
    asin: input.asin
  });
  const normalizedUrl = linkRecord.valid
    ? linkRecord.normalizedUrl
    : normalizeAmazonLink(input.normalizedUrl || resolvedOriginalUrl || originalUrl);
  const affiliateUrl = linkRecord.valid ? linkRecord.affiliateUrl : '';
  const asin =
    cleanText(input.asin).toUpperCase() || linkRecord.asin || extractAsin(normalizedUrl || resolvedOriginalUrl || originalUrl);
  if (affiliateUrl) {
    console.info('[COPYBOT_OWN_AFFILIATE_LINK_ENFORCED]', {
      sourceId,
      sourceName: cleanText(source.name),
      originalUrl: originalUrl || null,
      resolvedUrl: resolvedOriginalUrl || null,
      normalizedUrl: normalizedUrl || null,
      ownAffiliateUrl: affiliateUrl,
      asin: asin || null
    });
  }
  const sellerType = detectSellerType(input);
  const title = cleanText(input.title) || asin || normalizedUrl || originalUrl || 'Unbenannter Deal';
  const currentPrice = parseNumber(input.currentPrice ?? input.price);
  const oldPrice = parseNumber(input.oldPrice);
  const detectedDiscount =
    input.detectedDiscount !== undefined && input.detectedDiscount !== null
      ? Number(input.detectedDiscount)
      : currentPrice !== null && oldPrice && oldPrice > 0
        ? Math.round(((oldPrice - currentPrice) / oldPrice) * 10000) / 100
        : 0;

  const history = checkDealCooldown({
    asin,
    url: originalUrl,
    normalizedUrl
  });

  const keepaResult =
    input.keepaResult ?? {
      ok: !pricingRule.keepa_required,
      status: pricingRule.keepa_required ? 'missing' : 'not_required'
    };
  const comparisonResult =
    input.comparisonResult ?? {
      ok: !pricingRule.idealo_required,
      status: pricingRule.idealo_required ? 'missing' : 'not_required'
    };
  const reviewStatuses = buildReviewStatuses(pricingRule, keepaResult, comparisonResult);
  const baseScore = clamp(input.score ?? 0, 0, 100, detectedDiscount);
  const score = applyPenalties(baseScore, input, pricingRule);
  const sellerRating =
    input.sellerRating === undefined || input.sellerRating === null ? null : Number(input.sellerRating);
  const baseDecision = decideStatus({
    score,
    discount: detectedDiscount,
    source,
    pricingRule,
    samplingRule,
    sellerType,
    keepaStatus: reviewStatuses.keepaOk,
    idealoStatus: reviewStatuses.idealoOk,
    sellerRating,
    title
  });
  const reglerContext = buildCopybotReglerContext({
    source,
    pricingRule,
    samplingRule,
    sellerType,
    score,
    discount: detectedDiscount,
    sellerRating,
    baseDecision,
    reviewStatuses
  });
  const keepaSettings = getKeepaSettings();
  const internetContext = loadStoredInternetComparisonContext({
    asin
  });
  let effectiveKeepaContext = {
    available: false,
    status: 'fallback_not_required',
    cached: false,
    reason: 'Marktvergleich liegt bereits vor.'
  };

  if (internetContext.available) {
    logGeneratorDebug('INTERNET COMPARISON PRIMARY', {
      sourceId,
      asin,
      sellerType,
      comparisonStatus: internetContext.result?.comparisonStatus || internetContext.status
    });
  } else {
    effectiveKeepaContext = await loadKeepaClientByAsin({
      asin,
      sellerType,
      currentPrice,
      title,
      productUrl: affiliateUrl || normalizedUrl || resolvedOriginalUrl || originalUrl,
      imageUrl: cleanText(input.imageUrl),
      source: 'scrapper_import'
    });

    logGeneratorDebug('KEEPA FALLBACK USED', {
      sourceId,
      asin,
      sellerType,
      reason: internetContext.reason || 'Kein gespeicherter Marktvergleich vorhanden.'
    });
  }

  logGeneratorDebug('SCRAPPER CONNECTED TO LEARNING LOGIC', {
    sourceId,
    asin,
    sellerType,
    keepaStatus: effectiveKeepaContext?.status || 'missing',
    internetStatus: internetContext?.status || 'missing'
  });

  const learningContext = evaluateLearningRoute({
    sourceType: 'scrapper',
    enforceDecision: true,
    keepaRequired: true,
    asin,
    sellerType,
    currentPrice,
    internetContext,
    keepaContext: effectiveKeepaContext,
    dealLockStatus: history,
    reglerContext,
    queueContext: buildCopybotQueueContext(source),
    patternSupportEnabled: getKeepaDrawerControlConfig(sellerType).patternSupportEnabled === true,
    marketMinGapPct: keepaSettings.strongDealMinComparisonGapPct
  });
  const timestamp = nowIso();
  const learningDecision = learningContext?.learning?.routingDecision || 'review';
  let finalStatus = baseDecision.status;
  let finalReason = baseDecision.reason;

  if (originalUrl && !linkRecord.valid) {
    finalStatus = 'review';
    finalReason = 'Amazon-Link konnte nicht normalisiert werden oder ASIN fehlt.';
  }

  if (learningDecision === 'block') {
    finalStatus = 'rejected';
    finalReason = learningContext.learning.reason || 'Lern-Logik hat den Deal blockiert.';
  } else if (learningDecision === 'review') {
    finalStatus = 'review';
    finalReason = learningContext.learning.reason || 'Lern-Logik verlangt manuelle Review.';
  }

  if (history.blocked && finalStatus === 'posted') {
    finalStatus = 'review';
    finalReason = history.blockReason || 'Deal-Lock blockiert Auto-Post.';
  }

  const result = db
    .prepare(
      `
        INSERT INTO imported_deals (
          source_id,
          asin,
          original_url,
          normalized_url,
          title,
          current_price,
          old_price,
          seller_type,
          detected_discount,
          score,
          keepa_result_json,
          comparison_result_json,
          learning_context_json,
          learning_decision,
          status,
          review_reason,
          decision_reason,
          posted_at,
          created_at,
          updated_at
        ) VALUES (
          @source_id,
          @asin,
          @original_url,
          @normalized_url,
          @title,
          @current_price,
          @old_price,
          @seller_type,
          @detected_discount,
          @score,
          @keepa_result_json,
          @comparison_result_json,
          @learning_context_json,
          @learning_decision,
          @status,
          @review_reason,
          @decision_reason,
          @posted_at,
          @created_at,
          @updated_at
        )
      `
    )
    .run({
      source_id: sourceId,
      asin,
      original_url: originalUrl,
      normalized_url: normalizedUrl,
      title,
      current_price: currentPrice,
      old_price: oldPrice,
      seller_type: sellerType,
      detected_discount: detectedDiscount,
      score,
      keepa_result_json: stringifyJson(keepaResult),
      comparison_result_json: stringifyJson(comparisonResult),
      learning_context_json: stringifyJson(learningContext),
      learning_decision: learningDecision,
      status: finalStatus,
      review_reason: finalStatus === 'review' ? finalReason : null,
      decision_reason: finalReason,
      posted_at: finalStatus === 'posted' ? timestamp : null,
      created_at: timestamp,
      updated_at: timestamp
    });

  const importedDealId = result.lastInsertRowid;

  console.info('[DEAL_CREATED]', {
    importedDealId,
    sourceId,
    sourceName: cleanText(source.name),
    status: finalStatus,
    reason: finalReason,
    asin,
    normalizedUrl
  });

  db.prepare(`UPDATE sources SET last_import_at = ?, updated_at = ? WHERE id = ?`).run(timestamp, timestamp, sourceId);

  let queueEntryId = null;
  if (finalStatus === 'posted') {
    const queueEntry = enqueueCopybotPublishing({
      sourceId: importedDealId,
      payload: {
        title,
        link: affiliateUrl || normalizedUrl || resolvedOriginalUrl || originalUrl,
        normalizedUrl,
        asin,
        currentPrice: currentPrice === null ? '' : String(currentPrice),
        oldPrice: oldPrice === null ? '' : String(oldPrice),
        sellerType,
        couponCode: cleanText(input.couponCode),
        textByChannel: {
          telegram: cleanText(input.telegramText || title),
          whatsapp: cleanText(input.whatsappText || title),
          facebook: cleanText(input.facebookText || title)
        },
        imageVariants: {
          standard: cleanText(input.imageUrl),
          upload: cleanText(input.uploadedImageUrl)
        },
        targetImageSources: {
          telegram: 'standard',
          whatsapp: 'standard',
          facebook: 'link_preview'
        }
      },
      targets: [
        { channelType: 'telegram', isEnabled: source.platform === 'telegram', imageSource: 'standard' },
        { channelType: 'whatsapp', isEnabled: source.platform === 'whatsapp', imageSource: 'standard' }
      ]
    });
    queueEntryId = queueEntry?.id ?? null;

    console.info('[QUEUE_JOB_CREATED]', {
      queueId: queueEntryId,
      sourceType: 'copybot',
      sourceId: importedDealId,
      telegramEnabled: source.platform === 'telegram',
      whatsappEnabled: source.platform === 'whatsapp'
    });
  }

  logEvent({
    level: finalStatus === 'rejected' ? 'warning' : 'info',
    eventType: `deal.${finalStatus}`,
    sourceId,
    importedDealId,
    message: finalReason,
    payload: {
      score,
      sellerType,
      detectedDiscount,
      historyBlocked: history.blocked,
      queueEntryId,
      learningDecision,
      learningReason: learningContext?.learning?.reason || '',
      keepaStatus: learningContext?.keepa?.status || 'missing'
    }
  });

  syncImportedDealState({
    sourceId: importedDealId,
    asin,
    normalizedUrl,
    originalUrl,
    title,
    sellerType,
    status: queueEntryId ? 'queued' : finalStatus,
    queueId: queueEntryId,
    decisionReason: finalReason,
    origin: 'automatic',
    meta: {
      importedDealStatus: finalStatus,
      learningDecision,
      score,
      detectedDiscount
    }
  });

  return {
    id: importedDealId,
    status: finalStatus,
    reason: finalReason,
    queueEntryId,
    sellerType,
    score,
    detectedDiscount,
    asin,
    normalizedUrl,
    affiliateUrl,
    learningContext
  };
}

export function listReviewQueue(limit = 40) {
  const normalizedLimit = Math.max(1, Math.min(200, Number(limit) || 40));

  return db
    .prepare(
      `
        SELECT
          d.*,
          s.name AS source_name,
          s.platform,
          s.is_active AS source_is_active
        FROM imported_deals d
        JOIN sources s ON s.id = d.source_id
        WHERE d.status = 'review'
        ORDER BY d.created_at DESC
        LIMIT ?
      `
    )
    .all(normalizedLimit)
    .map((row) => ({
      ...row,
      keepa_result: row.keepa_result_json ? JSON.parse(row.keepa_result_json) : null,
      comparison_result: row.comparison_result_json ? JSON.parse(row.comparison_result_json) : null,
      learning_context: row.learning_context_json ? JSON.parse(row.learning_context_json) : null
    }));
}

export function updateReviewDecision(id, action) {
  const deal = db.prepare(`SELECT * FROM imported_deals WHERE id = ?`).get(id);
  if (!deal) {
    throw new Error('Review-Deal nicht gefunden.');
  }

  const nextStatus = action === 'approve' ? 'approved' : 'rejected';
  db.prepare(`UPDATE imported_deals SET status = ?, updated_at = ? WHERE id = ?`).run(nextStatus, nowIso(), id);

  if (action === 'approve') {
    const source = loadSourceById(deal.source_id);
    const linkRecord = buildAmazonAffiliateLinkRecord(deal.normalized_url || deal.original_url || deal.asin, {
      resolvedUrl: deal.normalized_url,
      asin: deal.asin
    });
    const queueEntry = enqueueCopybotPublishing({
      sourceId: deal.id,
      payload: {
        title: deal.title,
        link: linkRecord.valid ? linkRecord.affiliateUrl : deal.original_url,
        normalizedUrl: linkRecord.valid ? linkRecord.normalizedUrl : deal.normalized_url,
        asin: deal.asin || linkRecord.asin,
        currentPrice: deal.current_price === null ? '' : String(deal.current_price),
        oldPrice: deal.old_price === null ? '' : String(deal.old_price),
        sellerType: deal.seller_type,
        textByChannel: {
          telegram: deal.title,
          whatsapp: deal.title,
          facebook: deal.title
        },
        imageVariants: {
          standard: '',
          upload: ''
        },
        targetImageSources: {
          telegram: 'standard',
          whatsapp: 'standard',
          facebook: 'link_preview'
        }
      },
      targets: [
        { channelType: 'telegram', isEnabled: source?.platform === 'telegram', imageSource: 'standard' },
        { channelType: 'whatsapp', isEnabled: source?.platform === 'whatsapp', imageSource: 'standard' }
      ]
    });

    syncImportedDealState({
      sourceId: deal.id,
      asin: deal.asin,
      normalizedUrl: linkRecord.valid ? linkRecord.normalizedUrl : deal.normalized_url,
      originalUrl: linkRecord.valid ? linkRecord.affiliateUrl : deal.original_url,
      title: deal.title,
      sellerType: deal.seller_type,
      status: 'queued',
      queueId: queueEntry?.id ?? null,
      decisionReason: 'Deal manuell freigegeben und in Queue gelegt.',
      origin: 'manual'
    });
  } else {
    syncImportedDealState({
      sourceId: deal.id,
      asin: deal.asin,
      normalizedUrl: deal.normalized_url,
      originalUrl: deal.original_url,
      title: deal.title,
      sellerType: deal.seller_type,
      status: 'rejected',
      decisionReason: 'Deal manuell verworfen.',
      origin: 'manual'
    });
  }

  logEvent({
    level: action === 'approve' ? 'info' : 'warning',
    eventType: `review.${action}`,
    sourceId: deal.source_id,
    importedDealId: id,
    message: action === 'approve' ? 'Deal manuell freigegeben.' : 'Deal manuell verworfen.'
  });

  return db.prepare(`SELECT * FROM imported_deals WHERE id = ?`).get(id);
}

export function getCopybotOverview() {
  const copybotState = getCopybotRuntimeState();
  const statusAudit = getCopybotStatusAudit();
  const stats = db
    .prepare(
      `
        SELECT
          SUM(CASE WHEN platform = 'telegram' AND is_active = 1 THEN 1 ELSE 0 END) AS telegram_sources,
          SUM(CASE WHEN platform = 'whatsapp' AND is_active = 1 THEN 1 ELSE 0 END) AS whatsapp_sources,
          (SELECT COUNT(*) FROM pricing_rules) AS pricing_rules_count,
          (SELECT COUNT(*) FROM imported_deals WHERE status = 'review') AS review_count,
          (SELECT COUNT(*) FROM imported_deals WHERE status IN ('posted', 'approved')) AS approved_count,
          (SELECT COUNT(*) FROM imported_deals WHERE status = 'rejected') AS rejected_count
        FROM sources
      `
    )
    .get();
  const lastSource = db
    .prepare(
      `
        SELECT id, name, platform, last_import_at
        FROM sources
        WHERE last_import_at IS NOT NULL
        ORDER BY last_import_at DESC
        LIMIT 1
      `
    )
    .get();
  const lastDeals = db
    .prepare(
      `
        SELECT
          d.id,
          d.title,
          d.status,
          d.score,
          d.seller_type,
          d.detected_discount,
          d.created_at,
          s.name AS source_name,
          s.platform
        FROM imported_deals d
        JOIN sources s ON s.id = d.source_id
        ORDER BY d.created_at DESC
        LIMIT 10
      `
    )
    .all();

  return {
    copybotEnabled: copybotState.enabled === true,
    copybotRuntime: copybotState,
    statusAudit,
    processingStatus: {
      input: copybotState.enabled === true ? 'aktiv' : 'pausiert',
      queue: copybotState.enabled === true ? 'aktiv' : 'pausiert'
    },
    activeTelegramSources: Number(stats?.telegram_sources ?? 0),
    activeWhatsappSources: Number(stats?.whatsapp_sources ?? 0),
    pricingRulesCount: Number(stats?.pricing_rules_count ?? 0),
    reviewCount: Number(stats?.review_count ?? 0),
    approvedCount: Number(stats?.approved_count ?? 0),
    rejectedCount: Number(stats?.rejected_count ?? 0),
    lastProcessedSource: lastSource || null,
    lastProcessedDeals: lastDeals
  };
}

export function listCopybotLogs(limit = 120) {
  const normalizedLimit = Math.max(1, Math.min(400, Number(limit) || 120));

  return db
    .prepare(
      `
        SELECT
          l.*,
          s.name AS source_name
        FROM copybot_logs l
        LEFT JOIN sources s ON s.id = l.source_id
        ORDER BY l.created_at DESC
        LIMIT ?
      `
    )
    .all(normalizedLimit)
    .map((row) => ({
      ...row,
      payload: row.payload_json ? JSON.parse(row.payload_json) : null
    }));
}

export async function testSource(sourceId, sampleInput = {}) {
  return processImportedDeal(sourceId, {
    title: sampleInput.title || 'Testdeal',
    url: sampleInput.url || 'https://www.amazon.de/dp/B000TEST00',
    currentPrice: sampleInput.currentPrice ?? 49.99,
    oldPrice: sampleInput.oldPrice ?? 79.99,
    sellerType: sampleInput.sellerType || 'AMAZON',
    detectedDiscount: sampleInput.detectedDiscount ?? 18,
    score: sampleInput.score ?? 88,
    keepaResult: sampleInput.keepaResult ?? { ok: true, status: 'ok' },
    comparisonResult: sampleInput.comparisonResult ?? { ok: true, status: 'ok' }
  });
}
