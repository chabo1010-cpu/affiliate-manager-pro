import crypto from 'crypto';
import { getDb } from '../db.js';
import { getKeepaConfig, getTelegramConfig } from '../env.js';
import { sendTelegramPost } from './telegramSenderService.js';
import { getComparisonAdapterCatalog, resolveComparisonFromAdapters } from './keepaComparisonAdapters.js';
import {
  evaluateFakeDropAlertEligibility,
  getFakeDropSettingsView,
  getFakeDropSnapshotForResult,
  getFakeDropSummary,
  persistFakeDropAnalysis
} from './keepaFakeDropService.js';

const db = getDb();

const DOMAIN_OPTIONS = [
  { id: 1, label: 'Amazon.com (US)', host: 'amazon.com' },
  { id: 2, label: 'Amazon.co.uk (UK)', host: 'amazon.co.uk' },
  { id: 3, label: 'Amazon.de (DE)', host: 'amazon.de' },
  { id: 4, label: 'Amazon.fr (FR)', host: 'amazon.fr' },
  { id: 5, label: 'Amazon.co.jp (JP)', host: 'amazon.co.jp' },
  { id: 6, label: 'Amazon.ca (CA)', host: 'amazon.ca' },
  { id: 8, label: 'Amazon.it (IT)', host: 'amazon.it' },
  { id: 9, label: 'Amazon.es (ES)', host: 'amazon.es' },
  { id: 10, label: 'Amazon.in (IN)', host: 'amazon.in' },
  { id: 11, label: 'Amazon.com.mx (MX)', host: 'amazon.com.mx' }
];

const CATEGORY_CATALOG = [
  { id: 172282, name: 'Elektronik', description: 'Technik, Audio, Smart Home und Elektronik-Zubehoer.' },
  { id: 541966, name: 'Computer', description: 'Hardware, Komponenten, Speicher und Peripherie.' },
  { id: 1055398, name: 'Haushalt & Kueche', description: 'Kuechengeraete, Haushalt und Wohnbedarf.' },
  { id: 3760911, name: 'Beauty', description: 'Beauty, Pflege und Wellness.' },
  { id: 3375251, name: 'Sport & Freizeit', description: 'Sportzubehoer, Outdoor und Fitness.' },
  { id: 165793011, name: 'Spielzeug', description: 'Spielzeug, Games und Familienprodukte.' },
  { id: 7141123011, name: 'Mode', description: 'Bekleidung, Schuhe und Accessoires.' },
  { id: 283155, name: 'Buecher', description: 'Buecher, Ratgeber und Fachliteratur.' },
  { id: 1064954, name: 'Buero', description: 'Bueroartikel, Druckerbedarf und Organisation.' },
  { id: 2972638011, name: 'Garten & Outdoor', description: 'Garten, Balkon, BBQ und Outdoor.' }
];

const DEFAULT_COMPARISON_SOURCE_CONFIG = {
  'manual-source': { enabled: true },
  idealo: { enabled: false },
  'custom-api': { enabled: false }
};

const CATEGORY_ID_SET = new Set(CATEGORY_CATALOG.map((item) => item.id));
const COMPARISON_SOURCE_IDS = new Set(Object.keys(DEFAULT_COMPARISON_SOURCE_CONFIG));
const USAGE_MODULE_CATALOG = [
  { id: 'manual-search', label: 'Manuelle Suche' },
  { id: 'automation-run', label: 'Automatik' },
  { id: 'test-connection', label: 'Testverbindung' },
  { id: 'background-check', label: 'Hintergrundpruefung' },
  { id: 'result-refresh', label: 'Ergebnis-Refresh' },
  { id: 'alert-check', label: 'Alert-Pruefung' },
  { id: 'status-check', label: 'Status-Pruefung' }
];
const USAGE_MODULE_IDS = new Set(USAGE_MODULE_CATALOG.map((item) => item.id));
const USAGE_ACTION_IDS = new Set([
  'keepa-request',
  'manual-search',
  'automation-run',
  'test-connection',
  'result-refresh',
  'alert-check',
  'background-check'
]);
const USAGE_LOG_MODULE_MAP = {
  manual_search: 'manual-search',
  manual_search_products: 'manual-search',
  rule_scan: 'automation-run',
  rule_scan_products: 'automation-run',
  test_connection: 'test-connection',
  results: 'result-refresh',
  alerts: 'alert-check',
  background_check: 'background-check',
  scheduler: 'automation-run',
  status_check: 'status-check'
};

const DEFAULT_SETTINGS = {
  keepaEnabled: true,
  schedulerEnabled: true,
  domainId: 3,
  defaultCategories: [],
  defaultDiscount: 40,
  defaultSellerType: 'ALL',
  defaultMinPrice: null,
  defaultMaxPrice: null,
  defaultPageSize: 24,
  defaultIntervalMinutes: 60,
  strongDealMinDiscount: 40,
  strongDealMinComparisonGapPct: 10,
  goodRatingThreshold: 4,
  alertTelegramEnabled: false,
  alertInternalEnabled: true,
  alertWhatsappPlaceholderEnabled: false,
  alertCooldownMinutes: 180,
  alertMaxPerProduct: 2,
  telegramMessagePrefix: 'Keepa Alert',
  comparisonSourceConfig: DEFAULT_COMPARISON_SOURCE_CONFIG,
  loggingEnabled: true,
  estimatedTokensPerManualRun: 8
};

const MAX_MANUAL_PAGE_SIZE = 48;
const MAX_MANUAL_PAGE = 10;
const SEARCH_DATE_RANGE_DAYS = 7;
const SCHEDULER_INTERVAL_MS = 60 * 1000;

let keepaQueue = Promise.resolve();
let lastKeepaRequestStartedAt = 0;
let schedulerStarted = false;
let schedulerRunning = false;
let keepaConnectionCache = null;

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

  const normalized = trimmed.replace(/[^\d,.-]/g, '').replace(',', '.');
  const parsed = Number.parseFloat(normalized);
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

function getLocalDayStart(date = new Date()) {
  const nextDate = new Date(date);
  nextDate.setHours(0, 0, 0, 0);
  return nextDate;
}

function getLocalMonthStart(date = new Date()) {
  const nextDate = new Date(date);
  nextDate.setDate(1);
  nextDate.setHours(0, 0, 0, 0);
  return nextDate;
}

function shiftLocalDays(date, days) {
  const nextDate = new Date(date);
  nextDate.setDate(nextDate.getDate() + days);
  return nextDate;
}

function getRangeStart(range = 'today') {
  const now = new Date();

  if (range === 'month') {
    return getLocalMonthStart(now);
  }

  if (range === 'week') {
    return getLocalDayStart(shiftLocalDays(now, -6));
  }

  return getLocalDayStart(now);
}

function toMinorUnits(value) {
  const parsed = parseNumber(value, null);
  if (parsed === null) {
    return null;
  }

  return Math.round(parsed * 100);
}

function fromMinorUnits(value) {
  const parsed = parseNumber(value, null);
  if (parsed === null || parsed < 0) {
    return null;
  }

  return Math.round(parsed) / 100;
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function maskSecret(value, options = {}) {
  const trimmed = cleanText(value);
  if (!trimmed) {
    return '';
  }

  const visibleStart = options.visibleStart ?? 4;
  const visibleEnd = options.visibleEnd ?? 4;

  if (trimmed.length <= visibleStart + visibleEnd) {
    return `${trimmed.slice(0, 2)}***`;
  }

  return `${trimmed.slice(0, visibleStart)}***${trimmed.slice(-visibleEnd)}`;
}

function sanitizePriceBoundary(value) {
  const parsed = parseNumber(value, null);
  if (parsed === null) {
    return null;
  }

  return clamp(parsed, 0, 500000);
}

function normalizeCategoryIds(values, fallback = []) {
  const sourceValues = Array.isArray(values) ? values : fallback;

  return [...new Set(sourceValues.map(Number))]
    .filter((value) => Number.isFinite(value) && CATEGORY_ID_SET.has(value))
    .slice(0, 10);
}

function normalizeComparisonSourceIds(values, fallback = ['manual-source']) {
  const sourceValues = Array.isArray(values) ? values : fallback;
  const normalized = [...new Set(sourceValues.map((value) => cleanText(value)).filter((value) => COMPARISON_SOURCE_IDS.has(value)))];

  return normalized.length ? normalized.slice(0, 5) : ['manual-source'];
}

function normalizeComparisonSourceConfig(config, fallback = DEFAULT_COMPARISON_SOURCE_CONFIG) {
  const sourceConfig = typeof config === 'object' && config ? config : fallback;

  return Object.fromEntries(
    Object.keys(DEFAULT_COMPARISON_SOURCE_CONFIG).map((adapterId) => [
      adapterId,
      {
        enabled: parseBool(sourceConfig?.[adapterId]?.enabled, fallback?.[adapterId]?.enabled ?? false)
      }
    ])
  );
}

function normalizeUsageModule(value, fallback = 'manual-search') {
  const normalized = cleanText(String(value || ''))
    .toLowerCase()
    .replace(/[^a-z0-9_-]/g, '-');
  const mapped = USAGE_LOG_MODULE_MAP[normalized] || normalized;
  return USAGE_MODULE_IDS.has(mapped) ? mapped : fallback;
}

function resolveUsageModuleFilter(value) {
  const normalized = cleanText(String(value || ''))
    .toLowerCase()
    .replace(/[^a-z0-9_-]/g, '-');
  const mapped = USAGE_LOG_MODULE_MAP[normalized] || normalized;
  return USAGE_MODULE_IDS.has(mapped) ? mapped : '';
}

function normalizeUsageAction(value, fallback = 'manual-search') {
  const normalized = cleanText(String(value || ''))
    .toLowerCase()
    .replace(/[^a-z0-9_-]/g, '-');
  return USAGE_ACTION_IDS.has(normalized) ? normalized : fallback;
}

function normalizeUsageStatus(value, fallback = 'success') {
  const normalized = cleanText(String(value || ''))
    .toLowerCase()
    .replace(/[^a-z]/g, '');
  return ['success', 'error', 'warning', 'partial', 'skipped'].includes(normalized) ? normalized : fallback;
}

function sanitizeUsageFilters(filters) {
  if (!filters || typeof filters !== 'object') {
    return null;
  }

  const allowedKeys = [
    'page',
    'limit',
    'sellerType',
    'minDiscount',
    'minPrice',
    'maxPrice',
    'onlyPrime',
    'onlyInStock',
    'onlyGoodRating',
    'domainId',
    'categoryId',
    'categories',
    'asinCount',
    'ruleId',
    'workflowStatus'
  ];
  const safe = {};

  allowedKeys.forEach((key) => {
    const value = filters[key];
    if (value === undefined || value === null || value === '') {
      return;
    }

    if (key === 'categories') {
      const normalizedCategories = normalizeCategoryIds(value, []);
      if (normalizedCategories.length) {
        safe.categories = normalizedCategories;
        safe.categoryCount = normalizedCategories.length;
      }
      return;
    }

    safe[key] = value;
  });

  return Object.keys(safe).length ? safe : null;
}

function sanitizeUsageMeta(meta) {
  if (!meta || typeof meta !== 'object') {
    return null;
  }

  const safe = {};
  Object.entries(meta).forEach(([key, value]) => {
    const normalizedKey = cleanText(key);
    if (!normalizedKey || /key|token|secret|password/i.test(normalizedKey)) {
      return;
    }

    if (value === undefined) {
      return;
    }

    if (typeof value === 'string') {
      safe[normalizedKey] = value.slice(0, 250);
      return;
    }

    if (typeof value === 'number' || typeof value === 'boolean' || value === null) {
      safe[normalizedKey] = value;
      return;
    }

    if (Array.isArray(value)) {
      safe[normalizedKey] = value.slice(0, 20);
      return;
    }

    if (typeof value === 'object') {
      safe[normalizedKey] = value;
    }
  });

  return Object.keys(safe).length ? safe : null;
}

function estimateSearchUsage(filters, settings, meta = {}) {
  const base = clamp(parseInteger(settings?.estimatedTokensPerManualRun, DEFAULT_SETTINGS.estimatedTokensPerManualRun), 1, 250);
  const categories = normalizeCategoryIds(filters?.categories, []);
  const limit = clamp(parseInteger(filters?.limit, settings?.defaultPageSize || DEFAULT_SETTINGS.defaultPageSize), 1, MAX_MANUAL_PAGE_SIZE);
  const requestCount = clamp(parseInteger(meta.requestCount, 2), 1, 10);
  const categoryFactor = categories.length ? Math.ceil(categories.length / 3) : 0;
  const limitFactor = Math.max(0, Math.ceil(limit / 24) - 1);
  const priceFactor = filters?.minPrice !== null || filters?.maxPrice !== null ? 1 : 0;
  const qualityFactor = [filters?.onlyPrime, filters?.onlyInStock, filters?.onlyGoodRating].filter(Boolean).length;
  const automationFactor = meta.origin === 'automatic' ? 1 : 0;

  return clamp(base + categoryFactor + limitFactor + priceFactor + qualityFactor + automationFactor + Math.max(0, requestCount - 2), 1, 250);
}

function recordKeepaUsage(entry = {}) {
  const createdAt = cleanText(entry.createdAt) || nowIso();
  const usageDate = toLocalDateKey(createdAt);
  const action = normalizeUsageAction(entry.action, 'manual-search');
  const module = normalizeUsageModule(entry.module || entry.source, 'manual-search');
  const requestStatus = normalizeUsageStatus(entry.requestStatus, 'success');
  const resultCount = parseInteger(entry.resultCount, 0);
  const durationMs = clamp(parseInteger(entry.durationMs, 0), 0, 24 * 60 * 60 * 1000);
  const estimatedUsage = Math.max(0, parseNumber(entry.estimatedUsage, 0) ?? 0);
  const officialUsageValue = parseNumber(entry.officialUsageValue, null);
  const officialTokensLeft = parseInteger(entry.officialTokensLeft, null);
  const ruleId = parseInteger(entry.ruleId, null);
  const errorMessage = cleanText(entry.errorMessage) || null;
  const filtersJson = toJson(sanitizeUsageFilters(entry.filters));
  const metaJson = toJson(sanitizeUsageMeta(entry.meta));

  db.prepare(
    `
      INSERT INTO keepa_usage_logs (
        action,
        module,
        filters_json,
        result_count,
        duration_ms,
        request_status,
        estimated_usage,
        official_usage_value,
        official_tokens_left,
        rule_id,
        error_message,
        meta_json,
        created_at
      ) VALUES (
        @action,
        @module,
        @filtersJson,
        @resultCount,
        @durationMs,
        @requestStatus,
        @estimatedUsage,
        @officialUsageValue,
        @officialTokensLeft,
        @ruleId,
        @errorMessage,
        @metaJson,
        @createdAt
      )
    `
  ).run({
    action,
    module,
    filtersJson,
    resultCount,
    durationMs,
    requestStatus,
    estimatedUsage,
    officialUsageValue,
    officialTokensLeft,
    ruleId,
    errorMessage,
    metaJson,
    createdAt
  });

  db.prepare(
    `
      INSERT INTO keepa_usage_daily (
        usage_date,
        module,
        action,
        request_count,
        result_count,
        estimated_usage,
        official_usage_value,
        success_count,
        error_count,
        total_duration_ms,
        last_request_at
      ) VALUES (
        @usageDate,
        @module,
        @action,
        1,
        @resultCount,
        @estimatedUsage,
        @officialUsageValue,
        @successCount,
        @errorCount,
        @durationMs,
        @createdAt
      )
      ON CONFLICT(usage_date, module, action)
      DO UPDATE SET
        request_count = request_count + 1,
        result_count = result_count + @resultCount,
        estimated_usage = estimated_usage + @estimatedUsage,
        official_usage_value = COALESCE(official_usage_value, 0) + COALESCE(@officialUsageValue, 0),
        success_count = success_count + @successCount,
        error_count = error_count + @errorCount,
        total_duration_ms = total_duration_ms + @durationMs,
        last_request_at = CASE
          WHEN last_request_at IS NULL OR last_request_at < @createdAt THEN @createdAt
          ELSE last_request_at
        END
    `
  ).run({
    usageDate,
    module,
    action,
    resultCount,
    estimatedUsage,
    officialUsageValue,
    successCount: requestStatus === 'success' ? 1 : 0,
    errorCount: requestStatus === 'error' ? 1 : 0,
    durationMs,
    createdAt
  });

  return {
    action,
    module,
    requestStatus,
    resultCount,
    durationMs,
    estimatedUsage,
    officialUsageValue,
    officialTokensLeft,
    ruleId,
    createdAt
  };
}

function normalizeSellerType(value) {
  const normalized = cleanText(String(value || '')).toUpperCase();
  return ['ALL', 'AMAZON', 'FBA', 'FBM'].includes(normalized) ? normalized : 'ALL';
}

function normalizeWorkflowStatus(value) {
  const normalized = cleanText(String(value || '')).toLowerCase();
  return ['neu', 'geprueft', 'alert_gesendet', 'verworfen'].includes(normalized) ? normalized : 'neu';
}

function normalizeDealStrength(value) {
  const normalized = cleanText(String(value || '')).toLowerCase();
  return ['pruefenswert', 'stark', 'verwerfen'].includes(normalized) ? normalized : 'pruefenswert';
}

function queueKeepaTask(task) {
  const runTask = async () => {
    const { requestIntervalMs } = getKeepaConfig();
    const waitMs = Math.max(0, requestIntervalMs - (Date.now() - lastKeepaRequestStartedAt));

    if (waitMs > 0) {
      await delay(waitMs);
    }

    lastKeepaRequestStartedAt = Date.now();
    return task();
  };

  const nextTask = keepaQueue.catch(() => null).then(runTask);
  keepaQueue = nextTask.catch(() => null);
  return nextTask;
}

function getKeepaSettingsRow() {
  return (
    db.prepare(`SELECT * FROM keepa_settings WHERE id = 1`).get() || {
      id: 1
    }
  );
}

function normalizeSettingsRow(row) {
  const comparisonSourceConfig = normalizeComparisonSourceConfig(
    fromJson(row.comparison_source_config_json, {}),
    DEFAULT_COMPARISON_SOURCE_CONFIG
  );

  return {
    keepaEnabled: parseBool(row.keepa_enabled, DEFAULT_SETTINGS.keepaEnabled),
    schedulerEnabled: parseBool(row.scheduler_enabled, DEFAULT_SETTINGS.schedulerEnabled),
    domainId: parseInteger(row.domain_id, DEFAULT_SETTINGS.domainId),
    defaultCategories: normalizeCategoryIds(fromJson(row.default_categories_json, []), DEFAULT_SETTINGS.defaultCategories),
    defaultDiscount: clamp(parseNumber(row.default_discount, DEFAULT_SETTINGS.defaultDiscount), 0, 95),
    defaultSellerType: normalizeSellerType(row.default_seller_type || DEFAULT_SETTINGS.defaultSellerType),
    defaultMinPrice: parseNumber(row.default_min_price, null),
    defaultMaxPrice: parseNumber(row.default_max_price, null),
    defaultPageSize: clamp(parseInteger(row.default_page_size, DEFAULT_SETTINGS.defaultPageSize), 1, MAX_MANUAL_PAGE_SIZE),
    defaultIntervalMinutes: clamp(
      parseInteger(row.default_interval_minutes, DEFAULT_SETTINGS.defaultIntervalMinutes),
      5,
      1440
    ),
    strongDealMinDiscount: clamp(
      parseNumber(row.strong_deal_min_discount, DEFAULT_SETTINGS.strongDealMinDiscount),
      0,
      95
    ),
    strongDealMinComparisonGapPct: clamp(
      parseNumber(row.strong_deal_min_comparison_gap_pct, DEFAULT_SETTINGS.strongDealMinComparisonGapPct),
      0,
      95
    ),
    goodRatingThreshold: clamp(parseNumber(row.good_rating_threshold, DEFAULT_SETTINGS.goodRatingThreshold), 1, 5),
    alertTelegramEnabled: parseBool(row.alert_telegram_enabled, DEFAULT_SETTINGS.alertTelegramEnabled),
    alertInternalEnabled: parseBool(row.alert_internal_enabled, DEFAULT_SETTINGS.alertInternalEnabled),
    alertWhatsappPlaceholderEnabled: parseBool(
      row.alert_whatsapp_placeholder_enabled,
      DEFAULT_SETTINGS.alertWhatsappPlaceholderEnabled
    ),
    alertCooldownMinutes: clamp(
      parseInteger(row.alert_cooldown_minutes, DEFAULT_SETTINGS.alertCooldownMinutes),
      5,
      24 * 60
    ),
    alertMaxPerProduct: clamp(parseInteger(row.alert_max_per_product, DEFAULT_SETTINGS.alertMaxPerProduct), 1, 20),
    telegramMessagePrefix: cleanText(row.telegram_message_prefix) || DEFAULT_SETTINGS.telegramMessagePrefix,
    comparisonSourceConfig,
    loggingEnabled: parseBool(row.logging_enabled, DEFAULT_SETTINGS.loggingEnabled),
    estimatedTokensPerManualRun: clamp(
      parseInteger(row.estimated_tokens_per_manual_run, DEFAULT_SETTINGS.estimatedTokensPerManualRun),
      1,
      250
    ),
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null
  };
}

export function getKeepaSettings() {
  return normalizeSettingsRow(getKeepaSettingsRow());
}

function getDomainInfo(domainId) {
  return DOMAIN_OPTIONS.find((item) => item.id === Number(domainId)) || DOMAIN_OPTIONS[2];
}

function buildAmazonProductUrl(asin, domainId) {
  const domainInfo = getDomainInfo(domainId);
  return asin ? `https://www.${domainInfo.host}/dp/${asin}` : '';
}

function buildAmazonImageUrl(imageValue) {
  const cleaned = cleanText(imageValue);
  if (!cleaned) {
    return '';
  }

  if (/^https?:\/\//i.test(cleaned)) {
    return cleaned;
  }

  const firstImage = cleaned.split(',').map((item) => item.trim()).filter(Boolean)[0];
  if (!firstImage) {
    return '';
  }

  return `https://m.media-amazon.com/images/I/${firstImage}`;
}

function normalizeRating(value) {
  const parsed = parseNumber(value, null);
  if (parsed === null || parsed <= 0) {
    return null;
  }

  return parsed > 5 ? Math.round(parsed) / 10 : parsed;
}

function formatCurrency(value) {
  const parsed = parseNumber(value, null);
  if (parsed === null) {
    return '-';
  }

  return new Intl.NumberFormat('de-DE', {
    style: 'currency',
    currency: 'EUR'
  }).format(parsed);
}

export function getKeepaSettingsView() {
  const settings = getKeepaSettings();
  const keepaConfig = getKeepaConfig();
  const telegramConfig = getTelegramConfig();
  const domainInfo = getDomainInfo(settings.domainId);

  return {
    ...settings,
    domainLabel: domainInfo.label,
    domainOptions: DOMAIN_OPTIONS,
    categoryCatalog: CATEGORY_CATALOG,
    comparisonAdapters: getComparisonAdapterCatalog(settings.comparisonSourceConfig),
    keepaKeyStatus: keepaConfig.key
      ? {
          connected: true,
          masked: maskSecret(keepaConfig.key, { visibleStart: 6, visibleEnd: 4 })
        }
      : {
          connected: false,
          masked: ''
        },
    telegramConfigStatus: {
      botTokenConfigured: Boolean(telegramConfig.token),
      chatIdConfigured: Boolean(telegramConfig.chatId),
      maskedChatId: telegramConfig.chatId ? maskSecret(telegramConfig.chatId, { visibleStart: 3, visibleEnd: 2 }) : ''
    },
    fakeDropSettings: getFakeDropSettingsView()
  };
}

function logKeepaEvent(level, eventType, source, message, extra = {}) {
  const settings = getKeepaSettings();
  if (!settings.loggingEnabled && level !== 'error') {
    return;
  }

  db.prepare(
    `
      INSERT INTO keepa_logs (
        level,
        event_type,
        source,
        message,
        filters_json,
        result_count,
        tokens_left,
        tokens_consumed,
        payload_json,
        created_at
      ) VALUES (
        @level,
        @eventType,
        @source,
        @message,
        @filtersJson,
        @resultCount,
        @tokensLeft,
        @tokensConsumed,
        @payloadJson,
        @createdAt
      )
    `
  ).run({
    level,
    eventType,
    source,
    message,
    filtersJson: extra.filters ? toJson(extra.filters) : null,
    resultCount: extra.resultCount ?? null,
    tokensLeft: extra.tokensLeft ?? null,
    tokensConsumed: extra.tokensConsumed ?? null,
    payloadJson: extra.payload ? toJson(extra.payload) : null,
    createdAt: nowIso()
  });
}

function buildKeepaRequestError(error, fallbackMessage) {
  if (error instanceof Error) {
    return error.message;
  }

  return fallbackMessage;
}

function pickNumericField(source, keys = []) {
  for (const key of keys) {
    if (source && source[key] !== undefined && source[key] !== null && source[key] !== '') {
      const parsed = parseNumber(source[key], null);
      if (parsed !== null) {
        return parsed;
      }
    }
  }

  return null;
}

function extractAsin(source = {}) {
  const asin = cleanText(source.asin || source.ASIN || source.parentAsin);
  return /^[A-Z0-9]{10}$/i.test(asin) ? asin.toUpperCase() : '';
}

function extractOfferPrice(offer) {
  const csv = Array.isArray(offer?.offerCSV) ? offer.offerCSV : [];
  if (!csv.length) {
    return null;
  }

  let latestPrice = null;
  for (let index = 1; index < csv.length; index += 3) {
    const price = parseNumber(csv[index], null);
    if (price !== null && price >= 0) {
      latestPrice = price;
    }
  }

  return fromMinorUnits(latestPrice);
}

function extractOfferStock(offer) {
  const stockCsv = Array.isArray(offer?.stockCSV) ? offer.stockCSV : [];
  if (!stockCsv.length) {
    return null;
  }

  let latestStock = null;
  for (let index = 1; index < stockCsv.length; index += 2) {
    const stock = parseInteger(stockCsv[index], null);
    if (stock !== null && stock >= 0) {
      latestStock = stock;
    }
  }

  return latestStock;
}

function getOfferSummary(product) {
  const offers = Array.isArray(product?.offers) ? product.offers : [];
  const indices =
    Array.isArray(product?.liveOffersOrder) && product.liveOffersOrder.length
      ? product.liveOffersOrder
      : offers.map((_, index) => index);

  let bestOffer = null;

  indices.forEach((offerIndex) => {
    const offer = offers[offerIndex];
    if (!offer) {
      return;
    }

    const price = extractOfferPrice(offer);
    if (price === null) {
      return;
    }

    const candidate = {
      price,
      isPrime: Boolean(offer.isPrime),
      isAmazon: Boolean(offer.isAmazon),
      isFBA: Boolean(offer.isFBA),
      stock: extractOfferStock(offer)
    };

    if (!bestOffer || candidate.price < bestOffer.price) {
      bestOffer = candidate;
    }
  });

  if (!bestOffer) {
    return {
      price: null,
      sellerType: 'UNKNOWN',
      isPrime: false,
      inStock: false
    };
  }

  return {
    price: bestOffer.price,
    sellerType: bestOffer.isAmazon ? 'AMAZON' : bestOffer.isFBA ? 'FBA' : 'FBM',
    isPrime: bestOffer.isPrime,
    inStock: bestOffer.stock === null ? true : bestOffer.stock > 0
  };
}

function normalizeCategory(product, deal) {
  const categoryTree = Array.isArray(product?.categoryTree) ? product.categoryTree : [];
  if (categoryTree.length) {
    const rootCategory = categoryTree[0];
    return {
      categoryId: parseInteger(rootCategory?.catId || product?.rootCategory || deal?.rootCategory, null),
      categoryName:
        cleanText(rootCategory?.name || deal?.categoryName || deal?.rootCategoryName || product?.productGroup) || null
    };
  }

  return {
    categoryId: parseInteger(product?.rootCategory || deal?.rootCategory || deal?.categoryId, null),
    categoryName: cleanText(deal?.categoryName || deal?.rootCategoryName || product?.productGroup) || null
  };
}

function buildReferencePrice(currentPrice, discountPercent, deal, product) {
  const explicitReference = pickNumericField(deal, [
    'regularPrice',
    'referencePrice',
    'highestPrice',
    'listPrice',
    'avgPrice',
    'avg90'
  ]);

  if (explicitReference !== null) {
    return explicitReference >= 100 ? fromMinorUnits(explicitReference) : explicitReference;
  }

  const productReference = pickNumericField(product, ['listPrice']);
  if (productReference !== null && productReference > 0) {
    return productReference >= 100 ? fromMinorUnits(productReference) : productReference;
  }

  if (currentPrice !== null && discountPercent !== null && discountPercent > 0 && discountPercent < 100) {
    return Math.round((currentPrice / (1 - discountPercent / 100)) * 100) / 100;
  }

  return null;
}

function mergeExistingComparison(item, existingRow) {
  if (!existingRow) {
    return item;
  }

  if (item.comparisonPrice !== null || item.comparisonSource) {
    return item;
  }

  return {
    ...item,
    comparisonSource: cleanText(existingRow.comparison_source) || item.comparisonSource,
    comparisonStatus: cleanText(existingRow.comparison_status) || item.comparisonStatus,
    comparisonPrice: parseNumber(existingRow.comparison_price, item.comparisonPrice),
    comparisonPayload: fromJson(existingRow.comparison_payload_json, null) || item.comparisonPayload
  };
}

function computeDealScore(item, settings, rule = null) {
  let score = 0;
  const reasons = [];
  const discount = parseNumber(item.keepaDiscount, 0);

  score += Math.min(45, discount * 0.75);
  reasons.push(`Keepa-Rabatt ${discount.toFixed(0)}%`);

  if (item.sellerType === 'AMAZON') {
    score += 14;
    reasons.push('Verkauf direkt ueber Amazon');
  } else if (item.sellerType === 'FBA') {
    score += 10;
    reasons.push('FBA-Angebot');
  } else if (item.sellerType === 'FBM') {
    score += 6;
    reasons.push('FBM-Angebot');
  }

  if (item.isPrime) {
    score += 6;
    reasons.push('Prime geeignet');
  }

  if (item.isInStock) {
    score += 6;
    reasons.push('Lagernd');
  } else {
    score -= 10;
    reasons.push('Nicht lagernd');
  }

  if (item.rating !== null) {
    if (item.rating >= 4.5) {
      score += 10;
    } else if (item.rating >= settings.goodRatingThreshold) {
      score += 7;
    } else if (item.rating >= 3.5) {
      score += 4;
    } else {
      score -= 5;
    }

    reasons.push(`Bewertung ${item.rating.toFixed(1)}/5`);
  }

  if (item.priceDifferencePct !== null && item.priceDifferencePct > 0) {
    score += Math.min(20, item.priceDifferencePct * 0.75);
    reasons.push(`Vergleichspreis +${item.priceDifferencePct.toFixed(1)}%`);
  } else if (item.comparisonStatus === 'not_connected') {
    reasons.push('Keine legale Vergleichsquelle verbunden');
  }

  const minDealScore = rule?.minDealScore ?? 70;
  const isStrong =
    discount >= settings.strongDealMinDiscount &&
    item.priceDifferencePct !== null &&
    item.priceDifferencePct >= settings.strongDealMinComparisonGapPct &&
    score >= minDealScore;

  const finalScore = clamp(Math.round(score), 0, 100);
  let dealStrength = 'pruefenswert';

  if (isStrong) {
    dealStrength = 'stark';
  } else if (finalScore < 45 || discount < Math.max(15, settings.defaultDiscount / 2)) {
    dealStrength = 'verwerfen';
  }

  return {
    dealScore: finalScore,
    dealStrength,
    strengthReason: reasons.join(' | ')
  };
}

function applyManualFilters(items, filters, settings) {
  return items.filter((item) => {
    if (filters.sellerType !== 'ALL' && item.sellerType !== filters.sellerType) {
      return false;
    }

    if (filters.onlyPrime && !item.isPrime) {
      return false;
    }

    if (filters.onlyInStock && !item.isInStock) {
      return false;
    }

    if (filters.onlyGoodRating && (item.rating === null || item.rating < settings.goodRatingThreshold)) {
      return false;
    }

    if (filters.minPrice !== null && item.currentPrice !== null && item.currentPrice < filters.minPrice) {
      return false;
    }

    if (filters.maxPrice !== null && item.currentPrice !== null && item.currentPrice > filters.maxPrice) {
      return false;
    }

    return true;
  });
}

function mapExistingResultRow(row) {
  return row
    ? {
        ...row,
        search_payload: fromJson(row.search_payload_json, null),
        comparison_payload: fromJson(row.comparison_payload_json, null),
        keepa_payload: fromJson(row.keepa_payload_json, null)
      }
    : null;
}

function buildResultDto(row) {
  if (!row) {
    return null;
  }

  const fakeDrop = getFakeDropSnapshotForResult(row.id);

  return {
    id: row.id,
    asin: row.asin,
    domainId: row.domain_id,
    title: row.title,
    productUrl: row.product_url,
    imageUrl: row.image_url,
    currentPrice: parseNumber(row.current_price, null),
    referencePrice: parseNumber(row.reference_price, null),
    referenceLabel: row.reference_label,
    keepaDiscount: parseNumber(row.keepa_discount, null),
    sellerType: row.seller_type,
    categoryId: parseInteger(row.category_id, null),
    categoryName: row.category_name,
    rating: normalizeRating(row.rating),
    reviewCount: parseInteger(row.review_count, null),
    isPrime: row.is_prime === 1,
    isInStock: row.is_in_stock === 1,
    dealScore: parseNumber(row.deal_score, 0),
    dealStrength: normalizeDealStrength(row.deal_strength),
    strengthReason: row.strength_reason,
    workflowStatus: normalizeWorkflowStatus(row.workflow_status),
    comparisonSource: row.comparison_source,
    comparisonStatus: row.comparison_status,
    comparisonPrice: parseNumber(row.comparison_price, null),
    priceDifferenceAbs: parseNumber(row.price_difference_abs, null),
    priceDifferencePct: parseNumber(row.price_difference_pct, null),
    comparisonCheckedAt: row.comparison_checked_at,
    comparisonPayload: fromJson(row.comparison_payload_json, null),
    keepaPayload: fromJson(row.keepa_payload_json, null),
    searchPayload: fromJson(row.search_payload_json, null),
    origin: row.origin,
    ruleId: row.rule_id,
    note: row.note || '',
    alertCount: parseInteger(row.alert_count, 0),
    lastAlertedAt: row.last_alerted_at,
    firstSeenAt: row.first_seen_at,
    lastSeenAt: row.last_seen_at,
    lastSyncedAt: row.last_synced_at,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    fakeDrop
  };
}

function getExistingResult(asin, domainId) {
  return mapExistingResultRow(
    db
      .prepare(`SELECT * FROM keepa_results WHERE asin = ? AND domain_id = ? LIMIT 1`)
      .get(asin, domainId)
  );
}

async function keepaRequest(path, params = {}, options = {}) {
  const keepaConfig = getKeepaConfig();
  const key = cleanText(keepaConfig.key);
  if (!key) {
    throw new Error('KEEPA_API_KEY fehlt im Backend.');
  }

  const usageModule = normalizeUsageModule(
    options.module || options.source || (path === '/token' ? 'status-check' : 'manual-search'),
    path === '/token' ? 'status-check' : 'manual-search'
  );

  return queueKeepaTask(async () => {
    let lastError;
    let lastDurationMs = 0;

    for (let attempt = 1; attempt <= keepaConfig.retryLimit; attempt += 1) {
      const controller = new AbortController();
      const timeoutHandle = setTimeout(() => controller.abort(), keepaConfig.timeoutMs);
      const startedAt = Date.now();

      try {
        const searchParams = new URLSearchParams({
          key,
          ...Object.fromEntries(Object.entries(params).filter(([, value]) => value !== undefined && value !== null && value !== ''))
        });
        const response = await fetch(`https://api.keepa.com${path}?${searchParams.toString()}`, {
          method: 'GET',
          signal: controller.signal
        });
        lastDurationMs = Date.now() - startedAt;
        const rawText = await response.text();
        const data = rawText ? JSON.parse(rawText) : {};
        const keepaError =
          typeof data?.error === 'string'
            ? data.error
            : data?.error?.message || data?.error?.type || data?.message || '';

        if (!response.ok || keepaError) {
          throw new Error(keepaError || `Keepa-Request ${path} fehlgeschlagen (${response.status}).`);
        }

        const currentTokensLeft = parseInteger(data?.tokensLeft, null);
        const previousTokensLeft = parseInteger(keepaConnectionCache?.tokensLeft, null);
        const officialUsageValue =
          currentTokensLeft !== null && previousTokensLeft !== null && previousTokensLeft >= currentTokensLeft
            ? previousTokensLeft - currentTokensLeft
            : null;
        const usage = recordKeepaUsage({
          action: 'keepa-request',
          module: usageModule,
          filters: options.filters,
          resultCount: Array.isArray(data?.products) ? data.products.length : Array.isArray(data?.dr) ? data.dr.length : 0,
          durationMs: lastDurationMs,
          requestStatus: 'success',
          estimatedUsage: 0,
          officialUsageValue,
          officialTokensLeft: currentTokensLeft,
          ruleId: options.ruleId ?? null,
          meta: {
            endpoint: path,
            attempt,
            refillIn: parseInteger(data?.refillIn, null),
            refillRate: parseInteger(data?.refillRate, null)
          }
        });

        if (currentTokensLeft !== null) {
          keepaConnectionCache = {
            ...(keepaConnectionCache || {}),
            connected: true,
            checkedAt: nowIso(),
            tokensLeft: currentTokensLeft,
            refillRate: parseInteger(data?.refillRate, keepaConnectionCache?.refillRate ?? null),
            refillInMs: parseInteger(data?.refillIn, keepaConnectionCache?.refillInMs ?? null),
            tokensConsumed: parseInteger(data?.tokensConsumed, keepaConnectionCache?.tokensConsumed ?? null)
          };
        }

        logKeepaEvent('info', 'keepa_api_request', options.source || path, `Keepa Request ${path} erfolgreich.`, {
          filters: options.filters,
          resultCount: Array.isArray(data?.products) ? data.products.length : Array.isArray(data?.dr) ? data.dr.length : null,
          tokensLeft: parseInteger(data?.tokensLeft, null),
          tokensConsumed: parseInteger(data?.tokensConsumed, null),
          payload: {
            endpoint: path,
            refillIn: data?.refillIn,
            refillRate: data?.refillRate
          }
        });

        return {
          data,
          usage
        };
      } catch (error) {
        lastError = error;
        lastDurationMs = Date.now() - startedAt;
        if (attempt >= keepaConfig.retryLimit) {
          break;
        }
        await delay(500 * attempt);
      } finally {
        clearTimeout(timeoutHandle);
      }
    }

    recordKeepaUsage({
      action: 'keepa-request',
      module: usageModule,
      filters: options.filters,
      durationMs: lastDurationMs,
      requestStatus: 'error',
      estimatedUsage: 0,
      ruleId: options.ruleId ?? null,
      errorMessage: buildKeepaRequestError(lastError, 'Unbekannter Keepa-Fehler'),
      meta: {
        endpoint: path,
        retryLimit: keepaConfig.retryLimit
      }
    });

    logKeepaEvent('error', 'keepa_api_error', options.source || path, `Keepa Request ${path} fehlgeschlagen.`, {
      filters: options.filters,
      payload: {
        endpoint: path,
        message: buildKeepaRequestError(lastError, 'Unbekannter Keepa-Fehler')
      }
    });

    throw (lastError instanceof Error ? lastError : new Error('Keepa-Request fehlgeschlagen.'));
  });
}

async function loadKeepaConnectionStatus(force = false, reason = 'status-check') {
  const keepaConfig = getKeepaConfig();
  if (!keepaConfig.key) {
    throw new Error('KEEPA_API_KEY ist nicht im Backend gesetzt.');
  }

  if (
    !force &&
    keepaConnectionCache &&
    Date.now() - new Date(keepaConnectionCache.checkedAt).getTime() < 60 * 1000
  ) {
    return keepaConnectionCache;
  }

  const requestStartedAt = Date.now();
  const tokenResponse = await keepaRequest('/token', {}, { source: reason, module: reason });
  const tokenStatus = tokenResponse.data;
  keepaConnectionCache = {
    connected: true,
    checkedAt: nowIso(),
    keyMasked: maskSecret(keepaConfig.key, { visibleStart: 6, visibleEnd: 4 }),
    tokensLeft: parseInteger(tokenStatus.tokensLeft, 0),
    refillRate: parseInteger(tokenStatus.refillRate, 0),
    refillInMs: parseInteger(tokenStatus.refillIn, 0),
    tokensConsumed: parseInteger(tokenStatus.tokensConsumed, 0)
  };

  if (reason === 'test-connection') {
    recordKeepaUsage({
      action: 'test-connection',
      module: 'test-connection',
      durationMs: Date.now() - requestStartedAt,
      requestStatus: 'success',
      estimatedUsage: tokenResponse.usage?.officialUsageValue ?? 1,
      officialUsageValue: tokenResponse.usage?.officialUsageValue ?? null,
      officialTokensLeft: keepaConnectionCache.tokensLeft,
      meta: {
        refillRate: keepaConnectionCache.refillRate,
        refillInMs: keepaConnectionCache.refillInMs
      }
    });
  }

  return keepaConnectionCache;
}

export async function testKeepaConnection() {
  const startedAt = Date.now();

  try {
    return await loadKeepaConnectionStatus(true, 'test-connection');
  } catch (error) {
    recordKeepaUsage({
      action: 'test-connection',
      module: 'test-connection',
      durationMs: Date.now() - startedAt,
      requestStatus: 'error',
      estimatedUsage: 0,
      errorMessage: buildKeepaRequestError(error, 'Keepa-Verbindung fehlgeschlagen.')
    });
    throw error;
  }
}

export function saveKeepaSettings(input = {}) {
  const current = getKeepaSettings();
  const next = {
    keepaEnabled: input.keepaEnabled === undefined ? current.keepaEnabled : parseBool(input.keepaEnabled),
    schedulerEnabled: input.schedulerEnabled === undefined ? current.schedulerEnabled : parseBool(input.schedulerEnabled),
    domainId:
      input.domainId === undefined
        ? current.domainId
        : DOMAIN_OPTIONS.some((item) => item.id === Number(input.domainId))
          ? Number(input.domainId)
          : current.domainId,
    defaultCategories:
      input.defaultCategories === undefined
        ? current.defaultCategories
        : normalizeCategoryIds(input.defaultCategories, current.defaultCategories),
    defaultDiscount:
      input.defaultDiscount === undefined
        ? current.defaultDiscount
        : clamp(parseNumber(input.defaultDiscount, current.defaultDiscount), 0, 95),
    defaultSellerType:
      input.defaultSellerType === undefined
        ? current.defaultSellerType
        : normalizeSellerType(input.defaultSellerType),
    defaultMinPrice:
      input.defaultMinPrice === undefined ? current.defaultMinPrice : sanitizePriceBoundary(input.defaultMinPrice),
    defaultMaxPrice:
      input.defaultMaxPrice === undefined ? current.defaultMaxPrice : sanitizePriceBoundary(input.defaultMaxPrice),
    defaultPageSize:
      input.defaultPageSize === undefined
        ? current.defaultPageSize
        : clamp(parseInteger(input.defaultPageSize, current.defaultPageSize), 1, MAX_MANUAL_PAGE_SIZE),
    defaultIntervalMinutes:
      input.defaultIntervalMinutes === undefined
        ? current.defaultIntervalMinutes
        : clamp(parseInteger(input.defaultIntervalMinutes, current.defaultIntervalMinutes), 5, 1440),
    strongDealMinDiscount:
      input.strongDealMinDiscount === undefined
        ? current.strongDealMinDiscount
        : clamp(parseNumber(input.strongDealMinDiscount, current.strongDealMinDiscount), 0, 95),
    strongDealMinComparisonGapPct:
      input.strongDealMinComparisonGapPct === undefined
        ? current.strongDealMinComparisonGapPct
        : clamp(parseNumber(input.strongDealMinComparisonGapPct, current.strongDealMinComparisonGapPct), 0, 95),
    goodRatingThreshold:
      input.goodRatingThreshold === undefined
        ? current.goodRatingThreshold
        : clamp(parseNumber(input.goodRatingThreshold, current.goodRatingThreshold), 1, 5),
    alertTelegramEnabled:
      input.alertTelegramEnabled === undefined ? current.alertTelegramEnabled : parseBool(input.alertTelegramEnabled),
    alertInternalEnabled:
      input.alertInternalEnabled === undefined ? current.alertInternalEnabled : parseBool(input.alertInternalEnabled),
    alertWhatsappPlaceholderEnabled:
      input.alertWhatsappPlaceholderEnabled === undefined
        ? current.alertWhatsappPlaceholderEnabled
        : parseBool(input.alertWhatsappPlaceholderEnabled),
    alertCooldownMinutes:
      input.alertCooldownMinutes === undefined
        ? current.alertCooldownMinutes
        : clamp(parseInteger(input.alertCooldownMinutes, current.alertCooldownMinutes), 5, 24 * 60),
    alertMaxPerProduct:
      input.alertMaxPerProduct === undefined
        ? current.alertMaxPerProduct
        : clamp(parseInteger(input.alertMaxPerProduct, current.alertMaxPerProduct), 1, 20),
    telegramMessagePrefix:
      input.telegramMessagePrefix === undefined
        ? current.telegramMessagePrefix
        : cleanText(input.telegramMessagePrefix) || DEFAULT_SETTINGS.telegramMessagePrefix,
    comparisonSourceConfig:
      input.comparisonSourceConfig === undefined
        ? current.comparisonSourceConfig
        : normalizeComparisonSourceConfig(input.comparisonSourceConfig, current.comparisonSourceConfig),
    loggingEnabled: input.loggingEnabled === undefined ? current.loggingEnabled : parseBool(input.loggingEnabled),
    estimatedTokensPerManualRun:
      input.estimatedTokensPerManualRun === undefined
        ? current.estimatedTokensPerManualRun
        : clamp(parseInteger(input.estimatedTokensPerManualRun, current.estimatedTokensPerManualRun), 1, 250)
  };

  if (
    next.defaultMinPrice !== null &&
    next.defaultMaxPrice !== null &&
    Number(next.defaultMinPrice) > Number(next.defaultMaxPrice)
  ) {
    throw new Error('Der Mindestpreis darf nicht groesser als der Hoechstpreis sein.');
  }

  db.prepare(
    `
      UPDATE keepa_settings
      SET keepa_enabled = @keepaEnabled,
          scheduler_enabled = @schedulerEnabled,
          domain_id = @domainId,
          default_categories_json = @defaultCategoriesJson,
          default_discount = @defaultDiscount,
          default_seller_type = @defaultSellerType,
          default_min_price = @defaultMinPrice,
          default_max_price = @defaultMaxPrice,
          default_page_size = @defaultPageSize,
          default_interval_minutes = @defaultIntervalMinutes,
          strong_deal_min_discount = @strongDealMinDiscount,
          strong_deal_min_comparison_gap_pct = @strongDealMinComparisonGapPct,
          good_rating_threshold = @goodRatingThreshold,
          alert_telegram_enabled = @alertTelegramEnabled,
          alert_internal_enabled = @alertInternalEnabled,
          alert_whatsapp_placeholder_enabled = @alertWhatsappPlaceholderEnabled,
          alert_cooldown_minutes = @alertCooldownMinutes,
          alert_max_per_product = @alertMaxPerProduct,
          telegram_message_prefix = @telegramMessagePrefix,
          comparison_source_config_json = @comparisonSourceConfigJson,
          logging_enabled = @loggingEnabled,
          estimated_tokens_per_manual_run = @estimatedTokensPerManualRun,
          updated_at = @updatedAt
      WHERE id = 1
    `
  ).run({
    keepaEnabled: next.keepaEnabled ? 1 : 0,
    schedulerEnabled: next.schedulerEnabled ? 1 : 0,
    domainId: next.domainId,
    defaultCategoriesJson: toJson(next.defaultCategories),
    defaultDiscount: next.defaultDiscount,
    defaultSellerType: next.defaultSellerType,
    defaultMinPrice: next.defaultMinPrice,
    defaultMaxPrice: next.defaultMaxPrice,
    defaultPageSize: next.defaultPageSize,
    defaultIntervalMinutes: next.defaultIntervalMinutes,
    strongDealMinDiscount: next.strongDealMinDiscount,
    strongDealMinComparisonGapPct: next.strongDealMinComparisonGapPct,
    goodRatingThreshold: next.goodRatingThreshold,
    alertTelegramEnabled: next.alertTelegramEnabled ? 1 : 0,
    alertInternalEnabled: next.alertInternalEnabled ? 1 : 0,
    alertWhatsappPlaceholderEnabled: next.alertWhatsappPlaceholderEnabled ? 1 : 0,
    alertCooldownMinutes: next.alertCooldownMinutes,
    alertMaxPerProduct: next.alertMaxPerProduct,
    telegramMessagePrefix: next.telegramMessagePrefix,
    comparisonSourceConfigJson: toJson(next.comparisonSourceConfig),
    loggingEnabled: next.loggingEnabled ? 1 : 0,
    estimatedTokensPerManualRun: next.estimatedTokensPerManualRun,
    updatedAt: nowIso()
  });

  logKeepaEvent('info', 'settings_saved', 'settings', 'Keepa-Einstellungen aktualisiert.', {
    payload: {
      keepaEnabled: next.keepaEnabled,
      schedulerEnabled: next.schedulerEnabled,
      domainId: next.domainId
    }
  });

  return getKeepaSettingsView();
}

function normalizeRuleInput(input, existingRule = null) {
  const now = nowIso();
  const settings = getKeepaSettings();
  const minPrice = sanitizePriceBoundary(input.minPrice);
  const maxPrice = sanitizePriceBoundary(input.maxPrice);
  const intervalMinutes = clamp(
    parseInteger(input.intervalMinutes, existingRule?.interval_minutes ?? settings.defaultIntervalMinutes),
    5,
    1440
  );
  const nextRunAt = new Date(Date.now() + intervalMinutes * 60 * 1000).toISOString();

  if (minPrice !== null && maxPrice !== null && minPrice > maxPrice) {
    throw new Error('Der Mindestpreis darf nicht groesser als der Hoechstpreis sein.');
  }

  return {
    name: cleanText(input.name) || `Regel ${now.slice(0, 16)}`,
    min_discount: clamp(parseNumber(input.minDiscount, existingRule?.min_discount ?? settings.defaultDiscount), 0, 95),
    seller_type: normalizeSellerType(input.sellerType ?? existingRule?.seller_type ?? settings.defaultSellerType),
    categories_json: toJson(
      normalizeCategoryIds(
        input.categories,
        fromJson(existingRule?.categories_json, settings.defaultCategories) || settings.defaultCategories
      )
    ),
    min_price: minPrice,
    max_price: maxPrice,
    min_deal_score: clamp(parseNumber(input.minDealScore, existingRule?.min_deal_score ?? 70), 0, 100),
    interval_minutes: intervalMinutes,
    only_prime: parseBool(input.onlyPrime, existingRule?.only_prime === 1),
    only_in_stock: parseBool(input.onlyInStock, existingRule?.only_in_stock !== 0),
    only_good_rating: parseBool(input.onlyGoodRating, existingRule?.only_good_rating === 1),
    comparison_sources_json: toJson(
      normalizeComparisonSourceIds(
        input.comparisonSources,
        fromJson(existingRule?.comparison_sources_json, ['manual-source']) || ['manual-source']
      )
    ),
    is_active: parseBool(input.isActive, existingRule?.is_active !== 0),
    last_run_at: existingRule?.last_run_at || null,
    next_run_at: parseBool(input.isActive, existingRule?.is_active !== 0) ? nextRunAt : null,
    created_at: existingRule?.created_at || now,
    updated_at: now
  };
}

function mapRuleRow(row) {
  const categories = fromJson(row.categories_json, []) || [];
  const comparisonSources = fromJson(row.comparison_sources_json, []) || [];

  return {
    id: row.id,
    name: row.name,
    minDiscount: parseNumber(row.min_discount, 0),
    sellerType: row.seller_type,
    categories,
    minPrice: parseNumber(row.min_price, null),
    maxPrice: parseNumber(row.max_price, null),
    minDealScore: parseNumber(row.min_deal_score, 0),
    intervalMinutes: parseInteger(row.interval_minutes, 60),
    onlyPrime: row.only_prime === 1,
    onlyInStock: row.only_in_stock === 1,
    onlyGoodRating: row.only_good_rating === 1,
    comparisonSources,
    isActive: row.is_active === 1,
    lastRunAt: row.last_run_at,
    nextRunAt: row.next_run_at,
    createdAt: row.created_at,
    updatedAt: row.updated_at
  };
}

function getKeepaRuleStats(ruleId) {
  const usageTotals =
    db
      .prepare(
        `
          SELECT
            COALESCE(SUM(estimated_usage), 0) AS estimatedUsageTotal,
            MAX(created_at) AS lastTrackedAt
          FROM keepa_usage_logs
          WHERE rule_id = ?
            AND action = 'automation-run'
        `
      )
      .get(ruleId) || {};
  const latestUsage =
    db
      .prepare(
        `
          SELECT
            result_count,
            duration_ms,
            estimated_usage,
            request_status,
            created_at
          FROM keepa_usage_logs
          WHERE rule_id = ?
            AND action = 'automation-run'
          ORDER BY created_at DESC
          LIMIT 1
        `
      )
      .get(ruleId) || {};
  const totalHits = db.prepare(`SELECT COUNT(*) AS count FROM keepa_results WHERE rule_id = ?`).get(ruleId)?.count || 0;
  const alertsSent =
    db
      .prepare(`SELECT COUNT(*) AS count FROM keepa_alerts WHERE rule_id = ? AND status IN ('sent', 'stored')`)
      .get(ruleId)?.count || 0;

  return {
    totalHits: parseInteger(totalHits, 0),
    alertsSent: parseInteger(alertsSent, 0),
    estimatedUsageTotal: Math.round((parseNumber(usageTotals.estimatedUsageTotal, 0) || 0) * 10) / 10,
    lastResultCount: parseInteger(latestUsage.result_count, 0),
    lastDurationMs: parseInteger(latestUsage.duration_ms, 0),
    lastEstimatedUsage: parseNumber(latestUsage.estimated_usage, null),
    lastRunStatus: normalizeUsageStatus(latestUsage.request_status, 'success'),
    lastTrackedAt: latestUsage.created_at || usageTotals.lastTrackedAt || null
  };
}

export function listKeepaRules() {
  return db
    .prepare(`SELECT * FROM keepa_rules ORDER BY is_active DESC, updated_at DESC`)
    .all()
    .map(mapRuleRow)
    .map((rule) => ({
      ...rule,
      ...getKeepaRuleStats(rule.id)
    }));
}

export function createKeepaRule(input = {}) {
  const payload = normalizeRuleInput(input);
  const result = db
    .prepare(
      `
        INSERT INTO keepa_rules (
          name,
          min_discount,
          seller_type,
          categories_json,
          min_price,
          max_price,
          min_deal_score,
          interval_minutes,
          only_prime,
          only_in_stock,
          only_good_rating,
          comparison_sources_json,
          is_active,
          last_run_at,
          next_run_at,
          created_at,
          updated_at
        ) VALUES (
          @name,
          @min_discount,
          @seller_type,
          @categories_json,
          @min_price,
          @max_price,
          @min_deal_score,
          @interval_minutes,
          @only_prime,
          @only_in_stock,
          @only_good_rating,
          @comparison_sources_json,
          @is_active,
          @last_run_at,
          @next_run_at,
          @created_at,
          @updated_at
        )
      `
    )
    .run({
      ...payload,
      only_prime: payload.only_prime ? 1 : 0,
      only_in_stock: payload.only_in_stock ? 1 : 0,
      only_good_rating: payload.only_good_rating ? 1 : 0,
      is_active: payload.is_active ? 1 : 0
    });

  logKeepaEvent('info', 'rule_created', 'rules', `Keepa-Regel "${payload.name}" angelegt.`, {
    payload: {
      ruleId: result.lastInsertRowid
    }
  });

  return listKeepaRules().find((item) => item.id === Number(result.lastInsertRowid));
}

export function updateKeepaRule(id, input = {}) {
  const existing = db.prepare(`SELECT * FROM keepa_rules WHERE id = ?`).get(id);
  if (!existing) {
    throw new Error('Die angeforderte Keepa-Regel wurde nicht gefunden.');
  }

  const payload = normalizeRuleInput(input, existing);
  db.prepare(
    `
      UPDATE keepa_rules
      SET name = @name,
          min_discount = @min_discount,
          seller_type = @seller_type,
          categories_json = @categories_json,
          min_price = @min_price,
          max_price = @max_price,
          min_deal_score = @min_deal_score,
          interval_minutes = @interval_minutes,
          only_prime = @only_prime,
          only_in_stock = @only_in_stock,
          only_good_rating = @only_good_rating,
          comparison_sources_json = @comparison_sources_json,
          is_active = @is_active,
          next_run_at = @next_run_at,
          updated_at = @updated_at
      WHERE id = @id
    `
  ).run({
    id,
    ...payload,
    only_prime: payload.only_prime ? 1 : 0,
    only_in_stock: payload.only_in_stock ? 1 : 0,
    only_good_rating: payload.only_good_rating ? 1 : 0,
    is_active: payload.is_active ? 1 : 0
  });

  logKeepaEvent('info', 'rule_updated', 'rules', `Keepa-Regel ${id} aktualisiert.`, {
    payload: {
      ruleId: id
    }
  });

  return listKeepaRules().find((item) => item.id === Number(id));
}

function normalizeManualSearchInput(input = {}) {
  const settings = getKeepaSettings();
  const page = clamp(parseInteger(input.page, 1), 1, MAX_MANUAL_PAGE);
  const limit = clamp(parseInteger(input.limit, settings.defaultPageSize), 1, MAX_MANUAL_PAGE_SIZE);
  const minPrice = sanitizePriceBoundary(input.minPrice ?? settings.defaultMinPrice);
  const maxPrice = sanitizePriceBoundary(input.maxPrice ?? settings.defaultMaxPrice);

  if (minPrice !== null && maxPrice !== null && minPrice > maxPrice) {
    throw new Error('Der Mindestpreis darf nicht groesser als der Hoechstpreis sein.');
  }

  return {
    page,
    limit,
    domainId:
      DOMAIN_OPTIONS.some((item) => item.id === Number(input.domainId || settings.domainId))
        ? Number(input.domainId || settings.domainId)
        : settings.domainId,
    minDiscount: clamp(parseNumber(input.minDiscount, settings.defaultDiscount), 0, 95),
    sellerType: normalizeSellerType(input.sellerType ?? settings.defaultSellerType),
    categories: normalizeCategoryIds(input.categories, settings.defaultCategories),
    minPrice,
    maxPrice,
    onlyPrime: parseBool(input.onlyPrime, false),
    onlyInStock: parseBool(input.onlyInStock, true),
    onlyGoodRating: parseBool(input.onlyGoodRating, false)
  };
}

function normalizeDealRecord(deal, product, filters, existingRow = null) {
  const asin = extractAsin(deal) || extractAsin(product);
  if (!asin) {
    return null;
  }

  const offerSummary = getOfferSummary(product);
  const currentDealPrice = pickNumericField(deal, ['dealPrice', 'currentPrice', 'current', 'price']);
  const currentPrice =
    currentDealPrice !== null
      ? currentDealPrice >= 100
        ? fromMinorUnits(currentDealPrice)
        : currentDealPrice
      : offerSummary.price;
  const keepaDiscount = pickNumericField(deal, ['percentOff', 'deltaPercent', 'discountPercent', 'discount']);
  const referencePrice = buildReferencePrice(currentPrice, keepaDiscount, deal, product);
  const category = normalizeCategory(product, deal);
  const title = cleanText(deal.title || product?.title) || asin;
  const sellerTypeFromDeal = Boolean(deal?.isFulfilledByAmazon)
    ? Boolean(deal?.isAmazon)
      ? 'AMAZON'
      : 'FBA'
    : offerSummary.sellerType;
  const rating = normalizeRating(deal?.rating || product?.currentRating || product?.stats?.currentRating);
  const reviewCount = parseInteger(deal?.totalReviews || product?.currentRatingCount, null);

  return {
    asin,
    domainId: filters.domainId,
    title,
    productUrl: buildAmazonProductUrl(asin, filters.domainId),
    imageUrl: buildAmazonImageUrl(deal?.image || product?.imagesCSV),
    currentPrice,
    referencePrice,
    referenceLabel: 'Referenzpreis / Verlauf',
    keepaDiscount: keepaDiscount !== null ? keepaDiscount : 0,
    sellerType: sellerTypeFromDeal || 'UNKNOWN',
    categoryId: category.categoryId,
    categoryName: category.categoryName,
    rating,
    reviewCount,
    isPrime: Boolean(deal?.isPrimeEligible) || offerSummary.isPrime,
    isInStock: offerSummary.inStock,
    comparisonSource: cleanText(existingRow?.comparison_source),
    comparisonStatus: cleanText(existingRow?.comparison_status) || 'not_connected',
    comparisonPrice: parseNumber(existingRow?.comparison_price, null),
    comparisonPayload: fromJson(existingRow?.comparison_payload_json, null),
    keepaPayload: {
      deal: {
        asin,
        title,
        currentPrice,
        keepaDiscount,
        category,
        isPrimeEligible: Boolean(deal?.isPrimeEligible),
        isAmazon: Boolean(deal?.isAmazon),
        isFulfilledByAmazon: Boolean(deal?.isFulfilledByAmazon)
      },
      product: {
        imagesCSV: product?.imagesCSV || '',
        rootCategory: product?.rootCategory || category.categoryId,
        productGroup: product?.productGroup || '',
        stats: product?.stats || null,
        csv: Array.isArray(product?.csv) ? product.csv : null,
        offerCSV: product?.offerCSV || null,
        buyBoxSellerIdHistory: product?.buyBoxSellerIdHistory || null
      },
      raw: {
        deal: deal || null,
        product: product
          ? {
              stats: product.stats || null,
              csv: Array.isArray(product.csv) ? product.csv : null,
              offerCSV: product.offerCSV || null,
              buyBoxSellerIdHistory: product.buyBoxSellerIdHistory || null
            }
          : null
      }
    }
  };
}

async function compareAgainstLegalSources(item) {
  const existing = getExistingResult(item.asin, item.domainId);
  const settings = getKeepaSettings();
  const comparison = await resolveComparisonFromAdapters({
    settings,
    existingResult: existing
  });

  return {
    ...item,
    comparisonSource: comparison.source,
    comparisonStatus: comparison.status,
    comparisonPrice: comparison.price ?? null,
    comparisonPayload: comparison
  };
}

function saveKeepaResult(item, meta = {}) {
  const existing = getExistingResult(item.asin, item.domainId);
  const mergedItem = mergeExistingComparison(item, existing);
  const settings = getKeepaSettings();
  const currentPrice = parseNumber(mergedItem.currentPrice, null);
  const comparisonPrice = parseNumber(mergedItem.comparisonPrice, null);
  const priceDifferenceAbs =
    currentPrice !== null && comparisonPrice !== null ? Math.round((comparisonPrice - currentPrice) * 100) / 100 : null;
  const priceDifferencePct =
    currentPrice !== null && comparisonPrice !== null && currentPrice > 0
      ? Math.round(((comparisonPrice - currentPrice) / currentPrice) * 1000) / 10
      : null;
  const scored = computeDealScore(
    {
      ...mergedItem,
      currentPrice,
      comparisonPrice,
      priceDifferencePct
    },
    settings,
    meta.rule || null
  );
  const timestamp = nowIso();
  const workflowStatus =
    existing?.workflow_status && normalizeWorkflowStatus(existing.workflow_status) !== 'neu'
      ? normalizeWorkflowStatus(existing.workflow_status)
      : scored.dealStrength === 'verwerfen'
        ? 'verworfen'
        : existing?.workflow_status
          ? normalizeWorkflowStatus(existing.workflow_status)
          : 'neu';

  const payload = {
    asin: mergedItem.asin,
    domain_id: mergedItem.domainId,
    title: mergedItem.title,
    product_url: mergedItem.productUrl,
    image_url: mergedItem.imageUrl,
    current_price: currentPrice,
    reference_price: parseNumber(mergedItem.referencePrice, null),
    reference_label: mergedItem.referenceLabel || 'Referenzpreis',
    keepa_discount: parseNumber(mergedItem.keepaDiscount, null),
    seller_type: mergedItem.sellerType || 'UNKNOWN',
    category_id: parseInteger(mergedItem.categoryId, null),
    category_name: mergedItem.categoryName,
    rating: normalizeRating(mergedItem.rating),
    review_count: parseInteger(mergedItem.reviewCount, null),
    is_prime: mergedItem.isPrime ? 1 : 0,
    is_in_stock: mergedItem.isInStock ? 1 : 0,
    deal_score: scored.dealScore,
    deal_strength: scored.dealStrength,
    strength_reason: scored.strengthReason,
    workflow_status: workflowStatus,
    comparison_source: mergedItem.comparisonSource,
    comparison_status: mergedItem.comparisonStatus || 'not_connected',
    comparison_price: comparisonPrice,
    price_difference_abs: priceDifferenceAbs,
    price_difference_pct: priceDifferencePct,
    comparison_checked_at: timestamp,
    comparison_payload_json: toJson(mergedItem.comparisonPayload || null),
    keepa_payload_json: toJson(mergedItem.keepaPayload || null),
    search_payload_json: toJson(meta.searchPayload || null),
    origin: meta.origin || 'manual',
    rule_id: meta.rule?.id || null,
    note: existing?.note || '',
    alert_count: parseInteger(existing?.alert_count, 0),
    last_alerted_at: existing?.last_alerted_at || null,
    first_seen_at: existing?.first_seen_at || timestamp,
    last_seen_at: timestamp,
    last_synced_at: timestamp,
    created_at: existing?.created_at || timestamp,
    updated_at: timestamp
  };

  if (existing) {
    db.prepare(
      `
        UPDATE keepa_results
        SET title = @title,
            product_url = @product_url,
            image_url = @image_url,
            current_price = @current_price,
            reference_price = @reference_price,
            reference_label = @reference_label,
            keepa_discount = @keepa_discount,
            seller_type = @seller_type,
            category_id = @category_id,
            category_name = @category_name,
            rating = @rating,
            review_count = @review_count,
            is_prime = @is_prime,
            is_in_stock = @is_in_stock,
            deal_score = @deal_score,
            deal_strength = @deal_strength,
            strength_reason = @strength_reason,
            workflow_status = @workflow_status,
            comparison_source = @comparison_source,
            comparison_status = @comparison_status,
            comparison_price = @comparison_price,
            price_difference_abs = @price_difference_abs,
            price_difference_pct = @price_difference_pct,
            comparison_checked_at = @comparison_checked_at,
            comparison_payload_json = @comparison_payload_json,
            keepa_payload_json = @keepa_payload_json,
            search_payload_json = @search_payload_json,
            origin = @origin,
            rule_id = @rule_id,
            last_seen_at = @last_seen_at,
            last_synced_at = @last_synced_at,
            updated_at = @updated_at
        WHERE id = @id
      `
    ).run({
      id: existing.id,
      ...payload
    });
  } else {
    db.prepare(
      `
        INSERT INTO keepa_results (
          asin,
          domain_id,
          title,
          product_url,
          image_url,
          current_price,
          reference_price,
          reference_label,
          keepa_discount,
          seller_type,
          category_id,
          category_name,
          rating,
          review_count,
          is_prime,
          is_in_stock,
          deal_score,
          deal_strength,
          strength_reason,
          workflow_status,
          comparison_source,
          comparison_status,
          comparison_price,
          price_difference_abs,
          price_difference_pct,
          comparison_checked_at,
          comparison_payload_json,
          keepa_payload_json,
          search_payload_json,
          origin,
          rule_id,
          note,
          alert_count,
          last_alerted_at,
          first_seen_at,
          last_seen_at,
          last_synced_at,
          created_at,
          updated_at
        ) VALUES (
          @asin,
          @domain_id,
          @title,
          @product_url,
          @image_url,
          @current_price,
          @reference_price,
          @reference_label,
          @keepa_discount,
          @seller_type,
          @category_id,
          @category_name,
          @rating,
          @review_count,
          @is_prime,
          @is_in_stock,
          @deal_score,
          @deal_strength,
          @strength_reason,
          @workflow_status,
          @comparison_source,
          @comparison_status,
          @comparison_price,
          @price_difference_abs,
          @price_difference_pct,
          @comparison_checked_at,
          @comparison_payload_json,
          @keepa_payload_json,
          @search_payload_json,
          @origin,
          @rule_id,
          @note,
          @alert_count,
          @last_alerted_at,
          @first_seen_at,
          @last_seen_at,
          @last_synced_at,
          @created_at,
          @updated_at
        )
      `
    ).run(payload);
  }

  const storedRow = db
    .prepare(`SELECT * FROM keepa_results WHERE asin = ? AND domain_id = ? LIMIT 1`)
    .get(mergedItem.asin, mergedItem.domainId);
  const storedResult = buildResultDto(storedRow);
  persistFakeDropAnalysis(storedResult, {
    rule: meta.rule || null,
    origin: meta.origin || 'manual'
  });

  return buildResultDto(
    db
      .prepare(`SELECT * FROM keepa_results WHERE asin = ? AND domain_id = ? LIMIT 1`)
      .get(mergedItem.asin, mergedItem.domainId)
  );
}

async function executeSearch(filters, meta = {}) {
  const startedAt = Date.now();
  const action = meta.origin === 'automatic' ? 'automation-run' : 'manual-search';
  const module = action;
  const settings = getKeepaSettings();
  const estimatedBeforeStart = estimateSearchUsage(filters, settings, {
    origin: meta.origin,
    requestCount: 2
  });

  try {
    if (!settings.keepaEnabled) {
      throw new Error('Keepa ist in den Einstellungen deaktiviert.');
    }
    if (!getKeepaConfig().key) {
      throw new Error('KEEPA_API_KEY fehlt im Backend.');
    }

    const rawPageSize = Math.min(Math.max(filters.limit * 2, filters.limit), 60);
    const selection = {
      page: filters.page - 1,
      domainId: filters.domainId,
      includeCategories: filters.categories,
      currentRange: [
        filters.minPrice !== null ? toMinorUnits(filters.minPrice) : 0,
        filters.maxPrice !== null ? toMinorUnits(filters.maxPrice) : 999999999
      ],
      deltaPercentRange: [filters.minDiscount, 95],
      minRating: filters.onlyGoodRating ? Math.round(settings.goodRatingThreshold * 10) : undefined,
      hasReviews: filters.onlyGoodRating ? true : undefined,
      isOutOfStock: filters.onlyInStock ? false : undefined,
      isRangeEnabled: true,
      isFilterEnabled: true,
      filterErotic: true,
      dateRange: SEARCH_DATE_RANGE_DAYS,
      sortType: 0,
      perPage: rawPageSize
    };

    const keepaRequestUsage = [];
    const dealResponse = await keepaRequest(
      '/deal',
      {
        selection: JSON.stringify(selection)
      },
      {
        source: meta.source || 'manual_search',
        module,
        ruleId: meta.rule?.id ?? null,
        filters: {
          page: filters.page,
          limit: filters.limit,
          sellerType: filters.sellerType,
          categories: filters.categories,
          minDiscount: filters.minDiscount,
          minPrice: filters.minPrice,
          maxPrice: filters.maxPrice,
          onlyPrime: filters.onlyPrime,
          onlyInStock: filters.onlyInStock,
          onlyGoodRating: filters.onlyGoodRating
        }
      }
    );
    keepaRequestUsage.push(dealResponse.usage);

    const rawDeals = Array.isArray(dealResponse.data?.dr) ? dealResponse.data.dr : [];
    const asins = [...new Set(rawDeals.map((deal) => extractAsin(deal)).filter(Boolean))].slice(0, rawPageSize);
    const productResponse = asins.length
      ? await keepaRequest(
          '/product',
          {
            domain: filters.domainId,
            asin: asins.join(','),
            history: 1,
            offers: 20,
            rating: 1,
            stock: 1,
            update: 0,
            stats: 90
          },
          {
            source: meta.source ? `${meta.source}_products` : 'manual_search_products',
            module,
            ruleId: meta.rule?.id ?? null,
            filters: {
              asinCount: asins.length
            }
          }
        )
      : {
          data: { products: [] },
          usage: null
        };

    if (productResponse.usage) {
      keepaRequestUsage.push(productResponse.usage);
    }

    const products = Array.isArray(productResponse.data?.products) ? productResponse.data.products : [];
    const productByAsin = new Map(products.map((product) => [extractAsin(product), product]));
    const normalizedItems = [];
    const comparisonStartedAt = Date.now();

    for (const deal of rawDeals) {
      const asin = extractAsin(deal);
      if (!asin) {
        continue;
      }

      const existing = getExistingResult(asin, filters.domainId);
      const normalized = normalizeDealRecord(deal, productByAsin.get(asin), filters, existing);
      if (!normalized) {
        continue;
      }

      normalizedItems.push(await compareAgainstLegalSources(normalized));
    }

    recordKeepaUsage({
      action: 'background-check',
      module: 'background-check',
      filters,
      resultCount: normalizedItems.length,
      durationMs: Date.now() - comparisonStartedAt,
      requestStatus: 'success',
      estimatedUsage: 0,
      ruleId: meta.rule?.id ?? null,
      meta: {
        origin: meta.origin || 'manual',
        connectedComparisons: normalizedItems.filter((item) => item.comparisonStatus && item.comparisonStatus !== 'not_connected').length
      }
    });

    const filteredItems = applyManualFilters(normalizedItems, filters, settings).slice(0, filters.limit);
    const savedItems = filteredItems.map((item) =>
      saveKeepaResult(item, {
        origin: meta.origin || 'manual',
        rule: meta.rule || null,
        searchPayload: filters
      })
    );
    const officialUsageValue = keepaRequestUsage.reduce((sum, entry) => sum + (entry?.officialUsageValue ?? 0), 0);
    const hasOfficialUsage = keepaRequestUsage.some((entry) => entry?.officialUsageValue !== null);
    const durationMs = Date.now() - startedAt;
    const measuredUsage = recordKeepaUsage({
      action,
      module,
      filters,
      resultCount: savedItems.length,
      durationMs,
      requestStatus: 'success',
      estimatedUsage: hasOfficialUsage && officialUsageValue > 0 ? officialUsageValue : estimatedBeforeStart,
      officialUsageValue: hasOfficialUsage ? officialUsageValue : null,
      officialTokensLeft:
        [...keepaRequestUsage].reverse().find((entry) => entry?.officialTokensLeft !== null)?.officialTokensLeft ?? null,
      ruleId: meta.rule?.id ?? null,
      meta: {
        requestCount: keepaRequestUsage.length,
        rawResultCount: rawDeals.length,
        origin: meta.origin || 'manual'
      }
    });

    logKeepaEvent('info', 'search_completed', meta.source || 'manual_search', 'Keepa-Suche abgeschlossen.', {
      filters,
      resultCount: savedItems.length,
      payload: {
        rawResultCount: rawDeals.length,
        durationMs,
        estimatedUsage: measuredUsage.estimatedUsage,
        requestCount: keepaRequestUsage.length
      }
    });

    return {
      items: savedItems,
      pagination: {
        page: filters.page,
        limit: filters.limit,
        hasMore: rawDeals.length >= rawPageSize,
        rawResultCount: rawDeals.length
      },
      usage: {
        action,
        module,
        sourceLabel: action === 'automation-run' ? 'automatik' : 'manuell',
        estimatedBeforeStart,
        estimatedUsage: measuredUsage.estimatedUsage,
        officialUsageValue: hasOfficialUsage ? officialUsageValue : null,
        officialUsageKnown: hasOfficialUsage,
        keepaRequestCount: keepaRequestUsage.length,
        resultCount: savedItems.length,
        durationMs,
        requestStatus: 'success',
        usageModeLabel: hasOfficialUsage && officialUsageValue > 0 ? 'offiziell abgeleitet' : 'intern geschaetzt'
      }
    };
  } catch (error) {
    recordKeepaUsage({
      action,
      module,
      filters,
      durationMs: Date.now() - startedAt,
      requestStatus: 'error',
      estimatedUsage: 0,
      ruleId: meta.rule?.id ?? null,
      errorMessage: buildKeepaRequestError(error, 'Keepa-Suche fehlgeschlagen.')
    });
    throw error;
  }
}

export async function runKeepaManualSearch(input = {}) {
  const filters = normalizeManualSearchInput(input);
  return {
    filters,
    ...(await executeSearch(filters, {
      source: 'manual_search',
      origin: 'manual'
    }))
  };
}

function buildResultQuery(filters = {}) {
  const where = [];
  const params = {};

  if (filters.workflowStatus) {
    where.push(`workflow_status = @workflowStatus`);
    params.workflowStatus = normalizeWorkflowStatus(filters.workflowStatus);
  }

  if (filters.categoryId) {
    const categoryId = parseInteger(filters.categoryId, null);
    if (categoryId !== null && CATEGORY_ID_SET.has(categoryId)) {
      where.push(`category_id = @categoryId`);
      params.categoryId = categoryId;
    }
  }

  if (filters.minDiscount !== undefined && filters.minDiscount !== null && filters.minDiscount !== '') {
    where.push(`keepa_discount >= @minDiscount`);
    params.minDiscount = parseNumber(filters.minDiscount, 0);
  }

  if (filters.minDealScore !== undefined && filters.minDealScore !== null && filters.minDealScore !== '') {
    where.push(`deal_score >= @minDealScore`);
    params.minDealScore = parseNumber(filters.minDealScore, 0);
  }

  return {
    whereSql: where.length ? `WHERE ${where.join(' AND ')}` : '',
    params
  };
}

export function listKeepaResults(filters = {}) {
  const page = clamp(parseInteger(filters.page, 1), 1, MAX_MANUAL_PAGE);
  const limit = clamp(parseInteger(filters.limit, 20), 1, MAX_MANUAL_PAGE_SIZE);
  const offset = (page - 1) * limit;
  const { whereSql, params } = buildResultQuery(filters);
  const total = db.prepare(`SELECT COUNT(*) AS count FROM keepa_results ${whereSql}`).get(params)?.count || 0;
  const rows = db
    .prepare(
      `
        SELECT *
        FROM keepa_results
        ${whereSql}
        ORDER BY updated_at DESC
        LIMIT @limit OFFSET @offset
      `
    )
    .all({
      ...params,
      limit,
      offset
    });

  const response = {
    items: rows.map(buildResultDto),
    pagination: {
      page,
      limit,
      total,
      totalPages: Math.max(1, Math.ceil(total / limit))
    },
    filters: {
      workflowStatus: filters.workflowStatus || '',
      categoryId: filters.categoryId || '',
      minDiscount: filters.minDiscount || '',
      minDealScore: filters.minDealScore || ''
    }
  };

  recordKeepaUsage({
    action: 'result-refresh',
    module: 'result-refresh',
    filters: {
      workflowStatus: filters.workflowStatus,
      categoryId: filters.categoryId,
      minDiscount: filters.minDiscount,
      minDealScore: filters.minDealScore,
      page,
      limit
    },
    resultCount: rows.length,
    durationMs: 0,
    requestStatus: 'success',
    estimatedUsage: 0
  });

  return response;
}

export function updateKeepaResult(id, input = {}) {
  const existing = db.prepare(`SELECT * FROM keepa_results WHERE id = ?`).get(id);
  if (!existing) {
    throw new Error('Keepa-Treffer nicht gefunden.');
  }

  const nextComparisonPrice =
    input.comparisonPrice === undefined ? parseNumber(existing.comparison_price, null) : sanitizePriceBoundary(input.comparisonPrice);
  const nextComparisonSource =
    input.comparisonSource === undefined ? cleanText(existing.comparison_source) : cleanText(input.comparisonSource);
  const nextComparisonStatus =
    nextComparisonPrice !== null && nextComparisonSource ? 'manual' : cleanText(existing.comparison_status) || 'not_connected';
  const nextWorkflowStatus =
    input.workflowStatus === undefined ? normalizeWorkflowStatus(existing.workflow_status) : normalizeWorkflowStatus(input.workflowStatus);
  const nextNote = input.note === undefined ? existing.note || '' : cleanText(input.note);

  db.prepare(
    `
      UPDATE keepa_results
      SET workflow_status = @workflowStatus,
          note = @note,
          comparison_source = @comparisonSource,
          comparison_status = @comparisonStatus,
          comparison_price = @comparisonPrice,
          comparison_payload_json = @comparisonPayloadJson,
          comparison_checked_at = @comparisonCheckedAt,
          updated_at = @updatedAt
      WHERE id = @id
    `
  ).run({
    id,
    workflowStatus: nextWorkflowStatus,
    note: nextNote,
    comparisonSource: nextComparisonSource || null,
    comparisonStatus: nextComparisonStatus,
    comparisonPrice: nextComparisonPrice,
    comparisonPayloadJson: toJson({
      source: nextComparisonSource || null,
      status: nextComparisonStatus,
      notes:
        nextComparisonPrice !== null && nextComparisonSource
          ? 'Manueller Vergleichspreis hinterlegt.'
          : 'Keine manuelle Vergleichsquelle gesetzt.'
    }),
    comparisonCheckedAt: nowIso(),
    updatedAt: nowIso()
  });

  const refreshed = buildResultDto(db.prepare(`SELECT * FROM keepa_results WHERE id = ?`).get(id));
  const recalculated = saveKeepaResult(
    {
      ...refreshed,
      comparisonPayload: refreshed.comparisonPayload
    },
    {
      origin: refreshed.origin,
      rule: refreshed.ruleId ? listKeepaRules().find((rule) => rule.id === refreshed.ruleId) : null,
      searchPayload: refreshed.searchPayload
    }
  );

  logKeepaEvent('info', 'result_updated', 'results', `Keepa-Treffer ${id} aktualisiert.`, {
    payload: {
      workflowStatus: nextWorkflowStatus
    }
  });

  return recalculated;
}

function buildAlertMessage(result, settings, channelType) {
  const lines = [];
  if (settings.telegramMessagePrefix) {
    lines.push(settings.telegramMessagePrefix);
  }
  lines.push(result.title);
  lines.push(`Preis: ${formatCurrency(result.currentPrice)}`);
  lines.push(`Rabatt: ${result.keepaDiscount?.toFixed(1) || '0'}%`);
  lines.push(
    `Vergleich: ${result.comparisonPrice !== null ? formatCurrency(result.comparisonPrice) : 'nicht verbunden'}`
  );
  lines.push(
    `Preisvorteil: ${
      result.priceDifferenceAbs !== null && result.priceDifferencePct !== null
        ? `${formatCurrency(result.priceDifferenceAbs)} (${result.priceDifferencePct.toFixed(1)}%)`
        : 'nicht berechenbar'
    }`
  );
  lines.push(`Kategorie: ${result.categoryName || '-'}`);
  lines.push(`Deal-Score: ${result.dealScore}`);
  if (result.fakeDrop) {
    lines.push(`Fake-Drop Risiko: ${result.fakeDrop.fakeDropRisk}`);
    lines.push(`Analyse: ${result.fakeDrop.classificationLabel}`);
  }
  lines.push(`Link: ${result.productUrl}`);

  return {
    channelType,
    text: lines.filter(Boolean).join('\n'),
    preview: `${result.title} | ${formatCurrency(result.currentPrice)} | ${result.dealScore}`
  };
}

function buildAlertDedupeKey(result, ruleId, channelType) {
  const fingerprint = `${result.asin}:${ruleId || 0}:${channelType}:${Math.round((result.currentPrice || 0) * 100)}:${Math.round(
    (result.comparisonPrice || 0) * 100
  )}:${result.dealStrength}`;
  return crypto.createHash('sha1').update(fingerprint).digest('hex');
}

function canSendAlert(result, settings, channelType) {
  if (parseInteger(result.alertCount, 0) >= settings.alertMaxPerProduct) {
    return {
      allowed: false,
      reason: 'Maximale Alert-Anzahl fuer dieses Produkt erreicht.'
    };
  }

  if (result.lastAlertedAt) {
    const msSinceLastAlert = Date.now() - new Date(result.lastAlertedAt).getTime();
    if (Number.isFinite(msSinceLastAlert) && msSinceLastAlert < settings.alertCooldownMinutes * 60 * 1000) {
      return {
        allowed: false,
        reason: 'Alert-Cooldown ist noch aktiv.'
      };
    }
  }

  const duplicate = db
    .prepare(
      `
        SELECT id
        FROM keepa_alerts
        WHERE asin = ?
          AND channel_type = ?
          AND created_at >= ?
        LIMIT 1
      `
    )
    .get(result.asin, channelType, new Date(Date.now() - settings.alertCooldownMinutes * 60 * 1000).toISOString());

  if (duplicate) {
    return {
      allowed: false,
      reason: 'Es existiert bereits ein kuerzlich gesendeter Alert fuer dieses Produkt.'
    };
  }

  return { allowed: true };
}

async function insertAlertLog(result, rule, channelType, status, message, payload, errorMessage = null) {
  const dedupeKey = buildAlertDedupeKey(result, rule?.id, channelType);
  db.prepare(
    `
      INSERT OR IGNORE INTO keepa_alerts (
        keepa_result_id,
        asin,
        channel_type,
        status,
        rule_id,
        dedupe_key,
        message_preview,
        payload_json,
        error_message,
        created_at,
        sent_at
      ) VALUES (
        @keepaResultId,
        @asin,
        @channelType,
        @status,
        @ruleId,
        @dedupeKey,
        @messagePreview,
        @payloadJson,
        @errorMessage,
        @createdAt,
        @sentAt
      )
    `
  ).run({
    keepaResultId: result.id || null,
    asin: result.asin,
    channelType,
    status,
    ruleId: rule?.id || null,
    dedupeKey,
    messagePreview: message.preview,
    payloadJson: toJson(payload),
    errorMessage,
    createdAt: nowIso(),
    sentAt: status === 'sent' || status === 'stored' ? nowIso() : null
  });
}

async function dispatchAlert(result, rule, channelType, settings) {
  const eligibility = canSendAlert(result, settings, channelType);
  const message = buildAlertMessage(result, settings, channelType);

  if (!eligibility.allowed) {
    await insertAlertLog(result, rule, channelType, 'skipped', message, {
      reason: eligibility.reason
    });
    return {
      channelType,
      status: 'skipped',
      reason: eligibility.reason
    };
  }

  if (channelType === 'internal') {
    await insertAlertLog(result, rule, channelType, 'stored', message, {
      workflowStatus: result.workflowStatus
    });
    return {
      channelType,
      status: 'stored'
    };
  }

  if (channelType === 'whatsapp') {
    await insertAlertLog(result, rule, channelType, 'disabled', message, {
      reason: 'WhatsApp ist nur als sicherer Platzhalter vorbereitet.'
    });
    return {
      channelType,
      status: 'disabled'
    };
  }

  try {
    await sendTelegramPost({
      text: message.text,
      imageUrl: result.imageUrl || undefined
    });
    await insertAlertLog(result, rule, channelType, 'sent', message, {
      comparisonPrice: result.comparisonPrice
    });
    return {
      channelType,
      status: 'sent'
    };
  } catch (error) {
    await insertAlertLog(
      result,
      rule,
      channelType,
      'failed',
      message,
      {
        comparisonPrice: result.comparisonPrice
      },
      buildKeepaRequestError(error, 'Telegram-Versand fehlgeschlagen.')
    );
    return {
      channelType,
      status: 'failed',
      error: buildKeepaRequestError(error, 'Telegram-Versand fehlgeschlagen.')
    };
  }
}

async function maybeSendAlertsForResult(result, rule) {
  const settings = getKeepaSettings();
  if (result.dealStrength !== 'stark') {
    return [];
  }

  const fakeDropGate = evaluateFakeDropAlertEligibility(result, rule);
  if (!fakeDropGate.allowed) {
    return [
      {
        channelType: 'review',
        status: fakeDropGate.reviewQueue ? 'review_queue' : 'skipped',
        reason: fakeDropGate.reason
      }
    ];
  }

  const channels = [];
  if (settings.alertInternalEnabled) {
    channels.push('internal');
  }
  if (settings.alertTelegramEnabled) {
    channels.push('telegram');
  }
  if (settings.alertWhatsappPlaceholderEnabled) {
    channels.push('whatsapp');
  }

  const outputs = [];
  for (const channelType of channels) {
    outputs.push(await dispatchAlert(result, rule, channelType, settings));
  }

  if (outputs.some((item) => item.status === 'sent' || item.status === 'stored')) {
    db.prepare(
      `
        UPDATE keepa_results
        SET alert_count = COALESCE(alert_count, 0) + 1,
            last_alerted_at = @lastAlertedAt,
            workflow_status = CASE
              WHEN workflow_status = 'verworfen' THEN workflow_status
              ELSE 'alert_gesendet'
            END,
            updated_at = @updatedAt
        WHERE id = @id
      `
    ).run({
      id: result.id,
      lastAlertedAt: nowIso(),
      updatedAt: nowIso()
    });
  }

  return outputs;
}

function getOverviewCounts() {
  const activeRulesCount = db.prepare(`SELECT COUNT(*) AS count FROM keepa_rules WHERE is_active = 1`).get()?.count || 0;
  const lastSync =
    db.prepare(`SELECT MAX(last_synced_at) AS lastSync FROM keepa_results`).get()?.lastSync ||
    db.prepare(`SELECT MAX(last_run_at) AS lastSync FROM keepa_rules`).get()?.lastSync ||
    null;
  const latestHits = db
    .prepare(`SELECT * FROM keepa_results ORDER BY updated_at DESC LIMIT 5`)
    .all()
    .map(buildResultDto);
  const latestAlerts = db
    .prepare(`SELECT * FROM keepa_alerts ORDER BY created_at DESC LIMIT 5`)
    .all()
    .map((row) => ({
      id: row.id,
      asin: row.asin,
      channelType: row.channel_type,
      status: row.status,
      messagePreview: row.message_preview,
      createdAt: row.created_at,
      errorMessage: row.error_message
    }));

  return {
    activeRulesCount,
    lastSync,
    latestHits,
    latestAlerts
  };
}

function getUsageModuleLabel(module) {
  return USAGE_MODULE_CATALOG.find((item) => item.id === module)?.label || module;
}

function getUsageActionLabel(action) {
  const labels = {
    'keepa-request': 'Keepa API Request',
    'manual-search': 'Manuelle Suche',
    'automation-run': 'Automatik-Lauf',
    'test-connection': 'Testverbindung',
    'result-refresh': 'Ergebnis-Refresh',
    'alert-check': 'Alert-Pruefung',
    'background-check': 'Hintergrundpruefung'
  };

  return labels[action] || action;
}

function mapUsageLogRow(row) {
  return {
    id: row.id,
    action: row.action,
    actionLabel: getUsageActionLabel(row.action),
    module: row.module,
    moduleLabel: getUsageModuleLabel(row.module),
    filters: fromJson(row.filters_json, null),
    resultCount: parseInteger(row.result_count, 0),
    durationMs: parseInteger(row.duration_ms, 0),
    requestStatus: normalizeUsageStatus(row.request_status, 'success'),
    estimatedUsage: parseNumber(row.estimated_usage, 0) || 0,
    officialUsageValue: parseNumber(row.official_usage_value, null),
    officialTokensLeft: parseInteger(row.official_tokens_left, null),
    ruleId: parseInteger(row.rule_id, null),
    errorMessage: row.error_message || '',
    meta: fromJson(row.meta_json, null),
    createdAt: row.created_at
  };
}

function getUsageSourceBreakdown(startDate, moduleFilter = '') {
  const rows = db
    .prepare(
      `
        SELECT
          module,
          COUNT(*) AS requestCount,
          COALESCE(SUM(result_count), 0) AS resultCount,
          COALESCE(SUM(estimated_usage), 0) AS estimatedUsage,
          COALESCE(SUM(CASE WHEN request_status = 'error' THEN 1 ELSE 0 END), 0) AS errorCount,
          MAX(created_at) AS lastRequestAt
        FROM keepa_usage_logs
        WHERE created_at >= @startDate
          AND action != 'keepa-request'
          AND (@moduleFilter = '' OR module = @moduleFilter)
        GROUP BY module
      `
    )
    .all({
      startDate,
      moduleFilter
    });
  const grouped = new Map(rows.map((row) => [row.module, row]));

  return USAGE_MODULE_CATALOG.filter((item) => item.id !== 'status-check')
    .filter((item) => !moduleFilter || item.id === moduleFilter)
    .map((item) => {
      const current = grouped.get(item.id) || {};
      return {
        module: item.id,
        label: item.label,
        requestCount: parseInteger(current.requestCount, 0),
        resultCount: parseInteger(current.resultCount, 0),
        estimatedUsage: Math.round((parseNumber(current.estimatedUsage, 0) || 0) * 10) / 10,
        errorCount: parseInteger(current.errorCount, 0),
        lastRequestAt: current.lastRequestAt || null
      };
    });
}

function getLatestUsageLog(action) {
  const row = db
    .prepare(
      `
        SELECT *
        FROM keepa_usage_logs
        WHERE action = ?
        ORDER BY created_at DESC
        LIMIT 1
      `
    )
    .get(action);

  return row ? mapUsageLogRow(row) : null;
}

export function getKeepaUsageSummary() {
  const settings = getKeepaSettings();
  const todayStart = getRangeStart('today');
  const monthStart = getRangeStart('month');
  const todayStartIso = todayStart.toISOString();
  const monthStartIso = monthStart.toISOString();
  const requestStatsToday =
    db
      .prepare(
        `
          SELECT
            COUNT(*) AS requestCount,
            MAX(created_at) AS lastRequestAt
          FROM keepa_usage_logs
          WHERE action = 'keepa-request'
            AND created_at >= ?
        `
      )
      .get(todayStartIso) || {};
  const latestRequestOverall =
    db
      .prepare(
        `
          SELECT MAX(created_at) AS lastRequestAt
          FROM keepa_usage_logs
          WHERE action = 'keepa-request'
        `
      )
      .get() || {};
  const requestStatsMonth =
    db
      .prepare(
        `
          SELECT
            COUNT(*) AS requestCount
          FROM keepa_usage_logs
          WHERE action = 'keepa-request'
            AND created_at >= ?
        `
      )
      .get(monthStartIso) || {};
  const summaryToday =
    db
      .prepare(
        `
          SELECT
            COALESCE(SUM(CASE WHEN action IN ('manual-search', 'automation-run', 'test-connection') THEN estimated_usage ELSE 0 END), 0) AS estimatedUsage,
            COALESCE(SUM(CASE WHEN action IN ('manual-search', 'automation-run') THEN result_count ELSE 0 END), 0) AS hitCount,
            COALESCE(SUM(CASE WHEN request_status = 'error' THEN 1 ELSE 0 END), 0) AS errorCount
          FROM keepa_usage_logs
          WHERE created_at >= ?
        `
      )
      .get(todayStartIso) || {};
  const summaryMonth =
    db
      .prepare(
        `
          SELECT
            COALESCE(SUM(CASE WHEN action IN ('manual-search', 'automation-run', 'test-connection') THEN estimated_usage ELSE 0 END), 0) AS estimatedUsage,
            COALESCE(SUM(CASE WHEN action IN ('manual-search', 'automation-run') THEN result_count ELSE 0 END), 0) AS hitCount,
            COALESCE(SUM(CASE WHEN request_status = 'error' THEN 1 ELSE 0 END), 0) AS errorCount
          FROM keepa_usage_logs
          WHERE created_at >= ?
        `
      )
      .get(monthStartIso) || {};
  const officialUsagePresence =
    db
      .prepare(
        `
          SELECT COUNT(*) AS count
          FROM keepa_usage_logs
          WHERE official_usage_value IS NOT NULL
            AND official_usage_value > 0
            AND created_at >= ?
        `
      )
      .get(monthStartIso)?.count || 0;
  const activeRulesCount = db.prepare(`SELECT COUNT(*) AS count FROM keepa_rules WHERE is_active = 1`).get()?.count || 0;
  const latestManualSearch = getLatestUsageLog('manual-search');
  const latestAutomationRun = getLatestUsageLog('automation-run');
  const recentIssues = db
    .prepare(
      `
        SELECT *
        FROM keepa_usage_logs
        WHERE request_status IN ('error', 'warning')
        ORDER BY created_at DESC
        LIMIT 8
      `
    )
    .all()
    .map(mapUsageLogRow);
  const estimatedUsageMonth = Math.round((parseNumber(summaryMonth.estimatedUsage, 0) || 0) * 10) / 10;
  const todayIndex = Math.max(1, new Date().getDate());
  const daysInMonth = new Date(new Date().getFullYear(), new Date().getMonth() + 1, 0).getDate();

  return {
    requestedAt: nowIso(),
    usageModeLabel: officialUsagePresence ? 'teilweise offiziell abgeleitet, sonst intern geschaetzt' : 'intern geschaetzt',
    kpis: {
      lastRequestAt: latestRequestOverall.lastRequestAt || requestStatsToday.lastRequestAt || latestManualSearch?.createdAt || latestAutomationRun?.createdAt || null,
      requestsToday: parseInteger(requestStatsToday.requestCount, 0),
      requestsMonth: parseInteger(requestStatsMonth.requestCount, 0),
      estimatedUsageToday: Math.round((parseNumber(summaryToday.estimatedUsage, 0) || 0) * 10) / 10,
      estimatedUsageMonth,
      hitsToday: parseInteger(summaryToday.hitCount, 0),
      activeRulesCount,
      monthlyProjection: Math.round(((estimatedUsageMonth / todayIndex) * daysInMonth) * 10) / 10
    },
    today: {
      estimatedUsage: Math.round((parseNumber(summaryToday.estimatedUsage, 0) || 0) * 10) / 10,
      hitCount: parseInteger(summaryToday.hitCount, 0),
      errorCount: parseInteger(summaryToday.errorCount, 0),
      requestCount: parseInteger(requestStatsToday.requestCount, 0)
    },
    month: {
      estimatedUsage: estimatedUsageMonth,
      hitCount: parseInteger(summaryMonth.hitCount, 0),
      errorCount: parseInteger(summaryMonth.errorCount, 0),
      requestCount: parseInteger(requestStatsMonth.requestCount, 0)
    },
    lastManualSearch: latestManualSearch,
    lastAutomationRun: latestAutomationRun,
    dealsToday: parseInteger(summaryToday.hitCount, 0),
    sourceBreakdown: getUsageSourceBreakdown(todayStartIso),
    recentIssues,
    usageModules: USAGE_MODULE_CATALOG
      .filter((item) => item.id !== 'status-check')
      .map((item) => ({ id: item.id, label: item.label })),
    usageSettings: {
      estimatedManualRunCost: settings.estimatedTokensPerManualRun
    }
  };
}

export function getKeepaUsageHistory(filters = {}) {
  const days = clamp(parseInteger(filters.days, 30), 7, 90);
  const moduleFilter = cleanText(filters.module) && cleanText(filters.module) !== 'all' ? resolveUsageModuleFilter(filters.module) : '';
  const endDay = getLocalDayStart(new Date());
  const startDay = getLocalDayStart(shiftLocalDays(endDay, -(days - 1)));
  const startDateKey = toLocalDateKey(startDay);
  const endDateKey = toLocalDateKey(endDay);
  const rows = db
    .prepare(
      `
        SELECT
          usage_date,
          COALESCE(SUM(CASE WHEN action != 'keepa-request' THEN estimated_usage ELSE 0 END), 0) AS estimatedUsage,
          COALESCE(SUM(CASE WHEN action IN ('manual-search', 'automation-run') THEN result_count ELSE 0 END), 0) AS hitCount,
          COALESCE(SUM(CASE WHEN action = 'keepa-request' THEN request_count ELSE 0 END), 0) AS requestCount,
          COALESCE(SUM(CASE WHEN official_usage_value IS NOT NULL THEN official_usage_value ELSE 0 END), 0) AS officialUsage
        FROM keepa_usage_daily
        WHERE usage_date >= @startDateKey
          AND usage_date <= @endDateKey
          AND (@moduleFilter = '' OR module = @moduleFilter)
        GROUP BY usage_date
        ORDER BY usage_date ASC
      `
    )
    .all({
      startDateKey,
      endDateKey,
      moduleFilter
    });
  const rowMap = new Map(rows.map((row) => [row.usage_date, row]));
  const series = [];

  for (let offset = 0; offset < days; offset += 1) {
    const currentDate = shiftLocalDays(startDay, offset);
    const dateKey = toLocalDateKey(currentDate);
    const row = rowMap.get(dateKey) || {};

    series.push({
      date: dateKey,
      label: new Intl.DateTimeFormat('de-DE', { day: '2-digit', month: '2-digit' }).format(currentDate),
      estimatedUsage: Math.round((parseNumber(row.estimatedUsage, 0) || 0) * 10) / 10,
      officialUsage: parseNumber(row.officialUsage, null),
      hitCount: parseInteger(row.hitCount, 0),
      requestCount: parseInteger(row.requestCount, 0)
    });
  }

  return {
    range: {
      days,
      module: moduleFilter || 'all'
    },
    series,
    sourceBreakdown: getUsageSourceBreakdown(startDay.toISOString(), moduleFilter),
    usageModeLabel: getKeepaUsageSummary().usageModeLabel
  };
}

export function listKeepaUsageLogs(filters = {}) {
  const range = ['today', 'week', 'month'].includes(cleanText(filters.range).toLowerCase()) ? cleanText(filters.range).toLowerCase() : 'today';
  const moduleFilter = cleanText(filters.module) && cleanText(filters.module) !== 'all' ? resolveUsageModuleFilter(filters.module) : '';
  const limit = clamp(parseInteger(filters.limit, 40), 10, 200);
  const startDate = getRangeStart(range).toISOString();
  const rows = db
    .prepare(
      `
        SELECT *
        FROM keepa_usage_logs
        WHERE created_at >= @startDate
          AND (@moduleFilter = '' OR module = @moduleFilter)
        ORDER BY created_at DESC
        LIMIT @limit
      `
    )
    .all({
      startDate,
      moduleFilter,
      limit
    });

  return {
    items: rows.map(mapUsageLogRow),
    filters: {
      range,
      module: moduleFilter || 'all',
      limit
    },
    availableModules: USAGE_MODULE_CATALOG
      .filter((item) => item.id !== 'status-check')
      .map((item) => ({ id: item.id, label: item.label }))
  };
}

export async function getKeepaStatus() {
  const settingsView = getKeepaSettingsView();
  let connection = {
    connected: false,
    tokensLeft: null,
    refillRate: null,
    refillInMs: null,
    tokensConsumed: null,
    checkedAt: null
  };

  if (getKeepaConfig().key) {
    try {
      connection = await loadKeepaConnectionStatus(false);
    } catch (error) {
      connection = {
        connected: false,
        error: buildKeepaRequestError(error, 'Keepa-Verbindung fehlgeschlagen.'),
        checkedAt: nowIso()
      };
    }
  }

  const counts = getOverviewCounts();
  const usageSummary = getKeepaUsageSummary();
  const fakeDropSummary = getFakeDropSummary();
  return {
    settings: settingsView,
    connection,
    overview: {
      apiStatus: connection.connected ? 'verbunden' : settingsView.keepaKeyStatus.connected ? 'fehler' : 'nicht_konfiguriert',
      keepaConnected: connection.connected,
      lastSync: counts.lastSync,
      activeRulesCount: counts.activeRulesCount,
      latestHits: counts.latestHits,
      latestAlerts: counts.latestAlerts,
      fakeDropSummary,
      apiUsage: {
        requestCount24h: usageSummary.kpis.requestsToday,
        requestCountMonth: usageSummary.kpis.requestsMonth,
        estimatedUsage24h: usageSummary.kpis.estimatedUsageToday,
        tokensConsumed24h: usageSummary.kpis.estimatedUsageToday,
        estimatedUsageMonth: usageSummary.kpis.estimatedUsageMonth,
        tokensLeft: connection.tokensLeft,
        estimatedHourlyBurn: Math.round((usageSummary.kpis.estimatedUsageToday / 24) * 10) / 10,
        estimatedManualRunCost: usageSummary.usageSettings.estimatedManualRunCost,
        lastRequestAt: usageSummary.kpis.lastRequestAt,
        hitsToday: usageSummary.kpis.hitsToday,
        usageModeLabel: usageSummary.usageModeLabel
      },
      usageSummary
    }
  };
}

export function listKeepaAlerts(filters = {}) {
  const limit = clamp(parseInteger(filters.limit, 20), 1, 100);
  const rows = db
    .prepare(`SELECT * FROM keepa_alerts ORDER BY created_at DESC LIMIT ?`)
    .all(limit);

  return {
    items: rows.map((row) => ({
      id: row.id,
      keepaResultId: row.keepa_result_id,
      asin: row.asin,
      channelType: row.channel_type,
      status: row.status,
      ruleId: row.rule_id,
      messagePreview: row.message_preview,
      payload: fromJson(row.payload_json, null),
      errorMessage: row.error_message,
      createdAt: row.created_at,
      sentAt: row.sent_at
    }))
  };
}

export async function sendKeepaTestAlert() {
  const settings = getKeepaSettings();
  const startedAt = Date.now();
  const sampleResult = {
    id: null,
    asin: 'KEEPATEST01',
    title: 'Keepa Test Alert',
    currentPrice: 29.99,
    keepaDiscount: 35,
    comparisonPrice: 44.99,
    priceDifferenceAbs: 15,
    priceDifferencePct: 50.1,
    categoryName: 'Systemtest',
    productUrl: 'https://www.amazon.de',
    imageUrl: '',
    dealScore: 88,
    dealStrength: 'stark',
    workflowStatus: 'neu',
    alertCount: 0,
    lastAlertedAt: null
  };

  const outputs = [];
  if (settings.alertInternalEnabled) {
    outputs.push(await dispatchAlert(sampleResult, null, 'internal', settings));
  }
  if (settings.alertTelegramEnabled) {
    outputs.push(await dispatchAlert(sampleResult, null, 'telegram', settings));
  }
  if (settings.alertWhatsappPlaceholderEnabled) {
    outputs.push(await dispatchAlert(sampleResult, null, 'whatsapp', settings));
  }

  recordKeepaUsage({
    action: 'alert-check',
    module: 'alert-check',
    resultCount: outputs.filter((item) => item.status === 'sent' || item.status === 'stored').length,
    durationMs: Date.now() - startedAt,
    requestStatus: outputs.some((item) => item.status === 'failed') ? 'warning' : 'success',
    estimatedUsage: 0,
    meta: {
      mode: 'test-alert',
      processedChannels: outputs.length
    }
  });

  return {
    sentAt: nowIso(),
    outputs
  };
}

async function processRule(rule) {
  const filters = {
    page: 1,
    limit: 20,
    domainId: getKeepaSettings().domainId,
    minDiscount: rule.minDiscount,
    sellerType: rule.sellerType,
    categories: rule.categories,
    minPrice: rule.minPrice,
    maxPrice: rule.maxPrice,
    onlyPrime: rule.onlyPrime,
    onlyInStock: rule.onlyInStock,
    onlyGoodRating: rule.onlyGoodRating
  };

  const result = await executeSearch(filters, {
    source: 'rule_scan',
    origin: 'automatic',
    rule
  });

  let alertDispatchCount = 0;
  let alertFailureCount = 0;
  let strongDealCount = 0;

  for (const item of result.items) {
    if (item.dealStrength === 'stark') {
      strongDealCount += 1;
    }

    const outputs = await maybeSendAlertsForResult(item, rule);
    alertDispatchCount += outputs.filter((output) => output.status === 'sent' || output.status === 'stored').length;
    alertFailureCount += outputs.filter((output) => output.status === 'failed').length;
  }

  recordKeepaUsage({
    action: 'alert-check',
    module: 'alert-check',
    filters: {
      ruleId: rule.id,
      limit: filters.limit
    },
    resultCount: alertDispatchCount,
    durationMs: 0,
    requestStatus: alertFailureCount ? 'warning' : 'success',
    estimatedUsage: 0,
    ruleId: rule.id,
    meta: {
      checkedItems: result.items.length,
      strongDealCount,
      failedChannels: alertFailureCount
    }
  });

  db.prepare(
    `
      UPDATE keepa_rules
      SET last_run_at = @lastRunAt,
          next_run_at = @nextRunAt,
          updated_at = @updatedAt
      WHERE id = @id
    `
  ).run({
    id: rule.id,
    lastRunAt: nowIso(),
    nextRunAt: new Date(Date.now() + rule.intervalMinutes * 60 * 1000).toISOString(),
    updatedAt: nowIso()
  });

  logKeepaEvent('info', 'rule_processed', 'scheduler', `Keepa-Regel ${rule.name} verarbeitet.`, {
    payload: {
      ruleId: rule.id,
      resultCount: result.items.length
    }
  });
}

async function runDueRules() {
  if (schedulerRunning) {
    return;
  }

  schedulerRunning = true;
  try {
    const settings = getKeepaSettings();
    if (!settings.keepaEnabled || !settings.schedulerEnabled || !getKeepaConfig().key) {
      return;
    }

    const dueRules = listKeepaRules().filter(
      (rule) => rule.isActive && (!rule.nextRunAt || new Date(rule.nextRunAt).getTime() <= Date.now())
    );

    for (const rule of dueRules) {
      try {
        await processRule(rule);
      } catch (error) {
        logKeepaEvent('error', 'rule_processing_failed', 'scheduler', `Regel ${rule.name} fehlgeschlagen.`, {
          payload: {
            ruleId: rule.id,
            message: buildKeepaRequestError(error, 'Scheduler-Fehler')
          }
        });
      }
    }
  } finally {
    schedulerRunning = false;
  }
}

export function startKeepaScheduler() {
  if (schedulerStarted) {
    return;
  }

  schedulerStarted = true;
  setInterval(() => {
    void runDueRules();
  }, SCHEDULER_INTERVAL_MS);

  void runDueRules();
}
