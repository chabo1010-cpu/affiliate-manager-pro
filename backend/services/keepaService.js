import { getDb } from '../db.js';
import { getKeepaConfig, getTelegramConfig } from '../env.js';
import { publishAutoDealToTelegramTestGroup } from './autoDealPublisher.js';
import { buildAmazonAffiliateLinkRecord, checkDealLockStatus } from './dealHistoryService.js';
import { loadAmazonAffiliateContext } from './amazonAffiliateService.js';
import { logGeneratorDebug } from './generatorFlowService.js';
import { evaluateLearningRoute } from './learningLogicService.js';
import { getComparisonAdapterCatalog, resolveComparisonFromAdapters } from './keepaComparisonAdapters.js';
import {
  buildKeepaChartSnapshot,
  evaluateFakeDropAlertEligibility,
  getFakeDropSettingsView,
  getFakeDropSnapshotForResult,
  getSimilarCaseSignals,
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

const KEEPA_DRAWER_CATALOG = [
  { key: 'AMAZON', label: 'Amazon' },
  { key: 'FBA', label: 'FBA' },
  { key: 'FBM', label: 'FBM' }
];

const KEEPA_TREND_INTERVAL_OPTIONS = [
  { value: 'day', label: 'Tag', days: 1 },
  { value: 'week', label: 'Woche', days: 7 },
  { value: 'month', label: 'Monat', days: 30 },
  { value: 'three_months', label: '3 Monate', days: 90 },
  { value: 'all', label: 'Alle', days: 365 }
];

const KEEPA_SORT_OPTIONS = [
  { value: 'percent', label: 'Prozent' },
  { value: 'price_drop', label: 'Preissturz' },
  { value: 'price', label: 'Preis' },
  { value: 'newest', label: 'Neueste' },
  { value: 'sales_rank', label: 'Sales Rank' }
];

const KEEPA_AMAZON_OFFER_OPTIONS = [
  { value: 'all', label: 'egal' },
  { value: 'require', label: 'nur mit Amazon-Angebot' },
  { value: 'exclude', label: 'kein Amazon-Angebot' }
];

const DEFAULT_DRAWER_CONFIGS = {
  AMAZON: {
    active: true,
    sellerType: 'AMAZON',
    patternSupportEnabled: true,
    trendInterval: 'week',
    minDiscount: 20,
    minPrice: null,
    maxPrice: null,
    categories: [],
    onlyPrime: false,
    onlyInStock: true,
    onlyGoodRating: false,
    onlyWithReviews: true,
    amazonOfferMode: 'require',
    singleVariantOnly: false,
    recentPriceChangeOnly: false,
    sortBy: 'percent',
    autoModeAllowed: false,
    testGroupPostingAllowed: true
  },
  FBA: {
    active: true,
    sellerType: 'FBA',
    patternSupportEnabled: true,
    trendInterval: 'week',
    minDiscount: 25,
    minPrice: null,
    maxPrice: null,
    categories: [],
    onlyPrime: false,
    onlyInStock: true,
    onlyGoodRating: false,
    onlyWithReviews: true,
    amazonOfferMode: 'exclude',
    singleVariantOnly: false,
    recentPriceChangeOnly: false,
    sortBy: 'percent',
    autoModeAllowed: false,
    testGroupPostingAllowed: true
  },
  FBM: {
    active: true,
    sellerType: 'FBM',
    patternSupportEnabled: true,
    trendInterval: 'month',
    minDiscount: 35,
    minPrice: null,
    maxPrice: null,
    categories: [],
    onlyPrime: false,
    onlyInStock: true,
    onlyGoodRating: false,
    onlyWithReviews: true,
    amazonOfferMode: 'exclude',
    singleVariantOnly: true,
    recentPriceChangeOnly: false,
    sortBy: 'percent',
    autoModeAllowed: false,
    testGroupPostingAllowed: true
  }
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
  drawerConfigs: DEFAULT_DRAWER_CONFIGS,
  loggingEnabled: true,
  estimatedTokensPerManualRun: 8
};

const MAX_MANUAL_PAGE_SIZE = 48;
const MAX_MANUAL_PAGE = 10;
const SEARCH_DATE_RANGE_DAYS = 7;
const SCHEDULER_INTERVAL_MS = 60 * 1000;
const DEFAULT_KEEPA_TEST_ASIN = 'B0DDKZBYK6';
const KEEPA_HARD_STOP_MIN_TOKENS = 150;
const KEEPA_AUTO_HARD_STOP_MIN_TOKENS = 220;
const KEEPA_MANUAL_COOLDOWN_MS = 45 * 1000;
const KEEPA_MANUAL_CONFIRM_TTL_MS = 3 * 60 * 1000;
const KEEPA_MANUAL_RESULT_LIMIT_CAP = 12;
const KEEPA_MANUAL_TOKEN_BUFFER = 30;
const KEEPA_REQUEST_WINDOW_60S_MS = 60 * 1000;
const KEEPA_REQUEST_WINDOW_5M_MS = 5 * 60 * 1000;
const KEEPA_MAX_REQUESTS_PER_60S = 6;
const KEEPA_MAX_REQUESTS_PER_5M = 16;
const KEEPA_MAX_TOKENS_PER_60S = 70;
const KEEPA_MAX_TOKENS_PER_5M = 180;
const KEEPA_EXPENSIVE_REQUEST_TOKENS = 20;

let keepaQueue = Promise.resolve();
let lastKeepaRequestStartedAt = 0;
let schedulerStarted = false;
let schedulerRunning = false;
let keepaConnectionCache = null;
let keepaApiKeyLogWritten = false;
let keepaSearchExecutionState = {
  active: false,
  source: '',
  drawerKey: '',
  startedAt: null
};
let keepaManualProtectionState = {
  cooldownUntil: 0,
  lastFinishedAt: null,
  lastBlockedAt: null,
  lastBlockedReason: '',
  pendingConfirmation: null
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

function ensureKeepaApiKeyLog(key) {
  if (!key || keepaApiKeyLogWritten) {
    return;
  }

  keepaApiKeyLogWritten = true;
  logGeneratorDebug('KEEPA API KEY LOADED', {
    configured: true,
    keyPresent: true,
    keyLength: key.length
  });
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

function normalizeKeepaUsageMode(value, fallback = 'manual') {
  const normalized = cleanText(String(value || ''))
    .toLowerCase()
    .replace(/[^a-z]/g, '');
  return ['manual', 'auto', 'test'].includes(normalized) ? normalized : fallback;
}

function resolveKeepaUsageMode(entry = {}, action = 'manual-search', module = 'manual-search') {
  const explicitMode = normalizeKeepaUsageMode(entry.mode, '');
  if (explicitMode) {
    return explicitMode;
  }

  if (action === 'automation-run' || module === 'automation-run') {
    return 'auto';
  }

  if (action === 'test-connection' || module === 'test-connection' || module === 'status-check') {
    return 'test';
  }

  return 'manual';
}

function sanitizeUsageDrawerKey(value, fallback = '') {
  const normalized = cleanText(String(value || '')).toUpperCase();
  return ['AMAZON', 'FBA', 'FBM'].includes(normalized) ? normalized : fallback;
}

function deriveUsageDrawerKey(entry = {}) {
  const explicitDrawer = sanitizeUsageDrawerKey(entry.drawerKey, '');
  if (explicitDrawer) {
    return explicitDrawer;
  }

  const filterDrawer = sanitizeUsageDrawerKey(entry.filters?.drawerKey, '');
  if (filterDrawer) {
    return filterDrawer;
  }

  const metaDrawer = sanitizeUsageDrawerKey(entry.meta?.drawerKey, '');
  if (metaDrawer) {
    return metaDrawer;
  }

  const sellerDrawer = sanitizeUsageDrawerKey(entry.filters?.sellerType || entry.meta?.sellerType, '');
  return sellerDrawer || '';
}

function resolveKeepaTokensUsedValue(source = {}) {
  const direct = parseNumber(source.tokensUsed, null);
  if (direct !== null && direct >= 0) {
    return Math.round(direct * 10) / 10;
  }

  const before = parseInteger(source.tokensBefore, null);
  const after = parseInteger(source.tokensAfter ?? source.officialTokensLeft, null);
  if (before !== null && after !== null && before >= after) {
    return Math.round((before - after) * 10) / 10;
  }

  const official = parseNumber(source.officialUsageValue, null);
  if (official !== null && official >= 0) {
    return Math.round(official * 10) / 10;
  }

  const estimated = parseNumber(source.estimatedUsage, null);
  if (estimated !== null && estimated >= 0) {
    return Math.round(estimated * 10) / 10;
  }

  return 0;
}

function sanitizeUsageFilters(filters) {
  if (!filters || typeof filters !== 'object') {
    return null;
  }

  const allowedKeys = [
    'page',
    'limit',
    'drawerKey',
    'sellerType',
    'minDiscount',
    'minPrice',
    'maxPrice',
    'trendInterval',
    'sortBy',
    'onlyPrime',
    'onlyInStock',
    'onlyGoodRating',
    'onlyWithReviews',
    'amazonOfferMode',
    'singleVariantOnly',
    'recentPriceChangeOnly',
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

function createKeepaProtectionError(message, code = 'KEEPA_PROTECTION_BLOCKED', statusCode = 429, details = null) {
  const error = new Error(message);
  error.code = code;
  error.statusCode = statusCode;
  error.details = details;
  return error;
}

function buildKeepaManualConfirmationToken() {
  return `keepa-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}

function buildKeepaManualSearchFingerprint(filters = {}) {
  return JSON.stringify({
    drawerKey: normalizeDrawerKey(filters.drawerKey || inferDrawerKeyFromSellerType(filters.sellerType)),
    page: clamp(parseInteger(filters.page, 1), 1, MAX_MANUAL_PAGE),
    limit: clamp(parseInteger(filters.limit, DEFAULT_SETTINGS.defaultPageSize), 1, KEEPA_MANUAL_RESULT_LIMIT_CAP),
    sellerType: normalizeSellerType(filters.sellerType),
    categories: normalizeCategoryIds(filters.categories, []),
    minDiscount: clamp(parseNumber(filters.minDiscount, 0), 0, 95),
    minPrice: sanitizePriceBoundary(filters.minPrice),
    maxPrice: sanitizePriceBoundary(filters.maxPrice),
    trendInterval: normalizeTrendInterval(filters.trendInterval),
    sortBy: normalizeSortBy(filters.sortBy),
    onlyPrime: parseBool(filters.onlyPrime, false),
    onlyInStock: parseBool(filters.onlyInStock, false),
    onlyGoodRating: parseBool(filters.onlyGoodRating, false),
    onlyWithReviews: parseBool(filters.onlyWithReviews, false),
    amazonOfferMode: normalizeAmazonOfferMode(filters.amazonOfferMode),
    singleVariantOnly: parseBool(filters.singleVariantOnly, false),
    recentPriceChangeOnly: parseBool(filters.recentPriceChangeOnly, false),
    domainId: parseInteger(filters.domainId, DEFAULT_SETTINGS.domainId)
  });
}

function estimateProtectedManualTokenCost(filters, settings) {
  const cappedLimit = clamp(parseInteger(filters.limit, settings?.defaultPageSize || DEFAULT_SETTINGS.defaultPageSize), 1, KEEPA_MANUAL_RESULT_LIMIT_CAP);
  let estimate = Math.max(estimateSearchUsage({ ...filters, limit: cappedLimit }, settings, { requestCount: 2 }), 18 + cappedLimit * 4);

  if (!normalizeCategoryIds(filters.categories, []).length) {
    estimate += 10;
  }

  if (filters.minPrice === null && filters.maxPrice === null) {
    estimate += 6;
  }

  if (filters.amazonOfferMode === 'all') {
    estimate += 8;
  }

  if (!filters.onlyWithReviews) {
    estimate += 8;
  }

  if (filters.trendInterval === 'month') {
    estimate += 8;
  }

  if (filters.trendInterval === 'three_months') {
    estimate += 14;
  }

  if (filters.trendInterval === 'all') {
    estimate += 20;
  }

  if (filters.minDiscount < 25) {
    estimate += 12;
  } else if (filters.minDiscount < 40) {
    estimate += 6;
  }

  return clamp(Math.round(estimate), 12, 220);
}

function analyzeKeepaManualQueryRisk(filters, settings) {
  const categories = normalizeCategoryIds(filters.categories, []);
  const cappedLimit = clamp(parseInteger(filters.limit, settings?.defaultPageSize || DEFAULT_SETTINGS.defaultPageSize), 1, KEEPA_MANUAL_RESULT_LIMIT_CAP);
  const warnings = [];
  const blockingReasons = [];
  let riskScore = 0;

  if (parseInteger(filters.limit, cappedLimit) > KEEPA_MANUAL_RESULT_LIMIT_CAP) {
    warnings.push(`Trefferlimit wurde auf ${KEEPA_MANUAL_RESULT_LIMIT_CAP} Ergebnisse begrenzt.`);
    riskScore += 1;
  }

  if (!categories.length) {
    warnings.push('Keine Kategorie gesetzt: die Query bleibt dadurch breiter.');
    riskScore += 2;
  }

  if (filters.minPrice === null && filters.maxPrice === null) {
    warnings.push('Keine Preisgrenze gesetzt: die Query bleibt preislich offen.');
    riskScore += 1;
  }

  if (!filters.onlyWithReviews) {
    warnings.push('Ohne Bewertungsfilter koennen deutlich mehr Rohdeals entstehen.');
    riskScore += 1;
  }

  if (filters.amazonOfferMode === 'all') {
    warnings.push('Amazon-Angebot ist nicht eingegrenzt.');
    riskScore += 1;
  }

  if (filters.trendInterval === 'month') {
    warnings.push('Monatsintervall erzeugt mehr Rohdeals als Tag/Woche.');
    riskScore += 1;
  }

  if (filters.trendInterval === 'three_months') {
    warnings.push('3-Monats-Intervall ist fuer Live-Suchen deutlich breiter.');
    riskScore += 2;
  }

  if (filters.trendInterval === 'all') {
    blockingReasons.push('Intervall "Alle" ist fuer manuelle Live-Abfragen gesperrt.');
  }

  if (filters.trendInterval === 'three_months' && !categories.length && filters.minDiscount < 40) {
    blockingReasons.push('3 Monate ohne Kategorie und mit niedrigem Mindest-Rabatt ist zu breit.');
  }

  if (!categories.length && filters.minDiscount < 25 && filters.minPrice === null && filters.maxPrice === null) {
    blockingReasons.push('Die Query ist ohne Kategorie, ohne Preisgrenzen und mit sehr niedrigem Mindest-Rabatt zu breit.');
  }

  const riskLevel = blockingReasons.length ? 'blocked' : riskScore >= 4 ? 'high' : riskScore >= 2 ? 'medium' : 'low';

  return {
    cappedLimit,
    estimatedTokenCost: estimateProtectedManualTokenCost(filters, settings),
    estimatedRiskScore: riskScore,
    estimatedRawHitsRisk: riskLevel,
    warnings,
    blockingReasons
  };
}

function getKeepaRequestWindowMetrics(windowMs = KEEPA_REQUEST_WINDOW_60S_MS) {
  const startedAt = new Date(Date.now() - windowMs).toISOString();
  const row =
    db
      .prepare(
        `
          SELECT
            COUNT(*) AS requestCount,
            COALESCE(SUM(COALESCE(tokens_used, official_usage_value, estimated_usage, 0)), 0) AS tokensUsed,
            COALESCE(SUM(result_count), 0) AS resultCount,
            MAX(created_at) AS lastRequestAt
          FROM keepa_usage_logs
          WHERE action = 'keepa-request'
            AND created_at >= ?
        `
      )
      .get(startedAt) || {};
  const requestCount = parseInteger(row.requestCount, 0);
  const tokensUsed = Math.round((parseNumber(row.tokensUsed, 0) || 0) * 10) / 10;
  const resultCount = parseInteger(row.resultCount, 0);
  const windowMinutes = Math.max(windowMs / 60000, 1);

  return {
    windowMs,
    windowSeconds: Math.round(windowMs / 1000),
    requestCount,
    tokensUsed,
    resultCount,
    requestsPerMinute: Math.round(((requestCount / windowMinutes) * 10)) / 10,
    avgTokensPerRequest: requestCount ? Math.round(((tokensUsed / requestCount) * 10)) / 10 : 0,
    avgTokensPerResult: resultCount ? Math.round(((tokensUsed / resultCount) * 10)) / 10 : 0,
    lastRequestAt: row.lastRequestAt || null
  };
}

function getLatestKeepaApiRequest() {
  const row = db
    .prepare(
      `
        SELECT *
        FROM keepa_usage_logs
        WHERE action = 'keepa-request'
        ORDER BY created_at DESC
        LIMIT 1
      `
    )
    .get();

  return row ? mapUsageLogRow(row) : null;
}

function getMostExpensiveKeepaApiRequest() {
  const row = db
    .prepare(
      `
        SELECT *
        FROM keepa_usage_logs
        WHERE action = 'keepa-request'
        ORDER BY COALESCE(tokens_used, official_usage_value, estimated_usage, 0) DESC, created_at DESC
        LIMIT 1
      `
    )
    .get();

  return row ? mapUsageLogRow(row) : null;
}

function buildKeepaTrackingWarnings({ recent60s, recent5m, lastRequest, expensiveRequest, hardStopActive = false } = {}) {
  const warnings = [];

  if (hardStopActive) {
    warnings.push({
      code: 'HARD_STOP_ACTIVE',
      level: 'danger',
      title: 'Hard Stop aktiv',
      message: 'Neue Keepa-Requests werden aktuell durch die Schutzschicht blockiert.'
    });
  }

  if ((recent60s?.tokensUsed ?? 0) >= KEEPA_MAX_TOKENS_PER_60S || (recent5m?.tokensUsed ?? 0) >= KEEPA_MAX_TOKENS_PER_5M) {
    warnings.push({
      code: 'HIGH_TOKEN_USAGE',
      level: 'warning',
      title: 'High Token Usage',
      message: `${recent60s?.tokensUsed ?? 0} Tokens in 60s / ${recent5m?.tokensUsed ?? 0} Tokens in 5m.`,
      value: recent60s?.tokensUsed ?? 0
    });
  }

  if ((recent60s?.requestCount ?? 0) >= KEEPA_MAX_REQUESTS_PER_60S || (recent5m?.requestCount ?? 0) >= KEEPA_MAX_REQUESTS_PER_5M) {
    warnings.push({
      code: 'TOO_MANY_REQUESTS',
      level: 'warning',
      title: 'Too Many Requests',
      message: `${recent60s?.requestCount ?? 0} Requests in 60s / ${recent5m?.requestCount ?? 0} Requests in 5m.`,
      value: recent60s?.requestCount ?? 0
    });
  }

  if ((lastRequest?.tokensUsed ?? 0) >= KEEPA_EXPENSIVE_REQUEST_TOKENS || (expensiveRequest?.tokensUsed ?? 0) >= KEEPA_EXPENSIVE_REQUEST_TOKENS) {
    warnings.push({
      code: 'EXPENSIVE_QUERY',
      level: 'warning',
      title: 'Expensive Query',
      message: `Teuerster Request bisher: ${expensiveRequest?.tokensUsed ?? lastRequest?.tokensUsed ?? 0} Tokens.`,
      value: expensiveRequest?.tokensUsed ?? lastRequest?.tokensUsed ?? 0
    });
  }

  return warnings;
}

async function getKeepaProtectionConnection(reason = 'status-check') {
  const keepaConfig = getKeepaConfig();

  if (!keepaConfig.key) {
    return {
      configured: false,
      connected: false,
      tokensLeft: null,
      checkedAt: null,
      refillRate: null,
      refillInMs: null,
      errorMessage: 'KEEPA_API_KEY fehlt im Backend.'
    };
  }

  try {
    const connection = await loadKeepaConnectionStatus(true, reason);
    return {
      ...connection,
      configured: true,
      errorMessage: ''
    };
  } catch (error) {
    return {
      configured: true,
      connected: false,
      tokensLeft: keepaConnectionCache?.tokensLeft ?? null,
      checkedAt: keepaConnectionCache?.checkedAt || null,
      refillRate: keepaConnectionCache?.refillRate ?? null,
      refillInMs: keepaConnectionCache?.refillInMs ?? null,
      errorMessage: buildKeepaRequestError(error, 'Keepa-Verbindung konnte nicht geprueft werden.')
    };
  }
}

function buildKeepaProtectionState({
  origin = 'manual',
  drawerKey = '',
  connection = null,
  risk = null
} = {}) {
  const now = Date.now();
  const cooldownRemainingMs = Math.max(0, keepaManualProtectionState.cooldownUntil - now);
  const recent60s = getKeepaRequestWindowMetrics(KEEPA_REQUEST_WINDOW_60S_MS);
  const recent5m = getKeepaRequestWindowMetrics(KEEPA_REQUEST_WINDOW_5M_MS);
  const activeRequest =
    keepaSearchExecutionState.active
      ? {
          source: keepaSearchExecutionState.source,
          drawerKey: keepaSearchExecutionState.drawerKey,
          startedAt: keepaSearchExecutionState.startedAt
        }
      : null;
  const estimatedTokenCost = risk?.estimatedTokenCost ?? 0;
  const minTokensRequired = origin === 'automatic'
    ? Math.max(KEEPA_AUTO_HARD_STOP_MIN_TOKENS, estimatedTokenCost + KEEPA_MANUAL_TOKEN_BUFFER)
    : Math.max(KEEPA_HARD_STOP_MIN_TOKENS, estimatedTokenCost + KEEPA_MANUAL_TOKEN_BUFFER);
  const lowTokens =
    connection?.tokensLeft !== null &&
    connection?.tokensLeft !== undefined &&
    Number(connection.tokensLeft) < minTokensRequired;

  return {
    origin,
    drawerKey,
    tokensLeft: connection?.tokensLeft ?? null,
    tokensCheckedAt: connection?.checkedAt || null,
    connectionError: !connection?.configured
      ? 'KEEPA_API_KEY fehlt im Backend.'
      : connection?.connected
        ? ''
        : connection?.errorMessage || 'Keepa-Verbindung konnte nicht geprueft werden.',
    minTokensRequired,
    estimatedTokenCost,
    hardStopActive: Boolean(lowTokens),
    cooldownActive: cooldownRemainingMs > 0,
    cooldownRemainingMs,
    recentUsage: {
      last60s: recent60s,
      last5m: recent5m
    },
    tooManyRequestsActive:
      recent60s.requestCount >= KEEPA_MAX_REQUESTS_PER_60S || recent5m.requestCount >= KEEPA_MAX_REQUESTS_PER_5M,
    highUsageActive: recent60s.tokensUsed >= KEEPA_MAX_TOKENS_PER_60S || recent5m.tokensUsed >= KEEPA_MAX_TOKENS_PER_5M,
    requestActive: Boolean(activeRequest),
    currentRequest: activeRequest,
    blocked: false,
    blockCode: '',
    blockReason: '',
    riskLevel: risk?.estimatedRawHitsRisk || 'low',
    riskScore: risk?.estimatedRiskScore ?? 0,
    warnings: risk?.warnings || [],
    blockingReasons: risk?.blockingReasons || [],
    cappedLimit: risk?.cappedLimit ?? null,
    lastFinishedAt: keepaManualProtectionState.lastFinishedAt || null,
    lastBlockedAt: keepaManualProtectionState.lastBlockedAt || null,
    lastBlockedReason: keepaManualProtectionState.lastBlockedReason || '',
    confirmationPending: Boolean(
      keepaManualProtectionState.pendingConfirmation &&
      keepaManualProtectionState.pendingConfirmation.expiresAt > now
    ),
    confirmationExpiresAt:
      keepaManualProtectionState.pendingConfirmation?.expiresAt
        ? new Date(keepaManualProtectionState.pendingConfirmation.expiresAt).toISOString()
        : null
  };
}

function finalizeKeepaProtectionDecision(protection) {
  const next = { ...protection };

  if (protection.requestActive) {
    next.blocked = true;
    next.blockCode = 'KEEPA_REQUEST_ACTIVE';
    next.blockReason = 'Es laeuft bereits eine Keepa-Abfrage. Bitte warte bis der aktuelle Run beendet ist.';
    return next;
  }

  if (protection.cooldownActive && protection.origin === 'manual') {
    next.blocked = true;
    next.blockCode = 'KEEPA_REQUEST_COOLDOWN';
    next.blockReason = `Keepa-Cooldown aktiv. Bitte warte noch ${Math.ceil(protection.cooldownRemainingMs / 1000)} Sekunden.`;
    return next;
  }

  if (protection.connectionError) {
    next.blocked = true;
    next.blockCode = 'KEEPA_NOT_CONNECTED';
    next.blockReason = protection.connectionError;
    return next;
  }

  if (protection.hardStopActive) {
    next.blocked = true;
    next.blockCode = 'KEEPA_LOW_TOKENS';
    next.blockReason = `Keepa-Schutz aktiv – zu wenig Credits. Verfuegbar: ${protection.tokensLeft ?? '-'}, benoetigt: ${protection.minTokensRequired}.`;
    return next;
  }

  if (protection.tooManyRequestsActive) {
    next.blocked = true;
    next.blockCode = 'KEEPA_TOO_MANY_REQUESTS';
    next.blockReason = `Keepa-Schutz aktiv: zu viele Requests in kurzer Zeit (${protection.recentUsage?.last60s?.requestCount ?? 0} in 60s).`;
    return next;
  }

  if (protection.highUsageActive) {
    next.blocked = true;
    next.blockCode = 'KEEPA_HIGH_USAGE_WINDOW';
    next.blockReason = `Keepa-Schutz aktiv: hoher Verbrauch in kurzer Zeit (${protection.recentUsage?.last60s?.tokensUsed ?? 0} Tokens in 60s).`;
    return next;
  }

  if (protection.blockingReasons?.length) {
    next.blocked = true;
    next.blockCode = 'KEEPA_QUERY_TOO_BROAD';
    next.blockReason = protection.blockingReasons[0];
    return next;
  }

  return next;
}

async function buildKeepaManualDryRun(filters, options = {}) {
  const isConfirmed = options.confirmed === true;
  const settings = getKeepaSettings();
  const connection = await getKeepaProtectionConnection('status-check');
  const risk = analyzeKeepaManualQueryRisk(filters, settings);
  const effectiveFilters = {
    ...filters,
    limit: risk.cappedLimit
  };
  const protection = finalizeKeepaProtectionDecision(
    buildKeepaProtectionState({
      origin: 'manual',
      drawerKey: filters.drawerKey,
      connection,
      risk
    })
  );

  protection.connectionError = !connection.configured
    ? 'KEEPA_API_KEY fehlt im Backend.'
    : connection.connected
      ? ''
      : connection.errorMessage || 'Keepa-Verbindung konnte nicht geprueft werden.';

  const blocked = protection.blocked || Boolean(protection.connectionError);
  const confirmationToken = blocked || isConfirmed ? '' : buildKeepaManualConfirmationToken();
  const createdAt = nowIso();
  const expiresAt = Date.now() + KEEPA_MANUAL_CONFIRM_TTL_MS;

  if (blocked) {
    keepaManualProtectionState.lastBlockedAt = createdAt;
    keepaManualProtectionState.lastBlockedReason = protection.blockReason || protection.connectionError || 'Keepa-Schutz aktiv.';
    keepaManualProtectionState.pendingConfirmation = null;

    logGeneratorDebug('KEEPA REQUEST BLOCKED', {
      drawerKey: filters.drawerKey,
      blockCode: protection.blockCode || 'KEEPA_PROTECTION_BLOCKED',
      blockReason: protection.blockReason || protection.connectionError || 'Keepa-Schutz aktiv.'
    });

    if (protection.blockCode === 'KEEPA_LOW_TOKENS') {
      logGeneratorDebug('KEEPA REQUEST BLOCKED LOW TOKENS', {
        drawerKey: filters.drawerKey,
        tokensLeft: protection.tokensLeft,
        minTokensRequired: protection.minTokensRequired
      });
      logGeneratorDebug('KEEPA HARD STOP ACTIVE', {
        drawerKey: filters.drawerKey,
        tokensLeft: protection.tokensLeft,
        minTokensRequired: protection.minTokensRequired
      });
    } else if (protection.blockCode === 'KEEPA_QUERY_TOO_BROAD') {
      logGeneratorDebug('KEEPA REQUEST BLOCKED QUERY TOO BROAD', {
        drawerKey: filters.drawerKey,
        warnings: protection.warnings,
        blockingReasons: protection.blockingReasons
      });
    } else if (protection.blockCode === 'KEEPA_REQUEST_COOLDOWN') {
      logGeneratorDebug('KEEPA REQUEST SKIPPED COOLDOWN', {
        drawerKey: filters.drawerKey,
        cooldownRemainingMs: protection.cooldownRemainingMs
      });
    } else if (protection.blockCode === 'KEEPA_TOO_MANY_REQUESTS' || protection.blockCode === 'KEEPA_HIGH_USAGE_WINDOW') {
      logGeneratorDebug('KEEPA HIGH USAGE DETECTED', {
        drawerKey: filters.drawerKey,
        blockCode: protection.blockCode,
        requestCount60s: protection.recentUsage?.last60s?.requestCount ?? 0,
        tokensUsed60s: protection.recentUsage?.last60s?.tokensUsed ?? 0,
        requestCount5m: protection.recentUsage?.last5m?.requestCount ?? 0,
        tokensUsed5m: protection.recentUsage?.last5m?.tokensUsed ?? 0
      });
    }
  } else if (!isConfirmed) {
    keepaManualProtectionState.pendingConfirmation = {
      token: confirmationToken,
      fingerprint: buildKeepaManualSearchFingerprint(effectiveFilters),
      expiresAt,
      filters: effectiveFilters
    };

    logGeneratorDebug('KEEPA DRY RUN CREATED', {
      drawerKey: filters.drawerKey,
      estimatedTokenCost: risk.estimatedTokenCost,
      tokensLeft: protection.tokensLeft,
      riskLevel: protection.riskLevel,
      cappedLimit: risk.cappedLimit
    });
    logGeneratorDebug('KEEPA USER CONFIRMATION REQUIRED', {
      drawerKey: filters.drawerKey,
      confirmationExpiresAt: new Date(expiresAt).toISOString()
    });
  }

  return {
    blocked,
    confirmationRequired: !blocked && !isConfirmed,
    confirmationToken: !blocked && !isConfirmed ? confirmationToken : null,
    createdAt,
    expiresAt: new Date(expiresAt).toISOString(),
    effectiveFilters,
    protection: {
      ...protection,
      connectionError: protection.connectionError || ''
    }
  };
}

function validateKeepaManualConfirmation(filters, confirmationToken) {
  const pending = keepaManualProtectionState.pendingConfirmation;
  if (!pending || pending.expiresAt <= Date.now()) {
    keepaManualProtectionState.pendingConfirmation = null;
    throw createKeepaProtectionError(
      'Bitte zuerst einen aktuellen Dry-Run erzeugen und danach bewusst bestaetigen.',
      'KEEPA_CONFIRMATION_REQUIRED',
      409
    );
  }

  if (cleanText(confirmationToken) !== pending.token) {
    throw createKeepaProtectionError(
      'Die bestaetigte Keepa-Abfrage ist abgelaufen oder passt nicht mehr zu den aktuellen Filtern.',
      'KEEPA_CONFIRMATION_INVALID',
      409
    );
  }

  if (pending.fingerprint !== buildKeepaManualSearchFingerprint(filters)) {
    throw createKeepaProtectionError(
      'Die Filter wurden nach dem Dry-Run geaendert. Bitte zuerst eine neue Vorschau erzeugen.',
      'KEEPA_CONFIRMATION_STALE',
      409
    );
  }

  return pending.filters || filters;
}

function beginKeepaSearchExecution(source, drawerKey) {
  if (keepaSearchExecutionState.active) {
    throw createKeepaProtectionError(
      'Es laeuft bereits eine Keepa-Abfrage. Bitte warte bis der aktuelle Run beendet ist.',
      'KEEPA_REQUEST_ACTIVE',
      429
    );
  }

  keepaSearchExecutionState = {
    active: true,
    source,
    drawerKey,
    startedAt: nowIso()
  };
}

function finishKeepaSearchExecution(source = 'manual') {
  keepaSearchExecutionState = {
    active: false,
    source: '',
    drawerKey: '',
    startedAt: null
  };

  if (source === 'manual') {
    keepaManualProtectionState.cooldownUntil = Date.now() + KEEPA_MANUAL_COOLDOWN_MS;
    keepaManualProtectionState.lastFinishedAt = nowIso();
    keepaManualProtectionState.pendingConfirmation = null;
  }
}

function recordKeepaUsage(entry = {}) {
  const createdAt = cleanText(entry.createdAt) || nowIso();
  const usageDate = toLocalDateKey(createdAt);
  const action = normalizeUsageAction(entry.action, 'manual-search');
  const module = normalizeUsageModule(entry.module || entry.source, 'manual-search');
  const mode = resolveKeepaUsageMode(entry, action, module);
  const drawerKey = deriveUsageDrawerKey(entry);
  const requestStatus = normalizeUsageStatus(entry.requestStatus, 'success');
  const resultCount = parseInteger(entry.resultCount, 0);
  const timestampStart = cleanText(entry.timestampStart) || createdAt;
  const timestampEnd = cleanText(entry.timestampEnd) || createdAt;
  const durationMs = clamp(parseInteger(entry.durationMs, 0), 0, 24 * 60 * 60 * 1000);
  const estimatedUsage = Math.max(0, parseNumber(entry.estimatedUsage, 0) ?? 0);
  const tokensBefore = parseInteger(entry.tokensBefore, null);
  const officialUsageValue = parseNumber(entry.officialUsageValue, null);
  const officialTokensLeft = parseInteger(entry.officialTokensLeft, null);
  const tokensAfter = parseInteger(entry.tokensAfter ?? officialTokensLeft, null);
  const tokensUsed = resolveKeepaTokensUsedValue({
    tokensUsed: entry.tokensUsed,
    tokensBefore,
    tokensAfter,
    officialTokensLeft,
    officialUsageValue,
    estimatedUsage
  });
  const ruleId = parseInteger(entry.ruleId, null);
  const errorMessage = cleanText(entry.errorMessage) || null;
  const filtersJson = toJson(sanitizeUsageFilters(entry.filters));
  const metaJson = toJson(sanitizeUsageMeta(entry.meta));

  db.prepare(
    `
      INSERT INTO keepa_usage_logs (
        action,
        module,
        mode,
        drawer_key,
        timestamp_start,
        timestamp_end,
        tokens_before,
        tokens_after,
        tokens_used,
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
        @mode,
        @drawerKey,
        @timestampStart,
        @timestampEnd,
        @tokensBefore,
        @tokensAfter,
        @tokensUsed,
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
    mode,
    drawerKey,
    timestampStart,
    timestampEnd,
    tokensBefore,
    tokensAfter,
    tokensUsed,
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
        tokens_used_total,
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
        @tokensUsed,
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
        tokens_used_total = COALESCE(tokens_used_total, 0) + @tokensUsed,
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
    tokensUsed,
    successCount: requestStatus === 'success' ? 1 : 0,
    errorCount: requestStatus === 'error' ? 1 : 0,
    durationMs,
    createdAt
  });

  logGeneratorDebug('KEEPA TOKEN/CREDIT TRACK UPDATED', {
    action,
    module,
    mode,
    drawerKey,
    requestStatus,
    estimatedUsage,
    officialUsageValue,
    tokensBefore,
    tokensAfter,
    tokensUsed,
    officialTokensLeft,
    resultCount,
    createdAt
  });

  return {
    action,
    module,
    mode,
    drawerKey,
    requestStatus,
    timestampStart,
    timestampEnd,
    resultCount,
    durationMs,
    estimatedUsage,
    officialUsageValue,
    tokensBefore,
    tokensAfter,
    tokensUsed,
    officialTokensLeft,
    ruleId,
    createdAt
  };
}

function normalizeSellerType(value) {
  const normalized = cleanText(String(value || '')).toUpperCase();
  return ['ALL', 'AMAZON', 'FBA', 'FBM'].includes(normalized) ? normalized : 'ALL';
}

function normalizeDrawerKey(value, fallback = 'AMAZON') {
  const normalized = cleanText(String(value || '')).toUpperCase();
  return KEEPA_DRAWER_CATALOG.some((item) => item.key === normalized) ? normalized : fallback;
}

function normalizeTrendInterval(value, fallback = 'week') {
  const normalized = cleanText(String(value || '')).toLowerCase();
  return KEEPA_TREND_INTERVAL_OPTIONS.some((item) => item.value === normalized) ? normalized : fallback;
}

function normalizeSortBy(value, fallback = 'percent') {
  const normalized = cleanText(String(value || '')).toLowerCase();
  return KEEPA_SORT_OPTIONS.some((item) => item.value === normalized) ? normalized : fallback;
}

function normalizeAmazonOfferMode(value, fallback = 'all') {
  const normalized = cleanText(String(value || '')).toLowerCase();
  return KEEPA_AMAZON_OFFER_OPTIONS.some((item) => item.value === normalized) ? normalized : fallback;
}

function normalizeDrawerConfig(drawerKey, config = {}, fallback = DEFAULT_DRAWER_CONFIGS[drawerKey]) {
  const fallbackConfig = fallback || DEFAULT_DRAWER_CONFIGS[normalizeDrawerKey(drawerKey)];
  const minPrice = sanitizePriceBoundary(config?.minPrice ?? fallbackConfig.minPrice);
  const maxPrice = sanitizePriceBoundary(config?.maxPrice ?? fallbackConfig.maxPrice);

  return {
    active: parseBool(config?.active, fallbackConfig.active),
    sellerType: normalizeSellerType(config?.sellerType || fallbackConfig.sellerType),
    patternSupportEnabled: parseBool(config?.patternSupportEnabled, fallbackConfig.patternSupportEnabled),
    trendInterval: normalizeTrendInterval(config?.trendInterval, fallbackConfig.trendInterval),
    minDiscount: clamp(parseNumber(config?.minDiscount, fallbackConfig.minDiscount), 0, 95),
    minPrice,
    maxPrice,
    categories: normalizeCategoryIds(config?.categories, fallbackConfig.categories),
    onlyPrime: parseBool(config?.onlyPrime, fallbackConfig.onlyPrime),
    onlyInStock: parseBool(config?.onlyInStock, fallbackConfig.onlyInStock),
    onlyGoodRating: parseBool(config?.onlyGoodRating, fallbackConfig.onlyGoodRating),
    onlyWithReviews: parseBool(config?.onlyWithReviews, fallbackConfig.onlyWithReviews),
    amazonOfferMode: normalizeAmazonOfferMode(config?.amazonOfferMode, fallbackConfig.amazonOfferMode),
    singleVariantOnly: parseBool(config?.singleVariantOnly, fallbackConfig.singleVariantOnly),
    recentPriceChangeOnly: parseBool(config?.recentPriceChangeOnly, fallbackConfig.recentPriceChangeOnly),
    sortBy: normalizeSortBy(config?.sortBy, fallbackConfig.sortBy),
    autoModeAllowed: parseBool(config?.autoModeAllowed, fallbackConfig.autoModeAllowed),
    testGroupPostingAllowed: parseBool(config?.testGroupPostingAllowed, fallbackConfig.testGroupPostingAllowed)
  };
}

function normalizeDrawerConfigs(config = {}, fallback = DEFAULT_DRAWER_CONFIGS) {
  return Object.fromEntries(
    KEEPA_DRAWER_CATALOG.map((item) => [
      item.key,
      normalizeDrawerConfig(item.key, config?.[item.key], fallback?.[item.key] || DEFAULT_DRAWER_CONFIGS[item.key])
    ])
  );
}

function inferDrawerKeyFromSellerType(value) {
  const sellerType = normalizeSellerType(value);
  if (sellerType === 'AMAZON' || sellerType === 'FBA' || sellerType === 'FBM') {
    return sellerType;
  }

  return 'AMAZON';
}

function collectManualDrawerCandidates(input = {}) {
  const candidates = new Set();

  const addCandidate = (value) => {
    const normalized = normalizeDrawerKey(value, '');
    if (normalized) {
      candidates.add(normalized);
    }
  };

  addCandidate(input.drawerKey);

  if (Array.isArray(input.drawerKeys)) {
    input.drawerKeys.forEach(addCandidate);
  } else {
    addCandidate(input.drawerKeys);
  }

  if (Array.isArray(input.selectedDrawers)) {
    input.selectedDrawers.forEach(addCandidate);
  } else if (input.selectedDrawers && typeof input.selectedDrawers === 'object') {
    Object.entries(input.selectedDrawers).forEach(([key, enabled]) => {
      if (parseBool(enabled, false)) {
        addCandidate(key);
      }
    });
  } else {
    addCandidate(input.selectedDrawers);
  }

  if (input.drawers && typeof input.drawers === 'object' && !Array.isArray(input.drawers)) {
    Object.entries(input.drawers).forEach(([key, enabled]) => {
      if (parseBool(enabled, false)) {
        addCandidate(key);
      }
    });
  } else if (Array.isArray(input.drawers)) {
    input.drawers.forEach(addCandidate);
  }

  if (parseBool(input.amazonEnabled, false) || parseBool(input.useAmazon, false) || parseBool(input.amazonSelected, false)) {
    addCandidate('AMAZON');
  }
  if (parseBool(input.fbaEnabled, false) || parseBool(input.useFba, false) || parseBool(input.fbaSelected, false)) {
    addCandidate('FBA');
  }
  if (parseBool(input.fbmEnabled, false) || parseBool(input.useFbm, false) || parseBool(input.fbmSelected, false)) {
    addCandidate('FBM');
  }

  const sellerType = normalizeSellerType(input.sellerType);
  if (sellerType !== 'ALL') {
    addCandidate(sellerType);
  }

  return [...candidates];
}

function resolveManualDrawerSelection(input = {}, settings = getKeepaSettings()) {
  const explicitDrawerKeys = collectManualDrawerCandidates(input);
  if (explicitDrawerKeys.length > 1) {
    logGeneratorDebug('MULTI DRAWER REQUEST BLOCKED', {
      drawerKeys: explicitDrawerKeys
    });
    throw createKeepaProtectionError(
      'Bitte genau eine Schublade fuer die manuelle Keepa-Abfrage auswaehlen: AMAZON, FBA oder FBM.',
      'KEEPA_MULTI_DRAWER_BLOCKED',
      409
    );
  }

  const requestedDrawerKey = explicitDrawerKeys[0] || '';
  const sellerType = normalizeSellerType(input.sellerType || settings.defaultSellerType);
  const sellerTypeDrawerKey = sellerType !== 'ALL' ? inferDrawerKeyFromSellerType(sellerType) : '';

  if (requestedDrawerKey && sellerTypeDrawerKey && requestedDrawerKey !== sellerTypeDrawerKey) {
    logGeneratorDebug('MULTI DRAWER REQUEST BLOCKED', {
      drawerKeys: [requestedDrawerKey, sellerTypeDrawerKey]
    });
    throw createKeepaProtectionError(
      'Die manuelle Keepa-Abfrage hat widerspruechliche Schubladen-Angaben. Bitte genau eine Auswahl verwenden.',
      'KEEPA_MULTI_DRAWER_BLOCKED',
      409
    );
  }

  return normalizeDrawerKey(requestedDrawerKey || sellerTypeDrawerKey || settings.defaultSellerType || 'AMAZON');
}

function getDrawerConfig(settings, drawerKey) {
  const resolvedDrawerKey = normalizeDrawerKey(drawerKey, 'AMAZON');
  return normalizeDrawerConfig(
    resolvedDrawerKey,
    settings?.drawerConfigs?.[resolvedDrawerKey],
    DEFAULT_DRAWER_CONFIGS[resolvedDrawerKey]
  );
}

export function getKeepaDrawerControlConfig(value) {
  const settings = getKeepaSettings();
  const drawerKey = inferDrawerKeyFromSellerType(value);
  return getDrawerConfig(settings, drawerKey);
}

function resolveKeepaDealDateRangeCode(trendInterval) {
  const normalized = normalizeTrendInterval(trendInterval, 'week');
  return (
    {
      day: 0,
      week: 1,
      month: 2,
      three_months: 3,
      all: 4
    }[normalized] ?? 1
  );
}

function resolveKeepaDealSortType(sortBy) {
  switch (normalizeSortBy(sortBy, 'percent')) {
    case 'newest':
      return 1;
    case 'price_drop':
      return 2;
    case 'sales_rank':
      return 3;
    case 'price':
      return 5;
    case 'percent':
    default:
      return 4;
  }
}

function resolveKeepaDealPriceTypes(sellerType) {
  switch (normalizeSellerType(sellerType)) {
    case 'AMAZON':
      return [0];
    case 'FBA':
      return [10];
    case 'FBM':
      return [7];
    default:
      return [1];
  }
}

function sanitizeKeepaDealSelection(selection = {}) {
  const sanitized = {};

  Object.entries(selection).forEach(([key, value]) => {
    if (value === undefined || value === null || value === '') {
      return;
    }

    if (Array.isArray(value)) {
      if (!value.length && !['includeCategories', 'excludeCategories'].includes(key)) {
        return;
      }

      sanitized[key] = value;
      return;
    }

    sanitized[key] = value;
  });

  return sanitized;
}

function buildKeepaManualSelection(filters, settings, rawPageSize) {
  const dateRange = resolveKeepaDealDateRangeCode(filters.trendInterval);
  const sortType = resolveKeepaDealSortType(filters.sortBy);
  const priceTypes = resolveKeepaDealPriceTypes(filters.sellerType);
  const includeCategories = normalizeCategoryIds(filters.categories, []);
  const mustHaveAmazonOffer = filters.amazonOfferMode === 'require';
  const mustNotHaveAmazonOffer = filters.amazonOfferMode === 'exclude';

  const selection = sanitizeKeepaDealSelection({
    page: Math.max(0, filters.page - 1),
    domainId: filters.domainId,
    includeCategories,
    excludeCategories: [],
    priceTypes,
    currentRange: [
      filters.minPrice !== null ? toMinorUnits(filters.minPrice) : 0,
      filters.maxPrice !== null ? toMinorUnits(filters.maxPrice) : 2147483647
    ],
    deltaRange: [0, 1000000],
    deltaPercentRange: [filters.minDiscount, 95],
    salesRankRange: [-1, -1],
    minRating: filters.onlyGoodRating ? Math.round(settings.goodRatingThreshold * 10) : -1,
    hasReviews: Boolean(filters.onlyWithReviews || filters.onlyGoodRating),
    isOutOfStock: filters.onlyInStock ? false : undefined,
    isLowest: false,
    isLowest90: false,
    isLowestOffer: false,
    titleSearch: '',
    isRangeEnabled: true,
    isFilterEnabled: true,
    filterErotic: true,
    singleVariation: Boolean(filters.singleVariantOnly),
    mustHaveAmazonOffer,
    mustNotHaveAmazonOffer,
    isPrimeExclusive: false,
    sortType,
    dateRange,
    perPage: rawPageSize
  });

  return {
    selection,
    diagnostics: {
      localOnlyFields: ['drawerKey', 'onlyPrime', 'recentPriceChangeOnly'],
      mappedFields: {
        sellerType: { from: filters.sellerType, to: priceTypes },
        trendInterval: { from: filters.trendInterval, to: dateRange },
        sortBy: { from: filters.sortBy, to: sortType },
        amazonOfferMode: {
          from: filters.amazonOfferMode,
          to: { mustHaveAmazonOffer, mustNotHaveAmazonOffer }
        },
        singleVariantOnly: {
          from: filters.singleVariantOnly,
          to: selection.singleVariation
        }
      }
    }
  };
}

function extractKeepaDealRows(data) {
  if (Array.isArray(data?.deals?.dr)) {
    return data.deals.dr;
  }

  if (Array.isArray(data?.dr)) {
    return data.dr;
  }

  return [];
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
  const drawerConfigs = normalizeDrawerConfigs(
    fromJson(row.drawer_configs_json, {}),
    DEFAULT_SETTINGS.drawerConfigs
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
    drawerConfigs,
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

function buildAmazonImageUrlFromAsin(asin = '') {
  const normalizedAsin = cleanText(asin).toUpperCase();
  return normalizedAsin ? `https://images-na.ssl-images-amazon.com/images/P/${normalizedAsin}.jpg` : '';
}

function buildAmazonAffiliateUrl(asin, domainId, productUrl = '') {
  const domainInfo = getDomainInfo(domainId);
  const linkRecord = buildAmazonAffiliateLinkRecord(productUrl || buildAmazonProductUrl(asin, domainId), {
    asin,
    defaultHost: domainInfo.host
  });
  return linkRecord.valid ? linkRecord.affiliateUrl : '';
}

function normalizeKeepaResultOrigin(value) {
  const normalized = cleanText(value).toLowerCase();
  if (!normalized) {
    return 'keepa-manual';
  }

  if (normalized === 'manual' || normalized === 'keepa-manual') {
    return 'keepa-manual';
  }

  if (normalized === 'automatic' || normalized === 'keepa-auto') {
    return 'keepa-auto';
  }

  return normalized;
}

function getKeepaResultSourceLabel(origin) {
  const normalized = normalizeKeepaResultOrigin(origin);
  return (
    {
      'keepa-manual': 'manuell / Keepa',
      'keepa-auto': 'automatik / Keepa'
    }[normalized] || normalized
  );
}

function buildKeepaHistorySnapshot(item = {}) {
  return buildKeepaChartSnapshot(item);
}

function buildAmazonImageUrl(imageValue, asin = '') {
  const cleaned = cleanText(imageValue);
  if (!cleaned) {
    return buildAmazonImageUrlFromAsin(asin);
  }

  if (/^https?:\/\//i.test(cleaned)) {
    return cleaned;
  }

  const firstImage = cleaned.split(',').map((item) => item.trim()).filter(Boolean)[0];
  if (!firstImage) {
    return buildAmazonImageUrlFromAsin(asin);
  }

  return `https://m.media-amazon.com/images/I/${firstImage}`;
}

function collectKeepaTitleCandidates(item = {}) {
  return [
    { value: item.title, source: 'item.title' },
    { value: item.keepaPayload?.deal?.title, source: 'keepaPayload.deal.title' },
    { value: item.keepaPayload?.product?.title, source: 'keepaPayload.product.title' },
    { value: item.keepaPayload?.raw?.deal?.title, source: 'keepaPayload.raw.deal.title' },
    { value: item.keepaPayload?.raw?.product?.title, source: 'keepaPayload.raw.product.title' },
    { value: item.keepaPayload?.raw?.product?.itemTitle, source: 'keepaPayload.raw.product.itemTitle' },
    { value: item.keepaPayload?.raw?.product?.groupTitle, source: 'keepaPayload.raw.product.groupTitle' },
    { value: item.keepaPayload?.product?.productGroup, source: 'keepaPayload.product.productGroup' },
    { value: item.asin, source: 'asin' }
  ];
}

function resolveKeepaDealTitle(item = {}, options = {}) {
  const asin = extractAsin(item) || cleanText(item.asin).toUpperCase();
  const winner =
    collectKeepaTitleCandidates(item).find((candidate) => {
      const value = cleanText(candidate.value);
      return Boolean(value);
    }) || null;
  const title = cleanText(winner?.value) || asin;

  if (options.log && title) {
    logGeneratorDebug('DEAL TITLE MAPPED', {
      asin,
      source: winner?.source || 'asin',
      title
    });
  }

  return {
    title,
    source: winner?.source || (asin ? 'asin' : 'missing')
  };
}

function collectKeepaImageCandidates(item = {}) {
  return [
    { value: item.imageUrl || item.image_url, source: 'item.imageUrl' },
    { value: item.keepaPayload?.imageUrl, source: 'keepaPayload.imageUrl' },
    { value: item.keepaPayload?.product?.imageUrl, source: 'keepaPayload.product.imageUrl' },
    { value: item.keepaPayload?.product?.imagesCSV, source: 'keepaPayload.product.imagesCSV' },
    { value: item.keepaPayload?.deal?.image, source: 'keepaPayload.deal.image' },
    { value: item.keepaPayload?.raw?.deal?.image, source: 'keepaPayload.raw.deal.image' },
    { value: item.keepaPayload?.raw?.product?.imagesCSV, source: 'keepaPayload.raw.product.imagesCSV' }
  ];
}

function resolveKeepaDealImageUrl(item = {}, options = {}) {
  const asin = extractAsin(item) || cleanText(item.asin).toUpperCase();
  const winner =
    collectKeepaImageCandidates(item).find((candidate) => {
      const value = cleanText(candidate.value);
      return Boolean(value);
    }) || null;
  const imageUrl = buildAmazonImageUrl(winner?.value || '', asin);
  const usedGeneratedImage = !winner?.value || winner.source !== 'item.imageUrl';

  if (options.log && imageUrl && usedGeneratedImage) {
    logGeneratorDebug('DEAL IMAGE URL GENERATED', {
      asin,
      source: winner?.source || 'asin-fallback',
      imageUrl
    });
  }

  return {
    imageUrl,
    source: winner?.source || (asin ? 'asin-fallback' : 'missing')
  };
}

function resolveKeepaDealProductUrl(item = {}, options = {}) {
  const asin = extractAsin(item) || cleanText(item.asin).toUpperCase();
  const domainId = parseInteger(item.domainId ?? item.domain_id, getKeepaSettings().domainId);
  const existingProductUrl =
    cleanText(item.productUrl || item.product_url) ||
    cleanText(item.keepaPayload?.productUrl) ||
    cleanText(item.keepaPayload?.product?.productUrl) ||
    cleanText(item.keepaPayload?.raw?.product?.productUrl);
  const productUrl = existingProductUrl || buildAmazonProductUrl(asin, domainId);

  if (options.log && productUrl && !existingProductUrl) {
    logGeneratorDebug('DEAL PRODUCT URL GENERATED', {
      asin,
      domainId,
      productUrl
    });
  }

  return {
    productUrl,
    source: existingProductUrl ? 'existing' : asin ? 'asin' : 'missing'
  };
}

function resolveKeepaDealAffiliateUrl(item = {}, productUrl = '') {
  const asin = extractAsin(item) || cleanText(item.asin).toUpperCase();
  const domainId = parseInteger(item.domainId ?? item.domain_id, getKeepaSettings().domainId);
  const existingAffiliateUrl =
    cleanText(item.affiliateUrl || item.affiliate_url) ||
    cleanText(item.keepaPayload?.affiliateUrl) ||
    cleanText(item.keepaPayload?.product?.affiliateUrl);

  return existingAffiliateUrl || buildAmazonAffiliateUrl(asin, domainId, productUrl);
}

function enrichKeepaRecord(item = {}, options = {}) {
  if (!item?.asin) {
    return item;
  }

  const currentPayload =
    item.keepaPayload && typeof item.keepaPayload === 'object' && !Array.isArray(item.keepaPayload) ? item.keepaPayload : {};
  const titleResolution = resolveKeepaDealTitle({ ...item, keepaPayload: currentPayload }, { log: options.log === true });
  const productResolution = resolveKeepaDealProductUrl(
    {
      ...item,
      title: titleResolution.title,
      keepaPayload: currentPayload
    },
    { log: options.log === true }
  );
  const imageResolution = resolveKeepaDealImageUrl(
    {
      ...item,
      title: titleResolution.title,
      productUrl: productResolution.productUrl,
      keepaPayload: currentPayload
    },
    { log: options.log === true }
  );
  const affiliateUrl = resolveKeepaDealAffiliateUrl(
    {
      ...item,
      title: titleResolution.title,
      productUrl: productResolution.productUrl,
      imageUrl: imageResolution.imageUrl,
      keepaPayload: currentPayload
    },
    productResolution.productUrl
  );
  const history = buildKeepaHistorySnapshot({
    ...item,
    title: titleResolution.title,
    productUrl: productResolution.productUrl,
    affiliateUrl,
    imageUrl: imageResolution.imageUrl,
    keepaPayload: currentPayload
  });

  return {
    ...item,
    title: titleResolution.title,
    productUrl: productResolution.productUrl,
    affiliateUrl,
    imageUrl: imageResolution.imageUrl,
    keepaPayload: {
      ...currentPayload,
      title: titleResolution.title,
      productUrl: productResolution.productUrl,
      affiliateUrl,
      imageUrl: imageResolution.imageUrl,
      product: {
        ...(currentPayload.product || {}),
        title: cleanText(currentPayload?.product?.title) || titleResolution.title
      },
      deal: {
        ...(currentPayload.deal || {}),
        title: cleanText(currentPayload?.deal?.title) || titleResolution.title
      },
      history
    }
  };
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
    drawerCatalog: KEEPA_DRAWER_CATALOG,
    trendIntervalOptions: KEEPA_TREND_INTERVAL_OPTIONS,
    sortOptions: KEEPA_SORT_OPTIONS,
    amazonOfferOptions: KEEPA_AMAZON_OFFER_OPTIONS,
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

function createKeepaApiTestError(message, code = 'KEEPA_API_TEST_FAILED', statusCode = 500) {
  const error = new Error(message);
  error.code = code;
  error.statusCode = statusCode;
  return error;
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

function extractSalesRank(product, deal) {
  const directRank = pickNumericField(deal, ['salesRank', 'salesrank']) ?? pickNumericField(product, ['salesRank']);
  if (directRank !== null) {
    return Math.round(directRank);
  }

  const stats = product?.stats || {};
  const statsRank = pickNumericField(stats, ['salesRank', 'currentSalesRank']);
  if (statsRank !== null) {
    return Math.round(statsRank);
  }

  const salesRanks = product?.salesRanks;
  if (salesRanks && typeof salesRanks === 'object') {
    const firstRank = Object.values(salesRanks)
      .flatMap((value) => (Array.isArray(value) ? value : [value]))
      .map((value) => parseInteger(value, null))
      .find((value) => value !== null && value >= 0);

    if (firstRank !== undefined) {
      return firstRank;
    }
  }

  return null;
}

function hasAmazonOffer(product, sellerType) {
  const offers = Array.isArray(product?.offers) ? product.offers : [];
  if (offers.some((offer) => Boolean(offer?.isAmazon))) {
    return true;
  }

  return sellerType === 'AMAZON';
}

function hasMultipleVariations(product) {
  const variations = Array.isArray(product?.variations) ? product.variations : [];
  if (variations.length > 1) {
    return true;
  }

  const variationCsv = cleanText(product?.variationCSV || product?.variationsCSV || '');
  if (!variationCsv) {
    return false;
  }

  return variationCsv
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean).length > 1;
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

    if (filters.onlyWithReviews && (!item.reviewCount || Number(item.reviewCount) <= 0)) {
      return false;
    }

    if (filters.minPrice !== null && item.currentPrice !== null && item.currentPrice < filters.minPrice) {
      return false;
    }

    if (filters.maxPrice !== null && item.currentPrice !== null && item.currentPrice > filters.maxPrice) {
      return false;
    }

    if (filters.amazonOfferMode === 'require' && !item.hasAmazonOffer) {
      return false;
    }

    if (filters.amazonOfferMode === 'exclude' && item.hasAmazonOffer) {
      return false;
    }

    if (filters.singleVariantOnly && item.hasMultipleVariations) {
      return false;
    }

    return true;
  });
}

function sortSearchItems(items, filters) {
  const sortedItems = [...items];

  if (filters.sortBy === 'price_drop') {
    return sortedItems.sort((left, right) => (Number(right.priceDifferenceAbs) || 0) - (Number(left.priceDifferenceAbs) || 0));
  }

  if (filters.sortBy === 'price') {
    return sortedItems.sort((left, right) => {
      const leftPrice = Number(left.currentPrice);
      const rightPrice = Number(right.currentPrice);

      if (!Number.isFinite(leftPrice) && !Number.isFinite(rightPrice)) {
        return 0;
      }

      if (!Number.isFinite(leftPrice)) {
        return 1;
      }

      if (!Number.isFinite(rightPrice)) {
        return -1;
      }

      return leftPrice - rightPrice;
    });
  }

  if (filters.sortBy === 'sales_rank') {
    return sortedItems.sort((left, right) => {
      const leftRank = Number(left.salesRank);
      const rightRank = Number(right.salesRank);

      if (!Number.isFinite(leftRank) && !Number.isFinite(rightRank)) {
        return 0;
      }

      if (!Number.isFinite(leftRank)) {
        return 1;
      }

      if (!Number.isFinite(rightRank)) {
        return -1;
      }

      return leftRank - rightRank;
    });
  }

  if (filters.sortBy === 'newest') {
    return sortedItems;
  }

  return sortedItems.sort((left, right) => (Number(right.keepaDiscount) || 0) - (Number(left.keepaDiscount) || 0));
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

function repairStoredKeepaResultRow(row) {
  if (!row) {
    return row;
  }

  const existingKeepaPayload = fromJson(row.keepa_payload_json, null);
  const nextTitleCandidate = resolveKeepaDealTitle({
    asin: row.asin,
    title: row.title,
    keepaPayload: existingKeepaPayload
  }).title;
  const hydrated = enrichKeepaRecord(
    {
      id: row.id,
      asin: row.asin,
      domainId: row.domain_id,
      title: row.title,
      productUrl: row.product_url,
      imageUrl: row.image_url,
      currentPrice: row.current_price,
      referencePrice: row.reference_price,
      keepaPayload: existingKeepaPayload
    },
    {
      log:
        !cleanText(row.title) ||
        !cleanText(row.product_url) ||
        !cleanText(row.image_url) ||
        cleanText(nextTitleCandidate) !== cleanText(row.title)
    }
  );
  const nextTitle = cleanText(hydrated.title);
  const nextProductUrl = cleanText(hydrated.productUrl);
  const nextImageUrl = cleanText(hydrated.imageUrl);
  const nextKeepaPayloadJson = toJson(hydrated.keepaPayload || null);
  const shouldPersist =
    nextTitle !== cleanText(row.title) ||
    nextProductUrl !== cleanText(row.product_url) ||
    nextImageUrl !== cleanText(row.image_url) ||
    nextKeepaPayloadJson !== (row.keepa_payload_json || null);

  if (!shouldPersist) {
    return row;
  }

  db.prepare(
    `
      UPDATE keepa_results
      SET title = @title,
          product_url = @productUrl,
          image_url = @imageUrl,
          keepa_payload_json = @keepaPayloadJson
      WHERE id = @id
    `
  ).run({
    id: row.id,
    title: nextTitle || row.title,
    productUrl: nextProductUrl || row.product_url,
    imageUrl: nextImageUrl || row.image_url,
    keepaPayloadJson: nextKeepaPayloadJson
  });

  logGeneratorDebug('DEAL STORED WITH IMAGE AND LINK', {
    keepaResultId: row.id,
    asin: row.asin,
    hasTitle: Boolean(nextTitle),
    hasImageUrl: Boolean(nextImageUrl),
    hasProductUrl: Boolean(nextProductUrl),
    hasAffiliateUrl: Boolean(cleanText(hydrated.affiliateUrl))
  });

  return db.prepare(`SELECT * FROM keepa_results WHERE id = ?`).get(row.id);
}

function buildResultDto(row) {
  if (!row) {
    return null;
  }

  const repairedRow = repairStoredKeepaResultRow(row);
  const keepaPayload = fromJson(repairedRow.keepa_payload_json, null);
  const title = resolveKeepaDealTitle(
    {
      asin: repairedRow.asin,
      title: repairedRow.title,
      keepaPayload
    },
    {
      log: false
    }
  ).title;
  const productUrl = resolveKeepaDealProductUrl(
    {
      asin: repairedRow.asin,
      domain_id: repairedRow.domain_id,
      product_url: repairedRow.product_url,
      keepaPayload
    },
    { log: false }
  ).productUrl;
  const imageUrl = resolveKeepaDealImageUrl(
    {
      asin: repairedRow.asin,
      image_url: repairedRow.image_url,
      keepaPayload
    },
    { log: false }
  ).imageUrl;
  const history = keepaPayload?.history || buildKeepaHistorySnapshot({ ...repairedRow, keepaPayload });
  const fakeDrop = getFakeDropSnapshotForResult(repairedRow.id);
  const chartPoints = Array.isArray(fakeDrop?.chartPoints) && fakeDrop.chartPoints.length ? fakeDrop.chartPoints : history?.chartPoints || [];
  const priceSeries = history?.priceSeries || [];
  const relevantPricePoints = history?.relevantPoints || [];
  const currentPrice = parseNumber(repairedRow.current_price, null);
  const referencePrice = parseNumber(repairedRow.reference_price, null);
  const savingsAmount =
    currentPrice !== null && referencePrice !== null && referencePrice >= currentPrice
      ? Math.round((referencePrice - currentPrice) * 100) / 100
      : null;
  const normalizedOrigin = normalizeKeepaResultOrigin(repairedRow.origin);
  const similarCaseSignals =
    fakeDrop && repairedRow.seller_type
      ? getSimilarCaseSignals(
          {
            reviewItemId: fakeDrop.reviewItemId || 0,
            keepaResultId: repairedRow.id,
            asin: repairedRow.asin,
            sellerType: repairedRow.seller_type,
            categoryName: repairedRow.category_name,
            sourceType: normalizedOrigin,
            currentPrice,
            keepaDiscount: parseNumber(repairedRow.keepa_discount, null),
            fakeDropRisk: fakeDrop.fakeDropRisk ?? null,
            classification: fakeDrop.classification || '',
            features: fakeDrop.features || {}
          },
          {
            limit: 3,
            minSimilarityScore: 58,
            scanLimit: 40
          }
        )
      : {
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
  const learningAdjustedScore = clamp(
    (parseNumber(repairedRow.deal_score, 0) || 0) + Number(similarCaseSignals.summary?.scoreAdjustment || 0),
    0,
    100
  );
  const learningDecisionHint =
    Number(similarCaseSignals.summary?.total || 0) > 0
      ? `${similarCaseSignals.summary.total} aehnliche Faelle: Good ${similarCaseSignals.summary.positiveCount || 0}, kritisch ${
          similarCaseSignals.summary.negativeCount || 0
        }, Review ${similarCaseSignals.summary.uncertainCount || 0}.`
      : 'Noch keine aehnlichen Lernfaelle.';
  const affiliateUrl = resolveKeepaDealAffiliateUrl(
    {
      asin: repairedRow.asin,
      domain_id: repairedRow.domain_id,
      product_url: productUrl,
      keepaPayload
    },
    productUrl
  );

  return {
    id: repairedRow.id,
    asin: repairedRow.asin,
    domainId: repairedRow.domain_id,
    title,
    productUrl,
    affiliateUrl,
    imageUrl,
    currentPrice,
    referencePrice,
    referenceLabel: repairedRow.reference_label,
    keepaDiscount: parseNumber(repairedRow.keepa_discount, null),
    savingsAmount,
    sellerType: repairedRow.seller_type,
    drawerKey: inferDrawerKeyFromSellerType(repairedRow.seller_type),
    categoryId: parseInteger(repairedRow.category_id, null),
    categoryName: repairedRow.category_name,
    rating: normalizeRating(repairedRow.rating),
    reviewCount: parseInteger(repairedRow.review_count, null),
    isPrime: repairedRow.is_prime === 1,
    isInStock: repairedRow.is_in_stock === 1,
    dealScore: parseNumber(repairedRow.deal_score, 0),
    dealStrength: normalizeDealStrength(repairedRow.deal_strength),
    strengthReason: repairedRow.strength_reason,
    workflowStatus: normalizeWorkflowStatus(repairedRow.workflow_status),
    comparisonSource: repairedRow.comparison_source,
    comparisonStatus: repairedRow.comparison_status,
    comparisonPrice: parseNumber(repairedRow.comparison_price, null),
    priceDifferenceAbs: parseNumber(repairedRow.price_difference_abs, null),
    priceDifferencePct: parseNumber(repairedRow.price_difference_pct, null),
    comparisonCheckedAt: repairedRow.comparison_checked_at,
    comparisonPayload: fromJson(repairedRow.comparison_payload_json, null),
    keepaPayload,
    priceHistory: priceSeries,
    chartPoints,
    relevantPricePoints,
    learningAdjustedScore,
    learningDecisionHint,
    similarCaseSummary: similarCaseSignals.summary,
    similarCases: similarCaseSignals.cases,
    searchPayload: fromJson(repairedRow.search_payload_json, null),
    origin: normalizedOrigin,
    sourceLabel: getKeepaResultSourceLabel(normalizedOrigin),
    ruleId: repairedRow.rule_id,
    note: repairedRow.note || '',
    alertCount: parseInteger(repairedRow.alert_count, 0),
    lastAlertedAt: repairedRow.last_alerted_at,
    firstSeenAt: repairedRow.first_seen_at,
    lastSeenAt: repairedRow.last_seen_at,
    lastSyncedAt: repairedRow.last_synced_at,
    createdAt: repairedRow.created_at,
    updatedAt: repairedRow.updated_at,
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
  ensureKeepaApiKeyLog(key);

  const usageModule = normalizeUsageModule(
    options.module || options.source || (path === '/token' ? 'status-check' : 'manual-search'),
    path === '/token' ? 'status-check' : 'manual-search'
  );
  const usageMode = resolveKeepaUsageMode(options, 'keepa-request', usageModule);
  const usageDrawerKey = deriveUsageDrawerKey({
    drawerKey: options.drawerKey,
    filters: options.filters,
    meta: options.meta
  });

  return queueKeepaTask(async () => {
    let lastError;
    let lastDurationMs = 0;
    let lastStartedAtIso = nowIso();
    let lastTokensBefore = parseInteger(keepaConnectionCache?.tokensLeft, null);

    for (let attempt = 1; attempt <= keepaConfig.retryLimit; attempt += 1) {
      const controller = new AbortController();
      const timeoutHandle = setTimeout(() => controller.abort(), keepaConfig.timeoutMs);
      const startedAt = Date.now();
      const startedAtIso = nowIso();
      const previousTokensLeft = parseInteger(keepaConnectionCache?.tokensLeft, null);
      lastStartedAtIso = startedAtIso;
      lastTokensBefore = previousTokensLeft;

      try {
        logGeneratorDebug('KEEPA REQUEST START', {
          endpoint: path,
          source: options.source || null,
          module: usageModule,
          mode: usageMode,
          drawerKey: usageDrawerKey || null,
          attempt
        });
        logGeneratorDebug('KEEPA TOKENS BEFORE', {
          endpoint: path,
          module: usageModule,
          mode: usageMode,
          drawerKey: usageDrawerKey || null,
          tokensBefore: previousTokensLeft
        });
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
        const officialUsageValue =
          currentTokensLeft !== null && previousTokensLeft !== null && previousTokensLeft >= currentTokensLeft
            ? previousTokensLeft - currentTokensLeft
            : null;
        const tokensBefore = previousTokensLeft !== null
          ? previousTokensLeft
          : officialUsageValue !== null && currentTokensLeft !== null
            ? currentTokensLeft + officialUsageValue
            : null;
        const resultCount = Array.isArray(data?.products) ? data.products.length : Array.isArray(data?.dr) ? data.dr.length : 0;
        const usage = recordKeepaUsage({
          action: 'keepa-request',
          module: usageModule,
          mode: usageMode,
          drawerKey: usageDrawerKey,
          timestampStart: startedAtIso,
          timestampEnd: nowIso(),
          tokensBefore,
          tokensAfter: currentTokensLeft,
          filters: options.filters,
          resultCount,
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
            refillRate: parseInteger(data?.refillRate, null),
            sellerType: options.filters?.sellerType || null
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

        logGeneratorDebug('KEEPA RESPONSE RECEIVED', {
          endpoint: path,
          source: options.source || null,
          module: usageModule,
          attempt,
          statusCode: response.status,
          resultCount,
          tokensLeft: currentTokensLeft
        });
        logGeneratorDebug('KEEPA TOKENS AFTER', {
          endpoint: path,
          module: usageModule,
          mode: usageMode,
          drawerKey: usageDrawerKey || null,
          tokensAfter: currentTokensLeft
        });
        logGeneratorDebug('KEEPA TOKENS USED', {
          endpoint: path,
          module: usageModule,
          mode: usageMode,
          drawerKey: usageDrawerKey || null,
          tokensUsed: usage.tokensUsed
        });

        const recent60s = getKeepaRequestWindowMetrics(KEEPA_REQUEST_WINDOW_60S_MS);
        const recent5m = getKeepaRequestWindowMetrics(KEEPA_REQUEST_WINDOW_5M_MS);
        if (
          usage.tokensUsed >= KEEPA_EXPENSIVE_REQUEST_TOKENS ||
          recent60s.tokensUsed >= KEEPA_MAX_TOKENS_PER_60S ||
          recent60s.requestCount >= KEEPA_MAX_REQUESTS_PER_60S
        ) {
          logGeneratorDebug('KEEPA HIGH USAGE DETECTED', {
            endpoint: path,
            module: usageModule,
            mode: usageMode,
            drawerKey: usageDrawerKey || null,
            tokensUsed: usage.tokensUsed,
            recent60s,
            recent5m
          });
        }

        logKeepaEvent('info', 'keepa_api_request', options.source || path, `Keepa Request ${path} erfolgreich.`, {
          filters: options.filters,
          resultCount,
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
      mode: usageMode,
      drawerKey: usageDrawerKey,
      timestampStart: lastStartedAtIso,
      timestampEnd: nowIso(),
      tokensBefore: lastTokensBefore,
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
    logGeneratorDebug('KEEPA ERROR', {
      endpoint: path,
      source: options.source || null,
      module: usageModule,
      message: buildKeepaRequestError(lastError, 'Unbekannter Keepa-Fehler'),
      retryLimit: keepaConfig.retryLimit
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

export async function runKeepaApiTest(input = {}) {
  const settings = getKeepaSettings();
  const keepaConfig = getKeepaConfig();
  const asin = cleanText(input.asin).toUpperCase() || DEFAULT_KEEPA_TEST_ASIN;
  const domainId =
    DOMAIN_OPTIONS.some((item) => item.id === Number(input.domainId || settings.domainId))
      ? Number(input.domainId || settings.domainId)
      : settings.domainId;

  logGeneratorDebug('KEEPA TEST START', {
    asin,
    domainId,
    configured: Boolean(keepaConfig.key)
  });

  if (!keepaConfig.key) {
    logGeneratorDebug('KEEPA ERROR', {
      asin,
      domainId,
      message: 'Kein Keepa API Key gesetzt.',
      code: 'KEEPA_API_KEY_MISSING'
    });
    throw createKeepaApiTestError('Kein Keepa API Key gesetzt.', 'KEEPA_API_KEY_MISSING', 500);
  }

  try {
    const response = await keepaRequest(
      '/product',
      {
        domain: domainId,
        asin,
        history: 1,
        offers: 20,
        rating: 1,
        stock: 1,
        update: 0,
        stats: 90
      },
      {
        source: 'api_test',
        module: 'test-connection',
        filters: {
          asin,
          domainId
        }
      }
    );

    const product = Array.isArray(response.data?.products) ? response.data.products[0] : null;
    if (!product) {
      throw createKeepaApiTestError(
        'Keepa API hat kein Testprodukt fuer diese ASIN geliefert.',
        'KEEPA_TEST_EMPTY_RESPONSE',
        502
      );
    }

    const normalized = normalizeProductContextRecord(
      product,
      {
        asin
      },
      { domainId }
    );

    logGeneratorDebug('KEEPA RESPONSE RECEIVED', {
      asin,
      domainId,
      productCount: Array.isArray(response.data?.products) ? response.data.products.length : 0,
      tokensLeft: response.data?.tokensLeft ?? null
    });

    return {
      ok: true,
      testedAt: nowIso(),
      asin,
      domainId,
      connection: {
        connected: true,
        checkedAt: nowIso(),
        tokensLeft: response.data?.tokensLeft ?? null
      },
      product: normalized
        ? {
            status: 'loaded',
            cached: false,
            title: normalized.title || asin,
            sellerType: normalized.sellerType || 'FBM',
            currentPrice: normalized.currentPrice ?? null,
            referencePrice: normalized.referencePrice ?? null,
            keepaDiscount: normalized.keepaDiscount ?? null,
            dealScore: normalized.dealScore ?? null,
            categoryName: normalized.categoryName || '',
            checkedAt: nowIso()
          }
        : {
            status: 'loaded',
            cached: false,
            title: cleanText(product.title) || asin,
            sellerType: 'FBM',
            currentPrice: null,
            referencePrice: null,
            keepaDiscount: null,
            dealScore: null,
            categoryName: '',
            checkedAt: nowIso()
          },
      raw: {
        productCount: Array.isArray(response.data?.products) ? response.data.products.length : 0,
        hasTokensLeft: response.data?.tokensLeft !== undefined
      }
    };
  } catch (error) {
    const message = buildKeepaRequestError(error, 'Keepa-Test konnte nicht ausgefuehrt werden.');
    const statusCode =
      Number(error?.statusCode) > 0
        ? Number(error.statusCode)
        : error?.name === 'AbortError'
          ? 504
          : 502;
    const code =
      error instanceof Error && error.code ? error.code : statusCode === 504 ? 'KEEPA_TEST_TIMEOUT' : 'KEEPA_API_TEST_FAILED';

    logGeneratorDebug('KEEPA ERROR', {
      asin,
      domainId,
      message,
      code
    });

    throw createKeepaApiTestError(message, code, statusCode);
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
    drawerConfigs:
      input.drawerConfigs === undefined
        ? current.drawerConfigs
        : normalizeDrawerConfigs(input.drawerConfigs, current.drawerConfigs),
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

  for (const drawerKey of Object.keys(next.drawerConfigs)) {
    const drawerConfig = next.drawerConfigs[drawerKey];
    if (
      drawerConfig.minPrice !== null &&
      drawerConfig.maxPrice !== null &&
      Number(drawerConfig.minPrice) > Number(drawerConfig.maxPrice)
    ) {
      throw new Error(`Die Preisgrenzen in der Schublade ${drawerKey} sind ungueltig.`);
    }
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
          drawer_configs_json = @drawerConfigsJson,
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
    drawerConfigsJson: toJson(next.drawerConfigs),
    loggingEnabled: next.loggingEnabled ? 1 : 0,
    estimatedTokensPerManualRun: next.estimatedTokensPerManualRun,
    updatedAt: nowIso()
  });

  logKeepaEvent('info', 'settings_saved', 'settings', 'Keepa-Einstellungen aktualisiert.', {
    payload: {
      keepaEnabled: next.keepaEnabled,
      schedulerEnabled: next.schedulerEnabled,
      domainId: next.domainId,
      drawerConfigs: next.drawerConfigs
    }
  });
  logGeneratorDebug('FLOW STATUS UPDATED', {
    keepaEnabled: next.keepaEnabled,
    schedulerEnabled: next.schedulerEnabled,
    alertTelegramEnabled: next.alertTelegramEnabled
  });
  logGeneratorDebug('SOURCE STATUS UPDATED', {
    keepaEnabled: next.keepaEnabled,
    amazonPrepared: true,
    loggingEnabled: next.loggingEnabled,
    schedulerEnabled: next.schedulerEnabled
  });
  Object.entries(next.drawerConfigs || {}).forEach(([drawerKey, drawerConfig]) => {
    logGeneratorDebug(`PATTERN SUPPORT ACTIVE: ${drawerKey}`, {
      active: drawerConfig.active === true,
      patternSupportEnabled: drawerConfig.patternSupportEnabled === true,
      autoModeAllowed: drawerConfig.autoModeAllowed === true
    });
    logGeneratorDebug(`AUTO POST ACTIVE: ${drawerKey}`, {
      active: drawerConfig.active === true,
      autoPostingEnabled: drawerConfig.testGroupPostingAllowed === true,
      globalTelegramEnabled: next.alertTelegramEnabled === true
    });
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
  const drawerKey = resolveManualDrawerSelection(input, settings);
  const drawerConfig = getDrawerConfig(settings, drawerKey);
  const page = clamp(parseInteger(input.page, 1), 1, MAX_MANUAL_PAGE);
  const limit = clamp(
    parseInteger(input.limit, Math.min(settings.defaultPageSize || KEEPA_MANUAL_RESULT_LIMIT_CAP, KEEPA_MANUAL_RESULT_LIMIT_CAP)),
    1,
    MAX_MANUAL_PAGE_SIZE
  );
  const minPrice = sanitizePriceBoundary(input.minPrice ?? drawerConfig.minPrice ?? settings.defaultMinPrice);
  const maxPrice = sanitizePriceBoundary(input.maxPrice ?? drawerConfig.maxPrice ?? settings.defaultMaxPrice);

  if (minPrice !== null && maxPrice !== null && minPrice > maxPrice) {
    throw new Error('Der Mindestpreis darf nicht groesser als der Hoechstpreis sein.');
  }

  logGeneratorDebug('MANUAL KEEPA DRAWER SELECTED', {
    drawerKey,
    requestedSellerType: normalizeSellerType(input.sellerType || drawerKey),
    limit,
    page
  });

  return {
    page,
    limit,
    drawerKey,
    domainId:
      DOMAIN_OPTIONS.some((item) => item.id === Number(input.domainId || settings.domainId))
        ? Number(input.domainId || settings.domainId)
        : settings.domainId,
    minDiscount: clamp(parseNumber(input.minDiscount, drawerConfig.minDiscount ?? settings.defaultDiscount), 0, 95),
    sellerType: drawerKey,
    categories: normalizeCategoryIds(input.categories, drawerConfig.categories ?? settings.defaultCategories),
    minPrice,
    maxPrice,
    trendInterval: normalizeTrendInterval(input.trendInterval ?? drawerConfig.trendInterval),
    sortBy: normalizeSortBy(input.sortBy ?? drawerConfig.sortBy),
    onlyPrime: parseBool(input.onlyPrime, drawerConfig.onlyPrime),
    onlyInStock: parseBool(input.onlyInStock, drawerConfig.onlyInStock),
    onlyGoodRating: parseBool(input.onlyGoodRating, drawerConfig.onlyGoodRating),
    onlyWithReviews: parseBool(input.onlyWithReviews, drawerConfig.onlyWithReviews),
    amazonOfferMode: normalizeAmazonOfferMode(input.amazonOfferMode ?? drawerConfig.amazonOfferMode),
    singleVariantOnly: parseBool(input.singleVariantOnly, drawerConfig.singleVariantOnly),
    recentPriceChangeOnly: parseBool(input.recentPriceChangeOnly, drawerConfig.recentPriceChangeOnly)
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

  return enrichKeepaRecord({
    asin,
    domainId: filters.domainId,
    title,
    productUrl: buildAmazonProductUrl(asin, filters.domainId),
    imageUrl: buildAmazonImageUrl(deal?.image || product?.imagesCSV, asin),
    currentPrice,
    referencePrice,
    referenceLabel: 'Referenzpreis / Verlauf',
    keepaDiscount: keepaDiscount !== null ? keepaDiscount : 0,
    sellerType: sellerTypeFromDeal || 'UNKNOWN',
    salesRank: extractSalesRank(product, deal),
    categoryId: category.categoryId,
    categoryName: category.categoryName,
    rating,
    reviewCount,
    isPrime: Boolean(deal?.isPrimeEligible) || offerSummary.isPrime,
    isInStock: offerSummary.inStock,
    hasAmazonOffer: hasAmazonOffer(product, sellerTypeFromDeal),
    hasMultipleVariations: hasMultipleVariations(product),
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
        title: cleanText(product?.title) || title,
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
              title: cleanText(product?.title) || title,
              stats: product.stats || null,
              csv: Array.isArray(product.csv) ? product.csv : null,
              offerCSV: product.offerCSV || null,
              buyBoxSellerIdHistory: product.buyBoxSellerIdHistory || null
            }
          : null
      }
    }
  });
}

function extractAverageStatPrice(stats, keys = []) {
  for (const key of keys) {
    const value = stats?.[key];
    if (Array.isArray(value)) {
      const parsed = value.map((entry) => fromMinorUnits(parseNumber(entry, null))).find((entry) => entry !== null);
      if (parsed !== null) {
        return parsed;
      }
    }

    const parsed = fromMinorUnits(parseNumber(value, null));
    if (parsed !== null) {
      return parsed;
    }
  }

  return null;
}

function normalizeProductContextRecord(product, contextInput = {}, filters = {}, existingRow = null) {
  const asin = cleanText(contextInput.asin).toUpperCase() || extractAsin(product);
  if (!asin) {
    return null;
  }

  const offerSummary = getOfferSummary(product);
  const stats = product?.stats || {};
  const currentPrice =
    parseNumber(contextInput.currentPrice, null) ??
    offerSummary.price ??
    extractAverageStatPrice(stats, ['current', 'currentPrice', 'buyBoxPrice', 'min']);
  const referencePrice = buildReferencePrice(currentPrice, null, {}, product) ?? extractAverageStatPrice(stats, ['avg90', 'avg30', 'avg']);
  const keepaDiscount =
    currentPrice !== null && referencePrice !== null && referencePrice > 0 && referencePrice >= currentPrice
      ? Math.round((((referencePrice - currentPrice) / referencePrice) * 100) * 10) / 10
      : 0;
  const category = normalizeCategory(product, {});
  const title = cleanText(contextInput.title || product?.title) || asin;
  const normalizedSellerType = normalizeSellerType(contextInput.sellerType || offerSummary.sellerType || existingRow?.seller_type || 'FBM');
  const sellerType = normalizedSellerType === 'ALL' ? 'FBM' : normalizedSellerType;

  return enrichKeepaRecord({
    asin,
    domainId: filters.domainId,
    title,
    productUrl: cleanText(contextInput.productUrl) || buildAmazonProductUrl(asin, filters.domainId),
    imageUrl: cleanText(contextInput.imageUrl) || buildAmazonImageUrl(product?.imagesCSV, asin),
    currentPrice,
    referencePrice,
    referenceLabel: 'Keepa Verlauf / avg90',
    keepaDiscount,
    sellerType,
    salesRank: extractSalesRank(product, {}),
    categoryId: category.categoryId,
    categoryName: category.categoryName,
    rating: normalizeRating(product?.currentRating || stats?.currentRating),
    reviewCount: parseInteger(product?.currentRatingCount || stats?.reviewCount, null),
    isPrime: offerSummary.isPrime,
    isInStock: offerSummary.inStock,
    hasAmazonOffer: hasAmazonOffer(product, sellerType),
    hasMultipleVariations: hasMultipleVariations(product),
    comparisonSource: cleanText(existingRow?.comparison_source),
    comparisonStatus: cleanText(existingRow?.comparison_status) || 'not_connected',
    comparisonPrice: parseNumber(existingRow?.comparison_price, null),
    comparisonPayload: fromJson(existingRow?.comparison_payload_json, null),
    keepaPayload: {
      product: {
        title,
        imagesCSV: product?.imagesCSV || '',
        rootCategory: product?.rootCategory || category.categoryId,
        productGroup: product?.productGroup || '',
        stats,
        csv: Array.isArray(product?.csv) ? product.csv : null,
        offerCSV: product?.offerCSV || null,
        buyBoxSellerIdHistory: product?.buyBoxSellerIdHistory || null,
        offers: Array.isArray(product?.offers) ? product.offers : [],
        liveOffersOrder: Array.isArray(product?.liveOffersOrder) ? product.liveOffersOrder : []
      },
      raw: {
        product: {
          ...product,
          stats,
          csv: Array.isArray(product?.csv) ? product.csv : null,
          offerCSV: product?.offerCSV || null,
          buyBoxSellerIdHistory: product?.buyBoxSellerIdHistory || null
        }
      }
    }
  });
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
  const mergedItem = enrichKeepaRecord(mergeExistingComparison(item, existing), {
    log: true
  });
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
    origin: normalizeKeepaResultOrigin(meta.origin),
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

  if (normalizeKeepaResultOrigin(meta.origin) === 'keepa-manual') {
    logGeneratorDebug('MANUAL DEAL STORED', {
      keepaResultId: storedResult?.id || null,
      asin: mergedItem.asin,
      drawerKey: inferDrawerKeyFromSellerType(mergedItem.sellerType),
      affiliateUrlAvailable: Boolean(storedResult?.affiliateUrl),
      chartPointCount: Array.isArray(storedResult?.chartPoints) ? storedResult.chartPoints.length : 0
    });
  }

  logGeneratorDebug('DEAL STORED WITH IMAGE AND LINK', {
    keepaResultId: storedResult?.id || null,
    asin: mergedItem.asin,
    hasTitle: Boolean(storedResult?.title),
    hasImageUrl: Boolean(storedResult?.imageUrl),
    hasProductUrl: Boolean(storedResult?.productUrl),
    hasAffiliateUrl: Boolean(storedResult?.affiliateUrl)
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
  const drawerKey = normalizeDrawerKey(filters.drawerKey || inferDrawerKeyFromSellerType(filters.sellerType));
  const rawPageSize =
    meta.origin === 'manual'
      ? clamp(parseInteger(filters.limit, KEEPA_MANUAL_RESULT_LIMIT_CAP), 1, KEEPA_MANUAL_RESULT_LIMIT_CAP)
      : Math.min(Math.max(filters.limit * 2, filters.limit), 60);
  const { selection, diagnostics } = buildKeepaManualSelection(filters, settings, rawPageSize);

  logGeneratorDebug(`KEEPA AUTO MODE: ${meta.origin === 'automatic'}`, {
    drawerKey,
    origin: meta.origin || 'manual',
    ruleId: meta.rule?.id || null
  });
  logGeneratorDebug(`KEEPA DRAWER ACTIVE: ${drawerKey}`, {
    drawerKey,
    sellerType: filters.sellerType,
    origin: meta.origin || 'manual'
  });
  logGeneratorDebug('KEEPA FILTERS APPLIED', {
    drawerKey,
    sellerType: filters.sellerType,
    trendInterval: filters.trendInterval,
    sortBy: filters.sortBy,
    minDiscount: filters.minDiscount,
    minPrice: filters.minPrice,
    maxPrice: filters.maxPrice,
    categories: filters.categories,
    onlyPrime: filters.onlyPrime,
    onlyInStock: filters.onlyInStock,
    onlyGoodRating: filters.onlyGoodRating,
    onlyWithReviews: filters.onlyWithReviews,
    amazonOfferMode: filters.amazonOfferMode,
    singleVariantOnly: filters.singleVariantOnly,
    recentPriceChangeOnly: filters.recentPriceChangeOnly
  });
  logGeneratorDebug('KEEPA MANUAL FILTER RECEIVED', {
    origin: meta.origin || 'manual',
    drawerKey,
    filters: sanitizeUsageFilters(filters)
  });
  logGeneratorDebug('KEEPA QUERY SANITIZED', {
    drawerKey,
    origin: meta.origin || 'manual',
    localOnlyFields: diagnostics.localOnlyFields,
    mappedFields: diagnostics.mappedFields,
    selection
  });

  try {
    if (!settings.keepaEnabled) {
      throw new Error('Keepa ist in den Einstellungen deaktiviert.');
    }
    if (!getKeepaConfig().key) {
      throw new Error('KEEPA_API_KEY fehlt im Backend.');
    }

    const keepaRequestUsage = [];
    logGeneratorDebug('KEEPA QUERY SENT', {
      drawerKey,
      origin: meta.origin || 'manual',
      selection
    });
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
          drawerKey,
          sellerType: filters.sellerType,
          trendInterval: filters.trendInterval,
          sortBy: filters.sortBy,
          categories: filters.categories,
          minDiscount: filters.minDiscount,
          minPrice: filters.minPrice,
          maxPrice: filters.maxPrice,
          onlyPrime: filters.onlyPrime,
          onlyInStock: filters.onlyInStock,
          onlyGoodRating: filters.onlyGoodRating,
          onlyWithReviews: filters.onlyWithReviews,
          amazonOfferMode: filters.amazonOfferMode,
          singleVariantOnly: filters.singleVariantOnly,
          recentPriceChangeOnly: filters.recentPriceChangeOnly
        }
      }
    );
    keepaRequestUsage.push(dealResponse.usage);

    const rawDeals = extractKeepaDealRows(dealResponse.data);
    logGeneratorDebug('KEEPA QUERY SUCCESS', {
      drawerKey,
      origin: meta.origin || 'manual',
      rawResultCount: rawDeals.length
    });
    if (meta.origin === 'manual') {
      logGeneratorDebug('MANUAL KEEPA RESULTS RECEIVED', {
        drawerKey,
        rawResultCount: rawDeals.length
      });
    }
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

      normalizedItems.push({
        ...(await compareAgainstLegalSources(normalized)),
        sourceOrder: normalizedItems.length
      });
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

    const filteredItems = sortSearchItems(applyManualFilters(normalizedItems, filters, settings), filters).slice(0, filters.limit);
    logGeneratorDebug('KEEPA RESULT COUNT', {
      drawerKey,
      origin: meta.origin || 'manual',
      rawResultCount: rawDeals.length,
      normalizedCount: normalizedItems.length,
      filteredCount: filteredItems.length
    });
    const savedItems = filteredItems.map((item) =>
      saveKeepaResult(item, {
        origin: meta.origin || 'manual',
        rule: meta.rule || null,
        searchPayload: filters
      })
    );
    if (meta.origin === 'manual') {
      logGeneratorDebug('MANUAL DEAL PREVIEW BUILT', {
        drawerKey,
        previewCount: savedItems.length,
        chartReadyCount: savedItems.filter((item) => Array.isArray(item?.chartPoints) && item.chartPoints.length > 0).length
      });
    }
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
        origin: meta.origin || 'manual',
        drawerKey,
        trendInterval: filters.trendInterval,
        sortBy: filters.sortBy
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
    if (buildKeepaRequestError(error, '').includes('queryJSON')) {
      logGeneratorDebug('KEEPA QUERY REJECTED', {
        drawerKey,
        origin: meta.origin || 'manual',
        selection,
        message: buildKeepaRequestError(error, 'Keepa Deal-Query wurde abgelehnt.')
      });
    }
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

export async function loadKeepaProductContext(input = {}) {
  const asin = cleanText(input.asin).toUpperCase();
  const settings = getKeepaSettings();
  const requestedAt = nowIso();
  const domainId =
    DOMAIN_OPTIONS.some((item) => item.id === Number(input.domainId || settings.domainId))
      ? Number(input.domainId || settings.domainId)
      : settings.domainId;
  const existingRow = asin
    ? db.prepare(`SELECT * FROM keepa_results WHERE asin = ? AND domain_id = ? LIMIT 1`).get(asin, domainId)
    : null;
  const existingResult = existingRow ? buildResultDto(existingRow) : null;
  const maxAgeMinutes = clamp(parseInteger(input.maxAgeMinutes, 180), 15, 1440);
  const existingAgeMs = existingResult?.lastSyncedAt ? Date.now() - new Date(existingResult.lastSyncedAt).getTime() : Number.POSITIVE_INFINITY;

  if (!asin) {
    return {
      available: false,
      status: 'missing_asin',
      cached: false,
      requestedAt,
      reason: 'ASIN fehlt fuer die Keepa-Pruefung.'
    };
  }

  if (existingResult && Number.isFinite(existingAgeMs) && existingAgeMs <= maxAgeMinutes * 60 * 1000) {
    return {
      available: true,
      status: 'cached',
      cached: true,
      requestedAt,
      result: existingResult
    };
  }

  if (!settings.keepaEnabled) {
    return {
      available: Boolean(existingResult),
      status: existingResult ? 'cached_disabled' : 'disabled',
      cached: Boolean(existingResult),
      requestedAt,
      result: existingResult,
      reason: 'Keepa ist in den Einstellungen deaktiviert.'
    };
  }

  if (!getKeepaConfig().key) {
    return {
      available: Boolean(existingResult),
      status: existingResult ? 'cached_missing_key' : 'missing_key',
      cached: Boolean(existingResult),
      requestedAt,
      result: existingResult,
      reason: 'KEEPA_API_KEY fehlt im Backend.'
    };
  }

  try {
    const response = await keepaRequest(
      '/product',
      {
        domain: domainId,
        asin,
        history: 1,
        offers: 20,
        rating: 1,
        stock: 1,
        update: 0,
        stats: 90
      },
      {
        source: 'generator_context_product',
        module: 'background-check',
        filters: {
          asin,
          domainId,
          source: cleanText(input.source || 'generator')
        }
      }
    );

    const product = Array.isArray(response.data?.products) ? response.data.products[0] : null;
    if (!product) {
      return {
        available: Boolean(existingResult),
        status: existingResult ? 'cached_not_found' : 'not_found',
        cached: Boolean(existingResult),
        requestedAt,
        result: existingResult,
        reason: 'Keepa hat kein Produkt zu dieser ASIN geliefert.'
      };
    }

    const normalized = normalizeProductContextRecord(
      product,
      {
        asin,
        sellerType: input.sellerType,
        currentPrice: input.currentPrice,
        title: input.title,
        productUrl: input.productUrl,
        imageUrl: input.imageUrl
      },
      { domainId },
      existingRow
    );

    if (!normalized) {
      return {
        available: Boolean(existingResult),
        status: existingResult ? 'cached_invalid' : 'invalid',
        cached: Boolean(existingResult),
        requestedAt,
        result: existingResult,
        reason: 'Keepa-Produkt konnte nicht normalisiert werden.'
      };
    }

    const storedResult = saveKeepaResult(normalized, {
      origin: cleanText(input.origin || input.source || 'generator') || 'generator',
      searchPayload: {
        source: cleanText(input.source || 'generator') || 'generator',
        mode: 'product_context',
        asin,
        domainId
      }
    });

    logGeneratorDebug('api.keepa.generator_context.success', {
      asin,
      domainId,
      keepaResultId: storedResult?.id || null,
      sellerType: storedResult?.sellerType || null,
      keepaDiscount: storedResult?.keepaDiscount ?? null,
      dealScore: storedResult?.dealScore ?? null
    });

    return {
      available: true,
      status: existingResult ? 'refreshed' : 'loaded',
      cached: false,
      requestedAt,
      result: storedResult
    };
  } catch (error) {
    logGeneratorDebug('api.keepa.generator_context.error', {
      asin,
      domainId,
      error: error instanceof Error ? error.message : 'Keepa-Produktkontext fehlgeschlagen'
    });

    return {
      available: Boolean(existingResult),
      status: existingResult ? 'stale' : 'error',
      cached: Boolean(existingResult),
      requestedAt,
      result: existingResult,
      reason: error instanceof Error ? error.message : 'Keepa-Produktkontext fehlgeschlagen.'
    };
  }
}

export function loadStoredInternetComparisonContext(input = {}) {
  const asin = cleanText(input.asin).toUpperCase();
  const settings = getKeepaSettings();
  const requestedAt = nowIso();
  const domainId =
    DOMAIN_OPTIONS.some((item) => item.id === Number(input.domainId || settings.domainId))
      ? Number(input.domainId || settings.domainId)
      : settings.domainId;

  if (!asin) {
    return {
      available: false,
      status: 'missing_asin',
      cached: false,
      requestedAt,
      reason: 'ASIN fehlt fuer den Internetvergleich.'
    };
  }

  const existingRow = db.prepare(`SELECT * FROM keepa_results WHERE asin = ? AND domain_id = ? LIMIT 1`).get(asin, domainId);
  const existingResult = existingRow ? buildResultDto(existingRow) : null;
  const hasStoredMarketComparison = Boolean(
    existingResult &&
      ((existingResult.comparisonPrice !== null && existingResult.comparisonPrice > 0) ||
        existingResult.priceDifferencePct !== null ||
        cleanText(existingResult.comparisonSource))
  );

  if (!hasStoredMarketComparison) {
    return {
      available: false,
      status: existingResult ? 'missing_market_comparison' : 'missing',
      cached: Boolean(existingResult),
      requestedAt,
      result: existingResult,
      reason: existingResult ? 'Kein gespeicherter Marktvergleich gefunden.' : 'Noch kein Produktkontext gespeichert.'
    };
  }

  return {
    available: true,
    status: 'stored_market_comparison',
    cached: true,
    requestedAt,
    result: existingResult
  };
}

export async function runKeepaManualSearch(input = {}) {
  const filters = normalizeManualSearchInput(input);
  const confirmed = parseBool(input.confirmed, false);
  const confirmationToken = cleanText(input.confirmationToken);

  logGeneratorDebug('MANUAL KEEPA DRAWER SELECTED', {
    drawerKey: filters.drawerKey,
    sellerType: filters.sellerType,
    confirmed
  });

  if (!confirmed) {
    logGeneratorDebug('MANUAL KEEPA PREVIEW START', {
      drawerKey: filters.drawerKey,
      sellerType: filters.sellerType,
      limit: filters.limit,
      trendInterval: filters.trendInterval
    });
    const dryRun = await buildKeepaManualDryRun(filters, { confirmed: false });
    return {
      executed: false,
      dryRun,
      protection: dryRun.protection,
      items: [],
      pagination: {
        page: filters.page,
        limit: dryRun.effectiveFilters.limit,
        hasMore: false,
        rawResultCount: 0
      },
      usage: null
    };
  }

  const confirmedFilters = validateKeepaManualConfirmation(filters, confirmationToken);
  const dryRun = await buildKeepaManualDryRun(confirmedFilters, { confirmed: true });
  if (dryRun.blocked) {
    return {
      executed: false,
      dryRun,
      protection: dryRun.protection,
      items: [],
      pagination: {
        page: confirmedFilters.page,
        limit: dryRun.effectiveFilters.limit,
        hasMore: false,
        rawResultCount: 0
      },
      usage: null
    };
  }

  beginKeepaSearchExecution('manual', confirmedFilters.drawerKey);
  logGeneratorDebug('KEEPA MANUAL FETCH START', {
    drawerKey: confirmedFilters.drawerKey,
    sellerType: confirmedFilters.sellerType,
    trendInterval: confirmedFilters.trendInterval,
    minDiscount: confirmedFilters.minDiscount
  });
  logGeneratorDebug('KEEPA REQUEST START', {
    source: 'manual',
    drawerKey: confirmedFilters.drawerKey,
    estimatedTokenCost: dryRun.protection.estimatedTokenCost,
    tokensLeft: dryRun.protection.tokensLeft
  });

  try {
    const response = await executeSearch(confirmedFilters, {
      source: 'manual_search',
      origin: 'manual'
    });

    logGeneratorDebug('KEEPA REQUEST FINISHED', {
      source: 'manual',
      drawerKey: confirmedFilters.drawerKey,
      resultCount: response.items?.length || 0,
      rawResultCount: response.pagination?.rawResultCount || 0,
      estimatedUsage: response.usage?.estimatedUsage ?? null
    });
    logGeneratorDebug('KEEPA MANUAL FETCH DONE', {
      drawerKey: confirmedFilters.drawerKey,
      sellerType: confirmedFilters.sellerType,
      resultCount: response.items?.length || 0,
      rawResultCount: response.pagination?.rawResultCount || 0,
      estimatedUsage: response.usage?.estimatedUsage ?? null
    });

    return {
      executed: true,
      filters: confirmedFilters,
      protection: {
        ...dryRun.protection,
        cooldownActive: true,
        cooldownRemainingMs: KEEPA_MANUAL_COOLDOWN_MS,
        lastFinishedAt: nowIso()
      },
      ...response
    };
  } finally {
    finishKeepaSearchExecution('manual');
  }
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

async function maybeSendAlertsForResult(result, rule) {
  const settings = getKeepaSettings();
  const drawerConfig = getDrawerConfig(settings, inferDrawerKeyFromSellerType(result.sellerType));
  const amazonContext = await loadAmazonAffiliateContext({
    asin: result.asin
  });
  const enrichedResult =
    amazonContext?.available && amazonContext?.result
      ? {
          ...result,
          title: amazonContext.result.title || result.title,
          imageUrl: amazonContext.result.imageUrl || result.imageUrl,
          productUrl: amazonContext.result.affiliateUrl || amazonContext.result.detailPageUrl || result.productUrl,
          amazonAffiliate: amazonContext.result
        }
      : {
          ...result,
          amazonAffiliate: amazonContext?.result || null
        };
  const dealLockStatus = checkDealLockStatus({
    asin: result.asin,
    url: enrichedResult.productUrl || result.productUrl,
    normalizedUrl: enrichedResult.productUrl || result.productUrl,
    sourceType: 'auto_deals',
    origin: 'automatic'
  });

  const learningContext = evaluateLearningRoute({
    sourceType: 'auto_deals',
    enforceDecision: true,
    keepaRequired: true,
    asin: result.asin,
    sellerType: result.sellerType,
    currentPrice: result.currentPrice,
    internetContext: {
      available: Boolean(
        (result.comparisonPrice !== null && result.comparisonPrice !== undefined) ||
          result.priceDifferencePct !== null ||
          cleanText(result.comparisonSource)
      ),
      status: cleanText(result.comparisonStatus) || 'not_connected',
      requestedAt: nowIso(),
      result
    },
    keepaResultRecord: result,
    amazonContext,
    dealLockStatus,
    queueContext: {
      required: true,
      mode: 'publisher_queue',
      currentStatus: 'not_enqueued',
      splitByPlatform: true,
      preSendPersistence: true,
      recoveryEnabled: true,
      routeType: 'automatic',
      channels: ['telegram']
    },
    patternSupportEnabled: drawerConfig.patternSupportEnabled === true,
    marketMinGapPct: settings.strongDealMinComparisonGapPct
  });

  logGeneratorDebug('AUTO DEAL ROUTED THROUGH LEARNING LOGIC', {
    keepaResultId: result.id,
    asin: result.asin,
    sellerType: result.sellerType,
    routingDecision: learningContext?.learning?.routingDecision || 'review',
    keepaStatus: learningContext?.keepa?.status || 'missing',
    amazonStatus: learningContext?.amazon?.status || 'missing'
  });

  let decisionStatus = 'review';
  let decisionReason = 'Deal wurde noch nicht fuer die Testgruppe freigegeben.';

  if (result.dealStrength !== 'stark') {
    decisionReason = 'Deal-Staerke liegt unter der Auto-Output-Schwelle.';
  } else if (learningContext?.learning?.routingDecision !== 'test_group') {
    decisionStatus = learningContext?.learning?.routingDecision === 'block' ? 'blocked' : 'review';
    decisionReason = learningContext?.learning?.reason || 'Lern-Logik hat den Deal nicht freigegeben.';
  } else {
    const fakeDropGate = evaluateFakeDropAlertEligibility(result, rule);
    if (!fakeDropGate.allowed) {
      decisionStatus = fakeDropGate.reviewQueue ? 'review' : 'blocked';
      decisionReason = fakeDropGate.reason;
    } else {
      decisionStatus = 'approved_for_test_group';
      decisionReason = learningContext?.learning?.reason || 'Deal wurde fuer die Telegram-Testgruppe freigegeben.';
    }
  }

  const output = await publishAutoDealToTelegramTestGroup({
    result: enrichedResult,
    rule,
    settings,
    sourceType: cleanText(result.origin).includes('amazon') ? 'amazon' : 'keepa',
    decisionStatus,
    decisionReason,
    learningContext
  });

  return [output];
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
  const tokensUsed = resolveKeepaTokensUsedValue({
    tokensUsed: row.tokens_used,
    tokensBefore: row.tokens_before,
    tokensAfter: row.tokens_after,
    officialTokensLeft: row.official_tokens_left,
    officialUsageValue: row.official_usage_value,
    estimatedUsage: row.estimated_usage
  });
  const resultCount = parseInteger(row.result_count, 0);

  return {
    id: row.id,
    action: row.action,
    actionLabel: getUsageActionLabel(row.action),
    module: row.module,
    moduleLabel: getUsageModuleLabel(row.module),
    mode: normalizeKeepaUsageMode(row.mode, resolveKeepaUsageMode({ mode: row.mode }, row.action, row.module)),
    drawerKey: sanitizeUsageDrawerKey(row.drawer_key, ''),
    timestampStart: row.timestamp_start || row.created_at,
    timestampEnd: row.timestamp_end || row.created_at,
    filters: fromJson(row.filters_json, null),
    resultCount,
    durationMs: parseInteger(row.duration_ms, 0),
    requestStatus: normalizeUsageStatus(row.request_status, 'success'),
    estimatedUsage: parseNumber(row.estimated_usage, 0) || 0,
    officialUsageValue: parseNumber(row.official_usage_value, null),
    officialTokensLeft: parseInteger(row.official_tokens_left, null),
    tokensBefore: parseInteger(row.tokens_before, null),
    tokensAfter: parseInteger(row.tokens_after, parseInteger(row.official_tokens_left, null)),
    tokensUsed,
    tokensPerRequest: tokensUsed,
    tokensPerResult: resultCount > 0 ? Math.round(((tokensUsed / resultCount) * 10)) / 10 : 0,
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

function getKeepaRequestTrackingSummary() {
  const todayStartIso = getRangeStart('today').toISOString();
  const monthStartIso = getRangeStart('month').toISOString();
  const monthStartDateKey = toLocalDateKey(getRangeStart('month'));
  const recent60s = getKeepaRequestWindowMetrics(KEEPA_REQUEST_WINDOW_60S_MS);
  const recent5m = getKeepaRequestWindowMetrics(KEEPA_REQUEST_WINDOW_5M_MS);
  const lastRequest = getLatestKeepaApiRequest();
  const expensiveRequest = getMostExpensiveKeepaApiRequest();
  const todayStats =
    db
      .prepare(
        `
          SELECT
            COUNT(*) AS requestCount,
            COALESCE(SUM(COALESCE(tokens_used, official_usage_value, estimated_usage, 0)), 0) AS tokensUsed,
            COALESCE(SUM(result_count), 0) AS resultCount,
            MAX(created_at) AS lastRequestAt
          FROM keepa_usage_logs
          WHERE action = 'keepa-request'
            AND created_at >= ?
        `
      )
      .get(todayStartIso) || {};
  const monthStats =
    db
      .prepare(
        `
          SELECT
            COUNT(*) AS requestCount,
            COALESCE(SUM(COALESCE(tokens_used, official_usage_value, estimated_usage, 0)), 0) AS tokensUsed,
            COALESCE(SUM(result_count), 0) AS resultCount
          FROM keepa_usage_logs
          WHERE action = 'keepa-request'
            AND created_at >= ?
        `
      )
      .get(monthStartIso) || {};
  const activeDaysInMonth =
    db
      .prepare(
        `
          SELECT COUNT(*) AS count
          FROM keepa_usage_daily
          WHERE action = 'keepa-request'
            AND usage_date >= ?
            AND request_count > 0
        `
      )
      .get(monthStartDateKey)?.count || 0;
  const elapsedDaysInMonth = Math.max(1, new Date().getDate());
  const tokensToday = Math.round((parseNumber(todayStats.tokensUsed, 0) || 0) * 10) / 10;
  const requestsToday = parseInteger(todayStats.requestCount, 0);
  const resultsToday = parseInteger(todayStats.resultCount, 0);
  const tokensMonth = Math.round((parseNumber(monthStats.tokensUsed, 0) || 0) * 10) / 10;
  const hardStopActive = (() => {
    const cachedTokens = parseInteger(keepaConnectionCache?.tokensLeft, null);
    return cachedTokens !== null && cachedTokens < KEEPA_HARD_STOP_MIN_TOKENS;
  })();
  const warningsActive = buildKeepaTrackingWarnings({
    recent60s,
    recent5m,
    lastRequest,
    expensiveRequest,
    hardStopActive
  });

  return {
    lastRequest,
    expensiveRequest,
    windows: {
      last60s: recent60s,
      last5m: recent5m
    },
    requestsToday,
    tokensToday,
    tokensMonth,
    resultsToday,
    activeDaysInMonth,
    averageTokensPerDay: Math.round(((tokensMonth / elapsedDaysInMonth) * 10)) / 10,
    averageTokensPerRequest: requestsToday ? Math.round(((tokensToday / requestsToday) * 10)) / 10 : 0,
    averageTokensPerResult: resultsToday ? Math.round(((tokensToday / resultsToday) * 10)) / 10 : 0,
    tokensPerMinute: Math.round((recent5m.tokensUsed / Math.max(KEEPA_REQUEST_WINDOW_5M_MS / 60000, 1)) * 10) / 10,
    requestsPerMinute: recent5m.requestsPerMinute,
    hardStopActive,
    warningsActive
  };
}

export function getKeepaUsageSummary() {
  const settings = getKeepaSettings();
  const todayStart = getRangeStart('today');
  const monthStart = getRangeStart('month');
  const todayStartIso = todayStart.toISOString();
  const monthStartIso = monthStart.toISOString();
  const trackingSummary = getKeepaRequestTrackingSummary();
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
      lastRequestAt: trackingSummary.lastRequest?.createdAt || latestRequestOverall.lastRequestAt || requestStatsToday.lastRequestAt || latestManualSearch?.createdAt || latestAutomationRun?.createdAt || null,
      requestsToday: trackingSummary.requestsToday,
      requestsMonth: parseInteger(requestStatsMonth.requestCount, 0),
      estimatedUsageToday: Math.round((parseNumber(summaryToday.estimatedUsage, 0) || 0) * 10) / 10,
      estimatedUsageMonth,
      hitsToday: parseInteger(summaryToday.hitCount, 0),
      activeRulesCount,
      monthlyProjection: Math.round(((estimatedUsageMonth / todayIndex) * daysInMonth) * 10) / 10,
      tokensToday: trackingSummary.tokensToday,
      tokensMonth: trackingSummary.tokensMonth,
      burnLast60s: trackingSummary.windows.last60s.tokensUsed,
      burnLast5m: trackingSummary.windows.last5m.tokensUsed,
      requestsPerMinute: trackingSummary.requestsPerMinute,
      averageTokensPerRequest: trackingSummary.averageTokensPerRequest,
      averageTokensPerResult: trackingSummary.averageTokensPerResult,
      averageTokensPerDay: trackingSummary.averageTokensPerDay,
      mostExpensiveRequestTokens: trackingSummary.expensiveRequest?.tokensUsed ?? 0,
      hardStopActive: trackingSummary.hardStopActive
    },
    today: {
      estimatedUsage: Math.round((parseNumber(summaryToday.estimatedUsage, 0) || 0) * 10) / 10,
      tokensUsed: trackingSummary.tokensToday,
      hitCount: parseInteger(summaryToday.hitCount, 0),
      errorCount: parseInteger(summaryToday.errorCount, 0),
      requestCount: trackingSummary.requestsToday
    },
    month: {
      estimatedUsage: estimatedUsageMonth,
      tokensUsed: trackingSummary.tokensMonth,
      hitCount: parseInteger(summaryMonth.hitCount, 0),
      errorCount: parseInteger(summaryMonth.errorCount, 0),
      requestCount: parseInteger(requestStatsMonth.requestCount, 0)
    },
    lastManualSearch: latestManualSearch,
    lastAutomationRun: latestAutomationRun,
    dealsToday: parseInteger(summaryToday.hitCount, 0),
    sourceBreakdown: getUsageSourceBreakdown(todayStartIso),
    recentIssues,
    requestTracking: trackingSummary,
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
          COALESCE(SUM(CASE WHEN official_usage_value IS NOT NULL THEN official_usage_value ELSE 0 END), 0) AS officialUsage,
          COALESCE(SUM(CASE WHEN action = 'keepa-request' THEN tokens_used_total ELSE 0 END), 0) AS tokensUsed
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
      tokensUsed: Math.round((parseNumber(row.tokensUsed, 0) || 0) * 10) / 10,
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
  const protectionConnection = await getKeepaProtectionConnection('status-check');
  const connection = protectionConnection.configured
    ? {
        connected: Boolean(protectionConnection.connected),
        tokensLeft: protectionConnection.tokensLeft ?? null,
        refillRate: protectionConnection.refillRate ?? null,
        refillInMs: protectionConnection.refillInMs ?? null,
        tokensConsumed: protectionConnection.tokensConsumed ?? null,
        checkedAt: protectionConnection.checkedAt || null,
        configured: true,
        errorMessage: protectionConnection.errorMessage || ''
      }
    : {
        connected: false,
        tokensLeft: null,
        refillRate: null,
        refillInMs: null,
        tokensConsumed: null,
        checkedAt: null,
        configured: false,
        errorMessage: protectionConnection.errorMessage || ''
      };

  const counts = getOverviewCounts();
  const usageSummary = getKeepaUsageSummary();
  const fakeDropSummary = getFakeDropSummary();
  const protection = finalizeKeepaProtectionDecision(
    buildKeepaProtectionState({
      origin: 'manual',
      connection,
      risk: {
        estimatedTokenCost: usageSummary.usageSettings.estimatedManualRunCost,
        estimatedRiskScore: 0,
        estimatedRawHitsRisk: 'low',
        warnings: [],
        blockingReasons: []
      }
    })
  );
  const requestTracking = {
    ...(usageSummary.requestTracking || {}),
    hardStopActive: Boolean(protection.blocked || protection.hardStopActive),
    warningsActive: buildKeepaTrackingWarnings({
      recent60s: usageSummary.requestTracking?.windows?.last60s,
      recent5m: usageSummary.requestTracking?.windows?.last5m,
      lastRequest: usageSummary.requestTracking?.lastRequest,
      expensiveRequest: usageSummary.requestTracking?.expensiveRequest,
      hardStopActive: Boolean(protection.blocked || protection.hardStopActive)
    })
  };
  return {
    settings: settingsView,
    connection,
    protection,
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
        usageModeLabel: usageSummary.usageModeLabel,
        tokensToday: usageSummary.kpis.tokensToday,
        burnLast60s: usageSummary.kpis.burnLast60s
      },
      usageSummary: {
        ...usageSummary,
        requestTracking
      }
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
    sellerType: 'AMAZON',
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

  const outputs = [
    await publishAutoDealToTelegramTestGroup({
      result: sampleResult,
      rule: null,
      settings,
      sourceType: 'keepa',
      decisionStatus: 'approved_for_test_group',
      decisionReason: 'Manueller Keepa-Testgruppen-Test.',
      learningContext: {
        keepa: {
          status: 'test_alert',
          available: true
        }
      }
    })
  ];

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

function scheduleNextRuleRun(rule) {
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
}

async function processRule(rule) {
  const settings = getKeepaSettings();
  const drawerKey = inferDrawerKeyFromSellerType(rule.sellerType);
  const drawerConfig = getDrawerConfig(settings, drawerKey);
  const autoModeActive = settings.schedulerEnabled && drawerConfig.active && drawerConfig.autoModeAllowed;

  logGeneratorDebug(`KEEPA DRAWER ACTIVE: ${drawerKey}`, {
    drawerKey,
    ruleId: rule.id,
    sellerType: rule.sellerType
  });
  logGeneratorDebug(`KEEPA AUTO MODE: ${autoModeActive}`, {
    drawerKey,
    ruleId: rule.id,
    schedulerEnabled: settings.schedulerEnabled,
    drawerActive: drawerConfig.active,
    drawerAutoModeAllowed: drawerConfig.autoModeAllowed
  });

  if (!settings.schedulerEnabled || !drawerConfig.active || !drawerConfig.autoModeAllowed) {
    logGeneratorDebug('AUTO KEEPA BLOCKED NOT EXPLICITLY ENABLED', {
      drawerKey,
      ruleId: rule.id,
      schedulerEnabled: settings.schedulerEnabled,
      drawerActive: drawerConfig.active,
      drawerAutoModeAllowed: drawerConfig.autoModeAllowed
    });
    recordKeepaUsage({
      action: 'automation-run',
      module: 'automation-run',
      filters: {
        ruleId: rule.id,
        drawerKey,
        sellerType: rule.sellerType
      },
      resultCount: 0,
      durationMs: 0,
      requestStatus: 'skipped',
      estimatedUsage: 0,
      ruleId: rule.id,
      meta: {
        reason: !settings.schedulerEnabled ? 'scheduler_disabled' : !drawerConfig.active ? 'drawer_inactive' : 'auto_mode_disabled'
      }
    });
    scheduleNextRuleRun(rule);

    logKeepaEvent('info', 'rule_skipped', 'scheduler', `Keepa-Regel ${rule.name} wurde uebersprungen.`, {
      payload: {
        ruleId: rule.id,
        drawerKey,
        reason: !settings.schedulerEnabled ? 'scheduler_disabled' : !drawerConfig.active ? 'drawer_inactive' : 'auto_mode_disabled'
      }
    });
    return;
  }

  const categoryIntersection =
    drawerConfig.categories.length && rule.categories.length
      ? rule.categories.filter((categoryId) => drawerConfig.categories.includes(categoryId))
      : [];
  const mergedCategories =
    categoryIntersection.length > 0
      ? categoryIntersection
      : drawerConfig.categories.length
        ? drawerConfig.categories
        : rule.categories;
  const filters = {
    page: 1,
    limit: 20,
    drawerKey,
    domainId: settings.domainId,
    minDiscount: Math.max(rule.minDiscount, drawerConfig.minDiscount),
    sellerType: rule.sellerType,
    categories: mergedCategories,
    minPrice:
      drawerConfig.minPrice !== null && rule.minPrice !== null
        ? Math.max(drawerConfig.minPrice, rule.minPrice)
        : drawerConfig.minPrice ?? rule.minPrice,
    maxPrice:
      drawerConfig.maxPrice !== null && rule.maxPrice !== null
        ? Math.min(drawerConfig.maxPrice, rule.maxPrice)
        : drawerConfig.maxPrice ?? rule.maxPrice,
    trendInterval: drawerConfig.trendInterval,
    sortBy: drawerConfig.sortBy,
    onlyPrime: Boolean(rule.onlyPrime || drawerConfig.onlyPrime),
    onlyInStock: Boolean(rule.onlyInStock || drawerConfig.onlyInStock),
    onlyGoodRating: Boolean(rule.onlyGoodRating || drawerConfig.onlyGoodRating),
    onlyWithReviews: Boolean(drawerConfig.onlyWithReviews),
    amazonOfferMode: drawerConfig.amazonOfferMode,
    singleVariantOnly: Boolean(drawerConfig.singleVariantOnly),
    recentPriceChangeOnly: Boolean(drawerConfig.recentPriceChangeOnly)
  };

  if (filters.minPrice !== null && filters.maxPrice !== null && Number(filters.minPrice) > Number(filters.maxPrice)) {
    recordKeepaUsage({
      action: 'automation-run',
      module: 'automation-run',
      filters: {
        ruleId: rule.id,
        drawerKey,
        sellerType: rule.sellerType
      },
      resultCount: 0,
      durationMs: 0,
      requestStatus: 'skipped',
      estimatedUsage: 0,
      ruleId: rule.id,
      meta: {
        reason: 'invalid_price_range'
      }
    });
    scheduleNextRuleRun(rule);
    logKeepaEvent('warning', 'rule_skipped', 'scheduler', `Keepa-Regel ${rule.name} hat eine ungueltige Preisrange.`, {
      payload: {
        ruleId: rule.id,
        drawerKey,
        minPrice: filters.minPrice,
        maxPrice: filters.maxPrice
      }
    });
    return;
  }

  const autoConnection = await getKeepaProtectionConnection('status-check');
  const autoProtection = finalizeKeepaProtectionDecision(
    buildKeepaProtectionState({
      origin: 'automatic',
      drawerKey,
      connection: autoConnection,
      risk: {
        estimatedTokenCost: Math.max(estimateSearchUsage(filters, settings, { origin: 'automatic', requestCount: 2 }), 40),
        estimatedRiskScore: 0,
        estimatedRawHitsRisk: 'low',
        warnings: [],
        blockingReasons: []
      }
    })
  );

  if (autoProtection.blocked) {
    logGeneratorDebug('KEEPA REQUEST BLOCKED', {
      origin: 'automatic',
      drawerKey,
      blockCode: autoProtection.blockCode || 'protection_blocked',
      blockReason: autoProtection.blockReason
    });

    if (autoProtection.blockCode === 'KEEPA_LOW_TOKENS') {
      logGeneratorDebug('KEEPA REQUEST BLOCKED LOW TOKENS', {
        origin: 'automatic',
        drawerKey,
        tokensLeft: autoProtection.tokensLeft,
        minTokensRequired: autoProtection.minTokensRequired
      });
      logGeneratorDebug('KEEPA HARD STOP ACTIVE', {
        origin: 'automatic',
        drawerKey,
        tokensLeft: autoProtection.tokensLeft,
        minTokensRequired: autoProtection.minTokensRequired
      });
    } else if (autoProtection.blockCode === 'KEEPA_TOO_MANY_REQUESTS' || autoProtection.blockCode === 'KEEPA_HIGH_USAGE_WINDOW') {
      logGeneratorDebug('KEEPA HIGH USAGE DETECTED', {
        origin: 'automatic',
        drawerKey,
        blockCode: autoProtection.blockCode,
        requestCount60s: autoProtection.recentUsage?.last60s?.requestCount ?? 0,
        tokensUsed60s: autoProtection.recentUsage?.last60s?.tokensUsed ?? 0,
        requestCount5m: autoProtection.recentUsage?.last5m?.requestCount ?? 0,
        tokensUsed5m: autoProtection.recentUsage?.last5m?.tokensUsed ?? 0
      });
    }

    recordKeepaUsage({
      action: 'automation-run',
      module: 'automation-run',
      filters: {
        ruleId: rule.id,
        drawerKey,
        sellerType: rule.sellerType
      },
      resultCount: 0,
      durationMs: 0,
      requestStatus: 'skipped',
      estimatedUsage: 0,
      ruleId: rule.id,
      meta: {
        reason: autoProtection.blockCode || 'protection_blocked',
        message: autoProtection.blockReason
      }
    });
    scheduleNextRuleRun(rule);
    return;
  }

  beginKeepaSearchExecution('automatic', drawerKey);
  let result;
  try {
    result = await executeSearch(filters, {
      source: 'rule_scan',
      origin: 'automatic',
      rule
    });
  } finally {
    finishKeepaSearchExecution('automatic');
  }

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

  scheduleNextRuleRun(rule);

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
      if (!settings.schedulerEnabled) {
        logGeneratorDebug('AUTO KEEPA BLOCKED NOT EXPLICITLY ENABLED', {
          reason: 'scheduler_disabled'
        });
      }
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
  console.info('Scheduler gestartet', {
    intervalMs: SCHEDULER_INTERVAL_MS,
    scope: 'deal-processing'
  });
  setInterval(() => {
    void runDueRules();
  }, SCHEDULER_INTERVAL_MS);

  void runDueRules();
}

export function getKeepaSchedulerRuntimeStatus() {
  return {
    started: schedulerStarted,
    running: schedulerRunning,
    intervalMs: SCHEDULER_INTERVAL_MS
  };
}
