import { getDb } from '../db.js';
import {
  buildArrayFromTextList,
  cleanText,
  fromJson,
  nowIso,
  parseBool,
  parseInteger,
  parseNumber,
  round
} from './dealEngine/shared.js';

const db = getDb();

export const PRODUCT_RULE_BRAND_TYPE = {
  ANY: 'ANY',
  BRAND: 'BRAND',
  NONAME: 'NONAME',
  UNKNOWN: 'UNKNOWN'
};

const PRODUCT_RULE_BRAND_LABEL = {
  ANY: 'Egal',
  BRAND: 'Marke',
  NONAME: 'NoName',
  UNKNOWN: 'Unbekannt'
};

const KNOWN_BRANDS = new Set(
  [
    'Sony',
    'Beats',
    'JBL',
    'Samsung',
    'Apple',
    'Anker',
    'Soundcore',
    'Bose',
    'Sennheiser',
    'Philips',
    'Panasonic',
    'Nike',
    'Adidas',
    'Puma',
    'Reebok',
    'New Balance',
    'Asics',
    'Skechers',
    'Birkenstock',
    'Bosch',
    'Makita',
    'DeWalt',
    'Logitech',
    'Xiaomi',
    'Lenovo',
    'Acer',
    'Asus',
    'LG',
    'Canon',
    'Nikon'
  ].map((item) => normalizeBrandKey(item))
);

const NONAME_TITLE_HINTS = [
  'tws',
  'earbuds',
  'wireless earbuds',
  'mini beamer',
  'projector',
  'bluetooth kopfhoerer',
  'bluetooth kopfhörer',
  'wireless headset',
  'gaming headset'
];

const GENERIC_MERCHANT_HINTS = [
  'shop',
  'store',
  'direct',
  'trading',
  'official',
  'global',
  'digital',
  'tech',
  'market',
  'europe',
  'eu',
  'gmbh',
  'ltd'
];

const DEFAULT_PRODUCT_RULES = [
  {
    name: 'China Kopfhoerer',
    keywords: ['kopfhoerer', 'bluetooth kopfhoerer', 'in ear', 'earbuds', 'tws'],
    brandType: PRODUCT_RULE_BRAND_TYPE.NONAME,
    maxPrice: 12,
    minReviews: 50,
    minRating: 4,
    marketCompareRequired: false,
    capacityMin: null,
    capacityMax: null,
    active: true
  },
  {
    name: 'China Beamer',
    keywords: ['beamer', 'mini beamer', 'projector'],
    brandType: PRODUCT_RULE_BRAND_TYPE.NONAME,
    maxPrice: 50,
    minReviews: 50,
    minRating: 4,
    marketCompareRequired: false,
    capacityMin: null,
    capacityMax: null,
    active: true
  },
  {
    name: 'Marken Sneaker',
    keywords: ['sneaker', 'schuhe'],
    brandType: PRODUCT_RULE_BRAND_TYPE.BRAND,
    maxPrice: 33,
    minReviews: 0,
    minRating: 0,
    marketCompareRequired: true,
    capacityMin: null,
    capacityMax: null,
    active: true
  },
  {
    name: 'Powerbank 10000mAh',
    keywords: ['powerbank'],
    brandType: PRODUCT_RULE_BRAND_TYPE.ANY,
    maxPrice: 11,
    minReviews: 50,
    minRating: 4,
    marketCompareRequired: false,
    capacityMin: 9000,
    capacityMax: 11000,
    active: true
  },
  {
    name: 'Powerbank 19000-30000mAh',
    keywords: ['powerbank'],
    brandType: PRODUCT_RULE_BRAND_TYPE.ANY,
    maxPrice: 16,
    minReviews: 50,
    minRating: 4,
    marketCompareRequired: false,
    capacityMin: 19000,
    capacityMax: 30000,
    active: true
  },
  {
    name: 'Grosse Powerbank',
    keywords: ['powerbank'],
    brandType: PRODUCT_RULE_BRAND_TYPE.ANY,
    maxPrice: 25,
    minReviews: 50,
    minRating: 4,
    marketCompareRequired: false,
    capacityMin: 30001,
    capacityMax: null,
    active: true
  }
];

function normalizeSearchText(value = '') {
  return cleanText(String(value || ''))
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function normalizeCompactText(value = '') {
  return normalizeSearchText(value).replace(/\s+/g, '');
}

function normalizeBrandKey(value = '') {
  return normalizeCompactText(value);
}

function formatCurrency(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return '-';
  }

  return new Intl.NumberFormat('de-DE', {
    style: 'currency',
    currency: 'EUR'
  }).format(parsed);
}

function parseKeywords(value) {
  let input = value;

  if (!Array.isArray(input) && typeof input === 'string') {
    const trimmed = input.trim();
    if (trimmed.startsWith('[')) {
      input = fromJson(trimmed, []);
    }
  }

  if (!Array.isArray(input) && typeof input !== 'string') {
    input = cleanText(value?.keywords_json) ? fromJson(value.keywords_json, []) : [];
  }

  return [...new Set(buildArrayFromTextList(input).map((item) => cleanText(item)).filter(Boolean))];
}

function normalizeBrandType(value = '') {
  const normalized = normalizeSearchText(value);

  if (['marke', 'brand', 'branded'].includes(normalized)) {
    return PRODUCT_RULE_BRAND_TYPE.BRAND;
  }

  if (['noname', 'no name', 'china', 'noname produkt'].includes(normalized)) {
    return PRODUCT_RULE_BRAND_TYPE.NONAME;
  }

  if (['egal', 'any', 'alle', 'all'].includes(normalized)) {
    return PRODUCT_RULE_BRAND_TYPE.ANY;
  }

  return PRODUCT_RULE_BRAND_TYPE.ANY;
}

function normalizeCapacity(value) {
  const parsed = parseInteger(value, null);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

function normalizeRatingValue(value) {
  const parsed = parseNumber(value, null);
  if (parsed === null) {
    return null;
  }

  return Math.max(0, Math.min(5, round(parsed, 2)));
}

function mapProductRuleRow(row = {}) {
  return {
    id: Number(row.id || 0),
    name: cleanText(row.name),
    keywords: parseKeywords(row.keywords_json),
    brandType: normalizeBrandType(row.brand_type || PRODUCT_RULE_BRAND_TYPE.ANY),
    brandTypeLabel: PRODUCT_RULE_BRAND_LABEL[normalizeBrandType(row.brand_type || PRODUCT_RULE_BRAND_TYPE.ANY)] || 'Egal',
    maxPrice: parseNumber(row.max_price, null),
    minReviews: parseInteger(row.min_reviews, 0),
    minRating: normalizeRatingValue(row.min_rating) ?? 0,
    marketCompareRequired: parseBool(row.market_compare_required, false),
    capacityMin: normalizeCapacity(row.capacity_min),
    capacityMax: normalizeCapacity(row.capacity_max),
    active: parseBool(row.active, true),
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null
  };
}

function normalizeProductRulePayload(input = {}, existingRule = null) {
  const safeExisting = existingRule && typeof existingRule === 'object' ? existingRule : null;
  const name = cleanText(input.name ?? safeExisting?.name);
  if (!name) {
    throw new Error('Regelname fehlt.');
  }

  const keywords = parseKeywords(
    input.keywords ??
      input.keywordsText ??
      input.keywordsJson ??
      input.keywords_json ??
      safeExisting?.keywords ??
      []
  );
  const brandType = normalizeBrandType(input.brandType ?? input.brand_type ?? safeExisting?.brandType);
  const maxPrice =
    input.maxPrice === '' || input.max_price === '' ? null : parseNumber(input.maxPrice ?? input.max_price, safeExisting?.maxPrice ?? null);
  const minReviews = Math.max(0, parseInteger(input.minReviews ?? input.min_reviews, safeExisting?.minReviews ?? 0));
  const minRating = Math.max(0, normalizeRatingValue(input.minRating ?? input.min_rating ?? safeExisting?.minRating ?? 0) ?? 0);
  const marketCompareRequired =
    input.marketCompareRequired === undefined && input.market_compare_required === undefined
      ? safeExisting?.marketCompareRequired === true
      : parseBool(input.marketCompareRequired ?? input.market_compare_required, false);
  const capacityMin =
    input.capacityMin === '' || input.capacity_min === ''
      ? null
      : normalizeCapacity(input.capacityMin ?? input.capacity_min ?? safeExisting?.capacityMin);
  const capacityMax =
    input.capacityMax === '' || input.capacity_max === ''
      ? null
      : normalizeCapacity(input.capacityMax ?? input.capacity_max ?? safeExisting?.capacityMax);
  const active =
    input.active === undefined && input.isActive === undefined
      ? safeExisting?.active !== false
      : parseBool(input.active ?? input.isActive, true);

  if (!keywords.length && capacityMin === null && capacityMax === null) {
    throw new Error('Mindestens ein Keyword oder eine Kapazitaetsgrenze ist noetig.');
  }

  if (maxPrice !== null && maxPrice <= 0) {
    throw new Error('Maximalpreis muss groesser als 0 sein.');
  }

  if (capacityMin !== null && capacityMax !== null && capacityMin > capacityMax) {
    throw new Error('Kapazitaet Min darf nicht groesser als Max sein.');
  }

  return {
    name,
    keywords,
    brandType,
    maxPrice: maxPrice === null ? null : round(maxPrice, 2),
    minReviews,
    minRating,
    marketCompareRequired,
    capacityMin,
    capacityMax,
    active
  };
}

function loadProductRuleRowById(id) {
  return db.prepare(`SELECT * FROM product_rules WHERE id = ?`).get(id) || null;
}

function detectKnownBrandInText(title = '') {
  const compact = normalizeCompactText(title);
  if (!compact) {
    return '';
  }

  const brand = [...KNOWN_BRANDS].find((item) => compact.includes(item));
  return brand || '';
}

function resolveProductBrand(title = '', explicitBrand = '') {
  const explicit = cleanText(explicitBrand);
  if (explicit) {
    return explicit;
  }

  const detected = detectKnownBrandInText(title);
  return detected || '';
}

function hasNoNameKeyword(text = '') {
  const normalized = normalizeSearchText(text);
  if (!normalized) {
    return false;
  }

  return NONAME_TITLE_HINTS.some((item) => normalized.includes(normalizeSearchText(item)));
}

function looksLikeGenericMerchant(merchantName = '') {
  const normalized = normalizeSearchText(merchantName);
  if (!normalized) {
    return false;
  }

  return GENERIC_MERCHANT_HINTS.some((item) => normalized.includes(item));
}

function looksLikeNoNameBrand(brand = '') {
  const value = cleanText(brand);
  const compact = normalizeCompactText(value);
  if (!compact) {
    return true;
  }

  if (KNOWN_BRANDS.has(compact)) {
    return false;
  }

  if (compact.length <= 4) {
    return true;
  }

  if (/^[a-z]{2,6}\d{1,3}$/i.test(value) || /^[a-z0-9]{5,8}$/i.test(value)) {
    return true;
  }

  const vowelCount = (compact.match(/[aeiou]/g) || []).length;
  if (compact.length <= 7 && vowelCount <= 1) {
    return true;
  }

  return false;
}

export function extractProductCapacityMah(input = {}) {
  const explicit = normalizeCapacity(input.capacityMah ?? input.capacity_mah);
  if (explicit !== null) {
    return explicit;
  }

  const values = [
    input.title,
    input.category,
    ...(Array.isArray(input.features) ? input.features : buildArrayFromTextList(input.features))
  ];

  for (const value of values) {
    const match = String(value || '').match(/(\d{1,3}(?:[.\s]?\d{3})+|\d{4,6})\s*m\s*a\s*h/i);
    if (!match) {
      continue;
    }

    const parsed = Number.parseInt(match[1].replace(/[^\d]/g, ''), 10);
    if (Number.isFinite(parsed) && parsed > 0) {
      return parsed;
    }
  }

  return null;
}

export function resolveProductRuleBrandType(input = {}) {
  if (parseBool(input.isBrandProduct, false) === true) {
    return PRODUCT_RULE_BRAND_TYPE.BRAND;
  }

  if (parseBool(input.isNoName, false) === true || parseBool(input.isChinaProduct, false) === true) {
    return PRODUCT_RULE_BRAND_TYPE.NONAME;
  }

  const title = cleanText(input.title);
  const brand = resolveProductBrand(title, input.brand);
  const merchantName = cleanText(
    input.merchantName || input.sellerName || input.amazonMerchantName || input.offerMerchantInfo || input.paapiMerchantInfo
  );
  const normalizedBrandKey = normalizeBrandKey(brand);

  if (normalizedBrandKey && KNOWN_BRANDS.has(normalizedBrandKey)) {
    return PRODUCT_RULE_BRAND_TYPE.BRAND;
  }

  if (!brand) {
    return hasNoNameKeyword(title) || looksLikeGenericMerchant(merchantName)
      ? PRODUCT_RULE_BRAND_TYPE.NONAME
      : PRODUCT_RULE_BRAND_TYPE.UNKNOWN;
  }

  if (looksLikeNoNameBrand(brand) || hasNoNameKeyword(title) || looksLikeGenericMerchant(merchantName)) {
    return PRODUCT_RULE_BRAND_TYPE.NONAME;
  }

  return PRODUCT_RULE_BRAND_TYPE.UNKNOWN;
}

function resolveMarketComparisonState(input = {}) {
  const explicitAvailable =
    input.marketCompareAvailable === true ||
    input.marketComparisonAvailable === true ||
    input.marketCompareOk === true ||
    input.marketComparisonOk === true;
  const status = cleanText(input.marketComparisonStatus || input.marketCompareStatus || '').toLowerCase();
  const available = explicitAvailable || status === 'success' || status === 'available' || status === 'used';

  return {
    available,
    status: status || (available ? 'success' : 'missing')
  };
}

function buildRuleSearchProfile(input = {}) {
  const features = Array.isArray(input.features) ? input.features.map((item) => cleanText(item)).filter(Boolean) : buildArrayFromTextList(input.features);
  const title = cleanText(input.title);
  const category = cleanText(input.category);
  const featureText = features.join(' ');
  const mergedText = [title, category, featureText].filter(Boolean).join(' ');

  return {
    title,
    category,
    features,
    featureText,
    mergedText,
    normalizedText: normalizeSearchText(mergedText),
    compactText: normalizeCompactText(mergedText)
  };
}

function matchRuleKeywords(rule = {}, profile = {}) {
  const keywords = Array.isArray(rule.keywords) ? rule.keywords : [];
  if (!keywords.length) {
    return [];
  }

  return keywords.filter((keyword) => {
    const normalizedKeyword = normalizeSearchText(keyword);
    const compactKeyword = normalizeCompactText(keyword);
    if (!normalizedKeyword && !compactKeyword) {
      return false;
    }

    return (
      (normalizedKeyword && profile.normalizedText.includes(normalizedKeyword)) ||
      (compactKeyword && profile.compactText.includes(compactKeyword))
    );
  });
}

function resolveRuleSpecificity(rule = {}) {
  const keywordCount = Array.isArray(rule.keywords) ? rule.keywords.length : 0;
  const longestKeywordLength = (rule.keywords || []).reduce((max, item) => Math.max(max, cleanText(item).length), 0);
  const capacityBoundCount = Number(rule.capacityMin !== null) + Number(rule.capacityMax !== null);
  const capacityRangeSpan =
    rule.capacityMin !== null && rule.capacityMax !== null ? Math.max(0, rule.capacityMax - rule.capacityMin) : Number.MAX_SAFE_INTEGER;
  const brandSpecificity = rule.brandType === PRODUCT_RULE_BRAND_TYPE.ANY ? 0 : 1;

  return {
    keywordCount,
    longestKeywordLength,
    capacityBoundCount,
    capacityRangeSpan,
    brandSpecificity
  };
}

function selectBestMatchingRule(rules = [], input = {}) {
  const profile = buildRuleSearchProfile(input);
  const brandType = resolveProductRuleBrandType(input);
  const capacityMah = extractProductCapacityMah(input);
  const candidates = [];

  for (const rule of rules) {
    const matchedKeywords = matchRuleKeywords(rule, profile);
    const hasCapacityFilter = rule.capacityMin !== null || rule.capacityMax !== null;
    const hasKeywordMatch = matchedKeywords.length > 0;

    if (!hasKeywordMatch && !hasCapacityFilter) {
      continue;
    }

    if (rule.brandType !== PRODUCT_RULE_BRAND_TYPE.ANY && rule.brandType !== brandType) {
      continue;
    }

    if (hasCapacityFilter) {
      if (capacityMah === null) {
        continue;
      }

      if (rule.capacityMin !== null && capacityMah < rule.capacityMin) {
        continue;
      }

      if (rule.capacityMax !== null && capacityMah > rule.capacityMax) {
        continue;
      }
    }

    if (!hasKeywordMatch && Array.isArray(rule.keywords) && rule.keywords.length) {
      continue;
    }

    const specificity = resolveRuleSpecificity(rule);
    candidates.push({
      rule,
      matchedKeywords,
      specificity
    });
  }

  candidates.sort((left, right) => {
    if (right.specificity.capacityBoundCount !== left.specificity.capacityBoundCount) {
      return right.specificity.capacityBoundCount - left.specificity.capacityBoundCount;
    }

    if (right.matchedKeywords.length !== left.matchedKeywords.length) {
      return right.matchedKeywords.length - left.matchedKeywords.length;
    }

    if (right.specificity.longestKeywordLength !== left.specificity.longestKeywordLength) {
      return right.specificity.longestKeywordLength - left.specificity.longestKeywordLength;
    }

    if (right.specificity.brandSpecificity !== left.specificity.brandSpecificity) {
      return right.specificity.brandSpecificity - left.specificity.brandSpecificity;
    }

    if (left.specificity.capacityRangeSpan !== right.specificity.capacityRangeSpan) {
      return left.specificity.capacityRangeSpan - right.specificity.capacityRangeSpan;
    }

    return left.rule.id - right.rule.id;
  });

  return {
    profile,
    brandType,
    capacityMah,
    match: candidates[0] || null
  };
}

function buildReasonDetails(code = '', message = '', severity = 'warning', meta = {}) {
  if (!cleanText(message)) {
    return [];
  }

  return [
    {
      code: cleanText(code) || 'product_rule',
      message: cleanText(message),
      severity,
      meta
    }
  ];
}

function logProductRule(tag, payload = {}) {
  try {
    console.info(`[${tag}]`, payload);
  } catch {
    console.info(`[${tag}]`);
  }
}

export function evaluateProductRules(deal = {}) {
  const activeRules = listProductRules().filter((item) => item.active === true);
  const brand = resolveProductBrand(deal.title, deal.brand);
  const actualPrice = parseNumber(deal.finalPrice ?? deal.price ?? deal.currentPrice ?? deal.amazonPrice, null);
  const actualRating = normalizeRatingValue(deal.rating);
  const actualReviews = parseInteger(deal.reviewCount ?? deal.totalReviews ?? deal.reviews, null);
  const marketComparison = resolveMarketComparisonState(deal);
  const selection = selectBestMatchingRule(activeRules, {
    ...deal,
    brand
  });

  if (!selection.match) {
    logProductRule('PRODUCT_RULE_NO_MATCH', {
      scope: cleanText(deal.scope) || 'default',
      title: cleanText(deal.title).slice(0, 140),
      brand: brand || '',
      brandType: selection.brandType,
      price: actualPrice === null ? null : formatCurrency(actualPrice),
      capacityMah: selection.capacityMah
    });

    return {
      matchedRule: null,
      matchedRuleName: '',
      matchedKeywords: [],
      allowed: true,
      decision: 'allow',
      reason: 'Keine Produktregel ausgelost.',
      reasonCode: 'PRODUCT_RULE_NO_MATCH',
      maxPrice: null,
      actualPrice,
      minReviews: 0,
      actualReviews,
      minRating: 0,
      actualRating,
      marketCompareRequired: false,
      marketCompareAvailable: marketComparison.available,
      marketCompareStatus: marketComparison.status,
      capacityMah: selection.capacityMah,
      brand: brand || '',
      brandType: selection.brandType,
      brandTypeLabel: PRODUCT_RULE_BRAND_LABEL[selection.brandType] || 'Unbekannt',
      status: 'clear',
      action: 'none',
      summary: 'Keine Produktregel ausgelost.',
      items: [],
      reasonDetails: []
    };
  }

  const matchedRule = selection.match.rule;
  const matchedKeywords = selection.match.matchedKeywords;
  const hardFailures = [];
  const reviewReasons = [];

  if (matchedRule.maxPrice !== null) {
    if (actualPrice === null) {
      reviewReasons.push('Preis fehlt fuer die Produkt-Regel.');
    } else if (actualPrice > matchedRule.maxPrice) {
      hardFailures.push(`Preis ${formatCurrency(actualPrice)} ueber Regelgrenze ${formatCurrency(matchedRule.maxPrice)}.`);
    }
  }

  if (matchedRule.minReviews > 0) {
    if (actualReviews === null) {
      reviewReasons.push(`Rezensionszahl fehlt, Regel verlangt mindestens ${matchedRule.minReviews}.`);
    } else if (actualReviews < matchedRule.minReviews) {
      hardFailures.push(`Bewertungen ${actualReviews} unter Mindestwert ${matchedRule.minReviews}.`);
    }
  }

  if (matchedRule.minRating > 0) {
    if (actualRating === null) {
      reviewReasons.push(`Sterne fehlen, Regel verlangt mindestens ${matchedRule.minRating.toFixed(1)}.`);
    } else if (actualRating < matchedRule.minRating) {
      hardFailures.push(`Bewertung ${actualRating.toFixed(1)} unter Mindestwert ${matchedRule.minRating.toFixed(1)}.`);
    }
  }

  if (matchedRule.marketCompareRequired && marketComparison.available !== true) {
    reviewReasons.push('Marktvergleich ist fuer diese Regel Pflicht, aber nicht verfuegbar.');
  }

  const allowed = hardFailures.length === 0 && reviewReasons.length === 0;
  const decision = allowed ? 'allow' : hardFailures.length > 0 ? 'block' : 'review';
  const primaryReason =
    hardFailures[0] || reviewReasons[0] || `Regel ${matchedRule.name} erlaubt den Deal innerhalb der Grenzen.`;
  const reasonCode =
    allowed === true ? 'PRODUCT_RULE_ALLOWED' : decision === 'block' ? 'PRODUCT_RULE_BLOCKED' : 'PRODUCT_RULE_REVIEW_REQUIRED';
  const summary =
    allowed === true
      ? `Regel ${matchedRule.name} erlaubt den Deal.`
      : decision === 'block'
        ? `Regel ${matchedRule.name} blockiert den Deal: ${primaryReason}`
        : `Regel ${matchedRule.name} verlangt Review: ${primaryReason}`;
  const reasonDetails =
    allowed === true
      ? []
      : buildReasonDetails(
          cleanText(matchedRule.name) ? `product_rule_${matchedRule.id || 'match'}` : 'product_rule',
          summary,
          decision === 'block' ? 'critical' : 'warning',
          {
            ruleId: matchedRule.id,
            ruleName: matchedRule.name,
            maxPrice: matchedRule.maxPrice,
            actualPrice,
            minReviews: matchedRule.minReviews,
            actualReviews,
            minRating: matchedRule.minRating,
            actualRating,
            marketCompareRequired: matchedRule.marketCompareRequired,
            marketCompareStatus: marketComparison.status
          }
        );

  logProductRule('PRODUCT_RULE_MATCHED', {
    scope: cleanText(deal.scope) || 'default',
    rule: matchedRule.name,
    price: actualPrice === null ? null : formatCurrency(actualPrice),
    maxPrice: matchedRule.maxPrice === null ? null : formatCurrency(matchedRule.maxPrice),
    brandType: selection.brandType,
    capacityMah: selection.capacityMah,
    matchedKeywords
  });

  if (allowed) {
    logProductRule('PRODUCT_RULE_ALLOWED', {
      scope: cleanText(deal.scope) || 'default',
      rule: matchedRule.name,
      price: actualPrice === null ? null : formatCurrency(actualPrice),
      maxPrice: matchedRule.maxPrice === null ? null : formatCurrency(matchedRule.maxPrice)
    });
  } else {
    logProductRule('PRODUCT_RULE_BLOCKED', {
      scope: cleanText(deal.scope) || 'default',
      rule: matchedRule.name,
      decision,
      reason: primaryReason,
      actualPrice: actualPrice === null ? null : formatCurrency(actualPrice),
      maxPrice: matchedRule.maxPrice === null ? null : formatCurrency(matchedRule.maxPrice)
    });
  }

  return {
    matchedRule,
    matchedRuleName: matchedRule.name,
    matchedKeywords,
    allowed,
    decision,
    reason: primaryReason,
    reasonCode,
    maxPrice: matchedRule.maxPrice,
    actualPrice,
    minReviews: matchedRule.minReviews,
    actualReviews,
    minRating: matchedRule.minRating,
    actualRating,
    marketCompareRequired: matchedRule.marketCompareRequired,
    marketCompareAvailable: marketComparison.available,
    marketCompareStatus: marketComparison.status,
    capacityMah: selection.capacityMah,
    brand: brand || '',
    brandType: selection.brandType,
    brandTypeLabel: PRODUCT_RULE_BRAND_LABEL[selection.brandType] || 'Unbekannt',
    status: 'matched',
    action: decision === 'allow' ? 'allow' : decision === 'review' ? 'review' : 'reject',
    summary,
    items: [
      {
        id: `product_rule_${matchedRule.id || matchedRule.name}`,
        label: matchedRule.name,
        action: decision,
        detail: primaryReason,
        meta: {
          maxPrice: matchedRule.maxPrice,
          actualPrice,
          minReviews: matchedRule.minReviews,
          actualReviews,
          minRating: matchedRule.minRating,
          actualRating,
          capacityMah: selection.capacityMah,
          brandType: selection.brandType
        }
      }
    ],
    reasonDetails
  };
}

export function listProductRules() {
  return db
    .prepare(
      `
        SELECT *
        FROM product_rules
        ORDER BY active DESC, updated_at DESC, name COLLATE NOCASE ASC
      `
    )
    .all()
    .map(mapProductRuleRow);
}

export function saveProductRule(input = {}, id = null) {
  const existingRow = id ? loadProductRuleRowById(id) : null;
  if (id && !existingRow) {
    throw new Error('Produkt-Regel wurde nicht gefunden.');
  }

  const existingRule = existingRow ? mapProductRuleRow(existingRow) : null;
  const payload = normalizeProductRulePayload(input, existingRule);
  const timestamp = nowIso();

  try {
    if (existingRule) {
      db.prepare(
        `
          UPDATE product_rules
          SET name = @name,
              keywords_json = @keywordsJson,
              brand_type = @brandType,
              max_price = @maxPrice,
              min_reviews = @minReviews,
              min_rating = @minRating,
              market_compare_required = @marketCompareRequired,
              capacity_min = @capacityMin,
              capacity_max = @capacityMax,
              active = @active,
              updated_at = @updatedAt
          WHERE id = @id
        `
      ).run({
        id: existingRule.id,
        name: payload.name,
        keywordsJson: JSON.stringify(payload.keywords),
        brandType: payload.brandType,
        maxPrice: payload.maxPrice,
        minReviews: payload.minReviews,
        minRating: payload.minRating,
        marketCompareRequired: payload.marketCompareRequired ? 1 : 0,
        capacityMin: payload.capacityMin,
        capacityMax: payload.capacityMax,
        active: payload.active ? 1 : 0,
        updatedAt: timestamp
      });

      return mapProductRuleRow(loadProductRuleRowById(existingRule.id));
    }

    const result = db
      .prepare(
        `
          INSERT INTO product_rules (
            name,
            keywords_json,
            brand_type,
            max_price,
            min_reviews,
            min_rating,
            market_compare_required,
            capacity_min,
            capacity_max,
            active,
            created_at,
            updated_at
          ) VALUES (
            @name,
            @keywordsJson,
            @brandType,
            @maxPrice,
            @minReviews,
            @minRating,
            @marketCompareRequired,
            @capacityMin,
            @capacityMax,
            @active,
            @createdAt,
            @updatedAt
          )
        `
      )
      .run({
        name: payload.name,
        keywordsJson: JSON.stringify(payload.keywords),
        brandType: payload.brandType,
        maxPrice: payload.maxPrice,
        minReviews: payload.minReviews,
        minRating: payload.minRating,
        marketCompareRequired: payload.marketCompareRequired ? 1 : 0,
        capacityMin: payload.capacityMin,
        capacityMax: payload.capacityMax,
        active: payload.active ? 1 : 0,
        createdAt: timestamp,
        updatedAt: timestamp
      });

    return mapProductRuleRow(loadProductRuleRowById(result.lastInsertRowid));
  } catch (error) {
    if (error instanceof Error && /UNIQUE/i.test(error.message)) {
      throw new Error('Eine Produkt-Regel mit diesem Namen existiert bereits.');
    }

    throw error;
  }
}

export function deleteProductRule(id) {
  const existing = loadProductRuleRowById(id);
  if (!existing) {
    throw new Error('Produkt-Regel wurde nicht gefunden.');
  }

  db.prepare(`DELETE FROM product_rules WHERE id = ?`).run(id);
  return mapProductRuleRow(existing);
}

export function seedDefaultProductRules() {
  const timestamp = nowIso();
  const insert = db.prepare(
    `
      INSERT OR IGNORE INTO product_rules (
        name,
        keywords_json,
        brand_type,
        max_price,
        min_reviews,
        min_rating,
        market_compare_required,
        capacity_min,
        capacity_max,
        active,
        created_at,
        updated_at
      ) VALUES (
        @name,
        @keywordsJson,
        @brandType,
        @maxPrice,
        @minReviews,
        @minRating,
        @marketCompareRequired,
        @capacityMin,
        @capacityMax,
        @active,
        @createdAt,
        @updatedAt
      )
    `
  );

  for (const rule of DEFAULT_PRODUCT_RULES) {
    insert.run({
      name: rule.name,
      keywordsJson: JSON.stringify(rule.keywords),
      brandType: rule.brandType,
      maxPrice: rule.maxPrice,
      minReviews: rule.minReviews,
      minRating: rule.minRating,
      marketCompareRequired: rule.marketCompareRequired ? 1 : 0,
      capacityMin: rule.capacityMin,
      capacityMax: rule.capacityMax,
      active: rule.active ? 1 : 0,
      createdAt: timestamp,
      updatedAt: timestamp
    });
  }
}

export const __testablesProductRules = {
  KNOWN_BRANDS,
  normalizeBrandType,
  normalizeSearchText,
  normalizeCompactText,
  resolveProductBrand,
  selectBestMatchingRule
};
