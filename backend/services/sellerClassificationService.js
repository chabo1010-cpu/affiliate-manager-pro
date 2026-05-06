function cleanText(value) {
  return typeof value === 'string' ? value.trim() : '';
}

export const SELLER_CLASS = {
  AMAZON_DIRECT: 'AMAZON_DIRECT',
  FBA_THIRDPARTY: 'FBA_THIRDPARTY',
  FBM_THIRDPARTY: 'FBM_THIRDPARTY',
  UNKNOWN: 'UNKNOWN'
};

export const UNKNOWN_SELLER_MODES = {
  REVIEW: 'review',
  BLOCK: 'block'
};

const DIRECT_AMAZON_COMBINED_PATTERNS = [
  {
    key: 'verkauf_und_versand_durch_amazon',
    regex: /verkauf(?:t)? und versand(?:et)? durch amazon(?:\.de)?/
  },
  {
    key: 'verkauf_und_versand_amazon',
    regex: /verkauf(?:t)? und versand amazon(?:\.de)?/
  },
  {
    key: 'sold_and_shipped_by_amazon',
    regex: /sold and shipped by amazon(?:\.de)?/
  },
  {
    key: 'sold_by_and_ships_from_amazon',
    regex: /sold by amazon(?:\.de)?[\s,:;/-]+(?:and )?(?:ships|shipped|dispatched|fulfilled) (?:from|by) amazon(?:\.de)?/
  },
  {
    key: 'ships_from_then_sold_by_amazon',
    regex: /ships from amazon(?:\.de)?[\s,:;/-]+(?:and )?sold by amazon(?:\.de)?/
  },
  {
    key: 'sold_by_and_fulfilled_by_amazon',
    regex: /sold by amazon(?:\.de)?[\s,:;/-]+(?:and )?fulfilled by amazon(?:\.de)?/
  }
];
const BUYBOX_COMBINED_AMAZON_PATTERNS = [
  {
    key: 'versender_verkaufer_amazon',
    regex: /versender(?:\s+(?:und|and))?\s+verk(?:ae|a)ufer\s+amazon(?:\.de)?/
  },
  {
    key: 'verkaufer_versender_amazon',
    regex: /verk(?:ae|a)ufer(?:\s+(?:und|and))?\s+versender\s+amazon(?:\.de)?/
  },
  {
    key: 'versender_amazon_short',
    regex: /^versender\s+amazon(?:\.de)?$/
  },
  {
    key: 'verkaufer_amazon_short',
    regex: /^verk(?:ae|a)ufer\s+amazon(?:\.de)?$/
  }
];
const BUYBOX_COMBINED_THIRDPARTY_PATTERNS = [
  {
    key: 'versender_verkaufer_drittanbieter',
    regex: /versender(?:\s+(?:und|and))?\s+verk(?:ae|a)ufer\s+(?!amazon(?:\.de)?(?:\s|$))[^.,;\n]+/
  },
  {
    key: 'verkaufer_versender_drittanbieter',
    regex: /verk(?:ae|a)ufer(?:\s+(?:und|and))?\s+versender\s+(?!amazon(?:\.de)?(?:\s|$))[^.,;\n]+/
  }
];
const SOLD_BY_AMAZON_PATTERNS = [
  {
    key: 'verkauf_durch_amazon',
    regex: /verkauf(?:t)? durch amazon(?:\.de)?/
  },
  {
    key: 'verkauft_von_amazon',
    regex: /verkauft von amazon(?:\.de)?/
  },
  {
    key: 'verkaeufer_amazon',
    regex: /verk(?:ae|a)ufer\s*:?\s*amazon(?:\.de)?/
  },
  {
    key: 'seller_amazon',
    regex: /seller\s*:?\s*amazon(?:\.de)?/
  },
  {
    key: 'sold_by_amazon',
    regex: /sold by amazon(?:\.de)?/
  },
  {
    key: 'amazon_eu_sarl',
    regex: /amazon eu s\.?a\.?r\.?l/
  }
];
const SHIPPED_BY_AMAZON_PATTERNS = [
  {
    key: 'versand_durch_amazon',
    regex: /versand durch amazon(?:\.de)?/
  },
  {
    key: 'versendet_von_amazon',
    regex: /versendet von amazon(?:\.de)?/
  },
  {
    key: 'versand_amazon',
    regex: /versand amazon(?:\.de)?/
  },
  {
    key: 'fulfilled_by_amazon',
    regex: /fulfilled by amazon(?:\.de)?/
  },
  {
    key: 'ships_from_amazon',
    regex: /ships from amazon(?:\.de)?/
  },
  {
    key: 'shipped_by_amazon',
    regex: /shipped by amazon(?:\.de)?/
  },
  {
    key: 'dispatched_from_amazon',
    regex: /dispatched from amazon(?:\.de)?/
  },
  {
    key: 'dispatches_from_amazon',
    regex: /dispatches from amazon(?:\.de)?/
  }
];
const SOLD_BY_THIRDPARTY_PATTERNS = [
  {
    key: 'verkauf_durch_drittanbieter',
    regex: /verkauf(?:t)? durch\s+(?!amazon(?:\.de)?(?:\s|$))[^.,;\n]+/
  },
  {
    key: 'verkauft_von_drittanbieter',
    regex: /verkauft von\s+(?!amazon(?:\.de)?(?:\s|$))[^.,;\n]+/
  },
  {
    key: 'verkaeufer_drittanbieter',
    regex: /verk(?:ae|a)ufer(?:\s*:)?\s+(?!amazon(?:\.de)?(?:\s|$))[^.,;\n]+/
  },
  {
    key: 'sold_by_drittanbieter',
    regex: /sold by\s+(?!amazon(?:\.de)?(?:\s|$))[^.,;\n]+/
  },
  {
    key: 'seller_drittanbieter',
    regex: /seller(?:\s*:)?\s+(?!amazon(?:\.de)?(?:\s|$))[^.,;\n]+/
  }
];
const SHIPPED_BY_THIRDPARTY_PATTERNS = [
  {
    key: 'versand_durch_drittanbieter',
    regex: /versand durch\s+(?!amazon(?:\.de)?(?:\s|$))[^.,;\n]+/
  },
  {
    key: 'versendet_von_drittanbieter',
    regex: /versendet von\s+(?!amazon(?:\.de)?(?:\s|$))[^.,;\n]+/
  },
  {
    key: 'ships_from_drittanbieter',
    regex: /ships from\s+(?!amazon(?:\.de)?(?:\s|$))[^.,;\n]+/
  },
  {
    key: 'shipped_by_drittanbieter',
    regex: /shipped by\s+(?!amazon(?:\.de)?(?:\s|$))[^.,;\n]+/
  },
  {
    key: 'dispatched_from_drittanbieter',
    regex: /dispatched from\s+(?!amazon(?:\.de)?(?:\s|$))[^.,;\n]+/
  },
  {
    key: 'dispatches_from_drittanbieter',
    regex: /dispatches from\s+(?!amazon(?:\.de)?(?:\s|$))[^.,;\n]+/
  }
];
const SELLER_SOURCE_AMAZON_ONLY_ALLOWLIST = ['seller-profile', 'merchant-info', 'buybox', 'tabular-buybox', 'offer-display'];
const COMBINED_SELLER_SHIPPING_SOURCE_HINTS = ['buybox', 'tabular-buybox', 'offer-display', 'desktop-buybox', 'fallback-text'];
const COMBINED_SELLER_SHIPPING_DETECTION_SOURCE = 'combined-seller-shipping-text';
const SELLER_LOG_TEXT_LIMIT = 160;

function parseSellerBoolean(value) {
  if (value === true || value === false) {
    return value;
  }

  if (value === 1 || value === '1' || value === 'true') {
    return true;
  }

  if (value === 0 || value === '0' || value === 'false') {
    return false;
  }

  return null;
}

function normalizeSellerTextForMatching(value = '') {
  return cleanText(String(value || ''))
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/ß/g, 'ss')
    .replace(/\u00df/g, 'ss')
    .replace(/\u00a0/g, ' ')
    .replace(/[/:|]+/g, ' ')
    .replace(/[()[\],;]+/g, ' ')
    .replace(/[&+]+/g, ' und ')
    .replace(/-/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

function collectMatchedPatternKeys(value = '', patterns = []) {
  return patterns.filter((entry) => entry.regex.test(value)).map((entry) => entry.key);
}

function uniqueCleanValues(values = []) {
  return values
    .map((value) => cleanText(value))
    .filter(Boolean)
    .filter((value, index, allValues) => allValues.indexOf(value) === index);
}

function logSellerDetection(tag, payload = {}) {
  try {
    console.info(`[${tag}]`, payload);
  } catch {
    console.info(`[${tag}]`);
  }
}

function buildSellerSourceAvailability({ input = {}, sellerDetails = {}, detectionSources = [], primaryDetectionSource = '' } = {}) {
  const normalizedDetectionSources = uniqueCleanValues([primaryDetectionSource, ...(Array.isArray(detectionSources) ? detectionSources : [])]).map(
    (value) => cleanText(value).toLowerCase()
  );
  const hasDetectionSource = (patterns = []) =>
    normalizedDetectionSources.some((entry) => patterns.some((pattern) => entry.includes(pattern)));
  const sellerDetailsMerchantText = cleanText(sellerDetails.merchantText);
  const sellerDetailsRawText = cleanText(
    sellerDetails.rawText ||
      sellerDetails.scrapeText ||
      sellerDetails.buyboxText ||
      sellerDetails.offerText ||
      sellerDetails.offerDisplayText
  );
  const sellerRawText = cleanText(input.sellerRawText);
  const explicitTelegramText = cleanText(input.telegramText || sellerDetails.telegramText);

  return {
    paapiMerchantInfo: {
      checked: hasDetectionSource(['paapi']) || Boolean(cleanText(input.paapiMerchantInfo || sellerDetails.paapiMerchantInfo)),
      source: 'paapi',
      value: cleanText(
        input.paapiMerchantInfo ||
          sellerDetails.paapiMerchantInfo ||
          sellerDetails.paapiMerchant ||
          sellerDetails.paapiMerchantName ||
          (hasDetectionSource(['paapi']) ? sellerDetailsMerchantText : '')
      )
    },
    offerMerchantInfo: {
      checked:
        hasDetectionSource(['offer-display', 'buybox', 'tabular-buybox', 'desktop-buybox']) ||
        Boolean(cleanText(input.offerMerchantInfo || sellerDetails.offerMerchantInfo)),
      source: 'offer_merchant_info',
      value: cleanText(
        input.offerMerchantInfo ||
          sellerDetails.offerMerchantInfo ||
          sellerDetails.offerMerchant ||
          sellerDetails.offerMerchantName ||
          (hasDetectionSource(['offer-display', 'buybox', 'tabular-buybox', 'desktop-buybox'])
            ? sellerDetailsMerchantText
            : '')
      )
    },
    keepaSellerInfo: {
      checked: hasDetectionSource(['keepa']) || Boolean(cleanText(input.keepaSellerInfo || sellerDetails.keepaSellerInfo)),
      source: 'keepa_seller_info',
      value: cleanText(
        input.keepaSellerInfo || sellerDetails.keepaSellerInfo || sellerDetails.keepaSeller || sellerDetails.keepaMerchant
      )
    },
    scrapeText: {
      checked:
        hasDetectionSource(['scrape', 'buybox', 'offer-display', 'tabular-buybox', 'desktop-buybox']) ||
        Boolean(cleanText(input.scrapeText || sellerDetailsRawText)),
      source: 'scrape_text',
      value: cleanText(input.scrapeText || sellerDetailsRawText)
    },
    telegramText: {
      checked: hasDetectionSource(['telegram']) || Boolean(explicitTelegramText),
      source: 'telegram_text',
      value: cleanText(explicitTelegramText || (hasDetectionSource(['telegram']) ? sellerRawText : ''))
    },
    sellerRawText: {
      checked: Boolean(sellerRawText),
      source: cleanText(primaryDetectionSource) || 'seller_raw_text',
      value: sellerRawText
    },
    merchantText: {
      checked: Boolean(cleanText(input.merchantText || sellerDetailsMerchantText)),
      source: cleanText(primaryDetectionSource || sellerDetails.detectionSource) || 'merchant_text',
      value: cleanText(input.merchantText || sellerDetailsMerchantText)
    }
  };
}

function buildSellerTextCandidates({ input = {}, sellerDetails = {}, primaryDetectionSource = '', sourceAvailability = {} } = {}) {
  const rawCandidates = [
    {
      field: 'paapiMerchantInfo',
      detectionSource: sourceAvailability.paapiMerchantInfo?.source || 'paapi',
      value: sourceAvailability.paapiMerchantInfo?.value || ''
    },
    {
      field: 'offerMerchantInfo',
      detectionSource: sourceAvailability.offerMerchantInfo?.source || 'offer_merchant_info',
      value: sourceAvailability.offerMerchantInfo?.value || ''
    },
    {
      field: 'keepaSellerInfo',
      detectionSource: sourceAvailability.keepaSellerInfo?.source || 'keepa_seller_info',
      value: sourceAvailability.keepaSellerInfo?.value || ''
    },
    {
      field: 'scrapeText',
      detectionSource: sourceAvailability.scrapeText?.source || 'scrape_text',
      value: sourceAvailability.scrapeText?.value || ''
    },
    {
      field: 'telegramText',
      detectionSource: sourceAvailability.telegramText?.source || 'telegram_text',
      value: sourceAvailability.telegramText?.value || ''
    },
    {
      field: 'merchantText',
      detectionSource: cleanText(primaryDetectionSource || sellerDetails.detectionSource) || 'merchant_text',
      value: cleanText(input.merchantText || sellerDetails.merchantText)
    },
    {
      field: 'sellerRawText',
      detectionSource: cleanText(primaryDetectionSource).includes('telegram')
        ? 'telegram_message'
        : cleanText(primaryDetectionSource) || 'seller_raw_text',
      value: cleanText(input.sellerRawText)
    }
  ];
  const seen = new Set();

  return rawCandidates
    .map((candidate) => ({
      field: cleanText(candidate.field) || 'merchantText',
      detectionSource: cleanText(candidate.detectionSource) || 'unknown',
      value: cleanText(candidate.value)
    }))
    .filter((candidate) => candidate.value)
    .filter((candidate) => {
      const key = `${candidate.detectionSource}::${candidate.value}`;
      if (seen.has(key)) {
        return false;
      }

      seen.add(key);
      return true;
    });
}

function mergeSellerSignalCandidates(candidateAnalyses = []) {
  const soldByAmazon = candidateAnalyses.some(
    (candidate) => candidate.signals?.hasCombinedAmazonMatch === true || candidate.signals?.soldByAmazon === true
  )
    ? true
    : candidateAnalyses.some((candidate) => candidate.signals?.soldByAmazon === false)
      ? false
      : null;
  const shippedByAmazon = candidateAnalyses.some(
    (candidate) => candidate.signals?.hasCombinedAmazonMatch === true || candidate.signals?.shippedByAmazon === true
  )
    ? true
    : candidateAnalyses.some((candidate) => candidate.signals?.shippedByAmazon === false)
      ? false
      : null;
  const matchedPatterns = uniqueCleanValues(candidateAnalyses.flatMap((candidate) => candidate.signals?.matchedPatterns || []));
  const matchedDirectAmazonPatterns = uniqueCleanValues(
    candidateAnalyses.flatMap((candidate) => candidate.signals?.matchedDirectAmazonPatterns || [])
  );
  const selectedCandidate =
    candidateAnalyses.find(
      (candidate) =>
        candidate.signals?.hasCombinedAmazonMatch === true ||
        candidate.signals?.soldByAmazon !== null ||
        candidate.signals?.shippedByAmazon !== null ||
        (candidate.signals?.matchedPatterns || []).length > 0
    ) ||
    candidateAnalyses[0] ||
    null;

  return {
    soldByAmazon,
    shippedByAmazon,
    matchedPatterns,
    matchedDirectAmazonPatterns,
    hasCombinedAmazonMatch: candidateAnalyses.some((candidate) => candidate.signals?.hasCombinedAmazonMatch === true),
    merchantText: selectedCandidate?.signals?.merchantText || '',
    detectionSource: selectedCandidate?.signals?.detectionSource || '',
    rawDetectionSource: selectedCandidate?.signals?.rawDetectionSource || ''
  };
}

function collectCheckedSellerSources(sourceAvailability = {}) {
  return Object.entries(sourceAvailability)
    .filter(([, entry]) => entry && entry.checked === true)
    .map(([field, entry]) => ({
      field,
      source: cleanText(entry.source) || field,
      hasValue: Boolean(cleanText(entry.value)),
      valuePreview: cleanText(entry.value).slice(0, SELLER_LOG_TEXT_LIMIT)
    }));
}

function collectMissingSellerFields({
  sourceAvailability = {},
  inputSoldByAmazon = null,
  inputShippedByAmazon = null,
  derivedSoldByAmazon = null,
  derivedShippedByAmazon = null
} = {}) {
  const missingFields = [];

  if (inputSoldByAmazon === null && derivedSoldByAmazon === null) {
    missingFields.push('soldByAmazon');
  }
  if (inputShippedByAmazon === null && derivedShippedByAmazon === null) {
    missingFields.push('shippedByAmazon');
  }

  for (const [field, entry] of Object.entries(sourceAvailability)) {
    if (entry?.checked === true && !cleanText(entry.value)) {
      missingFields.push(field);
    }
  }

  return uniqueCleanValues(missingFields);
}

function resolveUnknownSellerReason({
  sellerClass = '',
  isNonAmazonDeal = false,
  sourceAvailability = {},
  candidateAnalyses = [],
  matchedPatterns = [],
  rawSoldByAmazon = null,
  rawShippedByAmazon = null,
  missingFields = []
} = {}) {
  if (sellerClass !== SELLER_CLASS.UNKNOWN || isNonAmazonDeal === true) {
    return '';
  }

  if (candidateAnalyses.length === 0 && rawSoldByAmazon === null && rawShippedByAmazon === null) {
    return 'no_seller_inputs_available';
  }
  if (sourceAvailability.paapiMerchantInfo?.checked === true && !cleanText(sourceAvailability.paapiMerchantInfo?.value)) {
    return 'paapi_merchant_info_missing';
  }
  if (sourceAvailability.offerMerchantInfo?.checked === true && !cleanText(sourceAvailability.offerMerchantInfo?.value)) {
    return 'offer_merchant_info_missing';
  }
  if (sourceAvailability.keepaSellerInfo?.checked === true && !cleanText(sourceAvailability.keepaSellerInfo?.value)) {
    return 'keepa_seller_info_missing';
  }
  if (sourceAvailability.scrapeText?.checked === true && !cleanText(sourceAvailability.scrapeText?.value)) {
    return 'scrape_text_missing';
  }
  if (sourceAvailability.telegramText?.checked === true && !cleanText(sourceAvailability.telegramText?.value)) {
    return 'telegram_text_missing';
  }
  if (rawSoldByAmazon === true && rawShippedByAmazon === false) {
    return 'sold_by_amazon_with_third_party_shipping_conflict';
  }
  if (rawSoldByAmazon === true && rawShippedByAmazon === null) {
    return 'amazon_seller_detected_shipping_missing';
  }
  if (rawSoldByAmazon === false && rawShippedByAmazon === null) {
    return 'third_party_seller_detected_shipping_missing';
  }
  if (rawSoldByAmazon === null && rawShippedByAmazon === true) {
    return 'shipping_amazon_detected_seller_missing';
  }
  if (rawSoldByAmazon === null && rawShippedByAmazon === false) {
    return 'shipping_third_party_detected_seller_missing';
  }
  if ((matchedPatterns || []).length === 0) {
    return 'seller_text_without_supported_patterns';
  }
  if (missingFields.length > 0) {
    return `missing_fields:${missingFields.join(',')}`;
  }

  return 'insufficient_seller_signals';
}

function sourceSupportsCombinedSellerShippingDetection(detectionSource = '') {
  return COMBINED_SELLER_SHIPPING_SOURCE_HINTS.some((entry) => detectionSource.includes(entry));
}

function collectCombinedAmazonPatternKeys(normalizedMerchantText = '', detectionSource = '') {
  const directPatternMatches = collectMatchedPatternKeys(normalizedMerchantText, DIRECT_AMAZON_COMBINED_PATTERNS);

  if (sourceSupportsCombinedSellerShippingDetection(detectionSource) !== true) {
    return directPatternMatches;
  }

  return uniqueCleanValues([
    ...directPatternMatches,
    ...collectMatchedPatternKeys(normalizedMerchantText, BUYBOX_COMBINED_AMAZON_PATTERNS)
  ]);
}

function collectCombinedThirdPartyPatternKeys(normalizedMerchantText = '', detectionSource = '') {
  if (sourceSupportsCombinedSellerShippingDetection(detectionSource) !== true) {
    return [];
  }

  return collectMatchedPatternKeys(normalizedMerchantText, BUYBOX_COMBINED_THIRDPARTY_PATTERNS);
}

function normalizeLegacySellerType(value = '') {
  const normalized = cleanText(String(value || '')).toUpperCase();

  if (normalized === 'AMAZON') {
    return 'AMAZON';
  }

  if (normalized === 'FBA' || normalized === 'AFN') {
    return 'FBA';
  }

  if (normalized === 'FBM') {
    return 'FBM';
  }

  if (normalized === 'UNKNOWN') {
    return 'UNKNOWN';
  }

  if (normalized === SELLER_CLASS.AMAZON_DIRECT) {
    return 'AMAZON';
  }

  if (normalized === SELLER_CLASS.FBA_THIRDPARTY) {
    return 'FBA';
  }

  if (normalized === SELLER_CLASS.FBM_THIRDPARTY) {
    return 'FBM';
  }

  return 'UNKNOWN';
}

export function formatSellerBoolean(value) {
  const normalized = parseSellerBoolean(value);
  if (normalized === true) {
    return 'ja';
  }

  if (normalized === false) {
    return 'nein';
  }

  return 'unbekannt';
}

export function normalizeUnknownSellerMode(value = '') {
  const normalized = cleanText(String(value || '')).toLowerCase();
  return normalized === UNKNOWN_SELLER_MODES.BLOCK ? UNKNOWN_SELLER_MODES.BLOCK : UNKNOWN_SELLER_MODES.REVIEW;
}

export function normalizeSellerClass(value = '') {
  const normalized = cleanText(String(value || '')).toUpperCase();

  if (normalized === SELLER_CLASS.AMAZON_DIRECT || normalized === 'AMAZON') {
    return SELLER_CLASS.AMAZON_DIRECT;
  }

  if (normalized === SELLER_CLASS.FBA_THIRDPARTY || normalized === 'FBA' || normalized === 'AFN') {
    return SELLER_CLASS.FBA_THIRDPARTY;
  }

  if (normalized === SELLER_CLASS.FBM_THIRDPARTY || normalized === 'FBM') {
    return SELLER_CLASS.FBM_THIRDPARTY;
  }

  return SELLER_CLASS.UNKNOWN;
}

function isVerifiedAmazonCatalogSource(value = '') {
  const normalized = cleanText(value).toLowerCase();

  return (
    normalized.includes('paapi') ||
    normalized.includes('creator') ||
    normalized.includes('amazon_creator_api') ||
    normalized.includes('amazon-creator-api')
  );
}

function looksLikeVerifiedAmazonMerchantName(value = '') {
  const normalized = normalizeSellerTextForMatching(value);

  return (
    normalized === 'amazon' ||
    normalized === 'amazon.de' ||
    normalized === 'amazon de' ||
    normalized === 'amazon eu sarl' ||
    normalized === 'amazon eu s.a.r.l.' ||
    normalized === 'amazon services europe' ||
    /^amazon(?:\.[a-z.]+)?$/.test(normalized) ||
    /amazon eu s\.?a\.?r\.?l/.test(normalized)
  );
}

function hasVerifiedAmazonMerchantHint({ sourceAvailability = {}, detectionSources = [], merchantText = '' } = {}) {
  const sourceBackedAmazonName =
    looksLikeVerifiedAmazonMerchantName(merchantText) && detectionSources.some((source) => isVerifiedAmazonCatalogSource(source));
  const paapiMerchantValue = sourceAvailability.paapiMerchantInfo?.value || '';
  const merchantSourceValue = sourceAvailability.merchantText?.value || '';
  const merchantSource = sourceAvailability.merchantText?.source || '';

  return (
    sourceBackedAmazonName ||
    (sourceAvailability.paapiMerchantInfo?.checked === true && looksLikeVerifiedAmazonMerchantName(paapiMerchantValue)) ||
    (isVerifiedAmazonCatalogSource(merchantSource) && looksLikeVerifiedAmazonMerchantName(merchantSourceValue))
  );
}

export function sellerClassToLegacySellerType(value = '') {
  const sellerClass = normalizeSellerClass(value);

  if (sellerClass === SELLER_CLASS.AMAZON_DIRECT) {
    return 'AMAZON';
  }

  if (sellerClass === SELLER_CLASS.FBA_THIRDPARTY) {
    return 'FBA';
  }

  if (sellerClass === SELLER_CLASS.FBM_THIRDPARTY) {
    return 'FBM';
  }

  return 'UNKNOWN';
}

export function extractSellerSignalsFromText(value = '', options = {}) {
  const merchantText = cleanText(value)
    .replace(/\s+/g, ' ')
    .trim();
  const rawDetectionSource = cleanText(options.detectionSource) || 'text';

  if (!merchantText) {
    return {
      soldByAmazon: null,
      shippedByAmazon: null,
      detectionSource: rawDetectionSource || 'missing_text',
      rawDetectionSource: rawDetectionSource || 'missing_text',
      merchantText: '',
      normalizedMerchantText: '',
      soldSignal: 'missing',
      shippedSignal: 'missing',
      matchedPatterns: [],
      matchedDirectAmazonPatterns: [],
      matchedSoldAmazonPatterns: [],
      matchedShippedAmazonPatterns: [],
      matchedSoldThirdPartyPatterns: [],
      matchedShippedThirdPartyPatterns: [],
      hasAmazonDirectPhrase: false,
      hasCombinedAmazonMatch: false
    };
  }

  const normalizedMerchantText = normalizeSellerTextForMatching(merchantText);
  const matchedDirectAmazonPatterns = collectCombinedAmazonPatternKeys(normalizedMerchantText, rawDetectionSource);
  const matchedCombinedThirdPartyPatterns = collectCombinedThirdPartyPatternKeys(normalizedMerchantText, rawDetectionSource);
  const matchedSoldAmazonPatterns = collectMatchedPatternKeys(normalizedMerchantText, SOLD_BY_AMAZON_PATTERNS);
  const matchedShippedAmazonPatterns = collectMatchedPatternKeys(normalizedMerchantText, SHIPPED_BY_AMAZON_PATTERNS);
  const matchedSoldThirdPartyPatterns = collectMatchedPatternKeys(normalizedMerchantText, SOLD_BY_THIRDPARTY_PATTERNS);
  const matchedShippedThirdPartyPatterns = collectMatchedPatternKeys(normalizedMerchantText, SHIPPED_BY_THIRDPARTY_PATTERNS);
  const amazonOnlySellerNameMatch =
    SELLER_SOURCE_AMAZON_ONLY_ALLOWLIST.some((entry) => rawDetectionSource.includes(entry)) &&
    /^amazon(?:\.de)?(?:\s|$)/.test(normalizedMerchantText);
  const hasCombinedAmazonMatch = matchedDirectAmazonPatterns.length > 0;
  const detectionSource = hasCombinedAmazonMatch ? COMBINED_SELLER_SHIPPING_DETECTION_SOURCE : rawDetectionSource;

  if (amazonOnlySellerNameMatch) {
    matchedSoldAmazonPatterns.push('amazon_only_seller_name');
  }

  const soldByAmazon = matchedDirectAmazonPatterns.length > 0 || matchedSoldAmazonPatterns.length > 0;
  const soldByThirdParty = matchedCombinedThirdPartyPatterns.length > 0 || matchedSoldThirdPartyPatterns.length > 0;
  const shippedByAmazon = matchedDirectAmazonPatterns.length > 0 || matchedShippedAmazonPatterns.length > 0;
  const shippedByThirdParty = matchedCombinedThirdPartyPatterns.length > 0 || matchedShippedThirdPartyPatterns.length > 0;
  const matchedPatterns = uniqueCleanValues([
    ...matchedDirectAmazonPatterns,
    ...matchedCombinedThirdPartyPatterns,
    ...matchedSoldAmazonPatterns,
    ...matchedShippedAmazonPatterns,
    ...matchedSoldThirdPartyPatterns,
    ...matchedShippedThirdPartyPatterns
  ]);

  return {
    soldByAmazon: soldByAmazon ? true : soldByThirdParty ? false : null,
    shippedByAmazon: shippedByAmazon ? true : shippedByThirdParty ? false : null,
    detectionSource,
    rawDetectionSource,
    merchantText,
    normalizedMerchantText,
    soldSignal: soldByAmazon ? 'amazon' : soldByThirdParty ? 'third_party' : 'unknown',
    shippedSignal: shippedByAmazon ? 'amazon' : shippedByThirdParty ? 'third_party' : 'unknown',
    matchedPatterns,
    matchedDirectAmazonPatterns: uniqueCleanValues(matchedDirectAmazonPatterns),
    matchedSoldAmazonPatterns: uniqueCleanValues(matchedSoldAmazonPatterns),
    matchedShippedAmazonPatterns: uniqueCleanValues(matchedShippedAmazonPatterns),
    matchedSoldThirdPartyPatterns: uniqueCleanValues(matchedSoldThirdPartyPatterns),
    matchedShippedThirdPartyPatterns: uniqueCleanValues(matchedShippedThirdPartyPatterns),
    hasAmazonDirectPhrase: hasCombinedAmazonMatch,
    hasCombinedAmazonMatch
  };
}

export function resolveSellerIdentity(input = {}) {
  const explicitSellerClass = cleanText(input.sellerClass);
  const explicitSellerType = cleanText(input.sellerType);
  const sellerDetailsSource =
    input.sellerDetails && typeof input.sellerDetails === 'object'
      ? input.sellerDetails
      : input.details && typeof input.details === 'object'
        ? input.details
        : {};
  const sellerDetails = sellerDetailsSource;
  const primaryDetectionSource = cleanText(input.sellerDetectionSource || input.detectionSource || sellerDetails.detectionSource);
  const inputSoldByAmazon = parseSellerBoolean(input.soldByAmazon);
  const inputShippedByAmazon = parseSellerBoolean(input.shippedByAmazon);
  const sourceAvailability = buildSellerSourceAvailability({
    input,
    sellerDetails,
    detectionSources: [
      ...(Array.isArray(input.detectionSources) ? input.detectionSources : []),
      ...(Array.isArray(sellerDetails.detectionSources) ? sellerDetails.detectionSources : [])
    ],
    primaryDetectionSource
  });
  const textCandidateAnalyses = buildSellerTextCandidates({
    input,
    sellerDetails,
    primaryDetectionSource,
    sourceAvailability
  }).map((candidate) => {
    const signals = extractSellerSignalsFromText(candidate.value, {
      detectionSource: candidate.detectionSource || 'unknown'
    });

    if (
      signals.hasCombinedAmazonMatch === true ||
      signals.soldByAmazon !== null ||
      signals.shippedByAmazon !== null ||
      (signals.matchedPatterns || []).length > 0
    ) {
      logSellerDetection('SELLER_TEXT_MATCH', {
        field: candidate.field,
        detectionSource: candidate.detectionSource || 'unknown',
        soldByAmazon: signals.soldByAmazon,
        shippedByAmazon: signals.shippedByAmazon,
        soldSignal: signals.soldSignal,
        shippedSignal: signals.shippedSignal,
        matchedPatterns: signals.matchedPatterns || [],
        textPreview: signals.merchantText.slice(0, SELLER_LOG_TEXT_LIMIT)
      });
    }

    return {
      ...candidate,
      signals
    };
  });
  const derivedSignals = mergeSellerSignalCandidates(textCandidateAnalyses);
  const merchantText = cleanText(
    derivedSignals.merchantText || input.merchantText || input.sellerRawText || sellerDetails.merchantText || sourceAvailability.scrapeText?.value
  );
  const detectionSources = uniqueCleanValues([
    ...(Array.isArray(input.detectionSources) ? input.detectionSources : []),
    ...(Array.isArray(sellerDetails.detectionSources) ? sellerDetails.detectionSources : []),
    input.sellerDetectionSource,
    input.detectionSource,
    sellerDetails.detectionSource,
    derivedSignals.rawDetectionSource,
    derivedSignals.detectionSource,
    ...textCandidateAnalyses.map((candidate) => candidate.detectionSource)
  ]);
  const verifiedAmazonMerchantHint = hasVerifiedAmazonMerchantHint({
    sourceAvailability,
    detectionSources,
    merchantText
  });
  if (verifiedAmazonMerchantHint) {
    logSellerDetection('SELLER_VERIFIED_AMAZON_MERCHANT_HINT', {
      detectionSource: primaryDetectionSource || 'unknown',
      detectionSources,
      merchantTextPreview: merchantText ? merchantText.slice(0, SELLER_LOG_TEXT_LIMIT) : ''
    });
  }
  const rawSoldByAmazon =
    inputSoldByAmazon !== null ? inputSoldByAmazon : derivedSignals.soldByAmazon ?? (verifiedAmazonMerchantHint ? true : null);
  const rawShippedByAmazon =
    inputShippedByAmazon !== null ? inputShippedByAmazon : derivedSignals.shippedByAmazon ?? (verifiedAmazonMerchantHint ? true : null);
  const matchedPatterns = uniqueCleanValues([
    ...(Array.isArray(input.matchedPatterns) ? input.matchedPatterns : []),
    ...(Array.isArray(sellerDetails.matchedPatterns) ? sellerDetails.matchedPatterns : []),
    ...(Array.isArray(derivedSignals.matchedPatterns) ? derivedSignals.matchedPatterns : [])
  ]);
  const matchedDirectAmazonPatterns = uniqueCleanValues([
    ...(Array.isArray(input.matchedDirectAmazonPatterns) ? input.matchedDirectAmazonPatterns : []),
    ...(Array.isArray(sellerDetails.matchedDirectAmazonPatterns) ? sellerDetails.matchedDirectAmazonPatterns : []),
    ...(Array.isArray(derivedSignals.matchedDirectAmazonPatterns) ? derivedSignals.matchedDirectAmazonPatterns : [])
  ]);
  const dealType = cleanText(input.dealType || sellerDetails.dealType).toUpperCase();
  const isNonAmazonDeal = dealType === 'NON_AMAZON' || input.isAmazonDeal === false || sellerDetails.isAmazonDeal === false;
  const hasCombinedAmazonMatch =
    input.hasCombinedAmazonMatch === true ||
    sellerDetails.hasCombinedAmazonMatch === true ||
    detectionSources.includes(COMBINED_SELLER_SHIPPING_DETECTION_SOURCE) ||
    derivedSignals.hasCombinedAmazonMatch === true;
  const sellerClassFromFlags =
    rawSoldByAmazon === true && rawShippedByAmazon === true
      ? SELLER_CLASS.AMAZON_DIRECT
      : rawSoldByAmazon === false && rawShippedByAmazon === true
        ? SELLER_CLASS.FBA_THIRDPARTY
        : rawSoldByAmazon === false && rawShippedByAmazon === false
          ? SELLER_CLASS.FBM_THIRDPARTY
          : rawSoldByAmazon === true && rawShippedByAmazon === false
            ? SELLER_CLASS.UNKNOWN
            : null;
  const normalizedExplicitSellerClass = explicitSellerClass ? normalizeSellerClass(explicitSellerClass) : '';
  const sellerClass =
    sellerClassFromFlags ||
    (normalizedExplicitSellerClass && normalizedExplicitSellerClass !== SELLER_CLASS.UNKNOWN
      ? normalizedExplicitSellerClass
      : normalizeSellerClass(explicitSellerType || ''));
  const sellerType =
    sellerClassFromFlags || (normalizedExplicitSellerClass && normalizedExplicitSellerClass !== SELLER_CLASS.UNKNOWN)
      ? sellerClassToLegacySellerType(sellerClass)
      : normalizeLegacySellerType(explicitSellerType || '');
  const soldByAmazon =
    rawSoldByAmazon !== null
      ? rawSoldByAmazon
      : sellerClass === SELLER_CLASS.AMAZON_DIRECT
        ? true
        : sellerClass === SELLER_CLASS.FBA_THIRDPARTY || sellerClass === SELLER_CLASS.FBM_THIRDPARTY
          ? false
          : null;
  const shippedByAmazon =
    rawShippedByAmazon !== null
      ? rawShippedByAmazon
      : sellerClass === SELLER_CLASS.AMAZON_DIRECT || sellerClass === SELLER_CLASS.FBA_THIRDPARTY
        ? true
        : sellerClass === SELLER_CLASS.FBM_THIRDPARTY
          ? false
          : null;
  const checkedSources = collectCheckedSellerSources(sourceAvailability);
  const missingFields = collectMissingSellerFields({
    sourceAvailability,
    inputSoldByAmazon,
    inputShippedByAmazon,
    derivedSoldByAmazon: derivedSignals.soldByAmazon,
    derivedShippedByAmazon: derivedSignals.shippedByAmazon
  });
  const unknownReason = resolveUnknownSellerReason({
    sellerClass,
    isNonAmazonDeal,
    sourceAvailability,
    candidateAnalyses: textCandidateAnalyses,
    matchedPatterns,
    rawSoldByAmazon,
    rawShippedByAmazon,
    missingFields
  });

  logSellerDetection('SELLER_DETECTION_INPUTS', {
    sellerClassInput: explicitSellerClass || '',
    sellerTypeInput: explicitSellerType || '',
    detectionSource: primaryDetectionSource || 'unknown',
    detectionSources,
    merchantTextPreview: merchantText ? merchantText.slice(0, SELLER_LOG_TEXT_LIMIT) : '',
    soldByAmazonInput: inputSoldByAmazon,
    shippedByAmazonInput: inputShippedByAmazon,
    soldByAmazonDerived: derivedSignals.soldByAmazon ?? null,
    shippedByAmazonDerived: derivedSignals.shippedByAmazon ?? null,
    verifiedAmazonMerchantHint,
    matchedPatterns,
    matchedDirectAmazonPatterns,
    hasCombinedAmazonMatch,
    checkedSources,
    textCandidateCount: textCandidateAnalyses.length,
    resolvedSellerClass: sellerClass,
    resolvedSellerType: sellerType
  });

  if (missingFields.length > 0) {
    logSellerDetection('SELLER_FIELD_MISSING', {
      detectionSource: primaryDetectionSource || 'unknown',
      detectionSources,
      checkedSources,
      missingFields,
      reason:
        sellerClass === SELLER_CLASS.UNKNOWN
          ? 'Seller-Erkennung hat nicht alle benoetigten Felder gefunden.'
          : 'Seller-Erkennung konnte den Seller trotz fehlender Einzelfelder aufloesen.'
    });
  }

  logSellerDetection('SELLER_CLASS_RESOLVED', {
    detectionSource: primaryDetectionSource || 'unknown',
    detectionSources,
    sellerClass,
    sellerType,
    soldByAmazon,
    shippedByAmazon,
    matchedPatterns,
    matchedDirectAmazonPatterns,
    hasCombinedAmazonMatch,
    checkedSources,
    unknownReason: unknownReason || ''
  });

  if (unknownReason) {
    logSellerDetection('SELLER_UNKNOWN_REASON', {
      detectionSource: primaryDetectionSource || 'unknown',
      detectionSources,
      merchantTextPreview: merchantText ? merchantText.slice(0, SELLER_LOG_TEXT_LIMIT) : '',
      soldByAmazon,
      shippedByAmazon,
      matchedPatterns,
      missingFields,
      checkedSources,
      reason: unknownReason
    });
  }

  return {
    sellerClass,
    sellerType,
    soldByAmazon,
    shippedByAmazon,
    isAmazonDirect: sellerClass === SELLER_CLASS.AMAZON_DIRECT,
    isFbaThirdParty: sellerClass === SELLER_CLASS.FBA_THIRDPARTY,
    isFbmThirdParty: sellerClass === SELLER_CLASS.FBM_THIRDPARTY,
    isUnknown: sellerClass === SELLER_CLASS.UNKNOWN,
    isNonAmazonDeal,
    details: {
      ...sellerDetails,
      dealType: isNonAmazonDeal ? 'NON_AMAZON' : 'AMAZON',
      isAmazonDeal: !isNonAmazonDeal,
      soldByAmazonLabel: formatSellerBoolean(soldByAmazon),
      shippedByAmazonLabel: formatSellerBoolean(shippedByAmazon),
      detectionSource: primaryDetectionSource || detectionSources.join(' + ') || 'unknown',
      detectionSources,
      merchantText,
      matchedPatterns,
      matchedDirectAmazonPatterns,
      hasCombinedAmazonMatch,
      checkedSources,
      missingFields,
      unknownReason,
      recognitionMessage: sellerClass === SELLER_CLASS.UNKNOWN ? 'Seller konnte nicht erkannt werden.' : ''
    }
  };
}

function resolveStageAllowance({
  stage,
  sellerProfile,
  allowAmazonDirect,
  stageAmazonDirectOnly,
  allowFba,
  allowFbm
}) {
  if (sellerProfile.isNonAmazonDeal) {
    return {
      allowed: true,
      code: `${stage}_non_amazon`,
      reason: `${stage === 'market_compare' ? 'Marktvergleich' : 'KI'} erlaubt: Non-Amazon-Deal nutzt Titel-, Preis- und Link-Kontext.`
    };
  }

  if (sellerProfile.isUnknown) {
    return {
      allowed: false,
      code: `${stage}_unknown_seller`,
      reason: `${stage === 'market_compare' ? 'Marktvergleich' : 'KI'} blockiert: Verkaeufer oder Versand unbekannt.`
    };
  }

  if (sellerProfile.isAmazonDirect) {
    if (allowAmazonDirect === false) {
      return {
        allowed: false,
        code: `${stage}_amazon_direct_disabled`,
        reason: `${stage === 'market_compare' ? 'Marktvergleich' : 'KI'} fuer Amazon Direct ist deaktiviert.`
      };
    }

    return {
      allowed: true,
      code: `${stage}_amazon_direct`,
      reason: `${stage === 'market_compare' ? 'Marktvergleich' : 'KI'} erlaubt: Verkauf und Versand durch Amazon bestaetigt.`
    };
  }

  if (sellerProfile.isFbaThirdParty) {
    if (allowFba === true) {
      return {
        allowed: true,
        code: `${stage}_fba_override`,
        reason: `${stage === 'market_compare' ? 'Marktvergleich' : 'KI'} erlaubt: FBA-Drittanbieter ist explizit freigegeben.`
      };
    }

    if (stageAmazonDirectOnly === false) {
      return {
        allowed: true,
        code: `${stage}_fba_general`,
        reason: `${stage === 'market_compare' ? 'Marktvergleich' : 'KI'} erlaubt: Amazon-Direct-Pflicht ist deaktiviert.`
      };
    }

    return {
      allowed: false,
      code: `${stage}_fba_blocked`,
      reason: `${stage === 'market_compare' ? 'Marktvergleich' : 'KI'} blockiert: FBA-Drittanbieter ist nicht freigegeben.`
    };
  }

  if (sellerProfile.isFbmThirdParty) {
    if (allowFbm === true) {
      return {
        allowed: true,
        code: `${stage}_fbm_override`,
        reason: `${stage === 'market_compare' ? 'Marktvergleich' : 'KI'} erlaubt: FBM ist explizit freigegeben.`
      };
    }

    if (stageAmazonDirectOnly === false) {
      return {
        allowed: true,
        code: `${stage}_fbm_general`,
        reason: `${stage === 'market_compare' ? 'Marktvergleich' : 'KI'} erlaubt: Amazon-Direct-Pflicht ist deaktiviert.`
      };
    }

    return {
      allowed: false,
      code: `${stage}_fbm_blocked`,
      reason: `${stage === 'market_compare' ? 'Marktvergleich' : 'KI'} blockiert: FBM ist nicht freigegeben.`
    };
  }

  return {
    allowed: false,
    code: `${stage}_unknown`,
    reason: `${stage === 'market_compare' ? 'Marktvergleich' : 'KI'} blockiert: Seller-Klasse ist unklar.`
  };
}

export function evaluateSellerDecisionPolicy(settings = {}, sellerInput = {}) {
  const sellerProfile = resolveSellerIdentity(sellerInput);
  const quality = settings.quality || {};
  const marketComparison = resolveStageAllowance({
    stage: 'market_compare',
    sellerProfile,
    allowAmazonDirect: quality.marketCompareAmazonDirectEnabled !== false,
    stageAmazonDirectOnly: quality.marketCompareAmazonDirectOnly !== false,
    allowFba: quality.allowFbaThirdPartyMarketCompare === true,
    allowFbm: quality.allowFbmMarketCompare === true
  });
  const ai = resolveStageAllowance({
    stage: 'ai',
    sellerProfile,
    allowAmazonDirect: (settings.ai || {}).amazonDirectEnabled !== false,
    stageAmazonDirectOnly: quality.aiAmazonDirectOnly !== false,
    allowFba: quality.allowFbaThirdPartyAi === true,
    allowFbm: quality.allowFbmAi === true
  });
  const unknownSellerMode = normalizeUnknownSellerMode(quality.unknownSellerMode);

  return {
    seller: sellerProfile,
    marketComparison,
    ai,
    unknownSellerMode,
    unknownSellerAction:
      sellerProfile.isNonAmazonDeal === true
        ? 'pass'
        : sellerProfile.isUnknown === true
        ? unknownSellerMode === UNKNOWN_SELLER_MODES.BLOCK
          ? 'block'
          : 'review'
        : 'pass'
  };
}

export function classifySellerType(input = {}) {
  return resolveSellerIdentity(input).sellerType;
}

export function normalizeSellerType(value = '') {
  return normalizeLegacySellerType(value);
}
