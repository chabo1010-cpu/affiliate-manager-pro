export const INTERNAL_WITHOUT_OPTIONS = '__WITHOUT_OPTIONS__';
export const WITHOUT_OPTIONS_LABEL = 'Ohne Optionen';
export const COUPON_OPTION_LABEL = '\u{1F3F7} Rabattgutschein:';
export const COUPON_LINE_PREFIX = '\u{1F3F7} Rabattgutschein:';
export const MASTER_PRIMARY_OPTIONS = [
  '\u2705 Coupon aktivieren',
  '\u2705 Werbeaktion aktivieren',
  '\u2705 Coupon + Werbeaktion aktivieren',
  '\u2139\uFE0F Automatischer Kassenrabatt',
  '\u26A1\uFE0F Blitzangebot',
  '\u23F0\uFE0F Zeitlich begrenztes Angebot',
  '\u{1F4C9} Spar-Abo einrichten (jederzeit kündbar)',
  '\u2139\uFE0F Ab 4 Stück nochmals 5% Ersparnis'
];
export const MASTER_EXTRA_OPTIONS = [
  '\u2139\uFE0F Verschiedene Größen und Farben',
  '\u2139\uFE0F Über ‚Andere Verkäufer‘ in den Warenkorb legen',
  '\u2139\uFE0F Verschiedene Ausführungen',
  '\u2139\uFE0F Lieferzeit beachten',
  '\u2139\uFE0F Zzgl. Pfand',
  COUPON_OPTION_LABEL,
  '\u2139\uFE0F Derzeit vorbestellbar',
  '\u2139\uFE0F Eventuell Verkäufer wechslen'
];
const OLD_PRICE_ICON = '\u{1F534}';
const PRICE_ICON = '\u{1F525}';
const LINK_ICON = '\u27A1\uFE0F';
const DEFAULT_PRIMARY_OPTION = INTERNAL_WITHOUT_OPTIONS;
const DEFAULT_FREE_TEXT = '\u2139\uFE0F';
const VALID_PRIMARY_OPTIONS = new Set(MASTER_PRIMARY_OPTIONS);

export const DEAL_IMAGE_RENDER = {
  width: 1200,
  height: 1200,
  fit: 'contain'
};

function cleanText(value) {
  return value?.trim() || '';
}

function escapeHtml(value) {
  return value
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function parsePriceValue(value) {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }

  const rawValue = cleanText(typeof value === 'string' ? value : '');
  if (!rawValue) {
    return null;
  }

  const sanitized = rawValue.replace(/[^\d.,-]/g, '');
  if (!sanitized) {
    return null;
  }

  const firstDecimalMatch = sanitized.match(/(\d+)[.,](\d{1,2})(?:[.,].*)?$/);
  if (firstDecimalMatch) {
    const parsed = Number.parseFloat(`${firstDecimalMatch[1]}.${firstDecimalMatch[2]}`);
    return Number.isFinite(parsed) ? parsed : null;
  }

  const lastDot = sanitized.lastIndexOf('.');
  const lastComma = sanitized.lastIndexOf(',');
  const decimalIndex = Math.max(lastDot, lastComma);

  if (decimalIndex > -1) {
    const integerPart = sanitized.slice(0, decimalIndex).replace(/[^\d-]/g, '');
    const decimalPart = sanitized.slice(decimalIndex + 1).replace(/[^\d]/g, '');
    const normalizedDecimalPart = decimalPart.slice(0, 2);
    const normalizedNumber = `${integerPart || '0'}${normalizedDecimalPart ? `.${normalizedDecimalPart}` : ''}`;
    const parsed = Number.parseFloat(normalizedNumber);
    return Number.isFinite(parsed) ? parsed : null;
  }

  const parsed = Number.parseFloat(sanitized.replace(/[^\d-]/g, ''));
  return Number.isFinite(parsed) ? parsed : null;
}

export function formatPrice(value) {
  const parsedValue = parsePriceValue(value);
  if (parsedValue === null) {
    return '';
  }

  const formattedValue = new Intl.NumberFormat('de-DE', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  }).format(parsedValue);
  const finalValue = `${formattedValue}€`;
  return finalValue;
}

function normalizePrice(value) {
  return formatPrice(value);
}

function normalizeFreeText(value) {
  const trimmed = cleanText(value);
  if (!trimmed || trimmed === DEFAULT_FREE_TEXT) {
    return '';
  }

  return trimmed;
}

function normalizePrimaryOptions(value) {
  if (Array.isArray(value)) {
    return value.map(cleanText).filter(Boolean);
  }

  const trimmed = cleanText(value);
  return trimmed ? [trimmed] : [];
}

const TELEGRAM_TITLE_MAX_LENGTH = 90;
const SHORT_TITLE_MAX_LENGTH = TELEGRAM_TITLE_MAX_LENGTH;
const SHORT_TITLE_TARGET_LENGTH = TELEGRAM_TITLE_MAX_LENGTH;
const TITLE_MODEL_WORDS = new Set(['cr0557', 'noos', 'dnm', 'dest']);
const TITLE_FILLER_WORDS = new Set([
  'the',
  'and',
  'with',
  'for',
  'neu',
  'new',
  'original',
  'premium',
  'official',
  'super',
  'ultra',
  'extra',
  'sale',
  'angebot'
]);
const TITLE_CONNECTOR_WORDS = new Set(['mit', 'oder', 'und', 'fuer', 'für', 'with', 'or', 'and']);
const COLOR_NORMALIZATIONS = [
  [/^medium\s+/i, ''],
  [/^light\s+/i, ''],
  [/^dark\s+/i, ''],
  [/\bdnm\b/gi, 'Denim'],
  [/\bblack\b/gi, 'Black'],
  [/\bblue\b/gi, 'Blue'],
  [/\bdenim\b/gi, 'Denim']
];
const PRODUCT_TYPE_RULES = [
  { pattern: /\b(?:kochgeschirr|topf|pfannen?)\s*-?\s*set\b/i, value: 'Kochgeschirr Set' },
  { pattern: /\bgeschenk(?:e|set)?\b|\bgeschenke\b/i, value: 'Geschenkset' },
  { pattern: /\b(?:armbanduhr|chronograph(?:en)?|uhr)\b/i, value: 'Uhr' },
  { pattern: /\b(?:akku\s*-?\s*)?bohrschrauber\b/i, value: 'Akku-Bohrschrauber' },
  { pattern: /\bpower\s*bank\b|\bpowerbank\b/i, value: 'Powerbank' },
  { pattern: /\b(?:kopfhoerer|kopfh[oö]rer|earbuds|in\s*ear|headphones)\b/i, value: 'Kopfhörer' },
  { pattern: /\b(?:usb\s*-?\s*c\s*)?hub\b/i, value: 'USB-C Hub' },
  { pattern: /\b(?:ladegeraet|ladeger[aä]t|charger)\b/i, value: 'Ladegerät' },
  { pattern: /\bkabel\b|\bcable\b/i, value: 'Kabel' },
  { pattern: /\bsmart\s*watch\b|\bsmartwatch\b/i, value: 'Smartwatch' },
  { pattern: /\bstaubsauger\b|\bvacuum\b/i, value: 'Staubsauger' },
  { pattern: /\bjeans\b|\bdenim\b/i, value: 'Jeans' },
  { pattern: /\bhose\b|\bpants\b|\btrousers\b/i, value: 'Hose' },
  { pattern: /\bjacke\b|\bjacket\b/i, value: 'Jacke' },
  { pattern: /\b(?:gaming\s*)?st[uü]hle?\b|\besszimmerst[uü]hle?\b|\bchair\b/i, value: 'Stuhl' },
  { pattern: /\bshirt\b|\bt-?shirt\b/i, value: 'Shirt' },
  { pattern: /\bkleid\b|\bdress\b/i, value: 'Kleid' },
  { pattern: /\bschuhe\b|\bsneaker\b|\bshoes\b/i, value: 'Sneaker' },
  { pattern: /\bkratzbaum\b|\bkatzenbaum\b/i, value: 'Kratzbaum' }
];
const FEATURE_RULES = [
  { pattern: /\bchronograph(?:en)?\b/i, value: 'Chronograph' },
  { pattern: /\bgaming\b/i, value: 'Gaming' },
  { pattern: /\bwide\s+leg\b/i, value: 'Wide Leg' },
  { pattern: /\bslim\s+fit\b/i, value: 'Slim Fit' },
  { pattern: /\bregular\s+fit\b/i, value: 'Regular Fit' },
  { pattern: /\bstraight\s+fit\b|\bstraight\b/i, value: 'Straight' },
  { pattern: /\bskinny\b/i, value: 'Skinny' },
  { pattern: /\bhigh\s+waist\b/i, value: 'High Waist' },
  { pattern: /\boversize(?:d)?\b/i, value: 'Oversize' },
  { pattern: /\banc\b|\bnoise\s+cancelling\b/i, value: 'ANC' },
  { pattern: /\b(?:pd|power\s+delivery)\b/i, value: 'PD' },
  { pattern: /\b\d{1,3}v(?:-\d{1,3})?\b/i },
  { pattern: /\b(?:\d{4,6})\s*mAh\b/i },
  { pattern: /\b\d{2,3}\s*w\b/i },
  { pattern: /\busb\s*-?\s*c\b/i, value: 'USB-C' }
];
const MATERIAL_FEATURE_RULES = [
  { pattern: /\bedelstahl\b/i, value: 'Edelstahl' },
  { pattern: /\blederband\b/i, value: 'Lederband' },
  { pattern: /\bleder\b/i, value: 'Leder' },
  { pattern: /\bholz\b/i, value: 'Holz' },
  { pattern: /\bmetall\b/i, value: 'Metall' },
  { pattern: /\bglas\b/i, value: 'Glas' },
  { pattern: /\bbaumwolle\b/i, value: 'Baumwolle' },
  { pattern: /\bwasserdicht\b|\bwasserabweisend\b/i, value: 'wasserabweisend' },
  { pattern: /\b(?:quarz|quartz)\b/i, value: 'Quarz' }
];

function normalizeTitleText(value) {
  return cleanText(value)
    .replace(/[()[\]{}]/g, ' ')
    .replace(/[|/]+/g, ', ')
    .replace(/\s+/g, ' ')
    .trim();
}

function toTitleCase(value) {
  return cleanText(value)
    .toLowerCase()
    .replace(/\b([a-zäöüß])/gi, (match) => match.toUpperCase())
    .replace(/\bUsb\b/g, 'USB')
    .replace(/\bPd\b/g, 'PD')
    .replace(/\bMah\b/g, 'mAh');
}

function toDisplayCase(value) {
  return cleanText(value)
    .toLowerCase()
    .replace(/\p{L}+/gu, (word) => `${word.charAt(0).toUpperCase()}${word.slice(1)}`)
    .replace(/\bUsb\b/g, 'USB')
    .replace(/\bPd\b/g, 'PD')
    .replace(/\bMah\b/g, 'mAh');
}

function isLikelyModelCode(word, index) {
  const normalized = cleanText(word).replace(/[^\w-]/g, '').toLowerCase();
  if (!normalized || index === 0) {
    return false;
  }

  if (/^\d{1,3}v(?:-\d{1,3})?$/i.test(normalized) || /^\d{1,4}(?:w|mah)$/i.test(normalized)) {
    return false;
  }

  if (TITLE_MODEL_WORDS.has(normalized) || normalized.startsWith('kog')) {
    return true;
  }

  return (
    /^[a-z]{1,4}\d{2,}[a-z0-9-]*$/i.test(normalized) ||
    /^\d{3,}[a-z0-9-]*$/i.test(normalized) ||
    (/^[a-z0-9-]{5,}$/i.test(normalized) && /\d/.test(normalized) && /[a-z]/i.test(normalized))
  );
}

function compactTitleWords(value) {
  const seen = new Set();
  return normalizeTitleText(value)
    .split(/\s+/)
    .filter((word, index) => {
      const normalized = word.replace(/[^\w-]/g, '').toLowerCase();
      if (!normalized || TITLE_FILLER_WORDS.has(normalized) || isLikelyModelCode(word, index)) {
        return false;
      }

      const dedupeKey = normalized.replace(/s$/, '');
      if (seen.has(dedupeKey)) {
        return false;
      }

      seen.add(dedupeKey);
      return true;
    })
    .join(' ');
}

function findFirstRuleValue(title, rules) {
  for (const rule of rules) {
    const match = title.match(rule.pattern);
    if (match) {
      return rule.value || match[0].replace(/\s+/g, '');
    }
  }

  return '';
}

function findRuleValues(title, rules, limit = 3) {
  const values = [];
  for (const rule of rules) {
    const match = title.match(rule.pattern);
    const value = match ? rule.value || match[0].replace(/\s+/g, '') : '';
    if (value && !values.some((entry) => entry.toLowerCase() === value.toLowerCase())) {
      values.push(value);
    }
    if (values.length >= limit) {
      break;
    }
  }

  return values;
}

function extractBrand(title) {
  const firstSegment = normalizeTitleText(title).split(',')[0] || title;
  const firstWord = cleanText(firstSegment.split(/\s+/)[0]);
  return firstWord ? firstWord.replace(/[^\p{L}\p{N}&.-]/gu, '') : '';
}

function extractAudience(title) {
  if (/\b(?:kids?|kinder|girls?|boys?)\b/i.test(title)) {
    return 'Kids';
  }

  if (/\bdamen\b|\bwomen'?s\b/i.test(title)) {
    return 'Damen';
  }

  if (/\bherren\b|\bmen'?s\b/i.test(title)) {
    return 'Herren';
  }

  return '';
}

function extractGiftRecipient(title) {
  if (/\b(?:mama|mutter|muttertag)\b/i.test(title)) {
    return 'für Mama';
  }

  if (/\b(?:papa|vater|vatertag)\b/i.test(title)) {
    return 'für Papa';
  }

  return '';
}

function extractGiftFeature(title) {
  const capacity = title.match(/\b\d{2,4}\s*ml\b/i)?.[0]?.replace(/\s+/g, ' ');
  const hasCoffeeCup = /\bkaffeetassen?\b/i.test(title);
  const hasCup = /\bbecher\b/i.test(title);

  if (capacity && (hasCoffeeCup || hasCup)) {
    return `${capacity} Kaffeetasse & Becher`;
  }

  if (hasCoffeeCup && hasCup) {
    return 'Kaffeetasse & Becher';
  }

  return '';
}

function extractProductLine(title, brand = '', productType = '') {
  const firstSegment = compactTitleWords(normalizeTitleText(title).split(',')[0] || title);
  const tokens = firstSegment.split(/\s+/).filter(Boolean);
  const normalizedBrand = cleanText(brand).toLowerCase();
  const normalizedProductType = cleanText(productType).toLowerCase();
  const skipWords = new Set([
    normalizedBrand,
    normalizedProductType,
    'damen',
    'herren',
    'kids',
    'kinder',
    'girl',
    'girls',
    'boy',
    'boys',
    'armbanduhr',
    'uhr',
    'hose',
    'jeans',
    'stuhl',
    'chronograph',
    'quarz',
    'uhrwerk'
  ]);

  for (const [index, token] of tokens.entries()) {
    const normalized = token.replace(/[^\w-]/g, '').toLowerCase();
    if (
      !normalized ||
      skipWords.has(normalized) ||
      TITLE_FILLER_WORDS.has(normalized) ||
      TITLE_CONNECTOR_WORDS.has(normalized) ||
      isLikelyModelCode(token, index)
    ) {
      continue;
    }

    if (/^[a-zäöüß][a-zäöüß-]{2,}$/i.test(token)) {
      return toDisplayCase(token);
    }
  }

  return '';
}

function extractSize(segments) {
  for (const [index, segment] of segments.entries()) {
    const explicitSize = segment.match(/\b(?:gr(?:\.|(?:o|\u00f6)sse)?|size)\s*(\d{2,3})\b/i);
    if (explicitSize) {
      return `Gr. ${explicitSize[1]}`;
    }

    const numericSize = index > 0 ? segment.match(/^\s*(\d{2,3})\s*$/i) : null;
    if (numericSize) {
      return `Gr. ${numericSize[1]}`;
    }

    const alphaSize = index > 0 ? segment.match(/^\s*(XXS|XS|S|M|L|XL|XXL|3XL|4XL|5XL)\s*$/i) : null;
    if (alphaSize) {
      return `Gr. ${alphaSize[1].toUpperCase()}`;
    }
  }

  return '';
}

function extractColor(segments) {
  const variantSegments = segments.slice(1);
  for (const segment of variantSegments) {
    if (/\b(?:gr(?:\.|(?:o|\u00f6)sse)?|size)?\s*\d{2,3}\b/i.test(segment)) {
      continue;
    }

    if (/\b(?:black|blue|denim|white|grey|gray|red|green|beige|brown|pink|purple|orange|yellow|schwarz|blau|weiss|wei\u00df|grau|rot|gruen|gr\u00fcn|braun)\b/i.test(segment)) {
      let color = compactTitleWords(segment);
      COLOR_NORMALIZATIONS.forEach(([pattern, replacement]) => {
        color = color.replace(pattern, replacement);
      });
      return toDisplayCase(color).replace(/\s+/g, ' ').trim();
    }
  }

  return '';
}

function compressFeatureParts(values = []) {
  const normalizedValues = values.map(cleanText).filter(Boolean);
  const hasEdelstahl = normalizedValues.some((value) => value.toLowerCase() === 'edelstahl');
  const hasLederband = normalizedValues.some((value) => value.toLowerCase() === 'lederband');
  const compacted = [];

  normalizedValues.forEach((value) => {
    if (value.toLowerCase() === 'leder' && hasLederband) {
      return;
    }
    if (value.toLowerCase() === 'quarz') {
      return;
    }
    if (!compacted.some((entry) => entry.toLowerCase() === value.toLowerCase())) {
      compacted.push(value);
    }
  });

  if (hasEdelstahl && hasLederband) {
    return compacted
      .filter((value) => !['edelstahl', 'lederband'].includes(value.toLowerCase()))
      .concat('Edelstahl/Lederband');
  }

  return compacted;
}

function enforceTelegramTitleLength(title, maxLength = TELEGRAM_TITLE_MAX_LENGTH) {
  const normalizedTitle = cleanText(title).replace(/\s+/g, ' ');
  if (normalizedTitle.length <= maxLength) {
    return normalizedTitle;
  }

  const protectedMatch = normalizedTitle.match(/^(.*?\b(?:Uhr|Hose|Jeans|Stuhl|Powerbank|Kopfhörer|KopfhÃ¶rer|Ladegerät|LadegerÃ¤t|Kabel|Smartwatch|Staubsauger)\b)(.*)$/i);
  const protectedPrefix = cleanText(protectedMatch?.[1] || '');
  const remainder = cleanText(protectedMatch?.[2] || '');

  if (protectedPrefix && protectedPrefix.length < maxLength - 8) {
    const remainingSpace = maxLength - protectedPrefix.length - 3;
    const shortenedRemainder = smartTrimTitle(remainder, remainingSpace);
    const rebuilt = cleanText(`${protectedPrefix} – ${shortenedRemainder}`);
    if (rebuilt.length <= maxLength && shortenedRemainder) {
      return rebuilt;
    }
  }

  const ellipsisMaxLength = Math.max(20, maxLength - 3);
  return `${smartTrimTitle(normalizedTitle, ellipsisMaxLength)}...`;
}

function smartTrimTitle(title, maxLength = SHORT_TITLE_MAX_LENGTH) {
  const trimmed = cleanText(title);
  if (trimmed.length <= maxLength) {
    return trimmed;
  }

  const cutAt = trimmed.lastIndexOf(' ', maxLength);
  const safeCutAt = cutAt > 40 ? cutAt : maxLength;
  return trimmed.slice(0, safeCutAt).replace(/[,\s-]+$/g, '').trim();
}

function appendUniqueTitlePart(parts, value) {
  const trimmed = cleanText(value);
  if (!trimmed) {
    return;
  }

  const normalized = trimmed.toLowerCase();
  if (parts.some((part) => part.toLowerCase() === normalized)) {
    return;
  }

  parts.push(trimmed);
}

function buildTelegramTitleInternal(originalTitle, maxLength = TELEGRAM_TITLE_MAX_LENGTH) {
  const normalizedTitle = normalizeTitleText(originalTitle);
  if (!normalizedTitle) {
    return '';
  }

  const segments = normalizedTitle.split(/\s*,\s*/).filter(Boolean);
  const brand = extractBrand(normalizedTitle);
  const audience = extractAudience(normalizedTitle);
  const productType = findFirstRuleValue(normalizedTitle, PRODUCT_TYPE_RULES);
  const productLine = extractProductLine(normalizedTitle, brand, productType);
  const giftRecipient = extractGiftRecipient(normalizedTitle);
  const keyFeature = extractGiftFeature(normalizedTitle) || findFirstRuleValue(normalizedTitle, FEATURE_RULES);
  const materialFeatures = compressFeatureParts(findRuleValues(normalizedTitle, MATERIAL_FEATURE_RULES, 3));
  const size = extractSize(segments);
  const color = extractColor(segments);
  const titleParts = [];
  const normalizedProductType = productType.toLowerCase();
  const productLineAllowed = ['uhr', 'akku-bohrschrauber'].includes(normalizedProductType);
  const keyFeatureInSuffix = ['uhr', 'geschenkset'].includes(normalizedProductType) && Boolean(keyFeature);
  const hasStructuredTitleSignal = Boolean(productType || keyFeature || materialFeatures.length || size || color);

  appendUniqueTitlePart(titleParts, brand);
  if (productLineAllowed) {
    appendUniqueTitlePart(titleParts, productLine);
  }
  appendUniqueTitlePart(titleParts, audience);
  appendUniqueTitlePart(titleParts, productType);
  if (normalizedProductType === 'geschenkset') {
    appendUniqueTitlePart(titleParts, giftRecipient);
  }
  if (!keyFeatureInSuffix) {
    appendUniqueTitlePart(titleParts, keyFeature);
  }

  let shortTitle = hasStructuredTitleSignal && titleParts.length >= 2 ? titleParts.join(' ') : compactTitleWords(normalizedTitle);
  const suffixParts = [
    ...(keyFeatureInSuffix ? [keyFeature] : []),
    ...materialFeatures
  ].filter(
    (value) => !shortTitle.toLowerCase().includes(value.toLowerCase())
  );

  if (suffixParts.length) {
    shortTitle = `${shortTitle} \u2013 ${suffixParts.join(' ')}`;
  }

  if (size && !shortTitle.includes(`(${size})`)) {
    shortTitle = `${shortTitle} (${size})`;
  }

  if (color && !shortTitle.toLowerCase().includes(color.toLowerCase())) {
    shortTitle = `${shortTitle} \u2013 ${color}`;
  }

  if (shortTitle.length > maxLength) {
    const withoutColor = color ? shortTitle.replace(new RegExp(`\\s+\\u2013\\s+${color.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i'), '') : shortTitle;
    shortTitle = withoutColor.length <= maxLength ? withoutColor : withoutColor;
  }

  return enforceTelegramTitleLength(shortTitle, maxLength);
}

export function buildTelegramTitle(originalTitle, maxLength = TELEGRAM_TITLE_MAX_LENGTH) {
  try {
    return buildTelegramTitleInternal(originalTitle, maxLength);
  } catch (error) {
    console.warn('[PIPELINE_ERROR_CONTINUED]', {
      stage: 'buildTelegramTitle',
      error: error instanceof Error ? error.message : 'Titel konnte nicht gekuerzt werden.'
    });
    return enforceTelegramTitleLength(compactTitleWords(originalTitle) || cleanText(originalTitle), maxLength);
  }
}

export function buildShortTitle(originalTitle) {
  return buildTelegramTitle(originalTitle);
}

export function hasRealFreeText(value) {
  return normalizeFreeText(value).length > 0;
}

export function hasValidPrimaryOption(value) {
  return normalizePrimaryOptions(value).some(
    (option) => option === WITHOUT_OPTIONS_LABEL || VALID_PRIMARY_OPTIONS.has(option)
  );
}

export function hasValidCouponOnlyCase(isCouponActive, couponCode) {
  return isCouponActive && cleanText(couponCode).length > 0;
}

export function hasEffectivePostQualifier(
  primaryOption,
  freeText,
  isCouponActive = false,
  couponCode
) {
  return (
    hasValidPrimaryOption(primaryOption) ||
    hasRealFreeText(freeText) ||
    hasValidCouponOnlyCase(isCouponActive, couponCode)
  );
}

export function getOrderedSelectedOptions(config) {
  const lines = [];
  const primaryOptions = normalizePrimaryOptions(config.textBaustein);
  const couponCode = cleanText(config.rabattgutscheinCode);

  primaryOptions.forEach((option) => {
    if (option && option !== DEFAULT_PRIMARY_OPTION && option !== WITHOUT_OPTIONS_LABEL) {
      lines.push(option);
    }
  });

  for (const option of config.extraOptions || []) {
    const trimmedOption = cleanText(option);
    if (!trimmedOption) {
      continue;
    }

    if (trimmedOption === COUPON_OPTION_LABEL) {
      if (couponCode) {
        lines.push(`${COUPON_LINE_PREFIX} ${couponCode}`);
      }
      continue;
    }

    lines.push(trimmedOption);
  }

  return lines;
}

function buildStructuredPost(config) {
  const originalTitle = cleanText(config.productTitle) || 'Amazon Produkt';
  const title = buildTelegramTitle(originalTitle) || originalTitle;
  const oldPrice = normalizePrice(config.alterPreis);
  const price = normalizePrice(config.neuerPreis);
  const oldPriceLabel = cleanText(config.alterPreisLabel) || 'Statt';
  const priceLabel = cleanText(config.neuerPreisLabel) || 'Jetzt';
  const link = cleanText(config.amazonLink);
  const optionLines = getOrderedSelectedOptions(config);
  const freeText = normalizeFreeText(config.freiText);

  const lines = [{ kind: 'title', value: title }, { kind: 'blank' }];

  if (oldPrice) {
    lines.push({ kind: 'oldPrice', label: oldPriceLabel, price: oldPrice });
  }

  if (price) {
    lines.push({ kind: 'price', label: priceLabel, price });
  }

  if (link) {
    lines.push({ kind: 'link', value: link });
  }

  optionLines.forEach((value) => {
    lines.push({ kind: 'text', value });
  });

  if (freeText) {
    lines.push({ kind: 'text', value: freeText });
  }

  lines.push({ kind: 'blank' }, { kind: 'blank' }, { kind: 'footer', value: 'Anzeige/Partnerlink' });
  return lines;
}

export function buildPostLines(config) {
  return buildStructuredPost(config).map((line) => {
    switch (line.kind) {
      case 'title':
      case 'text':
      case 'footer':
      case 'link':
        return line.value;
      case 'oldPrice':
        return `${OLD_PRICE_ICON} ${line.label} ${line.price}`;
      case 'price':
        return `${PRICE_ICON} ${line.label} ${line.price}`;
      case 'blank':
        return '';
    }
  });
}

function renderLine(line, channel) {
  if (channel === 'telegram') {
    switch (line.kind) {
      case 'title':
        return `<b>${escapeHtml(line.value)}</b>`;
      case 'oldPrice':
        return `${OLD_PRICE_ICON} ${escapeHtml(line.label)} <b>${escapeHtml(line.price)}</b>`;
      case 'price':
        return `${PRICE_ICON} ${escapeHtml(line.label)} <b>${escapeHtml(line.price)}</b>`;
      case 'link':
        return `${LINK_ICON} <b>${escapeHtml(line.value)}</b>`;
      case 'text':
        return escapeHtml(line.value);
      case 'footer':
        return '<i>Anzeige/Partnerlink</i>';
      case 'blank':
        return '';
    }
  }

  switch (line.kind) {
    case 'title':
      return `*${line.value}*`;
    case 'oldPrice':
      return `${OLD_PRICE_ICON} ${line.label} *${line.price}*`;
    case 'price':
      return `${PRICE_ICON} ${line.label} *${line.price}*`;
    case 'link':
      return `${LINK_ICON} *${line.value}*`;
    case 'text':
      return line.value;
    case 'footer':
      return '_Anzeige/Partnerlink_';
    case 'blank':
      return '';
  }
}

function buildBaseLines(config, channel) {
  return buildStructuredPost(config).map((line) => renderLine(line, channel));
}

export function generatePostText(config) {
  const telegramCaption = buildBaseLines(config, 'telegram').join('\n');
  const whatsappText = buildBaseLines(config, 'whatsapp').join('\n');
  const rabattgutscheinCode = cleanText(config.rabattgutscheinCode);
  const originalProductTitle = cleanText(config.productTitle) || 'Amazon Produkt';

  return {
    telegramCaption,
    whatsappText,
    couponFollowUp: rabattgutscheinCode,
    productTitle: originalProductTitle,
    shortProductTitle: buildTelegramTitle(originalProductTitle) || originalProductTitle,
    telegramTitle: buildTelegramTitle(originalProductTitle) || originalProductTitle
  };
}

export function normalizeDealImageUrl(imageUrl) {
  const trimmed = cleanText(imageUrl);
  if (!trimmed) {
    return '';
  }

  if (/images-amazon\.com|media-amazon\.com|ssl-images-amazon/i.test(trimmed)) {
    return trimmed.replace(/\._[^.]+_\./, `._SL${DEAL_IMAGE_RENDER.width}_.`);
  }

  return trimmed;
}

export function resolveDealImageUrlFromScrape(scrapeData = {}) {
  const imageCandidates = [
    scrapeData.imageUrl || '',
    scrapeData.image || '',
    scrapeData.productImage || '',
    scrapeData.previewImage || '',
    scrapeData.thumbnail || '',
    Array.isArray(scrapeData.images) ? scrapeData.images[0] || '' : '',
    scrapeData.product?.imageUrl || ''
  ];

  for (const imageCandidate of imageCandidates) {
    const normalizedImageUrl = normalizeDealImageUrl(imageCandidate || '');
    if (normalizedImageUrl) {
      return normalizedImageUrl;
    }
  }

  return '';
}

export function copyToClipboard(text) {
  return navigator.clipboard
    .writeText(text)
    .then(() => true)
    .catch(() => false);
}

export function formatForTelegram(text) {
  return text;
}

export function formatForWhatsApp(text) {
  return text;
}

export function downloadAsText(text, filename = 'post.txt') {
  const element = document.createElement('a');
  element.setAttribute('href', `data:text/plain;charset=utf-8,${encodeURIComponent(text)}`);
  element.setAttribute('download', filename);
  element.style.display = 'none';
  document.body.appendChild(element);
  element.click();
  document.body.removeChild(element);
}
