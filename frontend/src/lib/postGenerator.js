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
  const title = cleanText(config.productTitle) || 'Amazon Produkt';
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

  return {
    telegramCaption,
    whatsappText,
    couponFollowUp: rabattgutscheinCode,
    productTitle: cleanText(config.productTitle) || 'Amazon Produkt'
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
