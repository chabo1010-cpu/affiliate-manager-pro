const CHANNELS = ['telegram', 'whatsapp', 'facebook'];
const STANDARD_IMAGE_SOURCES = new Set(['standard', 'upload', 'none']);
const FACEBOOK_IMAGE_SOURCES = new Set(['standard', 'upload', 'none', 'link_preview']);
const DIRECT_IMPLEMENTED_CHANNELS = new Set(['telegram']);
const HTML_ENTITY_MAP = {
  '&nbsp;': ' ',
  '&amp;': '&',
  '&quot;': '"',
  '&#39;': "'",
  '&lt;': '<',
  '&gt;': '>'
};

function cleanText(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function decodeHtmlEntities(text) {
  return Object.entries(HTML_ENTITY_MAP).reduce((value, [entity, replacement]) => value.replaceAll(entity, replacement), text);
}

function normalizeImageSource(value, fallback, allowedValues) {
  const normalizedValue = cleanText(value).toLowerCase();

  if (!normalizedValue || !allowedValues.has(normalizedValue)) {
    return fallback;
  }

  return normalizedValue;
}

function getChannelLabel(channel) {
  if (channel === 'telegram') {
    return 'Telegram';
  }

  if (channel === 'whatsapp') {
    return 'WhatsApp';
  }

  if (channel === 'facebook') {
    return 'Facebook';
  }

  return channel;
}

function buildFinalTextPreview(text) {
  const normalized = normalizeFinalText(text);
  if (!normalized) {
    return '';
  }

  return normalized.length > 160 ? `${normalized.slice(0, 157)}...` : normalized;
}

export function parseGeneratorBoolean(value, fallback = false) {
  if (value === undefined || value === null || value === '') {
    return fallback;
  }

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

export function parseGeneratorJsonObject(value, fallback = {}) {
  if (!value) {
    return fallback;
  }

  if (typeof value === 'object' && !Array.isArray(value)) {
    return value;
  }

  if (typeof value !== 'string') {
    return fallback;
  }

  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : fallback;
  } catch {
    return fallback;
  }
}

export function normalizeFinalText(text) {
  const rawText = typeof text === 'string' ? text : '';

  return decodeHtmlEntities(rawText.replace(/<[^>]*>/g, ' '))
    .replace(/[\u0000-\u001F\u007F-\u009F\u200B-\u200D\uFEFF]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

export function normalizeGeneratorInput(input = {}) {
  const parsedTextByChannel = parseGeneratorJsonObject(input.textByChannel, {});
  const textByChannel = CHANNELS.reduce((accumulator, channel) => {
    const value = parsedTextByChannel[channel];
    accumulator[channel] = typeof value === 'string' ? value : '';
    return accumulator;
  }, {});

  return {
    title: cleanText(input.title),
    link: cleanText(input.link),
    normalizedUrl: cleanText(input.normalizedUrl),
    asin: cleanText(input.asin).toUpperCase(),
    sellerType: cleanText(input.sellerType) || 'FBM',
    sellerClass: cleanText(input.sellerClass),
    soldByAmazon: input.soldByAmazon ?? null,
    shippedByAmazon: input.shippedByAmazon ?? null,
    currentPrice: input.currentPrice ?? '',
    oldPrice: input.oldPrice ?? '',
    couponCode: cleanText(input.couponCode),
    textByChannel,
    generatedImagePath: cleanText(input.generatedImagePath),
    uploadedImagePath: typeof input.uploadedImagePath === 'string' ? input.uploadedImagePath : '',
    uploadedImageFile: input.uploadedImageFile || null,
    telegramImageSource: normalizeImageSource(input.telegramImageSource, 'standard', STANDARD_IMAGE_SOURCES),
    whatsappImageSource: normalizeImageSource(input.whatsappImageSource, 'standard', STANDARD_IMAGE_SOURCES),
    facebookImageSource: normalizeImageSource(input.facebookImageSource, 'link_preview', FACEBOOK_IMAGE_SOURCES),
    enableTelegram: parseGeneratorBoolean(input.enableTelegram, true),
    enableWhatsapp: parseGeneratorBoolean(input.enableWhatsapp, false),
    enableFacebook: parseGeneratorBoolean(input.enableFacebook, false)
  };
}

export function getEnabledGeneratorChannels(input = {}) {
  return CHANNELS.filter((channel) => {
    if (channel === 'telegram') {
      return input.enableTelegram === true;
    }

    if (channel === 'whatsapp') {
      return input.enableWhatsapp === true;
    }

    return input.enableFacebook === true;
  });
}

export function getGeneratorValidationError(input = {}, options = {}) {
  const mode = options.mode === 'direct' ? 'direct' : 'queue';
  const enabledChannels = getEnabledGeneratorChannels(input);

  if (!enabledChannels.length) {
    return 'Mindestens ein Kanal muss aktiviert sein.';
  }

  if (mode === 'direct' && !enabledChannels.some((channel) => DIRECT_IMPLEMENTED_CHANNELS.has(channel))) {
    return 'Direct Publish ist aktuell nur mit aktiviertem Telegram-Kanal verfuegbar.';
  }

  const channelsToValidate =
    mode === 'direct'
      ? enabledChannels.filter((channel) => DIRECT_IMPLEMENTED_CHANNELS.has(channel))
      : enabledChannels;

  for (const channel of channelsToValidate) {
    const finalText = input.textByChannel?.[channel] || input.title || '';

    if (!normalizeFinalText(finalText)) {
      return `Finaler Text fuer ${getChannelLabel(channel)} ist leer oder ungueltig.`;
    }
  }

  return '';
}

export function buildGeneratorDebugPayload(input = {}) {
  return {
    enabledChannels: getEnabledGeneratorChannels(input),
    seller: {
      sellerType: input.sellerType || 'FBM',
      sellerClass: input.sellerClass || 'UNKNOWN',
      soldByAmazon: input.soldByAmazon ?? null,
      shippedByAmazon: input.shippedByAmazon ?? null
    },
    imageSources: {
      telegram: input.telegramImageSource || 'standard',
      whatsapp: input.whatsappImageSource || 'standard',
      facebook: input.facebookImageSource || 'link_preview'
    },
    hasGeneratedImage: Boolean(cleanText(input.generatedImagePath)),
    hasUploadedImageFile: Boolean(input.uploadedImageFile?.buffer?.length),
    hasUploadedImageDataUrl:
      typeof input.uploadedImagePath === 'string' && cleanText(input.uploadedImagePath).startsWith('data:image'),
    finalTextLengths: {
      telegram: normalizeFinalText(input.textByChannel?.telegram || input.title || '').length,
      whatsapp: normalizeFinalText(input.textByChannel?.whatsapp || input.title || '').length,
      facebook: normalizeFinalText(input.textByChannel?.facebook || input.title || '').length
    },
    finalTextPreview: {
      telegram: buildFinalTextPreview(input.textByChannel?.telegram || input.title || ''),
      whatsapp: buildFinalTextPreview(input.textByChannel?.whatsapp || input.title || ''),
      facebook: buildFinalTextPreview(input.textByChannel?.facebook || input.title || '')
    }
  };
}

export function logGeneratorDebug(eventType, payload = {}) {
  try {
    console.info(`[generator-debug] ${eventType}`, JSON.stringify(payload));
  } catch {
    console.info(`[generator-debug] ${eventType}`, payload);
  }
}
