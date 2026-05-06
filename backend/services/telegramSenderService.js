import { getTelegramConfig } from '../env.js';
import { buildAmazonAffiliateLinkRecord, getTelegramCopyButtonText } from './dealHistoryService.js';
import { logGeneratorDebug } from './generatorFlowService.js';
import {
  checkAndReserveTelegramDuplicate,
  releaseTelegramDuplicateReservation,
  saveTelegramDuplicateAfterSend
} from './telegramDuplicateGuardService.js';
import { buildTelegramTitle } from '../../frontend/src/lib/postGenerator.js';
import sharp from 'sharp';

const NORMALIZED_POST_IMAGE = {
  width: 1200,
  height: 1200,
  padding: 140,
  background: '#ffffff'
};
const TELEGRAM_CAPTION_LIMIT = 1024;
const TELEGRAM_SAFE_CAPTION_LIMIT = 900;
const TELEGRAM_MESSAGE_LIMIT = 4096;
const TELEGRAM_EXTRA_INFO_HEADER_PATTERN =
  /\n{2,}(?=[^\n]*<b>(?:TESTPOST(?: NICHT FREIGEGEBEN)?|Kurzinfo|DEAL STATUS|SELLER CHECK|VERGLEICH & KI|SYSTEM REGELN)<\/b>)/;
const MAIN_POST_LABEL_PREFIX_PATTERN =
  /^(?:[^a-zA-Z0-9]*)(TOP DEAL|MEGA DEAL|PREISFEHLER)\b(?:[^a-zA-Z0-9]+)?/i;
const MAIN_POST_SOURCE_BADGE_PREFIX_PATTERN =
  /^(?:[^a-zA-Z0-9]*)(?:\d{1,3}\s*%\s*(?:RABATT|COUPON|SPAR-ABO)?|RABATT(?:-BADGE)?|COUPON|SPAR-ABO|GUTSCHEIN)\b(?:[^a-zA-Z0-9]+)?/i;
const MAIN_POST_BLOCKED_BADGE_LINE_PATTERN =
  /^(?:[^a-zA-Z0-9]*)(?:TOP DEAL|MEGA DEAL|PREISFEHLER|\d{1,3}\s*%\s*(?:RABATT|COUPON|SPAR-ABO)?|RABATT(?:-BADGE)?|COUPON|SPAR-ABO|GUTSCHEIN)\b.*$/i;
const MAIN_POST_SOURCE_PRICE_LINE_PATTERNS = [
  /^(?:[^\p{L}\p{N}]*)?\d{1,4}(?:[.,]\d{2})\s*(?:\u20ac|EUR)\s+statt\s+\d{1,4}(?:[.,]\d{2})\s*(?:\u20ac|EUR)(?:\s*[-–]?\s*\d{1,3}\s*%)?\s*$/iu,
  /^(?:[^\p{L}\p{N}]*)?jetzt\s+f(?:u|ue|\u00fc)r\s+nur\s*:?\s*\d{1,4}(?:[.,]\d{2})\s*(?:\u20ac|EUR)\s*$/iu,
  /^(?:[^\p{L}\p{N}]*)?statt\s+\d{1,4}(?:[.,]\d{2})\s*(?:\u20ac|EUR)(?:\s*[-–]?\s*\d{1,3}\s*%)?\s*$/iu,
  /^(?:[^\p{L}\p{N}]*)?[-–]?\s*\d{1,3}\s*%\s*$/u
];

function cleanText(value) {
  return typeof value === 'string' ? value.trim() : '';
}

export function buildTelegramCouponFollowUpText(couponCode = '') {
  const normalizedCouponCode = cleanText(couponCode);
  return normalizedCouponCode ? ['CODE:', normalizedCouponCode].join('\n') : '';
}

export async function sendTelegramCouponFollowUp({
  couponCode = '',
  chatId = '',
  titlePreview = 'Rabattcode',
  postContext = 'coupon_follow_up'
} = {}) {
  const normalizedCouponCode = cleanText(couponCode);
  const followUpText = buildTelegramCouponFollowUpText(normalizedCouponCode);
  if (!followUpText) {
    return null;
  }

  return await sendTelegramPost({
    text: followUpText,
    rabattgutscheinCode: normalizedCouponCode,
    chatId,
    disableWebPagePreview: true,
    titlePreview,
    hasAffiliateLink: false,
    postContext
  });
}

function escapeTelegramHtml(value = '') {
  return String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function buildTelegramMainDealText({ title = '', price = '', affiliateLink = '', fallbackText = '' } = {}) {
  const safeTitle = cleanText(title);
  const safePrice = cleanText(price);
  const safeAffiliateLink = cleanText(affiliateLink);

  if (!safeTitle && !safePrice && !safeAffiliateLink) {
    return '';
  }

  const titleBlock = safeTitle ? `<b>${escapeTelegramHtml(safeTitle)}</b>` : '';
  const dealLines = [];
  if (safePrice) {
    dealLines.push(`🔥 Jetzt <b>${escapeTelegramHtml(safePrice)}</b>`);
  }
  if (safeAffiliateLink) {
    dealLines.push(`➡️ <b>${escapeTelegramHtml(safeAffiliateLink)}</b>`);
  }
  const dealBlock = dealLines.join('\n').trim();
  const footerBlock = '<i>Anzeige/Partnerlink</i>';
  const leadingBlocks = [titleBlock, dealBlock].filter(Boolean).join('\n\n').trim();
  const finalText = `${leadingBlocks}\n\n\n${footerBlock}`.trim();

  console.info('[MAIN_POST_PROTECTED]', {
    hasTitle: Boolean(safeTitle),
    hasPrice: Boolean(safePrice),
    hasAffiliateLink: Boolean(safeAffiliateLink),
    hasDisclaimer: true,
    debugDetached: true
  });
  console.info('[MAIN_POST_SPACING_FIXED]', {
    hasTitle: Boolean(safeTitle),
    hasPrice: Boolean(safePrice),
    hasAffiliateLink: Boolean(safeAffiliateLink),
    footerSpacing: 'double_blank_line'
  });
  console.info('[MAIN_CAPTION_SPACING_FIXED]', {
    hasTitle: Boolean(safeTitle),
    hasPrice: Boolean(safePrice),
    hasAffiliateLink: Boolean(safeAffiliateLink),
    footerSpacing: 'double_blank_line'
  });
  return finalText;
}

function extractFirstUrl(text = '') {
  const match = cleanText(text).match(/https?:\/\/[^\s<>"']+/i);
  return cleanText(match?.[0] || '');
}

function looksLikeTelegramDebugText(text = '') {
  const normalized = cleanText(text);
  if (!normalized) {
    return false;
  }

  return /TESTPOST|DEAL STATUS|ERGEBNIS|PR\u00dcFUNGEN|PRÜFUNGEN|WO EINSTELLBAR/i.test(normalized);
}

function looksLikeGeneratorRenderedText(text = '') {
  const normalized = cleanText(text);
  if (!normalized || looksLikeTelegramDebugText(normalized)) {
    return false;
  }

  return (
    /Anzeige\/Partnerlink/i.test(normalized) &&
    /<b>.*<\/b>/i.test(normalized) &&
    /https?:\/\/[^\s<>"']+/i.test(normalized)
  );
}

function buildTelegramFallbackMainPostText({
  title = '',
  price = '',
  affiliateLink = '',
  sourceLink = '',
  diagnostic = false
} = {}) {
  const safeTitle = cleanText(title) || (diagnostic ? '⚠️ Deal erkannt' : 'Deal erkannt');
  const safePrice = cleanText(price) || 'n/a';
  const safeLink = cleanText(affiliateLink) || 'Link nicht verfuegbar';

  return [
    `<b>${escapeTelegramHtml(safeTitle)}</b>`,
    '',
    `🔥 Jetzt <b>${escapeTelegramHtml(safePrice)}</b>`,
    `➡️ <b>${escapeTelegramHtml(safeLink)}</b>`,
    '',
    '',
    '<i>Anzeige/Partnerlink</i>'
  ].join('\n');
}

function stripTelegramHtml(text = '') {
  return String(text || '')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/p>/gi, '\n')
    .replace(/<[^>]+>/g, '')
    .replace(/\r/g, '');
}

function normalizeFallbackLine(line = '') {
  return cleanText(
    stripTelegramHtml(line)
      .replace(/\s+/g, ' ')
      .replace(/\u00a0/g, ' ')
  );
}

function isMainPostSourcePriceLine(line = '') {
  const normalized = normalizeFallbackLine(line);
  if (!normalized) {
    return false;
  }

  return MAIN_POST_SOURCE_PRICE_LINE_PATTERNS.some((pattern) => pattern.test(normalized));
}

function sanitizeMainPostTitle(title = '') {
  const normalizedLines = stripTelegramHtml(title)
    .split('\n')
    .map((line) => normalizeFallbackLine(line))
    .filter(Boolean);
  const removedLabels = [];
  const blockedBadges = [];
  const removedSourcePriceLines = [];

  for (const line of normalizedLines) {
    let candidate = line;
    let changed = true;

    while (candidate && changed) {
      changed = false;

      const labelMatch = candidate.match(MAIN_POST_LABEL_PREFIX_PATTERN);
      if (labelMatch?.[1]) {
        removedLabels.push(labelMatch[1].toUpperCase());
        candidate = cleanText(candidate.replace(MAIN_POST_LABEL_PREFIX_PATTERN, ''));
        changed = true;
      }

      const badgeMatch = candidate.match(MAIN_POST_SOURCE_BADGE_PREFIX_PATTERN);
      if (badgeMatch?.[0]) {
        blockedBadges.push(cleanText(badgeMatch[0]));
        candidate = cleanText(candidate.replace(MAIN_POST_SOURCE_BADGE_PREFIX_PATTERN, ''));
        changed = true;
      }
    }

    if (!candidate) {
      blockedBadges.push(line);
      continue;
    }

    if (MAIN_POST_BLOCKED_BADGE_LINE_PATTERN.test(candidate)) {
      blockedBadges.push(candidate);
      continue;
    }

    if (isMainPostSourcePriceLine(candidate)) {
      removedSourcePriceLines.push(candidate);
      continue;
    }

    return {
      value: candidate,
      removedLabels: [...new Set(removedLabels)],
      blockedBadges: [...new Set(blockedBadges)],
      removedSourcePriceLines: [...new Set(removedSourcePriceLines)]
    };
  }

  return {
    value: '',
    removedLabels: [...new Set(removedLabels)],
    blockedBadges: [...new Set(blockedBadges)],
    removedSourcePriceLines: [...new Set(removedSourcePriceLines)]
  };
}

function resolveTelegramDisplayTitle(originalTitle = '') {
  const safeOriginalTitle = cleanText(originalTitle);
  if (!safeOriginalTitle) {
    return '';
  }

  try {
    return cleanText(buildTelegramTitle(safeOriginalTitle)) || safeOriginalTitle;
  } catch (error) {
    console.warn('[PIPELINE_ERROR_CONTINUED]', {
      stage: 'resolveTelegramDisplayTitle',
      error: error instanceof Error ? error.message : 'Telegram-Titel konnte nicht gekuerzt werden.'
    });
    return safeOriginalTitle;
  }
}

function sanitizeMainPostPrice(price = '') {
  const normalizedLines = stripTelegramHtml(price)
    .split('\n')
    .map((line) => normalizeFallbackLine(line))
    .filter(Boolean);
  const firstLine = normalizedLines.find(Boolean);
  const sourcePriceLines = normalizedLines.filter((line) => isMainPostSourcePriceLine(line));

  if (!firstLine || /^n\/a$/i.test(firstLine)) {
    return {
      value: '',
      normalized: false,
      sourcePriceLines: [...new Set(sourcePriceLines)]
    };
  }

  const extractedPrice = firstLine.match(
    /(\d{1,4}(?:[.,]\d{2})\s*(?:€|EUR)|(?:€|EUR)\s*\d{1,4}(?:[.,]\d{2})?)/i
  )?.[1];
  if (extractedPrice) {
    let normalizedPrice = cleanText(extractedPrice);
    if (/^(?:€|EUR)\s*/i.test(normalizedPrice)) {
      normalizedPrice = `${normalizedPrice.replace(/^(?:€|EUR)\s*/i, '').trim()} €`;
    }

    return {
      value: normalizedPrice,
      normalized: normalizedPrice !== firstLine,
      sourcePriceLines: [...new Set(sourcePriceLines)]
    };
  }

  const normalizedPrice = cleanText(firstLine.replace(/^(?:preis|jetzt(?:\s+fuer)?(?:\s+nur)?|nur)\s*:?\s*/i, ''));
  return {
    value: normalizedPrice,
    normalized: normalizedPrice !== firstLine,
    sourcePriceLines: [...new Set(sourcePriceLines)]
  };
}

function isMeaningfulFallbackTitle(line = '') {
  const normalized = normalizeFallbackLine(line);
  if (!normalized) {
    return false;
  }

  if (
    /^(TESTPOST|ERGEBNIS|DEAL STATUS|PRÜFUNGEN|PRUEFUNGEN|WERTE|WO EINSTELLBAR|ANZEIGE\/PARTNERLINK)$/i.test(
      normalized
    )
  ) {
    return false;
  }

  if (
    /^(📌|🚀|🧪|📝|🌍|🤖|📈|🛒|💶|📉|⭐|🎟|🔁|⚠️|🔥|➡️)/u.test(normalized) ||
    /^(Entscheidung|Live|Testgruppe|Grund|Preis|Rabatt|Score|Fake-Risiko|Coupon|Spar-Abo)\s*:/i.test(normalized)
  ) {
    return false;
  }

  if (/^https?:\/\//i.test(normalized)) {
    return false;
  }

  if (/^(?:⚠️\s*)?(Amazon Produkt|Deal erkannt)$/iu.test(normalized)) {
    return false;
  }

  return normalized.length >= 4;
}

function extractFallbackTitleFromText(text = '') {
  const normalizedText = cleanText(text);
  if (!normalizedText) {
    return { value: '', source: '' };
  }

  const boldMatches = [...normalizedText.matchAll(/<b>([^<]+)<\/b>/gi)];
  for (const match of boldMatches) {
    const candidate = normalizeFallbackLine(match[1]);
    if (isMeaningfulFallbackTitle(candidate)) {
      return {
        value: candidate,
        source: 'textPreviewBold'
      };
    }
  }

  const lines = stripTelegramHtml(normalizedText).split('\n');
  for (const line of lines) {
    const candidate = normalizeFallbackLine(line);
    if (isMeaningfulFallbackTitle(candidate)) {
      return {
        value: candidate,
        source: 'textPreviewLine'
      };
    }
  }

  return { value: '', source: '' };
}

function extractFallbackPriceFromText(text = '') {
  const normalizedText = cleanText(text);
  if (!normalizedText) {
    return { value: '', source: '' };
  }

  const plainText = stripTelegramHtml(normalizedText);
  const lines = plainText.split('\n').map((line) => normalizeFallbackLine(line)).filter(Boolean);
  const labelledLine =
    lines.find((line) => /^💶\s*Preis\s*:/u.test(line)) || lines.find((line) => /^Preis\s*:/i.test(line));
  if (labelledLine) {
    const candidate = cleanText(labelledLine.replace(/^.*?:\s*/u, ''));
    if (candidate && !/^n\/a$/i.test(candidate)) {
      return {
        value: candidate,
        source: 'priceFromMessage'
      };
    }
  }

  const dealLine = lines.find((line) => /🔥\s*Jetzt/u.test(line));
  if (dealLine) {
    const candidate = cleanText(dealLine.replace(/^.*?Jetzt\s*/u, ''));
    if (candidate && !/^n\/a$/i.test(candidate)) {
      return {
        value: candidate,
        source: 'parsedPrice'
      };
    }
  }

  const genericMatch = plainText.match(/(?:\b|^)(\d{1,4}(?:[.,]\d{2})\s*(?:€|EUR)|€\s*\d{1,4}(?:[.,]\d{2})?)/i);
  if (genericMatch?.[1]) {
    return {
      value: cleanText(genericMatch[1].replace(/^€\s*/i, '').replace(/\s+EUR$/i, ' EUR')),
      source: 'parsedPrice'
    };
  }

  return { value: '', source: '' };
}

function extractAllUrls(text = '') {
  return [...String(text || '').matchAll(/https?:\/\/[^\s<>"']+/gi)].map((match) => cleanText(match[0])).filter(Boolean);
}

function collectUniqueUrls(...values) {
  const uniqueUrls = new Set();

  for (const value of values) {
    for (const url of extractAllUrls(value)) {
      uniqueUrls.add(url);
    }

    const directUrl = cleanText(value);
    if (/^https?:\/\//i.test(directUrl)) {
      uniqueUrls.add(directUrl);
    }
  }

  return [...uniqueUrls];
}

function isAmazonDomainLink(url = '') {
  return /https?:\/\/(?:www\.)?amazon\.[^/\s]+/i.test(cleanText(url));
}

function resolveMainFallbackSourceValues({
  title = '',
  price = '',
  affiliateLink = '',
  sourceLink = '',
  fallbackText = '',
  debugInfo = '',
  testMode = false,
  imageUrl = '',
  uploadedFile,
  uploadedImage
} = {}) {
  const explicitTitle = cleanText(title);
  const explicitPrice = cleanText(price);
  const explicitAffiliateLink = cleanText(affiliateLink);
  const hasStructuredTitle = explicitTitle && !/^(?:âš ï¸\s*)?(Amazon Produkt|Deal erkannt)$/iu.test(explicitTitle);
  const hasStructuredPrice = explicitPrice && !/^n\/a$/i.test(explicitPrice);
  const resolvedImageSource = uploadedFile
    ? 'uploadedFile'
    : cleanText(uploadedImage)
      ? 'screenshot'
      : cleanText(imageUrl)
        ? 'sourceImage'
        : 'none';

  return {
    title: hasStructuredTitle ? explicitTitle : '',
    titleSource: hasStructuredTitle ? 'extractedTitle' : 'defaultTitle',
    price: hasStructuredPrice ? explicitPrice : '',
    priceSource: hasStructuredPrice ? 'extractedPrice' : 'defaultPrice',
    link: explicitAffiliateLink,
    linkSource: explicitAffiliateLink ? 'ownAffiliateLink' : 'defaultLink',
    imageSource: resolvedImageSource
  };
  const texts = [cleanText(fallbackText), cleanText(debugInfo)].filter(Boolean);
  const titleSources = [];
  const priceSources = [];
  const linkCandidates = [];

  if (explicitTitle && !/^(?:⚠️\s*)?(Amazon Produkt|Deal erkannt)$/iu.test(explicitTitle)) {
    titleSources.push({ value: explicitTitle, source: 'extractedTitle' });
  }
  for (const text of texts) {
    const titleCandidate = extractFallbackTitleFromText(text);
    if (titleCandidate.value) {
      titleSources.push(titleCandidate);
    }
  }

  if (explicitPrice && !/^n\/a$/i.test(explicitPrice)) {
    priceSources.push({ value: explicitPrice, source: 'extractedPrice' });
  }
  for (const text of texts) {
    const priceCandidate = extractFallbackPriceFromText(text);
    if (priceCandidate.value) {
      priceSources.push(priceCandidate);
    }
  }

  if (explicitAffiliateLink) {
    linkCandidates.push({ value: explicitAffiliateLink, source: 'ownAffiliateLink' });
  }
  for (const text of texts) {
    for (const url of extractAllUrls(text)) {
      linkCandidates.push({
        value: url,
        source: isAmazonDomainLink(url) ? 'originalAmazonLink' : 'sourceLink'
      });
    }
  }
  if (safeSourceLink) {
    linkCandidates.push({
      value: safeSourceLink,
      source: isAmazonDomainLink(safeSourceLink) ? 'originalAmazonLink' : 'sourceLink'
    });
  }

  const fallbackTitle = titleSources[0]?.value || '';
  const titleSource = titleSources[0]?.source || 'defaultTitle';
  const fallbackPrice = priceSources[0]?.value || '';
  const priceSource = priceSources[0]?.source || 'defaultPrice';
  const preferredAmazonLink = linkCandidates.find((candidate) => candidate.source === 'ownAffiliateLink');
  const rawAmazonLink = linkCandidates.find((candidate) => candidate.source === 'originalAmazonLink');
  const rawSourceLink = linkCandidates.find((candidate) => candidate.source === 'sourceLink');
  const selectedLink =
    preferredAmazonLink ||
    rawAmazonLink ||
    (testMode === true ? rawSourceLink : null) || {
      value: '',
      source: 'defaultLink'
    };
  const imageSource = uploadedFile
    ? 'uploadedFile'
    : cleanText(uploadedImage)
      ? 'screenshot'
      : cleanText(imageUrl)
        ? 'sourceImage'
        : 'none';

  return {
    title: fallbackTitle || (texts.length ? '' : ''),
    titleSource,
    price: fallbackPrice,
    priceSource,
    link: cleanText(selectedLink.value),
    linkSource: selectedLink.source,
    imageSource
  };
}

function resolveMainPostFieldValues({
  title = '',
  price = '',
  resolvedAffiliateLink = '',
  fallbackSourceValues = {}
} = {}) {
  const explicitTitle = cleanText(title);
  const explicitPrice = cleanText(price);
  const explicitAffiliateLink = cleanText(resolvedAffiliateLink);
  const fallbackTitle = cleanText(fallbackSourceValues?.title);
  const fallbackPrice = cleanText(fallbackSourceValues?.price);
  const fallbackLink = cleanText(fallbackSourceValues?.link);
  const hasExplicitTitle = explicitTitle && !/^(?:âš ï¸\s*)?(Amazon Produkt|Deal erkannt)$/iu.test(explicitTitle);
  const hasExplicitPrice = explicitPrice && !/^n\/a$/i.test(explicitPrice);

  return {
    title: hasExplicitTitle ? explicitTitle : fallbackTitle,
    titleSource: hasExplicitTitle ? 'extractedTitle' : fallbackSourceValues?.titleSource || 'defaultTitle',
    price: hasExplicitPrice ? explicitPrice : fallbackPrice,
    priceSource: hasExplicitPrice ? 'extractedPrice' : fallbackSourceValues?.priceSource || 'defaultPrice',
    link: explicitAffiliateLink || fallbackLink,
    linkSource: explicitAffiliateLink ? 'ownAffiliateLink' : fallbackSourceValues?.linkSource || 'defaultLink'
  };
}

function resolveTelegramAffiliateLink({ affiliateLink = '', asin = '' } = {}) {
  const rawAffiliateLink = cleanText(affiliateLink);
  const normalizedAsin = cleanText(asin).toUpperCase();
  const rebuildCandidates = [];

  if (rawAffiliateLink && isAmazonDomainLink(rawAffiliateLink)) {
    rebuildCandidates.push(rawAffiliateLink);
  }
  if (normalizedAsin) {
    rebuildCandidates.push(normalizedAsin);
  }

  for (const candidate of rebuildCandidates) {
    const rebuiltRecord = buildAmazonAffiliateLinkRecord(candidate, { asin: normalizedAsin });
    if (rebuiltRecord.valid && cleanText(rebuiltRecord.affiliateUrl)) {
      const resolvedLink = cleanText(rebuiltRecord.affiliateUrl);
      return {
        affiliateLink: resolvedLink,
        normalizedUrl: cleanText(rebuiltRecord.normalizedUrl),
        strippedForeignLink: Boolean(rawAffiliateLink) && resolvedLink !== rawAffiliateLink
      };
    }
  }

  return {
    affiliateLink: '',
    normalizedUrl: '',
    strippedForeignLink: Boolean(rawAffiliateLink)
  };

  if (normalizedAsin) {
    const rebuiltRecord = buildAmazonAffiliateLinkRecord(rawAffiliateLink || normalizedAsin, { asin: normalizedAsin });
    if (rebuiltRecord.valid && cleanText(rebuiltRecord.affiliateUrl)) {
      return {
        affiliateLink: cleanText(rebuiltRecord.affiliateUrl),
        normalizedUrl: cleanText(rebuiltRecord.normalizedUrl),
        strippedForeignLink: Boolean(rawAffiliateLink) && cleanText(rebuiltRecord.affiliateUrl) !== rawAffiliateLink
      };
    }
  }

  return {
    affiliateLink: rawAffiliateLink,
    normalizedUrl: rawAffiliateLink,
    strippedForeignLink: false
  };
}

async function sendTelegramRequest(token, method, payload, options = {}) {
  const useHtml = options.html !== false;
  const finalPayload = useHtml
    ? {
        ...payload,
        parse_mode: 'HTML'
      }
    : { ...payload };

  const telegramResponse = await fetch(`https://api.telegram.org/bot${token}/${method}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(finalPayload)
  });

  const responseText = await telegramResponse.text();
  let telegramData;

  try {
    telegramData = JSON.parse(responseText);
  } catch {
    telegramData = { raw: responseText };
  }

  return {
    telegramResponse,
    telegramData
  };
}

async function sendTelegramMultipartRequest(token, method, formData) {
  const telegramResponse = await fetch(`https://api.telegram.org/bot${token}/${method}`, {
    method: 'POST',
    body: formData
  });

  const responseText = await telegramResponse.text();
  let telegramData;

  try {
    telegramData = JSON.parse(responseText);
  } catch {
    telegramData = { raw: responseText };
  }

  return {
    telegramResponse,
    telegramData
  };
}

function parseUploadedImage(uploadedImage) {
  const trimmedUploadedImage = typeof uploadedImage === 'string' ? uploadedImage.trim() : '';
  if (!trimmedUploadedImage) {
    return null;
  }

  if (!trimmedUploadedImage.startsWith('data:image')) {
    return null;
  }

  const [metaPart, base64Data] = trimmedUploadedImage.split(',', 2);
  if (!base64Data) {
    throw new Error('Upload-Bild ist unvollstaendig oder leer.');
  }

  try {
    const mimeTypeMatch = metaPart.match(/^data:(image\/[a-zA-Z0-9.+-]+);base64$/);
    const mimeType = mimeTypeMatch?.[1] || 'image/jpeg';
    const extension = mimeType.split('/')[1]?.replace(/[^a-zA-Z0-9]/g, '') || 'jpg';
    const paddedBase64 = base64Data.padEnd(base64Data.length + ((4 - (base64Data.length % 4)) % 4), '=');
    const buffer = Buffer.from(paddedBase64, 'base64');

    if (!buffer.length) {
      throw new Error('Upload-Bild ist leer.');
    }

    return {
      buffer,
      mimeType,
      filename: `upload.${extension}`
    };
  } catch {
    throw new Error('Upload-Bild konnte nicht in ein gueltiges Telegram-Bild umgewandelt werden.');
  }
}

function normalizeUploadedFile(uploadedFile) {
  if (!uploadedFile || !uploadedFile.buffer) {
    return null;
  }

  if (!Buffer.isBuffer(uploadedFile.buffer) || uploadedFile.buffer.length === 0) {
    throw new Error('Upload-Bild ist leer oder ungueltig.');
  }

  const mimeType =
    typeof uploadedFile.mimetype === 'string' && uploadedFile.mimetype.trim()
      ? uploadedFile.mimetype.trim()
      : 'image/jpeg';
  const originalName =
    typeof uploadedFile.originalname === 'string' && uploadedFile.originalname.trim()
      ? uploadedFile.originalname.trim()
      : `upload.${mimeType.split('/')[1] || 'jpg'}`;

  return {
    buffer: uploadedFile.buffer,
    mimeType,
    filename: originalName
  };
}

async function normalizeImageForTelegram(inputBuffer) {
  const innerWidth = NORMALIZED_POST_IMAGE.width - NORMALIZED_POST_IMAGE.padding * 2;
  const innerHeight = NORMALIZED_POST_IMAGE.height - NORMALIZED_POST_IMAGE.padding * 2;

  const fittedImage = await sharp(inputBuffer)
    .rotate()
    .resize({
      width: innerWidth,
      height: innerHeight,
      fit: 'contain',
      background: NORMALIZED_POST_IMAGE.background
    })
    .png()
    .toBuffer();

  const normalizedBuffer = await sharp({
    create: {
      width: NORMALIZED_POST_IMAGE.width,
      height: NORMALIZED_POST_IMAGE.height,
      channels: 4,
      background: NORMALIZED_POST_IMAGE.background
    }
  })
    .composite([
      {
        input: fittedImage,
        left: NORMALIZED_POST_IMAGE.padding,
        top: NORMALIZED_POST_IMAGE.padding
      }
    ])
    .png()
    .toBuffer();

  return {
    buffer: normalizedBuffer,
    mimeType: 'image/png',
    filename: 'normalized-post-image.png'
  };
}

async function fetchAndNormalizeImageUrl(imageUrl) {
  const imageResponse = await fetch(imageUrl);
  if (!imageResponse.ok) {
    throw new Error(`Bild konnte nicht geladen werden (${imageResponse.status}).`);
  }

  const arrayBuffer = await imageResponse.arrayBuffer();
  return await normalizeImageForTelegram(Buffer.from(arrayBuffer), 'amazon');
}

function splitTelegramTextIntoChunks(text = '', limit = TELEGRAM_MESSAGE_LIMIT) {
  const normalizedText = typeof text === 'string' ? text.trim() : '';
  if (!normalizedText) {
    return [];
  }

  const chunks = [];
  let currentChunk = '';
  const lines = normalizedText.split('\n');

  const flushChunk = () => {
    const safeChunk = currentChunk.trim();
    if (safeChunk) {
      chunks.push(safeChunk);
    }
    currentChunk = '';
  };

  for (const line of lines) {
    const safeLine = String(line ?? '');
    const candidate = currentChunk ? `${currentChunk}\n${safeLine}` : safeLine;

    if (candidate.length <= limit) {
      currentChunk = candidate;
      continue;
    }

    if (currentChunk) {
      flushChunk();
    }

    if (safeLine.length <= limit) {
      currentChunk = safeLine;
      continue;
    }

    let remainder = safeLine;
    while (remainder.length > limit) {
      let splitIndex = remainder.lastIndexOf(' ', limit);
      if (splitIndex <= 0) {
        splitIndex = limit;
      }
      chunks.push(remainder.slice(0, splitIndex).trim());
      remainder = remainder.slice(splitIndex).trim();
    }

    currentChunk = remainder;
  }

  flushChunk();
  return chunks.filter(Boolean);
}

function splitTelegramPhotoPostText(text = '') {
  const normalizedText = typeof text === 'string' ? text.trim() : '';
  if (!normalizedText) {
    return {
      mainText: '',
      extraText: '',
      splitIndex: -1,
      splitMarker: ''
    };
  }

  const separatorMatch = TELEGRAM_EXTRA_INFO_HEADER_PATTERN.exec(normalizedText);
  if (!separatorMatch || separatorMatch.index <= 0) {
    return {
      mainText: normalizedText,
      extraText: '',
      splitIndex: -1,
      splitMarker: ''
    };
  }

  const splitIndex = separatorMatch.index;
  const mainText = normalizedText.slice(0, splitIndex).trim();
  const extraText = normalizedText.slice(splitIndex).trim();
  const splitMarkerMatch = extraText.match(/<b>([^<]+)<\/b>/);

  if (!mainText) {
    return {
      mainText: normalizedText,
      extraText: '',
      splitIndex: -1,
      splitMarker: ''
    };
  }

  return {
    mainText,
    extraText,
    splitIndex,
    splitMarker: splitMarkerMatch?.[1] || ''
  };
}

function closeUnbalancedTelegramTags(text = '') {
  const tagPattern = /<\/?(b|i)>/g;
  const openTags = [];
  let match;

  while ((match = tagPattern.exec(text))) {
    const tagName = match[1];
    const isClosingTag = match[0].startsWith('</');

    if (!isClosingTag) {
      openTags.push(tagName);
      continue;
    }

    if (openTags[openTags.length - 1] === tagName) {
      openTags.pop();
      continue;
    }

    const danglingIndex = openTags.lastIndexOf(tagName);
    if (danglingIndex !== -1) {
      openTags.splice(danglingIndex, 1);
    }
  }

  return `${text}${openTags.reverse().map((tagName) => `</${tagName}>`).join('')}`;
}

function findSafeCaptionCutIndex(text = '', limit = TELEGRAM_SAFE_CAPTION_LIMIT) {
  if (text.length <= limit) {
    return text.length;
  }

  let cutIndex = text.lastIndexOf('\n', limit);
  if (cutIndex <= 0) {
    cutIndex = text.lastIndexOf(' ', limit);
  }
  if (cutIndex <= 0) {
    cutIndex = limit;
  }

  const lastOpenTagIndex = text.lastIndexOf('<', cutIndex);
  const lastCloseTagIndex = text.lastIndexOf('>', cutIndex);
  if (lastOpenTagIndex > lastCloseTagIndex) {
    const priorLineBreak = text.lastIndexOf('\n', lastOpenTagIndex);
    const priorSpace = text.lastIndexOf(' ', lastOpenTagIndex);
    const priorBoundary = Math.max(priorLineBreak, priorSpace);
    cutIndex = priorBoundary > 0 ? priorBoundary : lastOpenTagIndex;
  }

  if (cutIndex <= 0) {
    cutIndex = Math.min(limit, text.length);
  }

  return cutIndex;
}

function trimTelegramPhotoCaption(text = '', limit = TELEGRAM_SAFE_CAPTION_LIMIT) {
  const normalizedText = typeof text === 'string' ? text.trim() : '';
  if (!normalizedText) {
    return {
      text: '',
      beforeLength: 0,
      afterLength: 0,
      cutAt: null,
      cutContext: '',
      trimmed: false,
      limit
    };
  }

  if (normalizedText.length <= limit) {
    return {
      text: normalizedText,
      beforeLength: normalizedText.length,
      afterLength: normalizedText.length,
      cutAt: null,
      cutContext: '',
      trimmed: false,
      limit
    };
  }

  let workingCutIndex = findSafeCaptionCutIndex(normalizedText, limit);
  let workingText = normalizedText.slice(0, workingCutIndex).trimEnd();
  let trimmedCaption = closeUnbalancedTelegramTags(workingText).trim();

  while (trimmedCaption.length > limit && workingText.length > 1) {
    const overflow = trimmedCaption.length - limit;
    workingText = workingText.slice(0, Math.max(1, workingText.length - overflow)).trimEnd();
    trimmedCaption = closeUnbalancedTelegramTags(workingText).trim();
  }

  workingCutIndex = workingText.length;

  return {
    text: trimmedCaption,
    beforeLength: normalizedText.length,
    afterLength: trimmedCaption.length,
    cutAt: workingCutIndex,
    cutContext: normalizedText.slice(Math.max(0, workingCutIndex - 80), Math.min(normalizedText.length, workingCutIndex + 80)),
    trimmed: true,
    limit
  };
}

async function sendSingleTelegramDelivery({
  token,
  finalChatId,
  text,
  parsedUploadedImage = null,
  effectiveImageUrl = '',
  resolvedDisableWebPagePreview = false,
  replyMarkup = undefined,
  deliveryMeta = {}
}) {
  const telegramMethod = parsedUploadedImage || effectiveImageUrl ? 'sendPhoto' : 'sendMessage';
  let telegramResponse;
  let telegramData;
  const titlePreview = cleanText(deliveryMeta.titlePreview || '').slice(0, 120);
  const captionLength = Number.isFinite(Number(deliveryMeta.captionLength))
    ? Number(deliveryMeta.captionLength)
    : cleanText(text).length;
  const hasAffiliateLink = deliveryMeta.hasAffiliateLink === true;
  const hasImage = Boolean(parsedUploadedImage || effectiveImageUrl);

  console.info('[TELEGRAM_SEND_START]', {
    chatId: finalChatId,
    method: telegramMethod,
    textLength: text.trim().length,
    hasUploadedImage: Boolean(parsedUploadedImage),
    hasImageUrl: Boolean(effectiveImageUrl),
    hasCouponCode: Boolean(replyMarkup),
    titlePreview: titlePreview || null,
    captionLength,
    hasAffiliateLink
  });
  console.info('[TELEGRAM_FORCE_SEND_START]', {
    chatId: finalChatId,
    method: telegramMethod,
    payload: {
      text: String(text),
      textLength: text.trim().length,
      uploadedImage: Boolean(parsedUploadedImage),
      imageUrl: effectiveImageUrl || null,
      disableWebPagePreview: resolvedDisableWebPagePreview,
      replyMarkup: Boolean(replyMarkup),
      titlePreview: titlePreview || null,
      captionLength,
      hasAffiliateLink
    }
  });

  logGeneratorDebug('api.telegram.request', {
    method: telegramMethod,
    textLength: text.trim().length,
    hasUploadedImage: Boolean(parsedUploadedImage),
    hasImageUrl: Boolean(effectiveImageUrl),
    disableWebPagePreview: resolvedDisableWebPagePreview,
    hasCouponCode: Boolean(replyMarkup)
  });

  if (parsedUploadedImage) {
    const normalizedImage = await normalizeImageForTelegram(parsedUploadedImage.buffer, 'upload');
    const formData = new FormData();
    formData.append('chat_id', finalChatId);
    formData.append('caption', String(text));
    formData.append('parse_mode', 'HTML');
    if (replyMarkup) {
      formData.append('reply_markup', JSON.stringify(replyMarkup));
    }

    const photoBlob = new Blob([normalizedImage.buffer], { type: normalizedImage.mimeType });
    formData.append('photo', photoBlob, normalizedImage.filename);

    ({ telegramResponse, telegramData } = await sendTelegramMultipartRequest(token, telegramMethod, formData));
  } else if (effectiveImageUrl) {
    const normalizedImage = await fetchAndNormalizeImageUrl(effectiveImageUrl);
    const formData = new FormData();
    formData.append('chat_id', finalChatId);
    formData.append('caption', String(text));
    formData.append('parse_mode', 'HTML');
    if (replyMarkup) {
      formData.append('reply_markup', JSON.stringify(replyMarkup));
    }

    const photoBlob = new Blob([normalizedImage.buffer], { type: normalizedImage.mimeType });
    formData.append('photo', photoBlob, normalizedImage.filename);

    ({ telegramResponse, telegramData } = await sendTelegramMultipartRequest(token, telegramMethod, formData));
  } else {
    ({ telegramResponse, telegramData } = await sendTelegramRequest(token, telegramMethod, {
      chat_id: finalChatId,
      text: String(text),
      ...(resolvedDisableWebPagePreview ? { disable_web_page_preview: true } : {}),
      ...(replyMarkup ? { reply_markup: replyMarkup } : {})
    }));
  }

  if (!telegramResponse.ok || !telegramData?.ok) {
    const telegramDescription =
      telegramData?.description || telegramData?.raw || 'Telegram API hat einen unbekannten Fehler geliefert';
    const errorCode = Number.isFinite(Number(telegramData?.error_code)) ? Number(telegramData.error_code) : null;
    console.warn('[NO_POST_REASON]', {
      reason: 'Telegram Send Fehler',
      detail: telegramDescription,
      chatId: finalChatId,
      method: telegramMethod
    });
    console.error('[TELEGRAM_SEND_ERROR]', {
      chatId: finalChatId,
      method: telegramMethod,
      errorCode,
      errorMessage: telegramDescription,
      titlePreview: titlePreview || null,
      captionLength,
      hasImage,
      hasAffiliateLink
    });
    console.error('[TELEGRAM_FORCE_SEND_ERROR]', {
      chatId: finalChatId,
      method: telegramMethod,
      reason: telegramDescription
    });
    logGeneratorDebug('api.telegram.error', {
      method: telegramMethod,
      error: telegramDescription,
      disableWebPagePreview: resolvedDisableWebPagePreview
    });
    throw new Error(`Telegram API Fehler: ${telegramDescription}`);
  }

  logGeneratorDebug('api.telegram.success', {
    method: telegramMethod,
    messageId: telegramData.result?.message_id,
    hasImageUrl: Boolean(effectiveImageUrl),
    disableWebPagePreview: resolvedDisableWebPagePreview
  });

  console.info('[TELEGRAM_SEND_SUCCESS]', {
    chatId: telegramData.result?.chat?.id ?? finalChatId,
    method: telegramMethod,
    messageId: telegramData.result?.message_id,
    titlePreview: titlePreview || null,
    captionLength,
    hasAffiliateLink
  });
  console.info('[TELEGRAM_FORCE_SEND_SUCCESS]', {
    chatId: telegramData.result?.chat?.id ?? finalChatId,
    method: telegramMethod,
    messageId: telegramData.result?.message_id
  });

  return {
    method: telegramMethod,
    messageId: telegramData.result?.message_id,
    chatId: telegramData.result?.chat?.id ?? finalChatId,
    imageUrl: effectiveImageUrl || null
  };
}

export async function sendTelegramDealPost({
  title = '',
  price = '',
  affiliateLink = '',
  asin = '',
  debugInfo = '',
  testMode = false,
  uploadedFile,
  uploadedImage,
  imageUrl = '',
  chatId,
  fallbackText = '',
  rabattgutscheinCode = '',
  duplicateContext = null
} = {}) {
  const resolvedAffiliate = resolveTelegramAffiliateLink({
    affiliateLink,
    asin
  });
  const sanitizedFallbackText = cleanText(fallbackText);
  const fallbackTextLooksLikeDebug = looksLikeTelegramDebugText(sanitizedFallbackText);
  const directDebugInfo = testMode === true ? cleanText(debugInfo) : '';
  const trimmedDebugInfo = directDebugInfo || (testMode === true && fallbackTextLooksLikeDebug ? sanitizedFallbackText : '');
  const blockedSourceLinks = new Set(collectUniqueUrls(sanitizedFallbackText, trimmedDebugInfo));
  const originalAffiliateLink = cleanText(affiliateLink);

  if (sanitizedFallbackText) {
    console.info('[ORIGINAL_TEXT_BLOCKED_FROM_MAIN_POST]', {
      sourceTextLength: sanitizedFallbackText.length,
      sourceTextLooksLikeDebug: fallbackTextLooksLikeDebug
    });
  }
  if (originalAffiliateLink && originalAffiliateLink !== cleanText(resolvedAffiliate.affiliateLink)) {
    blockedSourceLinks.add(originalAffiliateLink);
  }
  if (blockedSourceLinks.size > 0) {
    console.info('[FOREIGN_LINK_BLOCKED_FROM_MAIN_POST]', {
      blockedLinks: [...blockedSourceLinks],
      resolvedAffiliateLink: cleanText(resolvedAffiliate.affiliateLink) || null
    });
  }
  const fallbackSourceValues = resolveMainFallbackSourceValues({
    title,
    price,
    affiliateLink: resolvedAffiliate.affiliateLink || cleanText(affiliateLink),
    sourceLink: '',
    fallbackText,
    debugInfo: trimmedDebugInfo,
    testMode,
    imageUrl,
    uploadedFile,
    uploadedImage
  });
  const resolvedMainPostFields = resolveMainPostFieldValues({
    title,
    price,
    resolvedAffiliateLink: resolvedAffiliate.affiliateLink,
    fallbackSourceValues
  });
  const sanitizedTitleResult = sanitizeMainPostTitle(resolvedMainPostFields.title);
  const sanitizedPriceResult = sanitizeMainPostPrice(resolvedMainPostFields.price);
  const removedSourcePriceLines = [
    ...(Array.isArray(sanitizedTitleResult.removedSourcePriceLines) ? sanitizedTitleResult.removedSourcePriceLines : []),
    ...(Array.isArray(sanitizedPriceResult.sourcePriceLines) ? sanitizedPriceResult.sourcePriceLines : [])
  ].filter(Boolean);
  const originalDisplayTitle =
    cleanText(sanitizedTitleResult.value) || cleanText(resolvedMainPostFields.title) || cleanText(title);
  const telegramTitle = resolveTelegramDisplayTitle(originalDisplayTitle);
  const sanitizedMainPostFields = {
    title: telegramTitle,
    titleSource: resolvedMainPostFields.titleSource,
    price: cleanText(sanitizedPriceResult.value),
    priceSource: resolvedMainPostFields.priceSource,
    link: cleanText(resolvedMainPostFields.link),
    linkSource: resolvedMainPostFields.linkSource
  };
  const buildRequiresFallback = false;
  fallbackSourceValues.title = sanitizedMainPostFields.title;
  fallbackSourceValues.titleSource = sanitizedMainPostFields.titleSource;
  fallbackSourceValues.price = sanitizedMainPostFields.price;
  fallbackSourceValues.priceSource = sanitizedMainPostFields.priceSource;
  fallbackSourceValues.link = sanitizedMainPostFields.link;
  fallbackSourceValues.linkSource = sanitizedMainPostFields.linkSource;
  if (sanitizedTitleResult.removedLabels.length > 0) {
    console.info('[MAIN_POST_LABELS_SANITIZED]', {
      removedLabels: sanitizedTitleResult.removedLabels,
      originalTitle: cleanText(resolvedMainPostFields.title) || null,
      sanitizedTitle: sanitizedMainPostFields.title || null
    });
  }
  if (originalDisplayTitle && originalDisplayTitle !== telegramTitle) {
    console.info('[TELEGRAM_TITLE_APPLIED]', {
      originalLength: originalDisplayTitle.length,
      telegramLength: telegramTitle.length,
      originalTitle: originalDisplayTitle.slice(0, 140),
      telegramTitle
    });
  }
  if (sanitizedTitleResult.blockedBadges.length > 0) {
    console.info('[SOURCE_BADGE_BLOCKED_FROM_MAIN_POST]', {
      blockedBadges: sanitizedTitleResult.blockedBadges,
      originalTitle: cleanText(resolvedMainPostFields.title) || null,
      sanitizedTitle: sanitizedMainPostFields.title || null
    });
  }
  if (removedSourcePriceLines.length > 0) {
    console.info('[SOURCE_PRICE_LINE_REMOVED_FROM_MAIN_POST]', {
      removedLines: [...new Set(removedSourcePriceLines)],
      originalTitle: cleanText(resolvedMainPostFields.title) || null,
      originalPrice: cleanText(resolvedMainPostFields.price) || null,
      sanitizedTitle: sanitizedMainPostFields.title || null,
      sanitizedPrice: sanitizedMainPostFields.price || null
    });
    console.info('[SOURCE_SPECIFIC_SANITIZER_APPLIED]', {
      sourcePriceLineCount: [...new Set(removedSourcePriceLines)].length,
      titleAffected: (sanitizedTitleResult.removedSourcePriceLines || []).length > 0,
      priceAffected: (sanitizedPriceResult.sourcePriceLines || []).length > 0
    });
  }
  console.info('[MAIN_POST_SOURCE_SANITIZED]', {
    titleSource: sanitizedMainPostFields.titleSource,
    priceSource: sanitizedMainPostFields.priceSource,
    linkSource: sanitizedMainPostFields.linkSource,
    hasOriginalText: Boolean(sanitizedFallbackText),
    blockedSourceLinkCount: blockedSourceLinks.size
  });
  console.info('[MAIN_POST_CAPTION_SANITIZED]', {
    titleSanitized: cleanText(resolvedMainPostFields.title) !== sanitizedMainPostFields.title,
    priceSanitized: cleanText(resolvedMainPostFields.price) !== sanitizedMainPostFields.price,
    sourcePriceLineRemoved: removedSourcePriceLines.length > 0
  });
  console.info('[MAIN_POST_FINAL_ONLY]', {
    titleSource: sanitizedMainPostFields.titleSource,
    priceSource: sanitizedMainPostFields.priceSource,
    linkSource: sanitizedMainPostFields.linkSource,
    titleSanitized: cleanText(resolvedMainPostFields.title) !== sanitizedMainPostFields.title,
    priceSanitized: cleanText(resolvedMainPostFields.price) !== sanitizedMainPostFields.price,
    usesFallbackText: false
  });
  if (!resolvedAffiliate.affiliateLink) {
    console.warn('[MAIN_POST_SKIPPED_REASON]', {
      reason: 'own_affiliate_link_missing_review',
      titleSource: sanitizedMainPostFields.titleSource,
      priceSource: sanitizedMainPostFields.priceSource,
      linkSource: sanitizedMainPostFields.linkSource
    });
    throw new Error('REVIEW_REQUIRED: Eigener Affiliate-Link fehlt. Hauptpost wird ohne Quelllink nicht gesendet.');
  }
  console.info('[MAIN_POST_BUILD_START]', {
    titleSource: sanitizedMainPostFields.titleSource,
    priceSource: sanitizedMainPostFields.priceSource,
    linkSource: sanitizedMainPostFields.linkSource,
    buildRequiresFallback,
    hasFallbackText: Boolean(sanitizedFallbackText),
    usesGeneratorRenderedText: false
  });
  const initialMainPostText = buildTelegramMainDealText({
    title: sanitizedMainPostFields.title,
    price: sanitizedMainPostFields.price,
    affiliateLink: sanitizedMainPostFields.link
  });
  const fallbackMainRequired = !cleanText(initialMainPostText);
  const mainPostText = fallbackMainRequired
    ? buildTelegramFallbackMainPostText({
        title: sanitizedMainPostFields.title,
        price: sanitizedMainPostFields.price,
        affiliateLink: sanitizedMainPostFields.link,
        diagnostic: Boolean(trimmedDebugInfo) || fallbackTextLooksLikeDebug
      })
    : initialMainPostText;
  const trimmedMainPostText = cleanText(mainPostText);
  const hasImage = Boolean(uploadedFile || cleanText(uploadedImage) || cleanText(imageUrl));
  const captionResult = trimTelegramPhotoCaption(trimmedMainPostText, TELEGRAM_SAFE_CAPTION_LIMIT);
  const safeMainPostText = cleanText(captionResult.text || trimmedMainPostText);
  let mainPostSent = false;

  console.info('[CENTRAL_TELEGRAM_DEAL_SENDER_USED]', {
    titlePreview: cleanText(title).slice(0, 120) || null,
    hasImage,
    testMode: testMode === true,
    hasAffiliateLink: Boolean(resolvedAffiliate.affiliateLink),
    debugInfoLength: trimmedDebugInfo.length
  });
  if (trimmedMainPostText) {
    console.info('[MAIN_POST_BUILD_SUCCESS]', {
      titleSource: sanitizedMainPostFields.titleSource,
      priceSource: sanitizedMainPostFields.priceSource,
      linkSource: sanitizedMainPostFields.linkSource,
      textLength: trimmedMainPostText.length,
      fallbackUsed: fallbackMainRequired
    });
  } else {
    console.error('[MAIN_POST_BUILD_FAILED]', {
      titleSource: sanitizedMainPostFields.titleSource,
      priceSource: sanitizedMainPostFields.priceSource,
      linkSource: sanitizedMainPostFields.linkSource,
      fallbackUsed: fallbackMainRequired
    });
  }
  if (resolvedAffiliate.strippedForeignLink) {
    console.info('[FOREIGN_LINK_REMOVED]', {
      originalLink: cleanText(affiliateLink) || null,
      affiliateLink: resolvedAffiliate.affiliateLink || null,
      asin: cleanText(asin).toUpperCase() || ''
    });
  }
  if (resolvedAffiliate.affiliateLink) {
    console.info('[AFFILIATE_LINK_BUILT]', {
      affiliateLink: resolvedAffiliate.affiliateLink,
      asin: cleanText(asin).toUpperCase() || ''
    });
    console.info('[OWN_AFFILIATE_LINK_USED]', {
      affiliateLink: resolvedAffiliate.affiliateLink,
      asin: cleanText(asin).toUpperCase() || ''
    });
  } else {
    console.warn('[AFFILIATE_LINK_MISSING]', {
      asin: cleanText(asin).toUpperCase() || '',
      fallbackLinkSource: resolvedMainPostFields.linkSource,
      fallbackLink: cleanText(resolvedMainPostFields.link) || null
    });
  }
  if (hasImage) {
    console.info('[MAIN_POST_IMAGE_FOUND]', {
      imageSource: fallbackSourceValues.imageSource,
      hasUploadedFile: Boolean(uploadedFile),
      hasUploadedImage: Boolean(cleanText(uploadedImage)),
      hasImageUrl: Boolean(cleanText(imageUrl))
    });
  } else {
    console.warn('[MAIN_POST_IMAGE_MISSING]', {
      imageSource: fallbackSourceValues.imageSource
    });
  }

  console.info('[MAIN_POST_REQUIRED]', {
    chatId: cleanText(String(chatId || '')) || null,
    titlePreview: cleanText(title).slice(0, 120) || null,
    hasImage,
    fallbackMainRequired,
    hasDebugInfo: Boolean(trimmedDebugInfo)
  });
  if (fallbackMainRequired) {
    console.info('[MAIN_FALLBACK_FILLED_FROM_SOURCE]', {
      titleSource: resolvedMainPostFields.titleSource,
      priceSource: resolvedMainPostFields.priceSource,
      linkSource: resolvedMainPostFields.linkSource,
      imageSource: fallbackSourceValues.imageSource
    });
    console.warn('[MAIN_POST_SKIPPED_REASON]', {
      reason: buildRequiresFallback ? 'structured_values_incomplete' : 'fallback_text_or_debug_detected',
      titleSource: resolvedMainPostFields.titleSource,
      priceSource: resolvedMainPostFields.priceSource,
      linkSource: resolvedMainPostFields.linkSource,
      imageSource: fallbackSourceValues.imageSource
    });
    if (!resolvedMainPostFields.title && !resolvedMainPostFields.price && !resolvedMainPostFields.link) {
      console.warn('[MAIN_FALLBACK_VALUES_MISSING]', {
        titleSource: resolvedMainPostFields.titleSource,
        priceSource: resolvedMainPostFields.priceSource,
        linkSource: resolvedMainPostFields.linkSource,
        imageSource: fallbackSourceValues.imageSource
      });
    }
    console.warn('[MAIN_POST_MISSING_SEND_FALLBACK]', {
      chatId: cleanText(String(chatId || '')) || null,
      titlePreview: cleanText(title).slice(0, 120) || null,
      fallbackTitle: cleanText(fallbackSourceValues.title) || (trimmedDebugInfo ? '⚠️ Deal erkannt' : 'Deal erkannt'),
      fallbackPrice: cleanText(resolvedMainPostFields.price) || 'n/a',
      fallbackLink: cleanText(resolvedMainPostFields.link) || 'Link nicht verfuegbar'
    });
  }

  let mainDeliveryResult;
  const handleDuplicateBlockedMainDelivery = (deliveryResult) => {
    if (deliveryResult?.duplicateBlocked !== true) {
      return null;
    }

    console.info('[MAIN_POST_DUPLICATE_BLOCKED]', {
      chatId: deliveryResult.chatId || cleanText(String(chatId || '')) || null,
      titlePreview: cleanText(title).slice(0, 120) || null,
      duplicateKey: cleanText(deliveryResult.duplicateKey) || null,
      lastSentAt: deliveryResult.lastSentAt || null
    });

    return {
      ...deliveryResult,
      extraMessageIds: [],
      captionInfo: {
        beforeLength: captionResult.beforeLength,
        afterLength: captionResult.afterLength,
        cutAt: captionResult.cutAt,
        cutContext: captionResult.cutContext,
        trimmed: captionResult.trimmed,
        limit: TELEGRAM_SAFE_CAPTION_LIMIT
      },
      affiliateLink: resolvedAffiliate.affiliateLink,
      strippedForeignLink: resolvedAffiliate.strippedForeignLink
    };
  };

  if (hasImage) {
    console.info('[MAIN_POST_SENDPHOTO_START]', {
      chatId: cleanText(String(chatId || '')) || null,
      titlePreview: cleanText(resolvedMainPostFields.title || title).slice(0, 120) || null,
      imageSource: fallbackSourceValues.imageSource,
      hasAffiliateLink: Boolean(cleanText(resolvedMainPostFields.link))
    });
    console.info('[TELEGRAM_MAIN_PHOTO_POST_START]', {
      chatId: cleanText(String(chatId || '')) || null,
      titlePreview: cleanText(title).slice(0, 120) || null,
      captionLengthBeforeTrim: captionResult.beforeLength,
      captionLengthAfterTrim: captionResult.afterLength,
      hasAffiliateLink: Boolean(resolvedAffiliate.affiliateLink)
    });

    if (captionResult.trimmed) {
      console.info('[CAPTION_TOO_LONG]', {
        titlePreview: cleanText(title).slice(0, 120) || null,
        beforeLength: captionResult.beforeLength,
        safeLimit: TELEGRAM_SAFE_CAPTION_LIMIT
      });
      console.info('[CAPTION_TRIMMED_TO_SAFE_LIMIT]', {
        titlePreview: cleanText(title).slice(0, 120) || null,
        beforeLength: captionResult.beforeLength,
        afterLength: captionResult.afterLength,
        cutAt: captionResult.cutAt,
        cutContext: captionResult.cutContext
      });
    }

    try {
      mainDeliveryResult = await sendTelegramPost({
        text: safeMainPostText,
        uploadedFile,
        uploadedImage,
        imageUrl,
        disableWebPagePreview: false,
        rabattgutscheinCode,
        chatId,
        titlePreview: title,
        hasAffiliateLink: Boolean(resolvedAffiliate.affiliateLink),
        postContext: 'deal_main_photo',
        duplicateContext:
          duplicateContext && typeof duplicateContext === 'object'
            ? {
                ...duplicateContext,
                title,
                price,
                affiliateLink: resolvedAffiliate.affiliateLink || affiliateLink,
                asin
              }
            : null
      });
      const duplicateBlockedResult = handleDuplicateBlockedMainDelivery(mainDeliveryResult);
      if (duplicateBlockedResult) {
        return duplicateBlockedResult;
      }
      console.info('[TELEGRAM_MAIN_PHOTO_POST_SUCCESS]', {
        chatId: mainDeliveryResult.chatId,
        messageId: mainDeliveryResult.messageId,
        titlePreview: cleanText(title).slice(0, 120) || null,
        captionLengthAfterTrim: captionResult.afterLength,
        hasAffiliateLink: Boolean(resolvedAffiliate.affiliateLink)
      });
      console.info('[MAIN_POST_SENDPHOTO_SUCCESS]', {
        chatId: mainDeliveryResult.chatId,
        messageId: mainDeliveryResult.messageId,
        imageSource: fallbackSourceValues.imageSource
      });
      mainPostSent = true;
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'sendPhoto fehlgeschlagen.';
      console.error('[MAIN_POST_SENDPHOTO_FAILED]', {
        chatId: cleanText(String(chatId || '')) || null,
        titlePreview: cleanText(resolvedMainPostFields.title || title).slice(0, 120) || null,
        imageSource: fallbackSourceValues.imageSource,
        errorMessage
      });
      console.error('[TELEGRAM_MAIN_PHOTO_POST_ERROR]', {
        chatId: cleanText(String(chatId || '')) || null,
        titlePreview: cleanText(title).slice(0, 120) || null,
        errorMessage,
        captionLength: captionResult.afterLength,
        hasAffiliateLink: Boolean(resolvedAffiliate.affiliateLink)
      });
      console.error('[PHOTO_SEND_FAILED]', {
        chatId: cleanText(String(chatId || '')) || null,
        titlePreview: cleanText(title).slice(0, 120) || null,
        errorMessage,
        captionLength: captionResult.afterLength,
        hasAffiliateLink: Boolean(resolvedAffiliate.affiliateLink)
      });

      mainDeliveryResult = await sendTelegramPost({
        text: safeMainPostText,
        disableWebPagePreview: true,
        rabattgutscheinCode,
        chatId,
        titlePreview: title,
        hasAffiliateLink: Boolean(resolvedAffiliate.affiliateLink),
        postContext: 'deal_main_text_fallback',
        duplicateContext:
          duplicateContext && typeof duplicateContext === 'object'
            ? {
                ...duplicateContext,
                title,
                price,
                affiliateLink: resolvedAffiliate.affiliateLink || affiliateLink,
                asin
              }
            : null
      });
      const duplicateBlockedResult = handleDuplicateBlockedMainDelivery(mainDeliveryResult);
      if (duplicateBlockedResult) {
        return duplicateBlockedResult;
      }
      console.info('[PHOTO_SEND_FAILED_TEXT_FALLBACK_SENT]', {
        chatId: mainDeliveryResult.chatId,
        messageId: mainDeliveryResult.messageId,
        titlePreview: cleanText(title).slice(0, 120) || null,
        hasAffiliateLink: Boolean(resolvedAffiliate.affiliateLink)
      });
      mainPostSent = true;
    }
  } else {
    console.warn('[MAIN_POST_SKIPPED_REASON]', {
      reason: 'image_missing_send_message_used',
      titleSource: resolvedMainPostFields.titleSource,
      priceSource: resolvedMainPostFields.priceSource,
      linkSource: resolvedMainPostFields.linkSource,
      imageSource: fallbackSourceValues.imageSource
    });
    mainDeliveryResult = await sendTelegramPost({
      text: safeMainPostText,
      disableWebPagePreview: true,
      rabattgutscheinCode,
      chatId,
      titlePreview: title,
      hasAffiliateLink: Boolean(resolvedAffiliate.affiliateLink),
      postContext: 'deal_main_text_only',
      duplicateContext:
        duplicateContext && typeof duplicateContext === 'object'
          ? {
              ...duplicateContext,
              title,
              price,
              affiliateLink: resolvedAffiliate.affiliateLink || affiliateLink,
              asin
            }
          : null
    });
    const duplicateBlockedResult = handleDuplicateBlockedMainDelivery(mainDeliveryResult);
    if (duplicateBlockedResult) {
      return duplicateBlockedResult;
    }
    mainPostSent = true;
  }

  if (mainPostSent) {
    console.info('[MAIN_POST_SENT]', {
      chatId: mainDeliveryResult?.chatId || cleanText(String(chatId || '')) || null,
      messageId: mainDeliveryResult?.messageId || null,
      titlePreview: cleanText(title).slice(0, 120) || null,
      fallbackUsed: fallbackMainRequired
    });
  }

  const extraMessageIds = [];

  if (trimmedDebugInfo) {
    if (!mainPostSent) {
      console.warn('[NO_DEBUG_WITHOUT_MAIN]', {
        chatId: cleanText(String(chatId || '')) || null,
        titlePreview: cleanText(title).slice(0, 120) || null,
        reason: 'Debugpost blockiert bis Hauptpost gesendet wurde.'
      });
      console.warn('[MAIN_POST_MISSING_SEND_FALLBACK]', {
        chatId: cleanText(String(chatId || '')) || null,
        titlePreview: cleanText(title).slice(0, 120) || null,
        reason: 'Kein Hauptpost vor Debug erkannt.'
      });
      mainDeliveryResult = await sendTelegramPost({
        text: buildTelegramFallbackMainPostText({
          title: fallbackSourceValues.title,
          price: fallbackSourceValues.price,
          affiliateLink: fallbackSourceValues.linkSource === 'ownAffiliateLink' ? fallbackSourceValues.link : '',
          sourceLink: fallbackSourceValues.link,
          diagnostic: true
        }),
        disableWebPagePreview: true,
        chatId,
        titlePreview: cleanText(fallbackSourceValues.title) || '⚠️ Deal erkannt',
        hasAffiliateLink: fallbackSourceValues.linkSource === 'ownAffiliateLink',
        postContext: 'deal_main_required_fallback'
      });
      mainPostSent = true;
      console.info('[MAIN_POST_SENT]', {
        chatId: mainDeliveryResult?.chatId || cleanText(String(chatId || '')) || null,
        messageId: mainDeliveryResult?.messageId || null,
        titlePreview: cleanText(title).slice(0, 120) || null,
        fallbackUsed: true
      });
    }

    const debugDelivery = await sendTelegramPost({
      text: trimmedDebugInfo,
      disableWebPagePreview: true,
      chatId,
      titlePreview: title,
      hasAffiliateLink: false,
      postContext: 'deal_debug'
    });
    extraMessageIds.push(debugDelivery.messageId);
    console.info('[TELEGRAM_DEBUG_POST_SENT]', {
      chatId: debugDelivery.chatId,
      messageId: debugDelivery.messageId,
      titlePreview: cleanText(title).slice(0, 120) || null
    });
    console.info('[DEBUG_MESSAGE_ONLY]', {
      chatId: debugDelivery.chatId,
      messageId: debugDelivery.messageId,
      titlePreview: cleanText(title).slice(0, 120) || null
    });
    console.info('[DEBUG_POST_ONLY_UPDATED]', {
      chatId: debugDelivery.chatId,
      messageId: debugDelivery.messageId,
      titlePreview: cleanText(title).slice(0, 120) || null,
      totalMessages: 2
    });
    console.info('[DEBUG_SENT_AFTER_MAIN]', {
      chatId: debugDelivery.chatId,
      messageId: debugDelivery.messageId,
      mainPostMessageId: mainDeliveryResult?.messageId || null,
      titlePreview: cleanText(title).slice(0, 120) || null
    });
  }

  console.info('[NO_THIRD_MESSAGE]', {
    chatId: mainDeliveryResult?.chatId || cleanText(String(chatId || '')) || null,
    titlePreview: cleanText(title).slice(0, 120) || null,
    totalMessages: 1 + extraMessageIds.length,
    hasDebugPost: extraMessageIds.length > 0
  });
  console.info('[NO_THIRD_TELEGRAM_MESSAGE]', {
    chatId: mainDeliveryResult?.chatId || cleanText(String(chatId || '')) || null,
    titlePreview: cleanText(title).slice(0, 120) || null,
    totalMessages: 1 + extraMessageIds.length,
    hasDebugPost: extraMessageIds.length > 0
  });

  return {
    ...mainDeliveryResult,
    extraMessageIds,
    captionInfo: {
      beforeLength: captionResult.beforeLength,
      afterLength: captionResult.afterLength,
      cutAt: captionResult.cutAt,
      cutContext: captionResult.cutContext,
      trimmed: captionResult.trimmed,
      limit: TELEGRAM_SAFE_CAPTION_LIMIT
    },
    affiliateLink: resolvedAffiliate.affiliateLink,
    strippedForeignLink: resolvedAffiliate.strippedForeignLink
  };
}

export async function sendTelegramPost({
  text,
  uploadedFile,
  uploadedImage,
  imageUrl,
  disableWebPagePreview = false,
  rabattgutscheinCode,
  chatId,
  titlePreview = '',
  hasAffiliateLink = false,
  postContext = 'generic',
  duplicateContext = null
}) {
  const { token, chatId: envChatId } = getTelegramConfig();
  const finalChatId = (chatId || envChatId || '').toString().trim();
  const normalizedUploadedFile = normalizeUploadedFile(uploadedFile);
  const parsedUploadedImage = normalizedUploadedFile || parseUploadedImage(uploadedImage);
  const trimmedImageUrl = typeof imageUrl === 'string' ? imageUrl.trim() : '';
  const trimmedCouponCode = typeof rabattgutscheinCode === 'string' ? rabattgutscheinCode.trim() : '';
  const buttonText = getTelegramCopyButtonText().trim();
  const replyMarkup = trimmedCouponCode
    ? {
        inline_keyboard: [
          [
            {
              text: buttonText,
              copy_text: {
                text: trimmedCouponCode
              }
            }
          ]
        ]
      }
    : undefined;
  const effectiveImageUrl = parsedUploadedImage ? '' : trimmedImageUrl;
  const resolvedDisableWebPagePreview = disableWebPagePreview || (!parsedUploadedImage && !effectiveImageUrl);
  const telegramMethod = parsedUploadedImage || effectiveImageUrl ? 'sendPhoto' : 'sendMessage';
  const normalizedText = typeof text === 'string' ? text : '';
  const trimmedText = normalizedText.trim();
  const genericSplitPreview =
    postContext === 'generic'
      ? splitTelegramPhotoPostText(trimmedText)
      : { mainText: '', extraText: '', splitMarker: '', splitIndex: -1 };
  const standaloneDebugOnly =
    postContext === 'generic' &&
    looksLikeTelegramDebugText(trimmedText) &&
    !cleanText(genericSplitPreview.extraText) &&
    !/(Anzeige\/Partnerlink|🔥 Jetzt|➡️)/i.test(trimmedText);

  if (standaloneDebugOnly) {
    console.info('[MAIN_POST_REQUIRED]', {
      chatId: finalChatId || null,
      titlePreview: cleanText(titlePreview).slice(0, 120) || null,
      hasImage: Boolean(parsedUploadedImage || effectiveImageUrl),
      fallbackMainRequired: true,
      hasDebugInfo: true
    });
    console.warn('[NO_DEBUG_WITHOUT_MAIN]', {
      chatId: finalChatId || null,
      titlePreview: cleanText(titlePreview).slice(0, 120) || null,
      reason: 'Direkter Debugpost wurde in Hauptpost + Debug aufgeteilt.'
    });

    return await sendTelegramDealPost({
      title: cleanText(titlePreview) || '⚠️ Deal erkannt',
      price: 'n/a',
      affiliateLink: extractFirstUrl(trimmedText),
      debugInfo: trimmedText,
      testMode: true,
      chatId: finalChatId,
      fallbackText: trimmedText
    });
  }

  console.info('[OUTPUT_CONFIG]', {
    configSource: 'telegram_sender',
    explicitChatId: (chatId || '').toString().trim() || null,
    envChatId: envChatId || null,
    finalChatId: finalChatId || null,
    tokenConfigured: Boolean(token),
    method: telegramMethod,
    postContext
  });
  console.info('[OUTPUT_PAYLOAD]', {
    configSource: 'telegram_sender',
    textLength: trimmedText.length,
    textPreview: trimmedText.slice(0, 160),
    hasUploadedImage: Boolean(parsedUploadedImage),
    hasImageUrl: Boolean(effectiveImageUrl),
    disableWebPagePreview: resolvedDisableWebPagePreview,
    hasCouponCode: Boolean(trimmedCouponCode),
    titlePreview: cleanText(titlePreview).slice(0, 120) || null,
    hasAffiliateLink: hasAffiliateLink === true
  });

  if (!trimmedText) {
    console.warn('[NO_POST_REASON]', {
      reason: 'Telegram Send Fehler',
      detail: 'Text ist erforderlich',
      chatId: finalChatId || null,
      method: telegramMethod
    });
    console.error('[TELEGRAM_SEND_ERROR]', {
      chatId: finalChatId || null,
      method: telegramMethod,
      errorCode: null,
      errorMessage: 'Text ist erforderlich',
      titlePreview: cleanText(titlePreview).slice(0, 120) || null,
      captionLength: trimmedText.length,
      hasImage: Boolean(parsedUploadedImage || effectiveImageUrl),
      hasAffiliateLink: hasAffiliateLink === true
    });
    console.error('[TELEGRAM_FORCE_SEND_ERROR]', {
      chatId: finalChatId || null,
      method: telegramMethod,
      reason: 'Text ist erforderlich'
    });
    throw new Error('Text ist erforderlich');
  }

  if (!token) {
    console.warn('[NO_POST_REASON]', {
      reason: 'Telegram Send Fehler',
      detail: 'TELEGRAM_BOT_TOKEN fehlt im Backend',
      chatId: finalChatId || null,
      method: telegramMethod
    });
    console.error('[TELEGRAM_SEND_ERROR]', {
      chatId: finalChatId || null,
      method: telegramMethod,
      errorCode: null,
      errorMessage: 'TELEGRAM_BOT_TOKEN fehlt im Backend',
      titlePreview: cleanText(titlePreview).slice(0, 120) || null,
      captionLength: trimmedText.length,
      hasImage: Boolean(parsedUploadedImage || effectiveImageUrl),
      hasAffiliateLink: hasAffiliateLink === true
    });
    console.error('[TELEGRAM_FORCE_SEND_ERROR]', {
      chatId: finalChatId || null,
      method: telegramMethod,
      reason: 'TELEGRAM_BOT_TOKEN fehlt im Backend'
    });
    throw new Error('TELEGRAM_BOT_TOKEN fehlt im Backend');
  }

  if (!finalChatId) {
    console.warn('[NO_POST_REASON]', {
      reason: 'Telegram Send Fehler',
      detail: 'TELEGRAM_CHAT_ID fehlt im Backend',
      chatId: null,
      method: telegramMethod
    });
    console.error('[TELEGRAM_SEND_ERROR]', {
      chatId: null,
      method: telegramMethod,
      errorCode: null,
      errorMessage: 'TELEGRAM_CHAT_ID fehlt im Backend',
      titlePreview: cleanText(titlePreview).slice(0, 120) || null,
      captionLength: trimmedText.length,
      hasImage: Boolean(parsedUploadedImage || effectiveImageUrl),
      hasAffiliateLink: hasAffiliateLink === true
    });
    console.error('[TELEGRAM_FORCE_SEND_ERROR]', {
      chatId: null,
      method: telegramMethod,
      reason: 'TELEGRAM_CHAT_ID fehlt im Backend'
    });
    throw new Error('TELEGRAM_CHAT_ID fehlt im Backend');
  }

  const duplicateCheck =
    duplicateContext && typeof duplicateContext === 'object'
      ? checkAndReserveTelegramDuplicate({
          ...duplicateContext,
          chatId: finalChatId,
          titlePreview: cleanText(duplicateContext.titlePreview || titlePreview),
          postContext
        })
      : null;
  let duplicateSaved = false;
  const persistSuccessfulDuplicateSend = (deliveryResult = null) => {
    if (!duplicateCheck?.reservationTaken || duplicateSaved !== false) {
      return deliveryResult;
    }

    saveTelegramDuplicateAfterSend({
      descriptor: duplicateCheck.descriptor,
      messageId: deliveryResult?.messageId || null,
      postContext
    });
    duplicateSaved = true;
    return deliveryResult;
  };
  const releaseDuplicateReservation = () => {
    if (!duplicateCheck?.reservationTaken || duplicateSaved === true) {
      return;
    }

    releaseTelegramDuplicateReservation(duplicateCheck.descriptor);
  };

  if (duplicateCheck?.blocked) {
    return {
      method: 'duplicate_blocked',
      messageId: null,
      chatId: finalChatId,
      imageUrl: null,
      extraMessageIds: [],
      duplicateBlocked: true,
      duplicateKey: duplicateCheck.descriptor?.duplicateKey || '',
      lastSentAt: duplicateCheck.lastSentAt || null,
      duplicateReason: duplicateCheck.reason || 'DUPLICATE_WINDOW_ACTIVE',
      previousMessageId: duplicateCheck.previousMessageId || null
    };
  }

  try {
    if (parsedUploadedImage || effectiveImageUrl) {
    const photoPostContent = splitTelegramPhotoPostText(trimmedText);
    const captionResult = trimTelegramPhotoCaption(photoPostContent.mainText, TELEGRAM_SAFE_CAPTION_LIMIT);
    const captionText = captionResult.text || photoPostContent.mainText || trimmedText;

    if (captionResult.trimmed) {
      console.info('[CAPTION_TRIMMED]', {
        chatId: finalChatId,
        method: telegramMethod,
        originalTextLength: trimmedText.length,
        mainContentLengthBeforeTrim: photoPostContent.mainText.length,
        captionLengthBeforeTrim: captionResult.beforeLength,
        captionLengthAfterTrim: captionResult.afterLength,
        cutAt: captionResult.cutAt,
        cutContext: captionResult.cutContext,
        safeCaptionLimit: TELEGRAM_SAFE_CAPTION_LIMIT,
        telegramHardLimit: TELEGRAM_CAPTION_LIMIT
      });
    }

    console.info('[CAPTION_LENGTH_OK]', {
      chatId: finalChatId,
      method: telegramMethod,
      originalTextLength: trimmedText.length,
      mainContentLengthBeforeTrim: photoPostContent.mainText.length,
      captionLengthBeforeTrim: captionResult.beforeLength,
      captionLengthAfterTrim: captionResult.afterLength,
      cutAt: captionResult.cutAt,
      cutContext: captionResult.cutContext,
      safeCaptionLimit: TELEGRAM_SAFE_CAPTION_LIMIT,
      captionTrimmed: captionResult.trimmed
    });

    const photoResult = await sendSingleTelegramDelivery({
      token,
      finalChatId,
      text: captionText,
      parsedUploadedImage,
      effectiveImageUrl,
      resolvedDisableWebPagePreview: false,
      replyMarkup,
      deliveryMeta: {
        titlePreview,
        captionLength: captionResult.afterLength,
        hasAffiliateLink
      }
    });

    console.info('[PHOTO_WITH_MAIN_CONTENT_SENT]', {
      chatId: photoResult.chatId,
      messageId: photoResult.messageId,
      captionLengthBeforeTrim: captionResult.beforeLength,
      captionLengthAfterTrim: captionResult.afterLength,
      mainContentLength: photoPostContent.mainText.length,
      extraInfoLength: photoPostContent.extraText.length,
      splitMarker: photoPostContent.splitMarker || null,
      captionTrimmed: captionResult.trimmed
    });
    console.info('[PHOTO_CAPTION_MAIN_POST]', {
      chatId: photoResult.chatId,
      messageId: photoResult.messageId,
      captionLengthBeforeTrim: captionResult.beforeLength,
      captionLengthAfterTrim: captionResult.afterLength,
      cutAt: captionResult.cutAt,
      splitMarker: photoPostContent.splitMarker || null,
      hasExtraInfo: Boolean(photoPostContent.extraText)
    });

      persistSuccessfulDuplicateSend(photoResult);
      const extraTextChunks = splitTelegramTextIntoChunks(photoPostContent.extraText, TELEGRAM_MESSAGE_LIMIT);
      const extraTextResults = [];

      for (const chunk of extraTextChunks) {
        const textResult = await sendSingleTelegramDelivery({
          token,
          finalChatId,
          text: chunk,
          parsedUploadedImage: null,
          effectiveImageUrl: '',
          resolvedDisableWebPagePreview: true,
          replyMarkup: undefined,
          deliveryMeta: {
            titlePreview,
            captionLength: cleanText(chunk).length,
            hasAffiliateLink: false
          }
        });
        extraTextResults.push(textResult);
      }

      if (extraTextResults.length) {
        console.info('[DEBUG_INFO_SENT_SEPARATE]', {
          chatId: finalChatId,
          parts: extraTextResults.length,
          messageIds: extraTextResults.map((item) => item.messageId),
          extraInfoLength: photoPostContent.extraText.length,
          splitMarker: photoPostContent.splitMarker || null
        });
        console.info('[DEBUG_SENT_SEPARATE]', {
          chatId: finalChatId,
          parts: extraTextResults.length,
          messageIds: extraTextResults.map((item) => item.messageId),
          extraInfoLength: photoPostContent.extraText.length,
          splitMarker: photoPostContent.splitMarker || null
        });
      }

      return {
        method: photoResult.method,
        messageId: photoResult.messageId,
        chatId: photoResult.chatId,
        imageUrl: photoResult.imageUrl,
        extraMessageIds: extraTextResults.map((item) => item.messageId),
        duplicateKey: duplicateCheck?.descriptor?.duplicateKey || '',
        captionInfo: {
          originalTextLength: trimmedText.length,
          mainContentLength: photoPostContent.mainText.length,
          extraInfoLength: photoPostContent.extraText.length,
          beforeLength: captionResult.beforeLength,
          afterLength: captionResult.afterLength,
          cutAt: captionResult.cutAt,
          cutContext: captionResult.cutContext,
          trimmed: captionResult.trimmed,
          limit: TELEGRAM_SAFE_CAPTION_LIMIT,
          splitMarker: photoPostContent.splitMarker || null
        }
      };
    }

    if (!parsedUploadedImage && !effectiveImageUrl && trimmedText.length > TELEGRAM_MESSAGE_LIMIT) {
      const textChunks = splitTelegramTextIntoChunks(trimmedText, TELEGRAM_MESSAGE_LIMIT);
      const textResults = [];

      for (let index = 0; index < textChunks.length; index += 1) {
        const textResult = await sendSingleTelegramDelivery({
          token,
          finalChatId,
          text: textChunks[index],
          parsedUploadedImage: null,
          effectiveImageUrl: '',
          resolvedDisableWebPagePreview: true,
          replyMarkup: index === 0 ? replyMarkup : undefined,
          deliveryMeta: {
            titlePreview,
            captionLength: cleanText(textChunks[index]).length,
            hasAffiliateLink
          }
        });
        if (index === 0) {
          persistSuccessfulDuplicateSend(textResult);
        }
        textResults.push(textResult);
      }

      console.info('[TEXT_SPLIT_SENT]', {
        chatId: finalChatId,
        parts: textResults.length,
        messageIds: textResults.map((item) => item.messageId)
      });

      return {
        method: 'sendMessage',
        messageId: textResults[0]?.messageId || null,
        chatId: textResults[0]?.chatId || finalChatId,
        imageUrl: null,
        extraMessageIds: textResults.slice(1).map((item) => item.messageId),
        duplicateKey: duplicateCheck?.descriptor?.duplicateKey || ''
      };
    }

    const singleResult = await sendSingleTelegramDelivery({
      token,
      finalChatId,
      text: trimmedText,
      parsedUploadedImage,
      effectiveImageUrl,
      resolvedDisableWebPagePreview,
      replyMarkup,
      deliveryMeta: {
        titlePreview,
        captionLength: trimmedText.length,
        hasAffiliateLink
      }
    });
    persistSuccessfulDuplicateSend(singleResult);
    return {
      ...singleResult,
      duplicateKey: duplicateCheck?.descriptor?.duplicateKey || ''
    };
  } catch (error) {
    releaseDuplicateReservation();
    throw error;
  }
}

export const __testablesTelegramSender = {
  splitTelegramPhotoPostText,
  trimTelegramPhotoCaption,
  buildTelegramMainDealText,
  resolveTelegramAffiliateLink
};
