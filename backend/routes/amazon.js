import { Router } from 'express';
import { getAmazonAffiliateStatus, loadAmazonAffiliateContext, runAmazonAffiliateApiTest } from '../services/amazonAffiliateService.js';
import { classifySellerType, extractAsin, normalizeAmazonLink } from '../services/dealHistoryService.js';
import { logGeneratorDebug } from '../services/generatorFlowService.js';
import { extractSellerSignalsFromText, resolveSellerIdentity } from '../services/sellerClassificationService.js';

const router = Router();
const AMAZON_FETCH_HEADERS = {
  'User-Agent':
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36',
  'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8'
};
const AMAZON_SHORT_HOSTS = new Set(['amzn.to']);
const AMAZON_REDIRECT_LIMIT = 6;

function safeUrl(value) {
  try {
    return new URL(value);
  } catch {
    return null;
  }
}

function isAmazonShortLink(value = '') {
  const hostname = safeUrl(value)?.hostname?.toLowerCase().replace(/^www\./, '') || '';
  return AMAZON_SHORT_HOSTS.has(hostname);
}

function isRedirectStatus(status) {
  return Number(status) >= 300 && Number(status) < 400;
}

function decodeHtml(value) {
  return value
    .replace(/&amp;/g, '&')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>');
}

function decodeEscapedValue(value) {
  return value
    .replace(/\\u0026/gi, '&')
    .replace(/\\u003d/gi, '=')
    .replace(/\\u002f/gi, '/')
    .replace(/\\u0025/gi, '%')
    .replace(/\\\//g, '/')
    .replace(/\\"/g, '"');
}

function extractFirstMatch(html, patterns) {
  for (const pattern of patterns) {
    const match = html.match(pattern);
    if (match?.[1]) {
      return decodeHtml(decodeEscapedValue(match[1].trim()));
    }
  }

  return '';
}

function resolveUrlCandidate(value, baseUrl = '') {
  const trimmed = typeof value === 'string' ? decodeHtml(decodeEscapedValue(value.trim())) : '';
  if (!trimmed) {
    return '';
  }

  if (/^https?:\/\//i.test(trimmed)) {
    return trimmed;
  }

  if (trimmed.startsWith('//')) {
    return `https:${trimmed}`;
  }

  try {
    return new URL(trimmed, baseUrl || 'https://www.amazon.de/').toString();
  } catch {
    return '';
  }
}

function normalizeExtractedImageUrl(imageUrl, baseUrl = '') {
  const resolvedUrl = resolveUrlCandidate(imageUrl, baseUrl);
  if (!resolvedUrl || !/^https?:\/\//i.test(resolvedUrl)) {
    return '';
  }

  return normalizeDealImageUrl(resolvedUrl);
}

function extractDynamicImageUrl(html, baseUrl = '') {
  const matches = [...html.matchAll(/data-a-dynamic-image=(["'])([\s\S]*?)\1/gi)];

  for (const match of matches) {
    const rawJson = typeof match?.[2] === 'string' ? decodeHtml(decodeEscapedValue(match[2].trim())) : '';
    if (!rawJson) {
      continue;
    }

    try {
      const parsed = JSON.parse(rawJson);
      if (!parsed || typeof parsed !== 'object') {
        continue;
      }

      for (const candidateUrl of Object.keys(parsed)) {
        const normalizedCandidateUrl = normalizeExtractedImageUrl(candidateUrl, baseUrl);
        if (normalizedCandidateUrl) {
          return normalizedCandidateUrl;
        }
      }
    } catch {
      continue;
    }
  }

  return '';
}

function normalizeSrcSetValue(value = '') {
  const trimmed = typeof value === 'string' ? decodeHtml(decodeEscapedValue(value.trim())) : '';
  if (!trimmed) {
    return '';
  }

  return trimmed
    .split(',')
    .map((part) => part.trim().split(/\s+/)[0] || '')
    .find(Boolean);
}

function isLikelyContentImageUrl(imageUrl = '') {
  const lowered = String(imageUrl || '').toLowerCase();
  if (!lowered) {
    return false;
  }

  if (lowered.endsWith('.svg') || lowered.includes('.svg?')) {
    return false;
  }

  if (/\/(favicon|sprite|spacer|pixel|icon|logo|loading)[^/]*($|[/?#])/i.test(lowered)) {
    return /images-amazon\.com|media-amazon\.com|ssl-images-amazon/i.test(lowered);
  }

  return true;
}

function extractMetaImage(html, fieldName, baseUrl = '') {
  if (fieldName === 'og:image') {
    return normalizeExtractedImageUrl(
      extractFirstMatch(html, [/<meta[^>]+property=["']og:image["'][^>]+content=["']([^"']+)["']/i]),
      baseUrl
    );
  }

  if (fieldName === 'twitter:image') {
    return normalizeExtractedImageUrl(
      extractFirstMatch(html, [/<meta[^>]+name=["']twitter:image["'][^>]+content=["']([^"']+)["']/i]),
      baseUrl
    );
  }

  return '';
}

function extractExistingImageFields(html, baseUrl = '') {
  return {
    imageUrl: normalizeExtractedImageUrl(extractFirstMatch(html, [/"imageUrl"\s*:\s*"([^"]+)"/i]), baseUrl),
    image: normalizeExtractedImageUrl(extractFirstMatch(html, [/"large"\s*:\s*"([^"]+)"/i]), baseUrl),
    productImage: normalizeExtractedImageUrl(extractFirstMatch(html, [/"productImage"\s*:\s*"([^"]+)"/i]), baseUrl),
    previewImage: normalizeExtractedImageUrl(extractFirstMatch(html, [/"previewImage"\s*:\s*"([^"]+)"/i]), baseUrl),
    thumbnail: normalizeExtractedImageUrl(extractFirstMatch(html, [/"thumbnail"\s*:\s*"([^"]+)"/i]), baseUrl),
    images0: normalizeExtractedImageUrl(extractFirstMatch(html, [/"images"\s*:\s*\[\s*"([^"]+)"/i]), baseUrl),
    productImageUrl: normalizeExtractedImageUrl(
      extractFirstMatch(html, [/"product"\s*:\s*\{[\s\S]*?"imageUrl"\s*:\s*"([^"]+)"/i]),
      baseUrl
    ),
    dynamicImage: extractDynamicImageUrl(html, baseUrl)
  };
}

function extractAmazonProductImage(html, baseUrl = '') {
  const candidates = [
    normalizeExtractedImageUrl(
      extractFirstMatch(html, [/<img[^>]+id=["']landingImage["'][^>]+data-old-hires=["']([^"']+)["']/i]),
      baseUrl
    ),
    normalizeExtractedImageUrl(extractFirstMatch(html, [/"hiRes"\s*:\s*"([^"]+)"/i]), baseUrl),
    normalizeExtractedImageUrl(extractFirstMatch(html, [/"mainUrl"\s*:\s*"([^"]+)"/i]), baseUrl),
    normalizeExtractedImageUrl(extractFirstMatch(html, [/"landingImageUrl"\s*:\s*"([^"]+)"/i]), baseUrl),
    extractDynamicImageUrl(html, baseUrl),
    normalizeExtractedImageUrl(
      extractFirstMatch(html, [/<div[^>]+id=["']imgTagWrapperId["'][^>]*>[\s\S]*?<img[^>]+src=["']([^"']+)["']/i]),
      baseUrl
    ),
    normalizeExtractedImageUrl(
      extractFirstMatch(html, [/<img[^>]+id=["']landingImage["'][^>]+src=["']([^"']+)["']/i]),
      baseUrl
    )
  ];

  return candidates.find(Boolean) || '';
}

function extractFirstValidHtmlImage(html, baseUrl = '', excludedUrls = []) {
  const seenUrls = new Set(excludedUrls.filter(Boolean));
  const patterns = [
    /<img[^>]+data-old-hires=["']([^"']+)["']/gi,
    /<img[^>]+data-src=["']([^"']+)["']/gi,
    /<img[^>]+src=["']([^"']+)["']/gi,
    /<source[^>]+srcset=["']([^"']+)["']/gi
  ];

  for (const pattern of patterns) {
    const matches = [...html.matchAll(pattern)];
    for (const match of matches) {
      const rawCandidate = pattern.source.includes('srcset') ? normalizeSrcSetValue(match?.[1] || '') : match?.[1] || '';
      const normalizedCandidate = normalizeExtractedImageUrl(rawCandidate, baseUrl);
      if (!normalizedCandidate || seenUrls.has(normalizedCandidate) || !isLikelyContentImageUrl(normalizedCandidate)) {
        continue;
      }

      return normalizedCandidate;
    }
  }

  return '';
}

function pickFirstImageValue(fields = {}, keys = []) {
  for (const key of keys) {
    const value = fields[key];
    if (value) {
      return value;
    }
  }

  return '';
}

function resolveAmazonImage(html, options = {}) {
  const baseUrl = options.baseUrl || '';
  const paapiImage = normalizeExtractedImageUrl(options.paapiImage || '', baseUrl);
  const rawScrapeImage = extractAmazonProductImage(html, baseUrl);
  const ogImage = extractMetaImage(html, 'og:image', baseUrl);
  const twitterImage = extractMetaImage(html, 'twitter:image', baseUrl);
  const existingFields = extractExistingImageFields(html, baseUrl);
  const existingFieldImage = pickFirstImageValue(existingFields, [
    'image',
    'imageUrl',
    'thumbnail',
    'previewImage',
    'productImage',
    'images0',
    'productImageUrl',
    'dynamicImage'
  ]);
  const firstHtmlImage = extractFirstValidHtmlImage(html, baseUrl, [
    rawScrapeImage,
    ogImage,
    twitterImage,
    existingFieldImage,
    ...Object.values(existingFields)
  ]);
  const candidates = [
    { source: 'paapi', value: paapiImage },
    { source: 'amazon_scrape', value: rawScrapeImage },
    { source: 'og:image', value: ogImage },
    { source: 'twitter:image', value: twitterImage },
    { source: 'html:first_image', value: firstHtmlImage },
    { source: 'html:existing_fields', value: existingFieldImage }
  ];
  const winner = candidates.find((candidate) => candidate.value) || null;
  const resolvedImageUrl = winner?.value || null;
  const finalImageUrl = resolvedImageUrl ? normalizeDealImageUrl(resolvedImageUrl) || null : null;

  return {
    paapiImage: paapiImage || null,
    rawScrapeImage: rawScrapeImage || null,
    ogImage: ogImage || null,
    twitterImage: twitterImage || null,
    firstHtmlImage: firstHtmlImage || null,
    existingFieldImage: existingFieldImage || null,
    existingFields,
    resolvedImageUrl,
    finalImageUrl,
    selectedSource: winner?.source || 'none',
    reasonIfMissing: finalImageUrl ? null : 'no_image_found_after_scrape'
  };
}

async function resolveScrapeRequest(inputUrl) {
  let currentUrl = inputUrl;
  const redirectChain = [];

  for (let redirectIndex = 0; redirectIndex <= AMAZON_REDIRECT_LIMIT; redirectIndex += 1) {
    const response = await fetch(currentUrl, {
      headers: AMAZON_FETCH_HEADERS,
      redirect: 'manual'
    });
    const locationHeader = response.headers.get('location');
    const nextLocation = typeof locationHeader === 'string' ? locationHeader.trim() : '';

    redirectChain.push({
      url: currentUrl,
      status: response.status,
      location: nextLocation || null
    });

    if (isRedirectStatus(response.status) && nextLocation) {
      currentUrl = new URL(nextLocation, currentUrl).toString();
      continue;
    }

    return {
      response,
      resolvedUrl: response.url || currentUrl,
      redirectChain,
      wasShortLink: isAmazonShortLink(inputUrl)
    };
  }

  throw new Error('Zu viele Redirects beim Amazon-Link');
}

function normalizeDealImageUrl(imageUrl) {
  if (!imageUrl) {
    return '';
  }

  if (/images-amazon\.com|media-amazon\.com|ssl-images-amazon/i.test(imageUrl)) {
    return imageUrl.replace(/\._[^.]+_\./, '._SL1200_.');
  }

  return imageUrl;
}

function normalizeAnalysisText(value = '') {
  const normalized = decodeHtml(String(value || ''))
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .trim();

  return normalized;
}

function extractAmazonTitle(html) {
  return extractFirstMatch(html, [
    /<meta[^>]+property=["']og:title["'][^>]+content=["']([^"']+)["']/i,
    /<span[^>]+id=["']productTitle["'][^>]*>\s*([^<]+?)\s*<\/span>/i,
    /<title>\s*([^<]+?)\s*<\/title>/i
  ]);
}

function extractAmazonDescription(html) {
  return extractFirstMatch(html, [
    /<meta[^>]+name=["']description["'][^>]+content=["']([^"']+)["']/i,
    /<div[^>]+id=["']productDescription["'][^>]*>([\s\S]*?)<\/div>/i,
    /"productDescription"\s*:\s*"([^"]+)"/i
  ]);
}

function extractCanonicalUrl(html) {
  return extractFirstMatch(html, [
    /<link[^>]+rel=["']canonical["'][^>]+href=["']([^"']+)["']/i,
    /<meta[^>]+property=["']og:url["'][^>]+content=["']([^"']+)["']/i
  ]);
}

function extractAmazonPrice(html) {
  const whole = html.match(/<span[^>]+class=["'][^"']*a-price-whole[^"']*["'][^>]*>\s*([^<]+)\s*<\/span>/i)?.[1];
  const fraction = html.match(/<span[^>]+class=["'][^"']*a-price-fraction[^"']*["'][^>]*>\s*([^<]+)\s*<\/span>/i)?.[1];

  if (whole && fraction) {
    return decodeHtml(`${whole},${fraction}`);
  }

  return extractFirstMatch(html, [
    /<span[^>]+class=["'][^"']*a-offscreen[^"']*["'][^>]*>\s*([^<]+)\s*<\/span>/i,
    /"priceToPay"\s*:\s*\{"displayAmount":"([^"]+)"/i
  ]);
}

function extractAmazonOldPrice(html) {
  return extractFirstMatch(html, [
    /<span[^>]+data-a-strike=["']true["'][^>]*>\s*([^<]+)\s*<\/span>/i,
    /<span[^>]+class=["'][^"']*a-text-price[^"']*["'][^>]*>\s*<span[^>]+class=["'][^"']*a-offscreen[^"']*["'][^>]*>\s*([^<]+)\s*<\/span>/i
  ]);
}

function parseGermanNumericValue(value = '') {
  const normalized = String(value || '')
    .replace(/[^\d,.-]/g, '')
    .replace(/\.(?=\d{3}(?:\D|$))/g, '')
    .replace(',', '.');
  const parsed = Number.parseFloat(normalized);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizePercentValueText(value = '') {
  const parsed = parseGermanNumericValue(value);
  if (parsed === null) {
    return '';
  }

  return `${Number.isInteger(parsed) ? parsed : parsed.toFixed(1).replace('.', ',')}%`;
}

function normalizeEuroValueText(value = '') {
  const parsed = parseGermanNumericValue(value);
  if (parsed === null) {
    return '';
  }

  return new Intl.NumberFormat('de-DE', {
    minimumFractionDigits: parsed % 1 === 0 ? 0 : 2,
    maximumFractionDigits: 2
  }).format(parsed) + '€';
}

function normalizeCouponValueText(value = '') {
  const trimmed = stripHtml(value || '');
  if (!trimmed) {
    return '';
  }

  if (/%/.test(trimmed)) {
    const normalizedPercent = normalizePercentValueText(trimmed);
    return normalizedPercent ? `${normalizedPercent} sparen` : '';
  }

  if (/€|eur/i.test(trimmed)) {
    const normalizedEuro = normalizeEuroValueText(trimmed);
    return normalizedEuro ? `${normalizedEuro} Rabatt` : '';
  }

  return '';
}

function normalizeSubscribeValueText(value = '') {
  const trimmed = stripHtml(value || '');
  if (!trimmed) {
    return '';
  }

  if (/%/.test(trimmed)) {
    const normalizedPercent = normalizePercentValueText(trimmed);
    return normalizedPercent ? `bis zu ${normalizedPercent} extra` : '';
  }

  if (/€|eur/i.test(trimmed)) {
    const normalizedEuro = normalizeEuroValueText(trimmed);
    return normalizedEuro ? `${normalizedEuro} extra` : '';
  }

  return '';
}

function stripHtml(value) {
  return decodeHtml(value.replace(/<script[\s\S]*?<\/script>/gi, ' ').replace(/<[^>]+>/g, ' '))
    .replace(/\s+/g, ' ')
    .trim();
}

const SELLER_SOURCE_CONFIGS = [
  {
    id: 'merchantInfo_feature_div',
    source: 'merchant-info',
    windowSize: 2600
  },
  {
    id: 'merchant-info',
    source: 'merchant-info',
    windowSize: 2600
  },
  {
    id: 'desktop_merchantInfo_feature_div',
    source: 'merchant-info',
    windowSize: 2600
  },
  {
    id: 'sellerProfileTriggerId',
    source: 'seller-profile',
    windowSize: 1200
  },
  {
    id: 'shipsFromSoldByInsideBuyBox_feature_div',
    source: 'buybox',
    windowSize: 3200
  },
  {
    id: 'shipsFromSoldBy_feature_div',
    source: 'buybox',
    windowSize: 2600
  },
  {
    id: 'tabular-buybox',
    source: 'tabular-buybox',
    windowSize: 3200
  },
  {
    id: 'offerDisplayFeature_feature_div',
    source: 'offer-display',
    windowSize: 3600
  },
  {
    id: 'buybox',
    source: 'buybox',
    windowSize: 4200
  },
  {
    id: 'desktop_buybox',
    source: 'desktop-buybox',
    windowSize: 4200
  }
];
const SELLER_FALLBACK_TEXT_PATTERN =
  /verkauf(?:t)? und versand(?:et)? durch amazon|verkauf(?:t)? durch amazon|verkauft von amazon|verk(?:ä|ae)ufer\s*:?\s*amazon|versand durch amazon|versendet von amazon|ships from amazon|sold by amazon|dispatched from amazon|dispatches from amazon|fulfilled by amazon|amazon\.de/gi;
const SELLER_LOG_TEXT_LIMIT = 700;

function escapeRegex(value = '') {
  return String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function uniqueStrings(values = []) {
  return values
    .map((value) => (typeof value === 'string' ? value.trim() : ''))
    .filter(Boolean)
    .filter((value, index, allValues) => allValues.indexOf(value) === index);
}

function extractHtmlWindowsById(html = '', id = '', { windowSize = 3200, limit = 2 } = {}) {
  if (!html || !id) {
    return [];
  }

  const pattern = new RegExp(`<[^>]+id=["']${escapeRegex(id)}["'][^>]*>`, 'gi');
  const snippets = [];
  let match = pattern.exec(html);

  while (match && snippets.length < limit) {
    const startIndex = Math.max(0, match.index);
    const endIndex = Math.min(html.length, startIndex + match[0].length + windowSize);
    snippets.push(html.slice(startIndex, endIndex));
    match = pattern.exec(html);
  }

  return snippets;
}

function extractFallbackSellerTextCandidates(text = '') {
  if (!text) {
    return [];
  }

  const windows = [];
  SELLER_FALLBACK_TEXT_PATTERN.lastIndex = 0;
  let match = SELLER_FALLBACK_TEXT_PATTERN.exec(text);

  while (match) {
    const startIndex = Math.max(0, match.index - 120);
    const endIndex = Math.min(text.length, match.index + String(match[0] || '').length + 180);
    windows.push(text.slice(startIndex, endIndex).trim());
    match = SELLER_FALLBACK_TEXT_PATTERN.exec(text);
  }

  return uniqueStrings(windows).slice(0, 6);
}

function scoreSellerCandidateAnalysis(analysis = {}) {
  const signals = analysis.signals || {};
  let score = 0;

  if (signals.hasAmazonDirectPhrase === true) {
    score += 12;
  }

  if (signals.soldByAmazon !== null) {
    score += 4;
  }

  if (signals.shippedByAmazon !== null) {
    score += 4;
  }

  score += Math.min(6, Array.isArray(signals.matchedPatterns) ? signals.matchedPatterns.length : 0);

  if (analysis.source === 'merchant-info') {
    score += 4;
  } else if (analysis.source === 'buybox' || analysis.source === 'tabular-buybox') {
    score += 3;
  } else if (analysis.source === 'offer-display' || analysis.source === 'desktop-buybox') {
    score += 2;
  } else if (analysis.source === 'seller-profile') {
    score += 1;
  }

  return score;
}

function collectSellerDetectionCandidates(html = '') {
  const candidates = [];
  const strippedHtml = stripHtml(html);

  for (const config of SELLER_SOURCE_CONFIGS) {
    const snippets = extractHtmlWindowsById(html, config.id, {
      windowSize: config.windowSize,
      limit: 2
    });

    for (const snippet of snippets) {
      const text = stripHtml(snippet).slice(0, 1200).trim();
      if (!text) {
        continue;
      }

      candidates.push({
        source: config.source,
        sourceId: config.id,
        text
      });
    }
  }

  for (const text of extractFallbackSellerTextCandidates(strippedHtml)) {
    candidates.push({
      source: 'fallback-text',
      sourceId: 'fallback-text',
      text
    });
  }

  return candidates.filter((candidate, index, allCandidates) => {
    return (
      allCandidates.findIndex((entry) => entry.source === candidate.source && entry.text === candidate.text) === index
    );
  });
}

function extractCouponDetails(html) {
  const strippedHtml = stripHtml(html);
  const couponSnippet = extractFirstMatch(html, [
    /<div[^>]+id=["']couponBadge["'][^>]*>([\s\S]*?)<\/div>/i,
    /<div[^>]+class=["'][^"']*coupon[^"']*["'][^>]*>([\s\S]*?)<\/div>/i,
    /data-coupon=["']([^"']+)["']/i,
    /"couponBadgeText"\s*:\s*"([^"]+)"/i,
    /"couponText"\s*:\s*"([^"]+)"/i
  ]);
  const couponContext =
    couponSnippet ||
    strippedHtml.match(/(?:coupon|coupon anwenden|coupon aktivieren|gutschein|sparen)[^.]{0,140}/i)?.[0] ||
    '';
  const couponDetected = /coupon|gutschein/i.test(couponContext) || /coupon|gutschein/i.test(strippedHtml);
  const couponValueRaw =
    couponContext.match(/(\d{1,3}(?:[.,]\d{1,2})?\s*%)/i)?.[1] ||
    couponContext.match(/(\d{1,3}(?:[.,]\d{1,2})?\s*(?:€|eur))/i)?.[1] ||
    '';
  const couponValue = normalizeCouponValueText(couponValueRaw);

  if (couponDetected) {
    console.info('[COUPON_DETECTED]', {
      detected: true,
      context: stripHtml(couponContext).slice(0, 160)
    });
  }

  if (couponValue) {
    console.info('[COUPON_VALUE_FOUND]', {
      couponValue
    });
  }

  return {
    couponDetected,
    couponValue,
    couponContext: stripHtml(couponContext)
  };
}

function extractSubscribeDetails(html) {
  const strippedHtml = stripHtml(html);
  const subscribeSnippet = extractFirstMatch(html, [
    /<div[^>]+id=["']snsAccordion["'][^>]*>([\s\S]*?)<\/div>/i,
    /<div[^>]+id=["']subscribeAndSave[^"']*["'][^>]*>([\s\S]*?)<\/div>/i,
    /"subscribeAndSave"\s*:\s*"([^"]+)"/i
  ]);
  const subscribeContext =
    subscribeSnippet ||
    strippedHtml.match(/(?:spar-abo|subscribe\s*&\s*save|spare bis zu)[^.]{0,160}/i)?.[0] ||
    '';
  const subscribeDetected = /spar-abo|subscribe\s*&\s*save|spare bis zu/i.test(subscribeContext) || /spar-abo|subscribe\s*&\s*save/i.test(strippedHtml);
  const subscribeValueRaw =
    subscribeContext.match(/(\d{1,3}(?:[.,]\d{1,2})?\s*%)/i)?.[1] ||
    subscribeContext.match(/(\d{1,3}(?:[.,]\d{1,2})?\s*(?:€|eur))/i)?.[1] ||
    '';
  const subscribeDiscount = normalizeSubscribeValueText(subscribeValueRaw);

  if (subscribeDetected) {
    console.info('[SUBSCRIBE_DETECTED]', {
      detected: true,
      context: stripHtml(subscribeContext).slice(0, 160)
    });
  }

  if (subscribeDiscount) {
    console.info('[SUBSCRIBE_VALUE_FOUND]', {
      subscribeDiscount
    });
  }

  return {
    subscribeDetected,
    subscribeDiscount,
    subscribeContext: stripHtml(subscribeContext)
  };
}

function parseDiscountDescriptor(value = '') {
  const trimmed = String(value || '').trim();
  if (!trimmed) {
    return null;
  }

  if (/%/.test(trimmed)) {
    const percentValue = parseGermanNumericValue(trimmed);
    return percentValue === null ? null : { type: 'percent', value: percentValue };
  }

  if (/€|eur/i.test(trimmed)) {
    const amountValue = parseGermanNumericValue(trimmed);
    return amountValue === null ? null : { type: 'amount', value: amountValue };
  }

  return null;
}

function calculateFinalPriceFromDiscounts(basePriceText = '', couponValue = '', subscribeDiscount = '') {
  const basePriceValue = parseGermanNumericValue(basePriceText);
  if (basePriceValue === null) {
    return {
      finalPriceCalculated: false,
      finalPrice: '',
      finalPriceValue: null
    };
  }

  const couponDescriptor = parseDiscountDescriptor(couponValue);
  const subscribeDescriptor = parseDiscountDescriptor(subscribeDiscount);
  let finalPriceValue = basePriceValue;
  let appliedDiscount = false;

  if (couponDescriptor) {
    appliedDiscount = true;
    finalPriceValue =
      couponDescriptor.type === 'percent'
        ? finalPriceValue * (1 - couponDescriptor.value / 100)
        : finalPriceValue - couponDescriptor.value;
  }

  if (subscribeDescriptor) {
    appliedDiscount = true;
    finalPriceValue =
      subscribeDescriptor.type === 'percent'
        ? finalPriceValue * (1 - subscribeDescriptor.value / 100)
        : finalPriceValue - subscribeDescriptor.value;
  }

  if (!appliedDiscount || !Number.isFinite(finalPriceValue) || finalPriceValue <= 0) {
    return {
      finalPriceCalculated: false,
      finalPrice: '',
      finalPriceValue: null
    };
  }

  const finalPrice = new Intl.NumberFormat('de-DE', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  }).format(finalPriceValue);

  console.info('[FINAL_PRICE_CALCULATED]', {
    basePrice: basePriceText,
    couponValue,
    subscribeDiscount,
    finalPrice
  });

  return {
    finalPriceCalculated: true,
    finalPrice,
    finalPriceValue
  };
}

function extractAmazonBulletPoints(html) {
  const bulletMatches = [
    ...html.matchAll(
      /<div[^>]+id=["']feature-bullets["'][^>]*>[\s\S]*?<span[^>]+class=["'][^"']*a-list-item[^"']*["'][^>]*>([\s\S]*?)<\/span>/gi
    ),
    ...html.matchAll(
      /<li[^>]+class=["'][^"']*a-spacing-mini[^"']*["'][^>]*>[\s\S]*?<span[^>]*>([\s\S]*?)<\/span>/gi
    )
  ];

  return bulletMatches
    .map((match) => stripHtml(match?.[1] || ''))
    .filter(Boolean)
    .filter((value, index, values) => values.indexOf(value) === index)
    .slice(0, 6);
}

export function extractSellerInfoFromAmazonHtml(html = '') {
  const candidateAnalyses = collectSellerDetectionCandidates(html)
    .map((candidate) => {
      const signals = extractSellerSignalsFromText(candidate.text, {
        detectionSource: candidate.source
      });

      return {
        ...candidate,
        signals,
        score: 0
      };
    })
    .map((candidate) => ({
      ...candidate,
      score: scoreSellerCandidateAnalysis(candidate)
    }));

  const bestCandidate = [...candidateAnalyses].sort((left, right) => right.score - left.score)[0] || null;
  const matchedCandidates = candidateAnalyses.filter((candidate) => {
    return (
      candidate.signals.hasAmazonDirectPhrase === true ||
      candidate.signals.soldByAmazon !== null ||
      candidate.signals.shippedByAmazon !== null ||
      (candidate.signals.matchedPatterns || []).length > 0
    );
  });
  const combinedAmazonMatchedCandidates = matchedCandidates.filter((candidate) => candidate.signals.hasCombinedAmazonMatch === true);
  const soldByAmazon =
    matchedCandidates.some((candidate) => candidate.signals.hasAmazonDirectPhrase === true || candidate.signals.soldByAmazon === true)
      ? true
      : matchedCandidates.some((candidate) => candidate.signals.soldByAmazon === false)
        ? false
        : null;
  const shippedByAmazon =
    matchedCandidates.some(
      (candidate) => candidate.signals.hasAmazonDirectPhrase === true || candidate.signals.shippedByAmazon === true
    )
      ? true
      : matchedCandidates.some((candidate) => candidate.signals.shippedByAmazon === false)
        ? false
        : null;
  const matchedPatterns = uniqueStrings(matchedCandidates.flatMap((candidate) => candidate.signals.matchedPatterns || []));
  const matchedCombinedAmazonPatterns = uniqueStrings(
    combinedAmazonMatchedCandidates.flatMap((candidate) => candidate.signals.matchedDirectAmazonPatterns || [])
  );
  const contributingSources = uniqueStrings(
    matchedCandidates
      .filter((candidate) => {
        if (candidate.signals.hasAmazonDirectPhrase === true) {
          return true;
        }

        if (soldByAmazon === true && candidate.signals.soldByAmazon === true) {
          return true;
        }

        if (soldByAmazon === false && candidate.signals.soldByAmazon === false) {
          return true;
        }

        if (shippedByAmazon === true && candidate.signals.shippedByAmazon === true) {
          return true;
        }

        if (shippedByAmazon === false && candidate.signals.shippedByAmazon === false) {
          return true;
        }

        return false;
      })
      .map((candidate) => candidate.source)
  );
  const rawDetectionSources = contributingSources.length ? contributingSources : bestCandidate?.source ? [bestCandidate.source] : [];
  const detectionSources =
    rawDetectionSources.some((source) => source !== 'fallback-text')
      ? rawDetectionSources.filter((source) => source !== 'fallback-text')
      : rawDetectionSources;
  const hasCombinedAmazonMatch = combinedAmazonMatchedCandidates.length > 0;
  const sellerDetectionSource = hasCombinedAmazonMatch
    ? 'combined-seller-shipping-text'
    : detectionSources.join(' + ') || 'unknown';
  const sellerIdentity = resolveSellerIdentity({
    soldByAmazon,
    shippedByAmazon,
    sellerDetectionSource,
    detectionSources,
    matchedPatterns,
    sellerDetails: {
      merchantText: bestCandidate?.signals?.merchantText || '',
      detectionSource: sellerDetectionSource,
      detectionSources,
      matchedPatterns,
      matchedDirectAmazonPatterns: matchedCombinedAmazonPatterns,
      hasCombinedAmazonMatch
    }
  });

  return {
    sellerType: classifySellerType({
      soldByAmazon,
      shippedByAmazon
    }),
    sellerClass: sellerIdentity.sellerClass,
    soldByAmazon: sellerIdentity.soldByAmazon,
    shippedByAmazon: sellerIdentity.shippedByAmazon,
    sellerDetails: {
      ...sellerIdentity.details,
      merchantText: bestCandidate?.signals?.merchantText || '',
      detectionSource: sellerDetectionSource,
      detectionSources,
      matchedPatterns,
      matchedDirectAmazonPatterns: matchedCombinedAmazonPatterns,
      hasCombinedAmazonMatch
    },
    sellerDebug: {
      rawText: bestCandidate?.signals?.merchantText || '',
      rawTextPreview: (bestCandidate?.signals?.merchantText || '').slice(0, SELLER_LOG_TEXT_LIMIT),
      matchedPatterns,
      matchedCombinedAmazonPatterns,
      hasCombinedAmazonMatch,
      detectionSources,
      candidateCount: candidateAnalyses.length,
      matchedCandidates: matchedCandidates.map((candidate) => ({
        source: candidate.source,
        sourceId: candidate.sourceId,
        score: candidate.score,
        soldByAmazon: candidate.signals.soldByAmazon,
        shippedByAmazon: candidate.signals.shippedByAmazon,
        matchedPatterns: candidate.signals.matchedPatterns || [],
        textPreview: (candidate.signals.merchantText || '').slice(0, SELLER_LOG_TEXT_LIMIT)
      }))
    }
  };
}

function extractSellerProfileUrl(html = '', baseUrl = '') {
  const directMatch = extractFirstMatch(html, [
    /<a[^>]+id=["']sellerProfileTriggerId["'][^>]+href=["']([^"']+)["']/i,
    /<a[^>]+href=["']([^"']*(?:\/sp\?|seller=|\/gp\/help\/seller\/)[^"']*)["'][^>]*id=["']sellerProfileTriggerId["'][^>]*>/i,
    /<a[^>]+href=["']([^"']*(?:\/sp\?|seller=|\/gp\/help\/seller\/)[^"']*)["'][^>]*>/i
  ]);

  return resolveUrlCandidate(directMatch, baseUrl);
}

function convertSellerPeriodToMonths(rawAmount = '', rawUnit = '') {
  const amount = Number.parseInt(String(rawAmount || '').trim(), 10);
  if (!Number.isFinite(amount) || amount <= 0) {
    return null;
  }

  const normalizedUnit = normalizeAnalysisText(rawUnit);
  if (normalizedUnit.startsWith('jahr')) {
    return amount * 12;
  }

  if (normalizedUnit.startsWith('monat')) {
    return amount;
  }

  return null;
}

export function extractFbmSellerProfileFromHtml(html = '') {
  const sellerName = stripHtml(
    extractFirstMatch(html, [
      /<span[^>]+id=["']sellerName["'][^>]*>([\s\S]*?)<\/span>/i,
      /<h1[^>]*>([\s\S]*?)<\/h1>/i,
      /<title>\s*([^<]+?)\s*<\/title>/i
    ])
  );
  const strippedText = stripHtml(html);
  const normalizedText = normalizeAnalysisText(strippedText);
  const percentMatch =
    normalizedText.match(/(\d{1,3})\s*%\s*positive bewertungen/) ||
    normalizedText.match(/(\d{1,3})\s*%\s*positive ratings/);
  const periodMatch =
    normalizedText.match(/letzten\s+(\d{1,2})\s+(monat|monaten|jahr|jahren)/) ||
    normalizedText.match(/last\s+(\d{1,2})\s+(month|months|year|years)/);
  const positivePercent = percentMatch ? Number.parseInt(percentMatch[1], 10) : null;
  const periodMonths = periodMatch ? convertSellerPeriodToMonths(periodMatch[1], periodMatch[2]) : null;
  const periodLabel = periodMonths !== null ? `${periodMonths} Monate` : '';
  const profileOk = positivePercent !== null && periodMonths !== null && positivePercent >= 80 && periodMonths >= 12;
  const hasProfileData = positivePercent !== null || periodMonths !== null;
  const reason = profileOk
    ? 'FBM-Haendlerprofil erfuellt mindestens 80% positive Bewertungen und 12 Monate Historie.'
    : positivePercent === null && periodMonths === null
      ? 'Keine verwertbaren FBM-Haendlerprofil-Daten gefunden.'
      : positivePercent !== null && positivePercent < 80
        ? `FBM-Haendlerprofil blockiert: nur ${positivePercent}% positive Bewertungen.`
        : periodMonths !== null && periodMonths < 12
          ? `FBM-Haendlerprofil blockiert: nur ${periodMonths} Monate Historie.`
          : 'FBM-Haendlerprofil unvollstaendig.';

  return {
    required: true,
    checked: true,
    status: profileOk ? 'ok' : hasProfileData ? 'blocked' : 'missing',
    sellerName,
    positivePercent,
    periodMonths,
    periodLabel,
    profileOk,
    fbmAllowed: profileOk,
    hasProfileData,
    reason
  };
}

async function loadFbmSellerProfileContext({ html = '', baseUrl = '', asin = '' } = {}) {
  const sellerProfileUrl = extractSellerProfileUrl(html, baseUrl);

  if (!sellerProfileUrl) {
    console.info('[FBM_SELLER_PROFILE_BLOCKED]', {
      asin,
      reason: 'Kein Haendlerprofil-Link auf der Amazon-Seite gefunden.'
    });
    return {
      required: true,
      checked: false,
      status: 'missing',
      sellerName: '',
      positivePercent: null,
      periodMonths: null,
      periodLabel: '',
      profileOk: false,
      fbmAllowed: false,
      hasProfileData: false,
      profileUrl: '',
      reason: 'Kein Haendlerprofil-Link gefunden.'
    };
  }

  console.info('[FBM_SELLER_PROFILE_CHECK_START]', {
    asin,
    sellerProfileUrl
  });

  try {
    const response = await fetch(sellerProfileUrl, {
      headers: AMAZON_FETCH_HEADERS
    });
    const profileHtml = await response.text();

    if (response.status === 403 || /captcha|robot check|sorry/i.test(profileHtml)) {
      throw new Error('Amazon blockiert das FBM-Haendlerprofil.');
    }

    if (!response.ok) {
      throw new Error(`FBM-Haendlerprofil konnte nicht geladen werden (${response.status}).`);
    }

    const sellerProfile = extractFbmSellerProfileFromHtml(profileHtml);
    if (sellerProfile.positivePercent !== null) {
      console.info('[FBM_SELLER_RATING_FOUND]', {
        asin,
        sellerProfileUrl,
        positivePercent: sellerProfile.positivePercent
      });
    }
    if (sellerProfile.periodMonths !== null) {
      console.info('[FBM_SELLER_PERIOD_FOUND]', {
        asin,
        sellerProfileUrl,
        periodMonths: sellerProfile.periodMonths
      });
    }
    if (sellerProfile.profileOk === true) {
      console.info('[FBM_SELLER_PROFILE_OK]', {
        asin,
        sellerProfileUrl,
        positivePercent: sellerProfile.positivePercent,
        periodMonths: sellerProfile.periodMonths
      });
    } else {
      console.info('[FBM_SELLER_PROFILE_BLOCKED]', {
        asin,
        sellerProfileUrl,
        positivePercent: sellerProfile.positivePercent,
        periodMonths: sellerProfile.periodMonths,
        reason: sellerProfile.reason
      });
    }

    return {
      ...sellerProfile,
      profileUrl: sellerProfileUrl
    };
  } catch (error) {
    const reason = error instanceof Error ? error.message : 'FBM-Haendlerprofil konnte nicht geprueft werden.';
    console.info('[FBM_SELLER_PROFILE_BLOCKED]', {
      asin,
      sellerProfileUrl,
      reason
    });
    return {
      required: true,
      checked: false,
      status: 'error',
      sellerName: '',
      positivePercent: null,
      periodMonths: null,
      periodLabel: '',
      profileOk: false,
      fbmAllowed: false,
      hasProfileData: false,
      profileUrl: sellerProfileUrl,
      reason
    };
  }
}

export async function scrapeAmazonProduct(inputUrl = '') {
  if (!inputUrl || typeof inputUrl !== 'string' || !inputUrl.trim()) {
    const error = new Error('Kein url uebergeben');
    error.code = 'MISSING_URL';
    error.statusCode = 400;
    throw error;
  }

  const trimmedUrl = inputUrl.trim();
  logGeneratorDebug('api.amazon.scrape.request', {
    url: trimmedUrl
  });

  if (!/^https?:\/\//i.test(trimmedUrl)) {
    const error = new Error('Ungueltige URL');
    error.code = 'INVALID_URL';
    error.statusCode = 400;
    throw error;
  }

  const resolvedRequest = await resolveScrapeRequest(trimmedUrl);
  const response = resolvedRequest.response;
  const resolvedUrl = resolvedRequest.resolvedUrl || trimmedUrl;

  logGeneratorDebug('api.amazon.scrape.redirect_resolution', {
    originalUrl: trimmedUrl,
    resolvedUrl,
    wasShortLink: resolvedRequest.wasShortLink,
    redirectCount: Math.max(0, resolvedRequest.redirectChain.length - 1),
    redirectChain: resolvedRequest.redirectChain
  });

  const html = await response.text();

  if (response.status === 403 || /captcha|robot check|sorry/i.test(html)) {
    const error = new Error('Amazon blockiert den Scrape-Zugriff');
    error.code = 'AMAZON_BLOCKED';
    error.statusCode = 502;
    throw error;
  }

  if (!response.ok) {
    const error = new Error(`Scrape failed (${response.status})`);
    error.code = 'SCRAPE_FAILED';
    error.statusCode = 502;
    throw error;
  }

  const canonicalUrl = extractCanonicalUrl(html);
  const productTitle = extractAmazonTitle(html) || '';
  const productDescription = stripHtml(extractAmazonDescription(html) || '');
  const bulletPoints = extractAmazonBulletPoints(html);
  const basePrice = extractAmazonPrice(html) || '';
  const couponDetails = extractCouponDetails(html);
  const subscribeDetails = extractSubscribeDetails(html);
  const finalPriceResult = calculateFinalPriceFromDiscounts(basePrice, couponDetails.couponValue, subscribeDetails.subscribeDiscount);
  const finalUrl = canonicalUrl || resolvedUrl || trimmedUrl;
  const asin = extractAsin(canonicalUrl) || extractAsin(resolvedUrl) || extractAsin(trimmedUrl) || '';
  const normalizedUrl = normalizeAmazonLink(finalUrl);
  const sellerInfo = extractSellerInfoFromAmazonHtml(html);
  const sellerProfile =
    sellerInfo.sellerClass === 'FBM_THIRDPARTY'
      ? await loadFbmSellerProfileContext({
          html,
          baseUrl: finalUrl,
          asin
        })
      : {
          required: false,
          checked: false,
          status: 'not_needed',
          sellerName: '',
          positivePercent: null,
          periodMonths: null,
          periodLabel: '',
          profileOk: false,
          fbmAllowed: false,
          hasProfileData: false,
          profileUrl: '',
          reason: 'Haendlerprofil fuer diesen Seller-Typ nicht noetig.'
        };
  const paapiContext = asin ? await loadAmazonAffiliateContext({ asin }) : null;
  const paapiImage = paapiContext?.available ? paapiContext.result?.imageUrl || '' : '';
  const imageResolution = resolveAmazonImage(html, {
    baseUrl: finalUrl,
    paapiImage
  });
  const finalImageUrl = imageResolution.finalImageUrl;
  const imageDebug = {
    rawScrapeImage: imageResolution.rawScrapeImage,
    paapiImage: imageResolution.paapiImage,
    ogImage: imageResolution.ogImage,
    twitterImage: imageResolution.twitterImage,
    firstHtmlImage: imageResolution.firstHtmlImage,
    existingFieldImage: imageResolution.existingFieldImage,
    resolvedImageUrl: imageResolution.resolvedImageUrl,
    finalImageUrl,
    selectedSource: imageResolution.selectedSource,
    reason: imageResolution.reasonIfMissing,
    paapiStatus: paapiContext?.status || (asin ? 'not_requested' : 'missing_asin'),
    paapiReason: paapiContext?.available ? null : paapiContext?.reason || null,
    resolvedUrl,
    wasShortLink: resolvedRequest.wasShortLink,
    redirectCount: Math.max(0, resolvedRequest.redirectChain.length - 1)
  };

  logGeneratorDebug('api.amazon.scrape.image_resolution', {
    url: trimmedUrl,
    resolvedUrl,
    asin,
    paapiImage: imageDebug.paapiImage,
    rawScrapeImage: imageDebug.rawScrapeImage,
    ogImage: imageDebug.ogImage,
    twitterImage: imageDebug.twitterImage,
    resolvedImageUrl: imageDebug.resolvedImageUrl,
    finalImageUrl: imageDebug.finalImageUrl,
    reasonIfMissing: imageDebug.reason,
    selectedSource: imageDebug.selectedSource
  });

  logGeneratorDebug('api.amazon.scrape.success', {
    url: trimmedUrl,
    resolvedUrl,
    asin,
    sellerType: sellerInfo.sellerType,
    sellerClass: sellerInfo.sellerClass,
    soldByAmazon: sellerInfo.soldByAmazon,
    shippedByAmazon: sellerInfo.shippedByAmazon,
    hasImage: Boolean(finalImageUrl),
    normalizedUrl,
    finalImageUrl,
    selectedImageSource: imageResolution.selectedSource,
    reasonIfMissing: imageResolution.reasonIfMissing
  });
  console.info('[SELLER_DETAILS]', {
    asin,
    sellerType: sellerInfo.sellerType,
    sellerClass: sellerInfo.sellerClass,
    soldByAmazon: sellerInfo.soldByAmazon,
    shippedByAmazon: sellerInfo.shippedByAmazon,
    merchantText: sellerInfo.sellerDetails?.merchantText || '',
    detectionSource: sellerInfo.sellerDetails?.detectionSource || 'unknown'
  });
  console.info('[SELLER_RAW_TEXT]', {
    asin,
    detectionSource: sellerInfo.sellerDetails?.detectionSource || 'unknown',
    rawText: sellerInfo.sellerDebug?.rawTextPreview || ''
  });
  if (sellerInfo.sellerDebug?.hasCombinedAmazonMatch === true) {
    console.info('[SELLER_COMBINED_AMAZON_MATCH]', {
      asin,
      detectionSource: sellerInfo.sellerDetails?.detectionSource || 'unknown',
      matchedCombinedAmazonPatterns: sellerInfo.sellerDebug?.matchedCombinedAmazonPatterns || [],
      rawText: sellerInfo.sellerDebug?.rawTextPreview || ''
    });
  }
  console.info('[SELLER_TEXT_MATCHED]', {
    asin,
    sellerClass: sellerInfo.sellerClass,
    matchedPatterns: sellerInfo.sellerDetails?.matchedPatterns || [],
    matchedCandidates: sellerInfo.sellerDebug?.matchedCandidates || []
  });
  console.info('[SELLER_TYPE_DETECTED]', {
    asin,
    sellerType: sellerInfo.sellerType
  });
  console.info('[SELLER_CLASS_DETECTED]', {
    asin,
    sellerClass: sellerInfo.sellerClass
  });
  console.info('[SELLER_DETECTION_SOURCE]', {
    asin,
    detectionSource: sellerInfo.sellerDetails?.detectionSource || 'unknown',
    detectionSources: sellerInfo.sellerDetails?.detectionSources || []
  });
  if (sellerInfo.sellerClass === 'AMAZON_DIRECT') {
    console.info('[AMAZON_DIRECT_CONFIRMED]', {
      asin,
      sellerClass: sellerInfo.sellerClass,
      detectionSource: sellerInfo.sellerDetails?.detectionSource || 'unknown'
    });
  }

  return {
    success: true,
    title: productTitle,
    productTitle,
    productDescription,
    bulletPoints,
    imageUrl: finalImageUrl,
    image: finalImageUrl,
    productImage: finalImageUrl,
    previewImage: finalImageUrl,
    thumbnail: finalImageUrl,
    images: finalImageUrl ? [finalImageUrl] : [],
    product: {
      imageUrl: finalImageUrl
    },
    basePrice,
    price: finalPriceResult.finalPriceCalculated ? finalPriceResult.finalPrice : basePrice,
    oldPrice: extractAmazonOldPrice(html) || '',
    couponDetected: couponDetails.couponDetected,
    couponValue: couponDetails.couponValue,
    subscribeDetected: subscribeDetails.subscribeDetected,
    subscribeDiscount: subscribeDetails.subscribeDiscount,
    finalPrice: finalPriceResult.finalPrice,
    finalPriceCalculated: finalPriceResult.finalPriceCalculated,
    asin,
    finalUrl,
    resolvedUrl,
    originalUrl: trimmedUrl,
    normalizedUrl,
    sellerType: sellerInfo.sellerType,
    sellerClass: sellerInfo.sellerClass,
    soldByAmazon: sellerInfo.soldByAmazon,
    shippedByAmazon: sellerInfo.shippedByAmazon,
    sellerDetails: {
      ...(sellerInfo.sellerDetails || {}),
      sellerProfile
    },
    sellerProfile,
    imageDebug
  };
}

async function handleScrape(req, res) {
  try {
    return res.status(200).json(await scrapeAmazonProduct(req.body?.url));
  } catch (error) {
    logGeneratorDebug('api.amazon.scrape.error', {
      error: error instanceof Error ? error.message : 'Scrape failed'
    });
    return res.status(error?.statusCode && Number.isFinite(Number(error.statusCode)) ? Number(error.statusCode) : 500).json({
      success: false,
      error: error instanceof Error ? `Scrape failed: ${error.message}` : 'Scrape failed',
      code: error instanceof Error ? error.code || 'SCRAPE_FAILED' : 'SCRAPE_FAILED'
    });
  }
}

router.get('/status', (req, res) => {
  try {
    res.json(getAmazonAffiliateStatus());
  } catch (error) {
    res.status(500).json({
      error: error instanceof Error ? error.message : 'Amazon API Status konnte nicht geladen werden.'
    });
  }
});

router.get('/test', async (req, res) => {
  try {
    res.json(
      await runAmazonAffiliateApiTest({
        asin: req.query.asin
      })
    );
  } catch (error) {
    res.status(error?.statusCode && Number.isFinite(Number(error.statusCode)) ? Number(error.statusCode) : 400).json({
      error: error instanceof Error ? error.message : 'Amazon API Test fehlgeschlagen.',
      code: error instanceof Error ? error.code || 'AMAZON_API_TEST_FAILED' : 'AMAZON_API_TEST_FAILED'
    });
  }
});

router.post('/scrape', handleScrape);

export default router;
