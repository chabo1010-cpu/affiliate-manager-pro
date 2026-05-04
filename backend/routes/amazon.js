import { Router } from 'express';
import {
  getAmazonAffiliateStatus,
  loadAmazonAffiliateContext,
  runAmazonAffiliateApiTest,
  testCreatorApi
} from '../services/amazonAffiliateService.js';
import { buildAmazonAffiliateLinkRecord, classifySellerType, extractAsin, normalizeAmazonLink } from '../services/dealHistoryService.js';
import { logGeneratorDebug } from '../services/generatorFlowService.js';
import { extractSellerSignalsFromText, resolveSellerIdentity } from '../services/sellerClassificationService.js';

const router = Router();
const AMAZON_FETCH_HEADERS = {
  'User-Agent':
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36',
  'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8'
};
const MANUAL_GENERATOR_SHORT_HOSTS = new Set(['amzn.to', 'amzlink.to', 'amazon.to']);
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
  return Boolean(hostname && (MANUAL_GENERATOR_SHORT_HOSTS.has(hostname) || hostname.endsWith('.to')));
}

function isAmazonHostname(value = '') {
  return /(^|\.)amazon\./i.test(String(value || '').toLowerCase());
}

function isAmazonDirectLink(value = '') {
  const parsed = safeUrl(value);
  const hostname = parsed?.hostname?.toLowerCase().replace(/^www\./, '') || '';
  return Boolean(hostname && isAmazonHostname(hostname) && extractAsin(value));
}

function isRedirectStatus(status) {
  return Number(status) >= 300 && Number(status) < 400;
}

function buildManualGeneratorInputError(message, code, statusCode = 400) {
  const error = new Error(message);
  error.code = code;
  error.statusCode = statusCode;
  return error;
}

function cleanString(value = '') {
  return typeof value === 'string' ? value.trim() : '';
}

function hasVerifiedManualShortlinkProductData(result = {}) {
  const finalTitle = cleanString(result.title || result.productTitle);
  const finalPrice = cleanString(result.finalPrice || result.price);
  const finalImageUrl = cleanString(
    normalizeDealImageUrl(
      result.imageUrl ||
        result.image ||
        result.productImage ||
        result.previewImage ||
        result.thumbnail ||
        result.product?.imageUrl ||
        ''
    )
  );

  return Boolean(finalTitle && finalPrice && finalImageUrl);
}

function finalizeManualGeneratorScrapeResponse(inputUrl = '', result = {}) {
  const manualOriginalUrl = cleanString(inputUrl);

  if (!manualOriginalUrl || !result || typeof result !== 'object' || result.inputMode !== 'shortlink') {
    return result;
  }

  const manualProductDataVerified = hasVerifiedManualShortlinkProductData(result);
  const finalizedResult = {
    ...result,
    manualOriginalUrl,
    manualProductDataVerified,
    preserveInputLink: manualProductDataVerified
  };

  if (manualProductDataVerified) {
    console.info('[MANUAL_OUTPUT_LINK_PRESERVED]', {
      manualOriginalUrl,
      resolvedUrl: cleanString(result.resolvedUrl || result.normalizedUrl || result.finalUrl) || null,
      outputLink: manualOriginalUrl,
      asin: cleanString(result.asin).toUpperCase() || null
    });
  }

  return finalizedResult;
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

function cleanPaapiText(value) {
  if (typeof value === 'string') {
    return value.trim();
  }

  if (typeof value === 'number' && Number.isFinite(value)) {
    return String(value);
  }

  return '';
}

function formatPaapiAmount(amount, currency = 'EUR') {
  const numericAmount = Number(amount);
  if (!Number.isFinite(numericAmount)) {
    return '';
  }

  try {
    return new Intl.NumberFormat('de-DE', {
      style: 'currency',
      currency: cleanPaapiText(currency) || 'EUR'
    })
      .format(numericAmount)
      .replace(/\s/g, '');
  } catch {
    return `${numericAmount}`;
  }
}

function extractPaapiMappedFields(paapiContext = {}) {
  const result = paapiContext?.result || {};
  const rawItem = result?.rawItem && typeof result.rawItem === 'object' ? result.rawItem : {};
  const listing = rawItem?.Offers?.Listings?.[0] || rawItem?.OffersV2?.Listings?.[0] || null;
  const offersCount = Array.isArray(rawItem?.Offers?.Listings)
    ? rawItem.Offers.Listings.length
    : Array.isArray(rawItem?.OffersV2?.Listings)
      ? rawItem.OffersV2.Listings.length
      : 0;
  const imageUrl =
    cleanPaapiText(rawItem?.Images?.Primary?.Large?.URL) ||
    cleanPaapiText(rawItem?.Images?.Primary?.Medium?.URL) ||
    cleanPaapiText(rawItem?.Images?.Primary?.Small?.URL) ||
    cleanPaapiText(result?.imageUrl);
  const displayAmount =
    cleanPaapiText(listing?.Price?.DisplayAmount) ||
    cleanPaapiText(rawItem?.Offers?.Summaries?.[0]?.LowestPrice?.DisplayAmount) ||
    cleanPaapiText(rawItem?.OffersV2?.Summaries?.[0]?.LowestPrice?.DisplayAmount);
  const amount =
    Number.isFinite(Number(listing?.Price?.Amount))
      ? Number(listing.Price.Amount)
      : Number.isFinite(Number(rawItem?.OffersV2?.Summaries?.[0]?.LowestPrice?.Amount))
        ? Number(rawItem.OffersV2.Summaries[0].LowestPrice.Amount)
        : null;
  const currency =
    cleanPaapiText(listing?.Price?.Currency) ||
    cleanPaapiText(rawItem?.OffersV2?.Summaries?.[0]?.LowestPrice?.Currency) ||
    'EUR';
  const priceDisplay = displayAmount || (amount !== null ? formatPaapiAmount(amount, currency) : '') || cleanPaapiText(result?.priceDisplay);
  const title = cleanPaapiText(rawItem?.ItemInfo?.Title?.DisplayValue) || cleanPaapiText(result?.title);
  const availability =
    cleanPaapiText(listing?.Availability?.Message) ||
    cleanPaapiText(rawItem?.OffersV2?.Summaries?.[0]?.Condition?.DisplayValue) ||
    cleanPaapiText(result?.availability);
  const merchant =
    cleanPaapiText(listing?.MerchantInfo?.Name) ||
    cleanPaapiText(listing?.MerchantInfo?.DisplayName) ||
    cleanPaapiText(listing?.MerchantInfo?.FeedbackRating?.SellerName) ||
    cleanPaapiText(rawItem?.OffersV2?.Listings?.[0]?.MerchantInfo?.Name);

  return {
    asin: cleanPaapiText(rawItem?.ASIN || result?.asin).toUpperCase(),
    title,
    imageUrl,
    priceDisplay,
    availability,
    merchant,
    offersCount,
    rawItem,
    rawKeys: rawItem && typeof rawItem === 'object' ? Object.keys(rawItem) : []
  };
}

function hasCompletePaapiProductData(result = {}) {
  const mapped = extractPaapiMappedFields({ result });
  return Boolean(mapped.title && mapped.priceDisplay && mapped.imageUrl);
}

function mapPaapiContextToScrapeResponse({
  inputUrl = '',
  asin = '',
  normalizedUrl = '',
  affiliateUrl = '',
  resolvedUrl = '',
  paapiContext = {}
} = {}) {
  const result = paapiContext?.result || {};
  const mappedFields = extractPaapiMappedFields(paapiContext);
  const finalImageUrl = normalizeDealImageUrl(mappedFields.imageUrl || result.imageUrl || '');
  const finalNormalizedUrl = normalizeAmazonLink(normalizedUrl || result.normalizedUrl || result.detailPageUrl || '');
  const finalAffiliateUrl = affiliateUrl || result.affiliateUrl || finalNormalizedUrl;
  const rawFeatures = Array.isArray(result?.rawItem?.ItemInfo?.Features?.DisplayValues)
    ? result.rawItem.ItemInfo.Features.DisplayValues.filter((value) => cleanPaapiText(value))
    : [];
  const features = Array.isArray(result.features) && result.features.length ? result.features : rawFeatures;

  return {
    success: true,
    title: mappedFields.title || result.title || '',
    productTitle: mappedFields.title || result.title || '',
    productDescription: features.join(' | '),
    bulletPoints: features,
    imageUrl: finalImageUrl,
    image: finalImageUrl,
    productImage: finalImageUrl,
    previewImage: finalImageUrl,
    thumbnail: finalImageUrl,
    images: finalImageUrl ? [finalImageUrl] : [],
    product: {
      imageUrl: finalImageUrl
    },
    basePrice: mappedFields.priceDisplay || result.priceDisplay || '',
    price: mappedFields.priceDisplay || result.priceDisplay || '',
    availability: mappedFields.availability || result.availability || '',
    oldPrice: '',
    couponDetected: false,
    couponValue: '',
    subscribeDetected: false,
    subscribeDiscount: '',
    finalPrice: mappedFields.priceDisplay || result.priceDisplay || '',
    finalPriceCalculated: false,
    asin: asin || mappedFields.asin || result.asin || '',
    finalUrl: finalAffiliateUrl || finalNormalizedUrl || resolvedUrl || inputUrl,
    resolvedUrl: resolvedUrl || finalNormalizedUrl || result.detailPageUrl || inputUrl,
    originalUrl: inputUrl,
    normalizedUrl: finalNormalizedUrl,
    affiliateUrl: finalAffiliateUrl,
    sellerType: '',
    sellerClass: '',
    soldByAmazon: null,
    shippedByAmazon: null,
    sellerDetails: {
      detectionSource: 'paapi',
      detectionSources: ['paapi'],
      matchedPatterns: [],
      merchantText: mappedFields.merchant || '',
      sellerProfile: null
    },
    sellerProfile: null,
    imageDebug: {
      rawScrapeImage: null,
      paapiImage: finalImageUrl || null,
      ogImage: null,
      twitterImage: null,
      firstHtmlImage: null,
      existingFieldImage: null,
      resolvedImageUrl: finalImageUrl || null,
      finalImageUrl: finalImageUrl || null,
      selectedSource: finalImageUrl ? 'paapi' : 'none',
      reason: finalImageUrl ? null : 'no_image_from_paapi',
      paapiStatus: paapiContext?.status || 'loaded',
      paapiReason: paapiContext?.available ? null : paapiContext?.reason || null,
      resolvedUrl,
      wasShortLink: isAmazonShortLink(inputUrl)
    },
    dataSource: 'paapi'
  };
}

async function resolveManualGeneratorAmazonInput(inputValue = '') {
  const trimmedInput = String(inputValue || '').trim();
  const asinInput = extractAsin(trimmedInput);

  if (asinInput && !/^https?:\/\//i.test(trimmedInput)) {
    const linkRecord = buildAmazonAffiliateLinkRecord(asinInput, { asin: asinInput });
    console.info('[ASIN_DETECTED]', {
      asin: asinInput,
      source: 'asin_input'
    });
    console.info('[ASIN_INPUT_DETECTED]', {
      asin: asinInput
    });
    console.info('[AFFILIATE_LINK_BUILT_FROM_ASIN]', {
      asin: asinInput,
      affiliateUrl: linkRecord.affiliateUrl || null
    });
    return {
      inputType: 'asin',
      asin: asinInput,
      normalizedUrl: linkRecord.normalizedUrl || `https://www.amazon.de/dp/${asinInput}`,
      affiliateUrl: linkRecord.affiliateUrl || '',
      resolvedUrl: linkRecord.normalizedUrl || `https://www.amazon.de/dp/${asinInput}`,
      originalUrl: trimmedInput,
      redirectChain: []
    };
  }

  if (!/^https?:\/\//i.test(trimmedInput)) {
    console.error('[MANUAL_GENERATOR_INPUT_NEEDS_DIRECT_LINK_OR_ASIN]', {
      originalInput: trimmedInput || null,
      reason: 'Kein Amazon-Direktlink, Shortlink oder ASIN erkannt.'
    });
    throw buildManualGeneratorInputError(
      'Shortlink konnte technisch nicht aufgeloest werden. Bitte Amazon-Direktlink oder ASIN einfuegen.',
      'MANUAL_GENERATOR_INPUT_NEEDS_DIRECT_LINK_OR_ASIN',
      400
    );
  }

  if (isAmazonShortLink(trimmedInput)) {
    console.info('[MANUAL_SHORTLINK_INPUT_DETECTED]', {
      manualOriginalUrl: trimmedInput,
      hostname: safeUrl(trimmedInput)?.hostname?.toLowerCase().replace(/^www\./, '') || null
    });
    console.info('[SHORTLINK_DETECTED]', {
      originalUrl: trimmedInput
    });
    console.info('[SHORTLINK_RESOLVE_START]', {
      originalUrl: trimmedInput
    });
    logGeneratorDebug('SHORTLINK RESOLVE START', {
      originalUrl: trimmedInput
    });

    let currentUrl = trimmedInput;
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
        const nextUrl = new URL(nextLocation, currentUrl).toString();
        const nextAsin = extractAsin(nextUrl);
        if (nextAsin && isAmazonHostname(safeUrl(nextUrl)?.hostname || '')) {
          const linkRecord = buildAmazonAffiliateLinkRecord(nextUrl, { asin: nextAsin, resolvedUrl: nextUrl });
          console.info('[SHORTLINK_RESOLVE_SUCCESS]', {
            originalUrl: trimmedInput,
            resolvedUrl: nextUrl,
            redirectCount: redirectChain.length
          });
          console.info('[SHORTLINK_RESOLVED_URL]', {
            originalUrl: trimmedInput,
            resolvedUrl: nextUrl
          });
          console.info('[ASIN_EXTRACTED_FROM_RESOLVED_URL]', {
            asin: nextAsin,
            resolvedUrl: nextUrl
          });
          console.info('[ASIN_EXTRACTED]', {
            asin: nextAsin,
            resolvedUrl: nextUrl,
            source: 'shortlink_resolved_url'
          });
          console.info('[MANUAL_SHORTLINK_RESOLVED_FOR_DATA]', {
            manualOriginalUrl: trimmedInput,
            resolvedUrl: nextUrl,
            asin: nextAsin
          });
          console.info('[AFFILIATE_LINK_BUILT]', {
            asin: nextAsin,
            affiliateUrl: linkRecord.affiliateUrl || null
          });
          return {
            inputType: 'shortlink',
            asin: nextAsin,
            normalizedUrl: linkRecord.normalizedUrl || normalizeAmazonLink(nextUrl),
            affiliateUrl: linkRecord.affiliateUrl || '',
            resolvedUrl: nextUrl,
            originalUrl: trimmedInput,
            redirectChain
          };
        }
        currentUrl = nextUrl;
        continue;
      }

      const currentAsin = extractAsin(currentUrl);
      if (currentAsin && isAmazonHostname(safeUrl(currentUrl)?.hostname || '')) {
        const linkRecord = buildAmazonAffiliateLinkRecord(currentUrl, { asin: currentAsin, resolvedUrl: currentUrl });
        console.info('[SHORTLINK_RESOLVE_SUCCESS]', {
          originalUrl: trimmedInput,
          resolvedUrl: currentUrl,
          redirectCount: redirectChain.length
        });
        console.info('[SHORTLINK_RESOLVED_URL]', {
          originalUrl: trimmedInput,
          resolvedUrl: currentUrl
        });
        console.info('[ASIN_EXTRACTED_FROM_RESOLVED_URL]', {
          asin: currentAsin,
          resolvedUrl: currentUrl
        });
        console.info('[ASIN_EXTRACTED]', {
          asin: currentAsin,
          resolvedUrl: currentUrl,
          source: 'shortlink_current_url'
        });
        console.info('[MANUAL_SHORTLINK_RESOLVED_FOR_DATA]', {
          manualOriginalUrl: trimmedInput,
          resolvedUrl: currentUrl,
          asin: currentAsin
        });
        console.info('[AFFILIATE_LINK_BUILT]', {
          asin: currentAsin,
          affiliateUrl: linkRecord.affiliateUrl || null
        });
        return {
          inputType: 'shortlink',
          asin: currentAsin,
          normalizedUrl: linkRecord.normalizedUrl || normalizeAmazonLink(currentUrl),
          affiliateUrl: linkRecord.affiliateUrl || '',
          resolvedUrl: currentUrl,
          originalUrl: trimmedInput,
          redirectChain
        };
      }

      break;
    }

    console.error('[SHORTLINK_RESOLVE_FAILED]', {
      originalUrl: trimmedInput,
      redirectChain
    });
    return {
      inputType: 'shortlink',
      asin: '',
      normalizedUrl: '',
      affiliateUrl: trimmedInput,
      resolvedUrl: currentUrl !== trimmedInput ? currentUrl : '',
      originalUrl: trimmedInput,
      redirectChain,
      resolutionFailed: true
    };
  }

  if (isAmazonDirectLink(trimmedInput)) {
    const asin = extractAsin(trimmedInput);
    const linkRecord = buildAmazonAffiliateLinkRecord(trimmedInput, { asin, resolvedUrl: trimmedInput });
    console.info('[ASIN_DETECTED]', {
      asin,
      source: 'amazon_direct_link'
    });
    console.info('[AMAZON_DIRECT_LINK_DETECTED]', {
      originalUrl: trimmedInput
    });
    console.info('[ASIN_EXTRACTED]', {
      asin,
      originalUrl: trimmedInput
    });
    console.info('[AFFILIATE_LINK_BUILT]', {
      asin,
      affiliateUrl: linkRecord.affiliateUrl || null
    });
    return {
      inputType: 'direct_link',
      asin,
      normalizedUrl: linkRecord.normalizedUrl || normalizeAmazonLink(trimmedInput),
      affiliateUrl: linkRecord.affiliateUrl || '',
      resolvedUrl: trimmedInput,
      originalUrl: trimmedInput,
      redirectChain: []
    };
  }

  console.error('[MANUAL_GENERATOR_INPUT_NEEDS_DIRECT_LINK_OR_ASIN]', {
    originalUrl: trimmedInput,
    reason: 'Kein Amazon-Direktlink mit ASIN erkannt.'
  });
  throw buildManualGeneratorInputError(
    'Shortlink konnte technisch nicht aufgeloest werden. Bitte Amazon-Direktlink oder ASIN einfuegen.',
    'MANUAL_GENERATOR_INPUT_NEEDS_DIRECT_LINK_OR_ASIN',
    400
  );
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

function extractAmazonTitleFromResolvedUrl(value = '') {
  const parsed = safeUrl(value);
  const segments = parsed?.pathname?.split('/').filter(Boolean) || [];

  if (!segments.length) {
    return '';
  }

  const dpIndex = segments.findIndex((segment) => /^dp$/i.test(segment));
  const gpProductIndex = segments.findIndex((segment, index) => /^product$/i.test(segment) && /^gp$/i.test(segments[index - 1] || ''));
  const slugIndex = dpIndex > 0 ? dpIndex - 1 : gpProductIndex > 1 ? gpProductIndex - 2 : -1;
  const rawSlug = slugIndex >= 0 ? segments[slugIndex] || '' : '';

  if (!rawSlug) {
    return '';
  }

  return decodeURIComponent(rawSlug)
    .replace(/\+/g, ' ')
    .replace(/-/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
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

  const resolvedInput = await resolveManualGeneratorAmazonInput(trimmedUrl);
  const finalAffiliateUrl = resolvedInput.affiliateUrl || '';
  const finalScrapeUrl = resolvedInput.normalizedUrl || resolvedInput.resolvedUrl || trimmedUrl;
  const resolvedUrl = resolvedInput.resolvedUrl || finalScrapeUrl;
  const isShortlinkInput = resolvedInput.inputType === 'shortlink';
  const isForcedPaapiInput = resolvedInput.inputType === 'asin' || resolvedInput.inputType === 'direct_link';
  let shortlinkFallbackCandidate = null;
  let forcedPaapiFallbackCandidate = null;
  let forcedPaapiScrapeFallbackTriggered = false;

  logGeneratorDebug('api.amazon.scrape.redirect_resolution', {
    originalUrl: trimmedUrl,
    resolvedUrl,
    wasShortLink: isShortlinkInput,
    redirectCount: Math.max(0, resolvedInput.redirectChain.length),
    redirectChain: resolvedInput.redirectChain
  });

  if (resolvedInput.asin) {
    console.info('[PAAPI_LOOKUP_START]', {
      asin: resolvedInput.asin,
      resolvedUrl,
      inputType: resolvedInput.inputType
    });
  }

  const paapiContext = resolvedInput.asin ? await loadAmazonAffiliateContext({ asin: resolvedInput.asin }) : null;
  const paapiMappedFields = paapiContext?.available === true ? extractPaapiMappedFields(paapiContext) : null;
  if (resolvedInput.asin && paapiContext?.available === true) {
    console.info('[PAAPI_LOOKUP_SUCCESS]', {
      asin: resolvedInput.asin,
      resolvedUrl,
      inputType: resolvedInput.inputType,
      hasTitle: Boolean(paapiMappedFields?.title),
      hasPrice: Boolean(paapiMappedFields?.priceDisplay),
      hasImage: Boolean(paapiMappedFields?.imageUrl)
    });
    console.info('[PAAPI_RAW_ITEM_KEYS]', {
      asin: resolvedInput.asin,
      keys: paapiMappedFields?.rawKeys || [],
      hasItem: Boolean(paapiMappedFields?.rawItem && Object.keys(paapiMappedFields.rawItem).length)
    });
    if (paapiMappedFields?.title) {
      console.info('[PAAPI_TITLE_FOUND]', {
        asin: resolvedInput.asin,
        titleLength: paapiMappedFields.title.length
      });
    }
    if (paapiMappedFields?.imageUrl) {
      console.info('[PAAPI_IMAGE_FOUND]', {
        asin: resolvedInput.asin,
        imageUrl: paapiMappedFields.imageUrl
      });
    } else {
      console.warn('[PAAPI_IMAGE_MISSING]', {
        asin: resolvedInput.asin
      });
    }
    if (paapiMappedFields?.priceDisplay) {
      console.info('[PAAPI_PRICE_FOUND]', {
        asin: resolvedInput.asin,
        price: paapiMappedFields.priceDisplay
      });
    } else {
      console.warn('[PAAPI_PRICE_MISSING]', {
        asin: resolvedInput.asin
      });
    }
  }

  if (isForcedPaapiInput && resolvedInput.asin) {
    console.info('[PAAPI_FORCED_INSTEAD_OF_SCRAPE]', {
      asin: resolvedInput.asin,
      inputType: resolvedInput.inputType,
      resolvedUrl
    });

    const payload = mapPaapiContextToScrapeResponse({
      inputUrl: trimmedUrl,
      asin: resolvedInput.asin,
      normalizedUrl: finalScrapeUrl,
      affiliateUrl: finalAffiliateUrl,
      resolvedUrl,
      paapiContext
    });
    const offersCount = Number(paapiMappedFields?.offersCount || 0);
    const noOffers = offersCount === 0;

    if (paapiContext?.available === true && !noOffers) {
      console.info('[PRODUCT_DATA_SOURCE_PAAPI]', {
        asin: resolvedInput.asin,
        normalizedUrl: finalScrapeUrl,
        affiliateUrl: finalAffiliateUrl || paapiContext.result?.affiliateUrl || null
      });
      console.info('[PAAPI_PRODUCT_MAPPED]', {
        asin: payload.asin || resolvedInput.asin,
        title: payload.title || '',
        hasImage: Boolean(payload.imageUrl),
        price: payload.price || '',
        availability: payload.availability || '',
        merchant: payload.sellerDetails?.merchantText || ''
      });
      return payload;
    }

    forcedPaapiFallbackCandidate = payload;
    forcedPaapiScrapeFallbackTriggered = noOffers;

    if (noOffers) {
      console.info('[PAAPI_NO_OFFERS]', {
        asin: resolvedInput.asin,
        offersCount,
        inputType: resolvedInput.inputType,
        resolvedUrl,
        paapiAvailable: paapiContext?.available === true,
        paapiReason: paapiContext?.reason || null
      });
    } else {
      console.warn('[SCRAPE_BLOCKED_FOR_ASIN]', {
        asin: resolvedInput.asin,
        inputType: resolvedInput.inputType,
        resolvedUrl,
        paapiAvailable: paapiContext?.available === true,
        paapiReason: paapiContext?.reason || null,
        hasTitle: Boolean(paapiMappedFields?.title),
        hasPrice: Boolean(paapiMappedFields?.priceDisplay),
        hasImage: Boolean(paapiMappedFields?.imageUrl),
        offersCount
      });
      return forcedPaapiFallbackCandidate;
    }

    forcedPaapiFallbackCandidate.message = 'PAAPI liefert aktuell keinen Preis fuer diese ASIN';
  }

  if (isShortlinkInput) {
    if (paapiContext?.available === true && hasCompletePaapiProductData(paapiContext.result)) {
      console.info('[PRODUCT_DATA_SOURCE_PAAPI]', {
        asin: resolvedInput.asin,
        normalizedUrl: finalScrapeUrl,
        affiliateUrl: finalAffiliateUrl || paapiContext.result?.affiliateUrl || null
      });
      const payload = {
        ...mapPaapiContextToScrapeResponse({
          inputUrl: trimmedUrl,
          asin: resolvedInput.asin,
          normalizedUrl: finalScrapeUrl,
          affiliateUrl: finalAffiliateUrl,
          resolvedUrl,
          paapiContext
        }),
        inputMode: 'shortlink',
        preserveInputLink: false,
        finalUrl: finalAffiliateUrl || finalScrapeUrl || trimmedUrl,
        affiliateUrl: finalAffiliateUrl || '',
        originalUrl: trimmedUrl,
        shortlinkAllowed: true,
        manualCompletionNeeded: false,
        message: 'Shortlink aufgeloest. Produktdaten wurden automatisch ueber PAAPI geladen.'
      };
      console.info('[PRODUCT_DATA_FILLED]', {
        source: 'paapi',
        asin: resolvedInput.asin,
        hasTitle: true,
        hasPrice: true,
        hasImage: true,
        affiliateUrl: finalAffiliateUrl || paapiContext.result?.affiliateUrl || null
      });
      return payload;
    }

    const partialPaapiResult = paapiContext?.result || {};
    let resolvedUrlTitle = '';
    let resolvedUrlPrice = '';
    let resolvedUrlImage = '';
    let resolvedUrlAsin = resolvedInput.asin || '';
    let resolvedUrlNormalized = finalScrapeUrl;
    let resolvedUrlFinal = resolvedUrl;
    let resolvedUrlDataSource = '';

    resolvedUrlAsin =
      resolvedInput.asin ||
      partialPaapiResult.asin ||
      extractAsin(resolvedUrl) ||
      extractAsin(finalScrapeUrl) ||
      '';
    resolvedUrlTitle = extractAmazonTitleFromResolvedUrl(resolvedUrl || finalScrapeUrl || trimmedUrl);
    resolvedUrlFinal = resolvedUrl || finalScrapeUrl;
    resolvedUrlNormalized = normalizeAmazonLink(resolvedUrlFinal) || finalScrapeUrl;
    resolvedUrlDataSource = resolvedUrlAsin || resolvedUrlTitle ? 'resolved_amazon_url' : '';

    const finalResolvedAsin = resolvedInput.asin || partialPaapiResult.asin || resolvedUrlAsin || extractAsin(resolvedUrlFinal) || '';
    if (finalResolvedAsin && !resolvedInput.asin) {
      console.info('[ASIN_EXTRACTED]', {
        asin: finalResolvedAsin,
        resolvedUrl: resolvedUrlFinal || resolvedUrl || null,
        source: resolvedUrlDataSource || 'shortlink_fallback'
      });
    }
    const shortlinkAffiliateRecord = finalResolvedAsin
      ? buildAmazonAffiliateLinkRecord(finalResolvedAsin, {
          asin: finalResolvedAsin,
          resolvedUrl: resolvedUrlNormalized || resolvedUrlFinal || finalScrapeUrl
        })
      : null;
    const shortlinkImageUrl = normalizeDealImageUrl(partialPaapiResult.imageUrl || resolvedUrlImage || '');
    const shortlinkTitle = partialPaapiResult.title || resolvedUrlTitle || '';
    const shortlinkPrice = partialPaapiResult.priceDisplay || resolvedUrlPrice || '';
    const shortlinkAffiliateUrl = shortlinkAffiliateRecord?.affiliateUrl || '';
    const preserveInputLink = !shortlinkAffiliateUrl;
    const manualCompletionNeeded = !shortlinkTitle || !shortlinkPrice;
    const manualInputRequired = !finalResolvedAsin && !shortlinkTitle && !shortlinkPrice && !shortlinkImageUrl;

    shortlinkFallbackCandidate = {
      success: true,
      title: shortlinkTitle,
      productTitle: shortlinkTitle,
      productDescription: Array.isArray(partialPaapiResult.features) ? partialPaapiResult.features.join(' | ') : '',
      bulletPoints: Array.isArray(partialPaapiResult.features) ? partialPaapiResult.features : [],
      imageUrl: shortlinkImageUrl,
      image: shortlinkImageUrl,
      productImage: shortlinkImageUrl,
      previewImage: shortlinkImageUrl,
      thumbnail: shortlinkImageUrl,
      images: shortlinkImageUrl ? [shortlinkImageUrl] : [],
      product: {
        imageUrl: shortlinkImageUrl
      },
      basePrice: shortlinkPrice,
      price: shortlinkPrice,
      oldPrice: '',
      couponDetected: false,
      couponValue: '',
      subscribeDetected: false,
      subscribeDiscount: '',
      finalPrice: shortlinkPrice,
      finalPriceCalculated: false,
      asin: finalResolvedAsin,
      finalUrl: shortlinkAffiliateUrl || trimmedUrl,
      resolvedUrl: resolvedUrlFinal || resolvedUrl,
      originalUrl: trimmedUrl,
      normalizedUrl: shortlinkAffiliateRecord?.normalizedUrl || resolvedUrlNormalized || finalScrapeUrl,
      affiliateUrl: shortlinkAffiliateUrl,
      sellerType: '',
      sellerClass: '',
      soldByAmazon: null,
      shippedByAmazon: null,
      sellerDetails: {
        detectionSource: paapiContext?.available === true ? 'paapi_partial' : 'manual_shortlink',
        detectionSources: paapiContext?.available === true ? ['paapi_partial'] : ['manual_shortlink'],
        matchedPatterns: [],
        merchantText: '',
        sellerProfile: null
      },
      sellerProfile: null,
      imageDebug: {
        rawScrapeImage: null,
        paapiImage: normalizeDealImageUrl(partialPaapiResult.imageUrl || '') || null,
        ogImage: null,
        twitterImage: null,
        firstHtmlImage: null,
        existingFieldImage: null,
        resolvedImageUrl: shortlinkImageUrl || null,
        finalImageUrl: shortlinkImageUrl || null,
        selectedSource: normalizeDealImageUrl(partialPaapiResult.imageUrl || '')
          ? 'paapi'
          : resolvedUrlImage
            ? 'resolved_amazon_url'
            : 'none',
        reason: shortlinkImageUrl ? null : 'no_image_from_shortlink_resolution',
        paapiStatus: paapiContext?.status || (resolvedInput.asin ? 'missing' : 'not_requested'),
        paapiReason: paapiContext?.available ? null : paapiContext?.reason || null,
        resolvedUrl: resolvedUrlFinal || resolvedUrl,
        wasShortLink: true
      },
      dataSource:
        paapiContext?.available === true && partialPaapiResult.title
          ? 'paapi_partial'
          : resolvedUrlDataSource || (manualInputRequired ? 'manual_shortlink' : 'shortlink_partial'),
      inputMode: 'shortlink',
      preserveInputLink,
      manualCompletionNeeded,
      manualInputRequired,
      shortlinkAllowed: true,
      message: manualInputRequired
        ? 'Shortlink erkannt, aber es konnten keine Produktdaten automatisch geladen werden. Bitte Titel und Preis manuell eingeben.'
        : manualCompletionNeeded
          ? 'Shortlink aufgeloest. Teilweise Produktdaten wurden automatisch geladen. Fehlende Werte kannst du ergaenzen.'
          : 'Shortlink aufgeloest. Produktdaten wurden automatisch uebernommen.'
    };

    if (!manualCompletionNeeded && shortlinkAffiliateUrl) {
      console.info('[PRODUCT_DATA_FILLED]', {
        source: shortlinkFallbackCandidate.dataSource || 'shortlink_partial',
        asin: finalResolvedAsin || null,
        hasTitle: Boolean(shortlinkTitle),
        hasPrice: Boolean(shortlinkPrice),
        hasImage: Boolean(shortlinkImageUrl),
        affiliateUrl: shortlinkAffiliateUrl
      });
      return shortlinkFallbackCandidate;
    }

    if (resolvedInput.resolutionFailed || !isAmazonHostname(safeUrl(finalScrapeUrl)?.hostname || '')) {
      console.warn('[SHORTLINK_FAILED_NEEDS_MANUAL_INPUT]', {
        originalUrl: trimmedUrl,
        resolvedUrl: resolvedUrlFinal || resolvedUrl || null,
        asin: finalResolvedAsin || null,
        reason: resolvedInput.resolutionFailed ? 'SHORTLINK_RESOLUTION_FAILED' : 'NO_AMAZON_TARGET_URL'
      });
      console.warn('[MANUAL_INPUT_ONLY_LAST_FALLBACK]', {
        originalUrl: trimmedUrl,
        resolvedUrl: resolvedUrlFinal || resolvedUrl || null,
        asin: finalResolvedAsin || null
      });
      return shortlinkFallbackCandidate;
    }
  }

  if (paapiContext?.available === true && hasCompletePaapiProductData(paapiContext.result)) {
    console.info('[PRODUCT_DATA_SOURCE_PAAPI]', {
      asin: resolvedInput.asin,
      normalizedUrl: finalScrapeUrl,
      affiliateUrl: finalAffiliateUrl || paapiContext.result?.affiliateUrl || null
    });
    return mapPaapiContextToScrapeResponse({
      inputUrl: trimmedUrl,
      asin: resolvedInput.asin,
      normalizedUrl: finalScrapeUrl,
      affiliateUrl: finalAffiliateUrl,
      resolvedUrl,
      paapiContext
    });
  }

  if (isShortlinkInput) {
    console.info('[SCRAPE_LAST_FALLBACK_START]', {
      originalUrl: trimmedUrl,
      resolvedUrl: finalScrapeUrl,
      asin: shortlinkFallbackCandidate?.asin || resolvedInput.asin || null
    });
  }

  if (forcedPaapiScrapeFallbackTriggered) {
    console.info('[SCRAPE_FALLBACK_START]', {
      originalUrl: trimmedUrl,
      resolvedUrl: finalScrapeUrl,
      asin: resolvedInput.asin || null,
      reason: 'PAAPI_NO_OFFERS'
    });
  }

  const resolvedRequest = await resolveScrapeRequest(finalScrapeUrl);
  const response = resolvedRequest.response;
  const scrapeResolvedUrl = resolvedRequest.resolvedUrl || finalScrapeUrl;
  const html = await response.text();

  if (response.status === 403 || /captcha|robot check|sorry/i.test(html)) {
    if (isForcedPaapiInput && forcedPaapiFallbackCandidate) {
      console.warn('[SCRAPE_FAILED]', {
        asin: resolvedInput.asin || null,
        resolvedUrl: scrapeResolvedUrl || finalScrapeUrl || null,
        reason: 'AMAZON_BLOCKED'
      });
      return forcedPaapiFallbackCandidate;
    }
    if (isShortlinkInput && shortlinkFallbackCandidate) {
      console.warn('[SHORTLINK_FAILED_NEEDS_MANUAL_INPUT]', {
        originalUrl: trimmedUrl,
        resolvedUrl: scrapeResolvedUrl || finalScrapeUrl || null,
        asin: shortlinkFallbackCandidate.asin || null,
        reason: 'AMAZON_BLOCKED'
      });
      if (shortlinkFallbackCandidate.manualInputRequired === true) {
        console.warn('[MANUAL_INPUT_ONLY_LAST_FALLBACK]', {
          originalUrl: trimmedUrl,
          resolvedUrl: scrapeResolvedUrl || finalScrapeUrl || null,
          asin: shortlinkFallbackCandidate.asin || null
        });
      }
      return shortlinkFallbackCandidate;
    }
    const error = new Error('Amazon blockiert den Scrape-Zugriff');
    error.code = 'AMAZON_BLOCKED';
    error.statusCode = 502;
    throw error;
  }

  if (!response.ok) {
    if (isForcedPaapiInput && forcedPaapiFallbackCandidate) {
      console.warn('[SCRAPE_FAILED]', {
        asin: resolvedInput.asin || null,
        resolvedUrl: scrapeResolvedUrl || finalScrapeUrl || null,
        reason: `SCRAPE_FAILED_${response.status}`
      });
      return forcedPaapiFallbackCandidate;
    }
    if (isShortlinkInput && shortlinkFallbackCandidate) {
      console.warn('[SHORTLINK_FAILED_NEEDS_MANUAL_INPUT]', {
        originalUrl: trimmedUrl,
        resolvedUrl: scrapeResolvedUrl || finalScrapeUrl || null,
        asin: shortlinkFallbackCandidate.asin || null,
        reason: `SCRAPE_FAILED_${response.status}`
      });
      if (shortlinkFallbackCandidate.manualInputRequired === true) {
        console.warn('[MANUAL_INPUT_ONLY_LAST_FALLBACK]', {
          originalUrl: trimmedUrl,
          resolvedUrl: scrapeResolvedUrl || finalScrapeUrl || null,
          asin: shortlinkFallbackCandidate.asin || null
        });
      }
      return shortlinkFallbackCandidate;
    }
    const error = new Error(`Scrape failed (${response.status})`);
    error.code = 'SCRAPE_FAILED';
    error.statusCode = 502;
    throw error;
  }

  const canonicalUrl = extractCanonicalUrl(html);
  const productTitle = extractAmazonTitle(html) || shortlinkFallbackCandidate?.title || '';
  const productDescription = stripHtml(extractAmazonDescription(html) || '');
  const bulletPoints = extractAmazonBulletPoints(html);
  const basePrice = extractAmazonPrice(html) || shortlinkFallbackCandidate?.price || '';
  const couponDetails = extractCouponDetails(html);
  const subscribeDetails = extractSubscribeDetails(html);
  const finalPriceResult = calculateFinalPriceFromDiscounts(basePrice, couponDetails.couponValue, subscribeDetails.subscribeDiscount);
  const finalUrl = canonicalUrl || scrapeResolvedUrl || resolvedUrl || finalScrapeUrl;
  const asin =
    extractAsin(canonicalUrl) ||
    extractAsin(scrapeResolvedUrl) ||
    extractAsin(finalScrapeUrl) ||
    shortlinkFallbackCandidate?.asin ||
    resolvedInput.asin ||
    '';
  const normalizedUrl = normalizeAmazonLink(finalUrl);
  const affiliateLinkRecord = buildAmazonAffiliateLinkRecord(finalUrl || finalScrapeUrl || asin, {
    asin,
    resolvedUrl: normalizedUrl || scrapeResolvedUrl || finalScrapeUrl
  });
  const finalDisplayPrice = finalPriceResult.finalPriceCalculated ? finalPriceResult.finalPrice : basePrice;
  const finalProductTitle =
    (isForcedPaapiInput && forcedPaapiFallbackCandidate?.title) || productTitle;
  const finalProductDescription =
    (isForcedPaapiInput && forcedPaapiFallbackCandidate?.productDescription) || productDescription;
  const finalBulletPoints =
    isForcedPaapiInput &&
    Array.isArray(forcedPaapiFallbackCandidate?.bulletPoints) &&
    forcedPaapiFallbackCandidate.bulletPoints.length
      ? forcedPaapiFallbackCandidate.bulletPoints
      : bulletPoints;
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
  const paapiContextForImage = paapiContext || (asin ? await loadAmazonAffiliateContext({ asin }) : null);
  const paapiImage = paapiContextForImage?.available ? paapiContextForImage.result?.imageUrl || '' : '';
  const imageResolution = resolveAmazonImage(html, {
    baseUrl: finalUrl,
    paapiImage
  });
  const finalImageUrl = imageResolution.finalImageUrl || shortlinkFallbackCandidate?.imageUrl || '';
  if (forcedPaapiScrapeFallbackTriggered && finalDisplayPrice) {
    console.info('[SCRAPE_PRICE_FOUND]', {
      asin,
      price: finalDisplayPrice
    });
  }
  if (forcedPaapiScrapeFallbackTriggered && finalImageUrl) {
    console.info('[SCRAPE_IMAGE_FOUND]', {
      asin,
      imageUrl: finalImageUrl
    });
  }
  const shortlinkManualCompletionNeededAfterScrape =
    isShortlinkInput && (!finalProductTitle || !finalDisplayPrice);
  const shortlinkManualInputRequiredAfterScrape =
    isShortlinkInput && !asin && !finalProductTitle && !finalDisplayPrice && !finalImageUrl;
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
    paapiStatus: paapiContextForImage?.status || (asin ? 'not_requested' : 'missing_asin'),
    paapiReason: paapiContextForImage?.available ? null : paapiContextForImage?.reason || null,
    resolvedUrl: scrapeResolvedUrl,
    wasShortLink: resolvedInput.inputType === 'shortlink',
    redirectCount: Math.max(0, resolvedInput.redirectChain.length)
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
    resolvedUrl: scrapeResolvedUrl,
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
  console.info('[PRODUCT_DATA_SOURCE_SCRAPE]', {
    asin,
    normalizedUrl,
    affiliateUrl: affiliateLinkRecord.affiliateUrl || finalAffiliateUrl || null
  });
  if (isShortlinkInput) {
    console.info('[PRODUCT_DATA_FILLED]', {
      source: 'scrape_last_fallback',
      asin,
      hasTitle: Boolean(finalProductTitle),
      hasPrice: Boolean(finalDisplayPrice),
      hasImage: Boolean(finalImageUrl),
      affiliateUrl: affiliateLinkRecord.affiliateUrl || finalAffiliateUrl || null
    });
  }
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
    title: finalProductTitle,
    productTitle: finalProductTitle,
    productDescription: finalProductDescription,
    bulletPoints: finalBulletPoints,
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
    price: finalDisplayPrice,
    oldPrice: extractAmazonOldPrice(html) || '',
    couponDetected: couponDetails.couponDetected,
    couponValue: couponDetails.couponValue,
    subscribeDetected: subscribeDetails.subscribeDetected,
    subscribeDiscount: subscribeDetails.subscribeDiscount,
    finalPrice: finalPriceResult.finalPrice,
    finalPriceCalculated: finalPriceResult.finalPriceCalculated,
    asin,
    finalUrl: affiliateLinkRecord.affiliateUrl || finalAffiliateUrl || finalUrl,
    resolvedUrl: scrapeResolvedUrl,
    originalUrl: trimmedUrl,
    normalizedUrl,
    affiliateUrl: affiliateLinkRecord.affiliateUrl || finalAffiliateUrl,
    sellerType: sellerInfo.sellerType,
    sellerClass: sellerInfo.sellerClass,
    soldByAmazon: sellerInfo.soldByAmazon,
    shippedByAmazon: sellerInfo.shippedByAmazon,
    sellerDetails: {
      ...(sellerInfo.sellerDetails || {}),
      sellerProfile
    },
    sellerProfile,
    imageDebug,
    dataSource: 'scrape',
    ...(isShortlinkInput
      ? {
          inputMode: 'shortlink',
          preserveInputLink: false,
          manualCompletionNeeded: shortlinkManualCompletionNeededAfterScrape,
          manualInputRequired: shortlinkManualInputRequiredAfterScrape,
          shortlinkAllowed: true,
          message: shortlinkManualInputRequiredAfterScrape
            ? 'Shortlink erkannt, aber die Produktdaten konnten nicht vollstaendig geladen werden. Bitte Titel und Preis manuell ergaenzen.'
            : shortlinkManualCompletionNeededAfterScrape
              ? 'Shortlink aufgeloest. Einige Produktdaten wurden geladen. Fehlende Werte kannst du ergaenzen.'
              : 'Shortlink aufgeloest. Produktdaten wurden automatisch uebernommen.'
        }
      : {})
  };
}

async function handleScrape(req, res) {
  const inputUrl = typeof req.body?.url === 'string' ? req.body.url : '';

  try {
    const scrapeResult = await scrapeAmazonProduct(inputUrl);
    return res.status(200).json(finalizeManualGeneratorScrapeResponse(inputUrl, scrapeResult));
  } catch (error) {
    console.error('[PRODUCT_DATA_FAILED]', {
      inputUrl: inputUrl || null,
      errorCode: error instanceof Error ? error.code || 'SCRAPE_FAILED' : 'SCRAPE_FAILED',
      errorMessage: error instanceof Error ? error.message : 'Scrape failed'
    });
    logGeneratorDebug('api.amazon.scrape.error', {
      error: error instanceof Error ? error.message : 'Scrape failed'
    });
    const errorMessage =
      error instanceof Error
        ? error.message
        : 'Scrape failed';
    return res.status(error?.statusCode && Number.isFinite(Number(error.statusCode)) ? Number(error.statusCode) : 500).json({
      success: false,
      error: errorMessage,
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

router.get('/creator-test', async (req, res) => {
  try {
    res.json(
      await testCreatorApi({
        asin: req.query.asin
      })
    );
  } catch (error) {
    res.status(error?.statusCode && Number.isFinite(Number(error.statusCode)) ? Number(error.statusCode) : 400).json({
      error: error instanceof Error ? error.message : 'Creator API Test fehlgeschlagen.',
      code: error instanceof Error ? error.code || 'CREATOR_API_TEST_FAILED' : 'CREATOR_API_TEST_FAILED'
    });
  }
});

router.post('/scrape', handleScrape);

export default router;
