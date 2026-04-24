import { Router } from 'express';
import { getAmazonAffiliateStatus, loadAmazonAffiliateContext, runAmazonAffiliateApiTest } from '../services/amazonAffiliateService.js';
import { classifySellerType, extractAsin, normalizeAmazonLink } from '../services/dealHistoryService.js';
import { logGeneratorDebug } from '../services/generatorFlowService.js';

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

function extractAmazonTitle(html) {
  return extractFirstMatch(html, [
    /<meta[^>]+property=["']og:title["'][^>]+content=["']([^"']+)["']/i,
    /<span[^>]+id=["']productTitle["'][^>]*>\s*([^<]+?)\s*<\/span>/i,
    /<title>\s*([^<]+?)\s*<\/title>/i
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

function stripHtml(value) {
  return decodeHtml(value.replace(/<script[\s\S]*?<\/script>/gi, ' ').replace(/<[^>]+>/g, ' '))
    .replace(/\s+/g, ' ')
    .trim();
}

function extractSellerInfo(html) {
  const merchantChunk = extractFirstMatch(html, [
    /<div[^>]+id=["']merchantInfo_feature_div["'][^>]*>([\s\S]*?)<\/div>/i,
    /<div[^>]+id=["']merchant-info["'][^>]*>([\s\S]*?)<\/div>/i
  ]);

  const merchantText = stripHtml(merchantChunk || html);
  const soldByAmazon =
    /verkauf und versand durch amazon/i.test(merchantText) ||
    /sold by amazon/i.test(merchantText);
  const shippedByAmazon =
    soldByAmazon ||
    /versand durch amazon/i.test(merchantText) ||
    /fulfilled by amazon/i.test(merchantText) ||
    /ships from amazon/i.test(merchantText);

  return {
    sellerType: classifySellerType({ soldByAmazon, shippedByAmazon })
  };
}

async function handleScrape(req, res) {
  try {
    const { url } = req.body ?? {};

    if (!url || typeof url !== 'string' || !url.trim()) {
      return res.status(400).json({
        success: false,
        error: 'Kein url uebergeben',
        code: 'MISSING_URL'
      });
    }

    const trimmedUrl = url.trim();
    logGeneratorDebug('api.amazon.scrape.request', {
      url: trimmedUrl
    });

    if (!/^https?:\/\//i.test(trimmedUrl)) {
      return res.status(400).json({
        success: false,
        error: 'Ungueltige URL',
        code: 'INVALID_URL'
      });
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
      return res.status(502).json({
        success: false,
        error: 'Amazon blockiert den Scrape-Zugriff',
        code: 'AMAZON_BLOCKED'
      });
    }

    if (!response.ok) {
      return res.status(502).json({
        success: false,
        error: `Scrape failed (${response.status})`,
        code: 'SCRAPE_FAILED'
      });
    }

    const canonicalUrl = extractCanonicalUrl(html);
    const finalUrl = canonicalUrl || resolvedUrl || trimmedUrl;
    const asin = extractAsin(canonicalUrl) || extractAsin(resolvedUrl) || extractAsin(trimmedUrl) || '';
    const normalizedUrl = normalizeAmazonLink(finalUrl);
    const sellerInfo = extractSellerInfo(html);
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
      hasImage: Boolean(finalImageUrl),
      normalizedUrl,
      finalImageUrl,
      selectedImageSource: imageResolution.selectedSource,
      reasonIfMissing: imageResolution.reasonIfMissing
    });

    return res.status(200).json({
      success: true,
      title: extractAmazonTitle(html) || '',
      imageUrl: finalImageUrl,
      image: finalImageUrl,
      productImage: finalImageUrl,
      previewImage: finalImageUrl,
      thumbnail: finalImageUrl,
      images: finalImageUrl ? [finalImageUrl] : [],
      product: {
        imageUrl: finalImageUrl
      },
      price: extractAmazonPrice(html) || '',
      oldPrice: extractAmazonOldPrice(html) || '',
      asin,
      finalUrl,
      resolvedUrl,
      originalUrl: trimmedUrl,
      normalizedUrl,
      sellerType: sellerInfo.sellerType,
      imageDebug
    });
  } catch (error) {
    logGeneratorDebug('api.amazon.scrape.error', {
      error: error instanceof Error ? error.message : 'Scrape failed'
    });
    return res.status(500).json({
      success: false,
      error: error instanceof Error ? `Scrape failed: ${error.message}` : 'Scrape failed',
      code: 'SCRAPE_FAILED'
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
