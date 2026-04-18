import { Router } from 'express';
import { getAmazonAffiliateStatus, runAmazonAffiliateApiTest } from '../services/amazonAffiliateService.js';
import { classifySellerType, extractAsin, normalizeAmazonLink } from '../services/dealHistoryService.js';
import { logGeneratorDebug } from '../services/generatorFlowService.js';

const router = Router();

function decodeHtml(value) {
  return value
    .replace(/&amp;/g, '&')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>');
}

function extractFirstMatch(html, patterns) {
  for (const pattern of patterns) {
    const match = html.match(pattern);
    if (match?.[1]) {
      return decodeHtml(match[1].trim());
    }
  }

  return '';
}

function extractAmazonImage(html) {
  return extractFirstMatch(html, [
    /<meta[^>]+property=["']og:image["'][^>]+content=["']([^"']+)["']/i,
    /<meta[^>]+name=["']twitter:image["'][^>]+content=["']([^"']+)["']/i,
    /"hiRes"\s*:\s*"([^"]+)"/i
  ]);
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

    const response = await fetch(trimmedUrl, {
      headers: {
        'User-Agent':
          'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36',
        'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8'
      }
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
    const asin = extractAsin(canonicalUrl) || extractAsin(trimmedUrl) || '';
    const normalizedUrl = normalizeAmazonLink(canonicalUrl || trimmedUrl);
    const sellerInfo = extractSellerInfo(html);
    const imageUrl = normalizeDealImageUrl(extractAmazonImage(html)) || '';

    logGeneratorDebug('api.amazon.scrape.success', {
      url: trimmedUrl,
      asin,
      sellerType: sellerInfo.sellerType,
      hasImage: Boolean(imageUrl),
      normalizedUrl
    });

    return res.status(200).json({
      success: true,
      title: extractAmazonTitle(html) || '',
      image: imageUrl,
      price: extractAmazonPrice(html) || '',
      oldPrice: extractAmazonOldPrice(html) || '',
      asin,
      finalUrl: canonicalUrl || response.url || trimmedUrl,
      originalUrl: trimmedUrl,
      normalizedUrl,
      sellerType: sellerInfo.sellerType
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
