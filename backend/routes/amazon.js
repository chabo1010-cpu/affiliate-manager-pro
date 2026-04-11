import { Router } from 'express';

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
    /<meta[^>]+content=["']([^"']+)["'][^>]+property=["']og:image["']/i,
    /<meta[^>]+name=["']twitter:image["'][^>]+content=["']([^"']+)["']/i,
    /"hiRes"\s*:\s*"([^"]+)"/i,
    /"large"\s*:\s*"([^"]+)"/i
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

function extractAmazonPrice(html) {
  const whole = html.match(
    /<span[^>]+class=["'][^"']*a-price-whole[^"']*["'][^>]*>\s*([^<]+)\s*<\/span>/i
  )?.[1];
  const fraction = html.match(
    /<span[^>]+class=["'][^"']*a-price-fraction[^"']*["'][^>]*>\s*([^<]+)\s*<\/span>/i
  )?.[1];

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

async function handleScrape(req, res) {
  try {
    console.log('SCRAPE ROUTE HIT');
    console.log('REQ BODY:', req.body);
    const { url } = req.body ?? {};
    console.log('REQ BODY URL:', req.body?.url);

    if (!url || typeof url !== 'string' || !url.trim()) {
      return res.status(400).json({
        success: false,
        error: 'Kein url uebergeben',
        code: 'MISSING_URL'
      });
    }

    const trimmedUrl = url.trim();
    console.log('[amazon/scrape] start scraping', { url: trimmedUrl });

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

    if (response.status === 403 || /captcha|robot check|benningtonschools|sorry/i.test(html)) {
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

    const title = extractAmazonTitle(html);
    const image = normalizeDealImageUrl(extractAmazonImage(html));
    const oldPrice = extractAmazonOldPrice(html);
    const price = extractAmazonPrice(html);

    console.log('[amazon/scrape] result', {
      url: trimmedUrl,
      hasTitle: Boolean(title),
      hasPrice: Boolean(price),
      hasOldPrice: Boolean(oldPrice),
      hasImage: Boolean(image)
    });
    console.log('[amazon/scrape] success');

    return res.status(200).json({
      success: true,
      title: title || '',
      image: image || '',
      price: price || '',
      oldPrice: oldPrice || ''
    });
  } catch (error) {
    console.error('[amazon/scrape] scrape error', error);
    console.log('[amazon/scrape] error');

    return res.status(500).json({
      success: false,
      error:
        error instanceof Error
          ? `Scrape failed: ${error.message}`
          : 'Scrape failed',
      code: 'SCRAPE_FAILED'
    });
  }
}

router.post('/scrape', handleScrape);

export default router;
