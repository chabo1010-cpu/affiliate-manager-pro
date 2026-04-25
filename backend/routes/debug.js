import { Router } from 'express';
import { getReaderRuntimeConfig } from '../env.js';
import { buildAmazonAffiliateLinkRecord, normalizeSellerType, resetDealLockHistory } from '../services/dealHistoryService.js';
import { publishGeneratorPostDirect } from '../services/directPublisher.js';
import { forceScanTelegramReader } from '../services/telegramUserClientService.js';
import { generatePostText } from '../../frontend/src/lib/postGenerator.js';

const router = Router();

function getRequesterRole(req) {
  return String(req.headers['x-user-role'] || '').trim().toLowerCase();
}

function requireAdmin(req, res, next) {
  if (getRequesterRole(req) !== 'admin') {
    return res.status(403).json({ error: 'Nur Admin darf Debug-Generator-Posts ausloesen.' });
  }

  return next();
}

function cleanText(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function getDealLockDebugGuard() {
  const runtimeConfig = getReaderRuntimeConfig();
  return {
    enabled: runtimeConfig.readerTestMode || runtimeConfig.readerDebugMode,
    readerTestMode: runtimeConfig.readerTestMode,
    readerDebugMode: runtimeConfig.readerDebugMode
  };
}

router.post('/test-generator-publish', requireAdmin, async (req, res) => {
  const asin = cleanText(req.body?.asin).toUpperCase();
  const price = cleanText(req.body?.price) || '32.95';
  const sellerType = normalizeSellerType(req.body?.sellerType || 'AMAZON');
  const source = cleanText(req.body?.source) || 'debug';

  console.info('[DEBUG_GENERATOR_PUBLISH_START]', {
    asin,
    price,
    sellerType,
    source
  });

  try {
    if (!asin) {
      throw new Error('ASIN fehlt.');
    }

    const linkRecord = buildAmazonAffiliateLinkRecord(asin, { asin });
    if (!linkRecord.valid || !cleanText(linkRecord.affiliateUrl)) {
      throw new Error('Partnerlink konnte fuer den Debug-Test nicht gebaut werden.');
    }

    console.info('[GENERATOR_FORMAT_START]', {
      asin,
      sellerType,
      source
    });

    const generatedPost = generatePostText({
      productTitle: `Debug Amazon Deal ${asin}`,
      freiText: '',
      textBaustein: [],
      alterPreis: '',
      neuerPreis: price,
      alterPreisLabel: 'Vorher',
      neuerPreisLabel: 'Jetzt',
      amazonLink: linkRecord.affiliateUrl,
      werbung: false,
      extraOptions: [],
      rabattgutscheinCode: ''
    });

    const publishInput = {
      title: generatedPost.productTitle || `Debug Amazon Deal ${asin}`,
      link: linkRecord.affiliateUrl,
      normalizedUrl: linkRecord.normalizedUrl,
      asin,
      sellerType,
      currentPrice: price,
      oldPrice: '',
      couponCode: '',
      textByChannel: {
        telegram: generatedPost.telegramCaption,
        whatsapp: generatedPost.whatsappText,
        facebook: generatedPost.whatsappText
      },
      generatedImagePath: '',
      uploadedImagePath: '',
      uploadedImageFile: null,
      telegramImageSource: 'none',
      whatsappImageSource: 'none',
      facebookImageSource: 'link_preview',
      enableTelegram: true,
      enableWhatsapp: false,
      enableFacebook: false,
      queueSourceType: 'generator_direct',
      originOverride: 'automatic',
      contextSource: 'debug_generator_publish'
    };

    console.info('[GENERATOR_FORMAT_SUCCESS]', {
      asin,
      sellerType,
      title: publishInput.title,
      affiliateUrl: publishInput.link
    });
    console.info('[GENERATOR_OUTPUT_READY]', {
      asin,
      sellerType,
      link: publishInput.link,
      imageSource: publishInput.telegramImageSource
    });

    const result = await publishGeneratorPostDirect(publishInput);

    return res.json({
      success: true,
      asin,
      queue: result.queue || null,
      results: result.results || {}
    });
  } catch (error) {
    console.error('[ERROR_REASON]', {
      reason: error instanceof Error ? error.message : 'Debug Generator Publish fehlgeschlagen.'
    });
    return res.status(400).json({
      success: false,
      error: error instanceof Error ? error.message : 'Debug Generator Publish fehlgeschlagen.'
    });
  }
});

router.post('/reset-deal-lock', requireAdmin, async (req, res) => {
  const dealLockDebugGuard = getDealLockDebugGuard();

  if (!dealLockDebugGuard.enabled) {
    return res.status(403).json({
      success: false,
      error: 'Deal-Lock-Reset ist nur mit READER_TEST_MODE=1 oder READER_DEBUG_MODE=1 erlaubt.'
    });
  }

  const asin = cleanText(req.body?.asin).toUpperCase();
  const normalizedUrl = cleanText(req.body?.normalizedUrl);
  const url = cleanText(req.body?.url);
  const resetSummary = resetDealLockHistory({
    asin,
    normalizedUrl,
    url
  });

  console.info('[DEAL_LOCK_RESET]', {
    ...resetSummary,
    readerTestMode: dealLockDebugGuard.readerTestMode,
    readerDebugMode: dealLockDebugGuard.readerDebugMode
  });

  return res.json({
    success: true,
    reset: resetSummary
  });
});

router.post('/force-reader-scan', requireAdmin, async (req, res) => {
  const dealLockDebugGuard = getDealLockDebugGuard();

  console.log('[FORCE_SCAN_ENDPOINT_HIT]');

  if (!dealLockDebugGuard.enabled) {
    return res.status(403).json({
      success: false,
      error: 'Force-Reader-Scan ist nur mit READER_TEST_MODE=1 oder READER_DEBUG_MODE=1 erlaubt.'
    });
  }

  try {
    console.log('[FORCE_SCAN_START]');
    const result = await forceScanTelegramReader({
      ...(req.body ?? {}),
      ignoreLastSeen: true
    });

    return res.json({
      success: true,
      ...result
    });
  } catch (error) {
    console.error('[ERROR_REASON]', {
      reason: error instanceof Error ? error.message : 'Force-Reader-Scan konnte nicht gestartet werden.'
    });
    return res.status(400).json({
      success: false,
      error: error instanceof Error ? error.message : 'Force-Reader-Scan konnte nicht gestartet werden.'
    });
  }
});

export default router;
