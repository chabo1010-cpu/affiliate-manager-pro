import express from 'express';
import { getDb } from '../db.js';
import { logGeneratorDebug } from '../services/generatorFlowService.js';
import {
  checkDealCooldown,
  extractAsin,
  getRepostSettings,
  listDealsHistory,
  normalizeAmazonLink,
  savePostedDeal,
  saveRepostSettings
} from '../services/dealHistoryService.js';
import { buildGeneratorDealContext } from '../services/generatorDealScoringService.js';

const router = express.Router();
const db = getDb();

function extractFirstMatch(html, patterns) {
  for (const pattern of patterns) {
    const match = html.match(pattern);
    if (match?.[1]) {
      return match[1].trim();
    }
  }

  return '';
}

function extractCanonicalUrl(html) {
  return extractFirstMatch(html, [
    /<link[^>]+rel=["']canonical["'][^>]+href=["']([^"']+)["']/i,
    /<meta[^>]+property=["']og:url["'][^>]+content=["']([^"']+)["']/i
  ]);
}

async function resolveDealIdentity(inputUrl) {
  const rawUrl = (inputUrl || '').trim();
  let resolvedFinalUrl = rawUrl;
  let asin = extractAsin(rawUrl);
  let normalizedFinalUrl = normalizeAmazonLink(rawUrl);

  if (!rawUrl) {
    return { rawUrl, resolvedFinalUrl, asin, normalizedFinalUrl };
  }

  try {
    const response = await fetch(rawUrl, {
      headers: {
        'User-Agent':
          'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36',
        'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8'
      }
    });
    const html = await response.text();
    const canonicalUrl = extractCanonicalUrl(html);
    resolvedFinalUrl = canonicalUrl || response.url || rawUrl;
    asin = extractAsin(resolvedFinalUrl) || extractAsin(response.url || '') || asin;
    normalizedFinalUrl = normalizeAmazonLink(resolvedFinalUrl || rawUrl);
  } catch {
    return { rawUrl, resolvedFinalUrl, asin, normalizedFinalUrl };
  }

  return { rawUrl, resolvedFinalUrl, asin, normalizedFinalUrl };
}

router.get('/', async (req, res) => {
  try {
    const deals = db.prepare(`SELECT * FROM deals ORDER BY createdAt DESC`).all();
    return res.json(deals);
  } catch (error) {
    if (error instanceof Error && /no such table:\s*deals/i.test(error.message)) {
      console.warn('GET /api/deals: deals table not found, returning empty list');
      return res.json([]);
    }

    console.error('GET /api/deals error:', error);
    return res.status(500).json({
      error: 'Failed to fetch deals'
    });
  }
});

router.post('/check', async (req, res) => {
  const { url = '', asin = '', normalizedUrl = '', sellerType = '', currentPrice = '', title = '', imageUrl = '' } = req.body ?? {};
  const requestPayload = {
    url: typeof url === 'string' ? url.trim() : '',
    asin: typeof asin === 'string' ? asin.trim() : '',
    normalizedUrl: typeof normalizedUrl === 'string' ? normalizedUrl.trim() : '',
    sellerType: typeof sellerType === 'string' ? sellerType.trim() : '',
    currentPrice,
    title: typeof title === 'string' ? title.trim() : '',
    imageUrl: typeof imageUrl === 'string' ? imageUrl.trim() : ''
  };

  logGeneratorDebug('api.deals.check.request', requestPayload);

  if (
    (!url || typeof url !== 'string' || !url.trim()) &&
    (!asin || typeof asin !== 'string' || !asin.trim()) &&
    (!normalizedUrl || typeof normalizedUrl !== 'string' || !normalizedUrl.trim())
  ) {
    return res.status(400).json({
      blocked: false,
      lastPostedAt: null,
      minPrice: null,
      maxPrice: null,
      sellerType: null,
      remainingSeconds: 0,
      error: 'URL fehlt'
    });
  }

  let identity = {
    rawUrl: (url || '').trim(),
    resolvedFinalUrl: (url || '').trim(),
    asin: (asin || '').trim(),
    normalizedFinalUrl: (normalizedUrl || '').trim()
  };

  if (!identity.asin && !identity.normalizedFinalUrl && identity.rawUrl) {
    identity = await resolveDealIdentity(url);
  } else {
    identity.resolvedFinalUrl = identity.resolvedFinalUrl || identity.normalizedFinalUrl || identity.rawUrl;
    identity.normalizedFinalUrl = identity.normalizedFinalUrl || normalizeAmazonLink(identity.resolvedFinalUrl);
    identity.asin = identity.asin || extractAsin(identity.resolvedFinalUrl) || extractAsin(identity.normalizedFinalUrl);
  }

  const result = checkDealCooldown({
    url: identity.rawUrl,
    finalUrl: identity.resolvedFinalUrl,
    normalizedUrl: identity.normalizedFinalUrl,
    asin: identity.asin
  });
  const responsePayload = {
    blocked: result.blocked === true,
    lastPostedAt: result.lastDeal?.postedAt || null,
    minPrice: result.minPrice ?? null,
    maxPrice: result.maxPrice ?? null,
    sellerType: result.lastDeal?.sellerType || null,
    remainingSeconds: Number.isFinite(result.remainingSeconds) ? result.remainingSeconds : 0,
    postingCount: result.postingCount ?? 0,
    asin: result.asin || null,
    normalizedUrl: result.normalizedUrl || null,
    resolvedFinalUrl: identity.resolvedFinalUrl || null,
    lastDeal: result.lastDeal || null
  };

  try {
    responsePayload.generatorContext = await buildGeneratorDealContext({
      asin: responsePayload.asin || identity.asin || '',
      sellerType: requestPayload.sellerType || responsePayload.sellerType || 'FBM',
      currentPrice: requestPayload.currentPrice || result.lastDeal?.currentPrice || '',
      title: requestPayload.title || result.lastDeal?.title || '',
      productUrl: responsePayload.resolvedFinalUrl || responsePayload.normalizedUrl || requestPayload.url,
      imageUrl: requestPayload.imageUrl || '',
      source: 'generator_check'
    });
  } catch (error) {
    responsePayload.generatorContext = {
      asin: responsePayload.asin || identity.asin || '',
      sellerType: responsePayload.sellerType || 'FBM',
      keepa: {
        available: false,
        status: 'error',
        reason: error instanceof Error ? error.message : 'Generator-Kontext konnte nicht aufgebaut werden.'
      },
      evaluation: null,
      review: null
    };
  }

  logGeneratorDebug('api.deals.check.response', {
    blocked: responsePayload.blocked,
    asin: responsePayload.asin,
    normalizedUrl: responsePayload.normalizedUrl,
    postingCount: responsePayload.postingCount,
    remainingSeconds: responsePayload.remainingSeconds,
    keepaStatus: responsePayload.generatorContext?.keepa?.status || 'missing',
    sellerTypeDecision: responsePayload.generatorContext?.evaluation?.decision || 'unavailable'
  });

  return res.status(200).json(responsePayload);
});

router.post('/save', (req, res) => {
  const { url, title, price, sellerType, postedAt } = req.body ?? {};

  if (!url || !title || !price || !sellerType || !postedAt) {
    return res.status(400).json({
      success: false,
      error: 'Pflichtfelder fuer Deal-Speicherung fehlen'
    });
  }

  const saved = savePostedDeal(req.body ?? {});
  return res.status(201).json({
    success: true,
    item: saved
  });
});

router.get('/history', (req, res) => {
  return res.status(200).json({
    success: true,
    items: listDealsHistory({
      sellerType: req.query?.sellerType || req.query?.marketplaceType,
      startDate: req.query?.startDate,
      endDate: req.query?.endDate,
      asin: req.query?.asin,
      url: req.query?.url,
      title: req.query?.title
    })
  });
});

router.get('/settings', (req, res) => {
  try {
    db.exec(`
      CREATE TABLE IF NOT EXISTS app_settings (
        id INTEGER PRIMARY KEY,
        repostCooldownEnabled INTEGER NOT NULL DEFAULT 1,
        repostCooldownHours INTEGER NOT NULL DEFAULT 12,
        telegramCopyButtonText TEXT NOT NULL DEFAULT '📋 Zum Kopieren hier klicken'
      )
    `);

    const settings = getRepostSettings();
    logGeneratorDebug('REPOST LOCK LOAD SUCCESS', {
      repostCooldownEnabled: settings.repostCooldownEnabled,
      repostCooldownHours: settings.repostCooldownHours
    });

    return res.json({
      repostCooldownEnabled: settings.repostCooldownEnabled,
      repostCooldownHours: settings.repostCooldownHours,
      telegramCopyButtonText: settings.telegramCopyButtonText
    });
  } catch (error) {
    console.error('SETTINGS LOAD ERROR', error);
    logGeneratorDebug('REPOST LOCK LOAD FAILED', {
      error: error instanceof Error ? error.message : 'Settings-Laden fehlgeschlagen.'
    });
    return res.status(500).json({
      error: error instanceof Error ? error.message : 'Settings-Laden fehlgeschlagen.'
    });
  }
});

const saveSettingsHandler = (req, res) => {
  try {
    const rawEnabled = req.body?.repostCooldownEnabled;
    const rawHours = req.body?.repostCooldownHours;
    const rawTelegramCopyButtonText = req.body?.telegramCopyButtonText;
    const requesterRole = String(req.headers['x-user-role'] || '').trim().toLowerCase();
    const wantsToSaveTelegramCopyButtonText = rawTelegramCopyButtonText !== undefined;
    const currentSettings = getRepostSettings();
    logGeneratorDebug('REPOST LOCK SAVE START', {
      requesterRole,
      repostCooldownEnabled: rawEnabled,
      repostCooldownHours: rawHours
    });

    if (wantsToSaveTelegramCopyButtonText && requesterRole !== 'admin') {
      logGeneratorDebug('REPOST LOCK SAVE FAILED', {
        error: 'Nicht autorisiert fuer Telegram Copy-Button Text.',
        requesterRole
      });
      return res.status(403).json({ error: 'Nur Admin darf den Telegram Copy-Button Text speichern.' });
    }

    const enabled =
      rawEnabled === undefined
        ? currentSettings.repostCooldownEnabled
        : rawEnabled === true || rawEnabled === 1 || rawEnabled === '1';

    const hours =
      rawHours === undefined || rawHours === null || rawHours === ''
        ? currentSettings.repostCooldownHours
        : Number(rawHours);

    if (!Number.isFinite(hours) || hours < 1) {
      logGeneratorDebug('REPOST LOCK SAVE FAILED', {
        error: 'Ungueltige Sperrzeit.',
        repostCooldownHours: rawHours
      });
      return res.status(400).json({ error: 'Ungueltige Sperrzeit.' });
    }

    if (Number.isNaN(hours)) {
      logGeneratorDebug('REPOST LOCK SAVE FAILED', {
        error: 'Ungueltige Sperrzeit.',
        repostCooldownHours: rawHours
      });
      return res.status(400).json({ error: 'Ungültige Sperrzeit.' });
    }

    db.exec(`
      CREATE TABLE IF NOT EXISTS app_settings (
        id INTEGER PRIMARY KEY,
        repostCooldownEnabled INTEGER NOT NULL DEFAULT 1,
        repostCooldownHours INTEGER NOT NULL DEFAULT 12,
        telegramCopyButtonText TEXT NOT NULL DEFAULT '📋 Zum Kopieren hier klicken'
      )
    `);
    const saved = saveRepostSettings({
      repostCooldownEnabled: enabled ? 1 : 0,
      repostCooldownHours: hours,
      telegramCopyButtonText: wantsToSaveTelegramCopyButtonText ? rawTelegramCopyButtonText : undefined
    });
    logGeneratorDebug('REPOST LOCK SAVE SUCCESS', {
      repostCooldownEnabled: saved.repostCooldownEnabled,
      repostCooldownHours: saved.repostCooldownHours
    });

    return res.json({
      success: true,
      repostCooldownEnabled: Boolean(saved.repostCooldownEnabled),
      repostCooldownHours: Number(saved.repostCooldownHours),
      telegramCopyButtonText: saved.telegramCopyButtonText
    });
  } catch (error) {
    console.error('SETTINGS SAVE ERROR', error);
    logGeneratorDebug('REPOST LOCK SAVE FAILED', {
      error: error instanceof Error ? error.message : 'Settings-Speichern fehlgeschlagen.'
    });
    return res.status(500).json({
      error: error instanceof Error ? error.message : 'Settings-Speichern fehlgeschlagen.'
    });
  }
};

router.post('/settings', saveSettingsHandler);
router.put('/settings', saveSettingsHandler);

export default router;
