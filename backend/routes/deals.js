import express from 'express';
import { getDb } from '../db.js';
import {
  checkDealCooldown,
  extractAsin,
  getRepostSettings,
  getRepostSettingsRow,
  listDealsHistory,
  normalizeAmazonLink,
  savePostedDeal,
  saveRepostSettings
} from '../services/dealHistoryService.js';

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

  console.log('RAW INPUT URL', rawUrl);

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
  } catch (error) {
    console.log('CHECK RESOLVE FAILED', error instanceof Error ? error.message : error);
  }

  console.log('RESOLVED FINAL URL', resolvedFinalUrl);
  console.log('EXTRACTED ASIN', asin);

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
  const { url = '', asin = '', normalizedUrl = '' } = req.body ?? {};
  console.log('CHECK REQUEST BODY', req.body);

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
    console.log('RAW INPUT URL', identity.rawUrl);
    console.log('RESOLVED FINAL URL', identity.resolvedFinalUrl);
    console.log('EXTRACTED ASIN', identity.asin);
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

  console.log('DEALS CHECK RESPONSE', {
    blocked: responsePayload.blocked,
    lastPostedAt: responsePayload.lastPostedAt,
    minPrice: responsePayload.minPrice,
    maxPrice: responsePayload.maxPrice,
    sellerType: responsePayload.sellerType,
    remainingSeconds: responsePayload.remainingSeconds
  });
  console.log('FINAL CHECK RESPONSE', responsePayload);
  console.log('CHECK FINAL RESPONSE', responsePayload);

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
    items: listDealsHistory({ sellerType: req.query?.sellerType || req.query?.marketplaceType })
  });
});

router.get('/settings', (req, res) => {
  try {
    console.log('SETTINGS LOAD ROUTE HIT');
    db.exec(`
      CREATE TABLE IF NOT EXISTS app_settings (
        id INTEGER PRIMARY KEY,
        repostCooldownEnabled INTEGER NOT NULL DEFAULT 1,
        repostCooldownHours INTEGER NOT NULL DEFAULT 12,
        telegramCopyButtonText TEXT NOT NULL DEFAULT '📋 Zum Kopieren hier klicken'
      )
    `);

    const row = getRepostSettingsRow();
    const settings = getRepostSettings();

    console.log('SETTINGS LOAD ROW', row);
    console.log('SETTINGS USED IN DEAL-HISTORY LOAD', row);
    console.log('SETTINGS LOAD RESPONSE', settings);

    return res.json({
      repostCooldownEnabled: settings.repostCooldownEnabled,
      repostCooldownHours: settings.repostCooldownHours,
      telegramCopyButtonText: settings.telegramCopyButtonText
    });
  } catch (error) {
    console.error('SETTINGS LOAD ERROR', error);
    return res.status(500).json({
      error: error instanceof Error ? error.message : 'Settings-Laden fehlgeschlagen.'
    });
  }
});

const saveSettingsHandler = (req, res) => {
  try {
    console.log('SETTINGS SAVE ROUTE HIT');
    console.log('SETTINGS SAVE REQUEST BODY', req.body);

    const rawEnabled = req.body?.repostCooldownEnabled;
    const rawHours = req.body?.repostCooldownHours;
    const rawTelegramCopyButtonText = req.body?.telegramCopyButtonText;
    const requesterRole = String(req.headers['x-user-role'] || '').trim().toLowerCase();
    const wantsToSaveTelegramCopyButtonText = rawTelegramCopyButtonText !== undefined;
    const currentSettings = getRepostSettings();

    if (wantsToSaveTelegramCopyButtonText && requesterRole !== 'admin') {
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

    if (Number.isNaN(hours)) {
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
    console.log('SETTINGS SAVE DB BEFORE', db.prepare(`SELECT * FROM app_settings WHERE id = 1`).get());

    const saved = saveRepostSettings({
      repostCooldownEnabled: enabled ? 1 : 0,
      repostCooldownHours: hours,
      telegramCopyButtonText: wantsToSaveTelegramCopyButtonText ? rawTelegramCopyButtonText : undefined
    });

    console.log('SETTINGS SAVE FINAL', { enabled, hours });
    console.log('SETTINGS SAVE DB AFTER', saved);
    console.log('APP SETTINGS ROW AFTER SAVE', saved);

    return res.json({
      success: true,
      repostCooldownEnabled: Boolean(saved.repostCooldownEnabled),
      repostCooldownHours: Number(saved.repostCooldownHours),
      telegramCopyButtonText: saved.telegramCopyButtonText
    });
  } catch (error) {
    console.error('SETTINGS SAVE ERROR', error);
    return res.status(500).json({
      error: error instanceof Error ? error.message : 'Settings-Speichern fehlgeschlagen.'
    });
  }
};

router.post('/settings', saveSettingsHandler);
router.put('/settings', saveSettingsHandler);

export default router;
