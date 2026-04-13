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
      remainingSeconds: null,
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
  const isBlocked = result.blocked === true;
  const responsePayload = {
    blocked: isBlocked,
    lastPostedAt: result.lastDeal?.postedAt || null,
    minPrice: result.minPrice ?? null,
    maxPrice: result.maxPrice ?? null,
    sellerType: result.lastDeal?.sellerType || null,
    remainingSeconds: isBlocked && typeof result.remainingMs === 'number' ? Math.ceil(result.remainingMs / 1000) : 0,
    repostCooldownEnabled: Boolean(result.cooldownEnabled),
    repostCooldownHours: result.cooldownHours ?? 12,
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
        repostCooldownHours INTEGER NOT NULL DEFAULT 12
      )
    `);

    const row = getRepostSettingsRow();
    const settings = getRepostSettings();

    console.log('SETTINGS LOAD ROW', row);
    console.log('SETTINGS USED IN DEAL-HISTORY LOAD', row);
    console.log('SETTINGS LOAD RESPONSE', settings);

    return res.json({
      repostCooldownEnabled: settings.repostCooldownEnabled,
      repostCooldownHours: settings.repostCooldownHours
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

    const enabled =
      rawEnabled === true ||
      rawEnabled === 1 ||
      rawEnabled === '1';

    const hours =
      rawHours === undefined || rawHours === null || rawHours === ''
        ? 12
        : Number(rawHours);

    if (Number.isNaN(hours)) {
      return res.status(400).json({ error: 'Ungültige Sperrzeit.' });
    }

    db.exec(`
      CREATE TABLE IF NOT EXISTS app_settings (
        id INTEGER PRIMARY KEY,
        repostCooldownEnabled INTEGER NOT NULL DEFAULT 1,
        repostCooldownHours INTEGER NOT NULL DEFAULT 12
      )
    `);

    console.log('SETTINGS SAVE DB BEFORE', db.prepare(`SELECT * FROM app_settings WHERE id = 1`).get());

    const existing = db.prepare(`
      SELECT id FROM app_settings WHERE id = 1
    `).get();

    if (existing) {
      db.prepare(`
        UPDATE app_settings
        SET repostCooldownEnabled = ?, repostCooldownHours = ?
        WHERE id = 1
      `).run(enabled ? 1 : 0, hours);
    } else {
      db.prepare(`
        INSERT INTO app_settings (id, repostCooldownEnabled, repostCooldownHours)
        VALUES (1, ?, ?)
      `).run(enabled ? 1 : 0, hours);
    }

    const saved = db.prepare(`
      SELECT id, repostCooldownEnabled, repostCooldownHours
      FROM app_settings
      WHERE id = 1
    `).get();

    console.log('SETTINGS SAVE FINAL', { enabled, hours });
    console.log('SETTINGS SAVE DB AFTER', saved);
    console.log('APP SETTINGS ROW AFTER SAVE', saved);

    return res.json({
      success: true,
      repostCooldownEnabled: saved.repostCooldownEnabled === 1,
      repostCooldownHours: Number(saved.repostCooldownHours)
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
