import { getDb } from '../db.js';

const db = getDb();
const DEFAULT_TELEGRAM_COPY_BUTTON_TEXT = '📋 Zum Kopieren hier klicken';

function cleanText(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function parseEnabledFlag(value) {
  return value === 1 || value === '1' || value === true;
}

function parseNumber(value) {
  const trimmed = cleanText(value);
  if (!trimmed) {
    return null;
  }

  const normalized = trimmed
    .replace(/[^\d,.-]/g, '')
    .replace(/\.(?=\d{3}(?:\D|$))/g, '')
    .replace(',', '.');

  const parsed = Number.parseFloat(normalized);
  return Number.isFinite(parsed) ? parsed : null;
}

function safeUrl(value) {
  try {
    return new URL(value);
  } catch {
    return null;
  }
}

function findLatestDealByField(field, value) {
  const cleanValue = cleanText(value);
  if (!cleanValue) {
    return null;
  }

  return (
    db
      .prepare(
        `
          SELECT id, asin, url, normalizedUrl, title, price, oldPrice, sellerType, postedAt, channel, couponCode
          FROM deals_history
          WHERE ${field} = ?
          ORDER BY postedAt DESC
          LIMIT 1
        `
      )
      .get(cleanValue) || null
  );
}

function findDealsByField(field, value) {
  const cleanValue = cleanText(value);
  if (!cleanValue) {
    return [];
  }

  return db
    .prepare(
      `
        SELECT id, asin, url, normalizedUrl, title, price, oldPrice, sellerType, postedAt, channel, couponCode
        FROM deals_history
        WHERE ${field} = ?
        ORDER BY postedAt DESC
      `
    )
    .all(cleanValue);
}

export function extractAsin(value = '') {
  const match = cleanText(value).match(/(?:\/dp\/|\/gp\/product\/|[?&]asin=)([A-Z0-9]{10})/i);
  return match?.[1]?.toUpperCase() || '';
}

export function normalizeAmazonLink(value = '') {
  const trimmed = cleanText(value);
  if (!trimmed) {
    return '';
  }

  const asin = extractAsin(trimmed);
  const parsed = safeUrl(trimmed);
  if (!parsed) {
    return trimmed;
  }

  const hostname = parsed.hostname.toLowerCase().replace(/^www\./, '');
  if (asin && /amazon\./i.test(hostname)) {
    return `https://${hostname}/dp/${asin}`;
  }

  return `${parsed.protocol}//${hostname}${parsed.pathname.replace(/\/+$/, '') || '/'}`;
}

export function classifySellerType({ soldByAmazon, shippedByAmazon }) {
  if (soldByAmazon && shippedByAmazon) {
    return 'AMAZON';
  }

  if (!soldByAmazon && shippedByAmazon) {
    return 'FBA';
  }

  return 'FBM';
}

export function getRepostCooldownHours() {
  const row = db.prepare(`SELECT repostCooldownHours FROM app_settings WHERE id = 1`).get();
  const rawValue = row?.repostCooldownHours;
  console.log('SETTINGS LOAD DB VALUE', rawValue);
  const hours = rawValue === undefined || rawValue === null ? 12 : Number(rawValue);
  return Number.isFinite(hours) && hours >= 0 ? hours : 12;
}

export function getRepostSettingsRow() {
  let row = db
    .prepare(
      `SELECT id, repostCooldownEnabled, repostCooldownHours, telegramCopyButtonText FROM app_settings WHERE id = 1`
    )
    .get();

  if (!row) {
    db.prepare(
      `
        INSERT INTO app_settings (id, repostCooldownEnabled, repostCooldownHours, telegramCopyButtonText)
        VALUES (1, 1, 12, ?)
      `
    ).run(DEFAULT_TELEGRAM_COPY_BUTTON_TEXT);

    row = db
      .prepare(
        `SELECT id, repostCooldownEnabled, repostCooldownHours, telegramCopyButtonText FROM app_settings WHERE id = 1`
      )
      .get();
  }

  return row ?? null;
}

export function getRepostSettings() {
  const row = getRepostSettingsRow();

  const enabledRaw = row?.repostCooldownEnabled;
  const hoursRaw = row?.repostCooldownHours;
  const telegramCopyButtonTextRaw = cleanText(row?.telegramCopyButtonText);
  const repostCooldownEnabled = parseEnabledFlag(enabledRaw);
  const repostCooldownHours = hoursRaw === undefined || hoursRaw === null ? 12 : Number(hoursRaw);

  console.log('SETTINGS LOAD ROW', row ?? null);

  return {
    repostCooldownEnabled,
    repostCooldownHours:
      Number.isFinite(repostCooldownHours) && repostCooldownHours >= 0 ? repostCooldownHours : 12,
    telegramCopyButtonText: telegramCopyButtonTextRaw || DEFAULT_TELEGRAM_COPY_BUTTON_TEXT
  };
}

function parseEnabledValue(value) {
  return parseEnabledFlag(value);
}

export function saveRepostSettings({
  repostCooldownEnabled,
  repostCooldownHours,
  telegramCopyButtonText
}) {
  const enabled = parseEnabledValue(repostCooldownEnabled) ? 1 : 0;
  const hours = Number(repostCooldownHours);
  const nextHours = Number.isFinite(hours) && hours >= 0 ? Math.round(hours) : 12;
  const nextTelegramCopyButtonText = cleanText(telegramCopyButtonText) || DEFAULT_TELEGRAM_COPY_BUTTON_TEXT;

  const updateResult = db
    .prepare(
      `
        UPDATE app_settings
        SET repostCooldownEnabled = ?, repostCooldownHours = ?, telegramCopyButtonText = ?
        WHERE id = 1
      `
    )
    .run(enabled, nextHours, nextTelegramCopyButtonText);

  if (!updateResult.changes) {
    db.prepare(
      `
        INSERT INTO app_settings (id, repostCooldownEnabled, repostCooldownHours, telegramCopyButtonText)
        VALUES (1, ?, ?, ?)
      `
    ).run(enabled, nextHours, nextTelegramCopyButtonText);
  }

  return {
    repostCooldownEnabled: enabled === 1,
    repostCooldownHours: nextHours,
    telegramCopyButtonText: nextTelegramCopyButtonText
  };
}

export function getTelegramCopyButtonText() {
  const row = getRepostSettingsRow();
  const telegramCopyButtonText = cleanText(row?.telegramCopyButtonText);
  return telegramCopyButtonText || DEFAULT_TELEGRAM_COPY_BUTTON_TEXT;
}

function getDealMatchClause(asin, normalizedUrl) {
  if (asin) {
    return {
      clause: `asin = @asin`,
      params: { asin }
    };
  }

  if (normalizedUrl) {
    return {
      clause: `normalizedUrl = @normalizedUrl`,
      params: { normalizedUrl }
    };
  }

  return {
    clause: `1 = 0`,
    params: {}
  };
}

export function getDealHistorySummary({ asin = '', normalizedUrl = '' } = {}) {
  const cleanAsin = cleanText(asin).toUpperCase();
  const cleanNormalizedUrl = cleanText(normalizedUrl);
  const { clause, params } = getDealMatchClause(cleanAsin, cleanNormalizedUrl);
  const sixMonthsAgo = new Date();
  sixMonthsAgo.setMonth(sixMonthsAgo.getMonth() - 6);

  const rows = db
    .prepare(
      `
        SELECT id, asin, url, normalizedUrl, title, price, oldPrice, sellerType, postedAt, channel, couponCode
        FROM deals_history
        WHERE ${clause}
        ORDER BY postedAt DESC
      `
    )
    .all(params);

  const prices = rows
    .filter((row) => {
      const postedAt = new Date(row.postedAt).getTime();
      return Number.isFinite(postedAt) && postedAt >= sixMonthsAgo.getTime();
    })
    .map((row) => parseNumber(row.price))
    .filter((value) => typeof value === 'number');

  const latest = rows[0] || null;
  const minPrice = prices.length ? Math.min(...prices) : null;
  const maxPrice = prices.length ? Math.max(...prices) : null;

  console.log('PRICE HISTORY 6 MONTHS', rows.filter((row) => {
    const postedAt = new Date(row.postedAt).getTime();
    return Number.isFinite(postedAt) && postedAt >= sixMonthsAgo.getTime();
  }));
  console.log('MIN MAX RESULT', { minPrice, maxPrice });

  return {
    latest,
    minPrice,
    maxPrice,
    count: rows.length
  };
}

function buildHistorySummary(rows = []) {
  const sixMonthsAgo = new Date();
  sixMonthsAgo.setMonth(sixMonthsAgo.getMonth() - 6);

  const prices = rows
    .filter((row) => {
      const postedAt = new Date(row.postedAt).getTime();
      return Number.isFinite(postedAt) && postedAt >= sixMonthsAgo.getTime();
    })
    .map((row) => parseNumber(row.price))
    .filter((value) => typeof value === 'number');

  const latest = rows[0] || null;
  const minPrice = prices.length ? Math.min(...prices) : null;
  const maxPrice = prices.length ? Math.max(...prices) : null;

  return {
    latest,
    minPrice,
    maxPrice,
    count: rows.length
  };
}

export function checkDealCooldown(input = {}) {
  const rawUrl = cleanText(input.url || '');
  const finalUrl = cleanText(input.finalUrl || input.url || '');
  const extractedAsin =
    cleanText(input.asin).toUpperCase() || extractAsin(input.finalUrl || input.url || input.normalizedUrl || '');
  const normalizedUrl = normalizeAmazonLink(input.normalizedUrl || input.finalUrl || input.url || '');
  const matchesByAsin = extractedAsin ? findDealsByField('asin', extractedAsin) : [];
  const matchesByNormalizedUrl = normalizedUrl ? findDealsByField('normalizedUrl', normalizedUrl) : [];
  const matchesByUrl =
    !matchesByAsin.length && !matchesByNormalizedUrl.length && rawUrl ? findDealsByField('url', rawUrl) : [];
  const historyRows = matchesByAsin.length
    ? matchesByAsin
    : matchesByNormalizedUrl.length
      ? matchesByNormalizedUrl
      : matchesByUrl;
  const latestMatch = historyRows[0] || null;
  const asin = cleanText(latestMatch?.asin || extractedAsin).toUpperCase();
  const lookupNormalizedUrl = cleanText(latestMatch?.normalizedUrl || normalizedUrl);

  console.log('CHECK INPUT', {
    url: rawUrl,
    finalUrl,
    asin,
    normalizedUrl: lookupNormalizedUrl
  });
  console.log('MATCH BY ASIN', matchesByAsin);
  console.log(
    'MATCH BY NORMALIZED URL',
    matchesByNormalizedUrl.length ? matchesByNormalizedUrl : matchesByUrl
  );

  const summary = buildHistorySummary(historyRows);
  const settingsRow = getRepostSettingsRow();
  const repostCooldownEnabled = parseEnabledFlag(settingsRow?.repostCooldownEnabled);
  const parsedCooldownHours = Number(settingsRow?.repostCooldownHours);
  const repostCooldownHours =
    Number.isFinite(parsedCooldownHours) && parsedCooldownHours >= 0 ? parsedCooldownHours : 12;
  const lastPostedAt = summary.latest?.postedAt || null;
  let blocked = false;
  let remainingSeconds = 0;

  console.log('CHECK SETTINGS', {
    repostCooldownEnabled,
    repostCooldownHours
  });
  console.log('CHECK LAST POSTED AT', lastPostedAt);

  if (repostCooldownEnabled === true && lastPostedAt) {
    const nowMs = Date.now();
    const lastPostedMs = new Date(lastPostedAt).getTime();
    const cooldownMs = Number(repostCooldownHours) * 60 * 60 * 1000;
    const diffMs = nowMs - lastPostedMs;

    console.log('CHECK DIFF MS', diffMs);

    if (diffMs < cooldownMs) {
      blocked = true;
      remainingSeconds = Math.ceil((cooldownMs - diffMs) / 1000);
    }
  }

  if (repostCooldownEnabled !== true) {
    blocked = false;
    remainingSeconds = 0;
  }

  console.log('CHECK REMAINING SECONDS', remainingSeconds);

  return {
    asin,
    normalizedUrl: lookupNormalizedUrl,
    blocked,
    remainingSeconds,
    repostCooldownEnabled,
    repostCooldownHours,
    lastDeal: summary.latest,
    minPrice: summary.minPrice,
    maxPrice: summary.maxPrice,
    postingCount: summary.count
  };
}

export function savePostedDeal(input = {}) {
  const asin =
    cleanText(input.asin).toUpperCase() ||
    extractAsin(input.finalUrl || input.url || input.normalizedUrl || input.originalUrl || '');
  const finalUrl = cleanText(input.finalUrl || input.url || input.normalizedUrl || input.originalUrl);
  const normalizedUrl = normalizeAmazonLink(input.normalizedUrl || finalUrl || input.originalUrl || '');
  const postedAt = cleanText(input.postedAt) || new Date().toISOString();
  const sellerType = cleanText(input.sellerType) || 'FBM';
  const payload = {
    asin,
    url: finalUrl,
    normalizedUrl,
    title: cleanText(input.title),
    price: cleanText(input.price),
    oldPrice: cleanText(input.oldPrice),
    sellerType,
    postedAt,
    channel: cleanText(input.channel),
    couponCode: cleanText(input.couponCode)
  };

  console.log('SAVE DEAL PAYLOAD', payload);

  const result = db
    .prepare(
      `
        INSERT INTO deals_history (
          asin,
          url,
          normalizedUrl,
          title,
          price,
          oldPrice,
          sellerType,
          postedAt,
          channel,
          couponCode
        ) VALUES (
          @asin,
          @url,
          @normalizedUrl,
          @title,
          @price,
          @oldPrice,
          @sellerType,
          @postedAt,
          @channel,
          @couponCode
        )
      `
    )
    .run(payload);

  console.log('SAVE DEAL RESULT', {
    changes: result.changes,
    lastInsertRowid: result.lastInsertRowid
  });
  console.log(
    'LATEST DEALS AFTER SAVE',
    db
      .prepare(
        `
          SELECT id, asin, url, normalizedUrl, title, price, oldPrice, sellerType, postedAt, channel, couponCode
          FROM deals_history
          ORDER BY postedAt DESC
          LIMIT 5
        `
      )
      .all()
  );

  return {
    id: result.lastInsertRowid,
    asin,
    normalizedUrl,
    postedAt
  };
}

export function listDealsHistory({ sellerType = '' } = {}) {
  const cleanSellerType = cleanText(sellerType);
  const rows = cleanSellerType
    ? db
        .prepare(
          `
            SELECT id, asin, url, normalizedUrl, title, price, oldPrice, sellerType, postedAt, channel, couponCode
            FROM deals_history
            WHERE sellerType = ?
            ORDER BY postedAt DESC
          `
        )
        .all(cleanSellerType)
    : db
        .prepare(
          `
            SELECT id, asin, url, normalizedUrl, title, price, oldPrice, sellerType, postedAt, channel, couponCode
            FROM deals_history
            ORDER BY postedAt DESC
          `
        )
        .all();

  return rows;
}
