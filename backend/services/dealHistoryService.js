import { getDb } from '../db.js';

const db = getDb();

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
    .prepare(`SELECT id, repostCooldownEnabled, repostCooldownHours FROM app_settings WHERE id = 1`)
    .get();

  if (!row) {
    db.prepare(
      `
        INSERT INTO app_settings (id, repostCooldownEnabled, repostCooldownHours)
        VALUES (1, 1, 12)
      `
    ).run();

    row = db
      .prepare(`SELECT id, repostCooldownEnabled, repostCooldownHours FROM app_settings WHERE id = 1`)
      .get();
  }

  return row ?? null;
}

export function getRepostSettings() {
  const row = getRepostSettingsRow();

  const enabledRaw = row?.repostCooldownEnabled;
  const hoursRaw = row?.repostCooldownHours;
  const repostCooldownEnabled = parseEnabledFlag(enabledRaw);
  const repostCooldownHours = hoursRaw === undefined || hoursRaw === null ? 12 : Number(hoursRaw);

  console.log('SETTINGS LOAD ROW', row ?? null);

  return {
    repostCooldownEnabled,
    repostCooldownHours:
      Number.isFinite(repostCooldownHours) && repostCooldownHours >= 0 ? repostCooldownHours : 12
  };
}

function parseEnabledValue(value) {
  return parseEnabledFlag(value);
}

export function saveRepostSettings({ repostCooldownEnabled, repostCooldownHours }) {
  const enabled = parseEnabledValue(repostCooldownEnabled) ? 1 : 0;
  const hours = Number(repostCooldownHours);
  const nextHours = Number.isFinite(hours) && hours >= 0 ? Math.round(hours) : 12;

  const updateResult = db
    .prepare(
      `
        UPDATE app_settings
        SET repostCooldownEnabled = ?, repostCooldownHours = ?
        WHERE id = 1
      `
    )
    .run(enabled, nextHours);

  if (!updateResult.changes) {
    db.prepare(
      `
        INSERT INTO app_settings (id, repostCooldownEnabled, repostCooldownHours)
        VALUES (1, ?, ?)
      `
    ).run(enabled, nextHours);
  }

  return {
    repostCooldownEnabled: enabled === 1,
    repostCooldownHours: nextHours
  };
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

export function checkDealCooldown(input = {}) {
  const rawUrl = cleanText(input.url || '');
  const finalUrl = cleanText(input.finalUrl || input.url || '');
  const extractedAsin =
    cleanText(input.asin).toUpperCase() || extractAsin(input.finalUrl || input.url || input.normalizedUrl || '');
  const normalizedUrl = normalizeAmazonLink(input.normalizedUrl || input.finalUrl || input.url || '');
  const matchedByAsin = extractedAsin ? findLatestDealByField('asin', extractedAsin) : null;
  const matchesByAsin = extractedAsin ? findDealsByField('asin', extractedAsin) : [];
  const matchedByFinalUrl = !matchedByAsin && normalizedUrl ? findLatestDealByField('normalizedUrl', normalizedUrl) : null;
  const matchesByFinalUrl = !matchedByAsin && normalizedUrl ? findDealsByField('normalizedUrl', normalizedUrl) : [];
  const matchedByRawUrl = !matchedByAsin && !matchedByFinalUrl && rawUrl ? findLatestDealByField('url', rawUrl) : null;
  const matchesByUrl =
    !matchedByAsin && !matchedByFinalUrl && rawUrl
      ? findDealsByField('url', rawUrl)
      : [];
  const asin = cleanText(matchedByAsin?.asin || matchedByFinalUrl?.asin || matchedByRawUrl?.asin || extractedAsin).toUpperCase();
  const lookupNormalizedUrl = cleanText(
    matchedByAsin?.normalizedUrl || matchedByFinalUrl?.normalizedUrl || matchedByRawUrl?.normalizedUrl || normalizedUrl
  );

  console.log('CHECK INPUT', {
    url: rawUrl,
    finalUrl,
    asin,
    normalizedUrl: lookupNormalizedUrl
  });
  console.log('MATCH BY ASIN', matchesByAsin);
  console.log('MATCH BY NORMALIZED URL', matchesByFinalUrl.length ? matchesByFinalUrl : matchesByUrl);

  const summary = getDealHistorySummary({ asin, normalizedUrl: lookupNormalizedUrl });
  const settingsRow = getRepostSettingsRow();
  const settings = getRepostSettings();
  const cooldownEnabled = settings.repostCooldownEnabled;
  const cooldownHours = settings.repostCooldownHours;
  const lastPostedAt = summary.latest?.postedAt || null;
  let blocked = false;
  let remainingMs = 0;
  console.log('CHECK SETTINGS USED', {
    id: 1,
    repostCooldownEnabled: cooldownEnabled ? 1 : 0,
    repostCooldownHours: cooldownHours
  });
  console.log('CHECK SETTINGS', {
    repostCooldownEnabled: cooldownEnabled,
    repostCooldownHours: cooldownHours
  });
  console.log('SETTINGS USED IN CHECK', settingsRow);
  console.log('CHECK COOLDOWN HOURS', cooldownHours);
  console.log('CHECK MATCH FOUND', {
    hasHistory: !!lastPostedAt
  });
  console.log('CHECK LAST POSTED AT', lastPostedAt);

  if (summary.latest?.postedAt && cooldownEnabled) {
    const lastTimestamp = new Date(summary.latest.postedAt).getTime();
    const diffMs = Date.now() - lastTimestamp;
    const cooldownMs = cooldownHours * 60 * 60 * 1000;
    remainingMs = Math.max(0, cooldownMs - diffMs);
    console.log('CHECK TIME DIFF MS', diffMs);
    blocked = remainingMs > 0;
  }

  if (!summary.latest?.postedAt) {
    console.log('CHECK BLOCK RESULT', {
      blocked: false,
      remainingSeconds: 0
    });
    return {
      asin,
      normalizedUrl: lookupNormalizedUrl,
      blocked: false,
      remainingMs: 0,
      cooldownEnabled,
      cooldownHours,
      lastDeal: null,
      minPrice: summary.minPrice,
      maxPrice: summary.maxPrice,
      postingCount: summary.count
    };
  }

  console.log('CHECK BLOCK RESULT', {
    blocked,
    remainingSeconds: Math.ceil(remainingMs / 1000)
  });
  console.log('CHECK REMAINING SECONDS', Math.ceil(remainingMs / 1000));

  return {
    asin,
    normalizedUrl: lookupNormalizedUrl,
    blocked,
    remainingMs,
    cooldownEnabled,
    cooldownHours,
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
