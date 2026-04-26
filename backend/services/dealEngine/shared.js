import { normalizeSellerType } from '../sellerClassificationService.js';

export function cleanText(value) {
  return typeof value === 'string' ? value.trim() : '';
}

export function parseNumber(value, fallback = null) {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }

  if (typeof value !== 'string') {
    return fallback;
  }

  const trimmed = value.trim();
  if (!trimmed) {
    return fallback;
  }

  const normalized = trimmed.replace(/[^\d,.-]/g, '').replace(',', '.');
  const parsed = Number.parseFloat(normalized);
  return Number.isFinite(parsed) ? parsed : fallback;
}

export function parseInteger(value, fallback = 0) {
  const parsed = Number.parseInt(String(value ?? ''), 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

export function parseBool(value, fallback = false) {
  if (value === true || value === false) {
    return value;
  }

  if (value === 1 || value === '1' || value === 'true') {
    return true;
  }

  if (value === 0 || value === '0' || value === 'false') {
    return false;
  }

  return fallback;
}

export function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

export function round(value, precision = 2) {
  const numericValue = Number(value);
  if (!Number.isFinite(numericValue)) {
    return 0;
  }

  const factor = 10 ** precision;
  return Math.round(numericValue * factor) / factor;
}

export function nowIso() {
  return new Date().toISOString();
}

export function toJson(value) {
  return JSON.stringify(value ?? null);
}

export function fromJson(value, fallback = null) {
  try {
    if (!value) {
      return fallback;
    }

    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

export function ensureArray(value) {
  return Array.isArray(value) ? value : [];
}

export function formatMoney(value, currency = 'EUR') {
  const parsed = parseNumber(value, null);
  if (parsed === null) {
    return '-';
  }

  return new Intl.NumberFormat('de-DE', {
    style: 'currency',
    currency
  }).format(parsed);
}

export function normalizeSellerArea(value) {
  const normalized = normalizeSellerType(value);

  if (normalized === 'AMAZON' || normalized === 'FBA' || normalized === 'FBM') {
    return normalized;
  }

  return 'UNKNOWN';
}

export function isAmazonLink(value) {
  const rawValue = cleanText(value);
  if (!rawValue) {
    return false;
  }

  try {
    const url = new URL(rawValue);
    return /(^|\.)amazon\./i.test(url.hostname);
  } catch {
    return false;
  }
}

export function extractAsinFromAmazonUrl(value) {
  const rawValue = cleanText(value);
  if (!rawValue) {
    return '';
  }

  const match = rawValue.match(/\/(?:dp|gp\/product|gp\/aw\/d|exec\/obidos\/ASIN)\/([A-Z0-9]{10})(?:[/?]|$)/i);
  return match?.[1]?.toUpperCase() || '';
}

export function buildArrayFromTextList(value) {
  if (Array.isArray(value)) {
    return value.map((item) => cleanText(String(item))).filter(Boolean);
  }

  if (typeof value === 'string') {
    return value
      .split(',')
      .map((item) => cleanText(item))
      .filter(Boolean);
  }

  return [];
}

export function normalizeDecision(value) {
  const normalized = cleanText(String(value || '')).toUpperCase();
  return ['APPROVE', 'QUEUE', 'REJECT'].includes(normalized) ? normalized : 'REJECT';
}

export function normalizeDayPart(value) {
  const normalized = cleanText(String(value || '')).toLowerCase();
  return ['day', 'night'].includes(normalized) ? normalized : '';
}

export function summarizeReasons(reasons = [], fallback = 'Keine Detailbegruendung hinterlegt.') {
  const normalized = ensureArray(reasons)
    .map((item) => {
      if (item && typeof item === 'object' && !Array.isArray(item)) {
        return cleanText(item.message || item.reason || item.detail || item.code);
      }

      return cleanText(String(item));
    })
    .filter(Boolean);

  return normalized.length ? normalized.join(' | ') : fallback;
}

export function getOfferTotalPrice(offer = {}) {
  const explicitTotal = parseNumber(offer.totalPrice ?? offer.total_price, null);
  if (explicitTotal !== null) {
    return round(explicitTotal, 2);
  }

  const basePrice = parseNumber(offer.price ?? offer.currentPrice ?? offer.current_price, null);
  if (basePrice === null) {
    return null;
  }

  const shippingPrice = parseNumber(offer.shippingPrice ?? offer.shipping_price ?? offer.shipping ?? 0, 0) ?? 0;
  return round(basePrice + shippingPrice, 2);
}
