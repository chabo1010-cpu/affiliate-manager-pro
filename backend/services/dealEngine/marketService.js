import {
  buildArrayFromTextList,
  cleanText,
  ensureArray,
  getOfferTotalPrice,
  parseBool,
  parseNumber,
  round
} from './shared.js';

const SUSPICIOUS_SHOP_PATTERNS = [/fake/i, /flash/i, /mega/i, /best-deals/i, /warehouse-outlet/i];

function normalizeKey(value) {
  return cleanText(String(value || ''))
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function normalizeOffer(rawOffer = {}, index = 0) {
  return {
    id: cleanText(rawOffer.id) || `offer-${index + 1}`,
    shopName: cleanText(rawOffer.shopName || rawOffer.shop || rawOffer.seller || `Shop ${index + 1}`),
    title: cleanText(rawOffer.title),
    url: cleanText(rawOffer.url),
    variantKey: cleanText(rawOffer.variantKey || rawOffer.variant || rawOffer.model),
    quantityKey: cleanText(rawOffer.quantityKey || rawOffer.quantity || rawOffer.bundle || rawOffer.setSize),
    price: parseNumber(rawOffer.price ?? rawOffer.currentPrice, null),
    shippingPrice: parseNumber(rawOffer.shippingPrice ?? rawOffer.shipping ?? 0, 0) ?? 0,
    totalPrice: getOfferTotalPrice(rawOffer),
    currency: cleanText(rawOffer.currency) || 'EUR',
    isRealShop: rawOffer.isRealShop === undefined ? true : parseBool(rawOffer.isRealShop, true),
    variantMatch:
      rawOffer.variantMatch === undefined || rawOffer.variantMatch === null ? null : parseBool(rawOffer.variantMatch, true),
    quantityMatch:
      rawOffer.quantityMatch === undefined || rawOffer.quantityMatch === null
        ? null
        : parseBool(rawOffer.quantityMatch, true),
    incomplete: parseBool(rawOffer.incomplete, false),
    suspicionFlags: buildArrayFromTextList(rawOffer.suspicionFlags || rawOffer.flags)
  };
}

function matchesVariant(deal = {}, offer = {}, aiResolution = {}) {
  if (Array.isArray(aiResolution.resolvedOfferIds) && aiResolution.resolvedOfferIds.includes(offer.id)) {
    return true;
  }

  if (offer.variantMatch === true || offer.variantMatch === false) {
    return offer.variantMatch;
  }

  const dealVariant = normalizeKey(deal.variantKey);
  const offerVariant = normalizeKey(offer.variantKey);

  if (!dealVariant && !offerVariant) {
    return true;
  }

  if (!dealVariant || !offerVariant) {
    return false;
  }

  return dealVariant === offerVariant;
}

function matchesQuantity(deal = {}, offer = {}, aiResolution = {}) {
  if (Array.isArray(aiResolution.resolvedOfferIds) && aiResolution.resolvedOfferIds.includes(offer.id)) {
    return true;
  }

  if (offer.quantityMatch === true || offer.quantityMatch === false) {
    return offer.quantityMatch;
  }

  const dealQuantity = normalizeKey(deal.quantityKey);
  const offerQuantity = normalizeKey(offer.quantityKey);

  if (!dealQuantity && !offerQuantity) {
    return true;
  }

  if (!dealQuantity || !offerQuantity) {
    return false;
  }

  return dealQuantity === offerQuantity;
}

function isRealisticShop(offer = {}) {
  if (offer.isRealShop === false) {
    return false;
  }

  const suspiciousByName = SUSPICIOUS_SHOP_PATTERNS.some((pattern) => pattern.test(offer.shopName));
  const suspiciousByFlags = offer.suspicionFlags.some((item) => /fake|scam|spam|unreal/i.test(item));

  return !(suspiciousByName || suspiciousByFlags);
}

function isRealisticPrice(offer = {}, amazonPrice = null) {
  if (offer.totalPrice === null || offer.totalPrice <= 0) {
    return false;
  }

  if (amazonPrice !== null && offer.totalPrice < amazonPrice * 0.25) {
    return false;
  }

  return !offer.suspicionFlags.some((item) => /unreal|bait|anchor/i.test(item));
}

function buildInvalidOffer(offer, reasons) {
  return {
    ...offer,
    invalidReasons: reasons
  };
}

export function evaluateMarketComparison({ deal = {}, market = {}, aiResolution = {} }) {
  const amazonPrice = parseNumber(deal.amazonPrice, null);
  const offers = ensureArray(market.offers).map((offer, index) => normalizeOffer(offer, index));
  const validOffers = [];
  const invalidOffers = [];

  offers.forEach((offer) => {
    const reasons = [];

    if (!matchesVariant(deal, offer, aiResolution)) {
      reasons.push('variant_mismatch');
    }

    if (!matchesQuantity(deal, offer, aiResolution)) {
      reasons.push('quantity_mismatch');
    }

    if (!isRealisticShop(offer)) {
      reasons.push('unrealistic_shop');
    }

    if (!isRealisticPrice(offer, amazonPrice)) {
      reasons.push('unrealistic_price');
    }

    if (offer.incomplete) {
      reasons.push('incomplete_offer');
    }

    if (offer.totalPrice === null) {
      reasons.push('missing_price');
    }

    if (reasons.length) {
      invalidOffers.push(buildInvalidOffer(offer, reasons));
      return;
    }

    validOffers.push(offer);
  });

  const sortedValidOffers = [...validOffers].sort((left, right) => {
    if (left.totalPrice === null && right.totalPrice === null) {
      return 0;
    }

    if (left.totalPrice === null) {
      return 1;
    }

    if (right.totalPrice === null) {
      return -1;
    }

    return left.totalPrice - right.totalPrice;
  });
  const cheapestOffer = sortedValidOffers[0] || null;
  const marketPrice = cheapestOffer?.totalPrice ?? null;
  const marketAdvantagePct =
    marketPrice !== null && amazonPrice !== null && marketPrice > 0
      ? round(((marketPrice - amazonPrice) / marketPrice) * 100, 2)
      : null;
  const priceBand = sortedValidOffers
    .map((offer) => offer.totalPrice)
    .filter((value) => value !== null)
    .sort((left, right) => left - right);
  const contradictoryPrices =
    priceBand.length >= 2 && priceBand[0] !== null && priceBand[priceBand.length - 1] !== null
      ? priceBand[priceBand.length - 1] - priceBand[0] > priceBand[0] * 0.35
      : false;

  return {
    status: cheapestOffer ? 'market_available' : 'market_missing',
    available: Boolean(cheapestOffer),
    validOfferCount: sortedValidOffers.length,
    invalidOfferCount: invalidOffers.length,
    offersCount: offers.length,
    validOffers: sortedValidOffers,
    invalidOffers,
    cheapestOffer,
    marketPrice,
    marketAdvantagePct,
    contradictoryPrices,
    uncertaintyHints: {
      multipleSimilarHits: offers.length >= 2,
      contradictoryPrices,
      invalidVariantCount: invalidOffers.filter((offer) => offer.invalidReasons.includes('variant_mismatch')).length
    }
  };
}

