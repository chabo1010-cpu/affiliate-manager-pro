import { cleanText, ensureArray, parseBool, parseNumber } from './shared.js';

export function resolveAiAssistance({ ai = {}, market = {}, settings }) {
  const needed =
    parseBool(ai.variantUnclear, false) ||
    parseBool(ai.multipleSimilarHits, false) ||
    parseBool(ai.conflictingPrices, false) ||
    parseBool(ai.mappingUncertain, false) ||
    market?.contradictoryPrices === true ||
    Number(market?.uncertaintyHints?.invalidVariantCount || 0) > 0;

  if (!needed) {
    return {
      needed: false,
      used: false,
      status: 'not_needed',
      reason: 'Kein Unsicherheitsfall.'
    };
  }

  if (!settings.ai.resolverEnabled) {
    return {
      needed: true,
      used: false,
      status: 'disabled',
      reason: 'AI Resolver ist deaktiviert. System bleibt ohne KI entscheidungsfaehig.',
      resolvedOfferIds: []
    };
  }

  const resolvedOfferIds = ensureArray(ai.resolvedOfferIds)
    .map((item) => cleanText(String(item)))
    .filter(Boolean);
  const confidence = parseNumber(ai.confidence, null);
  const resolvedVariant = cleanText(ai.resolvedVariant || ai.variantKey);

  if (!resolvedOfferIds.length && !resolvedVariant) {
    return {
      needed: true,
      used: false,
      status: 'awaiting_hint',
      reason: 'AI Resolver waere erlaubt, aber es liegt kein Resolver-Hinweis vor.',
      resolvedOfferIds: []
    };
  }

  return {
    needed: true,
    used: true,
    status: 'resolved',
    reason: 'AI Resolver wurde nur fuer Unsicherheit genutzt.',
    resolvedOfferIds,
    resolvedVariant,
    confidence
  };
}

