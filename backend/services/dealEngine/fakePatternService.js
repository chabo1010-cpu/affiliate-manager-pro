import { evaluateKeepaFakeDropHeuristics } from '../keepaFakeDropService.js';
import { cleanText, parseBool, parseNumber, round } from './shared.js';

function buildReasonDetail(code, message, severity = 'info', source = 'deal_engine', meta = null) {
  return {
    code,
    message,
    severity,
    source,
    ...(meta && typeof meta === 'object' ? { meta } : {})
  };
}

function hasSignal(source = {}, keys = []) {
  return keys.some((key) => parseBool(source?.[key], false));
}

function toKeepaHeuristicInput({ deal = {}, keepa = {} }) {
  const amazonPrice = parseNumber(deal.amazonPrice, null);
  const avg90 = parseNumber(keepa.avg90 ?? keepa.average90, null);
  const currentPrice = amazonPrice;
  const keepaDiscount =
    currentPrice !== null && avg90 !== null && avg90 > 0
      ? round(((avg90 - currentPrice) / avg90) * 100, 2)
      : 0;

  return {
    asin: cleanText(deal.asin),
    title: cleanText(deal.title),
    sellerType: cleanText(deal.sellerArea || deal.sellerType || 'FBM'),
    categoryName: cleanText(deal.category),
    currentPrice,
    referencePrice: parseNumber(deal.referencePrice, null),
    keepaDiscount,
    keepaPayload:
      keepa.payload && typeof keepa.payload === 'object'
        ? keepa.payload
        : keepa.keepaPayload && typeof keepa.keepaPayload === 'object'
          ? keepa.keepaPayload
          : {}
  };
}

function buildManualFlagSignals({ deal = {}, keepa = {}, market = {} }) {
  const flags = [];
  const reasonDetails = [];

  if (hasSignal(keepa, ['highPriceNoHistory']) || hasSignal(deal, ['highPriceNoHistory'])) {
    flags.push('high_price_without_history');
    reasonDetails.push(
      buildReasonDetail(
        'high_price_without_history',
        'Hoher Preis ohne verlaessliche Historie erkannt.',
        'high',
        'direct_signal'
      )
    );
  }

  if (hasSignal(keepa, ['suddenDropWithoutHistory']) || hasSignal(deal, ['suddenDropWithoutHistory'])) {
    flags.push('sudden_drop_without_history');
    reasonDetails.push(
      buildReasonDetail(
        'sudden_drop_without_history',
        'Ploetzlicher Preisdrop ohne passenden Verlauf erkannt.',
        'critical',
        'direct_signal'
      )
    );
  }

  if (hasSignal(keepa, ['fakeAnchorPrice', 'anchorPriceSuspect']) || hasSignal(deal, ['fakeAnchorPrice', 'anchorPriceSuspect'])) {
    flags.push('fake_anchor_price');
    reasonDetails.push(
      buildReasonDetail('fake_anchor_price', 'Typischer Fake-Ankerpreis erkannt.', 'critical', 'direct_signal')
    );
  }

  if (hasSignal(keepa, ['illogicalChart']) || hasSignal(deal, ['illogicalChart'])) {
    flags.push('illogical_chart');
    reasonDetails.push(
      buildReasonDetail('illogical_chart', 'Unlogischer Chart-Verlauf erkannt.', 'critical', 'direct_signal')
    );
  }

  const amazonPrice = parseNumber(deal.amazonPrice, null);
  const avg90 = parseNumber(keepa.avg90, null);
  const referencePrice = parseNumber(deal.referencePrice, null);
  if (amazonPrice !== null && avg90 === null && referencePrice !== null && referencePrice >= amazonPrice * 2.2) {
    flags.push('reference_price_without_history');
    reasonDetails.push(
      buildReasonDetail(
        'reference_price_without_history',
        'Auffaellig hoher Referenzpreis ohne tragfaehige Historie.',
        'high',
        'derived_signal'
      )
    );
  }

  if (!market.available && Number(market.invalidOfferCount || 0) >= 2) {
    flags.push('market_noise');
    reasonDetails.push(
      buildReasonDetail(
        'market_noise',
        'Mehrere unbrauchbare Marktangebote deuten auf irrefuehrende Preisanker hin.',
        'medium',
        'market_context'
      )
    );
  }

  return {
    flags,
    reasonDetails
  };
}

function mapHeuristicFlag(flag = {}) {
  const code = cleanText(flag.id) || 'fake_pattern_flag';
  const message = cleanText(flag.label) || 'Verdaechtiges Fake-Pattern erkannt.';
  const severity = ['critical', 'high', 'medium', 'low'].includes(cleanText(flag.severity))
    ? cleanText(flag.severity)
    : 'medium';

  return buildReasonDetail(code, message, severity, 'keepa_fake_drop');
}

function derivePenalty({ hardReject, reviewRequired, manualFlags = [], heuristic = {} }) {
  if (hardReject) {
    return 50;
  }

  if (reviewRequired) {
    if ((heuristic.fakeDropRisk ?? 0) >= 70 || manualFlags.includes('high_price_without_history')) {
      return 40;
    }

    return 30;
  }

  return 0;
}

export function evaluateFakePatterns({ deal = {}, keepa = {}, market = {} }) {
  const manualSignals = buildManualFlagSignals({ deal, keepa, market });
  const heuristic = evaluateKeepaFakeDropHeuristics(toKeepaHeuristicInput({ deal, keepa }), {
    skipLog: true
  });
  const heuristicReasonDetails = (Array.isArray(heuristic.flags) ? heuristic.flags : []).map(mapHeuristicFlag);
  const reasonDetails = [...manualSignals.reasonDetails, ...heuristicReasonDetails];
  const flags = [...manualSignals.flags, ...heuristicReasonDetails.map((item) => item.code)];
  const substantialHeuristicSignal =
    heuristic.classification === 'wahrscheinlicher_fake_drop' ||
    heuristic.classification === 'verdaechtig' ||
    (Array.isArray(heuristic.flags) && heuristic.flags.some((flag) => cleanText(flag.id) !== 'history_sparse')) ||
    Number(heuristic.fakeDropRisk || 0) >= 45;
  const hardReject =
    manualSignals.flags.some((flag) =>
      ['sudden_drop_without_history', 'fake_anchor_price', 'illogical_chart'].includes(flag)
    ) || heuristic.classification === 'wahrscheinlicher_fake_drop';
  const reviewRequired =
    !hardReject &&
    (substantialHeuristicSignal ||
      reasonDetails.some((item) => ['high', 'medium'].includes(item.severity)));
  const penalty = derivePenalty({
    hardReject,
    reviewRequired,
    manualFlags: manualSignals.flags,
    heuristic
  });

  if (heuristic.classification === 'wahrscheinlicher_fake_drop') {
    reasonDetails.unshift(
      buildReasonDetail(
        'fake_drop_classification',
        `Heuristik stuft den Verlauf als ${heuristic.classificationLabel || 'wahrscheinlichen Fake-Drop'} ein.`,
        'critical',
        'keepa_fake_drop',
        {
          fakeDropRisk: heuristic.fakeDropRisk,
          reviewPriority: heuristic.reviewPriority
        }
      )
    );
  } else if (heuristic.classification === 'verdaechtig' || heuristic.reviewRecommended) {
    reasonDetails.unshift(
      buildReasonDetail(
        'fake_drop_review',
        heuristic.analysisReason || 'Heuristik meldet einen unsicheren oder verdaechtigen Verlauf.',
        'medium',
        'keepa_fake_drop',
        {
          classification: heuristic.classification,
          fakeDropRisk: heuristic.fakeDropRisk,
          reviewPriority: heuristic.reviewPriority
        }
      )
    );
  }

  const uniqueFlags = [...new Set(flags.filter(Boolean))];
  const reasons = [...new Set(reasonDetails.map((item) => item.message).filter(Boolean))];

  return {
    status: hardReject ? 'reject' : reviewRequired ? 'review' : 'clear',
    reject: hardReject,
    reviewRequired,
    forceQueue: reviewRequired,
    strongReject: hardReject,
    penalty,
    flags: uniqueFlags,
    reasons,
    reasonDetails,
    summary: cleanText(reasons.join(' | ')) || 'Keine Fake-Pattern erkannt.',
    engine: {
      available: heuristic.available === true,
      engineVersion: heuristic.engineVersion || '',
      classification: heuristic.classification || 'manuelle_pruefung',
      classificationLabel: heuristic.classificationLabel || 'Manuelle Pruefung',
      fakeDropRisk: parseNumber(heuristic.fakeDropRisk, null),
      reviewPriority: parseNumber(heuristic.reviewPriority, null),
      analysisReason: heuristic.analysisReason || '',
      features: heuristic.features || {},
      similarCaseSummary: heuristic.similarCaseSummary || null
    }
  };
}
