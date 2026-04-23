import { clamp, parseBool, parseNumber, round } from './shared.js';

function discountAgainstAverage(average, currentPrice) {
  if (average === null || currentPrice === null || average <= 0) {
    return null;
  }

  return round(((average - currentPrice) / average) * 100, 2);
}

export function evaluateKeepaFallback({ deal = {}, keepa = {}, sellerArea = 'FBM', fakePatterns = {}, settings }) {
  const currentPrice = parseNumber(deal.amazonPrice, null);
  const avg90 = parseNumber(keepa.avg90 ?? keepa.average90, null);
  const avg180 = parseNumber(keepa.avg180 ?? keepa.average180, null);
  const min90 = parseNumber(keepa.min90 ?? keepa.lowest90, null);
  const discount90 = discountAgainstAverage(avg90, currentPrice);
  const discount180 = discountAgainstAverage(avg180, currentPrice);
  const isLowest90 = parseBool(keepa.isLowest90, false) || (min90 !== null && currentPrice !== null && currentPrice <= min90);
  const nearLow = parseBool(keepa.nearLow, false) || (min90 !== null && currentPrice !== null && currentPrice <= min90 * 1.05);
  const available = avg90 !== null || avg180 !== null || min90 !== null || isLowest90;
  const scoreParts = [];
  let score = 0;

  if (!available) {
    return {
      available: false,
      score: 0,
      decision: 'REJECT',
      discount90: null,
      discount180: null,
      reasons: ['Kein brauchbarer Keepa-Fallback vorhanden.'],
      scoreParts: []
    };
  }

  if (isLowest90) {
    score += 40;
    scoreParts.push({ label: 'isLowest90', value: 40 });
  }

  if (discount90 !== null && discount90 >= 20) {
    score += 25;
    scoreParts.push({ label: 'avg90', value: 25 });
  }

  if (discount180 !== null && discount180 >= 25) {
    score += 20;
    scoreParts.push({ label: 'avg180', value: 20 });
  }

  if (nearLow) {
    score += 10;
    scoreParts.push({ label: 'near_low', value: 10 });
  }

  if (fakePatterns.penalty) {
    score -= fakePatterns.penalty;
    scoreParts.push({ label: 'fake_pattern_penalty', value: -fakePatterns.penalty });
  }

  if (sellerArea === 'AMAZON') {
    score += 10;
    scoreParts.push({ label: 'amazon_bonus', value: 10 });
  } else {
    const fbmPenalty = discount90 !== null && discount90 < 20 ? 20 : 15;
    score -= fbmPenalty;
    scoreParts.push({ label: 'fbm_risk', value: -fbmPenalty });
  }

  const finalScore = clamp(round(score, 2), 0, 100);
  let decision = 'REJECT';

  if (finalScore >= settings.global.keepaApproveScore) {
    decision = 'APPROVE';
  } else if (finalScore >= settings.global.keepaQueueScore) {
    decision = 'QUEUE';
  }

  const reasons = [
    discount90 !== null ? `Rabatt vs avg90 ${discount90}%` : 'avg90 nicht verfuegbar',
    discount180 !== null ? `Rabatt vs avg180 ${discount180}%` : 'avg180 nicht verfuegbar',
    sellerArea === 'AMAZON' ? 'Amazon Bonus aktiv.' : 'FBM Risikoabzug aktiv.'
  ];

  if (fakePatterns.reject) {
    reasons.push('Fake-Pattern Malus wurde eingerechnet.');
  }

  return {
    available,
    score: finalScore,
    decision,
    discount90,
    discount180,
    min90,
    isLowest90,
    nearLow,
    reasons,
    scoreParts
  };
}

