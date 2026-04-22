import { checkDealLockStatus, normalizeSellerType } from './dealHistoryService.js';
import { logGeneratorDebug } from './generatorFlowService.js';
import { loadKeepaClientByAsin } from './keepaClientService.js';
import {
  getKeepaDrawerControlConfig,
  getKeepaSettings,
  loadStoredInternetComparisonContext
} from './keepaService.js';
import { evaluateLearningRoute } from './learningLogicService.js';

function cleanText(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function parseNumber(value, fallback = null) {
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

  const parsed = Number.parseFloat(trimmed.replace(/[^\d,.-]/g, '').replace(',', '.'));
  return Number.isFinite(parsed) ? parsed : fallback;
}

export async function buildGeneratorDealContext(input = {}) {
  const asin = cleanText(input.asin).toUpperCase();
  const sellerType = normalizeSellerType(input.sellerType);
  const currentPrice = parseNumber(input.currentPrice, null);
  const drawerControl = getKeepaDrawerControlConfig(sellerType);
  const keepaSettings = getKeepaSettings();
  const dealLock = checkDealLockStatus({
    asin,
    url: input.productUrl || input.link,
    normalizedUrl: input.productUrl || input.link,
    sourceType: input.source || 'generator',
    origin: input.origin
  });

  logGeneratorDebug('GENERATOR CONNECTED TO LEARNING LOGIC', {
    asin,
    sellerType,
    source: input.source || 'generator',
    dealLockBlocked: dealLock.blocked,
    dealHash: dealLock.dealHash || null
  });

  let keepaContext = {
    available: false,
    status: 'missing_asin',
    cached: false,
    reason: asin ? 'Keepa noch nicht geladen.' : 'ASIN fehlt.'
  };
  let internetContext = {
    available: false,
    status: 'missing_asin',
    cached: false,
    reason: asin ? 'Kein gespeicherter Internetvergleich gefunden.' : 'ASIN fehlt.'
  };

  if (asin) {
    internetContext = loadStoredInternetComparisonContext({
      asin,
      domainId: input.domainId
    });

    try {
      if (!internetContext.available) {
        logGeneratorDebug('KEEPA FALLBACK USED', {
          asin,
          sellerType,
          reason: internetContext.reason || 'Kein Marktvergleich vorhanden.'
        });

        keepaContext = await loadKeepaClientByAsin({
          asin,
          sellerType,
          currentPrice,
          title: input.title,
          productUrl: input.productUrl || input.link,
          imageUrl: input.imageUrl || input.generatedImagePath,
          source: input.source || 'generator'
        });
      } else {
        logGeneratorDebug('INTERNET COMPARISON PRIMARY', {
          asin,
          sellerType,
          comparisonStatus: internetContext.result?.comparisonStatus || internetContext.status
        });
      }
    } catch (error) {
      keepaContext = {
        available: false,
        status: 'error',
        cached: false,
        reason: error instanceof Error ? error.message : 'Keepa konnte nicht geladen werden.'
      };
    }
  }

  return {
    ...evaluateLearningRoute({
      asin,
      sellerType,
      currentPrice,
      internetContext,
      keepaContext,
      sourceType: 'generator',
      enforceDecision: false,
      keepaRequired: false,
      dealLockStatus: dealLock,
      patternSupportEnabled: drawerControl.patternSupportEnabled === true,
      marketMinGapPct: keepaSettings.strongDealMinComparisonGapPct
    }),
    dealLock
  };
}
