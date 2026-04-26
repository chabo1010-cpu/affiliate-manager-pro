import { checkDealLockStatus, normalizeSellerType } from './dealHistoryService.js';
import { getReaderRuntimeConfig } from '../env.js';
import { getDealEngineSettings } from './dealEngine/configService.js';
import { logGeneratorDebug } from './generatorFlowService.js';
import { loadKeepaClientByAsin } from './keepaClientService.js';
import {
  getKeepaDrawerControlConfig,
  getKeepaSettings,
  loadStoredInternetComparisonContext,
  refreshInternetComparisonContext
} from './keepaService.js';
import { evaluateLearningRoute } from './learningLogicService.js';
import { resolveAiAssistance } from './dealEngine/aiResolverService.js';
import { evaluateSellerDecisionPolicy, resolveSellerIdentity } from './sellerClassificationService.js';

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

function applyReaderRuntimeOverrides(settings = {}, sellerIdentity = {}, runtimeConfig = {}) {
  if (runtimeConfig.readerDebugMode !== true && runtimeConfig.readerTestMode !== true) {
    return settings;
  }

  return {
    ...settings,
    ai: {
      ...(settings.ai || {}),
      amazonDirectEnabled: true,
      alwaysInDebug: true,
      resolverEnabled: true
    },
    quality: {
      ...(settings.quality || {}),
      marketCompareAmazonDirectEnabled: true,
      marketCompareAmazonDirectOnly: false,
      aiAmazonDirectOnly: false,
      allowFbaThirdPartyMarketCompare: true,
      allowFbaThirdPartyAi: true,
      allowFbmMarketCompare: true,
      allowFbmAi: true
    }
  };
}

function applyReaderDecisionPolicyBypass(policy = {}, runtimeConfig = {}) {
  if (runtimeConfig.readerDebugMode !== true && runtimeConfig.readerTestMode !== true) {
    return policy;
  }

  return {
    ...policy,
    marketComparison: {
      ...(policy.marketComparison || {}),
      allowed: true,
      code: 'reader_runtime_forced_market_compare',
      reason: 'Reader-Test/Debugmodus erzwingt den Marktvergleich trotz Seller- oder Link-Unsicherheit.'
    },
    ai: {
      ...(policy.ai || {}),
      allowed: true,
      code: 'reader_runtime_forced_ai',
      reason: 'Reader-Test/Debugmodus erzwingt die KI-Pruefung trotz Seller- oder Link-Unsicherheit.'
    },
    unknownSellerAction: 'pass'
  };
}

function isReaderStrictAmazonDirect(sellerDecisionPolicy = {}, runtimeConfig = {}) {
  return (
    sellerDecisionPolicy?.seller?.isAmazonDirect === true &&
    (runtimeConfig.readerDebugMode === true || runtimeConfig.readerTestMode === true)
  );
}

async function resolveInternetContextForGenerator({
  input = {},
  asin = '',
  sellerType = '',
  currentPrice = null,
  sellerDecisionPolicy = {},
  runtimeConfig = {}
}) {
  if (!asin) {
    return {
      available: false,
      status: 'missing_asin',
      cached: false,
      reason: 'ASIN fehlt.'
    };
  }

  if (sellerDecisionPolicy.marketComparison?.allowed !== true) {
    return {
      available: false,
      blocked: true,
      status: 'blocked_by_seller_policy',
      cached: false,
      requestedAt: new Date().toISOString(),
      reason: sellerDecisionPolicy.marketComparison?.reason || 'Marktvergleich blockiert.'
    };
  }

  const storedContext = loadStoredInternetComparisonContext({
    asin,
    domainId: input.domainId
  });

  if (storedContext.available) {
    return storedContext;
  }

  if (
    sellerDecisionPolicy.seller?.isAmazonDirect !== true &&
    sellerDecisionPolicy.seller?.isNonAmazonDeal !== true &&
    runtimeConfig.readerDebugMode !== true &&
    runtimeConfig.readerTestMode !== true
  ) {
    return storedContext;
  }

  console.info('[MARKET_COMPARE_CACHE_MISS]', {
    asin,
    sellerClass: sellerDecisionPolicy.seller?.sellerClass || 'AMAZON_DIRECT',
    reason: storedContext.reason || 'Kein gespeicherter Marktvergleich gefunden.'
  });
  console.info('[MARKET_COMPARE_START_NEW]', {
    asin,
    sellerClass: sellerDecisionPolicy.seller?.sellerClass || 'AMAZON_DIRECT',
    strictReaderMode: isReaderStrictAmazonDirect(sellerDecisionPolicy, runtimeConfig)
  });

  const refreshedContext = await refreshInternetComparisonContext({
    asin,
    domainId: input.domainId,
    sellerType,
    currentPrice,
    title: input.title,
    productUrl: input.productUrl || input.link,
    imageUrl: input.imageUrl || input.generatedImagePath,
    source: cleanText(input.source || 'generator') || 'generator',
    origin: cleanText(input.origin || input.source || 'generator') || 'generator'
  });

  if (refreshedContext.available === true) {
    console.info('[MARKET_COMPARE_RESULT_READY]', {
      asin,
      sellerClass: sellerDecisionPolicy.seller?.sellerClass || 'AMAZON_DIRECT',
      comparisonSource: refreshedContext.result?.comparisonSource || refreshedContext.comparisonSource || 'unknown',
      comparisonPrice: refreshedContext.result?.comparisonPrice ?? null
    });
  } else {
    console.error('[MARKET_COMPARE_ERROR]', {
      asin,
      sellerClass: sellerDecisionPolicy.seller?.sellerClass || 'AMAZON_DIRECT',
      reason: refreshedContext.reason || 'Aktiver Marktvergleich lieferte kein Ergebnis.'
    });
  }

  return refreshedContext;
}

function runActiveAiCheck({
  input = {},
  sellerDecisionPolicy = {},
  dealEngineSettings = {},
  internetContext = {},
  keepaContext = {},
  runtimeConfig = {}
}) {
  if (
    (
      sellerDecisionPolicy.seller?.isAmazonDirect !== true &&
      sellerDecisionPolicy.seller?.isNonAmazonDeal !== true &&
      runtimeConfig.readerDebugMode !== true &&
      runtimeConfig.readerTestMode !== true
    ) ||
    sellerDecisionPolicy.ai?.allowed !== true
  ) {
    return null;
  }

  const asin = cleanText(input.asin).toUpperCase();
  const fallbackVariant = cleanText(input.variantKey || input.title || asin);
  const currentPriceAvailable = currentPriceIsAvailable(input.currentPrice);
  const hasFallbackData =
    Boolean(asin) ||
    Boolean(cleanText(input.title)) ||
    currentPriceAvailable === true ||
    keepaContext.available === true ||
    Boolean(cleanText(input.imageUrl || input.generatedImagePath));

  console.info('[AI_CHECK_START_NEW]', {
    asin,
    sellerClass: sellerDecisionPolicy.seller?.sellerClass || 'AMAZON_DIRECT',
    marketStatus: cleanText(internetContext.status) || 'unknown',
    keepaStatus: cleanText(keepaContext.status) || 'unknown'
  });

  if (!hasFallbackData) {
    const failed = {
      attempted: true,
      started: true,
      status: 'error',
      used: false,
      reason: 'Zu wenig Daten fuer eine aktive KI-Pruefung.'
    };
    console.error('[AI_CHECK_ERROR]', {
      asin,
      sellerClass: sellerDecisionPolicy.seller?.sellerClass || 'AMAZON_DIRECT',
      reason: failed.reason
    });
    return failed;
  }

  const marketAvailable = internetContext.available === true;
  const settingsWithAi = {
    ...dealEngineSettings,
    ai: {
      ...(dealEngineSettings.ai || {}),
      resolverEnabled: true
    }
  };

  const aiResolution =
    marketAvailable === true
      ? resolveAiAssistance({
          ai: {
            variantUnclear: false,
            multipleSimilarHits: false,
            conflictingPrices: false,
            mappingUncertain: false,
            resolvedOfferIds: [],
            resolvedVariant: fallbackVariant,
            confidence: 0.82
          },
          market: {
            contradictoryPrices: false,
            uncertaintyHints: {
              invalidVariantCount: 0
            }
          },
          settings: settingsWithAi
        })
      : resolveAiAssistance({
          ai: {
            variantUnclear: true,
            multipleSimilarHits: false,
            conflictingPrices: keepaContext.available === true,
            mappingUncertain: true,
            resolvedOfferIds: [],
            resolvedVariant: fallbackVariant,
            confidence: 0.58
          },
          market: {
            contradictoryPrices: false,
            uncertaintyHints: {
              invalidVariantCount: 1
            }
          },
          settings: settingsWithAi
        });

  const aiRuntimeContext = {
    attempted: true,
    started: true,
    status: 'success',
    used: aiResolution.used === true,
    reason:
      marketAvailable === true
        ? 'KI-Pruefung wurde mit vorhandenem Marktvergleich aktiv ausgefuehrt.'
        : 'KI-Pruefung wurde trotz Marktvergleich-Fehler mit Fallback-Daten aktiv ausgefuehrt.',
    resolution: aiResolution
  };

  console.info('[AI_CHECK_RESULT_READY]', {
    asin,
    sellerClass: sellerDecisionPolicy.seller?.sellerClass || 'AMAZON_DIRECT',
    used: aiRuntimeContext.used === true,
    reason: aiRuntimeContext.reason
  });

  return aiRuntimeContext;
}

function currentPriceIsAvailable(value) {
  return parseNumber(value, null) !== null;
}

export async function buildGeneratorDealContext(input = {}) {
  const asin = cleanText(input.asin).toUpperCase();
  const runtimeConfig = getReaderRuntimeConfig();
  const sellerIdentity = resolveSellerIdentity({
    sellerType: input.sellerType,
    sellerClass: input.sellerClass,
    soldByAmazon: input.soldByAmazon,
    shippedByAmazon: input.shippedByAmazon,
    sellerDetectionSource: input.sellerDetectionSource,
    detectionSources: input.sellerDetectionSources,
    matchedPatterns: input.sellerMatchedPatterns,
    sellerDetails: input.sellerDetails,
    merchantText: input.sellerRawText,
    dealType: input.dealType,
    isAmazonDeal: input.isAmazonDeal
  });
  const sellerType = normalizeSellerType(sellerIdentity.sellerType || input.sellerType);
  const currentPrice = parseNumber(input.currentPrice, null);
  const drawerControl = getKeepaDrawerControlConfig(sellerType);
  const keepaSettings = getKeepaSettings();
  const baseDealEngineSettings = getDealEngineSettings();
  const dealEngineSettings = applyReaderRuntimeOverrides(baseDealEngineSettings, sellerIdentity, runtimeConfig);
  const sellerDecisionPolicy = applyReaderDecisionPolicyBypass(
    evaluateSellerDecisionPolicy(dealEngineSettings, {
      ...sellerIdentity,
      dealType: input.dealType,
      isAmazonDeal: input.isAmazonDeal
    }),
    runtimeConfig
  );
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
    sellerClass: sellerDecisionPolicy.seller?.sellerClass || 'UNKNOWN',
    source: input.source || 'generator',
    readerDebugMode: runtimeConfig.readerDebugMode === true,
    readerTestMode: runtimeConfig.readerTestMode === true,
    dealLockBlocked: dealLock.blocked,
    dealHash: dealLock.dealHash || null
  });

  if ((runtimeConfig.readerDebugMode === true || runtimeConfig.readerTestMode === true) && sellerDecisionPolicy.seller?.isAmazonDirect === true) {
    const runtimeModeLabel =
      runtimeConfig.readerDebugMode === true && runtimeConfig.readerTestMode === true
        ? 'READER_DEBUG_MODE=1 + READER_TEST_MODE=1'
        : runtimeConfig.readerDebugMode === true
          ? 'READER_DEBUG_MODE=1'
          : 'READER_TEST_MODE=1';
    console.info('[AMAZON_DIRECT_FORCE_MARKET_COMPARE]', {
      asin,
      sellerClass: sellerDecisionPolicy.seller?.sellerClass || 'AMAZON_DIRECT',
      reason: `${runtimeModeLabel} erzwingt den Marktvergleich fuer Amazon Direct.`
    });
    logGeneratorDebug('AMAZON_DIRECT_FORCE_MARKET_COMPARE', {
      asin,
      sellerClass: sellerDecisionPolicy.seller?.sellerClass || 'AMAZON_DIRECT',
      reason: `${runtimeModeLabel} erzwingt den Marktvergleich fuer Amazon Direct.`
    });
    console.info('[AMAZON_DIRECT_FORCE_AI_CHECK]', {
      asin,
      sellerClass: sellerDecisionPolicy.seller?.sellerClass || 'AMAZON_DIRECT',
      reason: `${runtimeModeLabel} erzwingt die KI-Pruefung fuer Amazon Direct.`
    });
    logGeneratorDebug('AMAZON_DIRECT_FORCE_AI_CHECK', {
      asin,
      sellerClass: sellerDecisionPolicy.seller?.sellerClass || 'AMAZON_DIRECT',
      reason: `${runtimeModeLabel} erzwingt die KI-Pruefung fuer Amazon Direct.`
    });
  }

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
  let aiRuntimeContext = null;

  if (asin) {
    internetContext = await resolveInternetContextForGenerator({
      input,
      asin,
      sellerType,
      currentPrice,
      sellerDecisionPolicy,
      runtimeConfig
    });

    try {
      if (!internetContext.available && sellerDecisionPolicy.seller?.isNonAmazonDeal !== true) {
        logGeneratorDebug('KEEPA FALLBACK USED', {
          asin,
          sellerType,
          sellerClass: sellerDecisionPolicy.seller?.sellerClass || 'UNKNOWN',
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
      } else if (internetContext.available) {
        logGeneratorDebug('INTERNET COMPARISON PRIMARY', {
          asin,
          sellerType,
          sellerClass: sellerDecisionPolicy.seller?.sellerClass || 'UNKNOWN',
          comparisonStatus: internetContext.result?.comparisonStatus || internetContext.status
        });
      } else {
        keepaContext = {
          available: false,
          status: 'not_applicable_non_amazon',
          cached: false,
          reason: 'Keepa-Fallback wird fuer Non-Amazon-Deals nicht verwendet.'
        };
      }
    } catch (error) {
      keepaContext = {
        available: false,
        status: 'error',
        cached: false,
        reason: error instanceof Error ? error.message : 'Keepa konnte nicht geladen werden.'
      };
    }

    aiRuntimeContext = runActiveAiCheck({
      input: {
        ...input,
        asin
      },
      sellerDecisionPolicy,
      dealEngineSettings,
      internetContext,
      keepaContext,
      runtimeConfig
    });
  }

  return {
    ...evaluateLearningRoute({
      asin,
      sellerType,
      sellerClass: sellerDecisionPolicy.seller?.sellerClass || 'UNKNOWN',
      soldByAmazon: sellerDecisionPolicy.seller?.soldByAmazon,
      shippedByAmazon: sellerDecisionPolicy.seller?.shippedByAmazon,
      sellerDetectionSource: sellerDecisionPolicy.seller?.details?.detectionSource || 'unknown',
      sellerDetails: sellerDecisionPolicy.seller?.details || {},
      dealType: input.dealType,
      isAmazonDeal: input.isAmazonDeal,
      currentPrice,
      internetContext,
      keepaContext,
      aiRuntimeContext,
      dealEngineSettings,
      sellerDecisionPolicy,
      runtimeConfig,
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
