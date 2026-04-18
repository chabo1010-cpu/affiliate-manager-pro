import { normalizeSellerType } from './dealHistoryService.js';
import { logGeneratorDebug } from './generatorFlowService.js';
import { getKeepaDrawerControlConfig, loadKeepaProductContext } from './keepaService.js';
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

  logGeneratorDebug('GENERATOR CONNECTED TO LEARNING LOGIC', {
    asin,
    sellerType,
    source: input.source || 'generator'
  });

  let keepaContext = {
    available: false,
    status: 'missing_asin',
    cached: false,
    reason: asin ? 'Keepa noch nicht geladen.' : 'ASIN fehlt.'
  };

  if (asin) {
    try {
      keepaContext = await loadKeepaProductContext({
        asin,
        sellerType,
        currentPrice,
        title: input.title,
        productUrl: input.productUrl || input.link,
        imageUrl: input.imageUrl || input.generatedImagePath,
        source: input.source || 'generator'
      });
    } catch (error) {
      keepaContext = {
        available: false,
        status: 'error',
        cached: false,
        reason: error instanceof Error ? error.message : 'Keepa konnte nicht geladen werden.'
      };
    }
  }

  return evaluateLearningRoute({
    asin,
    sellerType,
    currentPrice,
    keepaContext,
    sourceType: 'generator',
    enforceDecision: false,
    keepaRequired: false,
    patternSupportEnabled: drawerControl.patternSupportEnabled === true
  });
}
