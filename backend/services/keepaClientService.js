import { extractKeepaFallbackMetrics } from './keepaFakeDropService.js';
import { logGeneratorDebug } from './generatorFlowService.js';
import { loadKeepaProductContext } from './keepaService.js';

function cleanText(value) {
  return typeof value === 'string' ? value.trim() : '';
}

export async function loadKeepaClientByAsin(input = {}) {
  const asin = cleanText(input.asin).toUpperCase();
  const source = cleanText(input.source || 'analysis_fallback') || 'analysis_fallback';
  const context = await loadKeepaProductContext({
    ...input,
    asin,
    source
  });
  const metrics = extractKeepaFallbackMetrics(context?.result || {
    asin,
    currentPrice: input.currentPrice ?? null
  });
  const keepaClient = {
    asin,
    fallbackOnly: true,
    primaryDecision: false,
    status: context?.status || 'missing',
    requestedAt: context?.requestedAt || new Date().toISOString(),
    avg90: metrics.avg90,
    avg180: metrics.avg180,
    min90: metrics.min90,
    isLowest90: metrics.isLowest90,
    historyPointCount: metrics.historyPointCount,
    historySource: metrics.historySource
  };

  logGeneratorDebug('KEEPA CLIENT FALLBACK READY', {
    asin,
    source,
    status: keepaClient.status,
    avg90: keepaClient.avg90,
    avg180: keepaClient.avg180,
    min90: keepaClient.min90,
    isLowest90: keepaClient.isLowest90
  });

  return {
    ...context,
    client: keepaClient
  };
}
