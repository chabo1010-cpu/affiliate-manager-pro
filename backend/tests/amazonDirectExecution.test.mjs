import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'affiliate-amazon-direct-'));
process.env.APP_DATA_DIR = tempRoot;
process.env.APP_DB_PATH = path.join(tempRoot, 'deals.db');

const { evaluateLearningRoute } = await import('../services/learningLogicService.js');

const BASE_SETTINGS = {
  ai: {
    resolverEnabled: false,
    amazonDirectEnabled: true,
    onlyOnUncertainty: true,
    alwaysInDebug: true
  },
  quality: {
    marketCompareAmazonDirectEnabled: true,
    marketCompareAmazonDirectOnly: true,
    aiAmazonDirectOnly: true,
    allowFbaThirdPartyMarketCompare: false,
    allowFbaThirdPartyAi: false,
    allowFbmMarketCompare: false,
    allowFbmAi: false,
    unknownSellerMode: 'review'
  }
};

function buildSettings(overrides = {}) {
  return {
    ai: {
      ...BASE_SETTINGS.ai,
      ...(overrides.ai || {})
    },
    quality: {
      ...BASE_SETTINGS.quality,
      ...(overrides.quality || {})
    }
  };
}

function buildAmazonDirectInput(overrides = {}) {
  return {
    asin: 'B0AMAZON01',
    sellerType: 'AMAZON',
    sellerClass: 'AMAZON_DIRECT',
    soldByAmazon: true,
    shippedByAmazon: true,
    sellerDetectionSource: 'combined-seller-shipping-text',
    currentPrice: 99.99,
    sourceType: 'generator',
    keepaRequired: false,
    enforceDecision: false,
    marketMinGapPct: 10,
    dealEngineSettings: buildSettings(),
    runtimeConfig: {
      readerTestMode: false,
      readerDebugMode: false
    },
    ...overrides
  };
}

function test(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (error) {
    console.error(`FAIL ${name}`);
    throw error;
  }
}

test('Amazon Direct im Testmodus startet Marktvergleich und liefert klaren KI-Skip-Grund ohne Marktresultat', () => {
  const result = evaluateLearningRoute(
    buildAmazonDirectInput({
      runtimeConfig: {
        readerTestMode: true,
        readerDebugMode: false
      },
      internetPreview: {
        available: false,
        marketAvailable: false,
        status: 'missing',
        reason: 'Kein gespeicherter Internetvergleich gefunden.'
      },
      dealEngineSettings: buildSettings({
        ai: {
          resolverEnabled: true
        }
      })
    })
  );

  assert.equal(result.learning.marketComparisonRequired, true);
  assert.equal(result.learning.marketComparisonStarted, true);
  assert.equal(result.learning.marketComparisonUsed, false);
  assert.equal(result.learning.marketComparisonStatus, 'skipped');
  assert.equal(result.learning.marketComparisonReason, 'Kein gespeicherter Internetvergleich gefunden.');

  assert.equal(result.learning.aiRequired, true);
  assert.equal(result.learning.aiCheckStarted, false);
  assert.equal(result.learning.aiResolutionUsed, false);
  assert.equal(result.learning.aiCheckStatus, 'skipped');
  assert.equal(result.learning.aiCheckReason, 'Kein gespeicherter Internetvergleich gefunden.');
  assert.equal(result.learning.amazonDirectExecutionWarning, 'Amazon Direct erkannt, aber Pruefung nicht ausgefuehrt.');
});

test('Amazon Direct im Debugmodus fuehrt Marktvergleich und KI-Check mit vorhandenem Vergleich aus', () => {
  const result = evaluateLearningRoute(
    buildAmazonDirectInput({
      runtimeConfig: {
        readerTestMode: false,
        readerDebugMode: true
      },
      internetPreview: {
        available: true,
        marketAvailable: true,
        status: 'available',
        comparisonSource: 'stored-market',
        comparisonStatus: 'available',
        comparisonPrice: 129.99,
        priceDifferencePct: 22,
        comparisonCheckedAt: new Date().toISOString(),
        reason: ''
      },
      dealEngineSettings: buildSettings({
        ai: {
          resolverEnabled: true
        }
      })
    })
  );

  assert.equal(result.learning.marketComparisonRequired, true);
  assert.equal(result.learning.marketComparisonStarted, true);
  assert.equal(result.learning.marketComparisonUsed, true);
  assert.equal(result.learning.marketComparisonStatus, 'success');

  assert.equal(result.learning.aiRequired, true);
  assert.equal(result.learning.aiCheckStarted, true);
  assert.equal(result.learning.aiResolutionUsed, true);
  assert.equal(result.learning.aiCheckStatus, 'success');
  assert.equal(result.learning.aiCheckReason, 'Debugmodus hat die KI-Pruefung ausgefuehrt; kein Unsicherheitsfall erkannt.');
  assert.equal(result.learning.amazonDirectExecutionWarning, '');
});

test('Amazon Direct im Debugmodus meldet deaktivierten AI Resolver klar statt still zu skippen', () => {
  const result = evaluateLearningRoute(
    buildAmazonDirectInput({
      runtimeConfig: {
        readerTestMode: false,
        readerDebugMode: true
      },
      internetPreview: {
        available: true,
        marketAvailable: true,
        status: 'available',
        comparisonSource: 'stored-market',
        comparisonStatus: 'available',
        comparisonPrice: 129.99,
        priceDifferencePct: 22,
        comparisonCheckedAt: new Date().toISOString(),
        reason: ''
      },
      dealEngineSettings: buildSettings({
        ai: {
          resolverEnabled: false
        }
      })
    })
  );

  assert.equal(result.learning.marketComparisonRequired, true);
  assert.equal(result.learning.marketComparisonStarted, true);
  assert.equal(result.learning.marketComparisonUsed, true);

  assert.equal(result.learning.aiRequired, true);
  assert.equal(result.learning.aiCheckStarted, false);
  assert.equal(result.learning.aiResolutionUsed, false);
  assert.equal(result.learning.aiCheckStatus, 'skipped');
  assert.equal(
    result.learning.aiCheckReason,
    'Debugmodus hat die KI-Pruefung angefordert, aber der AI Resolver ist deaktiviert.'
  );
  assert.equal(result.learning.amazonDirectExecutionWarning, 'Amazon Direct erkannt, aber Pruefung nicht ausgefuehrt.');
});

console.log('OK Amazon-Direct-Execution-Tests bestanden');
