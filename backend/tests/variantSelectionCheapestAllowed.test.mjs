import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'affiliate-variant-selection-'));
process.env.APP_DATA_DIR = tempRoot;
process.env.APP_DB_PATH = path.join(tempRoot, 'deals.db');
process.env.READER_TEST_MODE = '1';
process.env.READER_DEBUG_MODE = '0';
process.env.SIMILAR_VARIANT_CHECK_ENABLED = '1';

const { __testablesTelegramUserClient } = await import('../services/telegramUserClientService.js');

async function test(name, fn) {
  try {
    await fn();
    console.log(`PASS ${name}`);
  } catch (error) {
    console.error(`FAIL ${name}`);
    throw error;
  }
}

function buildOriginalProfile() {
  return {
    asin: 'B0BLACK0001',
    title: 'Acme Thermobecher 500ml Schwarz',
    brand: 'Acme',
    price: 19.99,
    quantityInfo: {
      quantityTotal: 500,
      quantityUnit: 'ml',
      quantityLabel: '500 ml'
    },
    productRoleInfo: {
      role: 'MAIN_PRODUCT'
    },
    productTypeTokens: ['acme', 'thermobecher', '500ml', 'schwarz'],
    coreFeatureTokens: ['500ml']
  };
}

await test('Variantenscan waehlt die guenstigste erlaubte Variante nach kompletter Schleife', async () => {
  const originalLimit = process.env.SIMILAR_VARIANT_LIMIT;
  process.env.SIMILAR_VARIANT_LIMIT = '25';
  let requestedLimit = null;

  try {
    const result = await __testablesTelegramUserClient.findCheapestAllowedVariation({
      finalCandidate: {
        asin: 'B0BLACK0001',
        title: 'Acme Thermobecher 500ml Schwarz',
        brand: 'Acme',
        priceValue: 17.84,
        variationAttributes: [{ name: 'Color', value: 'Schwarz' }],
        isAmazonFulfilled: true
      },
      originalProfile: buildOriginalProfile(),
      currentResult: {
        optimizedPriceValue: 17.84,
        optimizedAsin: 'B0BLACK0001'
      },
      variationLoader: async ({ asin, limit }) => {
        requestedLimit = limit;
        assert.equal(asin, 'B0BLACK0001');
        return {
          status: 'success',
          items: [
            {
              asin: 'B0BLACK0001',
              title: 'Acme Thermobecher 500ml Schwarz',
              brand: 'Acme',
              priceValue: 17.84,
              variationAttributes: [{ name: 'Color', value: 'Schwarz' }],
              isAmazonFulfilled: true
            },
            {
              asin: 'B0BROWN0002',
              title: 'Acme Thermobecher 500ml Braun',
              brand: 'Acme',
              priceValue: 16.5,
              variationAttributes: [{ name: 'Color', value: 'Braun' }],
              merchantName: 'Amazon'
            },
            {
              asin: 'B0FBMLOW03',
              title: 'Acme Thermobecher 500ml Rot',
              brand: 'Acme',
              priceValue: 15.99,
              variationAttributes: [{ name: 'Color', value: 'Rot' }],
              sellerClass: 'FBM'
            }
          ]
        };
      }
    });

    assert.equal(requestedLimit, 10);
    assert.equal(result?.variant?.asin, 'B0BROWN0002');
    assert.equal(result?.variantPrice, 16.5);
    assert.match(result?.label || '', /Braun/);
    assert.match(result?.reason || '', /Guenstigste erlaubte Variante/);
  } finally {
    if (originalLimit === undefined) {
      delete process.env.SIMILAR_VARIANT_LIMIT;
    } else {
      process.env.SIMILAR_VARIANT_LIMIT = originalLimit;
    }
  }
});

await test('Variantenscan nutzt Preis-Fallback des Kandidaten und startet trotzdem', async () => {
  let loaderCalls = 0;

  const result = await __testablesTelegramUserClient.findCheapestAllowedVariation({
    finalCandidate: {
      asin: 'B0BLACK0001',
      title: 'Acme Thermobecher 500ml Schwarz',
      brand: 'Acme',
      priceDisplay: '17,84 EUR',
      variationAttributes: [{ name: 'Color', value: 'Schwarz' }],
      isAmazonFulfilled: true
    },
    originalProfile: buildOriginalProfile(),
    currentResult: {
      optimizedAsin: 'B0BLACK0001'
    },
    variationLoader: async () => {
      loaderCalls += 1;
      return {
        status: 'success',
        items: [
          {
            asin: 'B0BROWN0002',
            title: 'Acme Thermobecher 500ml Braun',
            brand: 'Acme',
            priceValue: 16.5,
            variationAttributes: [{ name: 'Color', value: 'Braun' }],
            merchantName: 'Amazon'
          }
        ]
      };
    }
  });

  assert.equal(loaderCalls, 1);
  assert.equal(result?.variant?.asin, 'B0BROWN0002');
  assert.equal(result?.variantPrice, 16.5);
});
