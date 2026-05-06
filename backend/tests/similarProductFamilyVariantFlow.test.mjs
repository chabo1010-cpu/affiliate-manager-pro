import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'affiliate-similar-family-variant-'));
process.env.APP_DATA_DIR = tempRoot;
process.env.APP_DB_PATH = path.join(tempRoot, 'deals.db');
process.env.READER_TEST_MODE = '0';
process.env.READER_DEBUG_MODE = '0';
process.env.SIMILAR_VARIANT_CHECK_ENABLED = '1';
process.env.AMAZON_BROWSER_VARIANT_SCAN_ENABLED = '0';

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

await test('Produktfamilien-Varianten werden vor Similar Search bevorzugt', async () => {
  let searchCalls = 0;

  const result = await __testablesTelegramUserClient.runSimilarProductOptimizationCheck({
    sessionName: 'family-variant-test',
    source: {
      id: 1,
      channelTitle: 'Testgruppe'
    },
    structuredMessage: {
      messageId: '42',
      channelTitle: 'Testgruppe'
    },
    generatorInput: {
      asin: 'B0BLACK4790',
      title: 'Lico Herren Bioline Man Pantoletten Schwarz 47 EU',
      brand: 'Lico',
      currentPrice: 17.9,
      sellerClass: 'AMAZON_DIRECT',
      amazonMerchantName: 'Amazon',
      normalizedUrl: 'https://www.amazon.de/dp/B0BLACK4790'
    },
    scrapedDeal: {
      asin: 'B0BLACK4790',
      title: 'Lico Herren Bioline Man Pantoletten Schwarz 47 EU',
      brand: 'Lico',
      price: 17.9
    },
    creatorVariationLoader: async ({ asin, limit }) => {
      assert.equal(asin, 'B0BLACK4790');
      assert.equal(limit, 10);
      return {
        status: 'success',
        cacheHit: false,
        items: [
          {
            asin: 'B0BLACK4790',
            title: 'Lico Herren Bioline Man Pantoletten Schwarz 47 EU',
            brand: 'Lico',
            priceValue: 17.9,
            merchantName: 'Amazon',
            variationAttributes: [
              { name: 'Color', value: 'Schwarz' },
              { name: 'Size', value: '47 EU' }
            ]
          },
          {
            asin: 'B0BROWN3650',
            title: 'Lico Herren Bioline Man Pantoletten Braun 36 EU',
            brand: 'Lico',
            priceValue: 16.5,
            merchantName: 'Amazon',
            variationAttributes: [
              { name: 'Color', value: 'Braun' },
              { name: 'Size', value: '36 EU' }
            ]
          }
        ]
      };
    },
    paapiVariationLoader: async () => ({
      status: 'no_hits',
      cacheHit: false,
      items: []
    }),
    searchProductsImpl: async () => {
      searchCalls += 1;
      return {
        status: 'success',
        items: []
      };
    },
    enrichCandidatesImpl: async (candidates) => candidates
  });

  assert.equal(searchCalls, 0);
  assert.equal(result.similarCheaperFound, true);
  assert.equal(result.optimizedAsin, 'B0BROWN3650');
  assert.equal(result.optimizedPriceValue, 16.5);
  assert.equal(result.similarSearchQueryType, 'product_family');
  assert.equal(result.variantScanSource, 'creator');
});
