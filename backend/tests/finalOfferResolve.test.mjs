import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'affiliate-final-offer-resolve-'));
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

function buildOriginalProfile(overrides = {}) {
  return {
    asin: 'B0ORIGINAL1',
    title: 'Lico Herren Bioline Man Pantoletten Schwarz 47 EU',
    brand: 'Lico',
    price: 24.9,
    quantityInfo: null,
    productRoleInfo: {
      role: 'MAIN_PRODUCT'
    },
    productTypeTokens: ['lico', 'herren', 'bioline', 'pantoletten'],
    coreFeatureTokens: ['pantoletten'],
    ...overrides
  };
}

await test('Finaler Offer-Resolver waehlt vor dem Posten die guenstigste erlaubte Variante', async () => {
  let paapiLoaderCalls = 0;

  const result = await __testablesTelegramUserClient.resolveCheapestFinalOffer(
    {
      similarCheaperFound: true,
      originalTitle: 'Lico Herren Bioline Man Pantoletten Schwarz 47 EU',
      originalPriceValue: 24.9,
      optimizedAsin: 'B0BLACK4790',
      optimizedTitle: 'Lico Herren Bioline Man Pantoletten Schwarz 47 EU',
      optimizedPriceValue: 17.9,
      optimizedPrice: '17,90€',
      optimizedSellerClass: 'FBA',
      optimizedIsAmazonFulfilled: true,
      optimizedAffiliateUrl: 'https://www.amazon.de/dp/B0BLACK4790',
      candidate: {
        asin: 'B0BLACK4790',
        title: 'Lico Herren Bioline Man Pantoletten Schwarz 47 EU',
        brand: 'Lico',
        priceValue: 17.9,
        isAmazonFulfilled: true,
        variationAttributes: [
          { name: 'Color', value: 'Schwarz' },
          { name: 'Size', value: '47 EU' }
        ]
      }
    },
    {
      originalProfile: buildOriginalProfile()
    },
    {
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
              isAmazonFulfilled: true,
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
            },
            {
              asin: 'B0FBM1599X',
              title: 'Lico Herren Bioline Man Pantoletten Rot 41 EU',
              brand: 'Lico',
              priceValue: 15.99,
              sellerClass: 'FBM',
              variationAttributes: [
                { name: 'Color', value: 'Rot' },
                { name: 'Size', value: '41 EU' }
              ]
            }
          ]
        };
      },
      paapiVariationLoader: async () => {
        paapiLoaderCalls += 1;
        return {
          status: 'no_hits',
          cacheHit: false,
          items: []
        };
      }
    }
  );

  assert.equal(result.rejectReason, '');
  assert.equal(result.finalAsin, 'B0BROWN3650');
  assert.equal(result.finalPrice, 16.5);
  assert.equal(result.finalSellerClass, 'AMAZON_DIRECT');
  assert.match(result.finalVariantLabel, /Braun/);
  assert.equal(result.originalCandidatePrice, 17.9);
  assert.equal(result.variantScanUsed, true);
  assert.equal(result.variantScanSource, 'creator');
  assert.equal(result.checkedVariantCount, 3);
  assert.equal(result.resolvedSimilarCheck?.optimizedAsin, 'B0BROWN3650');
  assert.equal(result.resolvedSimilarCheck?.optimizedPriceValue, 16.5);
  assert.equal(paapiLoaderCalls, 0);
});

await test('Finaler Offer-Resolver blockiert Rollen-Mismatch zwischen Set und Zubehoer', async () => {
  const result = await __testablesTelegramUserClient.resolveCheapestFinalOffer(
    {
      similarCheaperFound: true,
      originalTitle: 'Acme Kochgeschirr Set 5-teilig',
      originalPriceValue: 39.99,
      optimizedAsin: 'B0LIDONLY01',
      optimizedTitle: 'Acme Topfdeckel Edelstahl 28 cm',
      optimizedPriceValue: 12.99,
      optimizedSellerClass: 'AMAZON_DIRECT',
      optimizedMerchantName: 'Amazon',
      optimizedAffiliateUrl: 'https://www.amazon.de/dp/B0LIDONLY01',
      candidate: {
        asin: 'B0LIDONLY01',
        title: 'Acme Topfdeckel Edelstahl 28 cm',
        priceValue: 12.99,
        merchantName: 'Amazon'
      }
    },
    {
      originalProfile: buildOriginalProfile({
        asin: 'B0SET000001',
        title: 'Acme Kochgeschirr Set 5-teilig',
        brand: 'Acme',
        price: 39.99,
        productTypeTokens: ['acme', 'kochgeschirr', 'set'],
        coreFeatureTokens: ['kochgeschirr', 'set']
      })
    }
  );

  assert.equal(result.rejectReason, 'PRODUCT_ROLE_MISMATCH');
  assert.equal(result.variantScanUsed, false);
  assert.equal(result.variantScanSource, 'skipped');
});

await test('Finaler Offer-Resolver erlaubt FBA_UNKNOWN im Optimized-Flow fuer Produkte ohne Produkt-Regel', async () => {
  const result = await __testablesTelegramUserClient.resolveCheapestFinalOffer(
    {
      similarCheaperFound: true,
      originalTitle: 'Anker USB-C Hub 7-in-1 HDMI PD',
      originalPriceValue: 39.99,
      optimizedAsin: 'B0HUB700001',
      optimizedTitle: 'Anker USB-C Hub 7-in-1 HDMI PD',
      optimizedPriceValue: 29.99,
      optimizedSellerClass: 'FBA_OR_AMAZON_UNKNOWN',
      optimizedAffiliateUrl: 'https://www.amazon.de/dp/B0HUB700001',
      candidate: {
        asin: 'B0HUB700001',
        title: 'Anker USB-C Hub 7-in-1 HDMI PD',
        brand: 'Anker',
        priceValue: 29.99,
        isPrimeEligible: true
      }
    },
    {
      originalProfile: buildOriginalProfile({
        asin: 'B0HUB799999',
        title: 'Anker USB-C Hub 7-in-1 HDMI PD',
        brand: 'Anker',
        price: 39.99,
        productTypeTokens: ['anker', 'usb', 'hub'],
        coreFeatureTokens: ['hdmi', 'pd']
      })
    },
    {
      creatorVariationLoader: async () => ({
        status: 'no_hits',
        cacheHit: false,
        items: []
      }),
      paapiVariationLoader: async () => ({
        status: 'no_hits',
        cacheHit: false,
        items: []
      })
    }
  );

  assert.equal(result.rejectReason, '');
  assert.equal(result.finalSellerClass, 'FBA_UNKNOWN');
});

await test('Finaler Offer-Resolver blockiert Optimized Deal ueber Produkt-Regelgrenze', async () => {
  const result = await __testablesTelegramUserClient.resolveCheapestFinalOffer(
    {
      similarCheaperFound: true,
      originalTitle: 'A1 Wireless Earbuds Bluetooth Kopfhoerer Schwarz',
      originalPriceValue: 35,
      optimizedAsin: 'B0EARBUD2659',
      optimizedTitle: 'A1 Wireless Earbuds Bluetooth Kopfhoerer Schwarz',
      optimizedPriceValue: 26.59,
      optimizedSellerClass: 'AMAZON_DIRECT',
      optimizedMerchantName: 'Amazon',
      optimizedAffiliateUrl: 'https://www.amazon.de/dp/B0EARBUD2659',
      candidate: {
        asin: 'B0EARBUD2659',
        title: 'A1 Wireless Earbuds Bluetooth Kopfhoerer Schwarz',
        brand: 'A1',
        priceValue: 26.59,
        merchantName: 'Amazon',
        rating: 4.2,
        reviewCount: 120
      }
    },
    {
      originalProfile: buildOriginalProfile({
        asin: 'B0EARBUD3500',
        title: 'A1 Wireless Earbuds Bluetooth Kopfhoerer Schwarz',
        brand: 'A1',
        price: 35,
        productTypeTokens: ['wireless', 'earbuds', 'kopfhoerer'],
        coreFeatureTokens: ['bluetooth', 'earbuds']
      })
    },
    {
      creatorVariationLoader: async () => ({
        status: 'no_hits',
        cacheHit: false,
        items: []
      }),
      paapiVariationLoader: async () => ({
        status: 'no_hits',
        cacheHit: false,
        items: []
      })
    }
  );

  assert.equal(result.rejectReason, 'PRODUCT_RULE_BLOCKED');
  assert.equal(result.productRuleEvaluation?.matchedRuleName, 'China Kopfhoerer');
});

await test('Finaler Offer-Resolver blockiert andere Packgroessen', async () => {
  const result = await __testablesTelegramUserClient.resolveCheapestFinalOffer(
    {
      similarCheaperFound: true,
      originalTitle: 'Kinder Cards 30er Pack',
      originalPriceValue: 12.99,
      optimizedAsin: 'B0PACK0005',
      optimizedTitle: 'Kinder Cards 5er Pack',
      optimizedPriceValue: 2.69,
      optimizedSellerClass: 'AMAZON_DIRECT',
      optimizedMerchantName: 'Amazon',
      optimizedAffiliateUrl: 'https://www.amazon.de/dp/B0PACK0005',
      candidate: {
        asin: 'B0PACK0005',
        title: 'Kinder Cards 5er Pack',
        priceValue: 2.69,
        merchantName: 'Amazon'
      }
    },
    {
      originalProfile: buildOriginalProfile({
        asin: 'B0PACK0030',
        title: 'Kinder Cards 30er Pack',
        brand: 'Kinder',
        price: 12.99,
        productTypeTokens: ['kinder', 'cards', 'pack'],
        coreFeatureTokens: ['cards']
      })
    }
  );

  assert.equal(result.rejectReason, 'PACK_SIZE_MISMATCH');
  assert.equal(result.variantScanUsed, false);
  assert.equal(result.variantScanSource, 'skipped');
});

await test('Finaler Offer-Resolver nutzt Browser-Varianten, wenn Creator keine Familie liefert', async () => {
  const originalBrowserFlag = process.env.AMAZON_BROWSER_VARIANT_SCAN_ENABLED;
  process.env.AMAZON_BROWSER_VARIANT_SCAN_ENABLED = '1';
  let paapiLoaderCalls = 0;

  try {
    const result = await __testablesTelegramUserClient.resolveCheapestFinalOffer(
      {
        similarCheaperFound: true,
        originalTitle: 'Lico Herren Bioline Man Pantoletten Schwarz 47 EU',
        originalPriceValue: 24.9,
        optimizedAsin: 'B0BLACK47X',
        optimizedTitle: 'Lico Herren Bioline Man Pantoletten Schwarz 47 EU',
        optimizedPriceValue: 17.9,
        optimizedPrice: '17,90 EUR',
        optimizedSellerClass: 'FBA',
        optimizedIsAmazonFulfilled: true,
        optimizedAffiliateUrl: 'https://www.amazon.de/dp/B0BLACK47X',
        candidate: {
          asin: 'B0BLACK47X',
          title: 'Lico Herren Bioline Man Pantoletten Schwarz 47 EU',
          brand: 'Lico',
          priceValue: 17.9,
          isAmazonFulfilled: true
        }
      },
      {
        originalProfile: buildOriginalProfile()
      },
      {
        creatorVariationLoader: async () => ({
          status: 'no_family_variants',
          cacheHit: false,
          items: []
        }),
        paapiVariationLoader: async () => {
          paapiLoaderCalls += 1;
          return {
            status: 'no_hits',
            cacheHit: false,
            items: []
          };
        },
        fetchImpl: async () => ({
          ok: true,
          url: 'https://www.amazon.de/dp/B0BLACK47X',
          text: async () => `
            <div class="twister">
              <a class="a-button-text" data-dp-url="/dp/B0BROWN36X" href="/dp/B0BROWN36X" title="Braun 36 EU">
                <span class="a-price-whole">16</span><span class="a-price-fraction">50</span>
              </a>
            </div>
          `
        }),
        browserEnricher: async (candidates) =>
          candidates.map((candidate) => ({
            ...candidate,
            asin: candidate.asin,
            title: candidate.asin === 'B0BROWN36X' ? 'Lico Herren Bioline Man Pantoletten Braun 36 EU' : candidate.title,
            brand: candidate.asin === 'B0BROWN36X' ? 'Lico' : candidate.brand,
            priceValue: candidate.asin === 'B0BROWN36X' ? 16.5 : candidate.priceValue,
            merchantName: candidate.asin === 'B0BROWN36X' ? 'Amazon' : '',
            variationAttributes:
              candidate.asin === 'B0BROWN36X'
                ? [
                    { name: 'Color', value: 'Braun' },
                    { name: 'Size', value: '36 EU' }
                  ]
                : candidate.variationAttributes
          }))
      }
    );

    assert.equal(result.rejectReason, '');
    assert.equal(result.finalAsin, 'B0BROWN36X');
    assert.equal(result.finalPrice, 16.5);
    assert.equal(result.variantScanSource, 'browser');
    assert.equal(paapiLoaderCalls, 0);
  } finally {
    if (originalBrowserFlag === undefined) {
      delete process.env.AMAZON_BROWSER_VARIANT_SCAN_ENABLED;
    } else {
      process.env.AMAZON_BROWSER_VARIANT_SCAN_ENABLED = originalBrowserFlag;
    }
  }
});
