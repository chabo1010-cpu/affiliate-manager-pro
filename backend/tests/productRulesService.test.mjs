import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'affiliate-product-rules-'));
process.env.APP_DATA_DIR = tempRoot;
process.env.APP_DB_PATH = path.join(tempRoot, 'deals.db');

const { evaluateProductRules, saveProductRule } = await import('../services/productRulesService.js');

async function test(name, fn) {
  try {
    await fn();
    console.log(`PASS ${name}`);
  } catch (error) {
    console.error(`FAIL ${name}`);
    throw error;
  }
}

await test('China Bluetooth Kopfhoerer 26,59 Euro werden ueber Regelgrenze blockiert', async () => {
  const result = evaluateProductRules({
    title: 'A1 Bluetooth Kopfhoerer In Ear TWS Schwarz',
    brand: 'A1',
    finalPrice: 26.59,
    rating: 4.2,
    reviewCount: 100,
    sellerClass: 'AMAZON_DIRECT',
    scope: 'test'
  });

  assert.equal(result.matchedRuleName, 'China Kopfhoerer');
  assert.equal(result.allowed, false);
  assert.equal(result.decision, 'block');
  assert.equal(result.reasonCode, 'PRODUCT_RULE_BLOCKED');
});

await test('China Bluetooth Kopfhoerer 9,99 Euro bleiben mit guten Daten erlaubt', async () => {
  const result = evaluateProductRules({
    title: 'A1 Bluetooth Kopfhoerer In Ear TWS Schwarz',
    brand: 'A1',
    finalPrice: 9.99,
    rating: 4.2,
    reviewCount: 100,
    sellerClass: 'AMAZON_DIRECT',
    scope: 'test'
  });

  assert.equal(result.matchedRuleName, 'China Kopfhoerer');
  assert.equal(result.allowed, true);
  assert.equal(result.reasonCode, 'PRODUCT_RULE_ALLOWED');
});

await test('Powerbank 10000mAh 14,99 Euro wird geblockt', async () => {
  const result = evaluateProductRules({
    title: 'Anker Powerbank 10.000mAh Schwarz',
    brand: 'Anker',
    finalPrice: 14.99,
    rating: 4.5,
    reviewCount: 300,
    sellerClass: 'AMAZON_DIRECT',
    scope: 'test'
  });

  assert.equal(result.matchedRuleName, 'Powerbank 10000mAh');
  assert.equal(result.allowed, false);
  assert.equal(result.reasonCode, 'PRODUCT_RULE_BLOCKED');
});

await test('Powerbank 20000mAh 15,99 Euro bleibt erlaubt', async () => {
  const result = evaluateProductRules({
    title: 'Anker Powerbank 20.000mAh Schwarz',
    brand: 'Anker',
    finalPrice: 15.99,
    rating: 4.5,
    reviewCount: 300,
    sellerClass: 'AMAZON_DIRECT',
    scope: 'test'
  });

  assert.equal(result.matchedRuleName, 'Powerbank 19000-30000mAh');
  assert.equal(result.allowed, true);
  assert.equal(result.reasonCode, 'PRODUCT_RULE_ALLOWED');
});

await test('Markenprodukt mit Marktvergleich-Pflicht geht ohne Vergleich auf Review', async () => {
  await saveProductRule({
    name: 'Marken Kopfhoerer Review',
    keywords: 'kopfhoerer, bluetooth kopfhoerer',
    brandType: 'BRAND',
    maxPrice: 99,
    minReviews: 0,
    minRating: 0,
    marketCompareRequired: true,
    active: true
  });

  const result = evaluateProductRules({
    title: 'Sony Bluetooth Kopfhoerer Schwarz',
    brand: 'Sony',
    finalPrice: 49.99,
    rating: 4.6,
    reviewCount: 240,
    sellerClass: 'AMAZON_DIRECT',
    marketComparisonStatus: 'missing',
    scope: 'test'
  });

  assert.equal(result.matchedRuleName, 'Marken Kopfhoerer Review');
  assert.equal(result.allowed, false);
  assert.equal(result.decision, 'review');
  assert.equal(result.reasonCode, 'PRODUCT_RULE_REVIEW_REQUIRED');
});
