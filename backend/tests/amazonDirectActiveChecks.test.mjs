import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'affiliate-amazon-direct-active-'));
process.env.APP_DATA_DIR = tempRoot;
process.env.APP_DB_PATH = path.join(tempRoot, 'deals.db');
process.env.READER_TEST_MODE = '1';
process.env.READER_DEBUG_MODE = '0';

const { buildGeneratorDealContext } = await import('../services/generatorDealScoringService.js');

async function test(name, fn) {
  try {
    await fn();
    console.log(`PASS ${name}`);
  } catch (error) {
    console.error(`FAIL ${name}`);
    throw error;
  }
}

await test('Amazon Direct startet im Reader-Testmodus aktiven Marktvergleich und aktive KI auch ohne Cache', async () => {
  const result = await buildGeneratorDealContext({
    asin: 'B0ACTIVE01',
    sellerType: 'AMAZON',
    sellerClass: 'AMAZON_DIRECT',
    soldByAmazon: true,
    shippedByAmazon: true,
    sellerDetectionSource: 'combined-seller-shipping-text',
    currentPrice: '49,99',
    title: 'Aktiver Amazon Direct Testdeal',
    productUrl: 'https://www.amazon.de/dp/B0ACTIVE01',
    imageUrl: 'https://images-eu.ssl-images-amazon.com/images/I/81active._SL1200_.jpg',
    source: 'telegram_reader_polling',
    origin: 'automatic'
  });

  assert.equal(result.seller?.sellerClass, 'AMAZON_DIRECT');

  assert.equal(result.learning.marketComparisonRequired, true);
  assert.equal(result.learning.marketComparisonStarted, true);
  assert.equal(result.learning.marketComparisonStatus, 'error');
  assert.match(
    result.learning.marketComparisonReason,
    /Manuelle Vergleichsdaten|Aktiver Marktvergleich|Produktkontext/
  );

  assert.equal(result.learning.aiRequired, true);
  assert.equal(result.learning.aiCheckStarted, true);
  assert.equal(result.learning.aiCheckStatus, 'success');
  assert.match(result.learning.aiCheckReason, /Fallback-Daten|Marktvergleich/);
});

console.log('OK Amazon-Direct-Active-Checks getestet');
