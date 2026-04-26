import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'affiliate-non-amazon-active-'));
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

await test('Non-Amazon-Deal startet Marktvergleich und KI trotz UNKNOWN Seller', async () => {
  const result = await buildGeneratorDealContext({
    asin: 'NONAMZ001',
    sellerType: 'UNKNOWN',
    sellerClass: 'UNKNOWN',
    soldByAmazon: null,
    shippedByAmazon: null,
    sellerDetectionSource: 'non-amazon',
    sellerDetails: {
      detectionSource: 'non-amazon',
      detectionSources: ['non-amazon'],
      dealType: 'NON_AMAZON',
      isAmazonDeal: false
    },
    dealType: 'NON_AMAZON',
    isAmazonDeal: false,
    currentPrice: '29,99',
    title: 'Non Amazon Testdeal',
    productUrl: 'https://example.com/deal',
    imageUrl: 'https://example.com/deal-image.jpg',
    source: 'telegram_reader_polling',
    origin: 'automatic'
  });

  assert.equal(result.seller?.isNonAmazonDeal, true);
  assert.equal(result.decisionPolicy?.marketComparison?.allowed, true);
  assert.equal(result.decisionPolicy?.ai?.allowed, true);
  assert.equal(result.learning.marketComparisonRequired, true);
  assert.equal(result.learning.marketComparisonStarted, true);
  assert.equal(result.learning.aiRequired, true);
  assert.equal(result.learning.aiCheckStarted, true);
  assert.equal(result.learning.aiCheckStatus, 'success');
});

console.log('OK Non-Amazon-Active-Checks getestet');
