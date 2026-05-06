import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'affiliate-telegram-coupon-code-'));
process.env.APP_DATA_DIR = tempRoot;
process.env.APP_DB_PATH = path.join(tempRoot, 'deals.db');
process.env.READER_TEST_MODE = '1';
process.env.READER_DEBUG_MODE = '0';

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

function buildOptimizedSimilarCheck(overrides = {}) {
  return {
    optimizedTitle: 'Anker Powerbank 10000mAh Schwarz',
    similarCheaperTitle: 'Anker Powerbank 10000mAh Schwarz',
    originalTitle: 'Anker Powerbank 10000mAh Weiss',
    originalSourceGroup: '@dealgruppe',
    optimizedPriceValue: 9.99,
    similarCheaperPriceValue: 9.99,
    optimizedPrice: '9,99€',
    similarCheaperPrice: '9,99€',
    originalPrice: '12,99€',
    differenceAmount: '3,00€',
    differencePercent: 23.1,
    optimizedSellerClass: 'AMAZON_DIRECT',
    similarCheaperSellerClass: 'AMAZON_DIRECT',
    similarityScore: 93,
    similarCheaperReason: 'Gleiche Produktart guenstiger gefunden.',
    optimizedAffiliateUrl: 'https://www.amazon.de/dp/B012345678?tag=codeundcoup08-21',
    variantSelected: false,
    couponCode: '',
    couponInfo: 'Kein Coupon erkannt',
    ...overrides
  };
}

await test('Amazon Promotion ohne Telegram-Code liefert keinen Rabattcode', async () => {
  const couponCode = __testablesTelegramUserClient.extractTelegramCouponCode(
    'Spare 5% bei 4 ausgewaehlten Artikeln\nVORHER 19,99€\nJETZT 16,50€'
  );

  assert.equal(couponCode, '');
});

await test('Stopword wie VORHER wird selbst mit Code-Kontext nicht als Rabattcode uebernommen', async () => {
  const couponCode = __testablesTelegramUserClient.extractTelegramCouponCode('Mit Code: VORHER');

  assert.equal(couponCode, '');
});

await test('Echter Telegram-Rabattcode wird erkannt', async () => {
  const couponCode = __testablesTelegramUserClient.extractTelegramCouponCode(
    'Heute guenstiger\nMit Code: SOMMER10\nNur solange Vorrat reicht'
  );

  assert.equal(couponCode, 'SOMMER10');
});

await test('Optimized Post zeigt keinen Coupon-Hinweis ohne echten Telegram-Code', async () => {
  const postText = __testablesTelegramUserClient.buildOptimizedSimilarDealPost(
    buildOptimizedSimilarCheck({
      couponDetected: true,
      couponCode: '',
      couponInfo: 'Amazon Coupon: 5% | Kein Coupon erkannt'
    })
  );

  assert.equal(postText.includes('✔️ Coupon aktivieren'), true);
  assert.equal(postText.includes('ℹ️ Rabattgutschein:'), false);
  assert.equal(postText.includes('Amazon Coupon:'), false);
  assert.equal(postText.includes('CODE:'), false);
});

await test('Optimized Post zeigt echten Telegram-Rabattcode weiter an', async () => {
  const postText = __testablesTelegramUserClient.buildOptimizedSimilarDealPost(
    buildOptimizedSimilarCheck({
      couponCode: 'SOMMER10',
      couponInfo: 'Code: SOMMER10'
    })
  );

  assert.equal(postText.includes('ℹ️ Rabattgutschein: SOMMER10'), true);
});

await test('Reader Template zeigt klickbaren Amazon Coupon nur als Hinweis', async () => {
  const generatedPost = __testablesTelegramUserClient.buildTelegramReaderTemplatePayload({
    title: 'Anker Powerbank 10000mAh',
    affiliateUrl: 'https://www.amazon.de/dp/B012345678?tag=codeundcoup08-21',
    currentPrice: '9,99€',
    couponDetected: true
  });

  assert.equal(generatedPost.telegramCaption.includes('✔️ Coupon aktivieren'), true);
  assert.equal(generatedPost.telegramCaption.includes('ℹ️ Rabattgutschein:'), false);
  assert.equal(generatedPost.couponFollowUp, '');
});

await test('Reader Template uebernimmt echten Telegram-Code im Generator-Stil', async () => {
  const generatedPost = __testablesTelegramUserClient.buildTelegramReaderTemplatePayload({
    title: 'Anker Powerbank 10000mAh',
    affiliateUrl: 'https://www.amazon.de/dp/B012345678?tag=codeundcoup08-21',
    currentPrice: '9,99€',
    couponCode: 'SOMMER10'
  });

  assert.equal(generatedPost.telegramCaption.includes('ℹ️ Rabattgutschein: SOMMER10'), true);
  assert.equal(generatedPost.couponFollowUp, 'SOMMER10');
});

await test('Optimized Post uebernimmt die beste Variante in die Generator-Struktur', async () => {
  const postText = __testablesTelegramUserClient.buildOptimizedSimilarDealPost(
    buildOptimizedSimilarCheck({
      variantSelected: true,
      variantLabel: 'Braun • 36 EU'
    })
  );

  assert.equal(postText.includes('🎨 Beste Variante:'), true);
  assert.equal(postText.includes('Braun • 36 EU'), true);
});
