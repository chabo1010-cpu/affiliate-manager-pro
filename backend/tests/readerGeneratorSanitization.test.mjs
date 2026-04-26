import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'affiliate-reader-sanitize-'));
process.env.APP_DATA_DIR = tempRoot;
process.env.APP_DB_PATH = path.join(tempRoot, 'deals.db');
process.env.READER_TEST_MODE = '1';
process.env.READER_DEBUG_MODE = '0';

const { __testablesTelegramUserClient } = await import('../services/telegramUserClientService.js');
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

await test('Amazon-Post uebernimmt nur den Amazon-Titel und blockiert Telegram-Fremdtext', async () => {
  const titlePayload = __testablesTelegramUserClient.resolveReaderTitlePayload({
    dealType: 'AMAZON',
    scrapedDeal: {
      productTitle: 'Bosch Professional GSR 18V-55 Akku-Bohrschrauber'
    },
    structuredMessage: {
      text: '🔥 HISTORISCHER BESTPREIS\nBosch nur heute!\n29,99€',
      previewTitle: 'HISTORISCHER BESTPREIS',
      group: 'Fremdgruppe'
    }
  });

  assert.equal(titlePayload.title, 'Bosch Professional GSR 18V-55 Akku-Bohrschrauber');
  assert.equal(titlePayload.titleSource, 'amazon');

  const template = __testablesTelegramUserClient.buildTelegramReaderTemplatePayload({
    title: titlePayload.title,
    description: 'HISTORISCHER BESTPREIS',
    affiliateUrl: 'https://www.amazon.de/dp/B012345678',
    currentPrice: '29,99'
  });

  assert.match(template.telegramCaption, /Bosch Professional GSR 18V-55 Akku-Bohrschrauber/);
  assert.doesNotMatch(template.telegramCaption, /HISTORISCHER BESTPREIS/);
});

await test('Amazon-Preis priorisiert BuyBox vor Deal- und Telegram-Preis', async () => {
  const pricePayload = __testablesTelegramUserClient.resolveReaderPricePayload({
    dealType: 'AMAZON',
    scrapedDeal: {
      basePrice: '59,99€',
      price: '49,99€',
      finalPrice: '49,99€',
      finalPriceCalculated: true
    },
    pricing: {
      currentPrice: '39,99'
    }
  });

  assert.equal(pricePayload.priceSource, 'amazon');
  assert.match(pricePayload.currentPrice, /59,99/);

  const template = __testablesTelegramUserClient.buildTelegramReaderTemplatePayload({
    title: 'Amazon Produkt',
    affiliateUrl: 'https://www.amazon.de/dp/B012345678',
    currentPrice: pricePayload.currentPrice
  });

  assert.match(template.telegramCaption, /59,99/);
  assert.doesNotMatch(template.telegramCaption, /49,99/);
  assert.doesNotMatch(template.telegramCaption, /39,99/);
});

await test('Fallback-Bild wird erzeugt, wenn keine Bildquelle vorhanden ist', async () => {
  const imagePayload = __testablesTelegramUserClient.resolveReaderImagePayload({
    dealType: 'NON_AMAZON',
    scrapedDeal: {},
    structuredMessage: {},
    title: 'Fallback Deal',
    currentPrice: '19,99'
  });

  assert.equal(imagePayload.imageSource, 'fallback');
  assert.equal(imagePayload.generatedImagePath, '');
  assert.match(imagePayload.uploadedImagePath, /^data:image\/svg\+xml;base64,/);
});

await test('Cloudflare-Titel wird erkannt und nicht als Produktdaten verwendet', async () => {
  const matches = __testablesTelegramUserClient.collectProtectedSourceMatches([
    { source: 'previewTitle', value: 'Just a moment...' },
    { source: 'previewDescription', value: 'Checking your browser before accessing the site.' }
  ]);

  assert.ok(matches.length >= 2);
  assert.ok(matches.some((entry) => entry.key === 'just_a_moment'));
  assert.ok(matches.some((entry) => entry.key === 'checking_your_browser'));
});

await test('Amazon-Deal nutzt im Testpfad Quellenbild als Fallback statt zu blockieren', async () => {
  const imagePayload = __testablesTelegramUserClient.resolveReaderImagePayload({
    dealType: 'AMAZON',
    scrapedDeal: {
      imageUrl: '',
      previewImage: '',
      ogImage: ''
    },
    structuredMessage: {
      previewImage: 'https://cdn.example.com/source-image.jpg'
    },
    title: 'Amazon Produkt',
    currentPrice: '49,99'
  });

  assert.equal(imagePayload.imageSource, 'telegram');
  assert.equal(imagePayload.generatedImagePath, 'https://cdn.example.com/source-image.jpg');
  assert.equal(imagePayload.uploadedImagePath, '');
});

await test('Produktverifikation blockiert Cloudflare/Fremdlink ohne Amazon-Bild und Partnerlink', async () => {
  const verification = __testablesTelegramUserClient.resolveProductVerification({
    dealType: 'AMAZON',
    linkRecord: {
      affiliateUrl: 'https://s.pirat.deals/abc'
    },
    scrapedDeal: {
      asin: '',
      productTitle: '',
      title: 'Just a moment...',
      price: '',
      imageUrl: ''
    },
    generatorInput: {
      asin: '',
      title: '',
      currentPrice: '',
      link: 'https://s.pirat.deals/abc',
      generatedImagePath: ''
    },
    sourceMeta: {
      protectedSource: true
    }
  });

  assert.equal(verification.verified, false);
  assert.match(verification.reason, /Cloudflare|Partnerlink|ASIN|Amazon-Bild/i);
});

await test('Produktverifikation wird im Reader-Testmodus nur noch als Warnung markiert', async () => {
  const verification = __testablesTelegramUserClient.resolveProductVerification({
    dealType: 'AMAZON',
    linkRecord: {
      affiliateUrl: 'https://s.pirat.deals/abc'
    },
    scrapedDeal: {
      asin: '',
      productTitle: '',
      title: 'Just a moment...',
      price: '',
      imageUrl: ''
    },
    generatorInput: {
      asin: '',
      title: '',
      currentPrice: '',
      link: 'https://s.pirat.deals/abc',
      generatedImagePath: '',
      uploadedImagePath: 'data:image/svg+xml;base64,ZmFrZQ=='
    },
    sourceMeta: {
      protectedSource: true
    },
    readerConfig: {
      readerTestMode: true
    }
  });

  assert.equal(verification.verified, true);
  assert.equal(verification.warningOnly, true);
  assert.match(verification.reason, /Cloudflare|Partnerlink|ASIN/i);
});

await test('Relaxed Match-Score stuft Amazon-Suche in Auto-Post, Review und Debug ein', async () => {
  assert.deepEqual(__testablesTelegramUserClient.classifyRelaxedAmazonMatchScore(60), {
    tier: 'auto_post',
    decision: 'APPROVE',
    matched: true,
    reason: 'Amazon-Match >= 60 erkannt.'
  });
  assert.deepEqual(__testablesTelegramUserClient.classifyRelaxedAmazonMatchScore(45), {
    tier: 'review',
    decision: 'REVIEW',
    matched: false,
    reason: 'Kein perfekter Match, aber fuer die Testgruppe ausreichend.'
  });
  assert.deepEqual(__testablesTelegramUserClient.classifyRelaxedAmazonMatchScore(20), {
    tier: 'debug',
    decision: 'DEBUG',
    matched: false,
    reason: 'Kein perfekter Match; Deal bleibt in der Testgruppe als Debug sichtbar.'
  });
});

await test('Nur eigener Amazon-Partnerlink gilt als verifiziert', async () => {
  assert.equal(
    __testablesTelegramUserClient.isOwnAmazonAffiliateLink(
      'https://www.amazon.de/dp/B012345678?tag=codeundcoup08-21',
      'B012345678'
    ),
    true
  );
  assert.equal(
    __testablesTelegramUserClient.isOwnAmazonAffiliateLink('https://www.amazon.de/dp/B012345678', 'B012345678'),
    false
  );
});

await test('Seller UNKNOWN blockiert Marktvergleich und KI im Reader-Testmodus nicht mehr', async () => {
  const result = await buildGeneratorDealContext({
    asin: 'B0UNKPIPE1',
    sellerType: 'UNKNOWN',
    sellerClass: 'UNKNOWN',
    soldByAmazon: null,
    shippedByAmazon: null,
    sellerDetectionSource: 'forced-test',
    sellerDetails: {
      detectionSource: 'forced-test',
      detectionSources: ['forced-test'],
      dealType: 'AMAZON',
      isAmazonDeal: true
    },
    dealType: 'AMAZON',
    isAmazonDeal: true,
    currentPrice: '19,99',
    title: 'Amazon Produkt',
    productUrl: 'https://www.amazon.de/dp/B0UNKPIPE1',
    imageUrl: 'https://example.com/unknown-seller.jpg',
    source: 'telegram_reader_polling',
    origin: 'automatic'
  });

  assert.equal(result.decisionPolicy?.marketComparison?.allowed, true);
  assert.equal(result.decisionPolicy?.ai?.allowed, true);
  assert.equal(result.learning.marketComparisonStarted, true);
  assert.equal(result.learning.aiCheckStarted, true);
});

await test('Kein Amazon-Link wird als Non-Amazon-Deal weiterverarbeitet statt geskippt', async () => {
  const dealType = __testablesTelegramUserClient.resolveReaderDealType({
    amazonLink: '',
    detectedAsin: ''
  });
  const linkRecord = __testablesTelegramUserClient.buildReaderLinkRecord({
    dealType,
    fallbackLink: 'https://example.com/deal',
    structuredMessage: {
      text: 'Deal ohne Amazon-Link',
      group: 'Testgruppe'
    }
  });

  assert.equal(dealType, 'NON_AMAZON');
  assert.equal(linkRecord.valid, true);
  assert.equal(linkRecord.affiliateUrl, 'https://example.com/deal');
  assert.match(linkRecord.asin, /^[A-Z0-9]{10}$/);
});

console.log('OK Reader-Generator-Sanitizing getestet');
