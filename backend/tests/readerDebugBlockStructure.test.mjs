import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'affiliate-reader-debug-'));
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

await test('Strukturierter Kurzinfo-Block zeigt vier Bereiche und UI-Hinweise', async () => {
  const block = __testablesTelegramUserClient.buildReaderCompactDebugBlockV3({
    decisionDisplay: 'REVIEW',
    wouldPostNormally: false,
    testGroupPosted: true,
    reason: 'Pflichtprüfung fehlt',
    sellerClass: 'AMAZON_DIRECT',
    soldByAmazon: true,
    shippedByAmazon: true,
    sellerProfileStatus: 'NICHT NOETIG',
    marketComparisonStatus: 'error',
    marketComparisonStarted: true,
    marketComparisonReason: 'keine Vergleichsdaten vorhanden',
    marketComparisonRequired: true,
    marketComparisonUsed: false,
    marketComparisonSourceName: '',
    aiCheckStatus: 'success',
    aiCheckStarted: true,
    aiUsed: true,
    aiAllowed: true,
    aiOnlyOnUncertainty: true,
    keepaUsed: false,
    keepaFallbackUsed: false,
    thresholds: {
      minDiscountPercent: 20,
      minScore: 60,
      fakeRejectThreshold: 70
    },
    settingsAreas: {
      sampling: 'Sampling & Qualität',
      decision: 'Entscheidungslogik'
    }
  });

  assert.match(block, /🧾 <b>DEAL STATUS<\/b>/);
  assert.match(block, /🏪 <b>SELLER CHECK<\/b>/);
  assert.match(block, /📊 <b>VERGLEICH & KI<\/b>/);
  assert.match(block, /⚙️ <b>SYSTEM REGELN<\/b>/);
  assert.match(block, /📦 Verkauf: Amazon/);
  assert.match(block, /🚚 Versand: Amazon/);
  assert.match(block, /📊 Vergleich genutzt: KI \(Fallback\)/);
  assert.match(block, /🚫 Vergleich NICHT genutzt: Marktvergleich, Keepa/);
  assert.match(block, /→ ändern unter: Sampling (&|&amp;) Qualität/);
  assert.match(block, /→ ändern unter: Entscheidungslogik/);
  assert.doesNotMatch(block, /Queue-ID/);
  assert.doesNotMatch(block, /Coupon erkannt/);
});

await test('Strukturierter Kurzinfo-Block zeigt Fehlhinweise fuer fehlende Checks und unklaren Seller', async () => {
  const block = __testablesTelegramUserClient.buildReaderCompactDebugBlockV3({
    decisionDisplay: 'REVIEW',
    wouldPostNormally: false,
    testGroupPosted: true,
    reason: 'Seller unklar',
    sellerClass: 'UNKNOWN',
    soldByAmazon: null,
    shippedByAmazon: null,
    sellerProfileStatus: 'NICHT NOETIG',
    sellerRecognitionMessage: 'Seller konnte nicht erkannt werden.',
    marketComparisonStatus: 'skipped',
    marketComparisonStarted: false,
    marketComparisonReason: 'nicht gestartet',
    marketComparisonRequired: false,
    marketComparisonUsed: false,
    aiCheckStatus: 'skipped',
    aiCheckStarted: false,
    aiUsed: false,
    aiAllowed: false,
    aiReason: 'KI ist deaktiviert.',
    keepaUsed: false,
    keepaFallbackUsed: false,
    thresholds: {
      minDiscountPercent: 20,
      minScore: 60,
      fakeRejectThreshold: 70
    }
  });

  assert.match(block, /❌ Seller unklar/);
  assert.match(block, /👉 Problem: Scraper \/ Seller Detection/);
  assert.match(block, /❌ Marktvergleich fehlt/);
  assert.match(block, /👉 Einstellung prüfen: Marktvergleich aktiv/);
  assert.match(block, /❌ KI nicht gestartet/);
  assert.match(block, /👉 Einstellung prüfen: KI aktiv/);
  assert.match(block, /→ Quelle: KEINE/);
});

console.log('OK Reader-Debug-Block-Struktur getestet');
