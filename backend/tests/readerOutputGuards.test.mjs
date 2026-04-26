import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'affiliate-reader-guards-'));
process.env.APP_DATA_DIR = tempRoot;
process.env.APP_DB_PATH = path.join(tempRoot, 'deals.db');

const { __testablesDirectPublisher } = await import('../services/directPublisher.js');
const { resolveDealImageUrlFromScrape } = await import('../../frontend/src/lib/postGenerator.js');

function test(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (error) {
    console.error(`FAIL ${name}`);
    throw error;
  }
}

test('Gemeinsame Generator-Bildlogik nimmt dasselbe Amazon-Bild wie der Generator', () => {
  const imageUrl = resolveDealImageUrlFromScrape({
    imageUrl: 'https://images-eu.ssl-images-amazon.com/images/I/81abc123._SX342_.jpg'
  });

  assert.equal(
    imageUrl,
    'https://images-eu.ssl-images-amazon.com/images/I/81abc123._SL1200_.jpg'
  );
});

test('0,00 Preis wird fuer Live-Posts als ungueltig erkannt', () => {
  const validation = __testablesDirectPublisher.validatePublishingPrice({
    currentPrice: '0,00€'
  });

  assert.equal(validation.valid, false);
  assert.equal(validation.reason, 'Preis ist 0,00€ oder ungueltig.');
});

test('Fehlender Preis wird fuer Live-Posts als ungueltig erkannt', () => {
  const validation = __testablesDirectPublisher.validatePublishingPrice({
    currentPrice: ''
  });

  assert.equal(validation.valid, false);
  assert.equal(validation.reason, 'Preis fehlt oder ist ungueltig.');
});

test('Gueltiger Preis bleibt fuer Generator-Posts erlaubt', () => {
  const validation = __testablesDirectPublisher.validatePublishingPrice({
    currentPrice: '19,99€'
  });

  assert.equal(validation.valid, true);
  assert.equal(validation.parsedPrice, 19.99);
});

console.log('OK Reader-Output-Guards getestet');
