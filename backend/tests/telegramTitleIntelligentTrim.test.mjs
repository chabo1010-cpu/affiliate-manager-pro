import assert from 'node:assert/strict';

import { buildTelegramTitle, estimateTelegramTextWidth } from '../../frontend/src/lib/postGenerator.js';

async function test(name, fn) {
  try {
    await fn();
    console.log(`PASS ${name}`);
  } catch (error) {
    console.error(`FAIL ${name}`);
    throw error;
  }
}

await test('Kurze Titel bis 80 Zeichen bleiben unveraendert', async () => {
  const originalTitle = 'Bosch Professional GSR 18V-55 Akku-Bohrschrauber';

  assert.equal(buildTelegramTitle(originalTitle), originalTitle);
});

await test('Lange Titel werden intelligent auf zwei Telegram-Zeilen begrenzt', async () => {
  const originalTitle =
    'Bosch Professional GSR 18V-55 Akku-Bohrschrauber mit 2 Akkus und Ladegeraet, Blau, Groesse XL, Premium Angebot';
  const result = buildTelegramTitle(originalTitle);

  assert.ok(result.length <= 110);
  assert.ok(estimateTelegramTextWidth(result) <= 108);
  assert.match(result, /Bosch Professional/);
  assert.match(result, /Akku-Bohrschrauber/);
  assert.match(result, /2 Akkus/);
  assert.match(result, /Blau/);
  assert.match(result, /Gr\. XL/);
  assert.doesNotMatch(result, /Premium|Angebot/i);
  assert.doesNotMatch(result, /\.\.\.$/);
});

await test('Relevante Varianten und Zahlen bleiben trotz Bereinigung erhalten', async () => {
  const originalTitle =
    'Ninja Foodi MAX Dual Zone Heissluftfritteuse AF400EUCP, 9,5 L, 6 Kochfunktionen, Schwarz/Kupfer';
  const result = buildTelegramTitle(originalTitle);

  assert.ok(result.length <= 110);
  assert.ok(estimateTelegramTextWidth(result) <= 108);
  assert.match(result, /Ninja Foodi MAX Dual Zone Heissluftfritteuse/);
  assert.match(result, /9,5 L/);
  assert.match(result, /6 Kochfunktionen/);
  assert.match(result, /Schwarz\/Kupfer/);
  assert.doesNotMatch(result, /AF400EUCP/);
});

await test('Ellipsis erscheint erst als letzter Schritt', async () => {
  const originalTitle =
    'Philips Sonicare DiamondClean Smart Schallzahnbuerste mit App-Steuerung und Premium Plaque Defense Buerstenkoepfen fuer Erwachsene in elegantem Schwarz mit Reiseetui und Ladeglas sowie zusaetzlicher UV-Reinigungsstation fuer das Badezimmer';
  const result = buildTelegramTitle(originalTitle);

  assert.ok(result.length <= 110);
  assert.ok(estimateTelegramTextWidth(result) <= 108);
  assert.match(result, /\.\.\.$/);
  assert.ok(result.split(/\s+/).filter(Boolean).length >= 3);
});

await test('Doppelte Sprachreste werden auch bei kuerzeren Titeln entfernt', async () => {
  const originalTitle = 'Oral-B Pro Series 3 Elektrische Zahnbuerste Doppelpack Electric Toothbrush';
  const result = buildTelegramTitle(originalTitle);

  assert.equal(result, 'Oral-B Pro Series 3 Zahnbuerste Doppelpack');
  assert.ok(estimateTelegramTextWidth(result) <= 100);
  assert.doesNotMatch(result, /Electric Toothbrush/i);
});
