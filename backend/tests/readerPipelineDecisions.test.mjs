import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'affiliate-reader-pipeline-'));
process.env.APP_DATA_DIR = tempRoot;
process.env.APP_DB_PATH = path.join(tempRoot, 'deals.db');

const { __testablesTelegramUserClient } = await import('../services/telegramUserClientService.js');

function test(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (error) {
    console.error(`FAIL ${name}`);
    throw error;
  }
}

test('Amazon Direct ohne erfolgreiche Pflichtpruefungen blockiert APPROVE im Reader', () => {
  const result = __testablesTelegramUserClient.resolveAmazonDirectRequiredCheckBlock({
    generatorInput: {
      sellerClass: 'AMAZON_DIRECT'
    },
    generatorContext: {
      learning: {
        marketComparisonRequired: true,
        marketComparisonStatus: 'skipped',
        marketComparisonReason: 'Kein gespeicherter Internetvergleich gefunden.',
        aiRequired: true,
        aiCheckStatus: 'skipped',
        aiCheckReason: 'Kein gespeicherter Internetvergleich gefunden.'
      }
    }
  });

  assert.equal(result.blocked, true);
  assert.match(result.reason, /Pflichtpruefung fehlt:/);
  assert.deepEqual(result.missingChecks, [
    'Marktvergleich: Kein gespeicherter Internetvergleich gefunden.',
    'KI: Kein gespeicherter Internetvergleich gefunden.'
  ]);
});

test('Amazon Direct mit erfolgreichen Pflichtpruefungen bleibt freigegeben', () => {
  const result = __testablesTelegramUserClient.resolveAmazonDirectRequiredCheckBlock({
    generatorInput: {
      sellerClass: 'AMAZON_DIRECT'
    },
    generatorContext: {
      learning: {
        marketComparisonRequired: true,
        marketComparisonStatus: 'success',
        marketComparisonReason: 'stored-market erfolgreich geladen.',
        aiRequired: true,
        aiCheckStatus: 'success',
        aiCheckReason: 'KI-Pruefung wurde ausgefuehrt.'
      }
    }
  });

  assert.equal(result.blocked, false);
  assert.equal(result.reason, '');
  assert.deepEqual(result.missingChecks, []);
});

test('Reader-Testmodus akzeptiert auch REVIEW/UNKNOWN Deals fuer die Testgruppe', () => {
  const result = __testablesTelegramUserClient.evaluateTelegramReaderGeneratorCandidate(
    {
      learning: {
        routingDecision: 'review',
        reason: 'Seller unbekannt.'
      },
      keepa: {
        available: false
      },
      dealLock: {
        blocked: false
      }
    },
    {
      readerTestMode: true,
      readerDebugMode: false
    }
  );

  assert.equal(result.accepted, true);
  assert.equal(result.decision, 'test_group');
});

test('FBM ohne brauchbares Haendlerprofil wird fuer Live auf REVIEW gesetzt', () => {
  const result = __testablesTelegramUserClient.resolveFbmSellerProfileReviewBlock({
    generatorInput: {
      sellerClass: 'FBM_THIRDPARTY',
      sellerProfile: {
        status: 'blocked',
        positivePercent: 79,
        periodMonths: 12,
        profileOk: false,
        reason: 'FBM-Haendlerprofil blockiert: nur 79% positive Bewertungen.'
      }
    }
  });

  assert.equal(result.blocked, true);
  assert.match(result.reason, /79%/);
});

test('FBM mit 86 Prozent und 12 Monaten blockiert Live nicht', () => {
  const result = __testablesTelegramUserClient.resolveFbmSellerProfileReviewBlock({
    generatorInput: {
      sellerClass: 'FBM_THIRDPARTY',
      sellerProfile: {
        status: 'ok',
        positivePercent: 86,
        periodMonths: 12,
        profileOk: true
      }
    }
  });

  assert.equal(result.blocked, false);
});

console.log('OK Reader-Pipeline-Entscheidungstests bestanden');
