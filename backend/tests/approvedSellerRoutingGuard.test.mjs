import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'affiliate-approved-seller-guard-'));
process.env.APP_DATA_DIR = tempRoot;
process.env.APP_DB_PATH = path.join(tempRoot, 'deals.db');
process.env.READER_TEST_MODE = '1';

const { __testablesDirectPublisher } = await import('../services/directPublisher.js');

async function test(name, fn) {
  try {
    await fn();
    console.log(`PASS ${name}`);
  } catch (error) {
    console.error(`FAIL ${name}`);
    throw error;
  }
}

await test('FBM wird im finalen Routing immer hart geblockt', async () => {
  const routingState = __testablesDirectPublisher.buildFinalRoutingInputState(
    {
      asin: 'B0KUIYUE01',
      sellerClass: 'FBM_THIRDPARTY',
      sellerType: 'FBM',
      amazonMerchantName: 'KUIYUE',
      decision: 'APPROVE',
      routingDecision: 'approve',
      wouldPostNormally: true
    },
    {
      learning: {
        routingDecision: 'approve',
        wouldPostNormally: true,
        canReachTestGroup: true
      },
      evaluation: {
        decision: 'APPROVE',
        testGroupApproved: true
      }
    }
  );
  const decision = __testablesDirectPublisher.resolveTelegramRoutingDecision(routingState.input, routingState.generatorContext);

  assert.equal(routingState.hardBlockedFbm, true);
  assert.equal(routingState.input.decision, 'BLOCK');
  assert.equal(routingState.input.routingDecision, 'block');
  assert.equal(routingState.input.wouldPostNormally, false);
  assert.equal(routingState.input.reasonCode, 'FBM_NOT_ALLOWED');
  assert.equal(routingState.generatorContext.learning.routingDecision, 'block');
  assert.equal(routingState.generatorContext.learning.wouldPostNormally, false);
  assert.equal(decision.bucket, 'rejected');
  assert.equal(decision.label, 'BLOCK');
});

await test('FBA_OR_AMAZON_UNKNOWN darf trotz Approve-Signal nicht in Veroeffentlicht landen', async () => {
  const routingState = __testablesDirectPublisher.buildFinalRoutingInputState(
    {
      asin: 'B0UNKNOWN01',
      sellerClass: 'FBA_OR_AMAZON_UNKNOWN',
      sellerType: 'UNKNOWN',
      title: 'Testprodukt',
      link: 'https://www.amazon.de/dp/B0UNKNOWN01',
      currentPrice: '19,99€',
      decision: 'APPROVE',
      routingDecision: 'approve',
      wouldPostNormally: true
    },
    {
      learning: {
        routingDecision: 'approve',
        wouldPostNormally: true,
        canReachTestGroup: true
      },
      evaluation: {
        decision: 'APPROVE',
        testGroupApproved: true
      }
    }
  );
  const decision = __testablesDirectPublisher.resolveTelegramRoutingDecision(routingState.input, routingState.generatorContext);

  assert.equal(__testablesDirectPublisher.isApprovedFinalRoutingSellerAllowed(routingState.input, routingState.generatorContext), false);
  assert.equal(routingState.hardBlockedFbm, false);
  assert.equal(routingState.input.routingDecision, 'review');
  assert.equal(routingState.input.wouldPostNormally, false);
  assert.equal(decision.bucket, 'rejected');
  assert.equal(decision.label, 'REVIEW');
});

await test('FBA_UNKNOWN mit Legacy sellerType FBA bleibt fuer Veroeffentlicht blockiert', async () => {
  const routingState = __testablesDirectPublisher.buildFinalRoutingInputState(
    {
      asin: 'B0UNKNOWN02',
      sellerClass: 'FBA_UNKNOWN',
      sellerType: 'FBA',
      title: 'Testprodukt',
      link: 'https://www.amazon.de/dp/B0UNKNOWN02',
      currentPrice: '18,99â‚¬',
      decision: 'APPROVE',
      routingDecision: 'approve',
      wouldPostNormally: true
    },
    {
      learning: {
        routingDecision: 'approve',
        wouldPostNormally: true,
        canReachTestGroup: true
      },
      evaluation: {
        decision: 'APPROVE',
        testGroupApproved: true
      }
    }
  );
  const decision = __testablesDirectPublisher.resolveTelegramRoutingDecision(routingState.input, routingState.generatorContext);

  assert.equal(__testablesDirectPublisher.isApprovedFinalRoutingSellerAllowed(routingState.input, routingState.generatorContext), false);
  assert.equal(routingState.input.routingDecision, 'review');
  assert.equal(routingState.input.wouldPostNormally, false);
  assert.equal(decision.bucket, 'rejected');
  assert.equal(decision.label, 'REVIEW');
});

await test('AMAZON_DIRECT bleibt fuer Veroeffentlicht erlaubt', async () => {
  const routingState = __testablesDirectPublisher.buildFinalRoutingInputState(
    {
      asin: 'B0AMAZON01',
      sellerClass: 'AMAZON_DIRECT',
      sellerType: 'AMAZON',
      title: 'Testprodukt',
      link: 'https://www.amazon.de/dp/B0AMAZON01',
      currentPrice: '19,99€'
    },
    {
      learning: {
        routingDecision: 'approve'
      },
      evaluation: {
        decision: 'APPROVE',
        testGroupApproved: true
      }
    }
  );
  const decision = __testablesDirectPublisher.resolveTelegramRoutingDecision(routingState.input, routingState.generatorContext);

  assert.equal(__testablesDirectPublisher.isApprovedFinalRoutingSellerAllowed(routingState.input, routingState.generatorContext), true);
  assert.equal(decision.bucket, 'approved');
  assert.equal(decision.label, 'APPROVE');
});

await test('AMAZON_DIRECT Approve-Signal gewinnt gegen altes manual_review im finalen Routing', async () => {
  const routingState = __testablesDirectPublisher.buildFinalRoutingInputState(
    {
      asin: 'B0AMAZON02',
      sellerClass: 'AMAZON_DIRECT',
      sellerType: 'AMAZON',
      title: 'Testprodukt',
      link: 'https://www.amazon.de/dp/B0AMAZON02',
      currentPrice: '19,99â‚¬',
      decision: 'manual_review'
    },
    {
      learning: {
        routingDecision: 'approve',
        wouldPostNormally: true,
        canReachTestGroup: true,
        reason: 'Seller und Deal sind freigegeben.'
      },
      evaluation: {
        decision: 'manual_review',
        decisionLabel: 'Manuelle Pruefung',
        testGroupApproved: true
      }
    }
  );
  const decision = __testablesDirectPublisher.resolveTelegramRoutingDecision(routingState.input, routingState.generatorContext);

  assert.equal(__testablesDirectPublisher.isApprovedFinalRoutingSellerAllowed(routingState.input, routingState.generatorContext), true);
  assert.equal(routingState.input.decision, 'APPROVE');
  assert.equal(routingState.input.routingDecision, 'approve');
  assert.equal(routingState.input.wouldPostNormally, true);
  assert.equal(routingState.generatorContext.learning.routingDecision, 'approve');
  assert.equal(routingState.generatorContext.evaluation.decision, 'APPROVE');
  assert.equal(decision.bucket, 'approved');
  assert.equal(decision.label, 'APPROVE');
});

await test('Produkt-Regel blockiert NoName Kopfhoerer fuer Veroeffentlicht', async () => {
  const routingState = __testablesDirectPublisher.buildFinalRoutingInputState(
    {
      asin: 'B0HEAD26590',
      sellerClass: 'AMAZON_DIRECT',
      sellerType: 'AMAZON',
      title: 'A1 Bluetooth Kopfhoerer In Ear TWS Schwarz',
      brand: 'A1',
      currentPrice: '26.59',
      rating: 4.2,
      reviewCount: 100,
      link: 'https://www.amazon.de/dp/B0HEAD26590',
      decision: 'APPROVE',
      routingDecision: 'approve',
      wouldPostNormally: true
    },
    {
      learning: {
        routingDecision: 'approve',
        wouldPostNormally: true,
        canReachTestGroup: true,
        marketComparisonStatus: 'success',
        marketComparisonUsed: true
      },
      evaluation: {
        decision: 'APPROVE',
        testGroupApproved: true
      }
    }
  );
  const decision = __testablesDirectPublisher.resolveTelegramRoutingDecision(routingState.input, routingState.generatorContext);

  assert.equal(routingState.input.reasonCode, 'PRODUCT_RULE_BLOCKED');
  assert.equal(routingState.input.routingDecision, 'block');
  assert.equal(routingState.input.wouldPostNormally, false);
  assert.equal(routingState.input.productRuleEvaluation?.matchedRuleName, 'China Kopfhoerer');
  assert.equal(decision.bucket, 'rejected');
  assert.equal(decision.label, 'BLOCK');
});

await test('Geblockt-Post behaelt den Generator-Hauptpost oben und haengt den Kurzcheck darunter an', async () => {
  const combinedPost = __testablesDirectPublisher.buildRejectedCombinedPost({
    generatorPostText: '<b>Anker Powerbank 10000mAh</b>\n\n🔥 Jetzt <b>9,99€</b>\n➡️ <b>https://www.amazon.de/dp/B012345678</b>\n\n\n<i>Anzeige/Partnerlink</i>',
    shortcheckText: '📊 KURZCHECK\n📌 Grund: FBM_NOT_ALLOWED'
  });

  assert.equal(combinedPost.startsWith('<b>Anker Powerbank 10000mAh</b>'), true);
  assert.equal(combinedPost.includes('⚠️ NICHT VERÖFFENTLICHT'), true);
  assert.equal(combinedPost.endsWith('📌 Grund: FBM_NOT_ALLOWED'), true);
});
