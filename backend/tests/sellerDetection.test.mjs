import assert from 'node:assert/strict';

const { extractSellerSignalsFromText, resolveSellerIdentity, evaluateSellerDecisionPolicy } = await import('../services/sellerClassificationService.js');
const { extractSellerInfoFromAmazonHtml, extractFbmSellerProfileFromHtml } = await import('../routes/amazon.js');

function test(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (error) {
    console.error(`FAIL ${name}`);
    throw error;
  }
}

test('Direkter Amazon-Text setzt Amazon Direct sofort', () => {
  const result = extractSellerSignalsFromText('Verkauf und Versand durch Amazon.');
  assert.equal(result.soldByAmazon, true);
  assert.equal(result.shippedByAmazon, true);
  assert.equal(result.hasAmazonDirectPhrase, true);
});

test('Mobile Combined Text "Versender / Verkaeufer Amazon" ergibt AMAZON_DIRECT', () => {
  const signals = extractSellerSignalsFromText('Versender / Verkaeufer Amazon', {
    detectionSource: 'buybox'
  });
  const sellerIdentity = resolveSellerIdentity({
    soldByAmazon: signals.soldByAmazon,
    shippedByAmazon: signals.shippedByAmazon,
    sellerDetectionSource: signals.detectionSource,
    detectionSources: [signals.rawDetectionSource],
    matchedPatterns: signals.matchedPatterns,
    matchedDirectAmazonPatterns: signals.matchedDirectAmazonPatterns,
    hasCombinedAmazonMatch: signals.hasCombinedAmazonMatch,
    sellerDetails: {
      detectionSource: signals.detectionSource,
      detectionSources: [signals.rawDetectionSource],
      merchantText: signals.merchantText,
      matchedPatterns: signals.matchedPatterns,
      matchedDirectAmazonPatterns: signals.matchedDirectAmazonPatterns,
      hasCombinedAmazonMatch: signals.hasCombinedAmazonMatch
    }
  });

  assert.equal(signals.soldByAmazon, true);
  assert.equal(signals.shippedByAmazon, true);
  assert.equal(signals.detectionSource, 'combined-seller-shipping-text');
  assert.equal(sellerIdentity.sellerClass, 'AMAZON_DIRECT');
});

test('Text "Verkaeufer Haendler XY Versand durch Amazon" ergibt FBA_THIRDPARTY', () => {
  const signals = extractSellerSignalsFromText('Verkaeufer Haendler XY Versand durch Amazon', {
    detectionSource: 'buybox'
  });
  const sellerIdentity = resolveSellerIdentity({
    soldByAmazon: signals.soldByAmazon,
    shippedByAmazon: signals.shippedByAmazon
  });

  assert.equal(signals.soldByAmazon, false);
  assert.equal(signals.shippedByAmazon, true);
  assert.equal(sellerIdentity.sellerClass, 'FBA_THIRDPARTY');
});

test('Text "Verkaeufer Haendler XY Versand durch Haendler XY" ergibt FBM_THIRDPARTY', () => {
  const signals = extractSellerSignalsFromText('Verkaeufer Haendler XY Versand durch Haendler XY', {
    detectionSource: 'buybox'
  });
  const sellerIdentity = resolveSellerIdentity({
    soldByAmazon: signals.soldByAmazon,
    shippedByAmazon: signals.shippedByAmazon
  });

  assert.equal(signals.soldByAmazon, false);
  assert.equal(signals.shippedByAmazon, false);
  assert.equal(sellerIdentity.sellerClass, 'FBM_THIRDPARTY');
});

test('Explizites UNKNOWN ueberschreibt bestaetigte Amazon-Flags nicht mehr', () => {
  const sellerIdentity = resolveSellerIdentity({
    sellerClass: 'UNKNOWN',
    soldByAmazon: true,
    shippedByAmazon: true,
    sellerDetectionSource: 'combined-seller-shipping-text'
  });

  assert.equal(sellerIdentity.sellerClass, 'AMAZON_DIRECT');
  assert.equal(sellerIdentity.soldByAmazon, true);
  assert.equal(sellerIdentity.shippedByAmazon, true);
});

test('Amazon HTML kombiniert seller-profile und buybox zu Amazon Direct', () => {
  const sellerInfo = extractSellerInfoFromAmazonHtml(`
    <html>
      <body>
        <a id="sellerProfileTriggerId">Amazon.de</a>
        <div id="shipsFromSoldByInsideBuyBox_feature_div">
          <span>Versendet von Amazon</span>
        </div>
      </body>
    </html>
  `);

  assert.equal(sellerInfo.sellerClass, 'AMAZON_DIRECT');
  assert.equal(sellerInfo.soldByAmazon, true);
  assert.equal(sellerInfo.shippedByAmazon, true);
  assert.ok((sellerInfo.sellerDetails?.detectionSource || '').includes('seller-profile'));
  assert.ok((sellerInfo.sellerDetails?.detectionSource || '').includes('buybox'));
});

test('Tabular Buybox erkennt FBA-Drittanbieter getrennt von Amazon Direct', () => {
  const sellerInfo = extractSellerInfoFromAmazonHtml(`
    <html>
      <body>
        <div id="tabular-buybox">
          Verkauf durch Beispiel Shop
          Versand durch Amazon
        </div>
      </body>
    </html>
  `);

  assert.equal(sellerInfo.sellerClass, 'FBA_THIRDPARTY');
  assert.equal(sellerInfo.soldByAmazon, false);
  assert.equal(sellerInfo.shippedByAmazon, true);
  assert.equal(sellerInfo.sellerDetails?.detectionSource, 'tabular-buybox');
});

test('FBM-Haendlerprofil mit 86 Prozent und 12 Monaten wird freigegeben', () => {
  const sellerProfile = extractFbmSellerProfileFromHtml(`
    <html>
      <body>
        <h1>Beispiel Shop</h1>
        <div>86 % positive Bewertungen in den letzten 12 Monaten</div>
      </body>
    </html>
  `);

  assert.equal(sellerProfile.positivePercent, 86);
  assert.equal(sellerProfile.periodMonths, 12);
  assert.equal(sellerProfile.profileOk, true);
  assert.equal(sellerProfile.fbmAllowed, true);
});

test('FBM-Haendlerprofil unter 80 Prozent bleibt blockiert', () => {
  const sellerProfile = extractFbmSellerProfileFromHtml(`
    <html>
      <body>
        <div>79 % positive Bewertungen in den letzten 12 Monaten</div>
      </body>
    </html>
  `);

  assert.equal(sellerProfile.positivePercent, 79);
  assert.equal(sellerProfile.periodMonths, 12);
  assert.equal(sellerProfile.profileOk, false);
});

test('FBM-Haendlerprofil unter 12 Monaten bleibt blockiert', () => {
  const sellerProfile = extractFbmSellerProfileFromHtml(`
    <html>
      <body>
        <div>90 % positive Bewertungen in den letzten 6 Monaten</div>
      </body>
    </html>
  `);

  assert.equal(sellerProfile.positivePercent, 90);
  assert.equal(sellerProfile.periodMonths, 6);
  assert.equal(sellerProfile.profileOk, false);
});

test('Non-Amazon UNKNOWN blockiert Marktvergleich und KI nicht', () => {
  const policy = evaluateSellerDecisionPolicy(
    {},
    {
      sellerType: 'UNKNOWN',
      sellerClass: 'UNKNOWN',
      dealType: 'NON_AMAZON',
      isAmazonDeal: false
    }
  );

  assert.equal(policy.seller.isNonAmazonDeal, true);
  assert.equal(policy.marketComparison.allowed, true);
  assert.equal(policy.ai.allowed, true);
  assert.equal(policy.unknownSellerAction, 'pass');
});

console.log('OK Seller-Erkennungstests bestanden');
