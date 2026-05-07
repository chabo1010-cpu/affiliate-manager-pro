import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'affiliate-approved-coupon-guard-'));
process.env.APP_DATA_DIR = tempRoot;
process.env.APP_DB_PATH = path.join(tempRoot, 'deals.db');
process.env.TELEGRAM_BOT_TOKEN = 'test-token';
process.env.TELEGRAM_CHAT_ID = '-100TESTGROUP';
process.env.TELEGRAM_TEST_CHAT_ID = '-100TESTGROUP';
process.env.TELEGRAM_APPROVED_CHANNEL_ENABLED = '1';
process.env.TELEGRAM_APPROVED_CHANNEL_USERNAME = '@codeundcouponing';

const { publishGeneratorPostDirect } = await import('../services/directPublisher.js');

const originalFetch = global.fetch;
const requests = [];
let messageId = 0;

global.fetch = async (url, options = {}) => {
  const urlText = String(url || '');
  let chatId = '';
  const body = options?.body;

  if (body && typeof body.get === 'function') {
    chatId = String(body.get('chat_id') || '');
  } else if (typeof body === 'string' && body.trim()) {
    try {
      const parsed = JSON.parse(body);
      chatId = String(parsed?.chat_id || '');
    } catch {
      const match = body.match(/chat_id=([^&]+)/);
      chatId = match?.[1] ? decodeURIComponent(match[1]) : '';
    }
  }

  requests.push({
    url: urlText,
    chatId
  });

  if (urlText.includes('/getChat?')) {
    return {
      ok: true,
      json: async () => ({
        ok: true,
        result: {
          id: '-100LIVEAPPROVED'
        }
      }),
      text: async () =>
        JSON.stringify({
          ok: true,
          result: {
            id: '-100LIVEAPPROVED'
          }
        })
    };
  }

  const result = {
    ok: true,
    result: {
      message_id: ++messageId,
      chat: {
        id: chatId || '-100TESTGROUP'
      }
    }
  };

  return {
    ok: true,
    json: async () => result,
    text: async () => JSON.stringify(result)
  };
};

try {
  const result = await publishGeneratorPostDirect({
    title: 'Approved Guard Test',
    link: 'https://www.amazon.de/dp/B000COUPON1',
    normalizedUrl: 'https://www.amazon.de/dp/B000COUPON1',
    asin: 'B000COUPON1',
    sellerType: 'AMAZON',
    sellerClass: 'AMAZON_DIRECT',
    currentPrice: '19.99',
    oldPrice: '29.99',
    couponCode: 'SAVE10',
    textByChannel: {
      telegram: '<b>Approved Guard Test</b>\n\n🔥 Jetzt <b>19,99€</b>\n➡️ <b>https://www.amazon.de/dp/B000COUPON1</b>\n\n\n<i>Anzeige/Partnerlink</i>'
    },
    generatorContext: {
      learning: {
        routingDecision: 'approve',
        wouldPostNormally: true,
        canReachTestGroup: true
      },
      evaluation: {
        decision: 'APPROVE',
        testGroupApproved: true
      }
    },
    telegramImageSource: 'none',
    enableTelegram: true,
    enableWhatsapp: false,
    enableFacebook: false,
    queueSourceType: 'generator_direct',
    originOverride: 'automatic',
    skipDealLock: true
  });

  const sendChatIds = requests
    .filter((entry) => /\/send(?:Message|Photo)/.test(entry.url))
    .map((entry) => entry.chatId)
    .filter(Boolean);

  assert.equal(result.routingOutputs?.approved?.status, 'skipped');
  assert.equal(result.routingOutputs?.approved?.couponCodeMessageId || null, null);
  assert.ok(sendChatIds.includes('-100TESTGROUP'));
  assert.ok(!sendChatIds.includes('-100LIVEAPPROVED'));

  console.log('PASS Deaktivierte Approved Route sendet keinen Coupon-Follow-up');
} finally {
  global.fetch = originalFetch;
}

console.log('OK Approved Route Coupon Guard getestet');
