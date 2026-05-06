import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'affiliate-telegram-duplicate-'));
process.env.APP_DATA_DIR = tempRoot;
process.env.APP_DB_PATH = path.join(tempRoot, 'deals.db');
process.env.TELEGRAM_BOT_TOKEN = 'test-token';
process.env.TELEGRAM_CHAT_ID = '-1001234567890';
process.env.DUPLICATE_WINDOW_HOURS = '24';

const { getDb } = await import('../db.js');
const { sendTelegramPost } = await import('../services/telegramSenderService.js');
const { __testablesTelegramDuplicateGuard } = await import('../services/telegramDuplicateGuardService.js');

const db = getDb();

async function test(name, fn) {
  try {
    resetDuplicateState();
    await fn();
    console.log(`PASS ${name}`);
  } catch (error) {
    console.error(`FAIL ${name}`);
    throw error;
  }
}

function resetDuplicateState() {
  db.prepare(`DELETE FROM telegram_post_duplicates`).run();
  __testablesTelegramDuplicateGuard.pendingDuplicateReservations.clear();
}

function buildDuplicateContext(overrides = {}) {
  return {
    channelType: 'approved',
    targetRef: '-1003938688500',
    asin: 'B07PYNBYMY',
    title: 'STANLEY Adventure Vacuum Stein Thermobecher',
    price: '21,00€',
    url: 'https://www.amazon.de/dp/B07PYNBYMY?tag=abc',
    ...overrides
  };
}

function buildPostInput(overrides = {}) {
  const duplicateContext = buildDuplicateContext(overrides.duplicateContext || {});

  return {
    text: '<b>STANLEY Adventure Vacuum Stein Thermobecher</b>\n\n🔥 Jetzt <b>21,00€</b>\n➡️ <b>https://www.amazon.de/dp/B07PYNBYMY?tag=abc</b>\n\n\n<i>Anzeige/Partnerlink</i>',
    chatId: duplicateContext.targetRef,
    disableWebPagePreview: true,
    titlePreview: duplicateContext.title,
    hasAffiliateLink: true,
    postContext: 'deal_main_text_only',
    duplicateContext,
    ...overrides
  };
}

await test('gleiche ASIN und gleicher Preis werden im selben Kanal innerhalb von 24 Stunden blockiert', async () => {
  const requests = [];
  const originalFetch = global.fetch;

  global.fetch = async (url, init = {}) => {
    requests.push({ url: String(url), init });
    return {
      ok: true,
      text: async () =>
        JSON.stringify({
          ok: true,
          result: {
            message_id: requests.length,
            chat: {
              id: '-1003938688500'
            }
          }
        })
    };
  };

  try {
    const firstResult = await sendTelegramPost(buildPostInput());
    const secondResult = await sendTelegramPost(
      buildPostInput({
        duplicateContext: buildDuplicateContext({
          url: 'https://www.amazon.de/dp/B07PYNBYMY?tag=xyz'
        })
      })
    );

    assert.equal(requests.length, 1);
    assert.equal(firstResult.messageId, 1);
    assert.equal(secondResult.duplicateBlocked, true);
    assert.equal(secondResult.messageId, null);
    assert.equal(secondResult.duplicateReason, 'DUPLICATE_WINDOW_ACTIVE');

    const row = db
      .prepare(`SELECT duplicate_key, normalized_url, normalized_price, last_sent_at FROM telegram_post_duplicates WHERE duplicate_key = ?`)
      .get(firstResult.duplicateKey);
    assert.equal(row.normalized_url, 'https://amazon.de/dp/B07PYNBYMY');
    assert.equal(row.normalized_price, '21.00');
    assert.ok(typeof row.last_sent_at === 'string' && row.last_sent_at.length > 0);
  } finally {
    global.fetch = originalFetch;
  }
});

await test('gleiche ASIN und gleicher Preis duerfen in einem anderen Zielkanal gesendet werden', async () => {
  const requests = [];
  const originalFetch = global.fetch;

  global.fetch = async (url, init = {}) => {
    requests.push({ url: String(url), init });
    return {
      ok: true,
      text: async () =>
        JSON.stringify({
          ok: true,
          result: {
            message_id: requests.length,
            chat: {
              id: requests.length === 1 ? '-1003938688500' : '-1001111111111'
            }
          }
        })
    };
  };

  try {
    const firstResult = await sendTelegramPost(buildPostInput());
    const secondResult = await sendTelegramPost(
      buildPostInput({
        chatId: '-1001111111111',
        duplicateContext: buildDuplicateContext({
          targetRef: '-1001111111111'
        })
      })
    );

    assert.equal(requests.length, 2);
    assert.equal(firstResult.messageId, 1);
    assert.equal(secondResult.messageId, 2);
    assert.equal(secondResult.duplicateBlocked, undefined);
  } finally {
    global.fetch = originalFetch;
  }
});

await test('gleiche ASIN darf nach Ablauf des 24-Stunden-Fensters erneut gesendet werden', async () => {
  const requests = [];
  const originalFetch = global.fetch;

  global.fetch = async (url, init = {}) => {
    requests.push({ url: String(url), init });
    return {
      ok: true,
      text: async () =>
        JSON.stringify({
          ok: true,
          result: {
            message_id: requests.length,
            chat: {
              id: '-1003938688500'
            }
          }
        })
    };
  };

  try {
    const firstResult = await sendTelegramPost(buildPostInput());
    const olderThanWindow = new Date(Date.now() - 25 * 60 * 60 * 1000).toISOString();

    db.prepare(`UPDATE telegram_post_duplicates SET last_sent_at = ? WHERE duplicate_key = ?`).run(
      olderThanWindow,
      firstResult.duplicateKey
    );

    const secondResult = await sendTelegramPost(buildPostInput());

    assert.equal(requests.length, 2);
    assert.equal(secondResult.messageId, 2);
    assert.equal(secondResult.duplicateBlocked, undefined);
  } finally {
    global.fetch = originalFetch;
  }
});

console.log('OK Telegram Duplicate Guard getestet');
