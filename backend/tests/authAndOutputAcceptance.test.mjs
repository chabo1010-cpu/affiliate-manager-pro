import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import express from 'express';

const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'affiliate-auth-output-'));
const dbPath = path.join(tempRoot, 'deals.db');

process.env.APP_DATA_DIR = tempRoot;
process.env.APP_DB_PATH = dbPath;
process.env.AUTH_BOOTSTRAP_ADMIN_USERNAME = 'admin';
process.env.AUTH_BOOTSTRAP_ADMIN_PASSWORD = 'Admin12345!';
process.env.TELEGRAM_BOT_TOKEN = 'test-token';
process.env.TELEGRAM_CHAT_ID = '-100TESTGROUP';
process.env.TELEGRAM_TEST_CHAT_ID = '-100TESTGROUP';
process.env.COPYBOT_ENABLED = '1';

const { getDb } = await import('../db.js');
const { attachAuthenticatedUser, requireAuthenticatedUser } = await import('../middleware/auth.js');
const authRoutes = (await import('../routes/auth.js')).default;
const { getAuthCookieName } = await import('../services/authService.js');
const {
  createPublishingEntry,
  enqueueCopybotPublishing,
  getPublishingQueueEntry,
  listPublishingLogs,
  processPublishingQueueEntry
} = await import('../services/publisherService.js');
const { getTelegramBotClientConfig } = await import('../services/telegramBotClientService.js');

const db = getDb();

function buildQueuePayload(overrides = {}) {
  return {
    title: overrides.title || 'Acceptance Test Deal',
    link: overrides.link || 'https://www.amazon.de/dp/B000TEST01',
    normalizedUrl: overrides.normalizedUrl || overrides.link || 'https://www.amazon.de/dp/B000TEST01',
    asin: overrides.asin || 'B000TEST01',
    sellerType: overrides.sellerType || 'AMAZON',
    currentPrice: overrides.currentPrice || '39.99',
    oldPrice: overrides.oldPrice || '59.99',
    couponCode: overrides.couponCode || '',
    textByChannel: {
      telegram: overrides.telegramText || 'Telegram Acceptance Nachricht'
    },
    imageVariants: {},
    targetImageSources: {
      telegram: 'none'
    },
    skipPostedDealHistory: overrides.skipPostedDealHistory === true
  };
}

async function withMockedTelegramApi(fn) {
  const originalFetch = global.fetch;
  let messageId = 0;

  global.fetch = async () => ({
    ok: true,
    text: async () =>
      JSON.stringify({
        ok: true,
        result: {
          message_id: ++messageId,
          chat: {
            id: '-100TESTGROUP'
          }
        }
      })
  });

  try {
    return await fn();
  } finally {
    global.fetch = originalFetch;
  }
}

function resetState() {
  db.exec(`
    DELETE FROM publishing_logs;
    DELETE FROM publishing_targets;
    DELETE FROM publishing_queue;
    DELETE FROM telegram_bot_targets;
  `);

  getTelegramBotClientConfig();
  db.prepare(`UPDATE app_settings SET copybotEnabled = 1 WHERE id = 1`).run();
}

async function run(name, fn) {
  resetState();
  try {
    await fn();
    console.log(`PASS ${name}`);
    return true;
  } catch (error) {
    console.error(`FAIL ${name}`);
    console.error(error);
    return false;
  }
}

async function startAuthServer() {
  const app = express();
  app.use(express.json());
  app.use(attachAuthenticatedUser);
  app.use('/api/auth', authRoutes);
  app.get('/api/protected', requireAuthenticatedUser, (req, res) => {
    res.json({
      ok: true,
      username: req.auth?.user?.username || ''
    });
  });

  return await new Promise((resolve) => {
    const server = app.listen(0, () => {
      resolve({
        server,
        baseUrl: `http://127.0.0.1:${server.address().port}`
      });
    });
  });
}

function extractCookie(response) {
  const setCookieHeader = response.headers.get('set-cookie') || '';
  return setCookieHeader.split(';')[0] || '';
}

const results = [];

results.push(
  await run('Default Telegram targets include active test group and disabled live channel', async () => {
    const config = getTelegramBotClientConfig();
    const testTarget = config.targets.find((target) => target.chatId === '-100TESTGROUP');
    const liveTarget = config.targets.find((target) => target.chatId === '@codeundcouponing');

    assert.ok(testTarget, 'Testgruppe sollte vorhanden sein.');
    assert.equal(testTarget.isActive, true);
    assert.equal(testTarget.useForPublishing, true);
    assert.equal(testTarget.targetKind, 'test');

    assert.ok(liveTarget, 'Live-Kanal sollte vorhanden sein.');
    assert.equal(liveTarget.isActive, false);
    assert.equal(liveTarget.useForPublishing, true);
    assert.equal(liveTarget.targetKind, 'live');
  })
);

results.push(
  await run('Publishing queue does not send to disabled output channels and logs OUTPUT_DISABLED_SKIP', async () => {
    const entry = createPublishingEntry({
      sourceType: 'generator',
      sourceId: 1,
      payload: buildQueuePayload({
        skipPostedDealHistory: true
      }),
      targets: [{ channelType: 'telegram', isEnabled: true, imageSource: 'none' }]
    });
    const processingResult = await withMockedTelegramApi(async () => processPublishingQueueEntry(entry.id));
    const queue = getPublishingQueueEntry(entry.id);
    const logs = listPublishingLogs({ limit: 20 });
    const telegramResult = processingResult.results.find((item) => item.channelType === 'telegram');
    const deliveredChatIds = telegramResult?.workerResult?.targets?.map((target) => target.targetChatId) || [];
    const skippedChatIds = telegramResult?.workerResult?.skippedTargets?.map((target) => target.targetChatId) || [];

    assert.deepEqual(deliveredChatIds, ['-100TESTGROUP']);
    assert.deepEqual(skippedChatIds, ['@codeundcouponing']);
    assert.equal(queue.status, 'sent');
    assert.equal(queue.targets?.length, 1);
    assert.equal(queue.targets?.[0]?.status, 'sent');
    assert.ok(logs.some((item) => String(item.message || '').includes('OUTPUT_DISABLED_SKIP')));
  })
);

results.push(
  await run('Copybot does not send to disabled output channels', async () => {
    const entry = enqueueCopybotPublishing({
      sourceId: 42,
      payload: buildQueuePayload({
        title: 'Copybot Acceptance Deal',
        asin: 'B000COPY01',
        skipPostedDealHistory: true
      }),
      targets: [{ channelType: 'telegram', isEnabled: true, imageSource: 'none' }]
    });
    const processingResult = await withMockedTelegramApi(async () => processPublishingQueueEntry(entry.id));
    const queue = getPublishingQueueEntry(entry.id);
    const logs = listPublishingLogs({ limit: 20 });
    const telegramResult = processingResult.results.find((item) => item.channelType === 'telegram');
    const deliveredChatIds = telegramResult?.workerResult?.targets?.map((target) => target.targetChatId) || [];
    const skippedChatIds = telegramResult?.workerResult?.skippedTargets?.map((target) => target.targetChatId) || [];

    assert.deepEqual(deliveredChatIds, ['-100TESTGROUP']);
    assert.deepEqual(skippedChatIds, ['@codeundcouponing']);
    assert.equal(queue.status, 'sent');
    assert.equal(queue.targets?.length, 1);
    assert.equal(queue.targets?.[0]?.status, 'sent');
    assert.ok(logs.some((item) => String(item.message || '').includes('OUTPUT_DISABLED_SKIP')));
  })
);

results.push(
  await run('Auth accepts admin login, rejects wrong password, blocks protected routes, and logout revokes the session', async () => {
    const { server, baseUrl } = await startAuthServer();

    try {
      const protectedWithoutLogin = await fetch(`${baseUrl}/api/protected`);
      const protectedWithoutLoginBody = await protectedWithoutLogin.json();
      assert.equal(protectedWithoutLogin.status, 401);
      assert.match(protectedWithoutLoginBody.error, /Login erforderlich/i);

      const sessionBeforeLogin = await fetch(`${baseUrl}/api/auth/session`);
      assert.equal(sessionBeforeLogin.status, 401);

      const loginOk = await fetch(`${baseUrl}/api/auth/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          identifier: 'admin',
          password: 'Admin12345!'
        })
      });
      const loginOkBody = await loginOk.json();
      const sessionCookie = extractCookie(loginOk);
      assert.equal(loginOk.status, 200);
      assert.equal(loginOkBody.user.role, 'admin');
      assert.ok(sessionCookie.startsWith(`${getAuthCookieName()}=`));

      const sessionAfterLogin = await fetch(`${baseUrl}/api/auth/session`, {
        headers: {
          Cookie: sessionCookie
        }
      });
      const sessionAfterLoginBody = await sessionAfterLogin.json();
      assert.equal(sessionAfterLogin.status, 200);
      assert.equal(sessionAfterLoginBody.user.role, 'admin');

      const protectedWithLogin = await fetch(`${baseUrl}/api/protected`, {
        headers: {
          Cookie: sessionCookie
        }
      });
      const protectedWithLoginBody = await protectedWithLogin.json();
      assert.equal(protectedWithLogin.status, 200);
      assert.equal(protectedWithLoginBody.ok, true);

      const loginBad = await fetch(`${baseUrl}/api/auth/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          identifier: 'admin',
          password: 'falsch'
        })
      });
      const loginBadBody = await loginBad.json();
      assert.equal(loginBad.status, 401);
      assert.match(loginBadBody.message, /Ungueltige Zugangsdaten/i);

      const logout = await fetch(`${baseUrl}/api/auth/logout`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Cookie: sessionCookie
        }
      });
      const logoutBody = await logout.json();
      assert.equal(logout.status, 200);
      assert.equal(logoutBody.success, true);

      const sessionAfterLogout = await fetch(`${baseUrl}/api/auth/session`, {
        headers: {
          Cookie: sessionCookie
        }
      });
      assert.equal(sessionAfterLogout.status, 401);

      const protectedAfterLogout = await fetch(`${baseUrl}/api/protected`, {
        headers: {
          Cookie: sessionCookie
        }
      });
      const protectedAfterLogoutBody = await protectedAfterLogout.json();
      assert.equal(protectedAfterLogout.status, 401);
      assert.match(protectedAfterLogoutBody.error, /Login erforderlich/i);
    } finally {
      await new Promise((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
      });
    }
  })
);

const passed = results.filter(Boolean).length;
const failed = results.length - passed;

if (failed > 0) {
  console.error(`FAILURES ${failed}/${results.length}`);
  process.exitCode = 1;
} else {
  console.log(`OK ${passed} auth/output acceptance checks passed`);
}
