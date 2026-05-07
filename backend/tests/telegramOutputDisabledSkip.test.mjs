import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'affiliate-output-guards-'));
process.env.APP_DATA_DIR = tempRoot;
process.env.APP_DB_PATH = path.join(tempRoot, 'deals.db');
process.env.TELEGRAM_BOT_TOKEN = 'test-token';
process.env.TELEGRAM_CHAT_ID = '-100111';
process.env.COPYBOT_ENABLED = '1';

const { getDb } = await import('../db.js');
const { getTelegramBotClientConfig } = await import('../services/telegramBotClientService.js');
const { createPublishingEntry, processPublishingQueueEntry, listPublishingLogs } = await import('../services/publisherService.js');

const db = getDb();
db.prepare(`UPDATE app_settings SET copybotEnabled = 1 WHERE id = 1`).run();

function test(name, fn) {
  Promise.resolve()
    .then(fn)
    .then(() => {
      console.log(`PASS ${name}`);
    })
    .catch((error) => {
      console.error(`FAIL ${name}`);
      throw error;
    });
}

await (async () => {
  const config = getTelegramBotClientConfig();
  const liveTarget = config.targets.find((target) => target.chatId === '@codeundcouponing');

  assert.ok(liveTarget, 'Live Kanal @codeundcouponing muss vorhanden sein.');
  assert.equal(liveTarget.isActive, false);

  let processorCalled = false;
  const queue = createPublishingEntry({
    sourceType: 'copybot',
    sourceId: 42,
    payload: {
      title: 'Disabled Output Guard',
      link: 'https://example.com/deal',
      normalizedUrl: 'https://example.com/deal',
      asin: 'B000TEST42',
      currentPrice: '19.99',
      sellerType: 'FBM',
      telegramChatIds: [liveTarget.chatId],
      textByChannel: {
        telegram: 'Skip test'
      },
      imageVariants: {},
      targetImageSources: {
        telegram: 'none'
      }
    },
    targets: [
      {
        channelType: 'telegram',
        isEnabled: true,
        imageSource: 'none',
        targetRef: liveTarget.chatId,
        targetLabel: liveTarget.name,
        targetMeta: {
          targetId: liveTarget.id,
          name: liveTarget.name,
          chatId: liveTarget.chatId,
          channelKind: liveTarget.targetKind || liveTarget.channelKind || 'live'
        }
      }
    ]
  });

  const result = await processPublishingQueueEntry(queue.id, {
    processors: {
      telegram: async () => {
        processorCalled = true;
        return {
          forced: false
        };
      }
    }
  });

  const refreshedQueue = result.queue;
  const refreshedTarget = refreshedQueue?.targets?.[0];
  const logs = listPublishingLogs({ limit: 20 });

  assert.equal(processorCalled, false, 'Der Telegram-Prozessor darf fuer deaktivierte Live-Kanaele nicht laufen.');
  assert.equal(refreshedTarget?.status, 'skipped');
  assert.ok(
    logs.some((entry) => entry.event_type === 'output.disabled.skip' && String(entry.message || '').includes('OUTPUT_DISABLED_SKIP')),
    'Das Publishing-Log muss OUTPUT_DISABLED_SKIP enthalten.'
  );

  console.log('PASS Deaktivierter Live Kanal wird im Copybot-Flow uebersprungen');
})();

console.log('OK Telegram Output Disabled Skip getestet');
