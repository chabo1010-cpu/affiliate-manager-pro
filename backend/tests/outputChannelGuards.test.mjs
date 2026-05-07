import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'affiliate-output-guards-'));
process.env.APP_DATA_DIR = tempRoot;
process.env.APP_DB_PATH = path.join(tempRoot, 'deals.db');
process.env.TELEGRAM_BOT_TOKEN = 'test-token';
process.env.TELEGRAM_CHAT_ID = '-100TESTGROUP';
process.env.TELEGRAM_TEST_CHAT_ID = '-100TESTGROUP';
process.env.TELEGRAM_APPROVED_CHANNEL_ENABLED = '1';
process.env.TELEGRAM_APPROVED_CHANNEL_USERNAME = '@codeundcouponing';
process.env.TELEGRAM_REJECTED_CHANNEL_ENABLED = '1';
process.env.TELEGRAM_REJECTED_CHANNEL_USERNAME = '@geblockt1';

const { createPublishingEntry, listPublishingLogs, processPublishingQueueEntry } = await import('../services/publisherService.js');
const { getTelegramBotClientConfig } = await import('../services/telegramBotClientService.js');
const { listOutputChannelsSnapshot } = await import('../services/outputChannelService.js');

async function test(name, fn) {
  try {
    await fn();
    console.log(`PASS ${name}`);
  } catch (error) {
    console.error(`FAIL ${name}`);
    throw error;
  }
}

await test('Testgruppe bleibt aktiv und kann ueber den Publisher senden', async () => {
  let processorCalls = 0;
  getTelegramBotClientConfig();

  const queue = createPublishingEntry({
    sourceType: 'output_channel_test',
    payload: {
      title: 'Testgruppe OK',
      link: '',
      normalizedUrl: '',
      asin: '',
      sellerType: 'FBM',
      telegramChatIds: ['-100TESTGROUP'],
      testMode: true,
      skipDealLock: true,
      skipPostedDealHistory: true,
      textByChannel: {
        telegram: 'Testgruppe aktiv'
      },
      imageVariants: {},
      targetImageSources: {
        telegram: 'none'
      }
    },
    targets: [{ channelType: 'telegram', isEnabled: true, imageSource: 'none' }],
    originOverride: 'manual',
    skipDealLock: true
  });

  const result = await processPublishingQueueEntry(queue.id, {
    processors: {
      telegram: async () => {
        processorCalls += 1;
        return {
          status: 'sent',
          targets: [{ messageId: 'msg-test', targetChatId: '-100TESTGROUP' }]
        };
      }
    }
  });

  assert.equal(processorCalls, 1);
  assert.equal(result.results[0]?.status, 'sent');
});

await test('LIVE Kanal @codeundcouponing ist vorhanden und standardmaessig deaktiviert', async () => {
  getTelegramBotClientConfig();
  const snapshot = listOutputChannelsSnapshot();
  const liveChannel = snapshot.channels.find((item) => item.targetRef === '@codeundcouponing');

  assert.ok(liveChannel);
  assert.equal(liveChannel.channelType, 'live');
  assert.equal(liveChannel.isEnabled, false);
  assert.equal(liveChannel.warningText, 'LIVE KANAL Deaktiviert Erst manuell aktivieren');
});

await test('Deaktivierter Live Kanal wird mit OUTPUT_DISABLED_SKIP uebersprungen', async () => {
  let processorCalls = 0;
  const queue = createPublishingEntry({
    sourceType: 'generator_direct_approved_route',
    payload: {
      title: 'Live Kanal gesperrt',
      link: '',
      normalizedUrl: '',
      asin: '',
      sellerType: 'AMAZON',
      telegramChatIds: ['@codeundcouponing'],
      testMode: false,
      skipDealLock: true,
      skipPostedDealHistory: true,
      textByChannel: {
        telegram: 'Darf nicht live senden'
      },
      imageVariants: {},
      targetImageSources: {
        telegram: 'none'
      }
    },
    targets: [{ channelType: 'telegram', isEnabled: true, imageSource: 'none' }],
    originOverride: 'manual',
    skipDealLock: true
  });

  const result = await processPublishingQueueEntry(queue.id, {
    processors: {
      telegram: async () => {
        processorCalls += 1;
        return {
          status: 'sent'
        };
      }
    }
  });

  const latestSkipLog = listPublishingLogs({ limit: 20 }).find((item) => item.event_type === 'output.disabled.skip');

  assert.equal(processorCalls, 0);
  assert.equal(result.results[0]?.status, 'skipped');
  assert.ok(result.results[0]?.reason?.includes('OUTPUT_DISABLED_SKIP'));
  assert.ok(latestSkipLog?.message?.includes('OUTPUT_DISABLED_SKIP'));
});

await test('Approved Route mit expliziter Chat-ID bleibt am Live-Guard haengen und faellt nicht auf target_missing', async () => {
  let processorCalls = 0;
  const queue = createPublishingEntry({
    sourceType: 'generator_direct_approved_route',
    payload: {
      title: 'Approved Route Guard',
      link: '',
      normalizedUrl: '',
      asin: '',
      sellerType: 'AMAZON',
      telegramChatIds: ['-100APPROVEDCHAT'],
      testMode: false,
      skipDealLock: true,
      skipPostedDealHistory: true,
      textByChannel: {
        telegram: 'Approved Route darf live nicht senden'
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
        targetRef: '-100APPROVEDCHAT',
        targetLabel: '@codeundcouponing',
        targetMeta: {
          allowAdHoc: true,
          routeKey: 'approved',
          outputChannelKey: 'telegram:approved-route:@codeundcouponing',
          channelKind: 'live',
          targetKind: 'live',
          isLive: true
        }
      }
    ],
    originOverride: 'manual',
    skipDealLock: true
  });

  const result = await processPublishingQueueEntry(queue.id, {
    processors: {
      telegram: async () => {
        processorCalls += 1;
        return {
          status: 'sent'
        };
      }
    }
  });

  assert.equal(processorCalls, 0);
  assert.equal(result.results[0]?.status, 'skipped');
  assert.ok(result.results[0]?.reason?.includes('OUTPUT_DISABLED_SKIP'));
  assert.ok(!result.results[0]?.reason?.includes('Ziel wurde aus der Output-Konfiguration entfernt.'));
  assert.ok(result.results[0]?.reason?.includes('@codeundcouponing'));
});

console.log('OK Output Channel Guards getestet');
