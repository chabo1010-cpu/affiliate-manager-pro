import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'affiliate-approved-route-classification-'));
process.env.APP_DATA_DIR = tempRoot;
process.env.APP_DB_PATH = path.join(tempRoot, 'deals.db');
process.env.TELEGRAM_BOT_TOKEN = 'test-token';
process.env.TELEGRAM_CHAT_ID = '-100TESTGROUP';
process.env.TELEGRAM_TEST_CHAT_ID = '-100TESTGROUP';
process.env.TELEGRAM_APPROVED_CHANNEL_ENABLED = '1';
process.env.TELEGRAM_APPROVED_CHANNEL_USERNAME = '@veroeffentlicht';
process.env.TELEGRAM_REJECTED_CHANNEL_ENABLED = '1';
process.env.TELEGRAM_REJECTED_CHANNEL_USERNAME = '@geblockt1';

const { getDb } = await import('../db.js');
const { listOutputChannelsSnapshot } = await import('../services/outputChannelService.js');

const db = getDb();

function getApprovedChannel() {
  return (
    listOutputChannelsSnapshot().channels.find((item) => item.channelKey === 'telegram:approved-route:@veroeffentlicht') || null
  );
}

async function test(name, fn) {
  try {
    await fn();
    console.log(`PASS ${name}`);
  } catch (error) {
    console.error(`FAIL ${name}`);
    throw error;
  }
}

await test('Approved Route @veroeffentlicht wird nicht als Live-Hauptkanal seeded', async () => {
  const channel = getApprovedChannel();

  assert.ok(channel);
  assert.equal(channel.channelType, 'standard');
  assert.equal(channel.isEnabled, true);
  assert.equal(channel.allowTestMode, true);
  assert.equal(channel.allowLiveMode, true);
  assert.equal(channel.isDangerousLive, false);
  assert.equal(channel.warningText, '');
});

await test('Legacy Approved Route Migration aktiviert @veroeffentlicht wieder nach alter Live-Fehlklassifizierung', async () => {
  db.prepare(
    `
      UPDATE output_channels
      SET channel_type = 'live',
          is_enabled = 0,
          allow_test_mode = 0,
          allow_live_mode = 1,
          is_dangerous_live = 1,
          meta_json = NULL,
          updated_at = CURRENT_TIMESTAMP
      WHERE channel_key = 'telegram:approved-route:@veroeffentlicht'
    `
  ).run();

  const channel = getApprovedChannel();

  assert.ok(channel);
  assert.equal(channel.channelType, 'standard');
  assert.equal(channel.isEnabled, true);
  assert.equal(channel.allowTestMode, true);
  assert.equal(channel.isDangerousLive, false);
  assert.equal(channel.warningText, '');
});

console.log('OK Approved Route Classification getestet');
