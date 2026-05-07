import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'affiliate-approved-whatsapp-mirror-'));
process.env.APP_DATA_DIR = tempRoot;
process.env.APP_DB_PATH = path.join(tempRoot, 'deals.db');
process.env.TELEGRAM_BOT_TOKEN = 'test-token';
process.env.TELEGRAM_CHAT_ID = '-100TESTGROUP';
process.env.TELEGRAM_TEST_CHAT_ID = '-100TESTGROUP';
process.env.TELEGRAM_APPROVED_CHANNEL_ENABLED = '1';
process.env.TELEGRAM_APPROVED_CHANNEL_ID = '-100APPROVED';
process.env.TELEGRAM_APPROVED_CHANNEL_USERNAME = '@veroeffentlicht';
process.env.WHATSAPP_DELIVERY_ENABLED = '1';
process.env.WHATSAPP_DELIVERY_ENDPOINT = '';
process.env.WHATSAPP_DELIVERY_SENDER = '';
process.env.WHATSAPP_CONTROL_ENDPOINT = '';
process.env.WHATSAPP_PLAYWRIGHT_LOGIN_TIMEOUT_MS = '1500';
process.env.WHATSAPP_PLAYWRIGHT_LOGIN_POLL_INTERVAL_MS = '20';
process.env.WHATSAPP_KEEP_BROWSER_OPEN = '0';

const { getDb } = await import('../db.js');
const {
  getWhatsappRuntimeState,
  performWhatsappRuntimeAction,
  saveWhatsappRuntimeSettings
} = await import('../services/whatsappRuntimeService.js');
const {
  __setWhatsappAutomationOverrideForTests,
  __resetWhatsappPlaywrightWorkerForTests
} = await import('../services/whatsappPlaywrightWorkerService.js');
const { __testablesDirectPublisher } = await import('../services/directPublisher.js');
const { getPublishingQueueEntry, listPublishingLogs } = await import('../services/publisherService.js');

const db = getDb();

function nowIso() {
  return new Date().toISOString();
}

function resetDatabase() {
  db.exec(`
    DELETE FROM publishing_logs;
    DELETE FROM publishing_targets;
    DELETE FROM publishing_queue;
    DELETE FROM output_channels;
    DELETE FROM whatsapp_output_targets;
    DELETE FROM app_sessions WHERE session_key LIKE 'whatsapp_output:%';
  `);
}

function createAutomationState() {
  return {
    running: false,
    connected: false,
    qrRequired: true,
    sessionExpired: false,
    channelReachable: true,
    qrCodeDataUrl: 'data:image/png;base64,QRTEST',
    sentPhases: []
  };
}

function buildAutomationSnapshot(state) {
  if (!state.running) {
    return {
      connectionStatus: 'not_connected',
      workerStatus: 'stopped',
      sessionValid: false,
      qrRequired: false,
      qrCodeDataUrl: '',
      browserStatus: 'stopped',
      channelReachable: false,
      lastHealthCheckAt: nowIso()
    };
  }

  if (state.sessionExpired) {
    return {
      connectionStatus: 'session_expired',
      workerStatus: 'running',
      sessionValid: false,
      qrRequired: false,
      qrCodeDataUrl: '',
      browserStatus: 'running',
      channelReachable: false,
      lastHealthCheckAt: nowIso()
    };
  }

  if (state.connected) {
    return {
      connectionStatus: 'connected',
      workerStatus: 'running',
      sessionValid: true,
      qrRequired: false,
      qrCodeDataUrl: '',
      browserStatus: 'running',
      channelReachable: state.channelReachable,
      lastHealthCheckAt: nowIso()
    };
  }

  return {
    connectionStatus: 'qr_required',
    workerStatus: 'running',
    sessionValid: false,
    qrRequired: true,
    qrCodeDataUrl: state.qrCodeDataUrl,
    browserStatus: 'running',
    channelReachable: false,
    lastHealthCheckAt: nowIso()
  };
}

function buildAutomationAdapter(state) {
  return {
    async startSession(providerInfo) {
      state.running = true;
      return { providerInfo };
    },
    async stopSession() {
      state.running = false;
    },
    async inspectSession() {
      return buildAutomationSnapshot(state);
    },
    async refreshSession() {
      return buildAutomationSnapshot(state);
    },
    async sendPhase(_session, _providerInfo, input = {}) {
      state.sentPhases.push({
        phase: String(input.phase || ''),
        sendId: String(input.sendId || ''),
        targetRef: String(input.targetRef || ''),
        text: String(input.text || ''),
        imageUrl: String(input.imageUrl || ''),
        imageSource: String(input.imageSource || '')
      });

      return {
        status: 'sent',
        duplicatePrevented: false,
        messageId: `wa-msg-${state.sentPhases.length}`,
        deliveryId: `wa-delivery-${state.sentPhases.length}`,
        response: {
          phase: input.phase || 'main',
          messageKind: input.imageUrl ? 'image_caption' : 'text',
          imageUploadVerified: Boolean(input.imageUrl)
        }
      };
    }
  };
}

async function test(name, fn) {
  resetDatabase();
  await __resetWhatsappPlaywrightWorkerForTests();

  try {
    await fn();
    console.log(`PASS ${name}`);
  } finally {
    await __resetWhatsappPlaywrightWorkerForTests().catch(() => null);
  }
}

await test('Approved Route spiegelt Telegram Veroeffentlicht in WhatsApp Test Output und blockiert Live-WhatsApp weiter', async () => {
  const automationState = createAutomationState();
  __setWhatsappAutomationOverrideForTests(buildAutomationAdapter(automationState));
  saveWhatsappRuntimeSettings({
    workerEnabled: true,
    alertsEnabled: false,
    alertTargetRef: '',
    sendCooldownMs: 10
  });

  await performWhatsappRuntimeAction('start_worker');
  automationState.connected = true;
  automationState.qrRequired = false;
  await performWhatsappRuntimeAction('test_connection');

  const runtime = getWhatsappRuntimeState();
  assert.equal(runtime.connectionStatus, 'connected');
  assert.equal(runtime.sessionValid, true);

  const result = await __testablesDirectPublisher.publishSecondaryTelegramRoute({
    routeKey: 'approved',
    targetChatId: '-100APPROVED',
    queueSourceType: 'generator_direct_approved_route',
    generatorPostId: 42,
    imageSource: 'standard',
    payload: {
      title: 'Echo Dot Testdeal',
      link: 'https://www.amazon.de/dp/B000WAAPP1?tag=affman-21',
      normalizedUrl: 'https://www.amazon.de/dp/B000WAAPP1',
      asin: 'B000WAAPP1',
      sellerType: 'AMAZON',
      currentPrice: '19,99 €',
      oldPrice: '39,99 €',
      couponCode: 'TESTCODE10',
      skipPostedDealHistory: true,
      textByChannel: {
        telegram:
          '<b>Echo Dot Testdeal</b>\n\n🔥 Jetzt <b>19,99 €</b>\n➡️ <b>https://www.amazon.de/dp/B000WAAPP1?tag=affman-21</b>\n\n\n<i>Anzeige/Partnerlink</i>',
        whatsapp:
          'Echo Dot Testdeal\n\n🔥 Jetzt 19,99 €\n➡️ https://www.amazon.de/dp/B000WAAPP1?tag=affman-21\n\nAnzeige/Partnerlink'
      },
      imageVariants: {
        standard: 'https://placehold.co/1200x1200/png?text=Echo+Dot+Testdeal'
      },
      targetImageSources: {
        telegram: 'standard',
        whatsapp: 'standard'
      }
    },
    processorOverrides: {
      telegram: async (target) => ({
        targets: [
          {
            messageId: 'tg-approved-1',
            chatId: target.target_ref,
            targetChatId: target.target_ref,
            duplicateBlocked: false,
            lastSentAt: nowIso()
          }
        ]
      })
    }
  });

  assert.equal(result.targetStatus, 'sent');
  assert.equal(result.messageId, 'tg-approved-1');
  assert.equal(result.whatsappStatus, 'sent');
  assert.ok(Array.isArray(result.whatsappTargets));
  assert.equal(result.whatsappTargets.length, 2);

  const testTarget = result.whatsappTargets.find((target) => String(target.targetLabel || '').includes('WhatsApp Test Output'));
  const liveTarget = result.whatsappTargets.find((target) => String(target.targetLabel || '').includes('Code & Couponing WhatsApp'));

  assert.ok(testTarget);
  assert.ok(liveTarget);
  assert.equal(testTarget.status, 'sent');
  assert.equal(liveTarget.status, 'skipped');
  assert.ok(String(liveTarget.errorMessage || '').includes('OUTPUT_DISABLED_SKIP'));

  const queue = getPublishingQueueEntry(result.queueId);
  assert.ok(queue);
  assert.equal(queue.targets.filter((target) => target.channel_type === 'telegram').length, 1);
  assert.equal(queue.targets.filter((target) => target.channel_type === 'whatsapp').length, 2);

  const logs = listPublishingLogs({ limit: 120 });
  assert.ok(logs.some((item) => String(item.message || '').includes('WHATSAPP_PUBLISH_MIRROR_START')));
  assert.ok(logs.some((item) => String(item.message || '').includes('WHATSAPP_PUBLISH_MIRROR_TARGET_SELECTED')));
  assert.ok(logs.some((item) => String(item.message || '').includes('WHATSAPP_PUBLISH_MIRROR_IMAGE_START')));
  assert.ok(logs.some((item) => String(item.message || '').includes('WHATSAPP_PUBLISH_MIRROR_TEXT_START')));
  assert.ok(logs.some((item) => String(item.message || '').includes('WHATSAPP_PUBLISH_MIRROR_COUPON_START')));
  assert.ok(logs.some((item) => String(item.message || '').includes('WHATSAPP_PUBLISH_MIRROR_SUCCESS')));
  assert.ok(logs.some((item) => String(item.message || '').includes('WHATSAPP_MIRROR_REAL_FLOW_SEND_START')));
  assert.ok(logs.some((item) => String(item.message || '').includes('WHATSAPP_MIRROR_REAL_FLOW_SUCCESS')));
  assert.ok(logs.some((item) => String(item.message || '').includes('WHATSAPP_LIVE_OUTPUT_DISABLED_SKIP')));

  const sentTestTargetPhases = automationState.sentPhases.filter((phase) =>
    String(phase.targetRef || '').includes('0029VbCsyVY7NoZryjRrBU2P')
  );
  assert.equal(sentTestTargetPhases.length, 2);
  assert.equal(sentTestTargetPhases[0].phase, 'main');
  assert.equal(sentTestTargetPhases[1].phase, 'coupon');
  assert.ok(String(sentTestTargetPhases[0].imageUrl || '').includes('placehold.co/1200x1200'));
  assert.equal(sentTestTargetPhases[0].imageSource, 'standard');
  assert.equal(
    automationState.sentPhases.some((phase) => String(phase.targetRef || '').includes('0029Va8EEIFHLHQgQlvNdx1y')),
    false
  );
  assert.ok(logs.some((item) => String(item.message || '').includes('WHATSAPP_IMAGE_PATH_FOUND')));
  assert.ok(logs.some((item) => String(item.message || '').includes('WHATSAPP_IMAGE_UPLOAD_SUCCESS')));
  assert.ok(logs.some((item) => String(item.message || '').includes('WHATSAPP_FULL_POST_SUCCESS')));
});

console.log('OK Approved WhatsApp Mirror getestet');
