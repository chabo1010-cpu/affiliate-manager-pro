import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'affiliate-whatsapp-output-'));
process.env.APP_DATA_DIR = tempRoot;
process.env.APP_DB_PATH = path.join(tempRoot, 'deals.db');
process.env.TELEGRAM_BOT_TOKEN = 'test-token';
process.env.WHATSAPP_DELIVERY_ENABLED = '1';
process.env.WHATSAPP_DELIVERY_ENDPOINT = '';
process.env.WHATSAPP_DELIVERY_SENDER = '';
process.env.WHATSAPP_CONTROL_ENDPOINT = '';
process.env.WHATSAPP_PLAYWRIGHT_LOGIN_TIMEOUT_MS = '1500';
process.env.WHATSAPP_PLAYWRIGHT_LOGIN_POLL_INTERVAL_MS = '20';
process.env.WHATSAPP_KEEP_BROWSER_OPEN = '0';

const TEST_TARGET_REF = 'https://whatsapp.com/channel/0029VbCsyVY7NoZryjRrBU2P';
const LIVE_TARGET_REF = 'https://whatsapp.com/channel/0029Va8EEIFHLHQgQlvNdx1y';

const { getDb } = await import('../db.js');
const {
  createPublishingEntry,
  getPublishingQueueEntry,
  listPublishingLogs,
  processPublishingQueueEntry,
  sendWhatsappOutputTargetTestPost
} = await import('../services/publisherService.js');
const {
  getWhatsappOutputTargetConfig,
  saveWhatsappOutputTargetConfig
} = await import('../services/whatsappOutputTargetService.js');
const { listOutputChannelsSnapshot, saveOutputChannelConfig } = await import('../services/outputChannelService.js');
const {
  getWhatsappRuntimeState,
  performWhatsappRuntimeAction,
  runWhatsappHealthCheck,
  saveWhatsappRuntimeSettings
} = await import('../services/whatsappRuntimeService.js');
const {
  __resetWhatsappPlaywrightWorkerForTests,
  __setWhatsappAutomationOverrideForTests,
  __classifyWhatsappWebOnlyTargetForTests,
  __buildWhatsappChannelPlanForTests
} = await import('../services/whatsappPlaywrightWorkerService.js');

const db = getDb();

function nowIso() {
  return new Date().toISOString();
}

async function waitFor(check, { timeoutMs = 1000, intervalMs = 20 } = {}) {
  const startedAt = Date.now();

  while (Date.now() - startedAt <= timeoutMs) {
    const result = await check();
    if (result) {
      return result;
    }

    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }

  throw new Error(`waitFor timeout after ${timeoutMs}ms`);
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
  db.prepare(
    `
      UPDATE app_settings
      SET copybotEnabled = 1,
          outputQueueEnabled = 1,
          whatsappWorkerEnabled = 0,
          whatsappAlertTelegramEnabled = 0,
          whatsappAlertTelegramTarget = NULL,
          whatsappSendCooldownMs = 500
      WHERE id = 1
    `
  ).run();
}

function createAutomationState() {
  return {
    running: false,
    connected: false,
    qrRequired: true,
    sessionExpired: false,
    channelReachable: true,
    qrCodeDataUrl: 'data:image/png;base64,QRTEST',
    startCount: 0,
    stopCount: 0,
    refreshCount: 0,
    sentPhases: [],
    failCouponOnce: true,
    recoverOnNextStart: false
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
      state.startCount += 1;
      if (state.recoverOnNextStart) {
        state.sessionExpired = false;
        state.connected = true;
        state.qrRequired = false;
        state.recoverOnNextStart = false;
      }
      return {
        providerInfo
      };
    },
    async stopSession() {
      state.running = false;
      state.stopCount += 1;
    },
    async inspectSession() {
      return buildAutomationSnapshot(state);
    },
    async refreshSession() {
      state.refreshCount += 1;
      return buildAutomationSnapshot(state);
    },
    async testChannel(_session, _providerInfo, input = {}) {
      if (!state.connected || state.sessionExpired) {
        const error = new Error('WhatsApp ist nicht verbunden.');
        error.code = 'WHATSAPP_NOT_CONNECTED';
        throw error;
      }
      if (!state.channelReachable) {
        const error = new Error('WhatsApp Kanal ist nicht erreichbar oder nicht schreibbar.');
        error.code = 'WHATSAPP_CHANNEL_UNREACHABLE';
        throw error;
      }
      return {
        channelReachable: true,
        targetUrl: input.channelUrl || input.targetRef || TEST_TARGET_REF
      };
    },
    async debugChannel(_session, _providerInfo, input = {}) {
      if (!state.connected || state.sessionExpired) {
        const error = new Error('WhatsApp ist nicht verbunden.');
        error.code = 'WHATSAPP_NOT_CONNECTED';
        throw error;
      }
      return {
        success: true,
        channelReachable: true,
        targetUrl: input.channelUrl || input.targetRef || TEST_TARGET_REF,
        currentUrl: input.channelUrl || input.targetRef || TEST_TARGET_REF,
        currentTitle: 'WhatsApp Channel Debug',
        channelNavigationStatus: 'WHATSAPP_CHANNEL_COMPOSER_FOUND',
        channelAdminStatus: 'ADMIN_CONTROLS_VISIBLE',
        debugArtifacts: [
          {
            step: 'after-web-whatsapp',
            screenshotPath: path.join(tempRoot, 'debug-after-web.png'),
            jsonPath: path.join(tempRoot, 'debug-after-web.json')
          }
        ]
      };
    },
    async manualChannelDebugCapture(_session, _providerInfo, input = {}) {
      return {
        success: true,
        channelReachable: true,
        targetUrl: input.channelUrl || input.targetRef || TEST_TARGET_REF,
        currentUrl: input.channelUrl || input.targetRef || TEST_TARGET_REF,
        currentTitle: 'WhatsApp Manual Channel Debug',
        channelNavigationStatus: 'WHATSAPP_CHANNEL_COMPOSER_FOUND',
        channelAdminStatus: 'ADMIN_CONTROLS_VISIBLE',
        preferredSelector: 'footer [contenteditable=\"true\"]',
        composerCandidates: [
          {
            preferredSelector: 'footer [contenteditable=\"true\"]',
            ariaLabel: 'Nachricht',
            dataTab: '10',
            insideFooter: true
          }
        ],
        contentEditableFields: [
          {
            preferredSelector: 'footer [contenteditable=\"true\"]'
          }
        ],
        roleTextboxes: [],
        ariaLabels: [],
        dataTabFields: [],
        visibleButtons: ['Senden'],
        debugArtifacts: [
          {
            step: 'manual-channel-debug',
            screenshotPath: path.join(tempRoot, 'manual-debug.png'),
            jsonPath: path.join(tempRoot, 'manual-debug.json'),
            htmlPath: path.join(tempRoot, 'manual-debug.html')
          }
        ],
        screenshotPath: path.join(tempRoot, 'manual-debug.png'),
        domSnapshotPath: path.join(tempRoot, 'manual-debug.json'),
        htmlSnapshotPath: path.join(tempRoot, 'manual-debug.html'),
        lastChannelDebugMessage: 'Manueller Channel-Debug gespeichert.'
      };
    },
    async manualChannelDebugWait(_session, _providerInfo, input = {}) {
      return await this.manualChannelDebugCapture(_session, _providerInfo, input);
    },
    async sendPhase(_session, _providerInfo, input = {}) {
      if (!state.connected || state.sessionExpired) {
        const error = new Error('WhatsApp ist nicht verbunden.');
        error.code = 'WHATSAPP_NOT_CONNECTED';
        error.retryable = true;
        throw error;
      }

      state.sentPhases.push({
        phase: String(input.phase || ''),
        sendId: String(input.sendId || ''),
        text: String(input.text || ''),
        imageUrl: String(input.imageUrl || ''),
        imageSource: String(input.imageSource || '')
      });

      if (input.phase === 'coupon' && state.failCouponOnce) {
        state.failCouponOnce = false;
        const error = new Error('Coupon Versand kurzzeitig fehlgeschlagen.');
        error.code = 'WHATSAPP_SEND_TEMP';
        error.retryable = true;
        throw error;
      }

      const suffix = state.sentPhases.length;
      return {
        status: 'sent',
        duplicatePrevented: false,
        messageId: `wa-msg-${suffix}`,
        deliveryId: `wa-delivery-${suffix}`,
        response: {
          phase: input.phase || 'main',
          messageKind: input.imageUrl ? 'image_caption' : 'text',
          imageUploadVerified: Boolean(input.imageUrl)
        }
      };
    }
  };
}

function buildQueuePayload(overrides = {}) {
  return {
    title: overrides.title || 'WhatsApp Test Deal',
    link: overrides.link || 'https://www.amazon.de/dp/B000WA001',
    normalizedUrl: overrides.normalizedUrl || overrides.link || 'https://www.amazon.de/dp/B000WA001',
    asin: overrides.asin || 'B000WA001',
    sellerType: overrides.sellerType || 'AMAZON',
    currentPrice: overrides.currentPrice || '29.99',
    oldPrice: overrides.oldPrice || '59.99',
    couponCode: overrides.couponCode || 'SAVE10',
    textByChannel: {
      whatsapp: overrides.whatsappText || 'Generator WhatsApp Text unveraendert'
    },
    imageVariants: {
      standard: overrides.imageUrl || 'https://cdn.example.com/deal.jpg'
    },
    targetImageSources: {
      whatsapp: overrides.whatsappImageSource || 'standard'
    },
    whatsappTargetRefs: overrides.whatsappTargetRefs || [TEST_TARGET_REF],
    skipPostedDealHistory: true
  };
}

async function prepareAutomationRuntime(state, { alertsEnabled = true } = {}) {
  __setWhatsappAutomationOverrideForTests(buildAutomationAdapter(state));
  saveWhatsappRuntimeSettings({
    workerEnabled: true,
    alertsEnabled,
    alertTargetRef: '-100ALERT',
    sendCooldownMs: 500
  });
  await performWhatsappRuntimeAction('start_worker');
}

function getWhatsappTargetByRef(targetRef) {
  const config = getWhatsappOutputTargetConfig();
  const target = config.targets.find((item) => item.targetRef === targetRef);
  assert.ok(target, `WhatsApp Ziel fehlt: ${targetRef}`);
  return target;
}

function activateWhatsappTarget(targetRef = TEST_TARGET_REF) {
  const config = getWhatsappOutputTargetConfig();
  const target = getWhatsappTargetByRef(targetRef);

  saveWhatsappOutputTargetConfig({
    targets: config.targets.map((item) =>
      item.targetRef === targetRef
        ? {
            ...item,
            isActive: true,
            useForPublishing: true,
            requiresManualActivation: target.requiresManualActivation === true
          }
        : item
    )
  });

  const snapshot = listOutputChannelsSnapshot();
  const outputChannel = snapshot.channels.find((item) => item.targetRef === targetRef);
  assert.ok(outputChannel, 'Output Channel fuer WhatsApp muss vorhanden sein.');
  saveOutputChannelConfig(outputChannel.channelKey, {
    isEnabled: true,
    allowLiveMode: target.requiresManualActivation === true ? true : outputChannel.allowLiveMode
  });

  return target.id;
}

async function run(name, fn) {
  resetDatabase();
  await __resetWhatsappPlaywrightWorkerForTests();
  try {
    await fn();
    console.log(`PASS ${name}`);
    return true;
  } catch (error) {
    console.error(`FAIL ${name}`);
    console.error(error);
    return false;
  } finally {
    await __resetWhatsappPlaywrightWorkerForTests().catch(() => null);
  }
}

const results = [];

results.push(
  await run('Web-only Guard blockiert Desktop-App, Deep Links und Open-in-App Ziele', async () => {
    const deepLink = __classifyWhatsappWebOnlyTargetForTests({
      href: 'whatsapp://send?text=test',
      text: 'In WhatsApp ansehen'
    });
    const desktopApp = __classifyWhatsappWebOnlyTargetForTests({
      href: 'whatsapp-desktop://open',
      text: 'WhatsApp Desktop'
    });
    const storeApp = __classifyWhatsappWebOnlyTargetForTests({
      href: 'ms-windows-store://pdp/?ProductId=9NKSQGP7F2NH',
      text: 'Store'
    });
    const openInAppLanding = __classifyWhatsappWebOnlyTargetForTests({
      href: TEST_TARGET_REF,
      text: 'In WhatsApp ansehen'
    });
    const safeWebRoute = __classifyWhatsappWebOnlyTargetForTests({
      href: 'https://web.whatsapp.com/channel/0029VbCsyVY7NoZryjRrBU2P',
      text: 'Aff.Manager Tests Output'
    });

    assert.equal(deepLink.blocked, true);
    assert.equal(deepLink.logCode, 'WHATSAPP_DEEP_LINK_BLOCKED');
    assert.equal(desktopApp.blocked, true);
    assert.equal(desktopApp.logCode, 'WHATSAPP_DESKTOP_APP_NOT_ALLOWED');
    assert.equal(storeApp.blocked, true);
    assert.equal(storeApp.logCode, 'WHATSAPP_DESKTOP_APP_NOT_ALLOWED');
    assert.equal(openInAppLanding.blocked, true);
    assert.equal(openInAppLanding.logCode, 'WHATSAPP_OPEN_IN_APP_BUTTON_SKIPPED');
    assert.equal(safeWebRoute.blocked, false);
  })
);

results.push(
  await run('Channel Lookup nutzt den echten WhatsApp Kanalnamen als Alias', async () => {
    db.prepare(`DELETE FROM app_sessions WHERE session_key = ?`).run('whatsapp_output:session:default');
    db.prepare(
      `
        INSERT INTO app_sessions (
          session_key,
          module,
          session_type,
          status,
          external_ref,
          storage_path,
          meta_json,
          last_seen_at,
          created_at,
          updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `
    ).run(
      'whatsapp_output:session:default',
      'whatsapp_output',
      'runtime',
      'connected',
      'primary',
      tempRoot,
      JSON.stringify({
        lastChannelTargetRef: TEST_TARGET_REF,
        lastChannelComposerCandidates: [
          {
            ariaLabel: 'Nachricht an Aff.Manager Tests Output schreiben'
          }
        ]
      }),
      nowIso(),
      nowIso(),
      nowIso()
    );

    const plan = __buildWhatsappChannelPlanForTests(TEST_TARGET_REF, {
      targetType: 'WHATSAPP_TEST_CHANNEL',
      targetLabel: 'WhatsApp Test Output',
      channelUrl: TEST_TARGET_REF
    });

    assert.ok(plan.lookupTerms.includes('Aff.Manager Tests Output'));
    assert.ok(plan.lookupTerms.includes('WhatsApp Test Output'));
  })
);

results.push(
  await run('WhatsApp Test Output ist aktiv und Live-Kanal bleibt deaktiviert', async () => {
    const config = getWhatsappOutputTargetConfig();
    const testTarget = getWhatsappTargetByRef(TEST_TARGET_REF);
    const liveTarget = getWhatsappTargetByRef(LIVE_TARGET_REF);

    assert.equal(testTarget.name, 'WhatsApp Test Output');
    assert.equal(testTarget.targetType, 'WHATSAPP_TEST_CHANNEL');
    assert.equal(testTarget.isActive, true);
    assert.equal(testTarget.requiresManualActivation, false);

    assert.equal(liveTarget.name, 'Code & Couponing WhatsApp');
    assert.equal(liveTarget.targetType, 'WHATSAPP_CHANNEL');
    assert.equal(liveTarget.isActive, false);
    assert.equal(liveTarget.requiresManualActivation, true);

    const snapshot = listOutputChannelsSnapshot();
    const testOutputChannel = snapshot.channels.find((item) => item.targetRef === TEST_TARGET_REF);
    const liveOutputChannel = snapshot.channels.find((item) => item.targetRef === LIVE_TARGET_REF);
    assert.ok(testOutputChannel);
    assert.ok(liveOutputChannel);
    assert.equal(testOutputChannel.channelType, 'test');
    assert.equal(testOutputChannel.isEnabled, true);
    assert.equal(liveOutputChannel.channelType, 'live');
    assert.equal(liveOutputChannel.isEnabled, false);
    assert.equal(liveOutputChannel.warningText, 'LIVE KANAL Deaktiviert Erst manuell aktivieren');
  })
);

results.push(
  await run('Deaktivierter WhatsApp Live-Kanal sendet nicht und loggt LIVE_OUTPUT_DISABLED_SKIP', async () => {
    const queue = createPublishingEntry({
      sourceType: 'copybot',
      payload: buildQueuePayload({
        whatsappTargetRefs: [LIVE_TARGET_REF]
      }),
      targets: [{ channelType: 'whatsapp', isEnabled: true, imageSource: 'standard' }]
    });

    const result = await processPublishingQueueEntry(queue.id);
    const queueAfter = getPublishingQueueEntry(queue.id);
    const logs = listPublishingLogs({ limit: 30 });
    const latestSkipLog = logs.find((item) => String(item.message || '').includes('WHATSAPP_OUTPUT_DISABLED_SKIP'));
    const latestLiveSkipLog = logs.find((item) => String(item.message || '').includes('WHATSAPP_LIVE_OUTPUT_DISABLED_SKIP'));

    assert.equal(result.results[0]?.status, 'skipped');
    assert.equal(queueAfter.targets[0].status, 'skipped');
    assert.ok(String(latestSkipLog?.message || '').includes('WHATSAPP_OUTPUT_DISABLED_SKIP'));
    assert.ok(String(latestLiveSkipLog?.message || '').includes('WHATSAPP_LIVE_OUTPUT_DISABLED_SKIP'));
  })
);

results.push(
  await run('QR Login Status, Session-Speicherung sowie Worker Start und Stop funktionieren', async () => {
    const automationState = createAutomationState();
    const testTarget = getWhatsappTargetByRef(TEST_TARGET_REF);
    await prepareAutomationRuntime(automationState, {
      alertsEnabled: false
    });

    const runtimeAfterStart = getWhatsappRuntimeState();
    assert.equal(runtimeAfterStart.connectionStatus, 'qr_required');
    assert.equal(runtimeAfterStart.qrRequired, true);
    assert.equal(runtimeAfterStart.qrCodeDataUrl, 'data:image/png;base64,QRTEST');
    assert.equal(runtimeAfterStart.workerStatus, 'running');
    assert.equal(runtimeAfterStart.profileWritable, true);

    automationState.connected = true;
    automationState.qrRequired = false;

    const connectedRuntime = await waitFor(() => {
      const runtime = getWhatsappRuntimeState();
      return runtime.connectionStatus === 'connected' && runtime.sessionValid === true ? runtime : null;
    }, { timeoutMs: 800 });
    assert.equal(connectedRuntime.connectionStatus, 'connected');
    assert.equal(connectedRuntime.sessionValid, true);
    assert.ok(connectedRuntime.lastConnectedAt);
    assert.ok(connectedRuntime.sessionSavedAt);

    const channelTest = await performWhatsappRuntimeAction('test_channel', {
      targetId: testTarget.id,
      targetRef: TEST_TARGET_REF,
      targetLabel: 'WhatsApp Test Output',
      channelUrl: TEST_TARGET_REF
    });
    assert.equal(channelTest.runtime.channelReachable, true);

    const loginLogs = listPublishingLogs({ limit: 80 });
    assert.ok(loginLogs.some((item) => String(item.message || '').includes('WHATSAPP_QR_VISIBLE')));
    assert.ok(loginLogs.some((item) => String(item.message || '').includes('WHATSAPP_QR_SCAN_WAITING')));
    assert.ok(loginLogs.some((item) => String(item.message || '').includes('WHATSAPP_LOGIN_SUCCESS_DETECTED')));
    assert.ok(loginLogs.some((item) => String(item.message || '').includes('WHATSAPP_SESSION_SAVE_START')));
    assert.ok(loginLogs.some((item) => String(item.message || '').includes('WHATSAPP_SESSION_SAVE_OK')));

    const stopped = await performWhatsappRuntimeAction('stop_worker');
    assert.equal(stopped.runtime.workerStatus, 'stopped');
    assert.equal(automationState.stopCount >= 1, true);
  })
);

results.push(
  await run('Auto-Healthcheck startet keinen Browser, wenn der WhatsApp Worker deaktiviert ist', async () => {
    const automationState = createAutomationState();
    __setWhatsappAutomationOverrideForTests(buildAutomationAdapter(automationState));
    saveWhatsappRuntimeSettings({
      workerEnabled: false,
      alertsEnabled: false,
      alertTargetRef: ''
    });

    const runtime = await runWhatsappHealthCheck({ manual: false });
    const logs = listPublishingLogs({ limit: 30 });

    assert.equal(automationState.startCount, 0);
    assert.equal(runtime.workerEnabled, false);
    assert.ok(logs.some((item) => String(item.message || '').includes('WHATSAPP_WORKER_STOP_REASON')));
  })
);

results.push(
  await run('Gestoppter Worker-Owner blockiert den naechsten WhatsApp Start nicht mehr', async () => {
    const automationState = createAutomationState();
    __setWhatsappAutomationOverrideForTests(buildAutomationAdapter(automationState));
    saveWhatsappRuntimeSettings({
      workerEnabled: true,
      alertsEnabled: false,
      alertTargetRef: ''
    });

    db.prepare(
      `
        INSERT INTO app_sessions (
          session_key,
          module,
          session_type,
          status,
          external_ref,
          storage_path,
          meta_json,
          last_seen_at,
          created_at,
          updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `
    ).run(
      'whatsapp_output:playwright_owner:default',
      'whatsapp_output',
      'playwright_owner',
      'stopped',
      'primary',
      tempRoot,
      JSON.stringify({
        pid: process.ppid,
        profileDir: tempRoot,
        claimedAt: nowIso()
      }),
      nowIso(),
      nowIso(),
      nowIso()
    );

    const started = await performWhatsappRuntimeAction('start_worker');
    assert.equal(started.runtime?.workerStatus || started.workerStatus, 'running');
    assert.equal(automationState.startCount >= 1, true);
  })
);

results.push(
  await run('WhatsApp Session kann kontrolliert zurueckgesetzt werden', async () => {
    const automationState = createAutomationState();
    await prepareAutomationRuntime(automationState, {
      alertsEnabled: false
    });

    const runtime = getWhatsappRuntimeState();
    const profileDir = runtime.browserProfileDir;
    assert.ok(profileDir);
    fs.writeFileSync(path.join(profileDir, 'stale-session.txt'), 'legacy-session', 'utf8');

    const reset = await performWhatsappRuntimeAction('reset_session');
    const runtimeAfterReset = getWhatsappRuntimeState();

    assert.equal(reset.runtime.workerStatus, 'stopped');
    assert.equal(runtimeAfterReset.connectionStatus, 'not_connected');
    assert.equal(runtimeAfterReset.sessionValid, false);
    assert.ok(runtimeAfterReset.profileBackupDir);
    assert.ok(runtimeAfterReset.sessionResetAt);
    assert.ok(fs.existsSync(runtimeAfterReset.browserProfileDir));
    assert.ok(fs.existsSync(path.join(runtimeAfterReset.profileBackupDir, 'stale-session.txt')));
    assert.equal(fs.existsSync(path.join(runtimeAfterReset.browserProfileDir, 'stale-session.txt')), false);
  })
);

results.push(
  await run('Telegram Alert Test versendet eine Testmeldung', async () => {
    const automationState = createAutomationState();
    await prepareAutomationRuntime(automationState);

    const originalFetch = global.fetch;
    const telegramCalls = [];

    global.fetch = async (url) => {
      if (String(url).includes('api.telegram.org')) {
        telegramCalls.push(String(url));
        return {
          ok: true,
          text: async () =>
            JSON.stringify({
              ok: true,
              result: {
                message_id: 77,
                chat: {
                  id: '-100ALERT'
                }
              }
            })
        };
      }

      throw new Error(`Unerwarteter Fetch: ${url}`);
    };

    try {
      await performWhatsappRuntimeAction('alert_test');
      assert.ok(telegramCalls.length >= 1);
    } finally {
      global.fetch = originalFetch;
    }
  })
);

results.push(
  await run('Legacy Live-Flag am WhatsApp Test-Kanal wird geheilt und blockiert Testsends nicht', async () => {
    const automationState = createAutomationState();
    await prepareAutomationRuntime(automationState, {
      alertsEnabled: false
    });
    automationState.connected = true;
    automationState.qrRequired = false;
    await performWhatsappRuntimeAction('test_connection');

    const testTarget = getWhatsappTargetByRef(TEST_TARGET_REF);
    const poisonedChannel = listOutputChannelsSnapshot().channels.find(
      (item) => item.channelKey === `whatsapp:target:${testTarget.id}`
    );
    assert.ok(poisonedChannel, 'WhatsApp Test Output fehlt vor der Regression.');

    db.prepare(
      `
        UPDATE output_channels
        SET channel_type = 'test',
            allow_live_mode = 0,
            is_dangerous_live = 1,
            updated_at = ?
        WHERE channel_key = ?
      `
    ).run(nowIso(), poisonedChannel.channelKey);

    const healedChannel = listOutputChannelsSnapshot().channels.find((item) => item.channelKey === poisonedChannel.channelKey);
    assert.ok(healedChannel, 'WhatsApp Test Output fehlt nach der Heilung.');
    assert.equal(healedChannel.channelType, 'test');
    assert.equal(healedChannel.allowLiveMode, true);
    assert.equal(healedChannel.isDangerousLive, false);

    const sentResult = await sendWhatsappOutputTargetTestPost(testTarget.id, {
      text: 'Regression geheilt'
    });

    assert.equal(sentResult.results[0]?.status, 'sent');
  })
);

results.push(
  await run('WhatsApp Testpost sendet nur in den aktiven Test-Kanal und blockiert den Live-Kanal', async () => {
    const automationState = createAutomationState();
    await prepareAutomationRuntime(automationState, {
      alertsEnabled: false
    });
    automationState.connected = true;
    automationState.qrRequired = false;
    await performWhatsappRuntimeAction('test_connection');

    const testTarget = getWhatsappTargetByRef(TEST_TARGET_REF);
    const liveTarget = getWhatsappTargetByRef(LIVE_TARGET_REF);
    const sentResult = await sendWhatsappOutputTargetTestPost(testTarget.id);
    const blockedResult = await sendWhatsappOutputTargetTestPost(liveTarget.id);
    const logs = listPublishingLogs({ limit: 80 });

    assert.equal(sentResult.results[0]?.status, 'sent');
    assert.equal(blockedResult.results[0]?.status, 'skipped');
    assert.ok(logs.some((item) => String(item.message || '').includes('WHATSAPP_TEST_SEND_ALLOWED')));
    assert.ok(logs.some((item) => String(item.message || '').includes('WHATSAPP_LIVE_OUTPUT_DISABLED_SKIP')));
  })
);

results.push(
  await run('WhatsApp Channel Debug speichert Navigationsstatus und Artefakte', async () => {
    const automationState = createAutomationState();
    await prepareAutomationRuntime(automationState, {
      alertsEnabled: false
    });
    automationState.connected = true;
    automationState.qrRequired = false;
    await performWhatsappRuntimeAction('test_connection');

    const testTarget = getWhatsappTargetByRef(TEST_TARGET_REF);
    const debugResult = await performWhatsappRuntimeAction('channel_debug', {
      targetId: testTarget.id,
      targetRef: testTarget.targetRef,
      targetLabel: testTarget.targetLabel || testTarget.name,
      channelUrl: testTarget.channelUrl || testTarget.targetRef
    });

    assert.equal(debugResult.runtime.channelNavigationStatus, 'WHATSAPP_CHANNEL_COMPOSER_FOUND');
    assert.equal(debugResult.runtime.channelAdminStatus, 'ADMIN_CONTROLS_VISIBLE');
    assert.equal(debugResult.result.debugArtifacts.length, 1);
    assert.equal(debugResult.runtime.lastChannelDebugArtifacts.length, 1);
  })
);

results.push(
  await run('Manueller WhatsApp Channel Debug speichert Selector, Kandidaten und Artefakte', async () => {
    const automationState = createAutomationState();
    await prepareAutomationRuntime(automationState, {
      alertsEnabled: false
    });
    automationState.connected = true;
    automationState.qrRequired = false;
    await performWhatsappRuntimeAction('test_connection');

    const testTarget = getWhatsappTargetByRef(TEST_TARGET_REF);
    const debugResult = await performWhatsappRuntimeAction('manual_channel_debug_capture', {
      targetId: testTarget.id,
      targetRef: testTarget.targetRef,
      targetLabel: testTarget.targetLabel || testTarget.name,
      channelUrl: testTarget.channelUrl || testTarget.targetRef
    });

    assert.equal(debugResult.runtime.channelNavigationStatus, 'WHATSAPP_CHANNEL_COMPOSER_FOUND');
    assert.equal(debugResult.runtime.channelAdminStatus, 'ADMIN_CONTROLS_VISIBLE');
    assert.equal(debugResult.runtime.lastChannelPreferredSelector, 'footer [contenteditable="true"]');
    assert.equal(debugResult.runtime.lastChannelComposerCandidates.length, 1);
    assert.ok(String(debugResult.runtime.lastChannelScreenshotPath).endsWith('manual-debug.png'));
    assert.ok(String(debugResult.runtime.lastChannelDomSnapshotPath).endsWith('manual-debug.json'));
    assert.ok(String(debugResult.runtime.lastChannelHtmlSnapshotPath).endsWith('manual-debug.html'));
  })
);

results.push(
  await run('Channel-Navigationsfehler gehen auf HOLD statt in Retry-Schleifen', async () => {
    const automationState = createAutomationState();
    await prepareAutomationRuntime(automationState, {
      alertsEnabled: false
    });
    automationState.connected = true;
    automationState.qrRequired = false;
    await performWhatsappRuntimeAction('test_connection');

    const queue = createPublishingEntry({
      sourceType: 'output_channel_test',
      payload: buildQueuePayload(),
      targets: [{ channelType: 'whatsapp', isEnabled: true, imageSource: 'standard' }]
    });

    await processPublishingQueueEntry(queue.id, {
      processors: {
        whatsapp: async () => {
          const error = new Error('Channel Navigation fehlt noch.');
          error.code = 'WHATSAPP_CHANNEL_NAVIGATION_NOT_IMPLEMENTED';
          error.retryable = false;
          throw error;
        }
      }
    });

    const queueAfter = getPublishingQueueEntry(queue.id);
    const logs = listPublishingLogs({ limit: 40 });

    assert.equal(queueAfter.status, 'hold');
    assert.equal(queueAfter.targets[0].status, 'hold');
    assert.ok(logs.some((item) => String(item.message || '').includes('WHATSAPP_CHANNEL_NAVIGATION_NOT_IMPLEMENTED')));
    assert.ok(logs.some((item) => item.event_type === 'whatsapp.channel.navigation.hold'));
  })
);

results.push(
  await run('Healthcheck, Recovery, Retry und Duplicate-Schutz halten WhatsApp Queue stabil', async () => {
    const automationState = createAutomationState();
    const originalFetch = global.fetch;
    let alertCalls = 0;

    global.fetch = async (url) => {
      if (String(url).includes('api.telegram.org')) {
        alertCalls += 1;
        return {
          ok: true,
          text: async () =>
            JSON.stringify({
              ok: true,
              result: {
                message_id: 88,
                chat: {
                  id: '-100ALERT'
                }
              }
            })
        };
      }

      throw new Error(`Unerwarteter Fetch: ${url}`);
    };

    try {
      await prepareAutomationRuntime(automationState);
      automationState.connected = true;
      automationState.qrRequired = false;
      await performWhatsappRuntimeAction('test_connection');
      activateWhatsappTarget(TEST_TARGET_REF);

      automationState.sessionExpired = true;
      automationState.connected = false;
      automationState.recoverOnNextStart = true;

      const healthCheck = await performWhatsappRuntimeAction('health_check');
      assert.equal(healthCheck.runtime.connectionStatus, 'connected');
      assert.equal(healthCheck.runtime.workerStatus, 'running');

      const healthLogs = listPublishingLogs({ limit: 40 });
      assert.ok(healthLogs.some((item) => String(item.message || '').includes('WHATSAPP_HEALTH_ERROR')));
      assert.ok(healthLogs.some((item) => String(item.message || '').includes('WHATSAPP_WORKER_RESTART')));

      const queue = createPublishingEntry({
        sourceType: 'generator',
        payload: buildQueuePayload(),
        targets: [{ channelType: 'whatsapp', isEnabled: true, imageSource: 'standard' }]
      });

      await processPublishingQueueEntry(queue.id);
      const retryQueue = getPublishingQueueEntry(queue.id);
      const retrySendId = String(retryQueue.targets[0].send_id || '');

      assert.equal(retryQueue.status, 'retry');
      assert.ok(retrySendId.startsWith(`wa-${queue.id}-${retryQueue.targets[0].id}-`));
      assert.ok(listPublishingLogs({ limit: 60 }).some((item) => item.message.includes('WHATSAPP_SEND_RETRY')));

      db.prepare(`UPDATE publishing_queue SET next_retry_at = ? WHERE id = ?`).run(new Date(Date.now() - 1000).toISOString(), queue.id);

      await processPublishingQueueEntry(queue.id);
      const finalQueue = getPublishingQueueEntry(queue.id);
      const finalLogs = listPublishingLogs({ limit: 80 });

      assert.equal(finalQueue.status, 'sent');
      assert.equal(finalQueue.targets[0].status, 'sent');
      assert.equal(finalQueue.targets[0].send_id, retrySendId);
      assert.ok(automationState.sentPhases.filter((item) => item.phase === 'main').length === 1);
      assert.ok(automationState.sentPhases.filter((item) => item.phase === 'coupon').length === 2);
      assert.ok(finalLogs.some((item) => String(item.message || '').includes('WHATSAPP_TEST_SEND_ALLOWED')));
      assert.ok(finalLogs.some((item) => String(item.message || '').includes('WHATSAPP_DUPLICATE_PREVENTED')));
      assert.ok(finalLogs.some((item) => String(item.message || '').includes('WHATSAPP_SEND_SUCCESS')));
      assert.ok(alertCalls >= 1);
    } finally {
      global.fetch = originalFetch;
    }
  })
);

results.push(
  await run('KEEP_BROWSER_OPEN verhindert automatisches Recovery-Close im Debug-Modus', async () => {
    const automationState = createAutomationState();
    const previousKeepOpen = process.env.WHATSAPP_KEEP_BROWSER_OPEN;
    process.env.WHATSAPP_KEEP_BROWSER_OPEN = '1';

    try {
      await prepareAutomationRuntime(automationState, {
        alertsEnabled: false
      });
      automationState.connected = false;
      automationState.qrRequired = false;
      automationState.sessionExpired = true;

      const healthCheck = await performWhatsappRuntimeAction('health_check');
      const logs = listPublishingLogs({ limit: 40 });

      assert.equal(automationState.stopCount, 0);
      assert.equal(healthCheck.runtime.workerStatus, 'running');
      assert.ok(logs.some((item) => String(item.message || '').includes('WHATSAPP_BROWSER_CLOSE_SKIPPED_DEBUG')));
      assert.ok(logs.some((item) => String(item.message || '').includes('WHATSAPP_WORKER_STOP_REASON')));
    } finally {
      process.env.WHATSAPP_KEEP_BROWSER_OPEN = previousKeepOpen ?? '0';
    }
  })
);

const passed = results.filter(Boolean).length;
const failed = results.length - passed;

if (failed > 0) {
  console.error(`FAILURES ${failed}/${results.length}`);
  process.exitCode = 1;
} else {
  console.log(`OK ${passed} WhatsApp output checks passed`);
}
