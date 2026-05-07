import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';

process.env.WHATSAPP_DELIVERY_ENABLED = '1';
process.env.WHATSAPP_PLAYWRIGHT_HEADLESS = '0';
process.env.WHATSAPP_PLAYWRIGHT_BROWSER_CHANNEL = 'chrome';
process.env.WHATSAPP_PLAYWRIGHT_EXECUTABLE_PATH =
  process.env.WHATSAPP_PLAYWRIGHT_EXECUTABLE_PATH || 'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, '..', '..');
const sessionDir = path.join(repoRoot, 'backend', 'data', 'whatsapp-session');
const logPath = path.join(sessionDir, 'qr-helper.log');
const commandPath = path.join(sessionDir, 'helper-command.json');
const lastCommandResultPath = path.join(sessionDir, 'helper-command-result.json');

fs.mkdirSync(sessionDir, { recursive: true });

function nowIso() {
  return new Date().toISOString();
}

function log(event, payload = {}) {
  fs.appendFileSync(logPath, `[${nowIso()}] ${event} ${JSON.stringify(payload)}\n`);
}

const runtime = await import('../services/whatsappRuntimeService.js');
const { getWhatsappOutputTargetConfig } = await import('../services/whatsappOutputTargetService.js');
const {
  createPublishingEntry,
  getPublishingQueueEntry,
  listPublishingLogs,
  processPublishingQueueEntry
} = await import('../services/publisherService.js');
let commandInFlight = false;

function readCommandFile() {
  if (!fs.existsSync(commandPath)) {
    return null;
  }

  try {
    return JSON.parse(fs.readFileSync(commandPath, 'utf8'));
  } catch (error) {
    log('command_parse_error', {
      message: error instanceof Error ? error.message : String(error)
    });
    return null;
  }
}

function clearCommandFile() {
  if (fs.existsSync(commandPath)) {
    fs.unlinkSync(commandPath);
  }
}

function writeCommandResult(result = {}) {
  fs.writeFileSync(lastCommandResultPath, JSON.stringify(result, null, 2), 'utf8');
}

function formatBerlinTime(value = new Date()) {
  try {
    return new Intl.DateTimeFormat('de-DE', {
      hour: '2-digit',
      minute: '2-digit',
      timeZone: 'Europe/Berlin'
    }).format(value);
  } catch {
    return value.toISOString().slice(11, 16);
  }
}

async function waitForConnected(timeoutMs = 120000) {
  const startedAt = Date.now();

  while (Date.now() - startedAt < timeoutMs) {
    let state = runtime.getWhatsappRuntimeState();
    if (!(state.connectionStatus === 'connected' && state.sessionValid === true)) {
      try {
        const response = await runtime.performWhatsappRuntimeAction('test_connection', {
          preserveLocation: true
        });
        state = response?.runtime || state;
      } catch (error) {
        log('wait_connection_retry', {
          message: error instanceof Error ? error.message : String(error)
        });
      }
    }

    if (state.connectionStatus === 'connected' && state.sessionValid === true) {
      return state;
    }

    await new Promise((resolve) => setTimeout(resolve, 1500));
  }

  throw new Error(`WhatsApp Worker blieb nach ${timeoutMs}ms nicht verbunden.`);
}

function buildSingleTestQueue(target, text) {
  const timestamp = Date.now();
  const syntheticLink = `https://example.local/whatsapp-test/${timestamp}`;

  return createPublishingEntry({
    sourceType: 'output_channel_test',
    payload: {
      title: 'WhatsApp Output Test',
      link: syntheticLink,
      normalizedUrl: syntheticLink,
      asin: `WHTEST${String(timestamp).slice(-6)}`,
      sellerType: 'FBM',
      currentPrice: '0.00',
      oldPrice: '',
      couponCode: '',
      testMode: true,
      skipDealLock: true,
      skipPostedDealHistory: true,
      textByChannel: {
        whatsapp: text
      },
      imageVariants: {},
      targetImageSources: {
        whatsapp: 'none'
      },
      whatsappTargetIds: [target.id],
      whatsappTargetRefs: [target.targetRef]
    },
    targets: [
      {
        channelType: 'whatsapp',
        isEnabled: true,
        imageSource: 'none',
        targetRef: target.targetRef,
        targetLabel: target.targetLabel || target.name,
        targetMeta: {
          targetId: target.id,
          name: target.name,
          targetRef: target.targetRef,
          targetType: target.targetType,
          channelUrl: target.channelUrl || target.targetRef,
          requiresManualActivation: target.requiresManualActivation === true,
          isSystem: target.isSystem === true
        }
      }
    ],
    originOverride: 'manual',
    skipDealLock: true
  });
}

async function runStartupCommandIfRequested() {
  const command = readCommandFile();
  if (!command) {
    return null;
  }

  return await executeHelperCommand(command);
}

function cleanText(value = '') {
  return typeof value === 'string' ? value.trim() : '';
}

async function executeSendTestCommand(command = {}) {
  const state = await waitForConnected(Number(command.waitTimeoutMs || 120000));
  const config = getWhatsappOutputTargetConfig();
  const targetId = Number(command.targetId || 0);
  const target = config.targets.find((item) => item.id === targetId);

  if (!target) {
    throw new Error(`WhatsApp Ziel ${targetId} nicht gefunden.`);
  }
  if (target.targetType !== 'WHATSAPP_TEST_CHANNEL') {
    throw new Error(`WhatsApp Ziel ${target.name} ist kein Test-Kanal.`);
  }
  if (target.isActive !== true || target.useForPublishing !== true) {
    throw new Error(`WhatsApp Test-Kanal ${target.name} ist nicht aktiv freigegeben.`);
  }

  const preparedText = String(command.text || '').replaceAll('{time}', formatBerlinTime(new Date()));
  const queue = buildSingleTestQueue(target, preparedText);
  log('command_queue_created', {
    queueId: queue.id,
    targetId: target.id,
    connectionStatus: state.connectionStatus,
    sessionValid: state.sessionValid
  });

  const processed = await processPublishingQueueEntry(queue.id);
  const queueAfter = getPublishingQueueEntry(queue.id);
  const logs = listPublishingLogs({ limit: 40 }).filter((item) => item.queue_id === queue.id);
  return {
    commandId: cleanText(command.commandId) || crypto.randomUUID(),
    queueId: queue.id,
    processed,
    queueAfter,
    logs: logs.map((item) => ({
      created_at: item.created_at,
      event_type: item.event_type,
      level: item.level,
      message: item.message
    }))
  };
}

function buildManualDebugPayload(command = {}) {
  return {
    targetId: Number(command.targetId || 0) || undefined,
    targetRef: cleanText(command.targetRef),
    targetLabel: cleanText(command.targetLabel),
    channelUrl: cleanText(command.channelUrl) || cleanText(command.targetRef),
    waitTimeoutMs: Number(command.waitTimeoutMs || 120000),
    pollIntervalMs: Number(command.pollIntervalMs || 1500)
  };
}

async function executeGenericRuntimeCommand(command = {}) {
  const action = cleanText(command.action).toLowerCase();
  const payload = command.payload && typeof command.payload === 'object' ? { ...command.payload } : {};

  if (action === 'manual_channel_debug_wait' || action === 'manual_channel_debug_capture') {
    await waitForConnected(Number(command.waitTimeoutMs || payload.waitTimeoutMs || 120000));
    Object.assign(payload, buildManualDebugPayload({
      ...command,
      ...payload
    }));
  }

  if (action === 'test_connection' || action === 'health_check') {
    payload.preserveLocation = true;
  }

  return await runtime.performWhatsappRuntimeAction(action, payload);
}

async function executeHelperCommand(command = {}) {
  const action = cleanText(command.action).toLowerCase();
  if (!action) {
    return null;
  }

  const commandToken = cleanText(command.commandId) || crypto.randomUUID();
  clearCommandFile();
  commandInFlight = true;
  log('command_start', {
    action,
    commandId: cleanText(command.commandId),
    targetId: Number(command.targetId || 0) || null,
    targetRef: cleanText(command.targetRef),
    waitTimeoutMs: Number(command.waitTimeoutMs || 0) || null
  });

  try {
    const result = action === 'send_test' ? await executeSendTestCommand(command) : await executeGenericRuntimeCommand(command);
    writeCommandResult({
      ok: true,
      action,
      commandId: cleanText(command.commandId) || commandToken,
      executedAt: nowIso(),
      result
    });
    log('command_done', {
      action,
      commandId: cleanText(command.commandId) || commandToken
    });
    return result;
  } catch (error) {
    const result = {
      ok: false,
      action,
      commandId: cleanText(command.commandId) || commandToken,
      error: error instanceof Error ? error.message : String(error),
      code: error instanceof Error ? cleanText(error.code) : '',
      failedAt: nowIso()
    };
    writeCommandResult(result);
    log('command_error', result);
    throw error;
  } finally {
    commandInFlight = false;
  }
}

async function pollCommands() {
  if (commandInFlight || !fs.existsSync(commandPath)) {
    return;
  }

  const command = readCommandFile();
  if (!command) {
    return;
  }

  await executeHelperCommand(command).catch(() => null);
}

async function pollHealth() {
  if (commandInFlight) {
    return;
  }

  try {
    const state = await runtime.runWhatsappHealthCheck({ manual: false });
    log('health', {
      connectionStatus: state?.connectionStatus || '',
      sessionValid: state?.sessionValid === true,
      qrRequired: state?.qrRequired === true,
      workerStatus: state?.workerStatus || '',
      browserStatus: state?.browserStatus || '',
      channelReachable: state?.channelReachable === true
    });
  } catch (error) {
    log('health_error', {
      message: error instanceof Error ? error.message : String(error)
    });
  }
}

async function shutdown(signal = '') {
  log('shutdown', { signal });
  try {
    await runtime.performWhatsappRuntimeAction('stop_worker');
  } catch (error) {
    log('stop_error', {
      message: error instanceof Error ? error.message : String(error)
    });
  }
  process.exit(0);
}

process.on('SIGINT', () => {
  void shutdown('SIGINT');
});
process.on('SIGTERM', () => {
  void shutdown('SIGTERM');
});

log('helper_start');
runtime.saveWhatsappRuntimeSettings({
  workerEnabled: true,
  alertsEnabled: true,
  alertTargetRef: '@WhatsappStatusFehler'
});

const started = await runtime.performWhatsappRuntimeAction('start_worker');
log('worker_started', started?.runtime || {});
await runStartupCommandIfRequested().catch((error) => {
  const result = {
    ok: false,
    error: error instanceof Error ? error.message : String(error),
    failedAt: nowIso()
  };
  writeCommandResult(result);
  log('command_error', result);
});

const commandInterval = setInterval(() => {
  void pollCommands();
}, 1000);

commandInterval.unref?.();

const interval = setInterval(() => {
  void pollHealth();
}, 5000);

interval.unref?.();

await pollHealth();

setInterval(() => {}, 2147483647);
