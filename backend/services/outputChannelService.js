import { getDb } from '../db.js';
import { getTelegramConfig, getTelegramTestGroupConfig, getWhatsappDeliveryConfig } from '../env.js';
import { DEFAULT_LIVE_CHANNEL_CHAT_ID, getTelegramBotClientConfig } from './telegramBotClientService.js';
import { getWhatsappRuntimeConfig, getWhatsappRuntimeState } from './whatsappRuntimeService.js';

const db = getDb();

export const OUTPUT_DISABLED_SKIP = 'OUTPUT_DISABLED_SKIP';

function nowIso() {
  return new Date().toISOString();
}

function cleanText(value = '') {
  return typeof value === 'string' ? value.trim() : '';
}

function parseBool(value, fallback = false) {
  if (value === true || value === false) {
    return value;
  }

  if (value === 1 || value === '1' || value === 'true') {
    return true;
  }

  if (value === 0 || value === '0' || value === 'false') {
    return false;
  }

  return fallback;
}

function parseJson(value, fallback = null) {
  if (!value) {
    return fallback;
  }

  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function stringifyJson(value) {
  return JSON.stringify(value ?? null);
}

function normalizeChannelType(value = '') {
  const normalized = cleanText(value).toLowerCase();
  if (['test', 'live', 'review', 'standard'].includes(normalized)) {
    return normalized;
  }

  return 'standard';
}

function isWhatsappTestTargetType(value = '') {
  return cleanText(value).toUpperCase() === 'WHATSAPP_TEST_CHANNEL';
}

function buildWhatsappSeedDescriptor(target = {}) {
  const targetType = cleanText(target.targetType || target.target_type).toUpperCase() || 'WHATSAPP_CHANNEL';
  const requiresManualActivation = target.requiresManualActivation === true || target.requires_manual_activation === 1;

  if (isWhatsappTestTargetType(targetType)) {
    return {
      channelType: 'test',
      allowLiveMode: true,
      isDangerousLive: false,
      statusHint: 'WhatsApp Test-Kanal darf fuer Testposts und Queue-Sends aktiv genutzt werden.'
    };
  }

  if (requiresManualActivation) {
    return {
      channelType: 'live',
      allowLiveMode: false,
      isDangerousLive: true,
      statusHint: 'WhatsApp Live-Kanal bleibt standardmaessig deaktiviert.'
    };
  }

  return {
    channelType: 'standard',
    allowLiveMode: true,
    isDangerousLive: false,
    statusHint: 'Sendet ueber das konfigurierte WhatsApp Gateway.'
  };
}

function normalizeKeyPart(value = '', fallback = 'default') {
  const normalized = cleanText(String(value || '')).toLowerCase();
  if (!normalized) {
    return fallback;
  }

  return normalized.replace(/[^a-z0-9@._:-]+/g, '-');
}

function buildChannelKey(platform, scope, ref = 'default') {
  return [normalizeKeyPart(platform), normalizeKeyPart(scope), normalizeKeyPart(ref)].join(':');
}

function isDefaultLiveTelegramChannel(value = '') {
  return cleanText(value).toLowerCase() === cleanText(DEFAULT_LIVE_CHANNEL_CHAT_ID).toLowerCase();
}

function isApprovedRouteDangerousLive(targetRef = '', targetLabel = '') {
  return isDefaultLiveTelegramChannel(targetRef) || isDefaultLiveTelegramChannel(targetLabel);
}

function buildApprovedRouteSeedConfig(approvedRef = '', approvedLabel = '') {
  const dangerousLive = isApprovedRouteDangerousLive(approvedRef, approvedLabel);

  return {
    channelLabel: approvedLabel || (dangerousLive ? 'Telegram Live Kanal' : 'Telegram Veroeffentlicht'),
    channelType: dangerousLive ? 'live' : 'standard',
    isEnabled: dangerousLive ? false : true,
    allowTestMode: dangerousLive ? false : true,
    allowLiveMode: true,
    isDangerousLive: dangerousLive,
    statusHint: dangerousLive
      ? 'Live Kanal bleibt standardmaessig deaktiviert.'
      : 'Freigegebene Deals laufen hier getrennt vom echten Live-Hauptkanal.',
    meta: {
      routeKind: 'approved',
      classification: dangerousLive ? 'dangerous_live' : 'approved_output',
      schemaVersion: 2
    }
  };
}

function normalizeAllowedSourceTypes(value, fallback = ['*']) {
  const values = Array.isArray(value)
    ? value
    : typeof value === 'string'
      ? value.split(',')
      : [];

  const normalized = Array.from(
    new Set(
      values
        .map((item) => cleanText(String(item || '')).toLowerCase())
        .filter(Boolean)
    )
  );

  return normalized.length ? normalized : fallback;
}

function ensureAppSettingsRow() {
  const existing = db.prepare(`SELECT id FROM app_settings WHERE id = 1`).get();
  if (existing) {
    return;
  }

  db.prepare(
    `
      INSERT INTO app_settings (
        id,
        repostCooldownEnabled,
        repostCooldownHours,
        telegramCopyButtonText,
        copybotEnabled,
        outputQueueEnabled,
        telegramBotEnabled,
        telegramBotDefaultRetryLimit,
        facebookEnabled,
        facebookSessionMode,
        facebookDefaultRetryLimit,
        facebookDefaultTarget,
        telegramReaderGroupSlotCount,
        schedulerBootstrapVersion
      ) VALUES (1, 1, 12, 'Zum Kopieren hier klicken', 0, 1, 1, 3, 0, 'persistent', 3, NULL, 10, 0)
    `
  ).run();
}

function readAppSettings() {
  ensureAppSettingsRow();
  return db.prepare(`SELECT * FROM app_settings WHERE id = 1`).get() || null;
}

function getPlatformStatus(platform = '') {
  const settings = readAppSettings();
  const telegramConfig = getTelegramConfig();
  const whatsappConfig = getWhatsappDeliveryConfig();

  if (platform === 'telegram') {
    const tokenConfigured = Boolean(cleanText(telegramConfig.token));
    return {
      label: 'Telegram',
      active: settings?.telegramBotEnabled === 1 && tokenConfigured,
      enabled: settings?.telegramBotEnabled === 1,
      configured: tokenConfigured,
      detail: tokenConfigured ? 'Bot Token vorhanden' : 'Bot Token fehlt'
    };
  }

  if (platform === 'whatsapp') {
    const runtime = getWhatsappRuntimeState();
    const runtimeConfig = getWhatsappRuntimeConfig();
    const providerConfigured = runtime.providerConfigured === true || runtimeConfig.providerConfigured === true;
    const enabled = whatsappConfig.enabled === true;
    return {
      label: 'WhatsApp',
      active:
        enabled &&
        providerConfigured &&
        runtime.workerEnabled === true &&
        runtime.sessionValid === true &&
        runtime.connectionStatus === 'connected',
      enabled,
      configured: providerConfigured,
      detail:
        runtime.connectionStatus === 'connected'
          ? 'Session verbunden'
          : runtime.connectionStatus === 'qr_required'
            ? 'QR Login erforderlich'
            : runtime.connectionStatus === 'session_expired'
              ? 'Session abgelaufen'
              : providerConfigured
                ? runtime.providerLabel || 'Worker vorbereitet'
                : 'Browser oder Gateway fehlt'
    };
  }

  if (platform === 'facebook') {
    const enabled = settings?.facebookEnabled === 1;
    const defaultTargetConfigured = Boolean(cleanText(settings?.facebookDefaultTarget));
    return {
      label: 'Facebook',
      active: enabled,
      enabled,
      configured: defaultTargetConfigured,
      detail: defaultTargetConfigured ? 'Default Ziel gesetzt' : 'Default Ziel offen'
    };
  }

  return {
    label: cleanText(platform) || 'Output',
    active: false,
    enabled: false,
    configured: false,
    detail: 'Unbekannte Plattform'
  };
}

function mapOutputChannelRow(row = {}) {
  return {
    id: Number(row.id),
    channelKey: cleanText(row.channel_key),
    platform: cleanText(row.platform),
    channelLabel: cleanText(row.channel_label) || cleanText(row.target_label) || 'Output Kanal',
    channelType: normalizeChannelType(row.channel_type),
    sourceKind: cleanText(row.source_kind) || 'manual',
    targetRef: cleanText(row.target_ref),
    targetLabel: cleanText(row.target_label),
    isEnabled: row.is_enabled === 1,
    isBlocked: row.is_blocked === 1,
    allowTestMode: row.allow_test_mode === 1,
    allowLiveMode: row.allow_live_mode === 1,
    isDangerousLive: row.is_dangerous_live === 1,
    allowedSourceTypes: normalizeAllowedSourceTypes(parseJson(row.allowed_source_types_json, ['*']), ['*']),
    notes: cleanText(row.notes),
    statusHint: cleanText(row.status_hint),
    lastStatus: cleanText(row.last_status) || 'idle',
    lastSentAt: row.last_sent_at || null,
    lastErrorAt: row.last_error_at || null,
    lastErrorMessage: cleanText(row.last_error_message),
    lastQueueId: Number.isFinite(Number(row.last_queue_id)) ? Number(row.last_queue_id) : null,
    lastTargetId: Number.isFinite(Number(row.last_target_id)) ? Number(row.last_target_id) : null,
    lastEventType: cleanText(row.last_event_type),
    lastMessagePreview: cleanText(row.last_message_preview),
    meta: parseJson(row.meta_json, null),
    sortOrder: Number(row.sort_order || 100),
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null
  };
}

function getOutputChannelByKeyInternal(channelKey = '') {
  const normalizedKey = cleanText(channelKey);
  if (!normalizedKey) {
    return null;
  }

  const row = db.prepare(`SELECT * FROM output_channels WHERE channel_key = ?`).get(normalizedKey) || null;
  return row ? mapOutputChannelRow(row) : null;
}

function upsertSeedChannel(seed = {}) {
  const timestamp = nowIso();
  const channelKey = cleanText(seed.channelKey);

  if (!channelKey) {
    return null;
  }

  const existing = getOutputChannelByKeyInternal(channelKey);
  const normalizedChannelType = normalizeChannelType(seed.channelType);
  const payload = {
    channelKey,
    platform: cleanText(seed.platform) || 'telegram',
    channelLabel: cleanText(seed.channelLabel) || cleanText(seed.targetLabel) || 'Output Kanal',
    channelType: normalizedChannelType,
    sourceKind: cleanText(seed.sourceKind) || 'manual',
    targetRef: cleanText(seed.targetRef) || null,
    targetLabel: cleanText(seed.targetLabel) || cleanText(seed.channelLabel) || null,
    isEnabled: parseBool(seed.isEnabled, normalizedChannelType !== 'live'),
    isBlocked: parseBool(seed.isBlocked, false),
    allowTestMode: parseBool(seed.allowTestMode, true),
    allowLiveMode: parseBool(seed.allowLiveMode, true),
    isDangerousLive: parseBool(seed.isDangerousLive, false),
    allowedSourceTypesJson: stringifyJson(normalizeAllowedSourceTypes(seed.allowedSourceTypes, ['*'])),
    notes: cleanText(seed.notes) || null,
    statusHint: cleanText(seed.statusHint) || null,
    metaJson: seed.meta === undefined ? null : stringifyJson(seed.meta),
    sortOrder: Number.isFinite(Number(seed.sortOrder)) ? Number(seed.sortOrder) : 100,
    updatedAt: timestamp
  };

  if (existing) {
    db.prepare(
      `
        UPDATE output_channels
        SET platform = @platform,
            channel_label = @channelLabel,
            channel_type = @channelType,
            source_kind = @sourceKind,
            target_ref = @targetRef,
            target_label = @targetLabel,
            is_dangerous_live = CASE
              WHEN is_dangerous_live = 1 OR @isDangerousLive = 1 THEN 1
              ELSE 0
            END,
            allowed_source_types_json = @allowedSourceTypesJson,
            status_hint = CASE
              WHEN @statusHint IS NOT NULL AND @statusHint != '' THEN @statusHint
              ELSE status_hint
            END,
            meta_json = CASE
              WHEN @metaJson IS NOT NULL THEN @metaJson
              ELSE meta_json
            END,
            sort_order = @sortOrder,
            updated_at = @updatedAt
        WHERE channel_key = @channelKey
      `
    ).run({
      ...payload,
      isDangerousLive: payload.isDangerousLive ? 1 : 0
    });

    return getOutputChannelByKeyInternal(channelKey);
  }

  db.prepare(
    `
      INSERT INTO output_channels (
        channel_key,
        platform,
        channel_label,
        channel_type,
        source_kind,
        target_ref,
        target_label,
        is_enabled,
        is_blocked,
        allow_test_mode,
        allow_live_mode,
        is_dangerous_live,
        allowed_source_types_json,
        notes,
        status_hint,
        last_status,
        last_sent_at,
        last_error_at,
        last_error_message,
        last_queue_id,
        last_target_id,
        last_event_type,
        last_message_preview,
        meta_json,
        sort_order,
        created_at,
        updated_at
      ) VALUES (
        @channelKey,
        @platform,
        @channelLabel,
        @channelType,
        @sourceKind,
        @targetRef,
        @targetLabel,
        @isEnabled,
        @isBlocked,
        @allowTestMode,
        @allowLiveMode,
        @isDangerousLive,
        @allowedSourceTypesJson,
        @notes,
        @statusHint,
        'idle',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        @metaJson,
        @sortOrder,
        @createdAt,
        @updatedAt
      )
    `
  ).run({
    ...payload,
    isEnabled: payload.isEnabled ? 1 : 0,
    isBlocked: payload.isBlocked ? 1 : 0,
    allowTestMode: payload.allowTestMode ? 1 : 0,
    allowLiveMode: payload.allowLiveMode ? 1 : 0,
    isDangerousLive: payload.isDangerousLive ? 1 : 0,
    createdAt: timestamp
  });

  return getOutputChannelByKeyInternal(channelKey);
}

function syncManagedSeedChannel(seed = {}) {
  const channelKey = cleanText(seed.channelKey);
  const sourceKind = cleanText(seed.sourceKind);
  if (!channelKey || sourceKind !== 'whatsapp_output_target') {
    return getOutputChannelByKeyInternal(channelKey);
  }

  const existing = getOutputChannelByKeyInternal(channelKey);
  if (!existing) {
    return null;
  }

  const normalizedChannelType = normalizeChannelType(seed.channelType);
  const desiredTargetRef = cleanText(seed.targetRef) || '';
  const desiredTargetLabel = cleanText(seed.targetLabel) || cleanText(seed.channelLabel) || '';
  const desiredStatusHint = cleanText(seed.statusHint) || '';
  const desiredAllowedSourceTypes = normalizeAllowedSourceTypes(seed.allowedSourceTypes, ['*']);
  const desiredMetaJson = seed.meta === undefined ? stringifyJson(existing.meta) : stringifyJson(seed.meta);
  const desiredEnabled = parseBool(seed.isEnabled, normalizedChannelType !== 'live');
  const desiredAllowTestMode = parseBool(seed.allowTestMode, true);
  const desiredAllowLiveMode = parseBool(seed.allowLiveMode, true);
  const desiredDangerousLive = parseBool(seed.isDangerousLive, false);
  const desiredSortOrder = Number.isFinite(Number(seed.sortOrder)) ? Number(seed.sortOrder) : existing.sortOrder;

  const needsSync =
    existing.channelType !== normalizedChannelType ||
    existing.targetRef !== desiredTargetRef ||
    existing.targetLabel !== desiredTargetLabel ||
    existing.isEnabled !== desiredEnabled ||
    existing.allowTestMode !== desiredAllowTestMode ||
    existing.allowLiveMode !== desiredAllowLiveMode ||
    existing.isDangerousLive !== desiredDangerousLive ||
    cleanText(existing.statusHint) !== desiredStatusHint ||
    JSON.stringify(existing.allowedSourceTypes || ['*']) !== JSON.stringify(desiredAllowedSourceTypes) ||
    Number(existing.sortOrder || 0) !== desiredSortOrder ||
    stringifyJson(existing.meta) !== desiredMetaJson;

  if (!needsSync) {
    return existing;
  }

  db.prepare(
    `
      UPDATE output_channels
      SET channel_label = @channelLabel,
          channel_type = @channelType,
          source_kind = @sourceKind,
          target_ref = @targetRef,
          target_label = @targetLabel,
          is_enabled = @isEnabled,
          allow_test_mode = @allowTestMode,
          allow_live_mode = @allowLiveMode,
          is_dangerous_live = @isDangerousLive,
          allowed_source_types_json = @allowedSourceTypesJson,
          status_hint = @statusHint,
          meta_json = @metaJson,
          sort_order = @sortOrder,
          updated_at = @updatedAt
      WHERE channel_key = @channelKey
    `
  ).run({
    channelKey,
    channelLabel: cleanText(seed.channelLabel) || desiredTargetLabel || 'WhatsApp Output',
    channelType: normalizedChannelType,
    sourceKind,
    targetRef: desiredTargetRef || null,
    targetLabel: desiredTargetLabel || null,
    isEnabled: desiredEnabled ? 1 : 0,
    allowTestMode: desiredAllowTestMode ? 1 : 0,
    allowLiveMode: desiredAllowLiveMode ? 1 : 0,
    isDangerousLive: desiredDangerousLive ? 1 : 0,
    allowedSourceTypesJson: stringifyJson(desiredAllowedSourceTypes),
    statusHint: desiredStatusHint || null,
    metaJson: desiredMetaJson,
    sortOrder: desiredSortOrder,
    updatedAt: nowIso()
  });

  return getOutputChannelByKeyInternal(channelKey);
}

function migrateLegacyApprovedRouteChannel(seed = {}) {
  const channelKey = cleanText(seed.channelKey);
  if (!channelKey || cleanText(seed.sourceKind) !== 'env_approved_route') {
    return null;
  }

  const approvedConfig = buildApprovedRouteSeedConfig(seed.targetRef, seed.targetLabel || seed.channelLabel);
  if (approvedConfig.isDangerousLive) {
    return getOutputChannelByKeyInternal(channelKey);
  }

  const existing = getOutputChannelByKeyInternal(channelKey);
  if (!existing) {
    return null;
  }

  const classificationAlreadySafe =
    existing.channelType === approvedConfig.channelType &&
    existing.allowTestMode === approvedConfig.allowTestMode &&
    existing.allowLiveMode === approvedConfig.allowLiveMode &&
    existing.isDangerousLive === approvedConfig.isDangerousLive;

  if (classificationAlreadySafe) {
    return existing;
  }

  db.prepare(
    `
      UPDATE output_channels
      SET channel_label = @channelLabel,
          channel_type = @channelType,
          is_enabled = 1,
          allow_test_mode = 1,
          allow_live_mode = 1,
          is_dangerous_live = 0,
          status_hint = @statusHint,
          meta_json = @metaJson,
          updated_at = @updatedAt
      WHERE channel_key = @channelKey
    `
  ).run({
    channelKey,
    channelLabel: approvedConfig.channelLabel,
    channelType: approvedConfig.channelType,
    statusHint: approvedConfig.statusHint,
    metaJson: stringifyJson(approvedConfig.meta),
    updatedAt: nowIso()
  });

  return getOutputChannelByKeyInternal(channelKey);
}

function seedTelegramOutputChannels() {
  getTelegramBotClientConfig();

  const telegramConfig = getTelegramConfig();
  const testGroupConfig = getTelegramTestGroupConfig();
  const testGroupRef = cleanText(testGroupConfig.chatId) || cleanText(telegramConfig.chatId);
  const approvedRef = cleanText(process.env.TELEGRAM_APPROVED_CHANNEL_ID) || cleanText(process.env.TELEGRAM_APPROVED_CHANNEL_USERNAME);
  const rejectedRef = cleanText(process.env.TELEGRAM_REJECTED_CHANNEL_ID) || cleanText(process.env.TELEGRAM_REJECTED_CHANNEL_USERNAME);
  const seeds = [];

  if (testGroupRef) {
    seeds.push({
      channelKey: buildChannelKey('telegram', 'test-group', testGroupRef),
      platform: 'telegram',
      channelLabel: 'Telegram Testgruppe',
      channelType: 'test',
      sourceKind: 'env_test_group',
      targetRef: testGroupRef,
      targetLabel: 'Telegram Testgruppe',
      isEnabled: true,
      allowTestMode: true,
      allowLiveMode: true,
      isDangerousLive: false,
      allowedSourceTypes: ['*'],
      sortOrder: 10,
      statusHint: 'Sichere Testgruppe fuer kontrollierte Ausgaben.'
    });
  }

  if (approvedRef) {
    const approvedSeedConfig = buildApprovedRouteSeedConfig(
      approvedRef,
      cleanText(process.env.TELEGRAM_APPROVED_CHANNEL_USERNAME) || approvedRef
    );
    seeds.push({
      channelKey: buildChannelKey('telegram', 'approved-route', approvedRef),
      platform: 'telegram',
      channelLabel: approvedSeedConfig.channelLabel,
      channelType: approvedSeedConfig.channelType,
      sourceKind: 'env_approved_route',
      targetRef: approvedRef,
      targetLabel: cleanText(process.env.TELEGRAM_APPROVED_CHANNEL_USERNAME) || approvedRef,
      isEnabled: approvedSeedConfig.isEnabled,
      allowTestMode: approvedSeedConfig.allowTestMode,
      allowLiveMode: approvedSeedConfig.allowLiveMode,
      isDangerousLive: approvedSeedConfig.isDangerousLive,
      allowedSourceTypes: ['generator_direct_approved_route', 'output_channel_test'],
      sortOrder: 20,
      statusHint: approvedSeedConfig.statusHint,
      meta: approvedSeedConfig.meta
    });
  }

  if (rejectedRef) {
    seeds.push({
      channelKey: buildChannelKey('telegram', 'rejected-route', rejectedRef),
      platform: 'telegram',
      channelLabel: cleanText(process.env.TELEGRAM_REJECTED_CHANNEL_USERNAME) || 'Telegram Review Kanal',
      channelType: 'review',
      sourceKind: 'env_rejected_route',
      targetRef: rejectedRef,
      targetLabel: cleanText(process.env.TELEGRAM_REJECTED_CHANNEL_USERNAME) || rejectedRef,
      isEnabled: true,
      allowTestMode: true,
      allowLiveMode: true,
      isDangerousLive: false,
      allowedSourceTypes: ['generator_direct_rejected_route', 'output_channel_test'],
      sortOrder: 30,
      statusHint: 'Review und Block-Posts werden hier isoliert.'
    });
  }

  const botTargets = db.prepare(`SELECT * FROM telegram_bot_targets ORDER BY id ASC`).all();
  botTargets.forEach((row, index) => {
    const targetRef = cleanText(row.chat_id);
    if (!targetRef) {
      return;
    }

    const channelType =
      row.channel_kind === 'live'
        ? 'live'
        : row.channel_kind === 'review'
          ? 'review'
          : row.channel_kind === 'test'
            ? 'test'
            : 'standard';
    seeds.push({
      channelKey: buildChannelKey('telegram', 'bot-target', row.id),
      platform: 'telegram',
      channelLabel: cleanText(row.name) || `Telegram Ziel ${row.id}`,
      channelType,
      sourceKind: 'telegram_bot_target',
      targetRef,
      targetLabel: cleanText(row.name) || targetRef,
      isEnabled: row.is_active === 1 && row.use_for_publishing === 1,
      allowTestMode: true,
      allowLiveMode: channelType === 'live' ? false : true,
      isDangerousLive: channelType === 'live',
      allowedSourceTypes: ['*'],
      sortOrder: 40 + index
    });
  });

  seeds.forEach((seed) => {
    upsertSeedChannel(seed);
    migrateLegacyApprovedRouteChannel(seed);
  });
}

function seedPlatformOutputChannels() {
  const whatsappTargets = db.prepare(`SELECT * FROM whatsapp_output_targets ORDER BY id ASC`).all();

  if (whatsappTargets.length) {
    whatsappTargets.forEach((row, index) => {
      const descriptor = buildWhatsappSeedDescriptor(row);
      const seed = {
        channelKey: buildChannelKey('whatsapp', 'target', row.id),
        platform: 'whatsapp',
        channelLabel: cleanText(row.name) || cleanText(row.target_label) || `WhatsApp Ziel ${row.id}`,
        channelType: descriptor.channelType,
        sourceKind: 'whatsapp_output_target',
        targetRef: cleanText(row.target_ref) || null,
        targetLabel: cleanText(row.target_label) || cleanText(row.name) || `WhatsApp Ziel ${row.id}`,
        isEnabled: row.is_active === 1 && row.use_for_publishing === 1,
        allowTestMode: true,
        allowLiveMode: descriptor.allowLiveMode,
        isDangerousLive: descriptor.isDangerousLive,
        allowedSourceTypes: ['*'],
        sortOrder: Number(row.sort_order || 110 + index),
        statusHint: descriptor.statusHint,
        meta: {
          targetType: cleanText(row.target_type).toUpperCase() || 'WHATSAPP_CHANNEL',
          channelUrl: cleanText(row.channel_url) || cleanText(row.target_ref),
          targetId: Number(row.id),
          requiresManualActivation: row.requires_manual_activation === 1
        }
      };
      upsertSeedChannel(seed);
      syncManagedSeedChannel(seed);
    });
  } else {
    upsertSeedChannel({
      channelKey: buildChannelKey('whatsapp', 'default'),
      platform: 'whatsapp',
      channelLabel: 'WhatsApp Output',
      channelType: 'standard',
      sourceKind: 'env_whatsapp',
      targetRef: cleanText(getWhatsappDeliveryConfig().sender) || null,
      targetLabel: 'WhatsApp Gateway',
      isEnabled: true,
      allowTestMode: true,
      allowLiveMode: true,
      allowedSourceTypes: ['*'],
      sortOrder: 110,
      statusHint: 'Sendet ueber das konfigurierte WhatsApp Gateway.'
    });
  }

  upsertSeedChannel({
    channelKey: buildChannelKey('facebook', 'default'),
    platform: 'facebook',
    channelLabel: 'Facebook Output',
    channelType: 'standard',
    sourceKind: 'app_settings',
    targetRef: cleanText(readAppSettings()?.facebookDefaultTarget) || null,
    targetLabel: cleanText(readAppSettings()?.facebookDefaultTarget) || 'Facebook Default Ziel',
    isEnabled: true,
    allowTestMode: true,
    allowLiveMode: true,
    allowedSourceTypes: ['*'],
    sortOrder: 210,
    statusHint: 'Facebook Worker nutzt dieses Default Ziel.'
  });
}

function seedOutputChannels() {
  seedTelegramOutputChannels();
  seedPlatformOutputChannels();
}

function buildDynamicSeedForTarget({
  platform = '',
  queueSourceType = '',
  targetRef = '',
  targetLabel = '',
  targetMeta = null,
  payload = {}
} = {}) {
  const normalizedPlatform = cleanText(platform).toLowerCase();
  const normalizedSourceType = cleanText(queueSourceType).toLowerCase();
  const normalizedTargetRef = cleanText(targetRef);
  const normalizedTargetLabel = cleanText(targetLabel) || normalizedTargetRef || `${normalizedPlatform} output`;
  const telegramConfig = getTelegramConfig();
  const testGroupConfig = getTelegramTestGroupConfig();

  if (normalizedPlatform === 'telegram') {
    if (targetMeta && Number.isFinite(Number(targetMeta.targetId))) {
      const targetKind = cleanText(targetMeta.targetKind || targetMeta.channelKind).toLowerCase();
      const channelType = targetKind === 'live' ? 'live' : targetKind === 'review' ? 'review' : 'test';
      return {
        channelKey: buildChannelKey('telegram', 'bot-target', Number(targetMeta.targetId)),
        platform: 'telegram',
        channelLabel: normalizedTargetLabel || normalizedTargetRef || 'Telegram Ziel',
        channelType,
        sourceKind: 'telegram_bot_target',
        targetRef: normalizedTargetRef,
        targetLabel: normalizedTargetLabel,
        isEnabled: true,
        allowTestMode: true,
        allowLiveMode: channelType === 'live' ? false : true,
        isDangerousLive: channelType === 'live',
        allowedSourceTypes: ['*'],
        sortOrder: 60
      };
    }

    if (normalizedSourceType.includes('approved_route')) {
      const approvedSeedConfig = buildApprovedRouteSeedConfig(
        normalizedTargetRef || normalizedTargetLabel,
        normalizedTargetLabel
      );
      return {
        channelKey: buildChannelKey('telegram', 'approved-route', normalizedTargetRef || normalizedTargetLabel),
        platform: 'telegram',
        channelLabel: approvedSeedConfig.channelLabel,
        channelType: approvedSeedConfig.channelType,
        sourceKind: 'dynamic_approved_route',
        targetRef: normalizedTargetRef,
        targetLabel: normalizedTargetLabel,
        isEnabled: approvedSeedConfig.isEnabled,
        allowTestMode: approvedSeedConfig.allowTestMode,
        allowLiveMode: approvedSeedConfig.allowLiveMode,
        isDangerousLive: approvedSeedConfig.isDangerousLive,
        allowedSourceTypes: ['generator_direct_approved_route', 'output_channel_test'],
        sortOrder: 25,
        statusHint: approvedSeedConfig.statusHint,
        meta: approvedSeedConfig.meta
      };
    }

    if (normalizedSourceType.includes('rejected_route')) {
      return {
        channelKey: buildChannelKey('telegram', 'rejected-route', normalizedTargetRef || normalizedTargetLabel),
        platform: 'telegram',
        channelLabel: normalizedTargetLabel || 'Telegram Review Kanal',
        channelType: 'review',
        sourceKind: 'dynamic_rejected_route',
        targetRef: normalizedTargetRef,
        targetLabel: normalizedTargetLabel,
        isEnabled: true,
        allowTestMode: true,
        allowLiveMode: true,
        isDangerousLive: false,
        allowedSourceTypes: ['generator_direct_rejected_route', 'output_channel_test'],
        sortOrder: 35
      };
    }

    const testGroupRef = cleanText(testGroupConfig.chatId) || cleanText(telegramConfig.chatId);
    if (normalizedTargetRef && normalizedTargetRef === testGroupRef) {
      return {
        channelKey: buildChannelKey('telegram', 'test-group', normalizedTargetRef),
        platform: 'telegram',
        channelLabel: 'Telegram Testgruppe',
        channelType: 'test',
        sourceKind: 'env_test_group',
        targetRef: normalizedTargetRef,
        targetLabel: normalizedTargetLabel || 'Telegram Testgruppe',
        isEnabled: true,
        allowTestMode: true,
        allowLiveMode: true,
        isDangerousLive: false,
        allowedSourceTypes: ['*'],
        sortOrder: 10
      };
    }

    return {
      channelKey: buildChannelKey('telegram', 'target', normalizedTargetRef || normalizedTargetLabel),
      platform: 'telegram',
      channelLabel: normalizedTargetLabel || 'Telegram Output',
      channelType: payload?.testMode === true ? 'test' : 'standard',
      sourceKind: 'dynamic_target',
      targetRef: normalizedTargetRef,
      targetLabel: normalizedTargetLabel,
      isEnabled: true,
      allowTestMode: true,
      allowLiveMode: true,
      isDangerousLive: false,
      allowedSourceTypes: ['*'],
      sortOrder: 70
    };
  }

  if (normalizedPlatform === 'whatsapp') {
    const descriptor = buildWhatsappSeedDescriptor({
      targetType: targetMeta?.targetType,
      requiresManualActivation: targetMeta?.requiresManualActivation === true
    });
    if (targetMeta && Number.isFinite(Number(targetMeta.targetId))) {
      return {
        channelKey: buildChannelKey('whatsapp', 'target', Number(targetMeta.targetId)),
        platform: 'whatsapp',
        channelLabel: normalizedTargetLabel || normalizedTargetRef || 'WhatsApp Output',
        channelType: descriptor.channelType,
        sourceKind: 'whatsapp_output_target',
        targetRef: normalizedTargetRef || null,
        targetLabel: normalizedTargetLabel || normalizedTargetRef || 'WhatsApp Gateway',
        isEnabled: true,
        allowTestMode: true,
        allowLiveMode: descriptor.allowLiveMode,
        isDangerousLive: descriptor.isDangerousLive,
        allowedSourceTypes: ['*'],
        sortOrder: 110,
        meta: {
          targetType: cleanText(targetMeta.targetType).toUpperCase() || 'WHATSAPP_CHANNEL',
          channelUrl: cleanText(targetMeta.channelUrl) || normalizedTargetRef || null,
          targetId: Number(targetMeta.targetId),
          requiresManualActivation: targetMeta?.requiresManualActivation === true
        }
      };
    }

    return {
      channelKey: buildChannelKey('whatsapp', 'default', normalizedTargetRef || 'default'),
      platform: 'whatsapp',
      channelLabel: normalizedTargetLabel || 'WhatsApp Output',
      channelType: 'standard',
      sourceKind: 'dynamic_target',
      targetRef: normalizedTargetRef || null,
      targetLabel: normalizedTargetLabel || 'WhatsApp Gateway',
      isEnabled: true,
      allowTestMode: true,
      allowLiveMode: true,
      isDangerousLive: false,
      allowedSourceTypes: ['*'],
      sortOrder: 110
    };
  }

  return {
    channelKey: buildChannelKey('facebook', 'default', normalizedTargetRef || 'default'),
    platform: 'facebook',
    channelLabel: normalizedTargetLabel || 'Facebook Output',
    channelType: 'standard',
    sourceKind: 'dynamic_target',
    targetRef: normalizedTargetRef || null,
    targetLabel: normalizedTargetLabel || 'Facebook Default Ziel',
    isEnabled: true,
    allowTestMode: true,
    allowLiveMode: true,
    isDangerousLive: false,
    allowedSourceTypes: ['*'],
    sortOrder: 210
  };
}

export function resolveOutputChannel(input = {}) {
  seedOutputChannels();
  const targetMeta =
    input.targetMeta && typeof input.targetMeta === 'object'
      ? input.targetMeta
      : typeof input.targetMetaJson === 'string'
        ? parseJson(input.targetMetaJson, null)
        : null;

  const explicitKey = cleanText(targetMeta?.outputChannelKey || input.outputChannelKey);
  if (explicitKey) {
    const explicitChannel = getOutputChannelByKeyInternal(explicitKey);
    if (explicitChannel) {
      return explicitChannel;
    }
  }

  const dynamicSeed = buildDynamicSeedForTarget({
    platform: cleanText(input.platform || input.channelType),
    queueSourceType: cleanText(input.queueSourceType),
    targetRef: cleanText(input.targetRef),
    targetLabel: cleanText(input.targetLabel),
    targetMeta,
    payload: input.payload && typeof input.payload === 'object' ? input.payload : {}
  });

  upsertSeedChannel(dynamicSeed);
  return syncManagedSeedChannel(dynamicSeed);
}

function isSourceTypeAllowed(channel = null, queueSourceType = '') {
  const allowedTypes = normalizeAllowedSourceTypes(channel?.allowedSourceTypes, ['*']);
  if (allowedTypes.includes('*')) {
    return true;
  }

  const normalizedSourceType = cleanText(queueSourceType).toLowerCase();
  return normalizedSourceType ? allowedTypes.includes(normalizedSourceType) : false;
}

function buildGateFailure(channel, platformStatus, queueSourceType, reason) {
  return {
    allowed: false,
    channel,
    platformStatus,
    queueSourceType: cleanText(queueSourceType).toLowerCase(),
    reasonCode: OUTPUT_DISABLED_SKIP,
    message: `${OUTPUT_DISABLED_SKIP}: ${reason}`
  };
}

export function evaluateOutputGate(input = {}) {
  const channel =
    input.channel && typeof input.channel === 'object'
      ? input.channel
      : resolveOutputChannel({
          outputChannelKey: input.outputChannelKey,
          platform: input.platform || input.channelType,
          queueSourceType: input.queueSourceType,
          targetRef: input.targetRef,
          targetLabel: input.targetLabel,
          targetMeta: input.targetMeta,
          targetMetaJson: input.targetMetaJson,
          payload: input.payload
        });
  const queueSourceType = cleanText(input.queueSourceType).toLowerCase();
  const platformStatus = getPlatformStatus(channel?.platform || cleanText(input.platform || input.channelType).toLowerCase());
  const settings = readAppSettings();
  const isTestMode = input.testMode === true || input.payload?.testMode === true;
  const liveRoute = channel?.channelType === 'live' || channel?.isDangerousLive === true || queueSourceType.includes('approved_route');

  if (settings?.outputQueueEnabled !== 1) {
    return buildGateFailure(channel, platformStatus, queueSourceType, 'Queue ist deaktiviert.');
  }

  if (!channel?.isEnabled) {
    return buildGateFailure(channel, platformStatus, queueSourceType, `${channel?.channelLabel || 'Output Kanal'} ist deaktiviert.`);
  }

  if (channel?.isBlocked === true) {
    return buildGateFailure(channel, platformStatus, queueSourceType, `${channel.channelLabel} ist gesperrt.`);
  }

  if (!platformStatus.active) {
    return buildGateFailure(channel, platformStatus, queueSourceType, `${platformStatus.label} ist nicht aktiv.`);
  }

  if (!isSourceTypeAllowed(channel, queueSourceType)) {
    return buildGateFailure(channel, platformStatus, queueSourceType, `Typ ${queueSourceType || '-'} ist fuer ${channel.channelLabel} nicht erlaubt.`);
  }

  if (isTestMode && channel.allowTestMode !== true) {
    return buildGateFailure(channel, platformStatus, queueSourceType, `Testmodus ist fuer ${channel.channelLabel} nicht erlaubt.`);
  }

  if (liveRoute && channel.allowLiveMode !== true) {
    return buildGateFailure(channel, platformStatus, queueSourceType, `Live-Modus ist fuer ${channel.channelLabel} nicht erlaubt.`);
  }

  return {
    allowed: true,
    channel,
    platformStatus,
    queueSourceType,
    reasonCode: '',
    message: '',
    isTestMode,
    isLiveRoute: liveRoute
  };
}

export function recordOutputChannelEvent(channelInput, input = {}) {
  const channel =
    typeof channelInput === 'string' ? getOutputChannelByKeyInternal(channelInput) : channelInput && channelInput.channelKey ? channelInput : null;

  if (!channel?.channelKey) {
    return null;
  }

  const payload = {
    channelKey: channel.channelKey,
    lastStatus: cleanText(input.status) || channel.lastStatus || 'idle',
    lastSentAt: input.lastSentAt === undefined ? channel.lastSentAt : input.lastSentAt,
    lastErrorAt: input.lastErrorAt === undefined ? channel.lastErrorAt : input.lastErrorAt,
    lastErrorMessage:
      input.lastErrorMessage === undefined ? channel.lastErrorMessage : cleanText(input.lastErrorMessage) || null,
    lastQueueId: input.queueId === undefined ? channel.lastQueueId : input.queueId,
    lastTargetId: input.targetId === undefined ? channel.lastTargetId : input.targetId,
    lastEventType: cleanText(input.eventType) || channel.lastEventType || null,
    lastMessagePreview: cleanText(input.messagePreview).slice(0, 280) || channel.lastMessagePreview || null,
    updatedAt: nowIso()
  };

  db.prepare(
    `
      UPDATE output_channels
      SET last_status = @lastStatus,
          last_sent_at = @lastSentAt,
          last_error_at = @lastErrorAt,
          last_error_message = @lastErrorMessage,
          last_queue_id = @lastQueueId,
          last_target_id = @lastTargetId,
          last_event_type = @lastEventType,
          last_message_preview = @lastMessagePreview,
          updated_at = @updatedAt
      WHERE channel_key = @channelKey
    `
  ).run(payload);

  return getOutputChannelByKeyInternal(channel.channelKey);
}

export function listOutputChannelsSnapshot() {
  seedOutputChannels();
  const settings = readAppSettings();
  const channels = db
    .prepare(`SELECT * FROM output_channels ORDER BY platform ASC, sort_order ASC, channel_label ASC`)
    .all()
    .map(mapOutputChannelRow);
  const platforms = {
    telegram: getPlatformStatus('telegram'),
    whatsapp: getPlatformStatus('whatsapp'),
    facebook: getPlatformStatus('facebook')
  };

  return {
    controls: {
      outputQueueEnabled: settings?.outputQueueEnabled === 1,
      copybotEnabled: settings?.copybotEnabled === 1
    },
    platforms,
    channels: channels.map((channel) => ({
      ...channel,
      platformStatus: platforms[channel.platform] || getPlatformStatus(channel.platform),
      warningText:
        channel.isDangerousLive && channel.isEnabled !== true ? 'LIVE KANAL Deaktiviert Erst manuell aktivieren' : ''
    }))
  };
}

export function getOutputChannelByKey(channelKey = '') {
  seedOutputChannels();
  return getOutputChannelByKeyInternal(channelKey);
}

export function saveOutputChannelConfig(channelKey = '', input = {}) {
  const current = getOutputChannelByKey(channelKey);
  if (!current) {
    throw new Error('Output Kanal nicht gefunden.');
  }

  db.prepare(
    `
      UPDATE output_channels
      SET is_enabled = ?,
          is_blocked = ?,
          allow_test_mode = ?,
          allow_live_mode = ?,
          notes = ?,
          status_hint = ?,
          updated_at = ?
      WHERE channel_key = ?
    `
  ).run(
    input.isEnabled === undefined ? (current.isEnabled ? 1 : 0) : input.isEnabled ? 1 : 0,
    input.isBlocked === undefined ? (current.isBlocked ? 1 : 0) : input.isBlocked ? 1 : 0,
    input.allowTestMode === undefined ? (current.allowTestMode ? 1 : 0) : input.allowTestMode ? 1 : 0,
    input.allowLiveMode === undefined ? (current.allowLiveMode ? 1 : 0) : input.allowLiveMode ? 1 : 0,
    input.notes === undefined ? current.notes || null : cleanText(input.notes) || null,
    input.statusHint === undefined ? current.statusHint || null : cleanText(input.statusHint) || null,
    nowIso(),
    channelKey
  );

  return getOutputChannelByKey(channelKey);
}

export function saveOutputControlSettings(input = {}) {
  const current = readAppSettings();
  db.prepare(`UPDATE app_settings SET outputQueueEnabled = ? WHERE id = 1`).run(
    input.outputQueueEnabled === undefined ? (current?.outputQueueEnabled === 1 ? 1 : 0) : input.outputQueueEnabled ? 1 : 0
  );

  return listOutputChannelsSnapshot().controls;
}
