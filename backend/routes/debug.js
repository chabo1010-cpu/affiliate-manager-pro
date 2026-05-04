import { Router } from 'express';
import { getReaderRuntimeConfig, getTelegramConfig, getTelegramTestGroupConfig } from '../env.js';
import { buildAmazonAffiliateLinkRecord, normalizeSellerType, resetDealLockHistory } from '../services/dealHistoryService.js';
import { publishGeneratorPostDirect } from '../services/directPublisher.js';
import { getTelegramBotClientConfig } from '../services/telegramBotClientService.js';
import { forceScanTelegramReader, forceTestgroupFeed, getTelegramUserClientStatus } from '../services/telegramUserClientService.js';
import { sendTelegramPost } from '../services/telegramSenderService.js';
import { generatePostText } from '../../frontend/src/lib/postGenerator.js';

const router = Router();

function getRequesterRole(req) {
  return String(req.headers['x-user-role'] || '').trim().toLowerCase();
}

function requireAdmin(req, res, next) {
  if (getRequesterRole(req) !== 'admin') {
    return res.status(403).json({ error: 'Nur Admin darf Force-Reader-Scans ausloesen.' });
  }

  return next();
}

function cleanText(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function resolveTestgroupTargetMeta() {
  const testGroupConfig = getTelegramTestGroupConfig();
  const explicitTestChatId = cleanText(process.env.TELEGRAM_TEST_CHAT_ID);
  const fallbackChatId = cleanText(process.env.TELEGRAM_CHAT_ID);
  const targetChatId = cleanText(testGroupConfig.chatId);
  const targetSource = explicitTestChatId
    ? 'TELEGRAM_TEST_CHAT_ID'
    : fallbackChatId
      ? 'TELEGRAM_CHAT_ID'
      : 'missing';

  return {
    tokenConfigured: Boolean(cleanText(testGroupConfig.token)),
    targetChatId,
    targetSource
  };
}

function normalizeTelegramUsername(value = '') {
  const username = cleanText(value).replace(/^@+/, '');
  return username ? `@${username}` : '';
}

function pushTelegramChannelDebugCandidate(candidates, seen, input = {}) {
  const key = cleanText(input.key);
  const chatId = cleanText(input.chatId);
  const username = normalizeTelegramUsername(input.username);
  const title = cleanText(input.title || input.label);
  const dedupeKey = [key, chatId, username, title].filter(Boolean).join('|');

  if (!dedupeKey || seen.has(dedupeKey)) {
    return;
  }

  seen.add(dedupeKey);
  candidates.push({
    key,
    label: cleanText(input.label) || title || username || chatId || 'Telegram Ziel',
    title,
    username,
    chatId
  });
}

function getTelegramChannelDebugCandidates() {
  const candidates = [];
  const seen = new Set();
  const botConfig = getTelegramBotClientConfig();
  const testGroupConfig = getTelegramTestGroupConfig();

  pushTelegramChannelDebugCandidate(candidates, seen, {
    key: 'test',
    label: 'Testgruppe',
    title: 'Testgruppe',
    chatId: testGroupConfig.chatId
  });
  pushTelegramChannelDebugCandidate(candidates, seen, {
    key: 'approved',
    label: 'Approved Channel',
    title: 'Ver\u00F6ffentlicht Test',
    chatId: process.env.TELEGRAM_APPROVED_CHANNEL_ID,
    username: process.env.TELEGRAM_APPROVED_CHANNEL_USERNAME
  });
  pushTelegramChannelDebugCandidate(candidates, seen, {
    key: 'rejected',
    label: 'Rejected Channel',
    title: 'Geblockt Test',
    chatId: process.env.TELEGRAM_REJECTED_CHANNEL_ID,
    username: process.env.TELEGRAM_REJECTED_CHANNEL_USERNAME
  });

  [...(botConfig.targets || []), ...(botConfig.effectiveTargets || [])].forEach((target) => {
    pushTelegramChannelDebugCandidate(candidates, seen, {
      key: `bot_target_${target.id || target.chatId || target.name || 'unknown'}`,
      label: target.name,
      title: target.name,
      chatId: target.chatId
    });
  });

  return candidates;
}

async function resolveTelegramChannelDebugCandidate(token, candidate = {}) {
  const chatRef = cleanText(candidate.username) || cleanText(candidate.chatId);

  if (!chatRef) {
    return {
      ...candidate,
      ok: false,
      error: 'Kein Username oder Chat-ID konfiguriert.'
    };
  }

  const response = await fetch(
    `https://api.telegram.org/bot${token}/getChat?chat_id=${encodeURIComponent(chatRef)}`
  );
  const data = await response.json().catch(() => null);

  if (!response.ok || data?.ok !== true) {
    return {
      ...candidate,
      ok: false,
      error: data?.description || `Telegram getChat fehlgeschlagen (${response.status}).`
    };
  }

  const chat = data.result || {};
  const username = normalizeTelegramUsername(chat.username) || candidate.username || '';
  const result = {
    ...candidate,
    ok: true,
    title: cleanText(chat.title || chat.first_name || candidate.title || candidate.label),
    username,
    chatId: chat.id === undefined || chat.id === null ? candidate.chatId : String(chat.id)
  };

  console.log('[TELEGRAM_CHANNEL_ID_DEBUG]', {
    title: result.title,
    username: result.username,
    chatId: result.chatId
  });

  return result;
}

function classifyTelegramSendError(error) {
  const message = error instanceof Error ? error.message : 'Telegram API Fehler';
  const normalized = cleanText(message).toLowerCase();

  if (!normalized || normalized.includes('telegram_bot_token fehlt')) {
    return 'Bot Token fehlt';
  }
  if (normalized.includes('telegram_chat_id fehlt')) {
    return 'Chat ID fehlt';
  }
  if (normalized.includes('chat not found')) {
    return 'Chat ID falsch';
  }
  if (normalized.includes('bot is not a member')) {
    return 'Bot nicht in Gruppe';
  }
  if (
    normalized.includes('have no rights to send') ||
    normalized.includes('chat_write_forbidden') ||
    normalized.includes('forbidden')
  ) {
    return 'Bot hat keine Rechte';
  }
  if (normalized.includes('unauthorized')) {
    return 'Bot Token falsch';
  }

  return 'Telegram API Fehler';
}

function getDealLockDebugGuard() {
  const runtimeConfig = getReaderRuntimeConfig();
  return {
    enabled: runtimeConfig.readerTestMode || runtimeConfig.readerDebugMode,
    readerTestMode: runtimeConfig.readerTestMode,
    readerDebugMode: runtimeConfig.readerDebugMode
  };
}

router.get('/test', async (req, res) => {
  const runtimeConfig = getReaderRuntimeConfig();
  const readerStatus = await getTelegramUserClientStatus();

  return res.json({
    success: true,
    ok: true,
    runtimeFlags: {
      readerTestMode: runtimeConfig.readerTestMode === true,
      readerDebugMode: runtimeConfig.readerDebugMode === true,
      allowRawReaderFallback: runtimeConfig.allowRawReaderFallback === true,
      dealLockBypass: runtimeConfig.dealLockBypass === true
    },
    readerStatus: {
      configured: readerStatus.configured === true,
      enabled: readerStatus.enabled === true,
      listenerActive: readerStatus.listenerActive === true,
      pollingActive: readerStatus.pollingActive === true,
      listenerSessions: Number(readerStatus.listenerSessions || 0),
      watchedChannels: Array.isArray(readerStatus.channels) ? readerStatus.channels.length : 0,
      activeSourceCount: Number(readerStatus.activeSourceCount || 0),
      lastPollAt: readerStatus.lastPollAt || null,
      lastFoundMessageAt: readerStatus.lastFoundMessageAt || null
    }
  });
});

router.post('/test-generator-publish', requireAdmin, async (req, res) => {
  const asin = cleanText(req.body?.asin).toUpperCase();
  const price = cleanText(req.body?.price) || '32.95';
  const sellerType = normalizeSellerType(req.body?.sellerType || 'AMAZON');
  const source = cleanText(req.body?.source) || 'debug';

  console.info('[DEBUG_GENERATOR_PUBLISH_START]', {
    asin,
    price,
    sellerType,
    source
  });

  try {
    if (!asin) {
      throw new Error('ASIN fehlt.');
    }

    const linkRecord = buildAmazonAffiliateLinkRecord(asin, { asin });
    if (!linkRecord.valid || !cleanText(linkRecord.affiliateUrl)) {
      throw new Error('Partnerlink konnte fuer den Debug-Test nicht gebaut werden.');
    }

    console.info('[GENERATOR_FORMAT_START]', {
      asin,
      sellerType,
      source
    });

    const generatedPost = generatePostText({
      productTitle: `Debug Amazon Deal ${asin}`,
      freiText: '',
      textBaustein: [],
      alterPreis: '',
      neuerPreis: price,
      alterPreisLabel: 'Vorher',
      neuerPreisLabel: 'Jetzt',
      amazonLink: linkRecord.affiliateUrl,
      werbung: false,
      extraOptions: [],
      rabattgutscheinCode: ''
    });

    const publishInput = {
      title: generatedPost.productTitle || `Debug Amazon Deal ${asin}`,
      link: linkRecord.affiliateUrl,
      normalizedUrl: linkRecord.normalizedUrl,
      asin,
      sellerType,
      currentPrice: price,
      oldPrice: '',
      couponCode: '',
      textByChannel: {
        telegram: generatedPost.telegramCaption,
        whatsapp: generatedPost.whatsappText,
        facebook: generatedPost.whatsappText
      },
      generatedImagePath: '',
      uploadedImagePath: '',
      uploadedImageFile: null,
      telegramImageSource: 'none',
      whatsappImageSource: 'none',
      facebookImageSource: 'link_preview',
      enableTelegram: true,
      enableWhatsapp: false,
      enableFacebook: false,
      queueSourceType: 'generator_direct',
      originOverride: 'automatic',
      contextSource: 'debug_generator_publish'
    };

    console.info('[GENERATOR_FORMAT_SUCCESS]', {
      asin,
      sellerType,
      title: publishInput.title,
      affiliateUrl: publishInput.link
    });
    console.info('[GENERATOR_OUTPUT_READY]', {
      asin,
      sellerType,
      link: publishInput.link,
      imageSource: publishInput.telegramImageSource
    });

    const result = await publishGeneratorPostDirect(publishInput);

    return res.json({
      success: true,
      asin,
      queue: result.queue || null,
      results: result.results || {}
    });
  } catch (error) {
    console.error('[ERROR_REASON]', {
      reason: error instanceof Error ? error.message : 'Debug Generator Publish fehlgeschlagen.'
    });
    return res.status(400).json({
      success: false,
      error: error instanceof Error ? error.message : 'Debug Generator Publish fehlgeschlagen.'
    });
  }
});

router.post('/reset-deal-lock', requireAdmin, async (req, res) => {
  const dealLockDebugGuard = getDealLockDebugGuard();

  if (!dealLockDebugGuard.enabled) {
    return res.status(403).json({
      success: false,
      error: 'Deal-Lock-Reset ist nur mit READER_TEST_MODE=1 oder READER_DEBUG_MODE=1 erlaubt.'
    });
  }

  const asin = cleanText(req.body?.asin).toUpperCase();
  const normalizedUrl = cleanText(req.body?.normalizedUrl);
  const url = cleanText(req.body?.url);
  const resetSummary = resetDealLockHistory({
    asin,
    normalizedUrl,
    url
  });

  console.info('[DEAL_LOCK_RESET]', {
    ...resetSummary,
    readerTestMode: dealLockDebugGuard.readerTestMode,
    readerDebugMode: dealLockDebugGuard.readerDebugMode
  });

  return res.json({
    success: true,
    reset: resetSummary
  });
});

router.post('/force-reader-scan', async (req, res) => {
  console.info('[FORCE_SCAN_ENDPOINT_HIT]', {
    ignoreLastSeen: true,
    requestedSessionName: cleanText(req.body?.sessionName),
    requestedChannelRef: cleanText(req.body?.channelRef)
  });

  if (getRequesterRole(req) !== 'admin') {
    return res.status(403).json({ error: 'Nur Admin darf Debug-Generator-Posts ausloesen.' });
  }

  const dealLockDebugGuard = getDealLockDebugGuard();

  if (!dealLockDebugGuard.enabled) {
    return res.status(403).json({
      success: false,
      error: 'Force-Reader-Scan ist nur mit READER_TEST_MODE=1 oder READER_DEBUG_MODE=1 erlaubt.'
    });
  }

  try {
    console.log('[FORCE_SCAN_START]');
    const result = await forceScanTelegramReader({
      ...(req.body ?? {}),
      ignoreLastSeen: true
    });

    return res.json({
      success: true,
      ...result
    });
  } catch (error) {
    console.error('[ERROR_REASON]', {
      reason: error instanceof Error ? error.message : 'Force-Reader-Scan konnte nicht gestartet werden.'
    });
    return res.status(400).json({
      success: false,
      error: error instanceof Error ? error.message : 'Force-Reader-Scan konnte nicht gestartet werden.'
    });
  }
});

router.post('/force-testgroup-feed', async (req, res) => {
  if (getRequesterRole(req) !== 'admin') {
    return res.status(403).json({ error: 'Nur Admin darf den Testgruppen-Feed erzwingen.' });
  }

  try {
    const result = await forceTestgroupFeed({
      limitPerGroup: 20,
      maxGroups: 100,
      ignoreLastSeen: true,
      sendEverythingToTestGroup: true,
      ...(req.body ?? {})
    });

    return res.json(result);
  } catch (error) {
    return res.status(400).json({
      success: false,
      error: error instanceof Error ? error.message : 'Force-Testgruppen-Feed konnte nicht gestartet werden.'
    });
  }
});

router.post('/send-testgroup-ping', async (req, res) => {
  if (getRequesterRole(req) !== 'admin') {
    return res.status(403).json({ error: 'Nur Admin darf den Testgruppen-Ping ausloesen.' });
  }

  const targetMeta = resolveTestgroupTargetMeta();
  const timestamp = new Date().toISOString();
  const text = `PING Testgruppe ${timestamp}`;

  console.info('[TESTGROUP_PING_START]', {
    timestamp
  });
  console.info('[TESTGROUP_TARGET]', {
    explicitTestChatId: cleanText(process.env.TELEGRAM_TEST_CHAT_ID) || null,
    fallbackChatId: cleanText(process.env.TELEGRAM_CHAT_ID) || null
  });
  console.info('[TESTGROUP_TARGET_RESOLVED]', targetMeta);

  try {
    const result = await sendTelegramPost({
      text,
      disableWebPagePreview: true,
      chatId: targetMeta.targetChatId
    });

    return res.json({
      success: true,
      pingSent: true,
      targetChatId: targetMeta.targetChatId || null,
      targetSource: targetMeta.targetSource,
      tokenConfigured: targetMeta.tokenConfigured,
      telegram: result,
      text
    });
  } catch (error) {
    return res.status(400).json({
      success: false,
      pingSent: false,
      targetChatId: targetMeta.targetChatId || null,
      targetSource: targetMeta.targetSource,
      tokenConfigured: targetMeta.tokenConfigured,
      errorReason: classifyTelegramSendError(error),
      error: error instanceof Error ? error.message : 'Telegram API Fehler'
    });
  }
});

router.post('/telegram-channel-id-debug', requireAdmin, async (req, res) => {
  const telegramConfig = getTelegramConfig();
  const candidates = getTelegramChannelDebugCandidates();

  if (!cleanText(telegramConfig.token)) {
    return res.status(400).json({
      success: false,
      error: 'TELEGRAM_BOT_TOKEN fehlt im Backend.'
    });
  }

  const results = [];

  for (const candidate of candidates) {
    const result = await resolveTelegramChannelDebugCandidate(telegramConfig.token, candidate);
    results.push(result);

    if (!result.ok) {
      console.warn('[TELEGRAM_CHANNEL_ID_DEBUG_ERROR]', {
        title: result.title || result.label || '',
        username: result.username || '',
        chatId: result.chatId || '',
        error: result.error
      });
    }
  }

  const approved = results.find((item) => item.key === 'approved') || null;
  const rejected = results.find((item) => item.key === 'rejected') || null;
  const envOutput = {
    TELEGRAM_APPROVED_CHANNEL_ID: approved?.ok ? approved.chatId : '',
    TELEGRAM_REJECTED_CHANNEL_ID: rejected?.ok ? rejected.chatId : ''
  };

  console.log(`TELEGRAM_APPROVED_CHANNEL_ID=${envOutput.TELEGRAM_APPROVED_CHANNEL_ID}`);
  console.log(`TELEGRAM_REJECTED_CHANNEL_ID=${envOutput.TELEGRAM_REJECTED_CHANNEL_ID}`);

  return res.json({
    success: true,
    endpoint: '/api/debug/telegram-channel-id-debug',
    candidates: candidates.length,
    results,
    envOutput
  });
});

export default router;
