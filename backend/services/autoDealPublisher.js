import crypto from 'crypto';
import { getDb } from '../db.js';
import { getTelegramTestGroupConfig } from '../env.js';
import {
  buildAmazonAffiliateLinkRecord,
  checkDealCooldown,
  normalizeSellerType,
  savePostedDeal
} from './dealHistoryService.js';
import { logGeneratorDebug } from './generatorFlowService.js';
import { sendTelegramPost } from './telegramSenderService.js';

const db = getDb();

function cleanText(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function parseInteger(value, fallback = 0) {
  const parsed = Number.parseInt(String(value ?? ''), 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function nowIso() {
  return new Date().toISOString();
}

function formatCurrency(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return '-';
  }

  return new Intl.NumberFormat('de-DE', {
    style: 'currency',
    currency: 'EUR'
  }).format(parsed);
}

function buildDecisionStatus(input) {
  if (input === 'approved_for_test_group') {
    return 'approved_for_test_group';
  }

  if (input === 'blocked' || input === 'block') {
    return 'blocked';
  }

  return 'review';
}

function buildAutoDealMessage(result, settings = {}) {
  const linkRecord = buildAmazonAffiliateLinkRecord(result?.productUrl || result?.asin, {
    asin: result?.asin
  });
  const normalizedProductUrl = linkRecord.valid ? linkRecord.normalizedUrl : cleanText(result?.productUrl);
  const affiliateProductUrl = linkRecord.valid ? linkRecord.affiliateUrl : cleanText(result?.productUrl);
  const prefix = cleanText(settings.telegramMessagePrefix) || 'Keepa Auto Deal';
  const lines = [
    prefix,
    cleanText(result.title) || cleanText(result.asin) || 'Unbenannter Deal',
    `Preis: ${formatCurrency(result.currentPrice)}`,
    `Rabatt: ${Number(result.keepaDiscount || 0).toFixed(1)}%`,
    `Vergleich: ${result.comparisonPrice !== null && result.comparisonPrice !== undefined ? formatCurrency(result.comparisonPrice) : 'nicht verbunden'}`,
    `Preisvorteil: ${
      result.priceDifferenceAbs !== null && result.priceDifferenceAbs !== undefined && result.priceDifferencePct !== null && result.priceDifferencePct !== undefined
        ? `${formatCurrency(result.priceDifferenceAbs)} (${Number(result.priceDifferencePct).toFixed(1)}%)`
        : 'nicht berechenbar'
    }`,
    `Seller-Typ: ${normalizeSellerType(result.sellerType)}`,
    `Kategorie: ${cleanText(result.categoryName) || '-'}`,
    `Deal-Score: ${parseInteger(result.dealScore, 0)}`
  ];

  if (result.fakeDrop?.fakeDropRisk !== null && result.fakeDrop?.fakeDropRisk !== undefined) {
    lines.push(`Fake-Drop Risiko: ${result.fakeDrop.fakeDropRisk}`);
  }

  if (cleanText(result.fakeDrop?.classificationLabel)) {
    lines.push(`Analyse: ${result.fakeDrop.classificationLabel}`);
  }

  if (affiliateProductUrl) {
    lines.push(`Link: ${affiliateProductUrl}`);
  }

  return {
    text: lines.filter(Boolean).join('\n'),
    preview: `${cleanText(result.title) || result.asin} | ${formatCurrency(result.currentPrice)} | ${parseInteger(result.dealScore, 0)}`,
    normalizedProductUrl,
    affiliateProductUrl
  };
}

function buildAlertDedupeKey(result, ruleId, status) {
  const fingerprint = `${cleanText(result.asin)}:${ruleId || 0}:telegram_test_group:${cleanText(status)}:${Math.round(
    (Number(result.currentPrice) || 0) * 100
  )}:${Math.round((Number(result.comparisonPrice) || 0) * 100)}:${parseInteger(result.dealScore, 0)}`;
  return crypto.createHash('sha1').update(fingerprint).digest('hex');
}

function getDrawerConfig(settings = {}, sellerType = 'AMAZON') {
  const normalizedSellerType = normalizeSellerType(sellerType);
  const drawerKey = ['AMAZON', 'FBA', 'FBM'].includes(normalizedSellerType) ? normalizedSellerType : 'AMAZON';
  const config = settings?.drawerConfigs?.[drawerKey];

  return {
    drawerKey,
    active: config?.active !== false,
    autoModeAllowed: config?.autoModeAllowed !== false,
    testGroupPostingAllowed: config?.testGroupPostingAllowed !== false
  };
}

function canSendAutoDeal(result, settings = {}) {
  const alertMaxPerProduct = Math.max(1, parseInteger(settings.alertMaxPerProduct, 2));
  const alertCooldownMinutes = Math.max(1, parseInteger(settings.alertCooldownMinutes, 180));

  if (parseInteger(result.alertCount, 0) >= alertMaxPerProduct) {
    return {
      allowed: false,
      reason: 'Maximale Alert-Anzahl fuer dieses Produkt erreicht.'
    };
  }

  if (result.lastAlertedAt) {
    const msSinceLastAlert = Date.now() - new Date(result.lastAlertedAt).getTime();
    if (Number.isFinite(msSinceLastAlert) && msSinceLastAlert < alertCooldownMinutes * 60 * 1000) {
      return {
        allowed: false,
        reason: 'Alert-Cooldown ist noch aktiv.'
      };
    }
  }

  const duplicate = db
    .prepare(
      `
        SELECT id
        FROM keepa_alerts
        WHERE asin = ?
          AND channel_type = 'telegram_test_group'
          AND status = 'sent'
          AND created_at >= ?
        LIMIT 1
      `
    )
    .get(cleanText(result.asin), new Date(Date.now() - alertCooldownMinutes * 60 * 1000).toISOString());

  if (duplicate) {
    return {
      allowed: false,
      reason: 'Es existiert bereits ein kuerzlich gesendeter Testgruppen-Post fuer dieses Produkt.'
    };
  }

  return { allowed: true };
}

function insertAutoAlertLog({
  result,
  rule,
  message,
  status,
  payload = null,
  errorMessage = null
}) {
  db.prepare(
    `
      INSERT OR IGNORE INTO keepa_alerts (
        keepa_result_id,
        asin,
        channel_type,
        status,
        rule_id,
        dedupe_key,
        message_preview,
        payload_json,
        error_message,
        created_at,
        sent_at
      ) VALUES (
        @keepaResultId,
        @asin,
        'telegram_test_group',
        @status,
        @ruleId,
        @dedupeKey,
        @messagePreview,
        @payloadJson,
        @errorMessage,
        @createdAt,
        @sentAt
      )
    `
  ).run({
    keepaResultId: result.id || null,
    asin: cleanText(result.asin),
    status,
    ruleId: rule?.id || null,
    dedupeKey: buildAlertDedupeKey(result, rule?.id, status),
    messagePreview: message.preview,
    payloadJson: JSON.stringify(payload ?? null),
    errorMessage,
    createdAt: nowIso(),
    sentAt: status === 'sent' ? nowIso() : null
  });
}

function updateKeepaResultState(resultId, nextState = {}) {
  if (!resultId) {
    return;
  }

  db.prepare(
    `
      UPDATE keepa_results
      SET workflow_status = @workflowStatus,
          alert_count = CASE
            WHEN @incrementAlertCount = 1 THEN COALESCE(alert_count, 0) + 1
            ELSE COALESCE(alert_count, 0)
          END,
          last_alerted_at = CASE
            WHEN @lastAlertedAt != '' THEN @lastAlertedAt
            ELSE last_alerted_at
          END,
          updated_at = @updatedAt
      WHERE id = @id
    `
  ).run({
    id: resultId,
    workflowStatus: cleanText(nextState.workflowStatus) || 'geprueft',
    incrementAlertCount: nextState.incrementAlertCount ? 1 : 0,
    lastAlertedAt: cleanText(nextState.lastAlertedAt),
    updatedAt: nowIso()
  });
}

export async function publishAutoDealToTelegramTestGroup({
  result,
  rule = null,
  settings = {},
  sourceType = 'keepa',
  decisionStatus = 'review',
  decisionReason = '',
  learningContext = null
}) {
  const sellerType = normalizeSellerType(result?.sellerType);
  const drawerPolicy = getDrawerConfig(settings, sellerType);
  const finalDecisionStatus = buildDecisionStatus(decisionStatus);
  const message = buildAutoDealMessage(result, settings);
  const decisionReasonText = cleanText(decisionReason) || 'Keine Begruendung vorhanden.';

  logGeneratorDebug(sourceType === 'amazon' ? 'AUTO DEAL RECEIVED FROM AMAZON' : 'AUTO DEAL RECEIVED FROM KEEPA', {
    keepaResultId: result?.id || null,
    asin: cleanText(result?.asin).toUpperCase(),
    sourceType
  });
  logGeneratorDebug(`SELLER TYPE DETECTED: ${sellerType}`, {
    keepaResultId: result?.id || null,
    asin: cleanText(result?.asin).toUpperCase(),
    sellerType
  });
  logGeneratorDebug('PRICE TREND LOGIC APPLIED', {
    keepaResultId: result?.id || null,
    asin: cleanText(result?.asin).toUpperCase(),
    currentPrice: result?.currentPrice ?? null,
    keepaDiscount: result?.keepaDiscount ?? null,
    comparisonPrice: result?.comparisonPrice ?? null,
    dealScore: result?.dealScore ?? null,
    fakeDropRisk: result?.fakeDrop?.fakeDropRisk ?? null
  });
  logGeneratorDebug(`LEARNING LOGIC DECISION: ${finalDecisionStatus}`, {
    keepaResultId: result?.id || null,
    asin: cleanText(result?.asin).toUpperCase(),
    sellerType,
    reason: decisionReasonText
  });

  const basePayload = {
    sourceType,
    sellerType,
    decisionStatus: finalDecisionStatus,
    decisionReason: decisionReasonText,
    telegramStatus: finalDecisionStatus === 'approved_for_test_group' ? 'pending' : 'not_sent',
    keepaStatus: learningContext?.keepa?.status || null
  };

  if (finalDecisionStatus !== 'approved_for_test_group') {
    insertAutoAlertLog({
      result,
      rule,
      message,
      status: finalDecisionStatus === 'blocked' ? 'blocked' : 'review',
      payload: basePayload
    });
    updateKeepaResultState(result?.id, {
      workflowStatus: finalDecisionStatus === 'blocked' ? 'verworfen' : 'geprueft'
    });

    logGeneratorDebug('AUTO DEAL TELEGRAM OUTPUT BLOCKED', {
      keepaResultId: result?.id || null,
      asin: cleanText(result?.asin).toUpperCase(),
      sellerType,
      decisionStatus: finalDecisionStatus,
      reason: decisionReasonText
    });
    logGeneratorDebug('TELEGRAM TEST POST BLOCKED', {
      keepaResultId: result?.id || null,
      asin: cleanText(result?.asin).toUpperCase(),
      sellerType,
      decisionStatus: finalDecisionStatus,
      reason: decisionReasonText
    });

    return {
      channelType: 'telegram_test_group',
      status: finalDecisionStatus === 'blocked' ? 'blocked' : 'review',
      decisionStatus: finalDecisionStatus,
      reason: decisionReasonText
    };
  }

  if (!settings.alertTelegramEnabled) {
    const blockedReason = 'Telegram-Auto-Output ist in den Keepa-Einstellungen deaktiviert.';
    insertAutoAlertLog({
      result,
      rule,
      message,
      status: 'blocked',
      payload: {
        ...basePayload,
        telegramStatus: 'blocked',
        drawerKey: drawerPolicy.drawerKey
      },
      errorMessage: blockedReason
    });
    updateKeepaResultState(result?.id, {
      workflowStatus: 'geprueft'
    });
    logGeneratorDebug('AUTO DEAL TELEGRAM OUTPUT BLOCKED', {
      keepaResultId: result?.id || null,
      asin: cleanText(result?.asin).toUpperCase(),
      sellerType,
      decisionStatus: finalDecisionStatus,
      reason: blockedReason
    });
    logGeneratorDebug('TELEGRAM TEST POST BLOCKED', {
      keepaResultId: result?.id || null,
      asin: cleanText(result?.asin).toUpperCase(),
      sellerType,
      decisionStatus: finalDecisionStatus,
      reason: blockedReason
    });

    return {
      channelType: 'telegram_test_group',
      status: 'blocked',
      decisionStatus: finalDecisionStatus,
      reason: blockedReason
    };
  }

  if (!drawerPolicy.active || !drawerPolicy.autoModeAllowed || !drawerPolicy.testGroupPostingAllowed) {
    const blockedReason = !drawerPolicy.active
      ? `Die Keepa-Schublade ${drawerPolicy.drawerKey} ist deaktiviert.`
      : !drawerPolicy.autoModeAllowed
        ? `Der Auto-Modus der Keepa-Schublade ${drawerPolicy.drawerKey} ist deaktiviert.`
        : `Testgruppen-Posting ist fuer die Keepa-Schublade ${drawerPolicy.drawerKey} deaktiviert.`;
    insertAutoAlertLog({
      result,
      rule,
      message,
      status: 'blocked',
      payload: {
        ...basePayload,
        telegramStatus: 'blocked',
        drawerKey: drawerPolicy.drawerKey
      },
      errorMessage: blockedReason
    });
    updateKeepaResultState(result?.id, {
      workflowStatus: 'geprueft'
    });
    logGeneratorDebug('AUTO DEAL TELEGRAM OUTPUT BLOCKED', {
      keepaResultId: result?.id || null,
      asin: cleanText(result?.asin).toUpperCase(),
      sellerType,
      decisionStatus: finalDecisionStatus,
      reason: blockedReason,
      drawerKey: drawerPolicy.drawerKey
    });
    logGeneratorDebug('TELEGRAM TEST POST BLOCKED', {
      keepaResultId: result?.id || null,
      asin: cleanText(result?.asin).toUpperCase(),
      sellerType,
      decisionStatus: finalDecisionStatus,
      reason: blockedReason,
      drawerKey: drawerPolicy.drawerKey
    });

    return {
      channelType: 'telegram_test_group',
      status: 'blocked',
      decisionStatus: finalDecisionStatus,
      reason: blockedReason
    };
  }

  const eligibility = canSendAutoDeal(result, settings);
  if (!eligibility.allowed) {
    insertAutoAlertLog({
      result,
      rule,
      message,
      status: 'blocked',
      payload: {
        ...basePayload,
        telegramStatus: 'blocked'
      },
      errorMessage: eligibility.reason
    });
    updateKeepaResultState(result?.id, {
      workflowStatus: 'geprueft'
    });

    logGeneratorDebug('AUTO DEAL TELEGRAM OUTPUT BLOCKED', {
      keepaResultId: result?.id || null,
      asin: cleanText(result?.asin).toUpperCase(),
      sellerType,
      decisionStatus: finalDecisionStatus,
      reason: eligibility.reason
    });
    logGeneratorDebug('TELEGRAM TEST POST BLOCKED', {
      keepaResultId: result?.id || null,
      asin: cleanText(result?.asin).toUpperCase(),
      sellerType,
      decisionStatus: finalDecisionStatus,
      reason: eligibility.reason
    });

    return {
      channelType: 'telegram_test_group',
      status: 'blocked',
      decisionStatus: finalDecisionStatus,
      reason: eligibility.reason
    };
  }

  const repostCheck = checkDealCooldown({
    asin: result?.asin,
    url: message.affiliateProductUrl || result?.productUrl,
    normalizedUrl: message.normalizedProductUrl || result?.productUrl
  });
  if (repostCheck.blocked) {
    const repostReason = 'Repost-Sperre blockiert den automatischen Testgruppen-Post.';
    insertAutoAlertLog({
      result,
      rule,
      message,
      status: 'blocked',
      payload: {
        ...basePayload,
        telegramStatus: 'blocked',
        repostRemainingSeconds: repostCheck.remainingSeconds
      },
      errorMessage: repostReason
    });
    updateKeepaResultState(result?.id, {
      workflowStatus: 'geprueft'
    });

    logGeneratorDebug('AUTO DEAL TELEGRAM OUTPUT BLOCKED', {
      keepaResultId: result?.id || null,
      asin: cleanText(result?.asin).toUpperCase(),
      sellerType,
      decisionStatus: finalDecisionStatus,
      reason: repostReason,
      remainingSeconds: repostCheck.remainingSeconds
    });
    logGeneratorDebug('TELEGRAM TEST POST BLOCKED', {
      keepaResultId: result?.id || null,
      asin: cleanText(result?.asin).toUpperCase(),
      sellerType,
      decisionStatus: finalDecisionStatus,
      reason: repostReason,
      remainingSeconds: repostCheck.remainingSeconds
    });

    return {
      channelType: 'telegram_test_group',
      status: 'blocked',
      decisionStatus: finalDecisionStatus,
      reason: repostReason
    };
  }

  const testGroupConfig = getTelegramTestGroupConfig();
  const postedAt = nowIso();

  logGeneratorDebug('AUTO DEAL TELEGRAM OUTPUT START', {
    keepaResultId: result?.id || null,
    asin: cleanText(result?.asin).toUpperCase(),
    sellerType,
    chatId: testGroupConfig.chatId || null
  });

  try {
    const telegramResult = await sendTelegramPost({
      text: message.text,
      imageUrl: cleanText(result?.imageUrl) || undefined,
      disableWebPagePreview: !cleanText(result?.imageUrl),
      chatId: testGroupConfig.chatId
    });

    savePostedDeal({
      asin: result?.asin || '',
      originalUrl: result?.productUrl || message.normalizedProductUrl || '',
      finalUrl: message.affiliateProductUrl || result?.productUrl || '',
      normalizedUrl: message.normalizedProductUrl || result?.productUrl || '',
      title: result?.title || '',
      currentPrice: result?.currentPrice === null || result?.currentPrice === undefined ? '' : String(result.currentPrice),
      oldPrice: '',
      sellerType,
      postedAt,
      channel: 'TELEGRAM'
    });

    insertAutoAlertLog({
      result,
      rule,
      message,
      status: 'sent',
      payload: {
        ...basePayload,
        telegramStatus: 'sent',
        telegramMessageId: telegramResult?.messageId || null,
        telegramChatId: telegramResult?.chatId || testGroupConfig.chatId || null
      }
    });
    updateKeepaResultState(result?.id, {
      workflowStatus: 'alert_gesendet',
      incrementAlertCount: true,
      lastAlertedAt: postedAt
    });

    logGeneratorDebug('AUTO DEAL TELEGRAM OUTPUT SENT', {
      keepaResultId: result?.id || null,
      asin: cleanText(result?.asin).toUpperCase(),
      sellerType,
      messageId: telegramResult?.messageId || null,
      chatId: telegramResult?.chatId || testGroupConfig.chatId || null
    });
    logGeneratorDebug('TELEGRAM OUTPUT SENT', {
      sourceType,
      keepaResultId: result?.id || null,
      asin: cleanText(result?.asin).toUpperCase(),
      chatId: telegramResult?.chatId || testGroupConfig.chatId || null
    });
    logGeneratorDebug('TELEGRAM TEST POST SENT', {
      keepaResultId: result?.id || null,
      asin: cleanText(result?.asin).toUpperCase(),
      sellerType,
      messageId: telegramResult?.messageId || null,
      chatId: telegramResult?.chatId || testGroupConfig.chatId || null
    });

    return {
      channelType: 'telegram_test_group',
      status: 'sent',
      decisionStatus: finalDecisionStatus,
      reason: decisionReasonText,
      sentAt: postedAt,
      chatId: telegramResult?.chatId || testGroupConfig.chatId || null,
      messageId: telegramResult?.messageId || null,
      messagePreview: message.preview
    };
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : 'Telegram-Testgruppen-Output fehlgeschlagen.';
    insertAutoAlertLog({
      result,
      rule,
      message,
      status: 'failed',
      payload: {
        ...basePayload,
        telegramStatus: 'failed'
      },
      errorMessage
    });
    updateKeepaResultState(result?.id, {
      workflowStatus: 'geprueft'
    });

    logGeneratorDebug('AUTO DEAL TELEGRAM OUTPUT BLOCKED', {
      keepaResultId: result?.id || null,
      asin: cleanText(result?.asin).toUpperCase(),
      sellerType,
      decisionStatus: finalDecisionStatus,
      reason: errorMessage
    });
    logGeneratorDebug('TELEGRAM OUTPUT FAILED', {
      sourceType,
      keepaResultId: result?.id || null,
      asin: cleanText(result?.asin).toUpperCase(),
      reason: errorMessage
    });
    logGeneratorDebug('TELEGRAM TEST POST BLOCKED', {
      keepaResultId: result?.id || null,
      asin: cleanText(result?.asin).toUpperCase(),
      sellerType,
      decisionStatus: finalDecisionStatus,
      reason: errorMessage
    });

    return {
      channelType: 'telegram_test_group',
      status: 'failed',
      decisionStatus: finalDecisionStatus,
      reason: errorMessage
    };
  }
}
