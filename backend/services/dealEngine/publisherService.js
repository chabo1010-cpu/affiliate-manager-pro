import { createPublishingEntry, listPublishingQueue } from '../publisherService.js';
import { getTelegramBotClientConfig } from '../telegramBotClientService.js';
import { getWhatsappClientConfig } from '../whatsappClientService.js';
import { formatMoney, summarizeReasons } from './shared.js';

function buildDealEngineMessage(analysis = {}) {
  const lines = [
    analysis.title || 'Deal ohne Titel',
    `Amazon: ${formatMoney(analysis.amazonPrice)}`,
    analysis.marketPrice !== null ? `Marktpreis: ${formatMoney(analysis.marketPrice)}` : 'Marktpreis: kein valider Marktpreis',
    analysis.marketAdvantagePct !== null ? `Marktvorteil: ${analysis.marketAdvantagePct}%` : 'Marktvorteil: nicht berechenbar',
    `Entscheidung: ${analysis.decision}`,
    `Quelle: ${analysis.decisionSourceLabel}`,
    analysis.amazonUrl || ''
  ];

  return lines.filter(Boolean).join('\n');
}

export function getDealEngineOutputSnapshot(settings) {
  const telegramConfig = getTelegramBotClientConfig();
  const whatsappConfig = getWhatsappClientConfig();
  const engineQueues = listPublishingQueue().filter((item) => item.source_type === 'deal_engine').slice(0, 8);

  return {
    telegram: {
      enabledByEngine: settings.output.telegramEnabled,
      enabledByClient: telegramConfig.enabled,
      configured: telegramConfig.tokenConfigured && telegramConfig.effectiveTargets.length > 0,
      targets: telegramConfig.effectiveTargets.length
    },
    whatsapp: {
      enabledByEngine: settings.output.whatsappEnabled,
      enabledByClient: whatsappConfig.enabled,
      configured: whatsappConfig.endpointConfigured,
      sender: whatsappConfig.sender,
      retryLimit: whatsappConfig.retryLimit
    },
    latestQueues: engineQueues
  };
}

export function enqueueApprovedDeal(analysis = {}, settings) {
  if (analysis.decision !== 'APPROVE') {
    return {
      status: analysis.decision === 'QUEUE' ? 'internal_queue_only' : 'not_sent',
      queueId: null,
      targetCount: 0,
      reason: analysis.decision === 'QUEUE' ? 'QUEUE bleibt intern sichtbar.' : 'Nur APPROVED Deals werden versendet.'
    };
  }

  const outputSnapshot = getDealEngineOutputSnapshot(settings);
  const targets = [];

  if (
    outputSnapshot.telegram.enabledByEngine &&
    outputSnapshot.telegram.enabledByClient &&
    outputSnapshot.telegram.configured
  ) {
    targets.push({
      channelType: 'telegram',
      isEnabled: true,
      imageSource: 'none'
    });
  }

  if (
    outputSnapshot.whatsapp.enabledByEngine &&
    outputSnapshot.whatsapp.enabledByClient &&
    outputSnapshot.whatsapp.configured
  ) {
    targets.push({
      channelType: 'whatsapp',
      isEnabled: true,
      imageSource: 'none'
    });
  }

  if (!targets.length) {
    return {
      status: 'approved_without_active_output',
      queueId: null,
      targetCount: 0,
      reason: 'Keine aktiven Telegram- oder WhatsApp-Outputs konfiguriert.'
    };
  }

  const queue = createPublishingEntry({
    sourceType: 'deal_engine',
    payload: {
      title: analysis.title,
      link: analysis.amazonUrl,
      normalizedUrl: analysis.amazonUrl,
      asin: analysis.asin,
      sellerType: analysis.sellerArea,
      textByChannel: {
        telegram: buildDealEngineMessage(analysis),
        whatsapp: buildDealEngineMessage(analysis)
      },
      couponCode: '',
      imageVariants: {},
      targetImageSources: {
        telegram: 'none',
        whatsapp: 'none'
      },
      meta: {
        decisionSource: analysis.decisionSource,
        dayPart: analysis.dayPart,
        reason: summarizeReasons(analysis.reasons)
      }
    },
    targets
  });

  return {
    status: 'queued_for_delivery',
    queueId: queue?.id ?? null,
    queueStatus: queue?.status || 'pending',
    targetCount: targets.length,
    reason: 'APPROVED Deal wurde an die bestehende Publishing Queue uebergeben.'
  };
}

