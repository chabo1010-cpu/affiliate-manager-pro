import { cleanText, savePostedDeal } from './dealHistoryService.js';
import { sendTelegramDealToTargets } from './telegramBotClientService.js';

export async function processTelegramPublishingTarget(target, queuePayload) {
  const imageSource = target.image_source;
  const uploadedImage =
    imageSource === 'upload'
      ? queuePayload.imageVariants?.upload || ''
      : '';
  const imageUrl =
    imageSource === 'standard'
      ? queuePayload.imageVariants?.standard || ''
      : '';
  const postedAt = new Date().toISOString();
  const result = await sendTelegramDealToTargets({
    queuePayload,
    text: queuePayload.textByChannel?.telegram || queuePayload.title || '',
    uploadedImage,
    imageUrl,
    disableWebPagePreview: !uploadedImage && !imageUrl,
    telegramTargetChatIds: target.target_ref ? [target.target_ref] : [],
    rabattgutscheinCode: queuePayload.couponCode
  });

  result.targets.forEach((delivery) => {
    savePostedDeal({
      asin: queuePayload.asin || '',
      originalUrl: queuePayload.link,
      normalizedUrl: queuePayload.normalizedUrl || queuePayload.link,
      title: queuePayload.title,
      currentPrice: queuePayload.currentPrice || '',
      oldPrice: queuePayload.oldPrice || '',
      sellerType: queuePayload.sellerType || 'FBM',
      postedAt,
      channel: delivery.targetName ? `TELEGRAM:${delivery.targetName}` : 'TELEGRAM',
      couponCode: queuePayload.couponCode || '',
      sourceType: cleanText(queuePayload.databaseSourceType) || 'publisher_queue',
      sourceId: queuePayload.generatorPostId || queuePayload.sourceId || null,
      queueId: target.queue_id,
      origin: cleanText(queuePayload.databaseOrigin) || 'automatic',
      decisionReason: 'Telegram Queue-Target erfolgreich veroeffentlicht.',
      meta: {
        targetId: target.id,
        delivery
      }
    });
  });

  return result;
}
