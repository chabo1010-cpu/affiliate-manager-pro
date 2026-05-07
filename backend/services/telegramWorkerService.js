import { cleanText, savePostedDeal } from './dealHistoryService.js';
import { buildPublishingChannelLabel } from './databaseService.js';
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
  console.info('[TELEGRAM_PUBLISHER_STATUS]', {
    queueId: target.queue_id,
    targetId: target.id,
    targetRef: target.target_ref || '',
    targetLabel: target.target_label || '',
    imageSource,
    hasUploadedImage: Boolean(uploadedImage),
    hasImageUrl: Boolean(imageUrl),
    hasCouponCode: Boolean(cleanText(queuePayload.couponCode))
  });
  const result = await sendTelegramDealToTargets({
    queuePayload,
    queueId: target.queue_id,
    publishingTargetId: target.id,
    text: queuePayload.textByChannel?.telegram || queuePayload.title || '',
    uploadedImage,
    imageUrl,
    disableWebPagePreview: !uploadedImage && !imageUrl,
    telegramTargetChatIds: target.target_ref ? [target.target_ref] : [],
    rabattgutscheinCode: queuePayload.couponCode
  });
  console.info('[TELEGRAM_PUBLISHER_STATUS]', {
    queueId: target.queue_id,
    targetId: target.id,
    targetRef: target.target_ref || '',
    targetLabel: target.target_label || '',
    deliveryCount: Array.isArray(result.targets) ? result.targets.length : 0,
    sentCount: Array.isArray(result.targets) ? result.targets.filter((item) => cleanText(item?.messageId || '')).length : 0,
    duplicateCount: Array.isArray(result.targets) ? result.targets.filter((item) => item?.duplicateBlocked === true).length : 0,
    skippedCount: Array.isArray(result.targets) ? result.targets.filter((item) => item?.skipped === true).length : 0
  });

  if (queuePayload.skipPostedDealHistory !== true) {
    result.targets.forEach((delivery) => {
      if (delivery?.duplicateBlocked === true || delivery?.skipped === true || !cleanText(delivery?.messageId || '')) {
        return;
      }

      savePostedDeal({
        asin: queuePayload.asin || '',
        originalUrl: queuePayload.link,
        normalizedUrl: queuePayload.normalizedUrl || queuePayload.link,
        title: queuePayload.title,
        currentPrice: queuePayload.currentPrice || '',
        oldPrice: queuePayload.oldPrice || '',
        sellerType: queuePayload.sellerType || 'FBM',
        postedAt,
        channel: buildPublishingChannelLabel('telegram', target.target_label || delivery.targetName || target.target_ref || ''),
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
  }

  return result;
}
