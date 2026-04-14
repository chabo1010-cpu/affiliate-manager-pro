import { sendTelegramPost } from './telegramSenderService.js';
import { savePostedDeal } from './dealHistoryService.js';

export async function processTelegramPublishingTarget(target, queuePayload) {
  const imageSource = target.image_source;
  const imageUrl =
    imageSource === 'standard'
      ? queuePayload.imageVariants?.standard || ''
      : imageSource === 'upload'
        ? queuePayload.imageVariants?.upload || ''
        : '';
  const result = await sendTelegramPost({
    text: queuePayload.textByChannel?.telegram || queuePayload.title || '',
    imageUrl,
    rabattgutscheinCode: queuePayload.couponCode
  });

  savePostedDeal({
    asin: queuePayload.asin || '',
    originalUrl: queuePayload.link,
    normalizedUrl: queuePayload.normalizedUrl || queuePayload.link,
    title: queuePayload.title,
    currentPrice: queuePayload.currentPrice || '',
    oldPrice: queuePayload.oldPrice || '',
    sellerType: queuePayload.sellerType || 'FBM',
    postedAt: new Date().toISOString(),
    channel: 'TELEGRAM',
    couponCode: queuePayload.couponCode || ''
  });

  return result;
}
