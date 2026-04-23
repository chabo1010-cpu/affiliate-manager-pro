import { cleanText, savePostedDeal } from './dealHistoryService.js';
import { buildPublishingChannelLabel } from './databaseService.js';
import { sendWhatsappDeal } from './whatsappClientService.js';

export async function processWhatsappPublishingTarget(target, queuePayload) {
  const text = queuePayload.textByChannel?.whatsapp || queuePayload.title || '';
  if (!text.trim()) {
    throw new Error('WhatsApp Payload ohne Text kann nicht verarbeitet werden.');
  }

  const imageUrl =
    target.image_source === 'standard'
      ? queuePayload.imageVariants?.standard || ''
      : target.image_source === 'upload'
      ? queuePayload.imageVariants?.upload || ''
      : '';
  const postedAt = new Date().toISOString();
  const result = await sendWhatsappDeal({
    queuePayload,
    text,
    imageUrl,
    imageSource: target.image_source,
    link: queuePayload.link,
    couponCode: queuePayload.couponCode,
    asin: queuePayload.asin,
    normalizedUrl: queuePayload.normalizedUrl || queuePayload.link,
    title: queuePayload.title,
    queueId: target.queue_id,
    sourceType: cleanText(queuePayload.databaseSourceType) || 'publisher_queue',
    origin: cleanText(queuePayload.databaseOrigin) || 'automatic',
    targetRef: target.target_ref || '',
    targetLabel: target.target_label || '',
    targetMeta: target.target_meta_json || null
  });

  if (queuePayload.skipPostedDealHistory !== true) {
    savePostedDeal({
      asin: queuePayload.asin || '',
      originalUrl: queuePayload.link,
      normalizedUrl: queuePayload.normalizedUrl || queuePayload.link,
      title: queuePayload.title,
      currentPrice: queuePayload.currentPrice || '',
      oldPrice: queuePayload.oldPrice || '',
      sellerType: queuePayload.sellerType || 'FBM',
      postedAt,
      channel: buildPublishingChannelLabel('whatsapp', target.target_label || result.targetLabel || target.target_ref || ''),
      couponCode: queuePayload.couponCode || '',
      sourceType: cleanText(queuePayload.databaseSourceType) || 'publisher_queue',
      sourceId: queuePayload.generatorPostId || queuePayload.sourceId || null,
      queueId: target.queue_id,
      origin: cleanText(queuePayload.databaseOrigin) || 'automatic',
      decisionReason: 'WhatsApp Queue-Target erfolgreich verarbeitet.',
      meta: {
        targetId: target.id,
        delivery: result
      }
    });
  }

  return result;
}
