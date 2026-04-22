import { getDb } from '../db.js';
import { cleanText, savePostedDeal } from './dealHistoryService.js';

const db = getDb();

export async function processFacebookPublishingTarget(target, queuePayload) {
  const settings = db.prepare(`SELECT * FROM app_settings WHERE id = 1`).get();
  if (settings?.facebookEnabled !== 1) {
    throw new Error('Facebook Worker ist deaktiviert.');
  }

  const text = queuePayload.textByChannel?.facebook || queuePayload.title || '';
  const imageUrl =
    target.image_source === 'standard'
      ? queuePayload.imageVariants?.standard || ''
      : target.image_source === 'upload'
        ? queuePayload.imageVariants?.upload || ''
        : '';

  savePostedDeal({
    asin: queuePayload.asin || '',
    originalUrl: queuePayload.link,
    normalizedUrl: queuePayload.normalizedUrl || queuePayload.link,
    title: queuePayload.title,
    currentPrice: queuePayload.currentPrice || '',
    oldPrice: queuePayload.oldPrice || '',
    sellerType: queuePayload.sellerType || 'FBM',
    postedAt: new Date().toISOString(),
    channel: 'FACEBOOK',
    couponCode: queuePayload.couponCode || '',
    sourceType: cleanText(queuePayload.databaseSourceType) || 'publisher_queue',
    sourceId: queuePayload.generatorPostId || queuePayload.sourceId || null,
    queueId: target.queue_id,
    origin: cleanText(queuePayload.databaseOrigin) || 'automatic',
    decisionReason: 'Facebook Queue-Target erfolgreich verarbeitet.'
  });

  return {
    status: 'facebook-session-ready',
    sessionMode: settings.facebookSessionMode || 'persistent',
    target: queuePayload.facebookTarget || settings.facebookDefaultTarget || '',
    textLength: text.length,
    link: queuePayload.link || '',
    imageUsed: target.image_source === 'link_preview' || target.image_source === 'none' ? null : imageUrl || null
  };
}
