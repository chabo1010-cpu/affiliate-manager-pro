import { getDb } from '../db.js';

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

  return {
    status: 'facebook-session-ready',
    sessionMode: settings.facebookSessionMode || 'persistent',
    target: queuePayload.facebookTarget || settings.facebookDefaultTarget || '',
    textLength: text.length,
    link: queuePayload.link || '',
    imageUsed: target.image_source === 'link_preview' || target.image_source === 'none' ? null : imageUrl || null
  };
}
