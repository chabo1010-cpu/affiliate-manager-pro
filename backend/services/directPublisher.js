import { getDb } from '../db.js';
import { cleanText, savePostedDeal } from './dealHistoryService.js';
import { sendTelegramPost } from './telegramSenderService.js';

const db = getDb();

function nowIso() {
  return new Date().toISOString();
}

function insertGeneratorPost(input = {}) {
  const timestamp = nowIso();
  const result = db
    .prepare(
      `
        INSERT INTO generator_posts (
          title,
          product_link,
          telegram_text,
          whatsapp_text,
          facebook_text,
          generated_image_path,
          uploaded_image_path,
          telegram_image_source,
          whatsapp_image_source,
          facebook_image_source,
          created_at,
          updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `
    )
    .run(
      cleanText(input.title),
      cleanText(input.link),
      cleanText(input.textByChannel?.telegram),
      cleanText(input.textByChannel?.whatsapp),
      cleanText(input.textByChannel?.facebook),
      cleanText(input.generatedImagePath),
      cleanText(input.uploadedImagePath),
      cleanText(input.telegramImageSource) || 'standard',
      cleanText(input.whatsappImageSource) || 'standard',
      cleanText(input.facebookImageSource) || 'link_preview',
      timestamp,
      timestamp
    );

  return result.lastInsertRowid;
}

function getImageForSource(imageSource, input = {}) {
  if (imageSource === 'upload') {
    return {
      uploadedFile: input.uploadedImageFile || null,
      uploadedImage: cleanText(input.uploadedImagePath),
      imageUrl: ''
    };
  }

  if (imageSource === 'standard') {
    return {
      uploadedImage: '',
      imageUrl: cleanText(input.generatedImagePath)
    };
  }

  return {
    uploadedFile: null,
    uploadedImage: '',
    imageUrl: ''
  };
}

export async function publishGeneratorPostDirect(input = {}) {
  const generatorPostId = insertGeneratorPost(input);
  const postedAt = nowIso();
  const results = {
    generatorPostId,
    telegram: null,
    whatsapp: null,
    facebook: null
  };

  if (input.enableTelegram !== false) {
    const telegramImage = getImageForSource(input.telegramImageSource, input);
    const telegramResult = await sendTelegramPost({
      text: input.textByChannel?.telegram || input.title || '',
      uploadedFile: telegramImage.uploadedFile,
      uploadedImage: telegramImage.uploadedImage,
      imageUrl: telegramImage.imageUrl,
      disableWebPagePreview: !telegramImage.uploadedFile && !telegramImage.uploadedImage && !telegramImage.imageUrl,
      rabattgutscheinCode: input.couponCode
    });

    savePostedDeal({
      asin: input.asin || '',
      originalUrl: input.link,
      normalizedUrl: input.normalizedUrl || input.link,
      title: input.title,
      currentPrice: input.currentPrice || '',
      oldPrice: input.oldPrice || '',
      sellerType: input.sellerType || 'FBM',
      postedAt,
      channel: 'TELEGRAM',
      couponCode: input.couponCode || ''
    });

    results.telegram = {
      status: 'posted',
      imageSource: input.telegramImageSource || 'standard',
      ...telegramResult
    };
  }

  if (input.enableWhatsapp === true) {
    results.whatsapp = {
      status: 'not_implemented',
      imageSource: input.whatsappImageSource || 'standard'
    };
  }

  if (input.enableFacebook === true) {
    results.facebook = {
      status: 'not_implemented',
      imageSource: input.facebookImageSource || 'link_preview'
    };
  }

  return {
    success: true,
    postedAt,
    results
  };
}
