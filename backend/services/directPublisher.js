import { getDb } from '../db.js';
import { getTelegramTestGroupConfig } from '../env.js';
import { cleanText, savePostedDeal } from './dealHistoryService.js';
import { buildGeneratorDealContext } from './generatorDealScoringService.js';
import { logGeneratorDebug } from './generatorFlowService.js';
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
          asin,
          normalized_url,
          seller_type,
          telegram_text,
          whatsapp_text,
          facebook_text,
          generated_image_path,
          uploaded_image_path,
          telegram_image_source,
          whatsapp_image_source,
          facebook_image_source,
          keepa_result_id,
          generator_context_json,
          created_at,
          updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `
    )
    .run(
      cleanText(input.title),
      cleanText(input.link),
      cleanText(input.asin).toUpperCase(),
      cleanText(input.normalizedUrl),
      cleanText(input.sellerType) || 'FBM',
      cleanText(input.textByChannel?.telegram),
      cleanText(input.textByChannel?.whatsapp),
      cleanText(input.textByChannel?.facebook),
      cleanText(input.generatedImagePath),
      cleanText(input.uploadedImagePath),
      cleanText(input.telegramImageSource) || 'standard',
      cleanText(input.whatsappImageSource) || 'standard',
      cleanText(input.facebookImageSource) || 'link_preview',
      input.generatorContext?.keepa?.keepaResultId || null,
      input.generatorContext ? JSON.stringify(input.generatorContext) : null,
      timestamp,
      timestamp
    );

  return result.lastInsertRowid;
}

function updateGeneratorPostMeta(generatorPostId, meta = {}) {
  db.prepare(
    `
      UPDATE generator_posts
      SET keepa_result_id = @keepaResultId,
          generator_context_json = @generatorContextJson,
          telegram_message_id = @telegramMessageId,
          posted_channels_json = @postedChannelsJson,
          updated_at = @updatedAt
      WHERE id = @id
    `
  ).run({
    id: generatorPostId,
    keepaResultId: meta.keepaResultId ?? null,
    generatorContextJson: meta.generatorContext ? JSON.stringify(meta.generatorContext) : null,
    telegramMessageId: meta.telegramMessageId ?? null,
    postedChannelsJson: JSON.stringify(meta.postedChannels || null),
    updatedAt: nowIso()
  });
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
  const testGroupConfig = getTelegramTestGroupConfig();
  const generatorContext = await buildGeneratorDealContext({
    asin: input.asin,
    sellerType: input.sellerType,
    currentPrice: input.currentPrice,
    title: input.title,
    productUrl: input.normalizedUrl || input.link,
    imageUrl: input.generatedImagePath,
    source: 'generator_direct_publish'
  });
  const preparedInput = {
    ...input,
    generatorContext
  };
  const generatorPostId = insertGeneratorPost(preparedInput);
  const postedAt = nowIso();
  const results = {
    generatorPostId,
    telegram: null,
    whatsapp: null,
    facebook: null
  };

  logGeneratorDebug('GENERATOR DIRECT TEST POST', {
    generatorPostId,
    asin: cleanText(input.asin).toUpperCase(),
    sellerType: cleanText(input.sellerType) || 'FBM',
    decision: generatorContext?.evaluation?.decision || 'manual_review',
    testGroupApproved: generatorContext?.evaluation?.testGroupApproved === true,
    keepaStatus: generatorContext?.keepa?.status || 'missing'
  });

  if (input.enableTelegram !== false) {
    const telegramImage = getImageForSource(input.telegramImageSource, input);
    const telegramResult = await sendTelegramPost({
      text: input.textByChannel?.telegram || input.title || '',
      uploadedFile: telegramImage.uploadedFile,
      uploadedImage: telegramImage.uploadedImage,
      imageUrl: telegramImage.imageUrl,
      disableWebPagePreview: !telegramImage.uploadedFile && !telegramImage.uploadedImage && !telegramImage.imageUrl,
      rabattgutscheinCode: input.couponCode,
      chatId: testGroupConfig.chatId
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

    logGeneratorDebug('TEST GROUP POST SENT', {
      generatorPostId,
      asin: cleanText(input.asin).toUpperCase(),
      messageId: telegramResult?.messageId || null,
      chatId: telegramResult?.chatId || testGroupConfig.chatId || null,
      imageSource: input.telegramImageSource || 'standard'
    });
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

  updateGeneratorPostMeta(generatorPostId, {
    keepaResultId: generatorContext?.keepa?.keepaResultId || null,
    generatorContext,
    telegramMessageId: results.telegram?.messageId || null,
    postedChannels: results
  });

  return {
    success: true,
    postedAt,
    results,
    generatorContext
  };
}
