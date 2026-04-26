import { getTelegramConfig } from '../env.js';
import { getTelegramCopyButtonText } from './dealHistoryService.js';
import { logGeneratorDebug } from './generatorFlowService.js';
import sharp from 'sharp';

const NORMALIZED_POST_IMAGE = {
  width: 1200,
  height: 1200,
  padding: 140,
  background: '#ffffff'
};
const TELEGRAM_CAPTION_LIMIT = 1024;
const TELEGRAM_MESSAGE_LIMIT = 4096;
const TELEGRAM_SHORT_CAPTION_FALLBACK = '🔥 Deal gefunden';

async function sendTelegramRequest(token, method, payload, options = {}) {
  const useHtml = options.html !== false;
  const finalPayload = useHtml
    ? {
        ...payload,
        parse_mode: 'HTML'
      }
    : { ...payload };

  const telegramResponse = await fetch(`https://api.telegram.org/bot${token}/${method}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(finalPayload)
  });

  const responseText = await telegramResponse.text();
  let telegramData;

  try {
    telegramData = JSON.parse(responseText);
  } catch {
    telegramData = { raw: responseText };
  }

  return {
    telegramResponse,
    telegramData
  };
}

async function sendTelegramMultipartRequest(token, method, formData) {
  const telegramResponse = await fetch(`https://api.telegram.org/bot${token}/${method}`, {
    method: 'POST',
    body: formData
  });

  const responseText = await telegramResponse.text();
  let telegramData;

  try {
    telegramData = JSON.parse(responseText);
  } catch {
    telegramData = { raw: responseText };
  }

  return {
    telegramResponse,
    telegramData
  };
}

function parseUploadedImage(uploadedImage) {
  const trimmedUploadedImage = typeof uploadedImage === 'string' ? uploadedImage.trim() : '';
  if (!trimmedUploadedImage) {
    return null;
  }

  if (!trimmedUploadedImage.startsWith('data:image')) {
    return null;
  }

  const [metaPart, base64Data] = trimmedUploadedImage.split(',', 2);
  if (!base64Data) {
    throw new Error('Upload-Bild ist unvollstaendig oder leer.');
  }

  try {
    const mimeTypeMatch = metaPart.match(/^data:(image\/[a-zA-Z0-9.+-]+);base64$/);
    const mimeType = mimeTypeMatch?.[1] || 'image/jpeg';
    const extension = mimeType.split('/')[1]?.replace(/[^a-zA-Z0-9]/g, '') || 'jpg';
    const paddedBase64 = base64Data.padEnd(base64Data.length + ((4 - (base64Data.length % 4)) % 4), '=');
    const buffer = Buffer.from(paddedBase64, 'base64');

    if (!buffer.length) {
      throw new Error('Upload-Bild ist leer.');
    }

    return {
      buffer,
      mimeType,
      filename: `upload.${extension}`
    };
  } catch {
    throw new Error('Upload-Bild konnte nicht in ein gueltiges Telegram-Bild umgewandelt werden.');
  }
}

function normalizeUploadedFile(uploadedFile) {
  if (!uploadedFile || !uploadedFile.buffer) {
    return null;
  }

  if (!Buffer.isBuffer(uploadedFile.buffer) || uploadedFile.buffer.length === 0) {
    throw new Error('Upload-Bild ist leer oder ungueltig.');
  }

  const mimeType =
    typeof uploadedFile.mimetype === 'string' && uploadedFile.mimetype.trim()
      ? uploadedFile.mimetype.trim()
      : 'image/jpeg';
  const originalName =
    typeof uploadedFile.originalname === 'string' && uploadedFile.originalname.trim()
      ? uploadedFile.originalname.trim()
      : `upload.${mimeType.split('/')[1] || 'jpg'}`;

  return {
    buffer: uploadedFile.buffer,
    mimeType,
    filename: originalName
  };
}

async function normalizeImageForTelegram(inputBuffer) {
  const innerWidth = NORMALIZED_POST_IMAGE.width - NORMALIZED_POST_IMAGE.padding * 2;
  const innerHeight = NORMALIZED_POST_IMAGE.height - NORMALIZED_POST_IMAGE.padding * 2;

  const fittedImage = await sharp(inputBuffer)
    .rotate()
    .resize({
      width: innerWidth,
      height: innerHeight,
      fit: 'contain',
      background: NORMALIZED_POST_IMAGE.background
    })
    .png()
    .toBuffer();

  const normalizedBuffer = await sharp({
    create: {
      width: NORMALIZED_POST_IMAGE.width,
      height: NORMALIZED_POST_IMAGE.height,
      channels: 4,
      background: NORMALIZED_POST_IMAGE.background
    }
  })
    .composite([
      {
        input: fittedImage,
        left: NORMALIZED_POST_IMAGE.padding,
        top: NORMALIZED_POST_IMAGE.padding
      }
    ])
    .png()
    .toBuffer();

  return {
    buffer: normalizedBuffer,
    mimeType: 'image/png',
    filename: 'normalized-post-image.png'
  };
}

async function fetchAndNormalizeImageUrl(imageUrl) {
  const imageResponse = await fetch(imageUrl);
  if (!imageResponse.ok) {
    throw new Error(`Bild konnte nicht geladen werden (${imageResponse.status}).`);
  }

  const arrayBuffer = await imageResponse.arrayBuffer();
  return await normalizeImageForTelegram(Buffer.from(arrayBuffer), 'amazon');
}

function splitTelegramTextIntoChunks(text = '', limit = TELEGRAM_MESSAGE_LIMIT) {
  const normalizedText = typeof text === 'string' ? text.trim() : '';
  if (!normalizedText) {
    return [];
  }

  const chunks = [];
  let currentChunk = '';
  const lines = normalizedText.split('\n');

  const flushChunk = () => {
    const safeChunk = currentChunk.trim();
    if (safeChunk) {
      chunks.push(safeChunk);
    }
    currentChunk = '';
  };

  for (const line of lines) {
    const safeLine = String(line ?? '');
    const candidate = currentChunk ? `${currentChunk}\n${safeLine}` : safeLine;

    if (candidate.length <= limit) {
      currentChunk = candidate;
      continue;
    }

    if (currentChunk) {
      flushChunk();
    }

    if (safeLine.length <= limit) {
      currentChunk = safeLine;
      continue;
    }

    let remainder = safeLine;
    while (remainder.length > limit) {
      let splitIndex = remainder.lastIndexOf(' ', limit);
      if (splitIndex <= 0) {
        splitIndex = limit;
      }
      chunks.push(remainder.slice(0, splitIndex).trim());
      remainder = remainder.slice(splitIndex).trim();
    }

    currentChunk = remainder;
  }

  flushChunk();
  return chunks.filter(Boolean);
}

async function sendSingleTelegramDelivery({
  token,
  finalChatId,
  text,
  parsedUploadedImage = null,
  effectiveImageUrl = '',
  resolvedDisableWebPagePreview = false,
  replyMarkup = undefined
}) {
  const telegramMethod = parsedUploadedImage || effectiveImageUrl ? 'sendPhoto' : 'sendMessage';
  let telegramResponse;
  let telegramData;

  console.info('[TELEGRAM_SEND_START]', {
    chatId: finalChatId,
    method: telegramMethod,
    textLength: text.trim().length,
    hasUploadedImage: Boolean(parsedUploadedImage),
    hasImageUrl: Boolean(effectiveImageUrl),
    hasCouponCode: Boolean(replyMarkup)
  });
  console.info('[TELEGRAM_FORCE_SEND_START]', {
    chatId: finalChatId,
    method: telegramMethod,
    payload: {
      text: String(text),
      textLength: text.trim().length,
      uploadedImage: Boolean(parsedUploadedImage),
      imageUrl: effectiveImageUrl || null,
      disableWebPagePreview: resolvedDisableWebPagePreview,
      replyMarkup: Boolean(replyMarkup)
    }
  });

  logGeneratorDebug('api.telegram.request', {
    method: telegramMethod,
    textLength: text.trim().length,
    hasUploadedImage: Boolean(parsedUploadedImage),
    hasImageUrl: Boolean(effectiveImageUrl),
    disableWebPagePreview: resolvedDisableWebPagePreview,
    hasCouponCode: Boolean(replyMarkup)
  });

  if (parsedUploadedImage) {
    const normalizedImage = await normalizeImageForTelegram(parsedUploadedImage.buffer, 'upload');
    const formData = new FormData();
    formData.append('chat_id', finalChatId);
    formData.append('caption', String(text));
    formData.append('parse_mode', 'HTML');
    if (replyMarkup) {
      formData.append('reply_markup', JSON.stringify(replyMarkup));
    }

    const photoBlob = new Blob([normalizedImage.buffer], { type: normalizedImage.mimeType });
    formData.append('photo', photoBlob, normalizedImage.filename);

    ({ telegramResponse, telegramData } = await sendTelegramMultipartRequest(token, telegramMethod, formData));
  } else if (effectiveImageUrl) {
    const normalizedImage = await fetchAndNormalizeImageUrl(effectiveImageUrl);
    const formData = new FormData();
    formData.append('chat_id', finalChatId);
    formData.append('caption', String(text));
    formData.append('parse_mode', 'HTML');
    if (replyMarkup) {
      formData.append('reply_markup', JSON.stringify(replyMarkup));
    }

    const photoBlob = new Blob([normalizedImage.buffer], { type: normalizedImage.mimeType });
    formData.append('photo', photoBlob, normalizedImage.filename);

    ({ telegramResponse, telegramData } = await sendTelegramMultipartRequest(token, telegramMethod, formData));
  } else {
    ({ telegramResponse, telegramData } = await sendTelegramRequest(token, telegramMethod, {
      chat_id: finalChatId,
      text: String(text),
      ...(resolvedDisableWebPagePreview ? { disable_web_page_preview: true } : {}),
      ...(replyMarkup ? { reply_markup: replyMarkup } : {})
    }));
  }

  if (!telegramResponse.ok || !telegramData?.ok) {
    const telegramDescription =
      telegramData?.description || telegramData?.raw || 'Telegram API hat einen unbekannten Fehler geliefert';
    console.warn('[NO_POST_REASON]', {
      reason: 'Telegram Send Fehler',
      detail: telegramDescription,
      chatId: finalChatId,
      method: telegramMethod
    });
    console.error('[TELEGRAM_SEND_ERROR]', {
      chatId: finalChatId,
      method: telegramMethod,
      error: telegramDescription
    });
    console.error('[TELEGRAM_FORCE_SEND_ERROR]', {
      chatId: finalChatId,
      method: telegramMethod,
      reason: telegramDescription
    });
    logGeneratorDebug('api.telegram.error', {
      method: telegramMethod,
      error: telegramDescription,
      disableWebPagePreview: resolvedDisableWebPagePreview
    });
    throw new Error(`Telegram API Fehler: ${telegramDescription}`);
  }

  logGeneratorDebug('api.telegram.success', {
    method: telegramMethod,
    messageId: telegramData.result?.message_id,
    hasImageUrl: Boolean(effectiveImageUrl),
    disableWebPagePreview: resolvedDisableWebPagePreview
  });

  console.info('[TELEGRAM_SEND_SUCCESS]', {
    chatId: telegramData.result?.chat?.id ?? finalChatId,
    method: telegramMethod,
    messageId: telegramData.result?.message_id
  });
  console.info('[TELEGRAM_FORCE_SEND_SUCCESS]', {
    chatId: telegramData.result?.chat?.id ?? finalChatId,
    method: telegramMethod,
    messageId: telegramData.result?.message_id
  });

  return {
    method: telegramMethod,
    messageId: telegramData.result?.message_id,
    chatId: telegramData.result?.chat?.id ?? finalChatId,
    imageUrl: effectiveImageUrl || null
  };
}

export async function sendTelegramPost({
  text,
  uploadedFile,
  uploadedImage,
  imageUrl,
  disableWebPagePreview = false,
  rabattgutscheinCode,
  chatId
}) {
  const { token, chatId: envChatId } = getTelegramConfig();
  const finalChatId = (chatId || envChatId || '').toString().trim();
  const normalizedUploadedFile = normalizeUploadedFile(uploadedFile);
  const parsedUploadedImage = normalizedUploadedFile || parseUploadedImage(uploadedImage);
  const trimmedImageUrl = typeof imageUrl === 'string' ? imageUrl.trim() : '';
  const trimmedCouponCode = typeof rabattgutscheinCode === 'string' ? rabattgutscheinCode.trim() : '';
  const buttonText = getTelegramCopyButtonText().trim();
  const replyMarkup = trimmedCouponCode
    ? {
        inline_keyboard: [
          [
            {
              text: buttonText,
              copy_text: {
                text: trimmedCouponCode
              }
            }
          ]
        ]
      }
    : undefined;
  const effectiveImageUrl = parsedUploadedImage ? '' : trimmedImageUrl;
  const resolvedDisableWebPagePreview = disableWebPagePreview || (!parsedUploadedImage && !effectiveImageUrl);
  const telegramMethod = parsedUploadedImage || effectiveImageUrl ? 'sendPhoto' : 'sendMessage';
  const trimmedText = text.trim();

  console.info('[OUTPUT_CONFIG]', {
    configSource: 'telegram_sender',
    explicitChatId: (chatId || '').toString().trim() || null,
    envChatId: envChatId || null,
    finalChatId: finalChatId || null,
    tokenConfigured: Boolean(token),
    method: telegramMethod
  });
  console.info('[OUTPUT_PAYLOAD]', {
    configSource: 'telegram_sender',
    textLength: typeof text === 'string' ? text.trim().length : 0,
    textPreview: typeof text === 'string' ? text.trim().slice(0, 160) : '',
    hasUploadedImage: Boolean(parsedUploadedImage),
    hasImageUrl: Boolean(effectiveImageUrl),
    disableWebPagePreview: resolvedDisableWebPagePreview,
    hasCouponCode: Boolean(trimmedCouponCode)
  });

  if (!text || typeof text !== 'string' || text.trim().length === 0) {
    console.warn('[NO_POST_REASON]', {
      reason: 'Telegram Send Fehler',
      detail: 'Text ist erforderlich',
      chatId: finalChatId || null,
      method: telegramMethod
    });
    console.error('[TELEGRAM_SEND_ERROR]', {
      chatId: finalChatId || null,
      method: telegramMethod,
      error: 'Text ist erforderlich'
    });
    console.error('[TELEGRAM_FORCE_SEND_ERROR]', {
      chatId: finalChatId || null,
      method: telegramMethod,
      reason: 'Text ist erforderlich'
    });
    throw new Error('Text ist erforderlich');
  }

  if (!token) {
    console.warn('[NO_POST_REASON]', {
      reason: 'Telegram Send Fehler',
      detail: 'TELEGRAM_BOT_TOKEN fehlt im Backend',
      chatId: finalChatId || null,
      method: telegramMethod
    });
    console.error('[TELEGRAM_SEND_ERROR]', {
      chatId: finalChatId || null,
      method: telegramMethod,
      error: 'TELEGRAM_BOT_TOKEN fehlt im Backend'
    });
    console.error('[TELEGRAM_FORCE_SEND_ERROR]', {
      chatId: finalChatId || null,
      method: telegramMethod,
      reason: 'TELEGRAM_BOT_TOKEN fehlt im Backend'
    });
    throw new Error('TELEGRAM_BOT_TOKEN fehlt im Backend');
  }

  if (!finalChatId) {
    console.warn('[NO_POST_REASON]', {
      reason: 'Telegram Send Fehler',
      detail: 'TELEGRAM_CHAT_ID fehlt im Backend',
      chatId: null,
      method: telegramMethod
    });
    console.error('[TELEGRAM_SEND_ERROR]', {
      chatId: null,
      method: telegramMethod,
      error: 'TELEGRAM_CHAT_ID fehlt im Backend'
    });
    console.error('[TELEGRAM_FORCE_SEND_ERROR]', {
      chatId: null,
      method: telegramMethod,
      reason: 'TELEGRAM_CHAT_ID fehlt im Backend'
    });
    throw new Error('TELEGRAM_CHAT_ID fehlt im Backend');
  }
  if ((parsedUploadedImage || effectiveImageUrl) && trimmedText.length > TELEGRAM_CAPTION_LIMIT) {
    console.warn('[CAPTION_TOO_LONG]', {
      chatId: finalChatId,
      method: telegramMethod,
      captionLength: trimmedText.length,
      captionLimit: TELEGRAM_CAPTION_LIMIT
    });

    const photoResult = await sendSingleTelegramDelivery({
      token,
      finalChatId,
      text: TELEGRAM_SHORT_CAPTION_FALLBACK,
      parsedUploadedImage,
      effectiveImageUrl,
      resolvedDisableWebPagePreview: false,
      replyMarkup
    });

    console.info('[PHOTO_SENT_WITH_SHORT_CAPTION]', {
      chatId: photoResult.chatId,
      messageId: photoResult.messageId,
      shortCaption: TELEGRAM_SHORT_CAPTION_FALLBACK
    });

    const textChunks = splitTelegramTextIntoChunks(trimmedText, TELEGRAM_MESSAGE_LIMIT);
    const textResults = [];

    for (const chunk of textChunks) {
      const textResult = await sendSingleTelegramDelivery({
        token,
        finalChatId,
        text: chunk,
        parsedUploadedImage: null,
        effectiveImageUrl: '',
        resolvedDisableWebPagePreview: true,
        replyMarkup: undefined
      });
      textResults.push(textResult);
    }

    if (textResults.length === 1) {
      console.info('[TEXT_SENT_AFTER_PHOTO]', {
        chatId: textResults[0].chatId,
        messageId: textResults[0].messageId
      });
    } else if (textResults.length > 1) {
      console.info('[TEXT_SPLIT_SENT]', {
        chatId: finalChatId,
        parts: textResults.length,
        messageIds: textResults.map((item) => item.messageId)
      });
    }

    return {
      method: photoResult.method,
      messageId: photoResult.messageId,
      chatId: photoResult.chatId,
      imageUrl: photoResult.imageUrl,
      extraMessageIds: textResults.map((item) => item.messageId)
    };
  }

  if (!parsedUploadedImage && !effectiveImageUrl && trimmedText.length > TELEGRAM_MESSAGE_LIMIT) {
    const textChunks = splitTelegramTextIntoChunks(trimmedText, TELEGRAM_MESSAGE_LIMIT);
    const textResults = [];

    for (let index = 0; index < textChunks.length; index += 1) {
      const textResult = await sendSingleTelegramDelivery({
        token,
        finalChatId,
        text: textChunks[index],
        parsedUploadedImage: null,
        effectiveImageUrl: '',
        resolvedDisableWebPagePreview: true,
        replyMarkup: index === 0 ? replyMarkup : undefined
      });
      textResults.push(textResult);
    }

    console.info('[TEXT_SPLIT_SENT]', {
      chatId: finalChatId,
      parts: textResults.length,
      messageIds: textResults.map((item) => item.messageId)
    });

    return {
      method: 'sendMessage',
      messageId: textResults[0]?.messageId || null,
      chatId: textResults[0]?.chatId || finalChatId,
      imageUrl: null,
      extraMessageIds: textResults.slice(1).map((item) => item.messageId)
    };
  }

  return await sendSingleTelegramDelivery({
    token,
    finalChatId,
    text: trimmedText,
    parsedUploadedImage,
    effectiveImageUrl,
    resolvedDisableWebPagePreview,
    replyMarkup
  });
}
