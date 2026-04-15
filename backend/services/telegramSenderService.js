import { getTelegramConfig } from '../env.js';
import { getTelegramCopyButtonText } from './dealHistoryService.js';
import sharp from 'sharp';

const NORMALIZED_POST_IMAGE = {
  width: 1200,
  height: 1200,
  padding: 140,
  background: '#ffffff'
};

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
    console.error('Uploaded image conversion failed: missing base64 payload');
    throw new Error('Upload-Bild ist unvollstaendig oder leer.');
  }

  try {
    const mimeTypeMatch = metaPart.match(/^data:(image\/[a-zA-Z0-9.+-]+);base64$/);
    const mimeType = mimeTypeMatch?.[1] || 'image/jpeg';
    const extension = mimeType.split('/')[1]?.replace(/[^a-zA-Z0-9]/g, '') || 'jpg';
    const paddedBase64 = base64Data.padEnd(base64Data.length + ((4 - (base64Data.length % 4)) % 4), '=');
    const buffer = Buffer.from(paddedBase64, 'base64');

    if (!buffer.length) {
      console.error('Uploaded image conversion failed: empty buffer');
      throw new Error('Upload-Bild ist leer.');
    }

    return {
      buffer,
      mimeType,
      filename: `upload.${extension}`
    };
  } catch (error) {
    console.error('Uploaded image conversion failed:', error);
    throw new Error('Upload-Bild konnte nicht in ein gueltiges Telegram-Bild umgewandelt werden.');
  }
}

function normalizeUploadedFile(uploadedFile) {
  if (!uploadedFile || !uploadedFile.buffer) {
    return null;
  }

  if (!Buffer.isBuffer(uploadedFile.buffer) || uploadedFile.buffer.length === 0) {
    console.error('Uploaded file conversion failed: empty buffer');
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

async function normalizeImageForTelegram(inputBuffer, sourceLabel) {
  console.log('IMAGE NORMALIZATION SOURCE:', sourceLabel);
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

  console.log('IMAGE NORMALIZATION APPLIED');
  console.log('WHITE BORDER APPLIED');

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

  if (!text || typeof text !== 'string' || text.trim().length === 0) {
    throw new Error('Text ist erforderlich');
  }

  if (!token) {
    throw new Error('TELEGRAM_BOT_TOKEN fehlt im Backend');
  }

  if (!finalChatId) {
    throw new Error('TELEGRAM_CHAT_ID fehlt im Backend');
  }

  const effectiveImageUrl = parsedUploadedImage ? '' : trimmedImageUrl;
  const telegramMethod = parsedUploadedImage || effectiveImageUrl ? 'sendPhoto' : 'sendMessage';
  let telegramResponse;
  let telegramData;

  if (parsedUploadedImage) {
    console.log('GENERATOR SEND MODE: IMAGE_UPLOAD');
    console.log('GENERATOR TELEGRAM SEND WITH IMAGE');
    console.log('Upload Bild verwendet');
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
    console.log('GENERATOR SEND MODE: STANDARD_IMAGE');
    console.log('GENERATOR TELEGRAM SEND WITH IMAGE');
    console.log('Fallback Amazon Bild');
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
    console.log('GENERATOR SEND MODE: TEXT_ONLY');
    console.log('GENERATOR TELEGRAM PREVIEW DISABLED');
    console.log('GENERATOR TELEGRAM SEND TEXT ONLY WITHOUT LINK PREVIEW');
    ({ telegramResponse, telegramData } = await sendTelegramRequest(token, telegramMethod, {
      chat_id: finalChatId,
      text: String(text),
      ...(disableWebPagePreview ? { disable_web_page_preview: true } : {}),
      ...(replyMarkup ? { reply_markup: replyMarkup } : {})
    }));
  }

  if (!telegramResponse.ok || !telegramData?.ok) {
    const telegramDescription =
      telegramData?.description || telegramData?.raw || 'Telegram API hat einen unbekannten Fehler geliefert';
    throw new Error(`Telegram API Fehler: ${telegramDescription}`);
  }

  return {
    method: telegramMethod,
    messageId: telegramData.result?.message_id,
    chatId: telegramData.result?.chat?.id ?? finalChatId,
    imageUrl: effectiveImageUrl || null
  };
}
