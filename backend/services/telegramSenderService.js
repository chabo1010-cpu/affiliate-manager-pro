import { getTelegramConfig } from '../env.js';
import { getTelegramCopyButtonText } from './dealHistoryService.js';

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

export async function sendTelegramPost({
  text,
  imageUrl,
  rabattgutscheinCode,
  chatId
}) {
  const { token, chatId: envChatId } = getTelegramConfig();
  const finalChatId = (chatId || envChatId || '').toString().trim();
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

  const telegramMethod = trimmedImageUrl ? 'sendPhoto' : 'sendMessage';
  const telegramPayload = trimmedImageUrl
    ? {
        chat_id: finalChatId,
        photo: trimmedImageUrl,
        caption: String(text),
        ...(replyMarkup ? { reply_markup: replyMarkup } : {})
      }
    : {
        chat_id: finalChatId,
        text: String(text),
        ...(replyMarkup ? { reply_markup: replyMarkup } : {})
      };

  const { telegramResponse, telegramData } = await sendTelegramRequest(token, telegramMethod, telegramPayload);
  if (!telegramResponse.ok || !telegramData?.ok) {
    const telegramDescription =
      telegramData?.description || telegramData?.raw || 'Telegram API hat einen unbekannten Fehler geliefert';
    throw new Error(`Telegram API Fehler: ${telegramDescription}`);
  }

  return {
    method: telegramMethod,
    messageId: telegramData.result?.message_id,
    chatId: telegramData.result?.chat?.id ?? finalChatId,
    imageUrl: trimmedImageUrl || null
  };
}
