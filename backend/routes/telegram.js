import { Router } from 'express';
import { getTelegramConfig } from '../env.js';
import { getTelegramCopyButtonText } from '../services/dealHistoryService.js';

const router = Router();
async function sendTelegramRequest(token, method, payload, options = {}) {
  const useHtml = options.html !== false;
  const finalPayload = useHtml
    ? {
        ...payload,
        parse_mode: 'HTML'
      }
    : { ...payload };

  console.log('[telegram/send] final telegram request body', {
    method,
    finalCaption: typeof finalPayload.caption === 'string' ? finalPayload.caption : '',
    parse_mode: finalPayload.parse_mode,
    caption: typeof finalPayload.caption === 'string' ? finalPayload.caption : '',
    text: typeof finalPayload.text === 'string' ? finalPayload.text : '',
    photo: typeof finalPayload.photo === 'string' ? finalPayload.photo : '',
    reply_markup: finalPayload.reply_markup ?? null
  });

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

  console.log('[telegram/send] telegram response', {
    status: telegramResponse.status,
    data: telegramData
  });

  return {
    telegramResponse,
    telegramData
  };
}

router.get('/send', (req, res) => {
  res.status(405).json({
    success: false,
    error: 'Diese Route akzeptiert nur POST /api/telegram/send',
    code: 'METHOD_NOT_ALLOWED'
  });
});

router.post('/send', async (req, res) => {
  try {
    const {
      text,
      imageUrl,
      amazonLink,
      rabattgutscheinCode,
      chatId: bodyChatId
    } = req.body ?? {};
    const { token, chatId: envChatId } = getTelegramConfig();
    const finalChatId = (bodyChatId || envChatId || '').toString().trim();
    const trimmedImageUrl = typeof imageUrl === 'string' ? imageUrl.trim() : '';
    const trimmedCouponCode = typeof rabattgutscheinCode === 'string' ? rabattgutscheinCode.trim() : '';
    const buttonText = getTelegramCopyButtonText().trim() || '📋 Zum Kopieren hier klicken';
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
    const editOperationPayload = null;

    console.log('[telegram/send] incoming request', {
      hasText: typeof text === 'string' && text.trim().length > 0,
      hasImageUrl: Boolean(trimmedImageUrl),
      hasToken: Boolean(token),
      hasChatId: Boolean(finalChatId),
      imageUrl: trimmedImageUrl || '',
      amazonLink: typeof amazonLink === 'string' ? amazonLink : '',
      textLength: typeof text === 'string' ? text.length : 0,
      hasCouponCode: Boolean(trimmedCouponCode)
    });
    console.log('MAIN POST COUPON RAW', rabattgutscheinCode);
    console.log('MAIN POST COUPON TRIMMED', trimmedCouponCode);
    console.log('MAIN POST WITH COUPON', trimmedCouponCode);
    console.log('MAIN POST BUTTON TEXT', 'Code kopieren');
    console.log('MAIN POST REPLY MARKUP', JSON.stringify(replyMarkup, null, 2));
    console.log('SECOND COUPON MESSAGE DISABLED', true);
    console.log('TELEGRAM SEND START', { hasCoupon: !!trimmedCouponCode });

    if (!req.body || typeof req.body !== 'object') {
      return res.status(400).json({
        success: false,
        error: 'JSON-Body fehlt oder konnte nicht geparst werden',
        code: 'INVALID_JSON_BODY'
      });
    }

    if (!text || typeof text !== 'string' || text.trim().length === 0) {
      return res.status(400).json({
        success: false,
        error: 'Text ist erforderlich',
        code: 'INVALID_TEXT'
      });
    }

    if (!token) {
      return res.status(500).json({
        success: false,
        error: 'TELEGRAM_BOT_TOKEN fehlt im Backend',
        code: 'MISSING_TELEGRAM_BOT_TOKEN'
      });
    }

    if (!finalChatId) {
      return res.status(500).json({
        success: false,
        error: 'TELEGRAM_CHAT_ID fehlt im Backend',
        code: 'MISSING_TELEGRAM_CHAT_ID'
      });
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

    console.log('MAIN POST PAYLOAD', JSON.stringify(telegramPayload, null, 2));
    console.log('USES INLINE KEYBOARD', !!telegramPayload.reply_markup?.inline_keyboard);
    console.log('USES REPLY KEYBOARD', !!telegramPayload.reply_markup?.keyboard);
    console.log('HAS INLINE KEYBOARD AT SEND TIME', !!telegramPayload.reply_markup?.inline_keyboard);
    console.log('ANY EDIT AFTER SEND', editOperationPayload);

    const { telegramResponse, telegramData } = await sendTelegramRequest(token, telegramMethod, telegramPayload);

    console.log('[telegram/send] main telegram response', {
      method: telegramMethod,
      status: telegramResponse.status,
      response: telegramData
    });

    if (!telegramResponse.ok || !telegramData?.ok) {
      const telegramDescription =
        telegramData?.description || telegramData?.raw || 'Telegram API hat einen unbekannten Fehler geliefert';

      return res.status(502).json({
        success: false,
        error: `Telegram API Fehler: ${telegramDescription}`,
        code: 'TELEGRAM_API_ERROR',
        details: {
          method: telegramMethod,
          status: telegramResponse.status
        }
      });
    }

    return res.status(200).json({
      success: true,
      message: trimmedImageUrl
        ? 'Post erfolgreich mit Bild zu Telegram gesendet'
        : 'Post erfolgreich zu Telegram gesendet',
      data: {
        method: telegramMethod,
        messageId: telegramData.result?.message_id,
        couponMessageId: null,
        chatId: telegramData.result?.chat?.id ?? finalChatId,
        textLength: text.length,
        imageUrl: trimmedImageUrl || null
      }
    });
  } catch (error) {
    console.error('[telegram/send] unexpected error', error);

    return res.status(500).json({
      success: false,
      error:
        error instanceof Error
          ? `Fehler beim Telegram-Versand: ${error.message}`
          : 'Fehler beim Versenden. Bitte versuchen Sie es spaeter erneut.',
      code: 'SEND_ERROR'
    });
  }
});

export default router;
