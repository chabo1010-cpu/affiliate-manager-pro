import { Router } from 'express';
import { sendTelegramPost } from '../services/telegramSenderService.js';

const router = Router();

router.get('/send', (req, res) => {
  res.status(405).json({
    success: false,
    error: 'Diese Route akzeptiert nur POST /api/telegram/send',
    code: 'METHOD_NOT_ALLOWED'
  });
});

router.post('/send', async (req, res) => {
  try {
    const { text, imageUrl, rabattgutscheinCode, chatId } = req.body ?? {};

    if (!req.body || typeof req.body !== 'object') {
      return res.status(400).json({
        success: false,
        error: 'JSON-Body fehlt oder konnte nicht geparst werden',
        code: 'INVALID_JSON_BODY'
      });
    }

    const result = await sendTelegramPost({
      text,
      imageUrl,
      rabattgutscheinCode,
      chatId
    });

    return res.status(200).json({
      success: true,
      message: result.imageUrl
        ? 'Post erfolgreich mit Bild zu Telegram gesendet'
        : 'Post erfolgreich zu Telegram gesendet',
      data: result
    });
  } catch (error) {
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
