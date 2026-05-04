import { Router } from 'express';
import { getTelegramTestGroupConfig } from '../env.js';
import { getOptimizedDealsChannelStatus, sendOptimizedDealsChannelTestMessage } from '../services/telegramUserClientService.js';
import { sendTelegramPost } from '../services/telegramSenderService.js';

const router = Router();

function getRequesterRole(req) {
  return String(req.headers['x-user-role'] || '').trim().toLowerCase();
}

function requireAdmin(req, res, next) {
  if (getRequesterRole(req) !== 'admin') {
    return res.status(403).json({ error: 'Nur Admin darf den Telegram-Testversand ausloesen.' });
  }

  return next();
}

router.post('/test-telegram-send', requireAdmin, async (req, res) => {
  try {
    const testGroupConfig = getTelegramTestGroupConfig();
    const timestamp = new Date().toISOString();
    const text =
      typeof req.body?.text === 'string' && req.body.text.trim()
        ? req.body.text.trim()
        : `TEST POST ${timestamp}`;
    console.info('[TEST_SEND_START]', {
      chatId: testGroupConfig.chatId || null,
      textLength: text.length,
      textPreview: text.slice(0, 160)
    });
    const result = await sendTelegramPost({
      text,
      chatId: testGroupConfig.chatId,
      disableWebPagePreview: true
    });

    console.info('[TEST_SEND_SUCCESS]', {
      chatId: result.chatId,
      messageId: result.messageId,
      method: result.method
    });

    return res.json({
      success: true,
      chatId: result.chatId,
      messageId: result.messageId,
      method: result.method
    });
  } catch (error) {
    console.error('[TEST_SEND_ERROR]', {
      error: error instanceof Error ? error.message : 'Telegram-Testversand fehlgeschlagen.'
    });
    console.error('[ERROR_REASON]', {
      reason: error instanceof Error ? error.message : 'Telegram-Testversand fehlgeschlagen.'
    });
    return res.status(400).json({
      success: false,
      error: error instanceof Error ? error.message : 'Telegram-Testversand fehlgeschlagen.'
    });
  }
});

router.get('/product-intelligence/optimized-channel-status', requireAdmin, async (req, res) => {
  const status = await getOptimizedDealsChannelStatus({ resolve: false });

  return res.json({
    success: true,
    ...status
  });
});

router.post('/product-intelligence/test-optimized-channel', requireAdmin, async (req, res) => {
  const result = await sendOptimizedDealsChannelTestMessage();

  if (result.success !== true) {
    return res.status(400).json({
      success: false,
      error: result.reason || 'Optimierte Deals Test fehlgeschlagen.',
      reason: result.reason || 'optimized_channel_test_failed',
      status: result.status
    });
  }

  return res.json(result);
});

export default router;
