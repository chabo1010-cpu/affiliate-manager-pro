import { Router } from 'express';
import multer from 'multer';
import { publishGeneratorPostDirect } from '../services/directPublisher.js';
import { isAmazonShortLink } from '../services/dealHistoryService.js';
import {
  buildGeneratorDebugPayload,
  getGeneratorValidationError,
  logGeneratorDebug,
  normalizeGeneratorInput
} from '../services/generatorFlowService.js';
const router = Router();
const upload = multer({ storage: multer.memoryStorage() });

function normalizeDirectPublishInput(req) {
  return normalizeGeneratorInput({
    ...(req.body ?? {}),
    uploadedImagePath: req.file?.originalname || req.body?.uploadedImagePath || '',
    uploadedImageFile: req.file || null
  });
}

const posts = [
  { id: 1, platform: 'Telegram', status: 'bereit', caption: `🔸 Partnerlink
🔥 Jetzt 24,90€
Anzeige / Partnerlink`, createdBy: 'Sofie Rehm' },
  { id: 2, platform: 'WhatsApp', status: 'gespeichert', caption: `💸 Vorher 39,99€
🔥 Jetzt 29,99€
Anzeige / Partnerlink`, createdBy: 'Tobias Klein' }
];

router.get('/', (req, res) => {
  res.json(posts);
});

router.post('/', (req, res) => {
  const data = { id: Date.now(), ...req.body };
  posts.unshift(data);
  res.status(201).json(data);
});

router.post('/direct', upload.single('uploadedImageFile'), async (req, res) => {
  const normalizedInput = normalizeDirectPublishInput(req);
  const debugPayload = buildGeneratorDebugPayload(normalizedInput);
  logGeneratorDebug('api.posts.direct.request', debugPayload);
  if (isAmazonShortLink(normalizedInput.link)) {
    console.info('[MANUAL_SHORTLINK_ALLOWED]', {
      link: normalizedInput.link,
      normalizedUrl: normalizedInput.normalizedUrl || null
    });
  }

  const validationError = getGeneratorValidationError(normalizedInput, { mode: 'direct' });
  if (validationError) {
    logGeneratorDebug('api.posts.direct.rejected', {
      error: validationError,
      ...debugPayload
    });
    return res.status(400).json({
      success: false,
      error: validationError
    });
  }

  try {
    const result = await publishGeneratorPostDirect(normalizedInput);
    logGeneratorDebug('api.posts.direct.success', {
      result,
      ...debugPayload
    });
    return res.status(200).json(result);
  } catch (error) {
    logGeneratorDebug('api.posts.direct.error', {
      error: error instanceof Error ? error.message : 'Direkt-Posting fehlgeschlagen',
      ...debugPayload
    });
    const statusCode =
      error instanceof Error && typeof error.code === 'string'
        ? error.code.startsWith('DEAL_LOCK_')
          ? 409
          : ['NO_PUBLISH_TARGETS_SELECTED', 'NO_TELEGRAM_PUBLISH_TARGET'].includes(error.code)
            ? 400
          : error.code === 'PUBLISHING_QUEUE_FAILED'
            ? 502
            : 500
        : 500;
    return res.status(statusCode).json({
      success: false,
      error: error instanceof Error ? error.message : 'Direkt-Posting fehlgeschlagen',
      dealLock: error instanceof Error && error.dealLock ? error.dealLock : null,
      queue: error instanceof Error && error.queue ? error.queue : null
    });
  }
});

export default router;
