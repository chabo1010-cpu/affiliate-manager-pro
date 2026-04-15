import { Router } from 'express';
import multer from 'multer';
import { publishGeneratorPostDirect } from '../services/directPublisher.js';
const router = Router();
const upload = multer({ storage: multer.memoryStorage() });

function parseBoolean(value, fallback = false) {
  if (value === undefined) {
    return fallback;
  }

  return value === true || value === 'true' || value === 1 || value === '1';
}

function parseJsonObject(value, fallback = {}) {
  if (!value) {
    return fallback;
  }

  if (typeof value === 'object') {
    return value;
  }

  if (typeof value !== 'string') {
    return fallback;
  }

  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === 'object' ? parsed : fallback;
  } catch {
    return fallback;
  }
}

function normalizeDirectPublishInput(req) {
  const body = req.body ?? {};

  const normalizedInput = {
    title: body.title || '',
    link: body.link || '',
    normalizedUrl: body.normalizedUrl || '',
    asin: body.asin || '',
    sellerType: body.sellerType || 'FBM',
    currentPrice: body.currentPrice || '',
    oldPrice: body.oldPrice || '',
    couponCode: body.couponCode || '',
    textByChannel: parseJsonObject(body.textByChannel, {}),
    generatedImagePath: body.generatedImagePath || '',
    uploadedImagePath: req.file?.originalname || body.uploadedImagePath || '',
    uploadedImageFile: req.file || null,
    telegramImageSource: body.telegramImageSource || 'standard',
    whatsappImageSource: body.whatsappImageSource || 'standard',
    facebookImageSource: body.facebookImageSource || 'link_preview',
    enableTelegram: parseBoolean(body.enableTelegram, true),
    enableWhatsapp: parseBoolean(body.enableWhatsapp, false),
    enableFacebook: parseBoolean(body.enableFacebook, false)
  };

  return normalizedInput;
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
  try {
    const result = await publishGeneratorPostDirect(normalizeDirectPublishInput(req));
    return res.status(200).json(result);
  } catch (error) {
    return res.status(500).json({
      success: false,
      error: error instanceof Error ? error.message : 'Direkt-Posting fehlgeschlagen'
    });
  }
});

export default router;
