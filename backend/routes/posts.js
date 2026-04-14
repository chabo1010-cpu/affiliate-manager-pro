import { Router } from 'express';
import { publishGeneratorPostDirect } from '../services/directPublisher.js';
const router = Router();

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

router.post('/direct', async (req, res) => {
  try {
    const result = await publishGeneratorPostDirect(req.body ?? {});
    return res.status(200).json(result);
  } catch (error) {
    return res.status(500).json({
      success: false,
      error: error instanceof Error ? error.message : 'Direkt-Posting fehlgeschlagen'
    });
  }
});

export default router;
