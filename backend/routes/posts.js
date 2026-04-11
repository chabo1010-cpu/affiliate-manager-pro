import { Router } from 'express';
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

export default router;
