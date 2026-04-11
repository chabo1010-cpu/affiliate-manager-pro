import { Router } from 'express';
const router = Router();

const logs = [
  { id: 1, user: 'Lena Müller', action: 'Post gespeichert', time: 'vor 3m', status: 'erfolgreich' },
  { id: 2, user: 'Jan Richter', action: 'Template aktualisiert', time: 'vor 12m', status: 'erfolgreich' },
  { id: 3, user: 'Sofie Rehm', action: 'Scraper-Check', time: 'vor 42m', status: 'pending' }
];

router.get('/', (req, res) => {
  res.json(logs);
});

export default router;
