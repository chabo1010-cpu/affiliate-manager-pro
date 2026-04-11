import { Router } from 'express';
const router = Router();

const bot = {
  status: 'bereit',
  queue: 5,
  lastCheck: 'Gerade eben',
  activities: [
    { id: 1, action: 'Vorschlag geprüft', user: 'Lena Müller', time: '2m' },
    { id: 2, action: 'Post freigegeben', user: 'Tobias Klein', time: '14m' }
  ]
};

router.get('/', (req, res) => {
  res.json(bot);
});

export default router;
