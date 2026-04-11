import { Router } from 'express';
const router = Router();

const team = [
  { id: 1, name: 'Lena Müller', role: 'admin', status: 'aktiv' },
  { id: 2, name: 'Tobias Klein', role: 'editor', status: 'aktiv' },
  { id: 3, name: 'Sofie Rehm', role: 'poster', status: 'pausiert' },
  { id: 4, name: 'Jan Richter', role: 'viewer', status: 'aktiv' }
];

router.get('/', (req, res) => {
  res.json(team);
});

export default router;
