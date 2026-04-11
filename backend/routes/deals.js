import { Router } from 'express';
const router = Router();

const deals = [
  { id: 1, title: 'Aktionsangebot', price: '29,99€', status: 'live' },
  { id: 2, title: 'Coupon-Rabatt', price: '17,49€', status: 'geprüft' },
  { id: 3, title: 'Prime Blitz', price: '24,90€', status: 'entwurf' }
];

router.get('/', (req, res) => {
  res.json(deals);
});

export default router;
