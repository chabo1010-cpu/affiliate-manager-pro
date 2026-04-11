import { Router } from 'express';
const router = Router();

const users = [
  { id: 1, username: 'admin', role: 'admin' },
  { id: 2, username: 'editor', role: 'editor' },
  { id: 3, username: 'poster', role: 'poster' },
  { id: 4, username: 'viewer', role: 'viewer' }
];

router.post('/login', (req, res) => {
  const { username } = req.body;
  const user = users.find((u) => u.username === username);
  if (!user) {
    return res.status(401).json({ message: 'Ungültiger Benutzername' });
  }
  res.json({ user, token: 'mock-token' });
});

export default router;
