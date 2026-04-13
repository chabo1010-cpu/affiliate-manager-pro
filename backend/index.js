import express from 'express';
import cors from 'cors';
import { getApiPort } from './env.js';
import authRoutes from './routes/auth.js';
import usersRoutes from './routes/users.js';
import dealsRoutes from './routes/deals.js';
import postsRoutes from './routes/posts.js';
import botRoutes from './routes/bot.js';
import logsRoutes from './routes/logs.js';
import amazonRoutes from './routes/amazon.js';
import telegramRoutes from './routes/telegram.js';

const app = express();
const port = getApiPort();

app.use(cors());
app.use(express.json());

app.use('/api/auth', authRoutes);
app.use('/api/users', usersRoutes);
console.log('Registering deals routes on /api/deals');
app.use('/api/deals', dealsRoutes);
app.use('/api/posts', postsRoutes);
app.use('/api/bot', botRoutes);
app.use('/api/logs', logsRoutes);
app.use('/api', amazonRoutes);
app.use('/api/amazon', amazonRoutes);
app.use('/api/telegram', telegramRoutes);

app.get('/', (req, res) => {
  res.json({ status: 'Affiliate Manager Pro API läuft', version: '0.1.0' });
});

app.listen(port, () => {
  console.log(`Affiliate Manager Pro Backend läuft auf http://localhost:${port}`);
});
