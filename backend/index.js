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
import copybotRoutes from './routes/copybot.js';
import publishingRoutes from './routes/publishing.js';
import settingsRoutes from './routes/settings.js';
import historyRoutes from './routes/history.js';

const app = express();
const port = getApiPort();

app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ limit: '10mb', extended: true }));
app.use((req, res, next) => {
  console.log('API CALL:', req.method, req.url);
  next();
});

app.use('/api/auth', authRoutes);
app.use('/api/users', usersRoutes);
console.log('Registering deals routes on /api/deals');
app.use('/api/deals', dealsRoutes);
app.use('/api/settings', settingsRoutes);
app.use('/api/history', historyRoutes);
app.use('/api/posts', postsRoutes);
app.use('/api/bot', botRoutes);
app.use('/api/logs', logsRoutes);
app.use('/api', amazonRoutes);
app.use('/api/amazon', amazonRoutes);
app.use('/api/telegram', telegramRoutes);
app.use('/api/copybot', copybotRoutes);
app.use('/api/publishing', publishingRoutes);

app.get('/', (req, res) => {
  res.json({ status: 'Affiliate Manager Pro API läuft', version: '0.1.0' });
});

app.listen(port, () => {
  console.log(`Affiliate Manager Pro Backend läuft auf http://localhost:${port}`);
});
