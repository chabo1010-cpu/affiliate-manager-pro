import { Router } from 'express';
import { listAdvertisingHistory } from '../services/advertisingService.js';
import { listCopybotLogs } from '../services/copybotService.js';
import { listPublishingLogs } from '../services/publisherService.js';

const router = Router();

router.get('/', (req, res) => {
  const publishingLogs = listPublishingLogs().slice(0, 40).map((item) => ({
    id: `publishing-${item.id}`,
    scope: 'Publishing',
    title: item.event_type || 'Publishing Event',
    detail: item.message || '',
    status: item.level || 'info',
    createdAt: item.created_at
  }));
  const copybotLogs = listCopybotLogs().slice(0, 40).map((item) => ({
    id: `copybot-${item.id}`,
    scope: 'Quellen',
    title: item.event_type || 'Copybot Event',
    detail: item.message || '',
    status: item.level || 'info',
    createdAt: item.created_at
  }));
  const advertisingLogs = listAdvertisingHistory(40).items.map((item) => ({
    id: `advertising-${item.id}`,
    scope: 'Werbung',
    title: item.moduleName || 'Werbemodul',
    detail: `${item.status} | Queue ${item.queueId ?? '-'}${item.lastError ? ` | ${item.lastError}` : ''}`,
    status: item.status || 'info',
    createdAt: item.updatedAt || item.createdAt
  }));

  const items = [...publishingLogs, ...copybotLogs, ...advertisingLogs]
    .sort((left, right) => new Date(right.createdAt || 0).getTime() - new Date(left.createdAt || 0).getTime())
    .slice(0, 120);

  res.json({ items });
});

export default router;
