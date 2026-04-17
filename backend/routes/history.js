import { Router } from 'express';
import { listDealsHistory } from '../services/dealHistoryService.js';

const router = Router();

router.get('/', (req, res) => {
  try {
    return res.json({
      success: true,
      items: listDealsHistory({
        sellerType: req.query?.sellerType || req.query?.marketplaceType,
        startDate: req.query?.startDate,
        endDate: req.query?.endDate,
        asin: req.query?.asin,
        url: req.query?.url,
        title: req.query?.title
      })
    });
  } catch (error) {
    return res.status(500).json({
      error: error instanceof Error ? error.message : 'Failed to fetch history'
    });
  }
});

export default router;
