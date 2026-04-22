import { Router } from 'express';
import { CENTRAL_DATABASE_COLLECTIONS, getCentralDatabaseStructure } from '../services/databaseService.js';

const router = Router();

function getRequesterRole(req) {
  return String(req.headers['x-user-role'] || '').trim().toLowerCase();
}

function requireAdmin(req, res, next) {
  if (getRequesterRole(req) !== 'admin') {
    return res.status(403).json({ error: 'Nur Admin darf das Datenmodul einsehen.' });
  }

  return next();
}

router.get('/structure', requireAdmin, (req, res) => {
  res.json({
    collections: CENTRAL_DATABASE_COLLECTIONS,
    structure: getCentralDatabaseStructure()
  });
});

export default router;
