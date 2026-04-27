import { Router } from 'express';

function getRequesterRole(req) {
  return String(req.headers['x-user-role'] || '').trim().toLowerCase();
}

export default function createSystemRoutes(options = {}) {
  const router = Router();
  const getHealthPayload =
    typeof options.getHealthPayload === 'function'
      ? options.getHealthPayload
      : () => ({
          ok: true
        });
  const getRestartStatus =
    typeof options.getRestartStatus === 'function'
      ? options.getRestartStatus
      : () => ({
          enabled: false,
          pending: false,
          manager: 'unavailable'
        });
  const scheduleBackendRestart =
    typeof options.scheduleBackendRestart === 'function'
      ? options.scheduleBackendRestart
      : () => ({
          accepted: false,
          reason: 'restart_scheduler_missing'
        });

  router.get('/health', (req, res) => {
    res.json(getHealthPayload());
  });

  router.post('/restart-backend', (req, res) => {
    const requesterRole = getRequesterRole(req);
    const requestedAt = new Date().toISOString();

    console.info('[RESTART_REQUEST_RECEIVED]', {
      requestedAt,
      requesterRole: requesterRole || 'unknown'
    });

    if (requesterRole !== 'admin') {
      return res.status(403).json({ error: 'Nur Admin darf das Backend neu starten.' });
    }

    console.info('[RESTART_AUTHORIZED]', {
      requestedAt,
      requesterRole
    });

    const restartStatus = getRestartStatus();

    if (restartStatus.enabled !== true) {
      return res.status(409).json({
        error: 'Backend-Neustart ist nur verfuegbar, wenn das Backend ueber nodemon laeuft.',
        restartManager: restartStatus.manager || 'unavailable'
      });
    }

    if (restartStatus.pending === true) {
      return res.status(202).json({
        ok: true,
        restarting: true,
        alreadyPending: true,
        restartManager: restartStatus.manager || 'nodemon'
      });
    }

    const restartResult = scheduleBackendRestart({
      requestedAt,
      requesterRole,
      source: 'system_restart_endpoint'
    });

    if (restartResult?.accepted !== true) {
      return res.status(409).json({
        error: 'Backend-Neustart konnte nicht vorbereitet werden.',
        reason: restartResult?.reason || 'restart_not_accepted'
      });
    }

    return res.status(202).json({
      ok: true,
      restarting: true,
      restartManager: restartResult.manager || restartStatus.manager || 'nodemon',
      reloadAfterMs: restartResult.reloadAfterMs || 3000
    });
  });

  return router;
}
