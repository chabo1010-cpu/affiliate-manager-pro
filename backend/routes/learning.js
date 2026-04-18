import { Router } from 'express';
import { getCopybotOverview } from '../services/copybotService.js';
import { logGeneratorDebug } from '../services/generatorFlowService.js';
import { getAmazonAffiliateStatus } from '../services/amazonAffiliateService.js';
import { getKeepaStatus } from '../services/keepaService.js';
import { getFakeDropSummary } from '../services/keepaFakeDropService.js';
import { getLearningLogicOverview } from '../services/learningLogicService.js';
import { getWorkerStatus } from '../services/publisherService.js';

const router = Router();

router.get('/overview', async (req, res) => {
  try {
    const [keepaStatus, amazonStatus, fakeDropSummary, copybotOverview, publishingStatus] = await Promise.all([
      getKeepaStatus(),
      Promise.resolve(getAmazonAffiliateStatus()),
      Promise.resolve(getFakeDropSummary()),
      Promise.resolve(getCopybotOverview()),
      Promise.resolve(getWorkerStatus())
    ]);
    const learningOverview = getLearningLogicOverview();
    const drawerConfigs = keepaStatus?.settings?.drawerConfigs || {};
    const sellerControls = (learningOverview?.sellerTypes || []).map((item) => {
      const drawerConfig = drawerConfigs[item.id] || {};
      const autoPostingEnabled = drawerConfig.testGroupPostingAllowed === true && keepaStatus?.settings?.alertTelegramEnabled === true;

      return {
        id: item.id,
        active: drawerConfig.active === true,
        patternSupportEnabled: drawerConfig.patternSupportEnabled === true,
        autoModeAllowed: drawerConfig.autoModeAllowed === true,
        autoPostingEnabled,
        rulesActive: item.minDiscount > 0 && item.minScore > 0,
        lastDecision: item.activity?.lastDecision || 'noch_keine',
        lastDecisionDetail: item.activity?.lastDecisionDetail || '',
        lastRunAt: item.activity?.lastRunAt || null,
        lastAsin: item.activity?.lastAsin || '',
        lastStrength: item.activity?.lastStrength || ''
      };
    });
    const sourceStatuses = [
      {
        id: 'keepa',
        status: keepaStatus?.connection?.connected === true ? 'aktiv' : keepaStatus?.settings?.keepaKeyStatus?.connected ? 'fehler' : 'vorbereitet',
        connected: keepaStatus?.connection?.connected === true
      },
      {
        id: 'amazon_api',
        status: amazonStatus?.overview?.apiStatus || 'vorbereitet',
        connected: amazonStatus?.connection?.connected === true
      },
      {
        id: 'scrapper',
        status: copybotOverview?.copybotEnabled ? 'aktiv' : 'deaktiviert',
        connected: copybotOverview?.copybotEnabled === true
      },
      {
        id: 'learning_logic',
        status: 'aktiv',
        connected: true
      },
      {
        id: 'telegram_output',
        status: keepaStatus?.settings?.alertTelegramEnabled ? 'aktiv' : 'deaktiviert',
        connected: keepaStatus?.settings?.alertTelegramEnabled === true
      }
    ];
    const outputStatuses = {
      reviewCount: Number(fakeDropSummary?.kpis?.openReviewCount || 0),
      telegramChannels: Array.isArray(publishingStatus?.channels) ? publishingStatus.channels : [],
      facebookWorkerEnabled: publishingStatus?.facebook?.enabled === true
    };

    logGeneratorDebug('SOURCE STATUS UPDATED', {
      keepa: sourceStatuses[0].status,
      amazon: sourceStatuses[1].status,
      scrapper: sourceStatuses[2].status,
      telegram: sourceStatuses[4].status
    });
    logGeneratorDebug('FLOW STATUS UPDATED', {
      reviewCount: outputStatuses.reviewCount,
      generator: learningOverview?.pipeline?.find((item) => item.id === 'generator')?.integrationMode || '',
      scrapper: learningOverview?.pipeline?.find((item) => item.id === 'scrapper')?.integrationMode || '',
      autoDeals: learningOverview?.pipeline?.find((item) => item.id === 'auto_deals')?.integrationMode || ''
    });
    sellerControls.forEach((item) => {
      logGeneratorDebug(`PATTERN SUPPORT ACTIVE: ${item.id}`, {
        active: item.active,
        enabled: item.patternSupportEnabled
      });
      logGeneratorDebug(`AUTO POST ACTIVE: ${item.id}`, {
        active: item.active,
        enabled: item.autoPostingEnabled
      });
    });
    logGeneratorDebug('DASHBOARD STATE REFRESHED', {
      sellerControls: sellerControls.length,
      reviewCount: outputStatuses.reviewCount,
      publishingChannels: outputStatuses.telegramChannels.length
    });

    res.json({
      ...learningOverview,
      keepa: {
        connected: keepaStatus?.connection?.connected === true,
        keepaEnabled: keepaStatus?.settings?.keepaEnabled === true,
        maskedChatId: keepaStatus?.settings?.telegramConfigStatus?.maskedChatId || '',
        maskedKeepaKey: keepaStatus?.settings?.keepaKeyStatus?.masked || ''
      },
      amazon: {
        connected: amazonStatus?.connection?.connected === true,
        configured: amazonStatus?.settings?.configured === true,
        partnerTagMasked: amazonStatus?.settings?.partnerTagMasked || '',
        apiStatus: amazonStatus?.overview?.apiStatus || 'vorbereitet',
        lastSuccessfulFetch: amazonStatus?.overview?.lastSuccessfulFetch || null,
        lastErrorAt: amazonStatus?.overview?.lastErrorAt || null,
        lastErrorMessage: amazonStatus?.overview?.lastErrorMessage || '',
        deprecation: amazonStatus?.deprecation || null
      },
      copybot: copybotOverview,
      publishing: publishingStatus,
      sellerControls,
      sourceStatuses,
      outputStatuses,
      fakeDropSummary
    });
  } catch (error) {
    res.status(500).json({
      error: error instanceof Error ? error.message : 'Lern-Logik-Uebersicht konnte nicht geladen werden.'
    });
  }
});

export default router;
