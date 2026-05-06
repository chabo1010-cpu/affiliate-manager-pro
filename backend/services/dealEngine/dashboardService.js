import { getCopybotOverview, listSources } from '../copybotService.js';
import { getPublishingQueueCounts, listPublishingQueue } from '../publisherService.js';
import { getWorkerStatus } from '../publisherService.js';
import { getDealEngineSettings } from './configService.js';
import { getDealEngineMetrics, listDealEngineRuns } from './repositoryService.js';
import { getDealEngineOutputSnapshot } from './publisherService.js';

function getTone(status) {
  const normalized = String(status || '').toLowerCase();
  if (normalized.includes('error') || normalized.includes('reject') || normalized.includes('failed')) {
    return 'danger';
  }

  if (normalized.includes('queue') || normalized.includes('warning') || normalized.includes('pending')) {
    return 'warning';
  }

  if (normalized.includes('approve') || normalized.includes('ready') || normalized.includes('active')) {
    return 'success';
  }

  return 'info';
}

export function getDealEngineDashboard() {
  const settings = getDealEngineSettings();
  const metrics = getDealEngineMetrics();
  const runs = listDealEngineRuns({ limit: 12 }).items;
  const sourceItems = listSources();
  const copybotOverview = getCopybotOverview();
  const workerStatus = getWorkerStatus();
  const outputSnapshot = getDealEngineOutputSnapshot(settings);
  const queues = listPublishingQueue({ sourceType: 'deal_engine', limit: 10 });
  const queueCounts = getPublishingQueueCounts({ sourceType: 'deal_engine' });
  const activeSources = sourceItems.filter((item) => Number(item.is_active) === 1);
  const activeTelegramSources = activeSources.filter((item) => String(item.platform || '').toLowerCase() === 'telegram');
  const activeWhatsappSources = activeSources.filter((item) => String(item.platform || '').toLowerCase() === 'whatsapp');
  const openDealEngineQueues = queueCounts.openCount;
  const errors = runs
    .filter((item) => item.decision === 'REJECT' || String(item.outputStatus || '').includes('without_active_output'))
    .slice(0, 8)
    .map((item) => ({
      id: `run-${item.id}`,
      title: item.title || item.asin || `Run ${item.id}`,
      detail: item.decisionReason || item.outputStatus || 'Ohne Detailtext',
      tone: item.decision === 'REJECT' ? 'danger' : 'warning',
      createdAt: item.createdAt
    }));
  const systemStatus =
    errors.length > 0
      ? 'attention_required'
      : openDealEngineQueues > 0
        ? 'active'
        : metrics.totalRuns > 0
          ? 'ready'
          : 'configured';

  return {
    feasibility: {
      status: 'umsetzbar',
      detail:
        'Bestehende Quellen, Queue, Telegram, WhatsApp und Dashboard sind vorhanden. Die neue Deal-Engine haengt sich modular an diese Struktur an.'
    },
    systemStatus: {
      label: systemStatus,
      tone: getTone(systemStatus),
      detail: `${metrics.totalRuns} Runs | ${metrics.approvedRuns} approve | ${metrics.queuedRuns} queue | ${metrics.rejectedRuns} reject`
    },
    settings,
    metrics,
    sources: {
      activeCount: activeSources.length,
      telegramCount: activeTelegramSources.length,
      whatsappCount: activeWhatsappSources.length,
      items: activeSources.slice(0, 12),
      overview: copybotOverview
    },
    outputs: {
      snapshot: outputSnapshot,
      workers: workerStatus,
      openQueueCount: openDealEngineQueues,
      latestQueues: queues
    },
    operatingPrinciples: [
      {
        id: 'internet',
        label: 'Internet zuerst',
        detail: 'Der guenstigste echte Marktpreis bleibt die Hauptentscheidung, sobald die Seller-Regeln Marktvergleich erlauben.'
      },
      {
        id: 'keepa',
        label: 'Keepa nur Fallback',
        detail: 'Keepa greift nur, wenn kein brauchbarer Marktpreis vorhanden ist.'
      },
      {
        id: 'ai',
        label: 'KI nur Hilfe',
        detail: settings.ai.resolverEnabled
          ? 'AI Resolver ist optional aktiv und greift nur bei Unsicherheit.'
          : 'AI Resolver ist vorbereitet, aber deaktiviert. Die Engine bleibt ohne KI voll lauffaehig.'
      }
    ],
    runtimeStatus: {
      internetDecision: {
        label: 'produktiv',
        tone: 'success',
        detail: 'Marktvergleich mit billigstem echten Marktpreis ist live und wird ueber Seller-Regeln gegated.'
      },
      keepaFallback: {
        label: 'produktiv',
        tone: 'info',
        detail: 'Keepa bleibt ausschliesslich Fallback.'
      },
      fakePattern: {
        label: 'produktiv',
        tone: 'success',
        detail: 'Fake-Pattern sitzen verbindlich im Hauptpfad.'
      },
      aiResolver: {
        label: settings.ai.resolverEnabled ? 'optional aktiv' : 'deaktiviert',
        tone: settings.ai.resolverEnabled ? 'info' : 'warning',
        detail: 'KI kommt erst nach Marktvergleich, Keepa und Fake-Pattern bei Unsicherheit ins Spiel.'
      }
    },
    liveFlow: [
      {
        id: 'amazon',
        label: 'Amazon Link',
        title: runs[0]?.amazonUrl ? 'vorhanden' : 'wartet auf Eingang',
        detail: 'Ohne Amazon-Link wird immer REJECT ausgeliefert.',
        tone: runs[0]?.amazonUrl ? 'success' : 'info'
      },
      {
        id: 'market',
        label: 'Internetvergleich',
        title: `${metrics.marketRuns} Marktentscheidungen`,
        detail: 'Internetvergleich bleibt die primaere Entscheidungsstufe, wenn die Seller-Regeln ihn freigeben.',
        tone: 'success'
      },
      {
        id: 'keepa',
        label: 'Keepa Fallback',
        title: `${metrics.keepaFallbackRuns} Fallbacks`,
        detail: 'Keepa wird nur genutzt, wenn kein brauchbarer Marktpreis vorhanden ist.',
        tone: metrics.keepaFallbackRuns > 0 ? 'info' : 'warning'
      },
      {
        id: 'fake-pattern',
        label: 'Fake-Pattern',
        title: `${metrics.rejectedRuns} kritische Pruefungen`,
        detail: 'Verlauf, Spikes, Coupon-Effekte und Sparse-History fliessen vor der finalen Entscheidung ein.',
        tone: 'success'
      },
      {
        id: 'ai',
        label: 'AI Resolver',
        title: `${metrics.aiResolvedRuns} Unsicherheitsfaelle`,
        detail: settings.ai.resolverEnabled
          ? 'AI Resolver ist optional aktiv und kommt nur bei unklaren Faellen nach Markt, Keepa und Fake-Pattern hinzu.'
          : 'AI Resolver ist deaktiviert. Die Engine bleibt ohne KI voll entscheidungsfaehig.',
        tone: settings.ai.resolverEnabled ? 'info' : 'warning'
      },
      {
        id: 'decision',
        label: 'Finale Entscheidung',
        title: `${metrics.approvedRuns} approve | ${metrics.queuedRuns} queue | ${metrics.rejectedRuns} reject`,
        detail: 'Erst danach werden APPROVE, QUEUE oder REJECT verbindlich gesetzt.',
        tone: metrics.totalRuns > 0 ? 'success' : 'info'
      },
      {
        id: 'output',
        label: 'Output',
        title: `${outputSnapshot.latestQueues.length} Queue-Eintraege`,
        detail: 'Nur APPROVED Deals gehen an Queue, Publisher, Telegram und optional WhatsApp weiter.',
        tone: outputSnapshot.latestQueues.length ? 'success' : 'info'
      }
    ],
    timeline: runs.map((item) => ({
      id: item.id,
      title: item.title || item.asin || `Run ${item.id}`,
      detail: `${item.decision} | ${item.sellerArea} | ${item.dayPart} | ${item.analysis?.decisionSourceLabel || 'Decision Engine'}`,
      tone: getTone(item.decision),
      createdAt: item.createdAt
    })),
    errors,
    runs
  };
}
