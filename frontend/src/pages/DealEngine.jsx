import { useEffect, useMemo, useState } from 'react';
import Layout from '../components/layout/Layout';
import { useAuth } from '../context/AuthContext';
import './DealEngine.css';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:4000';

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function formatDateTime(value) {
  if (!value) {
    return '-';
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return new Intl.DateTimeFormat('de-DE', {
    dateStyle: 'short',
    timeStyle: 'short'
  }).format(date);
}

function prettyJson(value) {
  return JSON.stringify(value ?? null, null, 2);
}

function toSellerFlagValue(value) {
  if (value === true) {
    return 'yes';
  }

  if (value === false) {
    return 'no';
  }

  return 'unknown';
}

function fromSellerFlagValue(value) {
  if (value === 'yes') {
    return true;
  }

  if (value === 'no') {
    return false;
  }

  return null;
}

function buildFormFromSample(sample = {}) {
  return {
    sourceName: sample.source?.name || 'Demo Quelle',
    sourcePlatform: sample.source?.platform || 'telegram',
    sourceType: sample.source?.type || 'manual',
    title: sample.deal?.title || '',
    amazonUrl: sample.deal?.amazonUrl || '',
    amazonPrice: String(sample.deal?.amazonPrice ?? ''),
    sellerType: sample.deal?.sellerType || 'AMAZON',
    sellerClass: sample.deal?.sellerClass || 'AMAZON_DIRECT',
    soldByAmazon: toSellerFlagValue(sample.deal?.soldByAmazon ?? true),
    shippedByAmazon: toSellerFlagValue(sample.deal?.shippedByAmazon ?? true),
    brand: sample.deal?.brand || '',
    category: sample.deal?.category || '',
    variantKey: sample.deal?.variantKey || '',
    quantityKey: sample.deal?.quantityKey || '',
    isBrandProduct: sample.deal?.isBrandProduct === true,
    isNoName: sample.deal?.isNoName === true,
    isChinaProduct: sample.deal?.isChinaProduct === true,
    overrideDayPart: sample.meta?.overrideDayPart || 'day',
    marketOffersJson: prettyJson(sample.market?.offers || []),
    keepaJson: prettyJson(sample.keepa || {}),
    aiJson: prettyJson(sample.ai || {})
  };
}

function buildAnalyzePayload(form) {
  const marketOffers = JSON.parse(form.marketOffersJson || '[]');
  const keepa = JSON.parse(form.keepaJson || '{}');
  const ai = JSON.parse(form.aiJson || '{}');

  if (!Array.isArray(marketOffers)) {
    throw new Error('Marktangebote muessen als JSON-Array angegeben werden.');
  }

  return {
    source: {
      name: form.sourceName,
      platform: form.sourcePlatform,
      type: form.sourceType
    },
    deal: {
      title: form.title,
      amazonUrl: form.amazonUrl,
      amazonPrice: form.amazonPrice,
      sellerType: form.sellerType,
      sellerClass: form.sellerClass,
      soldByAmazon: fromSellerFlagValue(form.soldByAmazon),
      shippedByAmazon: fromSellerFlagValue(form.shippedByAmazon),
      brand: form.brand,
      category: form.category,
      variantKey: form.variantKey,
      quantityKey: form.quantityKey,
      isBrandProduct: form.isBrandProduct,
      isNoName: form.isNoName,
      isChinaProduct: form.isChinaProduct
    },
    market: {
      offers: marketOffers
    },
    keepa,
    ai,
    meta: {
      overrideDayPart: form.overrideDayPart
    }
  };
}

function getToneClass(value) {
  const normalized = String(value || '').toLowerCase();
  if (normalized.includes('reject') || normalized.includes('danger') || normalized.includes('failed') || normalized.includes('attention')) {
    return 'danger';
  }

  if (normalized.includes('queue') || normalized.includes('warning') || normalized.includes('pending')) {
    return 'warning';
  }

  if (normalized.includes('approve') || normalized.includes('success') || normalized.includes('ready') || normalized.includes('active')) {
    return 'success';
  }

  return 'info';
}

const DEAL_ENGINE_DEFAULTS = {
  amazon: {
    dayMinMarketAdvantagePct: 15,
    nightMinMarketAdvantagePct: 25
  },
  fbm: {
    dayMinMarketAdvantagePct: 20,
    nightMinMarketAdvantagePct: 30
  },
  global: {
    keepaApproveScore: 70,
    keepaQueueScore: 50,
    queueMarginPct: 3,
    queueEnabled: true,
    nightModeEnabled: true,
    cheapProductLimit: 20,
    requireMarketForCheapProducts: true,
    requireMarketForNoNameProducts: true
  },
  output: {
    telegramEnabled: true,
    whatsappEnabled: true
  },
  ai: {
    resolverEnabled: false,
    amazonDirectEnabled: true,
    onlyOnUncertainty: true,
    alwaysInDebug: true
  },
  quality: {
    marketCompareAmazonDirectEnabled: true,
    marketCompareAmazonDirectOnly: true,
    aiAmazonDirectOnly: true,
    allowFbaThirdPartyMarketCompare: false,
    allowFbaThirdPartyAi: false,
    allowFbmMarketCompare: false,
    allowFbmAi: false,
    unknownSellerMode: 'review'
  }
};

function mergeDealEngineSettings(current, patch) {
  if (!current) {
    return current;
  }

  return {
    ...current,
    amazon: {
      ...current.amazon,
      ...(patch.amazon || {})
    },
    fbm: {
      ...current.fbm,
      ...(patch.fbm || {})
    },
    global: {
      ...current.global,
      ...(patch.global || {})
    },
    output: {
      ...current.output,
      ...(patch.output || {})
    },
    ai: {
      ...current.ai,
      ...(patch.ai || {})
    },
    quality: {
      ...current.quality,
      ...(patch.quality || {})
    }
  };
}

function applyQualityPreset(current, preset) {
  const patches = {
    locker: {
      global: {
        keepaApproveScore: 60,
        keepaQueueScore: 45,
        queueMarginPct: 6
      },
      ai: {
        resolverEnabled: true
      },
      quality: {
        marketCompareAmazonDirectOnly: false,
        aiAmazonDirectOnly: false,
        allowFbaThirdPartyMarketCompare: true,
        allowFbaThirdPartyAi: true,
        allowFbmMarketCompare: true,
        allowFbmAi: true,
        unknownSellerMode: 'review'
      }
    },
    normal: DEAL_ENGINE_DEFAULTS,
    streng: {
      global: {
        keepaApproveScore: 75,
        keepaQueueScore: 55,
        queueMarginPct: 2.5,
        cheapProductLimit: 25,
        requireMarketForCheapProducts: true,
        requireMarketForNoNameProducts: true
      },
      quality: {
        marketCompareAmazonDirectOnly: true,
        aiAmazonDirectOnly: true,
        allowFbaThirdPartyMarketCompare: false,
        allowFbaThirdPartyAi: false,
        allowFbmMarketCompare: false,
        allowFbmAi: false,
        unknownSellerMode: 'review'
      }
    },
    profi: {
      global: {
        keepaApproveScore: 80,
        keepaQueueScore: 60,
        queueMarginPct: 1.5,
        cheapProductLimit: 30,
        requireMarketForCheapProducts: true,
        requireMarketForNoNameProducts: true
      },
      ai: {
        resolverEnabled: true,
        onlyOnUncertainty: true,
        alwaysInDebug: false
      },
      quality: {
        marketCompareAmazonDirectEnabled: true,
        marketCompareAmazonDirectOnly: true,
        aiAmazonDirectOnly: true,
        allowFbaThirdPartyMarketCompare: false,
        allowFbaThirdPartyAi: false,
        allowFbmMarketCompare: false,
        allowFbmAi: false,
        unknownSellerMode: 'block'
      }
    }
  };

  return mergeDealEngineSettings(current, patches[preset] || DEAL_ENGINE_DEFAULTS);
}

function detectQualityPreset(settings) {
  if (!settings) {
    return 'normal';
  }

  if (settings.quality.unknownSellerMode === 'block' && settings.global.keepaApproveScore >= 80) {
    return 'profi';
  }

  if (settings.global.keepaApproveScore >= 75 || settings.global.queueMarginPct <= 2.5) {
    return 'streng';
  }

  if (settings.quality.allowFbmMarketCompare || settings.quality.allowFbaThirdPartyMarketCompare || settings.global.keepaApproveScore < 70) {
    return 'locker';
  }

  return 'normal';
}

function applyProductFilterPreset(current, preset) {
  const patches = {
    standard: {
      global: {
        cheapProductLimit: 20,
        requireMarketForCheapProducts: true,
        requireMarketForNoNameProducts: true
      }
    },
    streng: {
      global: {
        cheapProductLimit: 25,
        requireMarketForCheapProducts: true,
        requireMarketForNoNameProducts: true
      },
      quality: {
        allowFbaThirdPartyMarketCompare: false,
        allowFbmMarketCompare: false
      }
    },
    china_filter: {
      global: {
        cheapProductLimit: 30,
        requireMarketForCheapProducts: true,
        requireMarketForNoNameProducts: true
      },
      quality: {
        allowFbaThirdPartyMarketCompare: false,
        allowFbaThirdPartyAi: false,
        allowFbmMarketCompare: false,
        allowFbmAi: false
      }
    }
  };

  return mergeDealEngineSettings(current, patches[preset] || patches.standard);
}

function detectProductFilterPreset(settings) {
  if (!settings) {
    return 'standard';
  }

  if (settings.global.cheapProductLimit >= 30 && settings.quality.allowFbmAi === false && settings.quality.allowFbaThirdPartyAi === false) {
    return 'china_filter';
  }

  if (settings.global.cheapProductLimit >= 25) {
    return 'streng';
  }

  return 'standard';
}

function applyAutomationPreset(current, preset) {
  const patches = {
    testmodus: {
      output: {
        telegramEnabled: false,
        whatsappEnabled: false
      },
      ai: {
        alwaysInDebug: true
      }
    },
    review_sammeln: {
      output: {
        telegramEnabled: false,
        whatsappEnabled: false
      },
      global: {
        queueEnabled: true
      }
    },
    auto_posten: {
      output: {
        telegramEnabled: true,
        whatsappEnabled: true
      },
      global: {
        queueEnabled: true
      }
    }
  };

  return mergeDealEngineSettings(current, patches[preset] || patches.review_sammeln);
}

function detectAutomationPreset(settings) {
  if (!settings) {
    return 'review_sammeln';
  }

  if (settings.output.telegramEnabled && settings.output.whatsappEnabled) {
    return 'auto_posten';
  }

  if (!settings.output.telegramEnabled && settings.ai.alwaysInDebug) {
    return 'testmodus';
  }

  return 'review_sammeln';
}

function isWithinLastMinutes(value, minutes) {
  if (!value) {
    return false;
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return false;
  }

  return Date.now() - date.getTime() <= minutes * 60 * 1000;
}

function openInternalRoute(path) {
  window.location.assign(path);
}

function DealEnginePage() {
  const { user } = useAuth();
  const [dashboard, setDashboard] = useState(null);
  const [botDashboard, setBotDashboard] = useState(null);
  const [settings, setSettings] = useState(null);
  const [samplePayload, setSamplePayload] = useState(null);
  const [form, setForm] = useState(() => buildFormFromSample({}));
  const [result, setResult] = useState(null);
  const [status, setStatus] = useState('');
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [analyzing, setAnalyzing] = useState(false);
  const [expertMode, setExpertMode] = useState(false);

  useEffect(() => {
    let cancelled = false;

    async function apiFetch(path, options = {}) {
      const response = await fetch(`${API_BASE_URL}${path}`, {
        headers: {
          'Content-Type': 'application/json',
          'X-User-Role': user?.role || ''
        },
        ...options
      });
      const data = await response.json().catch(() => ({}));

      if (!response.ok) {
        throw new Error(data?.error || `Request fehlgeschlagen (${response.status}).`);
      }

      return data;
    }

    async function loadPage() {
      try {
        setLoading(true);
        setStatus('');

        const [dashboardData, botDashboardData, settingsData, sampleData] = await Promise.all([
          apiFetch('/api/deal-engine/dashboard'),
          apiFetch('/api/bot'),
          apiFetch('/api/deal-engine/settings'),
          apiFetch('/api/deal-engine/sample')
        ]);

        if (cancelled) {
          return;
        }

        setDashboard(dashboardData);
        setBotDashboard(botDashboardData);
        setSettings(settingsData.item);
        setSamplePayload(sampleData.item);
        setForm((current) => {
          const isStillDefault = !current.title && !current.amazonUrl && !current.amazonPrice;

          return isStillDefault ? buildFormFromSample(sampleData.item) : current;
        });
      } catch (error) {
        if (!cancelled) {
          setStatus(error instanceof Error ? error.message : 'Deal Engine konnte nicht geladen werden.');
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    void loadPage();

    return () => {
      cancelled = true;
    };
  }, [user?.role]);

  function updateSettings(path, value) {
    setSettings((current) => {
      if (!current) {
        return current;
      }

      return {
        ...current,
        [path]: {
          ...current[path],
          ...value
        }
      };
    });
  }

  function updateForm(field, value) {
    setForm((current) => ({
      ...current,
      [field]: value
    }));
  }

  async function refreshDashboard() {
    const headers = {
      'Content-Type': 'application/json',
      'X-User-Role': user?.role || ''
    };
    const [dashboardResponse, botResponse] = await Promise.all([
      fetch(`${API_BASE_URL}/api/deal-engine/dashboard`, { headers }),
      fetch(`${API_BASE_URL}/api/bot`, { headers })
    ]);
    const dashboardData = await dashboardResponse.json().catch(() => ({}));
    const botData = await botResponse.json().catch(() => ({}));
    if (!dashboardResponse.ok) {
      throw new Error(dashboardData?.error || 'Dashboard konnte nicht aktualisiert werden.');
    }
    if (!botResponse.ok) {
      throw new Error(botData?.error || 'Bot Dashboard konnte nicht aktualisiert werden.');
    }

    setDashboard(dashboardData);
    setBotDashboard(botData);
  }

  async function handleSaveSettings() {
    if (!settings) {
      return;
    }

    try {
      setSaving(true);
      setStatus('');
      const response = await fetch(`${API_BASE_URL}/api/deal-engine/settings`, {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
          'X-User-Role': user?.role || ''
        },
        body: JSON.stringify(settings)
      });
      const data = await response.json().catch(() => ({}));

      if (!response.ok) {
        throw new Error(data?.error || 'Regler konnten nicht gespeichert werden.');
      }

      setSettings(data.item);
      await refreshDashboard();
      setStatus('Regler gespeichert.');
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'Regler konnten nicht gespeichert werden.');
    } finally {
      setSaving(false);
    }
  }

  async function handleAnalyze() {
    try {
      setAnalyzing(true);
      setStatus('');
      const payload = buildAnalyzePayload(form);
      const response = await fetch(`${API_BASE_URL}/api/deal-engine/analyze`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-User-Role': user?.role || ''
        },
        body: JSON.stringify(payload)
      });
      const data = await response.json().catch(() => ({}));

      if (!response.ok) {
        throw new Error(data?.error || 'Analyse fehlgeschlagen.');
      }

      setResult(data.item);
      await refreshDashboard();
      setStatus(`Analyse abgeschlossen: ${data.item?.decision || 'unbekannt'}.`);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'Analyse fehlgeschlagen.');
    } finally {
      setAnalyzing(false);
    }
  }

  const currentResult = result || dashboard?.runs?.[0] || null;
  const productRules = currentResult?.analysis?.productRules || currentResult?.productRules || null;
  const qualityPreset = detectQualityPreset(settings);
  const productFilterPreset = detectProductFilterPreset(settings);
  const automationPreset = detectAutomationPreset(settings);

  const recentDealCount = useMemo(
    () => (dashboard?.timeline || []).filter((entry) => isWithinLastMinutes(entry.createdAt, 5)).length,
    [dashboard]
  );

  const systemStatusCards = useMemo(
    () => [
      {
        title: 'Backend',
        value: loading ? 'Laedt' : 'Online',
        detail: dashboard?.systemStatus?.detail || 'API erreichbar',
        tone: loading ? 'info' : 'success'
      },
      {
        title: 'Telegram Reader',
        value: botDashboard?.operationalStatus?.telegramReader?.label || 'unbekannt',
        detail: botDashboard?.operationalStatus?.telegramReader?.detail || 'Reader-Status wird geladen.',
        tone: getToneClass(botDashboard?.operationalStatus?.telegramReader?.status || botDashboard?.operationalStatus?.telegramReader?.label)
      },
      {
        title: 'Queue',
        value: botDashboard?.operationalStatus?.scheduler?.label || 'bereit',
        detail:
          botDashboard?.operationalStatus?.scheduler?.detail ||
          `${dashboard?.outputs?.openQueueCount || 0} offene Queue-Eintraege fuer die Deal Engine`,
        tone: getToneClass(botDashboard?.operationalStatus?.scheduler?.status || botDashboard?.operationalStatus?.scheduler?.label)
      },
      {
        title: 'Fehlerstatus',
        value: dashboard?.errors?.length ? `${dashboard.errors.length} offen` : 'keine',
        detail: dashboard?.errors?.[0]?.detail || 'Keine aktuellen Blocker.',
        tone: dashboard?.errors?.length ? 'danger' : 'success'
      }
    ],
    [botDashboard, dashboard, loading]
  );

  const liveMetricCards = useMemo(
    () => [
      {
        title: 'Neue Deals',
        value: `${recentDealCount} / 5 min`,
        detail: `${dashboard?.metrics?.totalRuns || 0} Gesamt-Runs`,
        tone: recentDealCount > 0 ? 'success' : 'info'
      },
      {
        title: 'Approve Count',
        value: toNumber(dashboard?.metrics?.approvedRuns),
        detail: 'Nur APPROVE geht weiter in die Publisher-Strecke.',
        tone: 'success'
      },
      {
        title: 'Review Count',
        value: toNumber(dashboard?.metrics?.queuedRuns),
        detail: 'QUEUE wird hier als Review-Sammelstrecke behandelt.',
        tone: toNumber(dashboard?.metrics?.queuedRuns) > 0 ? 'warning' : 'info'
      },
      {
        title: 'Block Count',
        value: toNumber(dashboard?.metrics?.rejectedRuns),
        detail: 'REJECT durch Validierung, Fake-Pattern oder Produktregeln.',
        tone: toNumber(dashboard?.metrics?.rejectedRuns) > 0 ? 'danger' : 'success'
      }
    ],
    [dashboard, recentDealCount]
  );

  const outputStatusCards = useMemo(() => {
    const telegramReady = dashboard?.outputs?.snapshot?.telegram?.configured === true;
    const baseTone = telegramReady ? 'success' : 'warning';
    const baseDetail = telegramReady ? 'Telegram Bot ist bereit. Approved/Rejected laufen ueber ENV-Routen.' : 'Telegram Bot oder Zielgruppe pruefen.';

    return [
      {
        title: 'Testgruppe',
        value: telegramReady ? 'aktiv' : 'pruefen',
        detail: 'Bestehender Generator-Post plus Analyse bleiben aktiv.',
        tone: baseTone
      },
      {
        title: 'Approved Gruppe',
        value: telegramReady ? 'bereit' : 'inaktiv',
        detail: baseDetail,
        tone: baseTone
      },
      {
        title: 'Rejected Gruppe',
        value: telegramReady ? 'bereit' : 'inaktiv',
        detail: baseDetail,
        tone: baseTone
      }
    ];
  }, [dashboard]);

  const flowCards = useMemo(
    () => [
      {
        title: 'Reader',
        value: botDashboard?.operationalStatus?.telegramReader?.label || 'vorbereitet',
        detail: botDashboard?.operationalStatus?.telegramReader?.detail || 'Quelle nimmt Deals entgegen.',
        tone: getToneClass(botDashboard?.operationalStatus?.telegramReader?.status || botDashboard?.operationalStatus?.telegramReader?.label)
      },
      {
        title: 'Analyse',
        value: productRules?.status === 'matched' ? 'Produktregel aktiv' : 'Produktregeln + Keepa',
        detail:
          productRules?.summary ||
          'Marktvergleich zuerst, Keepa nur Fallback, Produktregeln greifen zusaetzlich.',
        tone: productRules?.status === 'matched' ? 'warning' : 'success'
      },
      {
        title: 'Decision',
        value: currentResult?.decision || 'wartet',
        detail: currentResult?.decisionReason || 'Noch keine finale Entscheidung.',
        tone: getToneClass(currentResult?.decision || dashboard?.systemStatus?.label)
      },
      {
        title: 'Output',
        value: dashboard?.outputs?.openQueueCount ? `${dashboard.outputs.openQueueCount} offen` : 'bereit',
        detail: 'Test / Approved / Rejected werden getrennt bedient.',
        tone: dashboard?.outputs?.openQueueCount ? 'warning' : 'success'
      }
    ],
    [botDashboard, currentResult, dashboard, productRules]
  );

  const moduleCards = [
    { title: 'Scrapper', path: '/scraper', detail: 'Quellen und Importstrecke.' },
    { title: 'Copybot', path: '/copybot', detail: 'Review Queue und Quellenmanagement.' },
    { title: 'Templates', path: '/templates', detail: 'Bausteine und Generator-Vorlagen.' },
    { title: 'Autobot', path: '/autobot', detail: 'Automatische Prozesse und Jobs.' },
    { title: 'Logik-Zentrale', path: '/learning', detail: 'Keepa, Fake-Drop und Lernlogik.' },
    { title: 'Sperrzeiten', path: '/sperrzeiten', detail: 'Deal-Lock und Cooldowns.' },
    { title: 'Logs', path: '/logs', detail: 'Fehler, Queue und Laufzeit-Logs.' }
  ];

  const safetyChecks = [
    'Hauptpost nutzt keine Telegram-Titel aus Fremdquellen.',
    'Hauptpost nutzt keine Telegram-Bilder oder fremde Collagen.',
    'Hauptpost nutzt keine fremden Links.',
    'Hauptpost darf nur mit PAAPI-, Amazon- und verifizierten Daten gebaut werden.'
  ];

  const routingExamples = [
    {
      title: 'Testgruppe',
      body: 'Nachricht 1: bestehender Generator-Post (unveraendert)\nNachricht 2: komplette Analyse'
    },
    {
      title: 'Approved Gruppe',
      body: 'Nur APPROVE\nNur Nachricht 1\nKeine Analyse'
    },
    {
      title: 'Rejected Gruppe',
      body: '\u26A0\uFE0F NICHT VEROEFFENTLICHT\n\u{1F4E2} Quelle: Beispielgruppe\n\u{1F4CC} Grund: Produktregel blockiert.\n\u{1F6E0} Loesung: Preislimit oder Daten pruefen.'
    }
  ];

  const presetOptions = {
    quality: [
      ['locker', 'Locker'],
      ['normal', 'Normal'],
      ['streng', 'Streng'],
      ['profi', 'Profi']
    ],
    productFilter: [
      ['standard', 'Standard'],
      ['streng', 'Streng'],
      ['china_filter', 'China-Filter aktiv']
    ],
    automation: [
      ['testmodus', 'Testmodus'],
      ['review_sammeln', 'Review sammeln'],
      ['auto_posten', 'Auto posten']
    ]
  };

  return (
    <Layout>
      <div className="engine-page">
        <section className="card engine-hero">
          <div className="engine-hero-grid">
            <div className="engine-hero-copy">
              <p className="section-title">Deal Engine</p>
              <h1 className="page-title">Produktregeln, sauberes Routing und ein Dashboard ohne Scroll-Chaos</h1>
              <p className="page-subtitle">
                Die Seite zeigt jetzt zuerst Status, Metriken, Output und Schnellaktionen. Darunter kommen Presets,
                Deal-Flow, Routing-Beispiele, Sicherheitscheck und erst dann die tieferen Expert-Regler.
              </p>
            </div>
            <div className="engine-hero-side">
              <span className="badge">Hauptpost bleibt unveraendert</span>
              <span className={`status-chip ${dashboard?.systemStatus?.tone || 'info'}`}>{dashboard?.systemStatus?.label || 'loading'}</span>
              <span className="badge">Keepa und Queue bleiben erhalten</span>
            </div>
          </div>
          {status ? (
            <div className="engine-inline-alert">
              <span className={`status-chip ${getToneClass(status)}`}>{getToneClass(status)}</span>
              <p>{status}</p>
            </div>
          ) : null}
        </section>

        {loading ? (
          <section className="card engine-panel">
            <p style={{ margin: 0 }}>Deal Engine wird geladen...</p>
          </section>
        ) : (
          <>
            <section className="card engine-panel" style={{ position: 'sticky', top: 16, zIndex: 4 }}>
              <div className="engine-panel-header">
                <div>
                  <p className="section-title">Sofortansicht</p>
                  <h2 className="page-title">Alles Wichtige oben</h2>
                </div>
                <span className="engine-header-note">{dashboard?.feasibility?.detail || '-'}</span>
              </div>

              <div className="engine-card-grid">
                {systemStatusCards.map((card) => (
                  <article key={card.title} className={`engine-card engine-tone-${card.tone}`}>
                    <div className="engine-card-head">
                      <p className="section-title">{card.title}</p>
                      <span className={`status-chip ${card.tone}`}>{card.tone}</span>
                    </div>
                    <h3>{card.value}</h3>
                    <p>{card.detail}</p>
                  </article>
                ))}
              </div>

              <div className="engine-card-grid">
                {liveMetricCards.map((card) => (
                  <article key={card.title} className={`engine-card engine-tone-${card.tone}`}>
                    <div className="engine-card-head">
                      <p className="section-title">{card.title}</p>
                      <span className={`status-chip ${card.tone}`}>{card.tone}</span>
                    </div>
                    <h3>{card.value}</h3>
                    <p>{card.detail}</p>
                  </article>
                ))}
              </div>

              <div className="engine-card-grid">
                {outputStatusCards.map((card) => (
                  <article key={card.title} className={`engine-card engine-tone-${card.tone}`}>
                    <div className="engine-card-head">
                      <p className="section-title">{card.title}</p>
                      <span className={`status-chip ${card.tone}`}>{card.tone}</span>
                    </div>
                    <h3>{card.value}</h3>
                    <p>{card.detail}</p>
                  </article>
                ))}
                <article className="engine-card engine-tone-info">
                  <div className="engine-card-head">
                    <p className="section-title">Quick Actions</p>
                    <span className="status-chip info">go</span>
                  </div>
                  <div style={{ display: 'grid', gap: 10 }}>
                    <button type="button" className="secondary" onClick={() => openInternalRoute('/generator')}>
                      Generator oeffnen
                    </button>
                    <button type="button" className="secondary" onClick={() => openInternalRoute('/publishing/telegram')}>
                      Testgruppe oeffnen
                    </button>
                    <button type="button" className="secondary" onClick={() => openInternalRoute('/logs')}>
                      Logs oeffnen
                    </button>
                    <button type="button" className="secondary" onClick={() => openInternalRoute('/settings')}>
                      Settings oeffnen
                    </button>
                  </div>
                </article>
              </div>
            </section>

            <section className="engine-mandatory-grid">
              <section className="card engine-panel">
                <div className="engine-panel-header">
                  <div>
                    <p className="section-title">Vereinfachte UI</p>
                    <h2 className="page-title">3 Hauptmodi statt Regler-Flut</h2>
                  </div>
                  <span className="engine-header-note">{user?.role === 'admin' ? 'Admin darf speichern' : 'Nur Lesemodus'}</span>
                </div>

                {settings ? (
                  <div style={{ display: 'grid', gap: 18 }}>
                    <article className="engine-card engine-tone-info">
                      <div className="engine-card-head">
                        <p className="section-title">Qualitaet</p>
                        <span className="status-chip info">{qualityPreset}</span>
                      </div>
                      <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
                        {presetOptions.quality.map(([value, label]) => (
                          <button
                            key={value}
                            type="button"
                            className={qualityPreset === value ? 'primary' : 'secondary'}
                            disabled={user?.role !== 'admin'}
                            onClick={() => setSettings((current) => applyQualityPreset(current, value))}
                          >
                            {label}
                          </button>
                        ))}
                      </div>
                    </article>

                    <article className="engine-card engine-tone-info">
                      <div className="engine-card-head">
                        <p className="section-title">Produktfilter</p>
                        <span className="status-chip info">{productFilterPreset}</span>
                      </div>
                      <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
                        {presetOptions.productFilter.map(([value, label]) => (
                          <button
                            key={value}
                            type="button"
                            className={productFilterPreset === value ? 'primary' : 'secondary'}
                            disabled={user?.role !== 'admin'}
                            onClick={() => setSettings((current) => applyProductFilterPreset(current, value))}
                          >
                            {label}
                          </button>
                        ))}
                      </div>
                      <p style={{ margin: '12px 0 0' }}>Beispielregeln aktiv: Powerbank-Limits und China-Kopfhoerer-Block ueber 25 EUR.</p>
                    </article>

                    <article className="engine-card engine-tone-info">
                      <div className="engine-card-head">
                        <p className="section-title">Automatik</p>
                        <span className="status-chip info">{automationPreset}</span>
                      </div>
                      <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
                        {presetOptions.automation.map(([value, label]) => (
                          <button
                            key={value}
                            type="button"
                            className={automationPreset === value ? 'primary' : 'secondary'}
                            disabled={user?.role !== 'admin'}
                            onClick={() => setSettings((current) => applyAutomationPreset(current, value))}
                          >
                            {label}
                          </button>
                        ))}
                      </div>
                      <p style={{ margin: '12px 0 0' }}>Testgruppenrouting laeuft separat im Publisher. Dieses Preset steuert die Deal-Engine-Outputs.</p>
                    </article>

                    <div className="engine-actions">
                      <button type="button" className="secondary" onClick={() => setExpertMode((current) => !current)}>
                        {expertMode ? 'Expert Mode ausblenden' : 'Expert Mode einblenden'}
                      </button>
                      <button type="button" className="secondary" disabled={user?.role !== 'admin' || saving} onClick={handleSaveSettings}>
                        {saving ? 'Speichert...' : 'Regler speichern'}
                      </button>
                    </div>

                    {expertMode ? (
                      <div className="engine-settings-grid">
                        <label>
                          <span>Amazon Tag %</span>
                          <input
                            type="number"
                            value={settings.amazon.dayMinMarketAdvantagePct}
                            onChange={(event) =>
                              updateSettings('amazon', {
                                dayMinMarketAdvantagePct: Number(event.target.value)
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label>
                          <span>Amazon Nacht %</span>
                          <input
                            type="number"
                            value={settings.amazon.nightMinMarketAdvantagePct}
                            onChange={(event) =>
                              updateSettings('amazon', {
                                nightMinMarketAdvantagePct: Number(event.target.value)
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label>
                          <span>FBM Tag %</span>
                          <input
                            type="number"
                            value={settings.fbm.dayMinMarketAdvantagePct}
                            onChange={(event) =>
                              updateSettings('fbm', {
                                dayMinMarketAdvantagePct: Number(event.target.value)
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label>
                          <span>FBM Nacht %</span>
                          <input
                            type="number"
                            value={settings.fbm.nightMinMarketAdvantagePct}
                            onChange={(event) =>
                              updateSettings('fbm', {
                                nightMinMarketAdvantagePct: Number(event.target.value)
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label>
                          <span>Keepa Approve Score</span>
                          <input
                            type="number"
                            value={settings.global.keepaApproveScore}
                            onChange={(event) =>
                              updateSettings('global', {
                                keepaApproveScore: Number(event.target.value)
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label>
                          <span>Keepa Queue Score</span>
                          <input
                            type="number"
                            value={settings.global.keepaQueueScore}
                            onChange={(event) =>
                              updateSettings('global', {
                                keepaQueueScore: Number(event.target.value)
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label>
                          <span>Queue Margin %</span>
                          <input
                            type="number"
                            value={settings.global.queueMarginPct}
                            onChange={(event) =>
                              updateSettings('global', {
                                queueMarginPct: Number(event.target.value)
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label>
                          <span>Cheap Product Limit</span>
                          <input
                            type="number"
                            value={settings.global.cheapProductLimit}
                            onChange={(event) =>
                              updateSettings('global', {
                                cheapProductLimit: Number(event.target.value)
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label className="engine-checkbox">
                          <span>Queue aktiv</span>
                          <input
                            type="checkbox"
                            checked={settings.global.queueEnabled}
                            onChange={(event) =>
                              updateSettings('global', {
                                queueEnabled: event.target.checked
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label className="engine-checkbox">
                          <span>Nachtmodus aktiv</span>
                          <input
                            type="checkbox"
                            checked={settings.global.nightModeEnabled}
                            onChange={(event) =>
                              updateSettings('global', {
                                nightModeEnabled: event.target.checked
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label className="engine-checkbox">
                          <span>Telegram Output</span>
                          <input
                            type="checkbox"
                            checked={settings.output.telegramEnabled}
                            onChange={(event) =>
                              updateSettings('output', {
                                telegramEnabled: event.target.checked
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label className="engine-checkbox">
                          <span>WhatsApp Output</span>
                          <input
                            type="checkbox"
                            checked={settings.output.whatsappEnabled}
                            onChange={(event) =>
                              updateSettings('output', {
                                whatsappEnabled: event.target.checked
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label className="engine-checkbox">
                          <span>AI Resolver optional aktiv</span>
                          <input
                            type="checkbox"
                            checked={settings.ai.resolverEnabled}
                            onChange={(event) =>
                              updateSettings('ai', {
                                resolverEnabled: event.target.checked
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label className="engine-checkbox">
                          <span>KI fuer Amazon Direct</span>
                          <input
                            type="checkbox"
                            checked={settings.ai.amazonDirectEnabled}
                            onChange={(event) =>
                              updateSettings('ai', {
                                amazonDirectEnabled: event.target.checked
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label className="engine-checkbox">
                          <span>KI nur bei Unsicherheit</span>
                          <input
                            type="checkbox"
                            checked={settings.ai.onlyOnUncertainty}
                            onChange={(event) =>
                              updateSettings('ai', {
                                onlyOnUncertainty: event.target.checked
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label className="engine-checkbox">
                          <span>KI immer im Debugmodus</span>
                          <input
                            type="checkbox"
                            checked={settings.ai.alwaysInDebug}
                            onChange={(event) =>
                              updateSettings('ai', {
                                alwaysInDebug: event.target.checked
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label className="engine-checkbox">
                          <span>Marktvergleich fuer Amazon Direct</span>
                          <input
                            type="checkbox"
                            checked={settings.quality.marketCompareAmazonDirectEnabled}
                            onChange={(event) =>
                              updateSettings('quality', {
                                marketCompareAmazonDirectEnabled: event.target.checked
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label className="engine-checkbox">
                          <span>Nur Amazon Direct fuer Marktvergleich</span>
                          <input
                            type="checkbox"
                            checked={settings.quality.marketCompareAmazonDirectOnly}
                            onChange={(event) =>
                              updateSettings('quality', {
                                marketCompareAmazonDirectOnly: event.target.checked
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label className="engine-checkbox">
                          <span>AI nur Amazon Direct</span>
                          <input
                            type="checkbox"
                            checked={settings.quality.aiAmazonDirectOnly}
                            onChange={(event) =>
                              updateSettings('quality', {
                                aiAmazonDirectOnly: event.target.checked
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label className="engine-checkbox">
                          <span>FBA fuer Marktvergleich erlauben</span>
                          <input
                            type="checkbox"
                            checked={settings.quality.allowFbaThirdPartyMarketCompare}
                            onChange={(event) =>
                              updateSettings('quality', {
                                allowFbaThirdPartyMarketCompare: event.target.checked
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label className="engine-checkbox">
                          <span>FBA fuer KI erlauben</span>
                          <input
                            type="checkbox"
                            checked={settings.quality.allowFbaThirdPartyAi}
                            onChange={(event) =>
                              updateSettings('quality', {
                                allowFbaThirdPartyAi: event.target.checked
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label className="engine-checkbox">
                          <span>FBM fuer Marktvergleich erlauben</span>
                          <input
                            type="checkbox"
                            checked={settings.quality.allowFbmMarketCompare}
                            onChange={(event) =>
                              updateSettings('quality', {
                                allowFbmMarketCompare: event.target.checked
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label className="engine-checkbox">
                          <span>FBM fuer KI erlauben</span>
                          <input
                            type="checkbox"
                            checked={settings.quality.allowFbmAi}
                            onChange={(event) =>
                              updateSettings('quality', {
                                allowFbmAi: event.target.checked
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label>
                          <span>Unbekannte Verkaeufer</span>
                          <select
                            value={settings.quality.unknownSellerMode}
                            onChange={(event) =>
                              updateSettings('quality', {
                                unknownSellerMode: event.target.value
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          >
                            <option value="review">REVIEW</option>
                            <option value="block">BLOCK</option>
                          </select>
                        </label>
                      </div>
                    ) : null}
                  </div>
                ) : null}
              </section>

              <section className="card engine-panel">
                <div className="engine-panel-header">
                  <div>
                    <p className="section-title">Deal Flow</p>
                    <h2 className="page-title">{'Reader -> Analyse -> Decision -> Output'}</h2>
                  </div>
                  <span className="engine-header-note">{dashboard?.sources?.activeCount || 0} aktive Quellen</span>
                </div>

                <div className="engine-card-grid">
                  {flowCards.map((card) => (
                    <article key={card.title} className={`engine-card engine-tone-${card.tone}`}>
                      <div className="engine-card-head">
                        <p className="section-title">{card.title}</p>
                        <span className={`status-chip ${card.tone}`}>{card.tone}</span>
                      </div>
                      <h3>{card.value}</h3>
                      <p>{card.detail}</p>
                    </article>
                  ))}
                </div>

                <div className="engine-flow-grid">
                  {moduleCards.map((item) => (
                    <article key={item.title} className="engine-flow-card engine-tone-info">
                      <div className="engine-card-head">
                        <strong>{item.title}</strong>
                        <span className="status-chip info">modul</span>
                      </div>
                      <p>{item.detail}</p>
                      <button type="button" className="secondary" onClick={() => openInternalRoute(item.path)}>
                        Oeffnen
                      </button>
                    </article>
                  ))}
                </div>
              </section>
            </section>

            <section className="engine-mandatory-grid">
              <section className="card engine-panel">
                <div className="engine-panel-header">
                  <div>
                    <p className="section-title">Telegram Routing</p>
                    <h2 className="page-title">3 Gruppen klar getrennt</h2>
                  </div>
                  <span className="engine-header-note">Test / Approved / Rejected</span>
                </div>
                <div className="engine-card-grid">
                  {routingExamples.map((example) => (
                    <article key={example.title} className="engine-card engine-tone-info">
                      <div className="engine-card-head">
                        <p className="section-title">{example.title}</p>
                        <span className="status-chip info">beispiel</span>
                      </div>
                      <pre className="engine-code" style={{ margin: 0 }}>
                        {example.body}
                      </pre>
                    </article>
                  ))}
                </div>
              </section>

              <section className="card engine-panel">
                <div className="engine-panel-header">
                  <div>
                    <p className="section-title">Sicherheitscheck</p>
                    <h2 className="page-title">Hauptpost bleibt sauber</h2>
                  </div>
                  <span className="engine-header-note">PAAPI / Amazon / verifizierte Daten only</span>
                </div>
                <div className="engine-list">
                  {safetyChecks.map((line) => (
                    <article key={line} className="engine-list-item">
                      <strong>Check</strong>
                      <p>{line}</p>
                    </article>
                  ))}
                </div>
              </section>
            </section>

            <section className="card engine-panel">
              <div className="engine-panel-header">
                <div>
                  <p className="section-title">Analyse Engine</p>
                  <h2 className="page-title">Kompletter Deal-Durchlauf</h2>
                </div>
                <span className="engine-header-note">Produktregeln greifen zusaetzlich zu Marktvergleich und Keepa.</span>
              </div>

              <div className="engine-form-grid">
                <label>
                  <span>Quelle</span>
                  <input value={form.sourceName} onChange={(event) => updateForm('sourceName', event.target.value)} />
                </label>
                <label>
                  <span>Plattform</span>
                  <select value={form.sourcePlatform} onChange={(event) => updateForm('sourcePlatform', event.target.value)}>
                    <option value="telegram">telegram</option>
                    <option value="whatsapp">whatsapp</option>
                    <option value="internal">internal</option>
                  </select>
                </label>
                <label>
                  <span>Source Type</span>
                  <input value={form.sourceType} onChange={(event) => updateForm('sourceType', event.target.value)} />
                </label>
                <label>
                  <span>Seller</span>
                  <select value={form.sellerType} onChange={(event) => updateForm('sellerType', event.target.value)}>
                    <option value="AMAZON">AMAZON</option>
                    <option value="FBA">FBA</option>
                    <option value="FBM">FBM</option>
                    <option value="UNKNOWN">UNKNOWN</option>
                  </select>
                </label>
                <label>
                  <span>Seller Klasse</span>
                  <select value={form.sellerClass} onChange={(event) => updateForm('sellerClass', event.target.value)}>
                    <option value="AMAZON_DIRECT">AMAZON_DIRECT</option>
                    <option value="FBA_THIRDPARTY">FBA_THIRDPARTY</option>
                    <option value="FBM_THIRDPARTY">FBM_THIRDPARTY</option>
                    <option value="UNKNOWN">UNKNOWN</option>
                  </select>
                </label>
                <label>
                  <span>Verkauf durch Amazon</span>
                  <select value={form.soldByAmazon} onChange={(event) => updateForm('soldByAmazon', event.target.value)}>
                    <option value="yes">ja</option>
                    <option value="no">nein</option>
                    <option value="unknown">unbekannt</option>
                  </select>
                </label>
                <label>
                  <span>Versand durch Amazon</span>
                  <select value={form.shippedByAmazon} onChange={(event) => updateForm('shippedByAmazon', event.target.value)}>
                    <option value="yes">ja</option>
                    <option value="no">nein</option>
                    <option value="unknown">unbekannt</option>
                  </select>
                </label>
                <label className="engine-span-2">
                  <span>Titel</span>
                  <input value={form.title} onChange={(event) => updateForm('title', event.target.value)} />
                </label>
                <label className="engine-span-2">
                  <span>Amazon URL</span>
                  <input value={form.amazonUrl} onChange={(event) => updateForm('amazonUrl', event.target.value)} />
                </label>
                <label>
                  <span>Amazon Preis</span>
                  <input value={form.amazonPrice} onChange={(event) => updateForm('amazonPrice', event.target.value)} />
                </label>
                <label>
                  <span>Brand</span>
                  <input value={form.brand} onChange={(event) => updateForm('brand', event.target.value)} />
                </label>
                <label>
                  <span>Kategorie</span>
                  <input value={form.category} onChange={(event) => updateForm('category', event.target.value)} />
                </label>
                <label>
                  <span>Variante</span>
                  <input value={form.variantKey} onChange={(event) => updateForm('variantKey', event.target.value)} />
                </label>
                <label>
                  <span>Menge / Set</span>
                  <input value={form.quantityKey} onChange={(event) => updateForm('quantityKey', event.target.value)} />
                </label>
                <label>
                  <span>Tag/Nacht Override</span>
                  <select value={form.overrideDayPart} onChange={(event) => updateForm('overrideDayPart', event.target.value)}>
                    <option value="day">day</option>
                    <option value="night">night</option>
                  </select>
                </label>
                <label className="engine-checkbox">
                  <span>Markenprodukt</span>
                  <input type="checkbox" checked={form.isBrandProduct} onChange={(event) => updateForm('isBrandProduct', event.target.checked)} />
                </label>
                <label className="engine-checkbox">
                  <span>No-Name</span>
                  <input type="checkbox" checked={form.isNoName} onChange={(event) => updateForm('isNoName', event.target.checked)} />
                </label>
                <label className="engine-checkbox">
                  <span>China Produkt</span>
                  <input type="checkbox" checked={form.isChinaProduct} onChange={(event) => updateForm('isChinaProduct', event.target.checked)} />
                </label>
                <label className="engine-span-2">
                  <span>Marktangebote JSON</span>
                  <textarea value={form.marketOffersJson} onChange={(event) => updateForm('marketOffersJson', event.target.value)} rows={12} />
                </label>
                <label>
                  <span>Keepa JSON</span>
                  <textarea value={form.keepaJson} onChange={(event) => updateForm('keepaJson', event.target.value)} rows={12} />
                </label>
                <label>
                  <span>AI JSON</span>
                  <textarea value={form.aiJson} onChange={(event) => updateForm('aiJson', event.target.value)} rows={12} />
                </label>
              </div>

              <div className="engine-actions">
                <button type="button" className="secondary" onClick={() => setForm(buildFormFromSample(samplePayload || {}))}>
                  Sample laden
                </button>
                <button type="button" className="primary" disabled={analyzing} onClick={handleAnalyze}>
                  {analyzing ? 'Analysiert...' : 'Analyse starten'}
                </button>
              </div>
            </section>

            <section className="engine-result-grid">
              <section className="card engine-panel">
                <div className="engine-panel-header">
                  <div>
                    <p className="section-title">Analyse Output</p>
                    <h2 className="page-title">Finale Entscheidung</h2>
                  </div>
                  <span className="engine-header-note">Letztes Ergebnis {currentResult ? formatDateTime(currentResult.createdAt) : '-'}</span>
                </div>
                {currentResult ? (
                  <>
                    <div className="engine-card-grid">
                      <article className={`engine-card engine-tone-${getToneClass(currentResult.decision)}`}>
                        <div className="engine-card-head">
                          <p className="section-title">Entscheidung</p>
                          <span className={`status-chip ${getToneClass(currentResult.decision)}`}>{currentResult.decision}</span>
                        </div>
                        <h3>{currentResult.analysis?.decisionSourceLabel || currentResult.analysis?.decisionSource || '-'}</h3>
                        <p>{currentResult.decisionReason}</p>
                      </article>
                      <article className={`engine-card engine-tone-${productRules?.status === 'matched' ? 'warning' : 'info'}`}>
                        <div className="engine-card-head">
                          <p className="section-title">Produktregeln</p>
                          <span className={`status-chip ${productRules?.status === 'matched' ? 'warning' : 'info'}`}>{productRules?.status || 'clear'}</span>
                        </div>
                        <h3>{productRules?.action || 'none'}</h3>
                        <p>{productRules?.summary || 'Keine Produktregel ausgelost.'}</p>
                      </article>
                      <article className="engine-card engine-tone-info">
                        <div className="engine-card-head">
                          <p className="section-title">Markt</p>
                          <span className="status-chip info">{currentResult.marketOfferCount || 0} gueltig</span>
                        </div>
                        <h3>{currentResult.marketAdvantagePct ?? '-'}%</h3>
                        <p>Marktpreis {currentResult.marketPrice ?? '-'} | Schwelle {currentResult.analysis?.thresholdPct ?? '-'}</p>
                      </article>
                      <article className="engine-card engine-tone-info">
                        <div className="engine-card-head">
                          <p className="section-title">Keepa</p>
                          <span className="status-chip info">{currentResult.analysis?.fallbackUsed ? 'fallback' : 'idle'}</span>
                        </div>
                        <h3>{currentResult.keepaScore ?? '-'}</h3>
                        <p>avg90 {currentResult.keepaDiscount90 ?? '-'}% | avg180 {currentResult.keepaDiscount180 ?? '-'}%</p>
                      </article>
                    </div>
                    <div className="engine-list">
                      {(currentResult.analysis?.reasons || []).map((reason, index) => (
                        <article key={`${currentResult.id}-reason-${index}`} className="engine-list-item">
                          <strong>Reason {index + 1}</strong>
                          <p>{reason}</p>
                        </article>
                      ))}
                    </div>
                    <div className="engine-code-grid">
                      <div>
                        <p className="section-title">Flow</p>
                        <pre className="engine-code">{prettyJson(currentResult.analysis?.flow || [])}</pre>
                      </div>
                      <div>
                        <p className="section-title">Analyse JSON</p>
                        <pre className="engine-code">{prettyJson(currentResult.analysis || {})}</pre>
                      </div>
                    </div>
                  </>
                ) : (
                  <p className="engine-empty">Noch kein Analyseergebnis vorhanden.</p>
                )}
              </section>

              <section className="card engine-panel">
                <div className="engine-panel-header">
                  <div>
                    <p className="section-title">Timeline und Fehler</p>
                    <h2 className="page-title">Letzte Runs</h2>
                  </div>
                  <span className="engine-header-note">{dashboard?.metrics?.totalRuns || 0} Gesamt-Runs</span>
                </div>
                <div className="engine-list">
                  {(dashboard?.timeline || []).length ? (
                    dashboard.timeline.map((entry) => (
                      <article key={entry.id} className="engine-list-item">
                        <div className="engine-card-head">
                          <strong>{entry.title}</strong>
                          <span className={`status-chip ${entry.tone}`}>{formatDateTime(entry.createdAt)}</span>
                        </div>
                        <p>{entry.detail}</p>
                      </article>
                    ))
                  ) : (
                    <p className="engine-empty">Noch keine Runs vorhanden.</p>
                  )}
                </div>
                <div className="engine-divider" />
                <div className="engine-list">
                  {(dashboard?.errors || []).length ? (
                    dashboard.errors.map((entry) => (
                      <article key={entry.id} className="engine-list-item">
                        <div className="engine-card-head">
                          <strong>{entry.title}</strong>
                          <span className={`status-chip ${entry.tone}`}>{entry.tone}</span>
                        </div>
                        <p>{entry.detail}</p>
                      </article>
                    ))
                  ) : (
                    <p className="engine-empty">Keine aktuellen Fehlerhinweise.</p>
                  )}
                </div>
              </section>
            </section>
          </>
        )}
      </div>
    </Layout>
  );
}

export default DealEnginePage;
