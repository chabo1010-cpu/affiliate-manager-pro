import { useEffect, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import Layout from '../components/layout/Layout';
import { useAuth } from '../context/AuthContext';
import './Home.css';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:4000';

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function getStatusTone(status) {
  const normalized = String(status || '').trim().toLowerCase();

  if (
    normalized.includes('fehler') ||
    normalized.includes('kritisch') ||
    normalized.includes('ausfall') ||
    normalized.includes('auth_')
  ) {
    return 'danger';
  }

  if (
    normalized.includes('review') ||
    normalized.includes('warn') ||
    normalized.includes('vorbereitet') ||
    normalized.includes('deaktiviert') ||
    normalized.includes('nicht')
  ) {
    return 'warning';
  }

  if (normalized.includes('live') || normalized.includes('fokus') || normalized.includes('testgruppe')) {
    return 'info';
  }

  if (
    normalized.includes('aktiv') ||
    normalized.includes('verbunden') ||
    normalized.includes('stabil') ||
    normalized.includes('ok') ||
    normalized.includes('bereit') ||
    normalized.includes('pflicht')
  ) {
    return 'success';
  }

  return 'info';
}

function formatDateTime(value) {
  if (!value) {
    return '-';
  }

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }

  return new Intl.DateTimeFormat('de-DE', {
    dateStyle: 'short',
    timeStyle: 'short'
  }).format(parsed);
}

function toTimestamp(value) {
  if (!value) {
    return 0;
  }

  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? 0 : parsed.getTime();
}

function shortenText(value, maxLength = 96) {
  if (!value) {
    return '-';
  }

  const normalized = String(value).replace(/\s+/g, ' ').trim();
  if (normalized.length <= maxLength) {
    return normalized;
  }

  return `${normalized.slice(0, Math.max(0, maxLength - 3)).trimEnd()}...`;
}

function formatLogTitle(log) {
  const worker = String(log?.worker_type || '').trim().toLowerCase();
  const workerLabel = worker ? worker.charAt(0).toUpperCase() + worker.slice(1) : 'System';

  if (log?.event_type === 'target.posted') {
    return `${workerLabel} Output`;
  }

  if (log?.event_type === 'target.failed') {
    return `${workerLabel} Fehler`;
  }

  if (log?.event_type === 'queue.created') {
    return 'Queue erstellt';
  }

  if (log?.event_type === 'target.retry') {
    return `${workerLabel} Retry`;
  }

  return worker ? workerLabel : 'Systemevent';
}

function pickLatest(items = []) {
  return [...items]
    .filter((item) => item && item.at)
    .sort((left, right) => toTimestamp(right.at) - toTimestamp(left.at))[0] || null;
}

function HomePage() {
  const { user } = useAuth();
  const [dashboard, setDashboard] = useState({
    overview: null,
    keepaStatus: null,
    amazonStatus: null,
    queue: null,
    logs: null,
    workers: null
  });
  const [loading, setLoading] = useState(true);
  const [status, setStatus] = useState('');

  useEffect(() => {
    let cancelled = false;

    async function apiFetch(path) {
      const response = await fetch(`${API_BASE_URL}${path}`, {
        headers: {
          'Content-Type': 'application/json',
          'X-User-Role': user?.role || ''
        }
      });
      const data = await response.json().catch(() => ({}));

      if (!response.ok) {
        throw new Error(data?.error || `Request fehlgeschlagen (${response.status}).`);
      }

      return data;
    }

    async function loadDashboard() {
      try {
        setLoading(true);
        setStatus('');

        const results = await Promise.allSettled([
          apiFetch('/api/learning/overview'),
          apiFetch('/api/keepa/status'),
          apiFetch('/api/amazon/status'),
          apiFetch('/api/publishing/queue'),
          apiFetch('/api/publishing/logs'),
          apiFetch('/api/publishing/workers/status')
        ]);

        const [overviewResult, keepaResult, amazonResult, queueResult, logsResult, workersResult] = results;

        if (overviewResult.status !== 'fulfilled') {
          throw overviewResult.reason instanceof Error
            ? overviewResult.reason
            : new Error('Operations-Dashboard konnte nicht geladen werden.');
        }

        const partialErrors = results
          .slice(1)
          .filter((item) => item.status === 'rejected')
          .map((item) => (item.reason instanceof Error ? item.reason.message : 'Ein Teilbereich konnte nicht geladen werden.'));

        if (!cancelled) {
          setDashboard({
            overview: overviewResult.value,
            keepaStatus: keepaResult.status === 'fulfilled' ? keepaResult.value : null,
            amazonStatus: amazonResult.status === 'fulfilled' ? amazonResult.value : null,
            queue: queueResult.status === 'fulfilled' ? queueResult.value : null,
            logs: logsResult.status === 'fulfilled' ? logsResult.value : null,
            workers: workersResult.status === 'fulfilled' ? workersResult.value : null
          });

          if (partialErrors.length) {
            setStatus(`Monitoring teilweise unvollstaendig: ${partialErrors[0]}`);
          }
        }
      } catch (error) {
        if (!cancelled) {
          setStatus(error instanceof Error ? error.message : 'Operations-Dashboard konnte nicht geladen werden.');
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    void loadDashboard();

    return () => {
      cancelled = true;
    };
  }, [user?.role]);

  const overview = dashboard.overview;
  const keepaStatus = dashboard.keepaStatus;
  const amazonStatus = dashboard.amazonStatus;
  const queueItems = Array.isArray(dashboard.queue?.items) ? dashboard.queue.items : [];
  const publishingLogs = Array.isArray(dashboard.logs?.items) ? dashboard.logs.items : [];
  const workerChannels = Array.isArray(dashboard.workers?.channels) ? dashboard.workers.channels : [];
  const facebookWorker = dashboard.workers?.facebook || {};
  const sourceStatuses = Array.isArray(overview?.sourceStatuses) ? overview.sourceStatuses : [];
  const sellerControls = Array.isArray(overview?.sellerControls) ? overview.sellerControls : [];
  const pipeline = Array.isArray(overview?.pipeline) ? overview.pipeline : [];
  const outputStatuses = overview?.outputStatuses || {};
  const copybot = overview?.copybot || {};
  const reviewCount = toNumber(outputStatuses?.reviewCount);
  const keepaSourceStatus = sourceStatuses.find((item) => item.id === 'keepa') || null;
  const amazonSourceStatus = sourceStatuses.find((item) => item.id === 'amazon_api') || null;
  const scrapperSourceStatus = sourceStatuses.find((item) => item.id === 'scrapper') || null;
  const learningSourceStatus = sourceStatuses.find((item) => item.id === 'learning_logic') || null;
  const telegramSourceStatus = sourceStatuses.find((item) => item.id === 'telegram_output') || null;
  const generatorPipeline = pipeline.find((item) => item.id === 'generator') || null;
  const scrapperPipeline = pipeline.find((item) => item.id === 'scrapper') || null;
  const autoPipeline = pipeline.find((item) => item.id === 'auto_deals') || null;
  const activePatternSupport = sellerControls.filter((item) => item.patternSupportEnabled).length;

  const workerChannelMap = useMemo(
    () => new Map(workerChannels.map((item) => [String(item.channel_type || '').toLowerCase(), item])),
    [workerChannels]
  );

  const queueSummary = useMemo(
    () =>
      workerChannels.reduce(
        (summary, item) => ({
          waiting: summary.waiting + toNumber(item.waiting),
          processing: summary.processing + toNumber(item.processing),
          posted: summary.posted + toNumber(item.posted),
          failed: summary.failed + toNumber(item.failed)
        }),
        { waiting: 0, processing: 0, posted: 0, failed: 0 }
      ),
    [workerChannels]
  );

  const latestPostedLog = pickLatest(
    publishingLogs
      .filter((log) => log?.event_type === 'target.posted')
      .map((log) => ({
        label: formatLogTitle(log),
        detail: shortenText(log?.message || 'Publishing erfolgreich verarbeitet.', 88),
        at: log?.created_at || log?.createdAt,
        tone: 'success'
      }))
  );

  const latestIssueLog = pickLatest(
    publishingLogs
      .filter((log) => ['warning', 'error'].includes(String(log?.level || '').toLowerCase()))
      .map((log) => ({
        label: formatLogTitle(log),
        detail: shortenText(log?.message || 'Warnung oder Fehler im Publishing.', 88),
        at: log?.created_at || log?.createdAt,
        tone: 'danger'
      }))
  );

  const latestSuccessAction = pickLatest([
    latestPostedLog,
    keepaStatus?.overview?.apiUsage?.lastRequestAt
      ? {
          label: 'Keepa Abruf',
          detail: `${toNumber(keepaStatus?.overview?.apiUsage?.requestCount24h)} Requests in 24h`,
          at: keepaStatus.overview.apiUsage.lastRequestAt,
          tone: 'info'
        }
      : null,
    amazonStatus?.overview?.lastSuccessfulFetch
      ? {
          label: 'Amazon API Abruf',
          detail: `${toNumber(amazonStatus?.overview?.successCount24h)} erfolgreiche Requests in 24h`,
          at: amazonStatus.overview.lastSuccessfulFetch,
          tone: 'info'
        }
      : null,
    copybot?.lastProcessedSource?.last_import_at
      ? {
          label: 'Scrapper Import',
          detail: shortenText(copybot?.lastProcessedSource?.name || 'Letzte Quelle verarbeitet.', 88),
          at: copybot.lastProcessedSource.last_import_at,
          tone: 'info'
        }
      : null
  ]);

  const latestIssueAction = pickLatest([
    latestIssueLog,
    amazonStatus?.overview?.lastErrorAt
      ? {
          label: 'Amazon API Fehler',
          detail: shortenText(amazonStatus?.overview?.lastErrorMessage || 'Letzter Amazon-Fehler.', 88),
          at: amazonStatus.overview.lastErrorAt,
          tone: 'danger'
        }
      : null
  ]);

  const latestGeneratorQueue = queueItems.find((item) => item?.source_type === 'generator') || null;

  const sourceCards = [
    {
      id: 'keepa',
      title: 'Keepa',
      status: keepaSourceStatus?.status || 'vorbereitet',
      description: 'Preisverlauf und Deal-Quelle',
      lastAction: keepaStatus?.overview?.apiUsage?.lastRequestAt || null,
      primaryMetric: `${toNumber(keepaStatus?.overview?.apiUsage?.hitsToday)} Treffer heute`,
      secondaryMetric: `${toNumber(keepaStatus?.overview?.apiUsage?.requestCount24h)} Requests / 24h`
    },
    {
      id: 'amazon',
      title: 'Amazon API',
      status: amazonSourceStatus?.status || amazonStatus?.overview?.apiStatus || 'vorbereitet',
      description: 'Produkt- und Affiliate-Daten',
      lastAction: amazonStatus?.overview?.lastSuccessfulFetch || amazonStatus?.connection?.checkedAt || null,
      primaryMetric: `${toNumber(amazonStatus?.overview?.requestCount24h)} Requests / 24h`,
      secondaryMetric:
        amazonStatus?.overview?.lastErrorAt && !amazonStatus?.overview?.lastSuccessfulFetch
          ? 'Letzter Abruf mit Fehler'
          : `${toNumber(amazonStatus?.overview?.successCount24h)} erfolgreiche Abrufe`
    },
    {
      id: 'scrapper',
      title: 'Scrapper',
      status: scrapperSourceStatus?.status || (copybot?.copybotEnabled ? 'aktiv' : 'deaktiviert'),
      description: 'Rohdeal-Eingang und Quellenimport',
      lastAction: copybot?.lastProcessedSource?.last_import_at || null,
      primaryMetric: `${toNumber(copybot?.reviewCount)} Review | ${toNumber(copybot?.approvedCount)} approved`,
      secondaryMetric: copybot?.lastProcessedSource?.name ? shortenText(copybot.lastProcessedSource.name, 44) : 'Noch kein letzter Import'
    },
    {
      id: 'generator',
      title: 'Generator',
      status: 'aktiv',
      description: 'Manueller Posting-Bereich',
      lastAction: latestGeneratorQueue?.created_at || null,
      primaryMetric: shortenText(generatorPipeline?.detail || 'Manueller Direct-Publish-Flow aktiv.', 56),
      secondaryMetric: latestGeneratorQueue ? 'Letzter Queue-Eintrag vorhanden' : 'Keine letzte Queue-Aktion'
    }
  ];

  const channelStats = {
    telegram: workerChannelMap.get('telegram') || {},
    whatsapp: workerChannelMap.get('whatsapp') || {},
    facebook: workerChannelMap.get('facebook') || {}
  };

  const channelCards = [
    {
      id: 'telegram',
      title: 'Telegram',
      status:
        telegramSourceStatus?.status === 'aktiv'
          ? latestPostedLog?.label?.toLowerCase().includes('telegram')
            ? 'aktiv'
            : 'bereit'
          : telegramSourceStatus?.status || 'vorbereitet',
      mode: overview?.keepa?.maskedChatId ? 'Testgruppe' : 'Chat-ID offen',
      description: 'Echter Testgruppen-Output',
      lastSuccess: publishingLogs.find((log) => log?.worker_type === 'telegram' && log?.event_type === 'target.posted') || null,
      lastIssue: publishingLogs.find((log) => log?.worker_type === 'telegram' && ['warning', 'error'].includes(String(log?.level || '').toLowerCase())) || null,
      metrics: channelStats.telegram
    },
    {
      id: 'whatsapp',
      title: 'WhatsApp',
      status: publishingLogs.find((log) => log?.worker_type === 'whatsapp' && ['warning', 'error'].includes(String(log?.level || '').toLowerCase()))
        ? 'warnung'
        : 'vorbereitet',
      mode: 'Simulierter Worker',
      description: 'Noch nicht live verbunden',
      lastSuccess: publishingLogs.find((log) => log?.worker_type === 'whatsapp' && log?.event_type === 'target.posted') || null,
      lastIssue: publishingLogs.find((log) => log?.worker_type === 'whatsapp' && ['warning', 'error'].includes(String(log?.level || '').toLowerCase())) || null,
      metrics: channelStats.whatsapp
    },
    {
      id: 'facebook',
      title: 'Facebook',
      status: publishingLogs.find((log) => log?.worker_type === 'facebook' && ['warning', 'error'].includes(String(log?.level || '').toLowerCase()))
        ? 'warnung'
        : facebookWorker?.enabled
          ? 'aktiv'
          : 'vorbereitet',
      mode: facebookWorker?.enabled ? `Session ${facebookWorker.sessionMode || 'persistent'}` : 'Worker vorbereitet',
      description: 'Worker-basierter Posting-Kanal',
      lastSuccess: publishingLogs.find((log) => log?.worker_type === 'facebook' && log?.event_type === 'target.posted') || null,
      lastIssue: publishingLogs.find((log) => log?.worker_type === 'facebook' && ['warning', 'error'].includes(String(log?.level || '').toLowerCase())) || null,
      metrics: channelStats.facebook
    }
  ];

  const activeSourceCount = sourceCards.filter((item) => getStatusTone(item.status) === 'success').length;
  const activeChannelCount = channelCards.filter((item) => getStatusTone(item.status) === 'success').length;

  const healthState = useMemo(() => {
    const sourceHasError = sourceCards.some((item) => getStatusTone(item.status) === 'danger');
    const channelHasError = channelCards.some((item) => getStatusTone(item.status) === 'danger');

    if (sourceHasError || channelHasError) {
      return {
        label: 'kritisch',
        tone: 'danger',
        detail: 'Mindestens eine Quelle oder ein Kanal meldet Fehler.'
      };
    }

    if (reviewCount > 0 || sourceCards.some((item) => getStatusTone(item.status) === 'warning')) {
      return {
        label: 'beobachten',
        tone: 'warning',
        detail: 'System laeuft, aber Reviews oder vorbereitete Bereiche brauchen Aufmerksamkeit.'
      };
    }

    return {
      label: 'stabil',
      tone: 'success',
      detail: 'Quellen, Logik und Output laufen ohne akuten Stoerfall.'
    };
  }, [channelCards, reviewCount, sourceCards]);

  const topStatusCards = [
    {
      title: 'System Health',
      value: healthState.label,
      detail: healthState.detail,
      tone: healthState.tone
    },
    {
      title: 'Aktive Quellen',
      value: `${activeSourceCount}/4`,
      detail: 'Keepa, Amazon API, Scrapper und Generator',
      tone: activeSourceCount >= 2 ? 'success' : 'warning'
    },
    {
      title: 'Aktive Kanaele',
      value: `${activeChannelCount}/3`,
      detail: 'Telegram, WhatsApp und Facebook',
      tone: activeChannelCount >= 1 ? 'info' : 'warning'
    },
    {
      title: 'Letzte erfolgreiche Aktion',
      value: latestSuccessAction ? formatDateTime(latestSuccessAction.at) : '-',
      detail: latestSuccessAction ? `${latestSuccessAction.label} - ${latestSuccessAction.detail}` : 'Noch keine letzte Erfolgsaktion vorhanden.',
      tone: latestSuccessAction?.tone || 'info'
    },
    {
      title: 'Offene Reviews',
      value: `${reviewCount}`,
      detail: reviewCount ? 'Faelle warten auf Freigabe oder Nachpruefung.' : 'Kein offener Review-Stau.',
      tone: reviewCount ? 'warning' : 'success'
    },
    {
      title: 'Letzte Warnung / Fehler',
      value: latestIssueAction ? formatDateTime(latestIssueAction.at) : 'Keine',
      detail: latestIssueAction ? `${latestIssueAction.label} - ${latestIssueAction.detail}` : 'Aktuell liegt keine letzte Warnung oder kein Fehler vor.',
      tone: latestIssueAction?.tone || 'success'
    }
  ];

  const flowColumns = [
    {
      title: 'Quellen',
      subtitle: 'Eingaenge',
      items: sourceCards.map((item) => ({
        title: item.title,
        status: item.status,
        detail: item.description
      }))
    },
    {
      title: 'Verarbeitung',
      subtitle: 'Pflichtschichten',
      items: [
        {
          title: 'Link Builder',
          status: 'aktiv',
          detail: 'Externe Amazon-Links werden standardisiert.'
        },
        {
          title: 'Lern-Logik',
          status: learningSourceStatus?.status || 'aktiv',
          detail: shortenText(autoPipeline?.detail || 'Alle Auto-Quellen laufen zuerst durch die Entscheidung.', 78)
        },
        {
          title: 'Muster-Erkennung',
          status: activePatternSupport ? 'aktiv' : 'deaktiviert',
          detail: `${activePatternSupport}/${Math.max(sellerControls.length, 3)} Seller-Type-Profile aktiv`
        }
      ]
    },
    {
      title: 'Entscheidung',
      subtitle: 'Regeln',
      items: [
        {
          title: 'Review Gate',
          status: reviewCount ? 'review' : 'bereit',
          detail: reviewCount ? `${reviewCount} offene Reviews` : 'Kein Review-Stau'
        },
        ...sellerControls.map((item) => ({
          title: item.id,
          status: item.patternSupportEnabled ? 'aktiv' : 'deaktiviert',
          detail: `Muster ${item.patternSupportEnabled ? 'an' : 'aus'} | Auto ${item.autoPostingEnabled ? 'an' : 'aus'}`
        }))
      ]
    },
    {
      title: 'Outputs',
      subtitle: 'Kanaele',
      items: channelCards.map((item) => ({
        title: item.title,
        status: item.status,
        detail: `${item.mode} | ${item.description}`
      }))
    }
  ];

  const processingCards = [
    {
      title: 'Link Builder',
      status: 'aktiv',
      detail: 'Scrapper- und Auto-Deals werden vor dem Output sauber auf Amazon-Affiliate-Format gebracht.',
      meta: 'nur externe Links'
    },
    {
      title: 'Logik-Zentrale',
      status: learningSourceStatus?.status || 'aktiv',
      detail: 'Zentrale Pflichtschicht fuer Bewertung, Scores, Review und Freigaben.',
      meta: shortenText(autoPipeline?.detail || 'Pflichtschicht fuer Auto-Deals', 60)
    },
    {
      title: 'Muster-Erkennung',
      status: activePatternSupport ? 'aktiv' : 'deaktiviert',
      detail: 'Unterstuetzt die Entscheidung, ersetzt sie aber nicht als Blackbox.',
      meta: `${activePatternSupport}/3 Seller-Type-Profile aktiv`
    },
    {
      title: 'Review / Entscheidung',
      status: reviewCount ? 'review' : 'bereit',
      detail: 'Approved, review oder blocked bleiben nachvollziehbar als zentrale Entscheidung.',
      meta: reviewCount ? `${reviewCount} offene Reviews` : 'kein offener Review-Stau'
    }
  ];

  const monitoringFeed = publishingLogs.slice(0, 6).map((log) => ({
    id: `${log.id}-${log.created_at || log.createdAt}`,
    title: formatLogTitle(log),
    detail: shortenText(log?.message || 'Kein Log-Text vorhanden.', 92),
    at: log?.created_at || log?.createdAt,
    tone: getStatusTone(log?.level || 'info')
  }));

  const latestQueueItems = queueItems.slice(0, 5).map((item) => ({
    id: item.id,
    title: shortenText(item?.payload?.title || `Queue ${item.id}`, 56),
    status: item.status,
    detail: `${item.source_type || 'unknown'} | ${formatDateTime(item.created_at)}`
  }));

  const workspaceCards = [
    {
      title: 'Generator',
      path: '/generator',
      description: 'Manueller Deal-Workflow'
    },
    {
      title: 'Scrapper',
      path: '/scraper',
      description: 'Rohdeal- und Importbereich'
    },
    user?.role === 'admin'
      ? {
          title: 'Logik-Zentrale',
          path: '/learning',
          description: 'Bewertung, Regeln und Review'
        }
      : null,
    {
      title: 'Publishing',
      path: '/publishing',
      description: 'Queue, Worker und Kanaele'
    },
    {
      title: 'Deal Historie',
      path: '/deal-history',
      description: 'Repost-Sperre und Verlauf'
    }
  ].filter(Boolean);

  return (
    <Layout>
      <div className="ops-home">
        <section className="card ops-hero">
          <div className="ops-hero-grid">
            <div className="ops-hero-copy">
              <p className="section-title">Operations Dashboard / Control Center</p>
              <h1 className="page-title">Affiliate Manager Pro Operations Center</h1>
              <p className="page-subtitle">
                Quellen, Pflichtschichten, Entscheidungen und Kanaele werden hier als laufendes System sichtbar,
                ohne Generator, Scrapper oder Logik-Zentrale durcheinander zu mischen.
              </p>
            </div>

            <div className="ops-hero-aside">
              <div className="ops-hero-chip-row">
                <span className="badge">Quelle -&gt; Verarbeitung -&gt; Entscheidung -&gt; Output</span>
                <span className={`status-chip ${user?.role === 'admin' ? 'info' : 'success'}`}>
                  {user?.role === 'admin' ? 'Admin Control View' : 'Workspace View'}
                </span>
              </div>
              <div className="ops-quick-links">
                {workspaceCards.map((item) => (
                  <Link key={item.title} to={item.path} className="ops-quick-link">
                    <span>{item.title}</span>
                    <small>{item.description}</small>
                  </Link>
                ))}
              </div>
            </div>
          </div>

          {status && (
            <div className="ops-inline-alert">
              <span className="status-chip warning">Hinweis</span>
              <p>{status}</p>
            </div>
          )}
        </section>

        {loading ? (
          <section className="card ops-panel">
            <p style={{ margin: 0 }}>Operations-Dashboard wird geladen...</p>
          </section>
        ) : (
          <>
            <section className="ops-topbar">
              {topStatusCards.map((card) => (
                <article key={card.title} className={`card ops-status-card ops-tone-${card.tone}`}>
                  <div className="ops-card-head">
                    <p className="section-title">{card.title}</p>
                    <span className={`status-chip ${card.tone}`}>{card.tone}</span>
                  </div>
                  <h2>{card.value}</h2>
                  <p className="ops-card-copy">{card.detail}</p>
                </article>
              ))}
            </section>

            <section className="ops-core-grid">
              <article className="card ops-panel ops-flow-panel">
                <div className="ops-panel-header">
                  <div>
                    <p className="section-title">Flow / Control Center</p>
                    <h2 className="page-title">Systemkette auf einen Blick</h2>
                  </div>
                  <span className="ops-header-note">Pflichtlogik vor jedem Auto-Output</span>
                </div>

                <div className="ops-flow-grid">
                  {flowColumns.map((column) => (
                    <section key={column.title} className="ops-flow-column">
                      <div className="ops-column-head">
                        <span className="ops-column-kicker">{column.subtitle}</span>
                        <h3>{column.title}</h3>
                      </div>

                      <div className="ops-node-stack">
                        {column.items.map((item) => (
                          <article key={`${column.title}-${item.title}`} className={`ops-node ops-tone-${getStatusTone(item.status)}`}>
                            <div className="ops-node-head">
                              <strong>{item.title}</strong>
                              <span className={`status-chip ${getStatusTone(item.status)}`}>{item.status}</span>
                            </div>
                            <p>{item.detail}</p>
                          </article>
                        ))}
                      </div>
                    </section>
                  ))}
                </div>
              </article>

              <aside className="ops-monitor-stack">
                <section className="card ops-panel">
                  <div className="ops-panel-header">
                    <div>
                      <p className="section-title">Monitoring</p>
                      <h2 className="page-title">Queue und Live-Status</h2>
                    </div>
                  </div>

                  <div className="ops-queue-grid">
                    <article className="ops-mini-stat">
                      <span>Pending</span>
                      <strong>{queueSummary.waiting}</strong>
                    </article>
                    <article className="ops-mini-stat">
                      <span>Processing</span>
                      <strong>{queueSummary.processing}</strong>
                    </article>
                    <article className="ops-mini-stat">
                      <span>Posted</span>
                      <strong>{queueSummary.posted}</strong>
                    </article>
                    <article className="ops-mini-stat">
                      <span>Failed</span>
                      <strong>{queueSummary.failed}</strong>
                    </article>
                  </div>

                  <div className="ops-monitor-list">
                    <div className="ops-list-head">
                      <strong>Letzte Events</strong>
                      <small>{monitoringFeed.length ? 'live aus Publishing-Logs' : 'noch keine Logs'}</small>
                    </div>
                    <div className="ops-feed">
                      {monitoringFeed.length ? (
                        monitoringFeed.map((item) => (
                          <article key={item.id} className="ops-feed-item">
                            <div className="ops-feed-head">
                              <strong>{item.title}</strong>
                              <span className={`status-chip ${item.tone}`}>{formatDateTime(item.at)}</span>
                            </div>
                            <p>{item.detail}</p>
                          </article>
                        ))
                      ) : (
                        <p className="ops-empty-state">Noch keine Publishing-Events vorhanden.</p>
                      )}
                    </div>
                  </div>
                </section>

                <section className="card ops-panel">
                  <div className="ops-panel-header">
                    <div>
                      <p className="section-title">Live Queue</p>
                      <h2 className="page-title">Letzte Eintraege</h2>
                    </div>
                  </div>

                  <div className="ops-feed">
                    {latestQueueItems.length ? (
                      latestQueueItems.map((item) => (
                        <article key={item.id} className="ops-feed-item">
                          <div className="ops-feed-head">
                            <strong>{item.title}</strong>
                            <span className={`status-chip ${getStatusTone(item.status)}`}>{item.status}</span>
                          </div>
                          <p>{item.detail}</p>
                        </article>
                      ))
                    ) : (
                      <p className="ops-empty-state">Keine Queue-Eintraege vorhanden.</p>
                    )}
                  </div>
                </section>
              </aside>
            </section>

            <section className="card ops-panel">
              <div className="ops-panel-header">
                <div>
                  <p className="section-title">Quellen</p>
                  <h2 className="page-title">Eingaenge und letzte Aktivitaet</h2>
                </div>
                <span className="ops-header-note">Keepa, Amazon API, Scrapper und Generator getrennt sichtbar</span>
              </div>

              <div className="ops-source-grid">
                {sourceCards.map((item) => (
                  <article key={item.id} className={`ops-source-card ops-tone-${getStatusTone(item.status)}`}>
                    <div className="ops-card-top">
                      <div>
                        <span className="ops-card-label">{item.title}</span>
                        <h3>{item.description}</h3>
                      </div>
                      <span className={`status-chip ${getStatusTone(item.status)}`}>{item.status}</span>
                    </div>

                    <div className="ops-meta-list">
                      <p>Letzte Aktion: {formatDateTime(item.lastAction)}</p>
                      <p>{item.primaryMetric}</p>
                      <p>{item.secondaryMetric}</p>
                    </div>
                  </article>
                ))}
              </div>
            </section>

            <section className="card ops-panel">
              <div className="ops-panel-header">
                <div>
                  <p className="section-title">Verarbeitung</p>
                  <h2 className="page-title">Logik, Muster-Erkennung und Review</h2>
                </div>
                <span className="ops-header-note">Muster-Unterstuetzung bleibt von Auto-Posting getrennt</span>
              </div>

              <div className="ops-processing-grid">
                {processingCards.map((item) => (
                  <article key={item.title} className={`ops-processing-card ops-tone-${getStatusTone(item.status)}`}>
                    <div className="ops-card-top">
                      <div>
                        <span className="ops-card-label">{item.title}</span>
                        <h3>{item.meta}</h3>
                      </div>
                      <span className={`status-chip ${getStatusTone(item.status)}`}>{item.status}</span>
                    </div>
                    <p className="ops-card-copy">{item.detail}</p>
                  </article>
                ))}
              </div>

              <div className="ops-rule-grid">
                {sellerControls.map((item) => (
                  <article key={item.id} className="ops-rule-card">
                    <div className="ops-feed-head">
                      <strong>{item.id}</strong>
                      <span className={`status-chip ${item.active ? 'success' : 'warning'}`}>
                        {item.active ? 'aktiv' : 'inaktiv'}
                      </span>
                    </div>
                    <div className="ops-rule-chips">
                      <span className={`status-chip ${item.patternSupportEnabled ? 'info' : 'warning'}`}>
                        Muster {item.patternSupportEnabled ? 'an' : 'aus'}
                      </span>
                      <span className={`status-chip ${item.autoPostingEnabled ? 'success' : 'warning'}`}>
                        Auto {item.autoPostingEnabled ? 'an' : 'aus'}
                      </span>
                    </div>
                    <p className="ops-card-copy">
                      Letzte Entscheidung: {item.lastDecision || '-'} | Letzter Lauf: {formatDateTime(item.lastRunAt)}
                    </p>
                  </article>
                ))}
              </div>
            </section>

            <section className="card ops-panel">
              <div className="ops-panel-header">
                <div>
                  <p className="section-title">Kanaele</p>
                  <h2 className="page-title">Telegram, WhatsApp und Facebook</h2>
                </div>
                <span className="ops-header-note">Keine Fake-Connected-States, nur echte Aktiv-, Warn- oder Prepared-Zustaende</span>
              </div>

              <div className="ops-channel-grid">
                {channelCards.map((item) => (
                  <article key={item.id} className={`ops-channel-card ops-tone-${getStatusTone(item.status)}`}>
                    <div className="ops-card-top">
                      <div>
                        <span className="ops-card-label">{item.title}</span>
                        <h3>{item.mode}</h3>
                      </div>
                      <span className={`status-chip ${getStatusTone(item.status)}`}>{item.status}</span>
                    </div>

                    <p className="ops-card-copy">{item.description}</p>

                    <div className="ops-channel-stats">
                      <span>Pending {toNumber(item.metrics.waiting)}</span>
                      <span>Processing {toNumber(item.metrics.processing)}</span>
                      <span>Posted {toNumber(item.metrics.posted)}</span>
                      <span>Failed {toNumber(item.metrics.failed)}</span>
                    </div>

                    <div className="ops-meta-list">
                      <p>
                        Letzter Versand:{' '}
                        {item.lastSuccess ? formatDateTime(item.lastSuccess.created_at || item.lastSuccess.createdAt) : '-'}
                      </p>
                      <p>
                        Letzte Warnung:{' '}
                        {item.lastIssue ? formatDateTime(item.lastIssue.created_at || item.lastIssue.createdAt) : '-'}
                      </p>
                    </div>
                  </article>
                ))}
              </div>
            </section>

            <section className="card ops-panel">
              <div className="ops-panel-header">
                <div>
                  <p className="section-title">Orientierung</p>
                  <h2 className="page-title">Arbeitsbereiche und kurze Einordnung</h2>
                </div>
              </div>

              <div className="ops-workspace-grid">
                {workspaceCards.map((item) => (
                  <Link key={item.title} to={item.path} className="ops-workspace-card">
                    <div className="ops-card-top">
                      <div>
                        <span className="ops-card-label">{item.title}</span>
                        <h3>{item.description}</h3>
                      </div>
                      <span className="status-chip info">oeffnen</span>
                    </div>
                    <p className="ops-card-copy">
                      {item.title === 'Generator' && 'Manueller Posting-Bereich fuer direkte Deals.'}
                      {item.title === 'Scrapper' && 'Rohdeal-Eingang fuer Quellen und Imports.'}
                      {item.title === 'Logik-Zentrale' && 'Bewertung, Muster-Erkennung, Regeln und Entscheidungen.'}
                      {item.title === 'Publishing' && 'Kanaele, Queue, Worker und Versandstatus.'}
                      {item.title === 'Deal Historie' && 'Verlauf, Repost-Sperre und Nachvollziehbarkeit.'}
                    </p>
                  </Link>
                ))}
              </div>
            </section>
          </>
        )}
      </div>
    </Layout>
  );
}

export default HomePage;
