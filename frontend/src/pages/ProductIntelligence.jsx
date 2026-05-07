import { useEffect, useMemo, useState } from 'react';
import Layout from '../components/layout/Layout';
import { useAuth } from '../context/AuthContext';
import './ProductIntelligence.css';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:4000';
const PRODUCT_INTELLIGENCE_TIMEOUT_MS = 1800;

const tabs = [
  { id: 'overview', label: 'Uebersicht' },
  { id: 'categories', label: 'Kategorien' },
  { id: 'master', label: 'Masterpreise' },
  { id: 'variants', label: 'Varianten' },
  { id: 'logs', label: 'Logs' },
  { id: 'experts', label: 'Expertenmodus' }
];

const categories = ['Powerbank', 'Kopfhoerer', 'USB-C Hub', 'Ladegeraet', 'Kabel', 'Kleidung'];
const masterRules = [
  { title: 'Powerbank 10000 mAh', detail: 'Bis 11 EUR, darueber blockiert die Produktregel.' },
  { title: 'Powerbank 20000-30000 mAh', detail: 'Bis 16 EUR, groessere Modelle nur mit Abstand.' },
  { title: 'China Kopfhoerer', detail: 'Nur bei gutem Rating und in klaren Preisgrenzen.' }
];
const latestChecks = [
  '[PRODUCT_INTELLIGENCE_START]',
  '[PRODUCT_CATEGORY_DETECTED]',
  '[SIMILAR_PRODUCT_MATCH_SCORE]',
  '[BASELINE_MASTER_UPDATED]',
  '[BASELINE_PRICE_ERROR_PROTECTED]'
];

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

function ProductIntelligencePage() {
  const { user } = useAuth();
  const [channelStatus, setChannelStatus] = useState({
    enabled: false,
    finalTarget: '',
    resolvedChatId: '',
    lastSkipReason: '',
    lastTestAt: '',
    lastTestStatus: '',
    lastTestMessageId: null,
    lastOptimizedDeal: null,
    lastOriginalSourceGroup: '',
    lastComparisonPrice: ''
  });
  const [activeTab, setActiveTab] = useState('overview');
  const [loading, setLoading] = useState(true);
  const [status, setStatus] = useState('');
  const [testState, setTestState] = useState({ loading: false, message: '', ok: null });

  async function apiFetch(path, options = {}, timeoutMs = PRODUCT_INTELLIGENCE_TIMEOUT_MS) {
    const controller = new AbortController();
    const timeoutId = window.setTimeout(() => controller.abort(), timeoutMs);

    try {
      const response = await fetch(`${API_BASE_URL}${path}`, {
        ...options,
        signal: controller.signal,
        credentials: 'include',
        headers: {
          'Content-Type': 'application/json',
          'X-User-Role': user?.role || '',
          ...(options.headers || {})
        }
      });
      const data = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(data?.error || `Request fehlgeschlagen (${response.status}).`);
      }
      return data;
    } catch (error) {
      if (error?.name === 'AbortError') {
        console.warn('[PRODUCT_INTELLIGENCE_LOAD_TIMEOUT]', {
          path,
          timeoutMs
        });
        throw new Error(`Timeout nach ${timeoutMs}ms: ${path}`);
      }
      throw error;
    } finally {
      window.clearTimeout(timeoutId);
    }
  }

  async function loadOptimizedChannelStatus() {
    const startedAt = performance.now();
    console.info('[PRODUCT_INTELLIGENCE_LOAD_START]', {
      path: '/api/product-intelligence/optimized-channel-status'
    });
    setLoading(true);

    try {
      const data = await apiFetch('/api/product-intelligence/optimized-channel-status');
      setChannelStatus(data);
      setStatus('');
      console.info('[PRODUCT_INTELLIGENCE_LOAD_DONE]', {
        durationMs: Math.round(performance.now() - startedAt),
        enabled: data?.enabled === true,
        lastTestStatus: data?.lastTestStatus || ''
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Status konnte nicht geladen werden.';
      setStatus(message);
      console.error('[PRODUCT_INTELLIGENCE_LOAD_ERROR]', {
        errorMessage: message
      });
    } finally {
      setLoading(false);
    }
  }

  async function testOptimizedChannel() {
    setTestState({ loading: true, message: 'Testnachricht wird gesendet...', ok: null });
    try {
      const data = await apiFetch(
        '/api/product-intelligence/test-optimized-channel',
        {
          method: 'POST'
        },
        3000
      );
      setChannelStatus(data.status || data);
      setTestState({
        loading: false,
        message: `Testnachricht gesendet${data.messageId ? ` | Message ${data.messageId}` : ''}`,
        ok: true
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Testnachricht fehlgeschlagen.';
      setTestState({ loading: false, message, ok: false });
      setStatus(message);
    }
  }

  useEffect(() => {
    void loadOptimizedChannelStatus();
  }, [user?.role]);

  const summaryCards = useMemo(
    () => [
      {
        title: 'Optimierte Deals',
        value: channelStatus.enabled ? 'Aktiv' : 'Inaktiv',
        detail: channelStatus.resolvedChatId || channelStatus.finalTarget || 'Kein Ziel aufgeloest'
      },
      {
        title: 'Letzter Test',
        value: formatDateTime(channelStatus.lastTestAt),
        detail: channelStatus.lastTestStatus || 'Noch kein Test'
      },
      {
        title: 'Letzte Quelle',
        value: channelStatus.lastOriginalSourceGroup || channelStatus.lastOptimizedDeal?.originalSourceGroup || '-',
        detail: 'Ursprungskanal des letzten optimierten Deals'
      },
      {
        title: 'Vergleichspreis',
        value: channelStatus.lastComparisonPrice || channelStatus.lastOptimizedDeal?.optimizedPrice || '-',
        detail: channelStatus.lastSkipReason || 'Keine aktuellen Sperrgruende'
      }
    ],
    [channelStatus]
  );

  const latestLogCards = useMemo(
    () => [
      {
        title: 'Letzter optimierter Deal',
        detail: channelStatus.lastOptimizedDeal?.optimizedTitle || 'Keine Daten vorhanden'
      },
      {
        title: 'Letzte beste Variante',
        detail: channelStatus.lastOptimizedDeal?.variantLabel || 'Keine Daten vorhanden'
      },
      {
        title: 'Letzte Sperrnotiz',
        detail: channelStatus.lastSkipReason || 'Keine Daten vorhanden'
      }
    ],
    [channelStatus]
  );
  const statusStripCards = useMemo(
    () => [
      {
        title: 'Optimierte Deals',
        value: channelStatus.enabled ? 'Aktiv' : 'Inaktiv',
        detail: channelStatus.finalTarget || channelStatus.resolvedChatId || 'Kein Kanal hinterlegt'
      },
      {
        title: 'Letzter optimierter Deal',
        value: channelStatus.lastOptimizedDeal?.optimizedPrice || '-',
        detail: channelStatus.lastOptimizedDeal?.optimizedTitle || 'Keine Daten vorhanden'
      },
      {
        title: 'Letzte Variantenpruefung',
        value: channelStatus.lastOptimizedDeal?.variantLabel || 'Keine Daten',
        detail: channelStatus.lastSkipReason || 'Keine aktuelle Sperrnotiz'
      }
    ],
    [channelStatus]
  );

  function renderOverview() {
    return (
      <div className="pi-stack">
        <section className="card pi-panel pi-panel-compact">
          <div className="pi-panel-header">
            <div>
              <p className="section-title">Status</p>
              <h2 className="page-title">Optimierte Deals und letzter Test</h2>
            </div>
            <span className="pi-note">Kanalstatus und letzte Aktivitaet</span>
          </div>
          <div className="pi-grid">
            {summaryCards.map((card) => (
              <article key={card.title} className="pi-card">
                <strong>{card.title}</strong>
                <h3>{card.value}</h3>
                <p>{card.detail}</p>
              </article>
            ))}
          </div>
        </section>

        <section className="card pi-panel pi-panel-compact">
          <div className="pi-panel-header">
            <div>
              <p className="section-title">Schnellzugriff</p>
              <h2 className="page-title">Pruefen, testen und bewerten</h2>
            </div>
          </div>
          <div className="pi-actions">
            <button type="button" className="primary" disabled={testState.loading} onClick={() => void testOptimizedChannel()}>
              {testState.loading ? 'Teste Optimierte Deals...' : 'Optimierte Deals testen'}
            </button>
            <button type="button" className="secondary" onClick={() => void loadOptimizedChannelStatus()}>
              Letzte Pruefung neu laden
            </button>
          </div>
          {testState.message ? (
            <span className={`status-chip ${testState.ok === true ? 'success' : testState.ok === false ? 'danger' : 'info'}`}>
              {testState.message}
            </span>
          ) : null}
        </section>
      </div>
    );
  }

  function renderMasterPrices() {
    return (
      <section className="card pi-panel">
        <div className="pi-panel-header">
          <div>
            <p className="section-title">Masterpreise</p>
            <h2 className="page-title">Sichere Preisanker und Preisfehler-Schutz</h2>
          </div>
        </div>
        <div className="pi-grid">
          {masterRules.map((item) => (
            <article key={item.title} className="pi-card">
              <strong>{item.title}</strong>
              <p>{item.detail}</p>
            </article>
          ))}
          <article className="pi-card">
            <strong>Preisfehler-Schutz</strong>
            <p>Neue Funde, die massiv unter dem aktuellen Master liegen, werden nicht blind als neue Baseline gespeichert.</p>
          </article>
        </div>
      </section>
    );
  }

  function renderCategories() {
    return (
      <section className="card pi-panel">
        <div className="pi-panel-header">
          <div>
            <p className="section-title">Kategorien</p>
            <h2 className="page-title">Produktfamilien und Regelcluster</h2>
          </div>
        </div>
        <div className="pi-chip-row">
          {categories.map((category) => (
            <span key={category} className="status-chip info">
              {category}
            </span>
          ))}
        </div>
      </section>
    );
  }

  function renderVariants() {
    return (
      <section className="card pi-panel">
        <div className="pi-panel-header">
          <div>
            <p className="section-title">Varianten</p>
            <h2 className="page-title">Varianten, Vergleich und Schutzschichten</h2>
          </div>
        </div>
        <div className="pi-feed">
          {latestChecks.map((item) => (
            <article key={item} className="pi-feed-item">
              <strong>{item}</strong>
            </article>
          ))}
        </div>
      </section>
    );
  }

  function renderLogs() {
    return (
      <section className="card pi-panel">
        <div className="pi-panel-header">
          <div>
            <p className="section-title">Logs</p>
            <h2 className="page-title">Letzte Ergebnisse und Hinweise</h2>
          </div>
        </div>
        <div className="pi-feed">
          {latestLogCards.map((item) => (
            <article key={item.title} className="pi-feed-item">
              <div className="pi-panel-header">
                <strong>{item.title}</strong>
              </div>
              <p>{item.detail}</p>
            </article>
          ))}
        </div>
      </section>
    );
  }

  function renderExperts() {
    return (
      <section className="card pi-panel">
        <div className="pi-panel-header">
          <div>
            <p className="section-title">Expertenmodus</p>
            <h2 className="page-title">Tieferer Kontext ohne die Standardansicht zu ueberladen</h2>
          </div>
        </div>
        <details className="pi-detail">
          <summary>Kanal-Details</summary>
          <pre className="pi-code">{JSON.stringify(channelStatus, null, 2)}</pre>
        </details>
      </section>
    );
  }

  function renderActiveTab() {
    if (activeTab === 'master') {
      return renderMasterPrices();
    }
    if (activeTab === 'categories') {
      return renderCategories();
    }
    if (activeTab === 'variants') {
      return renderVariants();
    }
    if (activeTab === 'logs') {
      return renderLogs();
    }
    if (activeTab === 'experts') {
      return renderExperts();
    }
    return renderOverview();
  }

  return (
    <Layout>
      <div className="pi-page">
        <section className="card pi-hero">
          <div className="pi-hero-top">
            <div className="pi-panel-header">
              <div>
                <p className="section-title">Produkt-Intelligenz</p>
                <h1 className="page-title">Optimierte Deals, Masterpreise und letzte Pruefungen in einer klaren Ansicht</h1>
                <p className="page-subtitle">
                  Diese Seite ist jetzt als kompakte Steuerzentrale aufgebaut: Status oben, Bereiche in Tabs und tieferer
                  Kontext nur noch im Expertenmodus.
                </p>
              </div>
            </div>
            <div className="pi-hero-side">
              <span className="badge">Aehnlichkeit, Preisanker, Varianten</span>
              <span className={`status-chip ${channelStatus.enabled ? 'success' : 'warning'}`}>
                Optimierte Deals {channelStatus.enabled ? 'aktiv' : 'pausiert'}
              </span>
              <span className="status-chip info">Letzter Test {channelStatus.lastTestStatus || 'offen'}</span>
            </div>
          </div>
        </section>

        {!loading ? (
          <section className="card pi-panel pi-panel-compact">
            <div className="pi-live-grid">
              {statusStripCards.map((card) => (
                <article key={card.title} className="pi-card pi-card-status">
                  <strong>{card.title}</strong>
                  <h3>{card.value}</h3>
                  <p>{card.detail}</p>
                </article>
              ))}
            </div>
          </section>
        ) : null}

        {status ? (
          <section className="card pi-panel pi-panel-compact">
            <p className="pi-empty">{status}</p>
          </section>
        ) : null}

        {loading ? (
          <section className="card pi-panel">
            <p style={{ margin: 0 }}>Produkt-Intelligenz wird geladen...</p>
          </section>
        ) : (
          <>
            <section className="card pi-panel pi-panel-compact">
              <div className="pi-tab-bar" role="tablist" aria-label="Produkt-Intelligenz Bereiche">
                {tabs.map((tab) => (
                  <button
                    key={tab.id}
                    type="button"
                    role="tab"
                    aria-selected={activeTab === tab.id}
                    className={`pi-tab ${activeTab === tab.id ? 'active' : ''}`}
                    onClick={() => setActiveTab(tab.id)}
                  >
                    {tab.label}
                  </button>
                ))}
              </div>
            </section>

            {renderActiveTab()}
          </>
        )}
      </div>
    </Layout>
  );
}

export default ProductIntelligencePage;
