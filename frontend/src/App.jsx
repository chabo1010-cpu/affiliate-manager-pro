import { useEffect, useState } from 'react';
import { Navigate, Route, Routes } from 'react-router-dom';
import { useAuth } from './context/AuthContext';
import ProtectedRoute from './routes/ProtectedRoute';
import LoginPage from './pages/Login';
import HomePage from './pages/Home';
import GeneratorPosterPage from './pages/GeneratorPoster';
import TemplatesPage from './pages/Templates';
import ScraperPage from './pages/Scraper';
import AutobotPage from './pages/Autobot';
import LogsPage from './pages/Logs';
import SettingsPage from './pages/Settings';
import DealHistoryAdminPage from './pages/DealHistoryAdmin';
import CopybotPage from './pages/Copybot';
import PublishingPage from './pages/Publishing';
import KeepaPage from './pages/Keepa';
import DealEnginePage from './pages/DealEngine';
import AdvertisingPage from './pages/Advertising';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:4000';

const productIntelligenceCategories = [
  'Powerbank',
  'Kopfhörer',
  'USB-C Hub',
  'Ladegerät',
  'Kabel',
  'Kleidung'
];

function ProductIntelligencePage() {
  const { user } = useAuth();
  const [channelStatus, setChannelStatus] = useState({
    enabled: false,
    finalTarget: '',
    lastSkipReason: '',
    lastTestAt: '',
    lastTestStatus: '',
    lastTestMessageId: null,
    lastOptimizedDeal: null,
    lastOriginalSourceGroup: '',
    lastComparisonPrice: ''
  });
  const [testState, setTestState] = useState({ loading: false, message: '', ok: null });
  const flowSteps = ['Deal', 'Analyse', 'Vergleich', 'Preisfehler-Schutz', 'Entscheidung'];
  const ruleCards = [
    {
      title: 'Powerbank',
      text: '10000 mAh bis 10 EUR, 20000 mAh bis 16 EUR, 30000 mAh zuerst REVIEW.'
    },
    {
      title: 'Kopfhörer',
      text: 'Unbekannte China-Marke über 25 EUR wird REVIEW oder BLOCK.'
    },
    {
      title: 'Similarity Score',
      text: 'Kategorie +30, Kapazität/Leistung +30, Marke +20, Ausstattung +15, FBA/Amazon +10. Ab 70 zählt ähnlich.'
    }
  ];
  const latestChecks = [
    '[PRODUCT_INTELLIGENCE_START]',
    '[PRODUCT_CATEGORY_DETECTED]',
    '[SIMILAR_PRODUCT_MATCH_SCORE]',
    '[BASELINE_MASTER_UPDATED]',
    '[BASELINE_PRICE_ERROR_PROTECTED]'
  ];
  const cardStyle = {
    minWidth: 0,
    minHeight: '190px',
    display: 'grid',
    alignContent: 'start',
    gap: '0.65rem',
    padding: '1.25rem',
    overflow: 'hidden'
  };
  const statusCardStyle = {
    minWidth: 0,
    display: 'grid',
    gap: '0.35rem',
    alignContent: 'start',
    minHeight: '110px'
  };
  const lastOptimizedDeal = channelStatus.lastOptimizedDeal || {};

  async function loadOptimizedChannelStatus() {
    try {
      const response = await fetch(`${API_BASE_URL}/api/product-intelligence/optimized-channel-status`, {
        headers: {
          'Content-Type': 'application/json',
          'X-User-Role': user?.role || ''
        }
      });
      const data = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(data?.error || 'Status konnte nicht geladen werden.');
      }
      setChannelStatus(data);
    } catch (error) {
      setChannelStatus((current) => ({
        ...current,
        lastSkipReason: error instanceof Error ? error.message : 'Status konnte nicht geladen werden.'
      }));
    }
  }

  async function testOptimizedChannel() {
    setTestState({ loading: true, message: 'Testnachricht wird gesendet...', ok: null });
    try {
      const response = await fetch(`${API_BASE_URL}/api/product-intelligence/test-optimized-channel`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-User-Role': user?.role || ''
        }
      });
      const data = await response.json().catch(() => ({}));
      if (!response.ok || data?.success !== true) {
        throw new Error(data?.reason || data?.error || 'Testnachricht fehlgeschlagen.');
      }
      setChannelStatus(data.status || data);
      setTestState({
        loading: false,
        message: `✅ Testnachricht gesendet${data.messageId ? ` · Message ${data.messageId}` : ''}`,
        ok: true
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Testnachricht fehlgeschlagen.';
      setTestState({ loading: false, message: `❌ Fehler: ${message}`, ok: false });
      setChannelStatus((current) => ({ ...current, lastSkipReason: message, lastTestStatus: 'failed' }));
    }
  }

  useEffect(() => {
    console.info('[PRODUCT_INTELLIGENCE_UI_RENDERED]', {
      layout: 'simple_cards',
      cards: ['Ablauf', 'Kategorien', 'Masterpreise', 'Letzte Prüfungen']
    });
    console.info('[PRODUCT_INTELLIGENCE_SAFE_EMPTY_STATE]', {
      masterPrices: 'Noch keine Daten',
      latestChecks: latestChecks.length
    });
    void loadOptimizedChannelStatus();
  }, [user?.role]);

  return (
    <main className="page" style={{ maxWidth: '1200px', margin: '0 auto', width: '100%', overflowX: 'hidden' }}>
      <section className="card" style={{ display: 'grid', gap: '1rem' }}>
        <div style={{ display: 'grid', gap: '0.4rem' }}>
          <p className="section-title">Produkt-Intelligenz</p>
          <h1 className="page-title">📦 Produkt-Intelligenz</h1>
          <p className="page-subtitle">
            Hier siehst du, wie das System ähnliche Produkte sucht, Masterpreise merkt und Preisfehler schützt.
          </p>
        </div>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.75rem', alignItems: 'center' }}>
          <button
            type="button"
            onClick={testOptimizedChannel}
            disabled={testState.loading}
            className="primary"
            style={{
              cursor: testState.loading ? 'wait' : 'pointer',
              minHeight: '46px',
              padding: '0.85rem 1.15rem',
              borderRadius: '14px',
              fontSize: '1rem',
              fontWeight: 800
            }}
          >
            {testState.loading ? 'Teste Optimierte Deals...' : '🧪 Optimierte Deals testen'}
          </button>
          <button
            type="button"
            onClick={loadOptimizedChannelStatus}
            className="secondary"
            style={{
              cursor: 'pointer',
              minHeight: '46px',
              padding: '0.85rem 1.15rem',
              borderRadius: '14px',
              fontSize: '1rem',
              fontWeight: 800
            }}
          >
            🔄 Letzte Prüfung neu laden
          </button>
          {testState.message && (
            <span className={`status-chip ${testState.ok === true ? 'success' : testState.ok === false ? 'danger' : 'info'}`}>
              {testState.message}
            </span>
          )}
        </div>
      </section>

      <section
        style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))',
          gap: '1rem',
          marginTop: '1rem',
          alignItems: 'stretch'
        }}
      >
        <article className="card" style={cardStyle}>
          <p className="section-title">1. Ablauf</p>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.5rem', fontSize: '1rem', lineHeight: 1.5 }}>
            {flowSteps.map((step, index) => (
              <span key={step}>
                <strong>{step}</strong>
                {index < flowSteps.length - 1 ? ' →' : ''}
              </span>
            ))}
          </div>
        </article>

        <article className="card" style={cardStyle}>
          <p className="section-title">2. Kategorien</p>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.5rem' }}>
            {productIntelligenceCategories.map((category) => (
              <span key={category} className="status-pill active">
                {category}
              </span>
            ))}
          </div>
        </article>

        <article className="card" style={cardStyle}>
          <p className="section-title">3. Masterpreise</p>
          <h3 style={{ marginTop: 0 }}>Noch keine Daten</h3>
          <p style={{ marginBottom: 0 }}>
            Sobald Deals geprüft werden, landen die günstigsten sicheren FBA/Amazon Vergleichspreise in der Masterdatenbank.
          </p>
        </article>

        <article className="card" style={cardStyle}>
          <p className="section-title">4. Letzte Prüfungen</p>
          <ul style={{ margin: 0, paddingLeft: '1.1rem', display: 'grid', gap: '0.35rem' }}>
            {latestChecks.map((entry) => (
              <li key={entry}>{entry}</li>
            ))}
          </ul>
        </article>
      </section>

      <section className="card" style={{ marginTop: '1rem', display: 'grid', gap: '0.75rem', minWidth: 0 }}>
        <p className="section-title">📡 Optimierte Deals Kanal</p>
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fit, minmax(190px, 1fr))',
            gap: '0.75rem'
          }}
        >
          <div className="metric-card" style={statusCardStyle}>
            <strong>Status</strong>
            <p>{channelStatus.enabled ? 'Aktiv' : 'Inaktiv'}</p>
          </div>
          <div className="metric-card" style={statusCardStyle}>
            <strong>Ziel</strong>
            <p>{channelStatus.resolvedChatId || channelStatus.finalTarget || 'Keine Daten'}</p>
          </div>
          <div className="metric-card" style={statusCardStyle}>
            <strong>Letzter Test</strong>
            <p>{channelStatus.lastTestAt || 'Noch nicht getestet'}</p>
          </div>
          <div className="metric-card" style={statusCardStyle}>
            <strong>Letzter Skip-Grund</strong>
            <p>{channelStatus.lastSkipReason || 'Keiner'}</p>
          </div>
          <div className="metric-card" style={statusCardStyle}>
            <strong>Letzter optimierter Deal</strong>
            <p>{lastOptimizedDeal.optimizedTitle || 'Keine Daten'}</p>
          </div>
          <div className="metric-card" style={statusCardStyle}>
            <strong>Letzter Ursprungskanal</strong>
            <p>{channelStatus.lastOriginalSourceGroup || lastOptimizedDeal.originalSourceGroup || 'Keine Daten'}</p>
          </div>
          <div className="metric-card" style={statusCardStyle}>
            <strong>Letzter Vergleichspreis</strong>
            <p>{channelStatus.lastComparisonPrice || lastOptimizedDeal.optimizedPrice || 'Keine Daten'}</p>
          </div>
        </div>
      </section>

      <section className="card" style={{ marginTop: '1rem', display: 'grid', gap: '0.75rem', minWidth: 0 }}>
        <p className="section-title">Regeln einfach erklärt</p>
        {ruleCards.map((card) => (
          <details key={card.title} className="card" style={{ background: 'rgba(255,255,255,0.03)', padding: 0, overflow: 'hidden' }}>
            <summary
              style={{
                cursor: 'pointer',
                fontWeight: 800,
                padding: '1rem',
                borderRadius: '16px',
                outline: 'none',
                minHeight: '48px'
              }}
            >
              {card.title}
            </summary>
            <div style={{ padding: '0 1rem 1rem' }}>
            <p style={{ marginBottom: 0 }}>{card.text}</p>
            </div>
          </details>
        ))}
        <details className="card" style={{ background: 'rgba(255,255,255,0.03)', padding: 0, overflow: 'hidden' }}>
          <summary
            style={{
              cursor: 'pointer',
              fontWeight: 800,
              padding: '1rem',
              borderRadius: '16px',
              minHeight: '48px'
            }}
          >
            Preisfehler-Schutz
          </summary>
          <div style={{ padding: '0 1rem 1rem' }}>
          <p style={{ marginBottom: 0 }}>
            Ein neuer Fund, der mindestens 50% unter dem aktuellen Masterpreis liegt, wird markiert und nicht als neuer Master gespeichert.
          </p>
          </div>
        </details>
      </section>
    </main>
  );
}

function App() {
  const { user } = useAuth();
  const learningPageElement = user?.role === 'admin' ? <KeepaPage /> : <Navigate to="/generator" replace />;

  useEffect(() => {
    console.info('SYSTEM SPEC APPLIED', {
      sections: ['Dashboard', 'Quellen', 'Regler', 'Output', 'Telegram Login', 'Sperrmodul', 'Queue']
    });
    console.info('PRIMARY DECISION SOURCE UPDATED', {
      primary: 'internetvergleich',
      fallback: 'keepa',
      aiMode: 'optional_unsicherheitsfall'
    });
    console.info('NAVIGATION RESTORED', {
      preservedMenus: [
        'Dashboard',
        'Generator',
        'Scrapper',
        'Copybot',
        'Templates',
        'Autobot',
        'Werbung',
        'Logik-Zentrale',
        'Produkt-Intelligenz',
        'Publishing',
        'Sperrzeiten',
        'Logs',
        'Einstellungen'
      ]
    });
  }, []);

  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />
      <Route
        path="/*"
        element={
          <ProtectedRoute>
            <Routes>
              <Route index element={<HomePage />} />
              <Route path="dashboard" element={<HomePage />} />
              <Route path="generator" element={<GeneratorPosterPage />} />
              <Route path="templates" element={<TemplatesPage />} />
              <Route path="scraper" element={<ScraperPage />} />
              <Route path="scrapper" element={<Navigate to="/scraper" replace />} />
              <Route path="autobot" element={<AutobotPage />} />
              <Route path="copybot" element={<CopybotPage />} />
              <Route path="copybot/telegram-sources" element={<CopybotPage />} />
              <Route path="copybot/whatsapp-sources" element={<CopybotPage />} />
              <Route path="copybot/pricing-rules" element={<CopybotPage />} />
              <Route path="copybot/sampling" element={<CopybotPage />} />
              <Route path="copybot/review" element={<CopybotPage />} />
              <Route path="copybot/logs" element={<CopybotPage />} />
              <Route path="publishing" element={<PublishingPage />} />
              <Route path="deal-engine" element={<DealEnginePage />} />
              <Route path="product-intelligence" element={<ProductIntelligencePage />} />
              <Route path="advertising" element={<AdvertisingPage />} />
              <Route path="publishing/workers" element={<PublishingPage />} />
              <Route path="publishing/telegram" element={<PublishingPage />} />
              <Route path="publishing/whatsapp" element={<PublishingPage />} />
              <Route path="publishing/facebook" element={<PublishingPage />} />
              <Route path="publishing/logs" element={<PublishingPage />} />
              <Route path="keepa" element={learningPageElement} />
              <Route path="keepa/manual-search" element={learningPageElement} />
              <Route path="keepa/automatik" element={learningPageElement} />
              <Route path="keepa/ergebnisse" element={learningPageElement} />
              <Route path="keepa/benachrichtigungen" element={learningPageElement} />
              <Route path="keepa/einstellungen" element={learningPageElement} />
              <Route path="keepa/verbrauch-logs" element={learningPageElement} />
              <Route path="keepa/fake-drop-analyse" element={learningPageElement} />
              <Route path="keepa/review-queue" element={learningPageElement} />
              <Route path="keepa/lern-datenbank" element={learningPageElement} />
              <Route path="learning" element={learningPageElement} />
              <Route path="learning/manual-search" element={learningPageElement} />
              <Route path="learning/automatik" element={learningPageElement} />
              <Route path="learning/ergebnisse" element={learningPageElement} />
              <Route path="learning/benachrichtigungen" element={learningPageElement} />
              <Route path="learning/einstellungen" element={learningPageElement} />
              <Route path="learning/verbrauch-logs" element={learningPageElement} />
              <Route path="learning/fake-drop-analyse" element={learningPageElement} />
              <Route path="learning/review-queue" element={learningPageElement} />
              <Route path="learning/lern-datenbank" element={learningPageElement} />
              <Route path="logic" element={<Navigate to="/learning" replace />} />
              <Route path="sperrzeiten" element={<DealHistoryAdminPage />} />
              <Route path="deal-history" element={<Navigate to="/sperrzeiten" replace />} />
              <Route path="logs" element={<LogsPage />} />
              <Route path="settings" element={<SettingsPage />} />
              <Route path="*" element={<Navigate to="/" replace />} />
            </Routes>
          </ProtectedRoute>
        }
      />
      <Route path="*" element={<Navigate to={user ? '/' : '/login'} replace />} />
    </Routes>
  );
}

export default App;
