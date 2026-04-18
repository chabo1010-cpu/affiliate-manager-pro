import { useEffect, useMemo, useState } from 'react';
import Layout from '../components/layout/Layout';
import { useAuth } from '../context/AuthContext';
import { scraperItems } from '../data/mock';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:4000';

function ScraperPage() {
  const { user } = useAuth();
  const [status, setStatus] = useState('');
  const [loading, setLoading] = useState(true);
  const [learningOverview, setLearningOverview] = useState(null);

  useEffect(() => {
    let cancelled = false;

    async function loadLearningOverview() {
      try {
        setLoading(true);
        const response = await fetch(`${API_BASE_URL}/api/learning/overview`, {
          headers: {
            'Content-Type': 'application/json',
            'X-User-Role': user?.role || ''
          }
        });
        const data = await response.json().catch(() => ({}));

        if (!response.ok) {
          throw new Error(data?.error || `Lern-Logik konnte nicht geladen werden (${response.status}).`);
        }

        if (!cancelled) {
          setLearningOverview(data);
        }
      } catch (error) {
        if (!cancelled) {
          setStatus(error instanceof Error ? error.message : 'Lern-Logik-Uebersicht konnte nicht geladen werden.');
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    void loadLearningOverview();

    return () => {
      cancelled = true;
    };
  }, [user?.role]);

  const scrapperPipeline = useMemo(
    () => learningOverview?.pipeline?.find((item) => item.id === 'scrapper') || null,
    [learningOverview]
  );

  const sellerTypeSummary = useMemo(() => {
    if (!Array.isArray(learningOverview?.sellerTypes)) {
      return [];
    }

    return learningOverview.sellerTypes;
  }, [learningOverview]);

  return (
    <Layout>
      <div className="dashboard-shell">
        <section className="card dashboard-hero">
          <div className="dashboard-hero-top">
            <div className="dashboard-hero-copy">
              <p className="section-title">Scrapper / Rohdeal-Bereich</p>
              <h2 className="page-title">Rohdeals bleiben ein eigener Eingang</h2>
              <p className="page-subtitle">Quelle hier, Bewertung danach zentral in der Logik-Zentrale.</p>
            </div>
            <div className="dashboard-hero-chips">
              <span className="badge">Quelle -&gt; Pflichtpruefung -&gt; Review / Output</span>
              <span className={`status-chip ${scrapperPipeline?.connected ? 'success' : 'warning'}`}>
                {scrapperPipeline?.integrationMode || 'Pflichtpruefung'}
              </span>
            </div>
          </div>
        </section>

        {status && (
          <section className="card dashboard-section">
            <p style={{ margin: 0 }}>{status}</p>
          </section>
        )}

        <div className="dashboard-compact-grid">
          <section className="card dashboard-compact-card">
            <p className="section-title">Quelle</p>
            <h3 style={{ margin: 0 }}>Scrapper / Rohdeal-Eingang</h3>
            <p className="dashboard-meta-line">Import und Vorpruefung bleiben hier schlank.</p>
          </section>

          <section className="card dashboard-compact-card">
            <p className="section-title">Pflichtschicht</p>
            <h3 style={{ margin: 0 }}>Lern-Logik Pflichtpruefung</h3>
            <p className="dashboard-meta-line">
              {loading
                ? 'Lern-Logik wird verbunden...'
                : scrapperPipeline?.detail || 'Scrapper-Deals werden zentral bewertet und erst danach weitergeleitet.'}
            </p>
            <span className="badge">{scrapperPipeline?.integrationMode || 'Pflichtpruefung'}</span>
          </section>

          <section className="card dashboard-compact-card">
            <p className="section-title">Output</p>
            <h3 style={{ margin: 0 }}>Review, Testgruppe oder Block</h3>
            <p className="dashboard-meta-line">Kein Rohdeal geht ohne Entscheidung weiter.</p>
          </section>
        </div>

        <section className="card dashboard-section">
          <div className="dashboard-title-block">
            <p className="section-title">AMAZON / FBA / FBM</p>
            <h3>Getrennte Regeln fuer eingehende Rohdeals</h3>
          </div>
          <div className="dashboard-compact-grid">
            {sellerTypeSummary.map((item) => (
              <div key={item.id} className="dashboard-compact-card">
                <span className="dashboard-link-meta">{item.id}</span>
                <h3>{item.keepaRating}</h3>
                <p className="dashboard-meta-line">Min. Rabatt {item.minDiscount}% | Min. Score {item.minScore}</p>
              </div>
            ))}
            {!sellerTypeSummary.length && <p className="text-muted">Verkaeufertyp-Logik wird geladen.</p>}
          </div>
        </section>

        <div className="dashboard-flow-grid">
          {scraperItems.map((item) => (
            <section key={item.id} className="dashboard-flow-card">
              <div className="dashboard-flow-header">
                <div className="dashboard-flow-copy">
                  <h3 style={{ margin: 0 }}>{item.title}</h3>
                  <p className="dashboard-meta-line">{item.status} | geht zuerst durch die Lern-Logik</p>
                </div>
                <span className="badge">{item.price}</span>
              </div>
              <button
                type="button"
                className="secondary small"
                onClick={() =>
                  setStatus(
                    `"${item.title}" bleibt hier ein Rohdeal. Der Scrapper liefert die Quelle, die Lern-Logik bewertet danach getrennt in Testgruppe, Review oder Block.`
                  )
                }
              >
                {item.action}
              </button>
            </section>
          ))}
        </div>
      </div>
    </Layout>
  );
}

export default ScraperPage;
