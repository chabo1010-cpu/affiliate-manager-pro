import { useEffect, useState } from 'react';
import Layout from '../components/layout/Layout';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:4000';

function AutobotPage() {
  const [botData, setBotData] = useState({
    status: 'Unbekannt',
    queue: 0,
    reviewed: 0,
    lastCheck: '-',
    entries: []
  });
  const [loading, setLoading] = useState(true);
  const [status, setStatus] = useState('');

  useEffect(() => {
    let cancelled = false;

    async function loadBot() {
      setLoading(true);
      setStatus('');

      try {
        const response = await fetch(`${API_BASE_URL}/api/bot`, {
          credentials: 'include'
        });
        const data = await response.json().catch(() => ({}));

        if (!response.ok) {
          throw new Error(data?.error || `Autobot konnte nicht geladen werden (${response.status}).`);
        }

        if (cancelled) {
          return;
        }

        setBotData({
          status: data?.status || 'Unbekannt',
          queue: Number(data?.queue || 0),
          reviewed: Array.isArray(data?.activities) ? data.activities.length : 0,
          lastCheck: data?.lastCheck || '-',
          entries: Array.isArray(data?.activities)
            ? data.activities.map((entry) => ({
                id: entry.id,
                title: entry.action || 'Aktivitaet',
                status: `${entry.user || 'System'} · ${entry.time || '-'}`
              }))
            : []
        });
      } catch (error) {
        if (!cancelled) {
          setStatus(error instanceof Error ? error.message : 'Autobot konnte nicht geladen werden.');
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    void loadBot();

    return () => {
      cancelled = true;
    };
  }, []);

  return (
    <Layout>
      <div style={{ display: 'grid', gap: '1rem' }}>
        <section className="card" style={{ padding: '1.25rem' }}>
          <p className="section-title">Autobot</p>
          <h2 style={{ margin: '0.25rem 0 1rem', fontSize: '1.5rem' }}>Status & Queue</h2>
          {status && <p className="text-muted" style={{ margin: '0.75rem 0 0' }}>{status}</p>}
          <div style={{ display: 'grid', gap: '0.75rem', gridTemplateColumns: 'repeat(auto-fit, minmax(160px,1fr))' }}>
            <div className="card" style={{ padding: '1rem' }}>
              <p className="text-muted">Bot Status</p>
              <h3 style={{ marginTop: '0.5rem' }}>{loading ? 'Laedt...' : botData.status}</h3>
            </div>
            <div className="card" style={{ padding: '1rem' }}>
              <p className="text-muted">Queue</p>
              <h3 style={{ marginTop: '0.5rem' }}>{loading ? '-' : botData.queue}</h3>
            </div>
            <div className="card" style={{ padding: '1rem' }}>
              <p className="text-muted">Gepruefte Eintraege</p>
              <h3 style={{ marginTop: '0.5rem' }}>{loading ? '-' : botData.reviewed}</h3>
            </div>
            <div className="card" style={{ padding: '1rem' }}>
              <p className="text-muted">Letzter Check</p>
              <h3 style={{ marginTop: '0.5rem' }}>{loading ? '-' : botData.lastCheck}</h3>
            </div>
          </div>
        </section>
        <section className="card" style={{ padding: '1.25rem' }}>
          <p className="section-title">Vorschlagsliste</p>
          <div style={{ display: 'grid', gap: '0.75rem' }}>
            {botData.entries.map((entry) => (
              <div key={entry.id} className="radio-card" style={{ justifyContent: 'space-between' }}>
                <span>{entry.title}</span>
                <span className="status-chip info">{entry.status}</span>
              </div>
            ))}
            {!loading && botData.entries.length === 0 && <p className="text-muted">Keine aktuellen Autobot-Aktivitaeten vorhanden.</p>}
          </div>
        </section>
      </div>
    </Layout>
  );
}

export default AutobotPage;
