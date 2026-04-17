import { useEffect, useState } from 'react';
import Layout from '../components/layout/Layout';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:4000';

function LogsPage() {
  const [logs, setLogs] = useState([]);
  const [loading, setLoading] = useState(true);
  const [status, setStatus] = useState('');

  useEffect(() => {
    let cancelled = false;

    async function loadLogs() {
      setLoading(true);
      setStatus('');

      try {
        const response = await fetch(`${API_BASE_URL}/api/logs`);
        const data = await response.json().catch(() => []);

        if (!response.ok) {
          throw new Error(`Logs konnten nicht geladen werden (${response.status}).`);
        }

        if (!cancelled) {
          setLogs(Array.isArray(data) ? data : []);
        }
      } catch (error) {
        if (!cancelled) {
          setStatus(error instanceof Error ? error.message : 'Logs konnten nicht geladen werden.');
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    void loadLogs();

    return () => {
      cancelled = true;
    };
  }, []);

  return (
    <Layout>
      <div style={{ display: 'grid', gap: '1rem' }}>
        <section className="card" style={{ padding: '1.25rem' }}>
          <p className="section-title">Logs</p>
          <h2 style={{ margin: '0.25rem 0 1rem', fontSize: '1.5rem' }}>Aktionen & Historie</h2>
          {status && <p className="text-muted" style={{ margin: 0 }}>{status}</p>}
        </section>
        <div style={{ display: 'grid', gap: '0.85rem' }}>
          {loading && <section className="card" style={{ padding: '1rem' }}>Logs werden geladen...</section>}
          {logs.map((item) => (
            <section key={item.id} className="card" style={{ padding: '1rem', display: 'grid', gap: '0.5rem' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', alignItems: 'center' }}>
                <h3 style={{ margin: 0, fontSize: '1rem' }}>{item.action}</h3>
                <span
                  className={`status-chip ${
                    item.status === 'erfolgreich' ? 'success' : item.status === 'pending' ? 'warning' : 'danger'
                  }`}
                >
                  {item.status}
                </span>
              </div>
              <p className="text-muted" style={{ margin: 0 }}>{item.user} · {item.time}</p>
            </section>
          ))}
          {!loading && !logs.length && <section className="card" style={{ padding: '1rem' }}>Noch keine Logs vorhanden.</section>}
        </div>
      </div>
    </Layout>
  );
}

export default LogsPage;
