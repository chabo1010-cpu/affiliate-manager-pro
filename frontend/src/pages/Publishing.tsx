import { useEffect, useMemo, useState } from 'react';
import { NavLink, useLocation } from 'react-router-dom';
import Layout from '../components/layout/Layout';
import { useAuth } from '../context/AuthContext';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:4000';

const tabs = [
  { label: 'Queue', path: '/publishing' },
  { label: 'Worker Status', path: '/publishing/workers' },
  { label: 'Facebook Worker', path: '/publishing/facebook' },
  { label: 'Logs', path: '/publishing/logs' }
] as const;

function PublishingPage() {
  const { user } = useAuth();
  const isAdmin = user?.role === 'admin';
  const location = useLocation();
  const [queue, setQueue] = useState<any[]>([]);
  const [logs, setLogs] = useState<any[]>([]);
  const [workerStatus, setWorkerStatus] = useState<any>(null);
  const [status, setStatus] = useState('');
  const [facebookSettings, setFacebookSettings] = useState({
    facebookEnabled: false,
    facebookSessionMode: 'persistent',
    facebookDefaultRetryLimit: 3,
    facebookDefaultTarget: ''
  });

  const currentTab = useMemo(() => tabs.find((item) => item.path === location.pathname)?.path || '/publishing', [location.pathname]);

  async function apiFetch(path: string, options: RequestInit = {}) {
    const response = await fetch(`${API_BASE_URL}${path}`, {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        'X-User-Role': user?.role || '',
        ...(options.headers || {})
      }
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(data?.error || `Request fehlgeschlagen (${response.status})`);
    }
    return data;
  }

  async function loadAll() {
    const [queueData, logsData, workerData] = await Promise.all([
      apiFetch('/api/publishing/queue'),
      apiFetch('/api/publishing/logs'),
      apiFetch('/api/publishing/workers/status')
    ]);
    setQueue(queueData.items || []);
    setLogs(logsData.items || []);
    setWorkerStatus(workerData);
    setFacebookSettings({
      facebookEnabled: Boolean(workerData?.facebook?.enabled),
      facebookSessionMode: workerData?.facebook?.sessionMode || 'persistent',
      facebookDefaultRetryLimit: Number(workerData?.facebook?.retryLimit || 3),
      facebookDefaultTarget: workerData?.facebook?.defaultTarget || ''
    });
  }

  useEffect(() => {
    void loadAll();
  }, [user?.role]);

  async function runWorkers(channelType?: string) {
    const data = await apiFetch('/api/publishing/workers/run', {
      method: 'POST',
      body: JSON.stringify({ channelType: channelType || null })
    });
    setStatus(`${data.items?.length || 0} Worker-Aufgaben verarbeitet.`);
    void loadAll();
  }

  async function retryQueue(id: number) {
    await apiFetch(`/api/publishing/queue/${id}/retry`, {
      method: 'POST',
      body: JSON.stringify({})
    });
    setStatus('Queue-Eintrag fuer Retry markiert.');
    void loadAll();
  }

  async function saveFacebookWorker() {
    await apiFetch('/api/publishing/facebook-worker', {
      method: 'PUT',
      body: JSON.stringify(facebookSettings)
    });
    setStatus('Facebook Worker gespeichert.');
    void loadAll();
  }

  return (
    <Layout>
      <div style={{ display: 'grid', gap: '1rem' }}>
        <section className="card" style={{ padding: '1rem' }}>
          <nav style={{ display: 'flex', gap: '0.65rem', flexWrap: 'wrap' }}>
            {tabs.map((item) => (
              <NavLink key={item.path} to={item.path} className={({ isActive }) => (isActive ? 'status-chip success' : 'status-chip info')}>
                {item.label}
              </NavLink>
            ))}
          </nav>
        </section>

        {status && <section className="card" style={{ padding: '1rem' }}><p style={{ margin: 0 }}>{status}</p></section>}

        {currentTab === '/publishing' && (
          <section className="card" style={{ padding: '1.25rem', display: 'grid', gap: '0.75rem' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', flexWrap: 'wrap' }}>
              <div>
                <p className="section-title">Publishing</p>
                <h1 className="page-title">Queue</h1>
              </div>
              {isAdmin && <button className="primary" onClick={() => void runWorkers()}>Alle Worker starten</button>}
            </div>
            {queue.map((item) => (
              <div key={item.id} className="radio-card" style={{ display: 'grid', gap: '0.65rem' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', flexWrap: 'wrap' }}>
                  <strong>{item.payload?.title || `Queue ${item.id}`}</strong>
                  <span className="badge">{item.status}</span>
                </div>
                <p className="text-muted" style={{ margin: 0 }}>
                  {item.source_type} · Retry {item.retry_count} · erstellt {new Date(item.created_at).toLocaleString('de-DE')}
                </p>
                <p className="text-muted" style={{ margin: 0 }}>
                  Targets: {(item.targets || []).map((target: any) => `${target.channel_type}:${target.image_source}:${target.status}`).join(' | ')}
                </p>
                {isAdmin && <button className="secondary" onClick={() => void retryQueue(item.id)}>Erneut senden</button>}
              </div>
            ))}
            {!queue.length && <p className="text-muted">Keine Queue-Eintraege vorhanden.</p>}
          </section>
        )}

        {currentTab === '/publishing/workers' && (
          <section className="card" style={{ padding: '1.25rem', display: 'grid', gap: '0.75rem' }}>
            <div>
              <p className="section-title">Worker Status</p>
              <h1 className="page-title">Dispatcher und Kanal-Worker</h1>
            </div>
            <div className="responsive-grid">
              {(workerStatus?.channels || []).map((item: any) => (
                <div key={item.channel_type} className="card" style={{ padding: '1rem', display: 'grid', gap: '0.35rem' }}>
                  <strong>{item.channel_type}</strong>
                  <p className="text-muted" style={{ margin: 0 }}>waiting {item.waiting || 0}</p>
                  <p className="text-muted" style={{ margin: 0 }}>processing {item.processing || 0}</p>
                  <p className="text-muted" style={{ margin: 0 }}>posted {item.posted || 0}</p>
                  <p className="text-muted" style={{ margin: 0 }}>failed {item.failed || 0}</p>
                  {isAdmin && <button className="secondary" onClick={() => void runWorkers(item.channel_type)}>Worker starten</button>}
                </div>
              ))}
            </div>
          </section>
        )}

        {currentTab === '/publishing/facebook' && (
          <section className="card" style={{ padding: '1.25rem', display: 'grid', gap: '0.75rem' }}>
            <div>
              <p className="section-title">Facebook Worker</p>
              <h1 className="page-title">Persistente Session und Retry</h1>
            </div>
            <label className="checkbox-card"><span>Facebook Worker aktiv</span><input type="checkbox" checked={facebookSettings.facebookEnabled} onChange={(event) => setFacebookSettings((prev) => ({ ...prev, facebookEnabled: event.target.checked }))} /></label>
            <div className="form-row">
              <select value={facebookSettings.facebookSessionMode} onChange={(event) => setFacebookSettings((prev) => ({ ...prev, facebookSessionMode: event.target.value }))}>
                <option value="persistent">persistent</option>
                <option value="manual-refresh">manual-refresh</option>
              </select>
              <input type="number" value={facebookSettings.facebookDefaultRetryLimit} onChange={(event) => setFacebookSettings((prev) => ({ ...prev, facebookDefaultRetryLimit: Number(event.target.value || 3) }))} />
              <input value={facebookSettings.facebookDefaultTarget} placeholder="Default Zielgruppe / Seite" onChange={(event) => setFacebookSettings((prev) => ({ ...prev, facebookDefaultTarget: event.target.value }))} />
            </div>
            {isAdmin && <button className="primary" onClick={() => void saveFacebookWorker()}>Speichern</button>}
            <p className="text-muted" style={{ margin: 0 }}>
              Der Facebook Worker erstellt keine Screenshots. Er nutzt nur Text, Link und optional ein bereits vorhandenes Bild.
            </p>
          </section>
        )}

        {currentTab === '/publishing/logs' && (
          <section className="card" style={{ padding: '1.25rem', display: 'grid', gap: '0.75rem' }}>
            <div>
              <p className="section-title">Logs</p>
              <h1 className="page-title">Publishing und Worker Logs</h1>
            </div>
            {logs.map((item) => (
              <div key={item.id} className="radio-card" style={{ justifyContent: 'space-between', alignItems: 'start' }}>
                <div>
                  <strong>{item.event_type}</strong>
                  <p className="text-muted" style={{ margin: '0.35rem 0 0' }}>{item.message}</p>
                </div>
                <div style={{ textAlign: 'right' }}>
                  <span className={`status-chip ${item.level === 'warning' ? 'warning' : 'info'}`}>{item.level}</span>
                  <p className="text-muted" style={{ margin: '0.35rem 0 0' }}>{new Date(item.created_at).toLocaleString('de-DE')}</p>
                </div>
              </div>
            ))}
          </section>
        )}
      </div>
    </Layout>
  );
}

export default PublishingPage;
