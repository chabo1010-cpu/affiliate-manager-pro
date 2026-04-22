import { useEffect, useMemo, useState } from 'react';
import { NavLink, useLocation } from 'react-router-dom';
import Layout from '../components/layout/Layout';
import { useAuth } from '../context/AuthContext';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:4000';

const tabs = [
  { label: 'Queue', path: '/publishing' },
  { label: 'Worker Status', path: '/publishing/workers' },
  { label: 'Telegram Bot', path: '/publishing/telegram' },
  { label: 'WhatsApp Client', path: '/publishing/whatsapp' },
  { label: 'Facebook Worker', path: '/publishing/facebook' },
  { label: 'Logs', path: '/publishing/logs' }
];

function getDefaultTelegramBotConfig() {
  return {
    enabled: true,
    defaultRetryLimit: 3,
    tokenConfigured: false,
    fallbackChatConfigured: false,
    targets: [],
    effectiveTargets: []
  };
}

function getDefaultWhatsappClientConfig() {
  return {
    enabled: false,
    endpointConfigured: false,
    senderConfigured: false,
    sender: '',
    retryLimit: 3
  };
}

function PublishingPage() {
  const { user } = useAuth();
  const isAdmin = user?.role === 'admin';
  const location = useLocation();
  const [queue, setQueue] = useState([]);
  const [logs, setLogs] = useState([]);
  const [workerStatus, setWorkerStatus] = useState(null);
  const [status, setStatus] = useState('');
  const [loading, setLoading] = useState(true);
  const [telegramBotConfig, setTelegramBotConfig] = useState(getDefaultTelegramBotConfig);
  const [whatsappClientConfig, setWhatsappClientConfig] = useState(getDefaultWhatsappClientConfig);
  const [facebookSettings, setFacebookSettings] = useState({
    facebookEnabled: false,
    facebookSessionMode: 'persistent',
    facebookDefaultRetryLimit: 3,
    facebookDefaultTarget: ''
  });

  const currentTab = useMemo(() => tabs.find((item) => item.path === location.pathname)?.path || '/publishing', [location.pathname]);

  async function apiFetch(path, options = {}) {
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
    setLoading(true);
    try {
      const [queueData, logsData, workerData, telegramBotData] = await Promise.all([
        apiFetch('/api/publishing/queue'),
        apiFetch('/api/publishing/logs'),
        apiFetch('/api/publishing/workers/status'),
        isAdmin ? apiFetch('/api/publishing/telegram-bot-client') : Promise.resolve(null)
      ]);
      setQueue(queueData.items || []);
      setLogs(logsData.items || []);
      setWorkerStatus(workerData);
      setTelegramBotConfig({
        ...getDefaultTelegramBotConfig(),
        ...(telegramBotData?.item || {})
      });
      setWhatsappClientConfig({
        ...getDefaultWhatsappClientConfig(),
        ...(workerData?.whatsapp || {})
      });
      setFacebookSettings({
        facebookEnabled: Boolean(workerData?.facebook?.enabled),
        facebookSessionMode: workerData?.facebook?.sessionMode || 'persistent',
        facebookDefaultRetryLimit: Number(workerData?.facebook?.retryLimit || 3),
        facebookDefaultTarget: workerData?.facebook?.defaultTarget || ''
      });
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'Publishing-Daten konnten nicht geladen werden.');
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void loadAll();
  }, [isAdmin, user?.role]);

  async function runWorkers(channelType) {
    if (!isAdmin) {
      return;
    }

    try {
      const data = await apiFetch('/api/publishing/workers/run', {
        method: 'POST',
        body: JSON.stringify({ channelType: channelType || null })
      });
      setStatus(`${data.items?.length || 0} Worker-Aufgaben verarbeitet.`);
      void loadAll();
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'Worker konnten nicht gestartet werden.');
    }
  }

  async function retryQueue(id) {
    try {
      await apiFetch(`/api/publishing/queue/${id}/retry`, {
        method: 'POST',
        body: JSON.stringify({})
      });
      setStatus('Queue-Eintrag fuer Retry markiert.');
      void loadAll();
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'Queue-Retry fehlgeschlagen.');
    }
  }

  async function saveFacebookWorker() {
    if (!isAdmin) {
      return;
    }

    try {
      await apiFetch('/api/publishing/facebook-worker', {
        method: 'PUT',
        body: JSON.stringify(facebookSettings)
      });
      setStatus('Facebook Worker gespeichert.');
      void loadAll();
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'Facebook Worker konnte nicht gespeichert werden.');
    }
  }

  function addTelegramTarget() {
    setTelegramBotConfig((prev) => ({
      ...prev,
      targets: [
        ...(prev.targets || []),
        {
          id: `new-${Date.now()}-${prev.targets?.length || 0}`,
          name: '',
          chatId: '',
          isActive: true,
          useForPublishing: true
        }
      ]
    }));
  }

  function updateTelegramTarget(index, patch) {
    setTelegramBotConfig((prev) => ({
      ...prev,
      targets: (prev.targets || []).map((target, targetIndex) =>
        targetIndex === index ? { ...target, ...patch } : target
      )
    }));
  }

  function removeTelegramTarget(index) {
    setTelegramBotConfig((prev) => ({
      ...prev,
      targets: (prev.targets || []).filter((_, targetIndex) => targetIndex !== index)
    }));
  }

  async function saveTelegramBotClient() {
    if (!isAdmin) {
      return;
    }

    try {
      await apiFetch('/api/publishing/telegram-bot-client', {
        method: 'PUT',
        body: JSON.stringify({
          enabled: telegramBotConfig.enabled,
          defaultRetryLimit: telegramBotConfig.defaultRetryLimit,
          targets: (telegramBotConfig.targets || []).map((target) => ({
            id: typeof target.id === 'number' ? target.id : undefined,
            name: target.name,
            chatId: target.chatId,
            isActive: target.isActive,
            useForPublishing: target.useForPublishing
          }))
        })
      });
      setStatus('Telegram Bot Client gespeichert.');
      void loadAll();
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'Telegram Bot Client konnte nicht gespeichert werden.');
    }
  }

  return (
    <Layout>
      <div style={{ display: 'grid', gap: '1rem' }}>
        <section className="card" style={{ padding: '1rem', display: 'grid', gap: '0.65rem' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', flexWrap: 'wrap' }}>
            <div>
              <p className="section-title">Publishing / Output</p>
              <h1 className="page-title">Versand, Queue und Worker getrennt vom Generator</h1>
              <p className="text-muted" style={{ margin: '0.35rem 0 0' }}>
                Dieser Bereich ist fuer Ausgabe, Versandstatus und Worker gedacht. Generator und Templates
                liefern Inhalte, Publishing uebernimmt den eigentlichen Output.
              </p>
            </div>
            <span className="badge">Entscheidung -&gt; Queue -&gt; Worker -&gt; Zielkanal</span>
          </div>
        </section>

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

        {loading && <section className="card" style={{ padding: '1rem' }}>Publishing-Daten werden geladen...</section>}

        {!loading && currentTab === '/publishing' && (
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
                  Targets: {(item.targets || [])
                    .map((target) => {
                      const targetLabel = target.target_label ? `/${target.target_label}` : '';
                      return `${target.channel_type}${targetLabel}:${target.image_source}:${target.status}`;
                    })
                    .join(' | ')}
                </p>
                {isAdmin && <button className="secondary" onClick={() => void retryQueue(item.id)}>Erneut senden</button>}
              </div>
            ))}
            {!queue.length && <p className="text-muted">Keine Queue-Eintraege vorhanden.</p>}
          </section>
        )}

        {!loading && currentTab === '/publishing/workers' && (
          <section className="card" style={{ padding: '1.25rem', display: 'grid', gap: '0.75rem' }}>
            <div>
              <p className="section-title">Worker Status</p>
              <h1 className="page-title">Dispatcher und Kanal-Worker</h1>
            </div>
            <div className="responsive-grid">
              <div className="card" style={{ padding: '1rem', display: 'grid', gap: '0.35rem' }}>
                <strong>Telegram Bot Client</strong>
                <p className="text-muted" style={{ margin: 0 }}>
                  {workerStatus?.telegramBot?.publishTargets || 0} aktive Ziele bei {workerStatus?.telegramBot?.configuredTargets || 0} gespeicherten Gruppen
                </p>
                <p className="text-muted" style={{ margin: 0 }}>
                  Retry Limit {workerStatus?.telegramBot?.retryLimit || 0} Â· Token {workerStatus?.telegramBot?.tokenConfigured ? 'vorhanden' : 'fehlt'}
                </p>
              </div>
              <div className="card" style={{ padding: '1rem', display: 'grid', gap: '0.35rem' }}>
                <strong>WhatsApp Client</strong>
                <p className="text-muted" style={{ margin: 0 }}>
                  {workerStatus?.whatsapp?.enabled ? 'Output aktiv' : 'Output deaktiviert'} · Endpoint {workerStatus?.whatsapp?.endpointConfigured ? 'vorhanden' : 'fehlt'}
                </p>
                <p className="text-muted" style={{ margin: 0 }}>
                  Retry Limit {workerStatus?.whatsapp?.retryLimit || 0} · Sender {workerStatus?.whatsapp?.senderConfigured ? 'vorhanden' : 'optional'}
                </p>
              </div>
            </div>
            <div className="responsive-grid">
              {(workerStatus?.channels || []).map((item) => (
                <div key={item.channel_type} className="card" style={{ padding: '1rem', display: 'grid', gap: '0.35rem' }}>
                  <strong>{item.channel_type}</strong>
                  <p className="text-muted" style={{ margin: 0 }}>pending {item.pending ?? item.waiting ?? 0}</p>
                  <p className="text-muted" style={{ margin: 0 }}>sending {item.sending ?? item.processing ?? 0}</p>
                  <p className="text-muted" style={{ margin: 0 }}>retry {item.retry || 0}</p>
                  <p className="text-muted" style={{ margin: 0 }}>sent {item.sent ?? item.posted ?? 0}</p>
                  <p className="text-muted" style={{ margin: 0 }}>failed {item.failed || 0}</p>
                  {isAdmin && <button className="secondary" onClick={() => void runWorkers(item.channel_type)}>Worker starten</button>}
                </div>
              ))}
            </div>
          </section>
        )}

        {!loading && currentTab === '/publishing/telegram' && (
          <section className="card" style={{ padding: '1.25rem', display: 'grid', gap: '0.75rem' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', flexWrap: 'wrap' }}>
              <div>
                <p className="section-title">Telegram Bot Client</p>
                <h1 className="page-title">Output-Ziele, Retry und Publisher-Anbindung</h1>
              </div>
              {isAdmin && <button className="secondary" onClick={addTelegramTarget}>Zielgruppe hinzufuegen</button>}
            </div>

            {!isAdmin && (
              <p className="text-muted" style={{ margin: 0 }}>
                Nur Admin kann den Telegram Bot Client konfigurieren.
              </p>
            )}

            <label className="checkbox-card">
              <span>Telegram Output aktiv</span>
              <input
                type="checkbox"
                checked={telegramBotConfig.enabled}
                disabled={!isAdmin}
                onChange={(event) => setTelegramBotConfig((prev) => ({ ...prev, enabled: event.target.checked }))}
              />
            </label>

            <div className="form-row">
              <input
                type="number"
                min="0"
                value={telegramBotConfig.defaultRetryLimit}
                disabled={!isAdmin}
                placeholder="Retry Limit"
                onChange={(event) =>
                  setTelegramBotConfig((prev) => ({
                    ...prev,
                    defaultRetryLimit: Number(event.target.value || 0)
                  }))
                }
              />
            </div>

            <div className="responsive-grid">
              <div className="card" style={{ padding: '1rem', display: 'grid', gap: '0.35rem' }}>
                <strong>Bot Token</strong>
                <p className="text-muted" style={{ margin: 0 }}>
                  {telegramBotConfig.tokenConfigured ? 'vorhanden' : 'fehlt'}
                </p>
              </div>
              <div className="card" style={{ padding: '1rem', display: 'grid', gap: '0.35rem' }}>
                <strong>ENV Fallback</strong>
                <p className="text-muted" style={{ margin: 0 }}>
                  {telegramBotConfig.fallbackChatConfigured ? 'Telegram Chat per ENV vorhanden' : 'kein ENV Chat gesetzt'}
                </p>
              </div>
              <div className="card" style={{ padding: '1rem', display: 'grid', gap: '0.35rem' }}>
                <strong>Aktive Publisher-Ziele</strong>
                <p className="text-muted" style={{ margin: 0 }}>
                  {telegramBotConfig.effectiveTargets?.length || 0} Ziele werden vom Publisher angesteuert.
                </p>
              </div>
            </div>

            {(telegramBotConfig.targets || []).map((target, index) => (
              <div key={target.id || index} className="radio-card" style={{ display: 'grid', gap: '0.75rem' }}>
                <div className="form-row">
                  <input
                    value={target.name || ''}
                    disabled={!isAdmin}
                    placeholder="Name der Zielgruppe"
                    onChange={(event) => updateTelegramTarget(index, { name: event.target.value })}
                  />
                  <input
                    value={target.chatId || ''}
                    disabled={!isAdmin}
                    placeholder="Chat ID / Channel ID"
                    onChange={(event) => updateTelegramTarget(index, { chatId: event.target.value })}
                  />
                </div>
                <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap' }}>
                  <label className="checkbox-card" style={{ flex: 1 }}>
                    <span>Ziel aktiv</span>
                    <input
                      type="checkbox"
                      checked={Boolean(target.isActive)}
                      disabled={!isAdmin}
                      onChange={(event) => updateTelegramTarget(index, { isActive: event.target.checked })}
                    />
                  </label>
                  <label className="checkbox-card" style={{ flex: 1 }}>
                    <span>Fuer Publisher verwenden</span>
                    <input
                      type="checkbox"
                      checked={Boolean(target.useForPublishing)}
                      disabled={!isAdmin}
                      onChange={(event) => updateTelegramTarget(index, { useForPublishing: event.target.checked })}
                    />
                  </label>
                </div>
                {isAdmin && (
                  <button className="secondary" onClick={() => removeTelegramTarget(index)}>
                    Zielgruppe entfernen
                  </button>
                )}
              </div>
            ))}

            {!telegramBotConfig.targets?.length && (
              <p className="text-muted" style={{ margin: 0 }}>
                Noch keine persistenten Telegram-Zielgruppen gespeichert. Wenn keine Zielgruppe hinterlegt ist, nutzt der
                Publisher automatisch den ENV Fallback.
              </p>
            )}

            {isAdmin && <button className="primary" onClick={() => void saveTelegramBotClient()}>Speichern</button>}
          </section>
        )}

        {!loading && currentTab === '/publishing/whatsapp' && (
          <section className="card" style={{ padding: '1.25rem', display: 'grid', gap: '0.75rem' }}>
            <div>
              <p className="section-title">WhatsApp Client</p>
              <h1 className="page-title">Queue, Retry und Gateway-Anbindung</h1>
            </div>
            <div className="responsive-grid">
              <div className="card" style={{ padding: '1rem', display: 'grid', gap: '0.35rem' }}>
                <strong>Status</strong>
                <p className="text-muted" style={{ margin: 0 }}>
                  {whatsappClientConfig.enabled ? 'WhatsApp Output aktiviert' : 'WhatsApp Output deaktiviert'}
                </p>
              </div>
              <div className="card" style={{ padding: '1rem', display: 'grid', gap: '0.35rem' }}>
                <strong>Gateway Endpoint</strong>
                <p className="text-muted" style={{ margin: 0 }}>
                  {whatsappClientConfig.endpointConfigured ? 'Endpoint konfiguriert' : 'Endpoint fehlt in der ENV'}
                </p>
              </div>
              <div className="card" style={{ padding: '1rem', display: 'grid', gap: '0.35rem' }}>
                <strong>Retry Limit</strong>
                <p className="text-muted" style={{ margin: 0 }}>
                  {whatsappClientConfig.retryLimit || 0} Wiederholungen pro Queue-Target
                </p>
              </div>
              <div className="card" style={{ padding: '1rem', display: 'grid', gap: '0.35rem' }}>
                <strong>Sender</strong>
                <p className="text-muted" style={{ margin: 0 }}>
                  {whatsappClientConfig.senderConfigured ? whatsappClientConfig.sender || 'gesetzt' : 'nicht gesetzt'}
                </p>
              </div>
            </div>
            <p className="text-muted" style={{ margin: 0 }}>
              Der WhatsApp Client speichert Deals zuerst in der Queue, sendet danach an das konfigurierte Gateway und
              laesst Fehler ueber Retry oder Recovery im Publisher weiterlaufen. Die Konfiguration selbst kommt aus der
              Backend-ENV.
            </p>
          </section>
        )}

        {!loading && currentTab === '/publishing/facebook' && (
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

        {!loading && currentTab === '/publishing/logs' && (
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
