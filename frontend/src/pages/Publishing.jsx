import { useEffect, useMemo, useState } from 'react';
import { NavLink, useLocation } from 'react-router-dom';
import Layout from '../components/layout/Layout';
import { useAuth } from '../context/AuthContext';
import './Publishing.css';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:4000';
const PUBLISHING_LOAD_TIMEOUT_MS = 2000;

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

function getDefaultFacebookSettings() {
  return {
    facebookEnabled: false,
    facebookSessionMode: 'persistent',
    facebookDefaultRetryLimit: 3,
    facebookDefaultTarget: ''
  };
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

function getStatusTone(value) {
  const normalized = String(value || '').toLowerCase();
  if (normalized.includes('failed') || normalized.includes('error') || normalized.includes('fehlt')) {
    return 'danger';
  }
  if (normalized.includes('retry') || normalized.includes('pending') || normalized.includes('deaktiviert')) {
    return 'warning';
  }
  if (normalized.includes('aktiv') || normalized.includes('ready') || normalized.includes('sent') || normalized.includes('vorhanden')) {
    return 'success';
  }
  return 'info';
}

function PublishingPage() {
  const { user } = useAuth();
  const isAdmin = user?.role === 'admin';
  const location = useLocation();
  const currentTab = useMemo(() => tabs.find((item) => item.path === location.pathname)?.path || '/publishing', [location.pathname]);

  const [queue, setQueue] = useState([]);
  const [logs, setLogs] = useState([]);
  const [workerStatus, setWorkerStatus] = useState(null);
  const [status, setStatus] = useState('');
  const [loading, setLoading] = useState(true);
  const [queueLoading, setQueueLoading] = useState(false);
  const [reloadKey, setReloadKey] = useState(0);
  const [telegramBotConfig, setTelegramBotConfig] = useState(getDefaultTelegramBotConfig);
  const [whatsappClientConfig, setWhatsappClientConfig] = useState(getDefaultWhatsappClientConfig);
  const [facebookSettings, setFacebookSettings] = useState(getDefaultFacebookSettings);

  async function apiFetch(path, options = {}, config = {}) {
    const timeoutMs = Number(config.timeoutMs || PUBLISHING_LOAD_TIMEOUT_MS);
    const controller = new AbortController();
    const timeoutId = window.setTimeout(() => controller.abort(), timeoutMs);

    try {
      const response = await fetch(`${API_BASE_URL}${path}`, {
        ...options,
        signal: controller.signal,
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
        console.warn('[PUBLISHING_LOAD_TIMEOUT]', {
          tab: currentTab,
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

  useEffect(() => {
    let cancelled = false;

    async function loadCurrentTab() {
      const startedAt = performance.now();
      const requests = [{ key: 'workerStatus', path: '/api/publishing/workers/status' }];

      if (currentTab === '/publishing/logs') {
        requests.push({ key: 'logs', path: '/api/publishing/logs?limit=80' });
      }

      if (currentTab === '/publishing/telegram' && isAdmin) {
        requests.push({ key: 'telegramBotConfig', path: '/api/publishing/telegram-bot-client' });
      }

      console.info('[PUBLISHING_LOAD_START]', {
        tab: currentTab,
        requests: requests.map((item) => item.path)
      });

      setLoading(true);
      setStatus('');
      if (currentTab !== '/publishing') {
        setQueueLoading(false);
      }

      try {
        const results = await Promise.allSettled(requests.map((item) => apiFetch(item.path)));
        if (cancelled) {
          return;
        }

        requests.forEach((request, index) => {
          const result = results[index];
          if (result.status !== 'fulfilled') {
            return;
          }

          if (request.key === 'workerStatus') {
            setWorkerStatus(result.value);
            setWhatsappClientConfig({
              ...getDefaultWhatsappClientConfig(),
              ...(result.value?.whatsapp || {})
            });
            setFacebookSettings({
              facebookEnabled: Boolean(result.value?.facebook?.enabled),
              facebookSessionMode: result.value?.facebook?.sessionMode || 'persistent',
              facebookDefaultRetryLimit: Number(result.value?.facebook?.retryLimit || 3),
              facebookDefaultTarget: result.value?.facebook?.defaultTarget || ''
            });
          }

          if (request.key === 'logs') {
            setLogs(result.value?.items || []);
          }

          if (request.key === 'telegramBotConfig') {
            setTelegramBotConfig({
              ...getDefaultTelegramBotConfig(),
              ...(result.value?.item || {})
            });
          }
        });

        const errors = results
          .filter((item) => item.status === 'rejected')
          .map((item) => (item.reason instanceof Error ? item.reason.message : 'Publishing konnte nicht geladen werden.'));

        if (errors.length) {
          setStatus(errors[0]);
          console.error('[PUBLISHING_LOAD_ERROR]', {
            tab: currentTab,
            errors
          });
        }

        console.info('[PUBLISHING_LOAD_DONE]', {
          tab: currentTab,
          durationMs: Math.round(performance.now() - startedAt),
          loadedCount: results.filter((item) => item.status === 'fulfilled').length,
          failedCount: results.filter((item) => item.status === 'rejected').length
        });
      } catch (error) {
        if (!cancelled) {
          const message = error instanceof Error ? error.message : 'Publishing-Daten konnten nicht geladen werden.';
          setStatus(message);
          console.error('[PUBLISHING_LOAD_ERROR]', {
            tab: currentTab,
            errorMessage: message
          });
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    async function loadQueuePreview() {
      if (currentTab !== '/publishing') {
        return;
      }

      setQueueLoading(true);

      try {
        const data = await apiFetch('/api/publishing/queue?limit=18', {}, { timeoutMs: 3000 });
        if (!cancelled) {
          setQueue(data?.items || []);
        }
      } catch (error) {
        if (!cancelled) {
          const message = error instanceof Error ? error.message : 'Queue konnte nicht geladen werden.';
          setStatus((current) => current || message);
          console.error('[PUBLISHING_LOAD_ERROR]', {
            tab: currentTab,
            errorMessage: message,
            section: 'queue'
          });
        }
      } finally {
        if (!cancelled) {
          setQueueLoading(false);
        }
      }
    }

    void loadCurrentTab();
    void loadQueuePreview();

    return () => {
      cancelled = true;
    };
  }, [currentTab, isAdmin, reloadKey, user?.role]);

  const queueSummary = useMemo(
    () =>
      (workerStatus?.channels || []).reduce(
        (summary, item) => ({
          pending: summary.pending + Number(item.pending ?? item.waiting ?? 0),
          sending: summary.sending + Number(item.sending ?? item.processing ?? 0),
          sent: summary.sent + Number(item.sent ?? item.posted ?? 0),
          retry: summary.retry + Number(item.retry || 0),
          failed: summary.failed + Number(item.failed || 0)
        }),
        { pending: 0, sending: 0, sent: 0, retry: 0, failed: 0 }
      ),
    [workerStatus]
  );

  const summaryCards = useMemo(
    () => [
      {
        title: 'Queue offen',
        value: `${queueSummary.pending + queueSummary.sending + queueSummary.retry}`,
        detail: `Pending ${queueSummary.pending} | Sending ${queueSummary.sending} | Retry ${queueSummary.retry}`,
        tone: queueSummary.failed > 0 ? 'warning' : queueSummary.pending + queueSummary.sending + queueSummary.retry > 0 ? 'info' : 'success'
      },
      {
        title: 'Telegram Bot',
        value: workerStatus?.telegramBot?.publishTargets || 0,
        detail: workerStatus?.telegramBot?.tokenConfigured ? 'Ziele aktiv' : 'Token fehlt',
        tone: workerStatus?.telegramBot?.tokenConfigured ? 'success' : 'warning'
      },
      {
        title: 'WhatsApp',
        value: workerStatus?.whatsapp?.enabled ? 'Aktiv' : 'Aus',
        detail: workerStatus?.whatsapp?.endpointConfigured ? 'Gateway bereit' : 'Endpoint fehlt',
        tone: workerStatus?.whatsapp?.enabled ? 'success' : 'warning'
      },
      {
        title: 'Facebook Worker',
        value: workerStatus?.facebook?.enabled ? 'Aktiv' : 'Aus',
        detail: workerStatus?.facebook?.defaultTarget || 'Kein Default-Ziel',
        tone: workerStatus?.facebook?.enabled ? 'info' : 'warning'
      }
    ],
    [queueSummary, workerStatus]
  );

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
      setReloadKey((current) => current + 1);
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
      setReloadKey((current) => current + 1);
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
      setReloadKey((current) => current + 1);
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
      targets: (prev.targets || []).map((target, targetIndex) => (targetIndex === index ? { ...target, ...patch } : target))
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
      setReloadKey((current) => current + 1);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'Telegram Bot Client konnte nicht gespeichert werden.');
    }
  }

  function renderQueueTab() {
    return (
      <section className="card publishing-panel">
        <div className="publishing-panel-header">
          <div>
            <p className="section-title">Queue</p>
            <h2 className="page-title">Offene Jobs und Retry-Faelle</h2>
          </div>
          {isAdmin ? (
            <button type="button" className="primary" onClick={() => void runWorkers()}>
              Alle Worker starten
            </button>
          ) : null}
        </div>
        {queue.length ? (
          <div className="publishing-feed">
            {queue.map((item) => (
              <article key={item.id} className="publishing-feed-item">
                <div className="publishing-item-head">
                  <strong>{item.payload?.title || `Queue ${item.id}`}</strong>
                  <span className={`status-chip ${getStatusTone(item.status)}`}>{item.status}</span>
                </div>
                <p>
                  {item.source_type} | Retry {item.retry_count} | erstellt {formatDateTime(item.created_at)}
                </p>
                <p>
                  {(item.targets || [])
                    .map((target) => {
                      const targetLabel = target.target_label ? `/${target.target_label}` : '';
                      return `${target.channel_type}${targetLabel}:${target.status}`;
                    })
                    .join(' | ') || 'Keine Targets'}
                </p>
                {isAdmin ? (
                  <button type="button" className="secondary" onClick={() => void retryQueue(item.id)}>
                    Erneut senden
                  </button>
                ) : null}
              </article>
            ))}
          </div>
        ) : queueLoading ? (
          <p className="publishing-empty">Queue wird nachgeladen...</p>
        ) : (
          <p className="publishing-empty">Keine Daten vorhanden</p>
        )}
      </section>
    );
  }

  function renderWorkersTab() {
    return (
      <section className="card publishing-panel">
        <div className="publishing-panel-header">
          <div>
            <p className="section-title">Worker Status</p>
            <h2 className="page-title">Dispatcher und Kanal-Worker</h2>
          </div>
          <span className="publishing-note">Nur Statusdaten, keine schweren Listen</span>
        </div>
        <div className="publishing-grid">
          {(workerStatus?.channels || []).map((item) => (
            <article key={item.channel_type} className="publishing-stat-card">
              <div className="publishing-item-head">
                <strong>{item.channel_type}</strong>
                {isAdmin ? (
                  <button type="button" className="secondary" onClick={() => void runWorkers(item.channel_type)}>
                    Starten
                  </button>
                ) : null}
              </div>
              <p>pending {item.pending ?? item.waiting ?? 0}</p>
              <p>sending {item.sending ?? item.processing ?? 0}</p>
              <p>retry {item.retry || 0}</p>
              <p>sent {item.sent ?? item.posted ?? 0}</p>
              <p>failed {item.failed || 0}</p>
            </article>
          ))}
          <article className="publishing-stat-card">
            <strong>Telegram Bot Client</strong>
            <p>{workerStatus?.telegramBot?.publishTargets || 0} aktive Ziele</p>
            <p>Retry Limit {workerStatus?.telegramBot?.retryLimit || 0}</p>
            <p>Token {workerStatus?.telegramBot?.tokenConfigured ? 'vorhanden' : 'fehlt'}</p>
          </article>
          <article className="publishing-stat-card">
            <strong>WhatsApp Client</strong>
            <p>{workerStatus?.whatsapp?.enabled ? 'Output aktiv' : 'Output deaktiviert'}</p>
            <p>Endpoint {workerStatus?.whatsapp?.endpointConfigured ? 'vorhanden' : 'fehlt'}</p>
            <p>Sender {workerStatus?.whatsapp?.senderConfigured ? 'gesetzt' : 'optional'}</p>
          </article>
        </div>
      </section>
    );
  }

  function renderTelegramTab() {
    return (
      <section className="card publishing-panel">
        <div className="publishing-panel-header">
          <div>
            <p className="section-title">Telegram Bot</p>
            <h2 className="page-title">Output-Ziele, Retry und Fallbacks</h2>
          </div>
          {isAdmin ? (
            <button type="button" className="secondary" onClick={addTelegramTarget}>
              Zielgruppe hinzufuegen
            </button>
          ) : null}
        </div>

        {!isAdmin ? <p className="publishing-empty">Nur Admin kann den Telegram Bot Client konfigurieren.</p> : null}

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

        <div className="publishing-grid">
          <article className="publishing-stat-card">
            <strong>Bot Token</strong>
            <p>{telegramBotConfig.tokenConfigured ? 'vorhanden' : 'fehlt'}</p>
          </article>
          <article className="publishing-stat-card">
            <strong>ENV Fallback</strong>
            <p>{telegramBotConfig.fallbackChatConfigured ? 'Telegram Chat per ENV vorhanden' : 'kein ENV Chat gesetzt'}</p>
          </article>
          <article className="publishing-stat-card">
            <strong>Aktive Publisher-Ziele</strong>
            <p>{telegramBotConfig.effectiveTargets?.length || 0} Ziele werden angesteuert.</p>
          </article>
        </div>

        {(telegramBotConfig.targets || []).length ? (
          <div className="publishing-feed">
            {(telegramBotConfig.targets || []).map((target, index) => (
              <article key={target.id || index} className="publishing-feed-item">
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
                <div className="publishing-split">
                  <label className="checkbox-card">
                    <span>Ziel aktiv</span>
                    <input
                      type="checkbox"
                      checked={Boolean(target.isActive)}
                      disabled={!isAdmin}
                      onChange={(event) => updateTelegramTarget(index, { isActive: event.target.checked })}
                    />
                  </label>
                  <label className="checkbox-card">
                    <span>Fuer Publishing verwenden</span>
                    <input
                      type="checkbox"
                      checked={Boolean(target.useForPublishing)}
                      disabled={!isAdmin}
                      onChange={(event) => updateTelegramTarget(index, { useForPublishing: event.target.checked })}
                    />
                  </label>
                </div>
                {isAdmin ? (
                  <button type="button" className="secondary" onClick={() => removeTelegramTarget(index)}>
                    Zielgruppe entfernen
                  </button>
                ) : null}
              </article>
            ))}
          </div>
        ) : (
          <p className="publishing-empty">Keine persistenten Telegram-Zielgruppen gespeichert.</p>
        )}

        {isAdmin ? (
          <button type="button" className="primary" onClick={() => void saveTelegramBotClient()}>
            Speichern
          </button>
        ) : null}
      </section>
    );
  }

  function renderWhatsappTab() {
    return (
      <section className="card publishing-panel">
        <div className="publishing-panel-header">
          <div>
            <p className="section-title">WhatsApp Client</p>
            <h2 className="page-title">Gateway, Retry und Senderstatus</h2>
          </div>
        </div>
        <div className="publishing-grid">
          <article className="publishing-stat-card">
            <strong>Status</strong>
            <p>{whatsappClientConfig.enabled ? 'WhatsApp Output aktiviert' : 'WhatsApp Output deaktiviert'}</p>
          </article>
          <article className="publishing-stat-card">
            <strong>Gateway Endpoint</strong>
            <p>{whatsappClientConfig.endpointConfigured ? 'Endpoint konfiguriert' : 'Endpoint fehlt in der ENV'}</p>
          </article>
          <article className="publishing-stat-card">
            <strong>Retry Limit</strong>
            <p>{whatsappClientConfig.retryLimit || 0} Wiederholungen pro Queue-Target</p>
          </article>
          <article className="publishing-stat-card">
            <strong>Sender</strong>
            <p>{whatsappClientConfig.senderConfigured ? whatsappClientConfig.sender || 'gesetzt' : 'nicht gesetzt'}</p>
          </article>
        </div>
      </section>
    );
  }

  function renderFacebookTab() {
    return (
      <section className="card publishing-panel">
        <div className="publishing-panel-header">
          <div>
            <p className="section-title">Facebook Worker</p>
            <h2 className="page-title">Persistente Session und Retry</h2>
          </div>
        </div>
        <label className="checkbox-card">
          <span>Facebook Worker aktiv</span>
          <input
            type="checkbox"
            checked={facebookSettings.facebookEnabled}
            onChange={(event) => setFacebookSettings((prev) => ({ ...prev, facebookEnabled: event.target.checked }))}
          />
        </label>
        <div className="form-row">
          <select
            value={facebookSettings.facebookSessionMode}
            onChange={(event) => setFacebookSettings((prev) => ({ ...prev, facebookSessionMode: event.target.value }))}
          >
            <option value="persistent">persistent</option>
            <option value="manual-refresh">manual-refresh</option>
          </select>
          <input
            type="number"
            value={facebookSettings.facebookDefaultRetryLimit}
            onChange={(event) =>
              setFacebookSettings((prev) => ({ ...prev, facebookDefaultRetryLimit: Number(event.target.value || 3) }))
            }
          />
          <input
            value={facebookSettings.facebookDefaultTarget}
            placeholder="Default Zielgruppe / Seite"
            onChange={(event) => setFacebookSettings((prev) => ({ ...prev, facebookDefaultTarget: event.target.value }))}
          />
        </div>
        {isAdmin ? (
          <button type="button" className="primary" onClick={() => void saveFacebookWorker()}>
            Speichern
          </button>
        ) : null}
      </section>
    );
  }

  function renderLogsTab() {
    return (
      <section className="card publishing-panel">
        <div className="publishing-panel-header">
          <div>
            <p className="section-title">Logs</p>
            <h2 className="page-title">Publishing und Worker-Events</h2>
          </div>
          <span className="publishing-note">{logs.length} Eintraege geladen</span>
        </div>
        {logs.length ? (
          <div className="publishing-feed">
            {logs.map((item) => (
              <article key={item.id} className="publishing-feed-item">
                <div className="publishing-item-head">
                  <strong>{item.event_type}</strong>
                  <span className={`status-chip ${item.level === 'warning' ? 'warning' : 'info'}`}>{item.level}</span>
                </div>
                <p>{item.message}</p>
                <p>{formatDateTime(item.created_at)}</p>
              </article>
            ))}
          </div>
        ) : (
          <p className="publishing-empty">Keine Daten vorhanden</p>
        )}
      </section>
    );
  }

  function renderCurrentTab() {
    if (currentTab === '/publishing/workers') {
      return renderWorkersTab();
    }
    if (currentTab === '/publishing/telegram') {
      return renderTelegramTab();
    }
    if (currentTab === '/publishing/whatsapp') {
      return renderWhatsappTab();
    }
    if (currentTab === '/publishing/facebook') {
      return renderFacebookTab();
    }
    if (currentTab === '/publishing/logs') {
      return renderLogsTab();
    }
    return renderQueueTab();
  }

  return (
    <Layout>
      <div className="publishing-page">
        <section className="card publishing-hero">
          <div className="publishing-panel-header">
            <div>
              <p className="section-title">Output</p>
              <h1 className="page-title">Publishing, Queue und Worker als klarer Versandbereich</h1>
              <p className="page-subtitle">
                Diese Seite laedt jetzt tab-spezifisch, zeigt Fehler sofort sichtbar an und blockiert nicht mehr durch
                unnoetige Queue- oder Log-Requests.
              </p>
            </div>
            <span className="badge">Entscheidung - Queue - Worker - Zielkanal</span>
          </div>
        </section>

        <section className="card publishing-panel publishing-panel-compact">
          <div className="publishing-tabs" role="tablist" aria-label="Publishing Bereiche">
            {tabs.map((item) => (
              <NavLink
                key={item.path}
                to={item.path}
                className={({ isActive }) => `publishing-tab ${isActive ? 'active' : ''}`}
              >
                {item.label}
              </NavLink>
            ))}
          </div>
        </section>

        {status ? (
          <section className="card publishing-panel publishing-status-strip">
            <p>{status}</p>
          </section>
        ) : null}

        {loading ? (
          <section className="card publishing-panel">
            <p style={{ margin: 0 }}>Publishing-Daten werden geladen...</p>
          </section>
        ) : (
          <>
            <section className="card publishing-panel publishing-panel-compact">
              <div className="publishing-panel-header">
                <div>
                  <p className="section-title">Sofortansicht</p>
                  <h2 className="page-title">Wichtigste Versanddaten oben</h2>
                </div>
                <span className="publishing-note">{tabs.find((item) => item.path === currentTab)?.label || 'Publishing'}</span>
              </div>
              <div className="publishing-grid">
                {summaryCards.map((card) => (
                  <article key={card.title} className={`publishing-stat-card publishing-tone-${card.tone}`}>
                    <div className="publishing-item-head">
                      <strong>{card.title}</strong>
                      <span className={`status-chip ${card.tone}`}>{card.tone}</span>
                    </div>
                    <h3>{card.value}</h3>
                    <p>{card.detail}</p>
                  </article>
                ))}
              </div>
            </section>

            {renderCurrentTab()}
          </>
        )}
      </div>
    </Layout>
  );
}

export default PublishingPage;
