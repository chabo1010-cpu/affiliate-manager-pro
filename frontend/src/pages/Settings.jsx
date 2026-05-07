import { useEffect, useMemo, useRef, useState } from 'react';
import { Link, useLocation } from 'react-router-dom';
import Layout from '../components/layout/Layout';
import { useAuth } from '../context/AuthContext';
import './Settings.css';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:4000';
const settingsApiUrl = `${API_BASE_URL}/api/deals/settings`;
const liveStatusApiUrl = `${API_BASE_URL}/api/settings/live-status`;
const outputChannelsApiUrl = `${API_BASE_URL}/api/settings/output-channels`;
const restartApiUrl = `${API_BASE_URL}/api/system/restart-backend`;
const healthApiUrl = `${API_BASE_URL}/api/health`;
const DEFAULT_TELEGRAM_COPY_BUTTON_TEXT = '\u{1F4CB} Zum Kopieren hier klicken';
const SETTINGS_TIMEOUT_MS = 2200;

const tabs = [
  { id: 'live', label: 'Zugaenge & Live Status' },
  { id: 'output', label: 'Kanaele & Output' },
  { id: 'telegram', label: 'Telegram' },
  { id: 'amazon', label: 'Amazon' },
  { id: 'whatsapp', label: 'WhatsApp' },
  { id: 'facebook', label: 'Facebook' },
  { id: 'system', label: 'System' },
  { id: 'security', label: 'Sicherheit' }
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

function getTone(status = '') {
  const normalized = String(status || '').toLowerCase();
  if (normalized.includes('fehler') || normalized.includes('error') || normalized.includes('offline') || normalized.includes('block')) {
    return 'danger';
  }
  if (normalized.includes('warn') || normalized.includes('pruefen') || normalized.includes('pause') || normalized.includes('deaktiv')) {
    return 'warning';
  }
  if (normalized.includes('ki') || normalized.includes('automation')) {
    return 'automation';
  }
  if (normalized.includes('aktiv') || normalized.includes('online') || normalized.includes('bereit') || normalized.includes('verbunden') || normalized.includes('vorhanden')) {
    return 'success';
  }
  return 'info';
}

function buildStatusText(flag, positive = 'aktiv', negative = 'pausiert') {
  return flag ? positive : negative;
}

function getOutputChannelTone(channel = {}) {
  if (channel.isDangerousLive && channel.isEnabled !== true) {
    return 'danger';
  }
  if (channel.isBlocked) {
    return 'warning';
  }
  if (channel.lastStatus === 'failed' || channel.lastStatus === 'disabled_skip') {
    return 'warning';
  }
  if (channel.isEnabled && channel.platformStatus?.active) {
    return 'success';
  }
  return 'info';
}

function SettingsPage() {
  const { user } = useAuth();
  const location = useLocation();
  const isAdmin = user?.role === 'admin';
  const [activeTab, setActiveTab] = useState('live');
  const [telegramCopyButtonText, setTelegramCopyButtonText] = useState(DEFAULT_TELEGRAM_COPY_BUTTON_TEXT);
  const [loading, setLoading] = useState(isAdmin);
  const [saving, setSaving] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [outputView, setOutputView] = useState('telegram');
  const [outputSnapshot, setOutputSnapshot] = useState(null);
  const [outputBusyKey, setOutputBusyKey] = useState('');
  const [outputTestingKey, setOutputTestingKey] = useState('');
  const [settingsStatus, setSettingsStatus] = useState('');
  const [restartStatus, setRestartStatus] = useState('');
  const [restarting, setRestarting] = useState(false);
  const [restartLocked, setRestartLocked] = useState(false);
  const [toast, setToast] = useState(null);
  const [liveStatus, setLiveStatus] = useState(null);
  const restartTimerIdsRef = useRef([]);

  function showToast(message, tone = 'success') {
    setToast({ message, tone });
  }

  function registerRestartTimer(callback, delayMs) {
    const timerId = window.setTimeout(() => {
      restartTimerIdsRef.current = restartTimerIdsRef.current.filter((entry) => entry !== timerId);
      callback();
    }, delayMs);

    restartTimerIdsRef.current.push(timerId);
    return timerId;
  }

  async function apiFetch(path, options = {}, timeoutMs = SETTINGS_TIMEOUT_MS) {
    const controller = new AbortController();
    const timeoutId = window.setTimeout(() => controller.abort(), timeoutMs);

    try {
      const response = await fetch(path, {
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
        throw new Error(`Timeout nach ${timeoutMs}ms.`);
      }
      throw error;
    } finally {
      window.clearTimeout(timeoutId);
    }
  }

  async function loadSettings(forceRefresh = false) {
    if (!isAdmin) {
      setLoading(false);
      return;
    }

    if (forceRefresh) {
      setRefreshing(true);
    } else {
      setLoading(true);
    }

    setSettingsStatus('');

    try {
      const [settingsResponse, liveResponse, outputResponse] = await Promise.allSettled([
        apiFetch(settingsApiUrl),
        apiFetch(liveStatusApiUrl),
        apiFetch(outputChannelsApiUrl)
      ]);

      if (settingsResponse.status === 'fulfilled') {
        setTelegramCopyButtonText(settingsResponse.value?.telegramCopyButtonText || DEFAULT_TELEGRAM_COPY_BUTTON_TEXT);
      }

      if (liveResponse.status === 'fulfilled') {
        setLiveStatus(liveResponse.value?.item || null);
      }

      if (outputResponse.status === 'fulfilled') {
        setOutputSnapshot(outputResponse.value?.item || null);
      } else if (liveResponse.status === 'fulfilled') {
        setOutputSnapshot(liveResponse.value?.item?.outputs || null);
      }

      if (settingsResponse.status === 'rejected' || liveResponse.status === 'rejected' || outputResponse.status === 'rejected') {
        const message =
          settingsResponse.status === 'rejected'
            ? settingsResponse.reason?.message || 'Settings konnten nicht geladen werden.'
            : liveResponse.status === 'rejected'
              ? liveResponse.reason?.message || 'Live Status konnte nicht geladen werden.'
              : outputResponse.reason?.message || 'Output Kanaele konnten nicht geladen werden.';
        setSettingsStatus(message);
      }
    } catch (error) {
      setSettingsStatus(error instanceof Error ? error.message : 'Settings konnten nicht geladen werden.');
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }

  useEffect(() => () => {
    restartTimerIdsRef.current.forEach((timerId) => window.clearTimeout(timerId));
    restartTimerIdsRef.current = [];
  }, []);

  useEffect(() => {
    void loadSettings();
  }, [isAdmin, user?.role]);

  useEffect(() => {
    const params = new URLSearchParams(location.search);
    const requestedTab = params.get('tab');
    const requestedOutputView = params.get('view');

    if (requestedTab && tabs.some((tab) => tab.id === requestedTab)) {
      setActiveTab(requestedTab);
    }

    if (requestedOutputView && ['telegram', 'whatsapp', 'facebook', 'live', 'security'].includes(requestedOutputView)) {
      setOutputView(requestedOutputView);
    }
  }, [location.search]);

  useEffect(() => {
    if (!toast) {
      return undefined;
    }

    const timerId = window.setTimeout(() => setToast(null), 2800);
    return () => window.clearTimeout(timerId);
  }, [toast]);

  const pollHealthUntilReady = async (deadlineAt) => {
    try {
      const response = await fetch(healthApiUrl, { cache: 'no-store' });
      const data = await response.json().catch(() => ({}));

      if (response.ok && data?.ok === true) {
        setRestartStatus('Backend ist wieder online. Seite wird neu geladen...');
        registerRestartTimer(() => {
          window.location.reload();
        }, 700);
        return;
      }
    } catch {}

    if (Date.now() >= deadlineAt) {
      setRestarting(false);
      setRestartStatus('Backend antwortet noch nicht. Bitte Seite manuell neu laden.');
      showToast('Backend antwortet noch nicht.', 'error');
      return;
    }

    registerRestartTimer(() => {
      void pollHealthUntilReady(deadlineAt);
    }, 1000);
  };

  const handleSave = async () => {
    if (!isAdmin || saving) {
      return;
    }

    setSaving(true);
    setSettingsStatus('');

    try {
      const data = await apiFetch(settingsApiUrl, {
        method: 'POST',
        body: JSON.stringify({ telegramCopyButtonText })
      });

      setTelegramCopyButtonText(data?.telegramCopyButtonText || DEFAULT_TELEGRAM_COPY_BUTTON_TEXT);
      setSettingsStatus('Telegram Copy-Button Text gespeichert.');
      showToast('Einstellungen gespeichert.', 'success');
      await loadSettings(true);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Speichern fehlgeschlagen.';
      setSettingsStatus(message);
      showToast('Einstellungen konnten nicht gespeichert werden.', 'error');
    } finally {
      setSaving(false);
    }
  };

  const handleRestartBackend = async () => {
    if (!isAdmin || restarting || restartLocked) {
      return;
    }

    setRestartLocked(true);
    setRestarting(true);
    setRestartStatus('Neustart laeuft...');

    registerRestartTimer(() => {
      setRestartLocked(false);
    }, 5000);

    try {
      const data = await apiFetch(restartApiUrl, {
        method: 'POST',
        headers: {
          'X-User-Role': user?.role || ''
        }
      });

      setRestartStatus('Server wird neu gestartet...');
      showToast('Backend-Neustart gestartet.', 'info');

      const initialDelayMs = Number.isFinite(Number(data?.reloadAfterMs)) && Number(data?.reloadAfterMs) > 0 ? Number(data.reloadAfterMs) : 3000;
      const deadlineAt = Date.now() + 30000;
      registerRestartTimer(() => {
        void pollHealthUntilReady(deadlineAt);
      }, initialDelayMs);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Backend-Neustart konnte nicht gestartet werden.';
      setRestarting(false);
      setRestartStatus(message);
      showToast('Backend-Neustart fehlgeschlagen.', 'error');
    }
  };

  const handleSaveOutputChannel = async (channelKey, patch) => {
    if (!isAdmin || !channelKey) {
      return;
    }

    setOutputBusyKey(channelKey);

    try {
      const data = await apiFetch(`${outputChannelsApiUrl}/${encodeURIComponent(channelKey)}`, {
        method: 'PUT',
        body: JSON.stringify(patch)
      });
      setOutputSnapshot((current) => {
        if (!current) {
          return current;
        }

        return {
          ...current,
          channels: (current.channels || []).map((item) => (item.channelKey === channelKey ? data?.item || item : item))
        };
      });
      showToast('Output Kanal gespeichert.', 'success');
      await loadSettings(true);
    } catch (error) {
      setSettingsStatus(error instanceof Error ? error.message : 'Output Kanal konnte nicht gespeichert werden.');
      showToast('Output Kanal konnte nicht gespeichert werden.', 'error');
    } finally {
      setOutputBusyKey('');
    }
  };

  const handleSaveOutputControls = async (patch) => {
    if (!isAdmin) {
      return;
    }

    setOutputBusyKey('controls');

    try {
      const data = await apiFetch(`${outputChannelsApiUrl}/controls`, {
        method: 'PUT',
        body: JSON.stringify(patch)
      });
      setOutputSnapshot((current) => ({
        ...(current || {}),
        controls: data?.item || current?.controls || null,
        platforms: current?.platforms || {},
        channels: current?.channels || []
      }));
      showToast('Output Controls gespeichert.', 'success');
      await loadSettings(true);
    } catch (error) {
      setSettingsStatus(error instanceof Error ? error.message : 'Output Controls konnten nicht gespeichert werden.');
      showToast('Output Controls konnten nicht gespeichert werden.', 'error');
    } finally {
      setOutputBusyKey('');
    }
  };

  const handleRunOutputTest = async (channelKey) => {
    if (!isAdmin || !channelKey) {
      return;
    }

    setOutputTestingKey(channelKey);

    try {
      await apiFetch(`${outputChannelsApiUrl}/${encodeURIComponent(channelKey)}/test`, {
        method: 'POST',
        body: JSON.stringify({})
      }, 10000);
      showToast('Output Test verarbeitet.', 'success');
      await loadSettings(true);
    } catch (error) {
      setSettingsStatus(error instanceof Error ? error.message : 'Output Test fehlgeschlagen.');
      showToast('Output Test fehlgeschlagen.', 'error');
    } finally {
      setOutputTestingKey('');
    }
  };

  const modules = liveStatus?.config?.modules || {};
  const frontendOnline = typeof navigator === 'undefined' ? true : navigator.onLine !== false;
  const heroCards = useMemo(
    () => [
      {
        title: 'Backend',
        value: liveStatus?.system?.backendOnline ? 'Online' : 'Unbekannt',
        detail: `Uptime ${liveStatus?.system?.uptimeSeconds || 0}s`,
        tone: liveStatus?.system?.backendOnline ? 'success' : 'warning'
      },
      {
        title: 'Frontend',
        value: frontendOnline ? 'Online' : 'Offline',
        detail: window?.location?.origin || '-',
        tone: frontendOnline ? 'info' : 'danger'
      },
      {
        title: 'Copybot',
        value: liveStatus?.copybot?.enabled ? 'Aktiv' : 'Pausiert',
        detail: `Input ${liveStatus?.copybot?.inputProcessing || '-'} | Queue ${liveStatus?.copybot?.queueProcessing || '-'}`,
        tone: liveStatus?.copybot?.enabled ? 'success' : 'warning'
      },
      {
        title: 'Queue Worker',
        value: buildStatusText(liveStatus?.system?.queueWorkerActive, 'Aktiv', 'Pausiert'),
        detail: `${liveStatus?.system?.openQueueCount || 0} offene Jobs`,
        tone: liveStatus?.system?.queueWorkerActive ? 'success' : 'warning'
      }
    ],
    [frontendOnline, liveStatus]
  );

  const liveSections = useMemo(
    () => [
      {
        title: 'Telegram',
        value: liveStatus?.telegram?.readerStatus || '-',
        detail: `${liveStatus?.telegram?.activeInputGroups || 0} aktive Input Gruppen | ${liveStatus?.telegram?.botTargets || 0} Output Ziele`,
        tone: getTone(liveStatus?.telegram?.readerStatus)
      },
      {
        title: 'Amazon',
        value: liveStatus?.amazon?.creatorApiStatus || '-',
        detail: `PAAPI ${liveStatus?.amazon?.paapiStatus || '-'} | Creator ${liveStatus?.amazon?.creatorApiConfigured ? 'konfiguriert' : 'offen'}`,
        tone: getTone(liveStatus?.amazon?.creatorApiStatus || liveStatus?.amazon?.paapiStatus)
      },
      {
        title: 'WhatsApp',
        value: liveStatus?.whatsapp?.clientStatus || '-',
        detail: `Endpoint ${liveStatus?.whatsapp?.endpointStatus || '-'} | Sender ${liveStatus?.whatsapp?.senderStatus || '-'}`,
        tone: getTone(liveStatus?.whatsapp?.clientStatus)
      },
      {
        title: 'Facebook',
        value: liveStatus?.facebook?.workerStatus || '-',
        detail: `Session ${liveStatus?.facebook?.sessionMode || '-'} | Retry ${liveStatus?.facebook?.retryLimit || 0}`,
        tone: getTone(liveStatus?.facebook?.workerStatus)
      },
      {
        title: 'System',
        value: buildStatusText(liveStatus?.system?.schedulerActive, 'Aktiv', 'Pausiert'),
        detail: `Keepa ${buildStatusText(liveStatus?.system?.keepaSchedulerActive, 'an', 'aus')} | Werbung ${buildStatusText(liveStatus?.system?.advertisingSchedulerActive, 'an', 'aus')}`,
        tone: liveStatus?.system?.schedulerActive ? 'automation' : 'warning'
      },
      {
        title: 'Sicherheit',
        value: liveStatus?.copybot?.cooldownEnabled ? 'Geschuetzt' : 'Locker',
        detail: `Cooldown ${liveStatus?.copybot?.cooldownHours || 0}h | Copy Button maskiert`,
        tone: liveStatus?.copybot?.cooldownEnabled ? 'success' : 'info'
      }
    ],
    [liveStatus]
  );

  const outputPlatformTabs = useMemo(
    () => [
      { id: 'telegram', label: 'Telegram Output' },
      { id: 'whatsapp', label: 'WhatsApp Output' },
      { id: 'facebook', label: 'Facebook Output' },
      { id: 'live', label: 'Live Status' },
      { id: 'security', label: 'Sicherheit' }
    ],
    []
  );

  const filteredOutputChannels = useMemo(() => {
    const channels = outputSnapshot?.channels || [];
    if (outputView === 'live') {
      return channels.filter((channel) => channel.isDangerousLive || channel.channelType === 'live');
    }

    if (outputView === 'security') {
      return channels.filter(
        (channel) =>
          channel.isBlocked ||
          channel.isEnabled !== true ||
          channel.lastStatus === 'disabled_skip' ||
          Boolean(channel.warningText) ||
          Boolean(channel.lastErrorMessage)
      );
    }

    return channels.filter((channel) => channel.platform === outputView);
  }, [outputSnapshot, outputView]);

  function renderLiveTab() {
    return (
      <section className="settings-panel card">
        <div className="settings-panel-header">
          <div>
            <p className="section-title">Live Status</p>
            <h2 className="page-title">Zugaenge, Verbindungen und Produktionslage</h2>
          </div>
          <div className="settings-panel-actions">
            <button type="button" className="secondary" disabled={refreshing} onClick={() => void loadSettings(true)}>
              {refreshing ? 'Lade neu...' : 'Status neu laden'}
            </button>
            <Link to="/logs" className="tab-chip">
              Logs oeffnen
            </Link>
          </div>
        </div>
        <div className="settings-grid">
          {liveSections.map((item) => (
            <article key={item.title} className={`status-card settings-status-card settings-tone-${item.tone}`}>
              <div className="settings-card-head">
                <p className="section-title">{item.title}</p>
                <span className={`status-chip ${item.tone}`}>{item.value}</span>
              </div>
              <h3 className="card-title">{item.value}</h3>
              <p className="meta-text">{item.detail}</p>
            </article>
          ))}
        </div>
      </section>
    );
  }

  function renderOutputChannelCard(channel) {
    const tone = getOutputChannelTone(channel);
    const busy = outputBusyKey === channel.channelKey;
    const testing = outputTestingKey === channel.channelKey;
    const statusLabel = channel.isBlocked
      ? 'Gesperrt'
      : channel.isEnabled
        ? channel.platformStatus?.active
          ? 'Aktiv'
          : 'Wartet'
        : 'Deaktiviert';

    return (
      <article key={channel.channelKey} className={`status-card settings-status-card settings-tone-${tone} output-channel-card`}>
        <div className="settings-card-head">
          <p className="section-title">{channel.platform}</p>
          <span className={`status-chip ${tone}`}>{statusLabel}</span>
        </div>
        <h3 className="card-title">{channel.channelLabel}</h3>
        <p className="meta-text">
          Typ {channel.channelType} | Plattform {channel.platformStatus?.active ? 'aktiv' : 'aus'} | Queue{' '}
          {outputSnapshot?.controls?.outputQueueEnabled ? 'aktiv' : 'aus'}
        </p>
        {channel.targetRef ? <p className="meta-text">Ziel: {channel.targetRef}</p> : null}
        {channel.warningText ? <div className="warning-card output-warning-banner">{channel.warningText}</div> : null}
        <div className="output-check-grid">
          <p className="meta-text">Output-Kanal: {channel.isEnabled ? 'Aktiv' : 'Aus'}</p>
          <p className="meta-text">Plattform: {channel.platformStatus?.active ? 'Aktiv' : 'Aus'}</p>
          <p className="meta-text">Typ erlaubt: {(channel.allowedSourceTypes || []).join(', ') || '-'}</p>
          <p className="meta-text">Modus: {channel.channelType}</p>
          <p className="meta-text">Copybot: {outputSnapshot?.controls?.copybotEnabled ? 'Aktiv' : 'Aus'}</p>
          <p className="meta-text">Kanal gesperrt: {channel.isBlocked ? 'Ja' : 'Nein'}</p>
        </div>
        <div className="output-history-grid">
          <article className="info-card">
            <p className="section-title">Letzte Sendung</p>
            <h3 className="card-title">{formatDateTime(channel.lastSentAt)}</h3>
            <p className="meta-text">{channel.lastMessagePreview || 'Noch keine gespeicherte Ausgabe.'}</p>
          </article>
          <article className="warning-card">
            <p className="section-title">Letzter Fehler</p>
            <h3 className="card-title">{formatDateTime(channel.lastErrorAt)}</h3>
            <p className="meta-text">{channel.lastErrorMessage || 'Kein aktueller Fehler gespeichert.'}</p>
          </article>
        </div>
        <div className="settings-panel-actions">
          <button
            type="button"
            className={channel.isEnabled ? 'secondary' : 'primary'}
            disabled={busy}
            onClick={() => void handleSaveOutputChannel(channel.channelKey, { isEnabled: !channel.isEnabled })}
          >
            {busy ? 'Speichert...' : channel.isEnabled ? 'Deaktivieren' : 'Aktivieren'}
          </button>
          <button
            type="button"
            className="secondary"
            disabled={busy}
            onClick={() => void handleSaveOutputChannel(channel.channelKey, { isBlocked: !channel.isBlocked })}
          >
            {channel.isBlocked ? 'Entsperren' : 'Sperren'}
          </button>
          <button
            type="button"
            className="secondary"
            disabled={testing}
            onClick={() => void handleRunOutputTest(channel.channelKey)}
          >
            {testing ? 'Test laeuft...' : 'Test senden'}
          </button>
        </div>
      </article>
    );
  }

  function renderOutputTab() {
    const platformCards = Object.entries(outputSnapshot?.platforms || {}).map(([key, item]) => ({
      key,
      title: item?.label || key,
      value: item?.active ? 'Aktiv' : 'Aus',
      detail: item?.detail || '-',
      tone: item?.active ? 'success' : 'warning'
    }));
    const securityChannels = filteredOutputChannels;

    return (
      <section className="settings-panel card">
        <div className="settings-panel-header">
          <div>
            <p className="section-title">Kanaele & Output</p>
            <h2 className="page-title">Output-Kanaele, Live-Schutz und Tests</h2>
          </div>
          <div className="settings-panel-actions">
            <button type="button" className="secondary" disabled={refreshing} onClick={() => void loadSettings(true)}>
              {refreshing ? 'Lade neu...' : 'Status neu laden'}
            </button>
            <button
              type="button"
              className={outputSnapshot?.controls?.outputQueueEnabled ? 'secondary' : 'primary'}
              disabled={outputBusyKey === 'controls'}
              onClick={() =>
                void handleSaveOutputControls({
                  outputQueueEnabled: !(outputSnapshot?.controls?.outputQueueEnabled === true)
                })
              }
            >
              {outputBusyKey === 'controls'
                ? 'Speichert...'
                : outputSnapshot?.controls?.outputQueueEnabled
                  ? 'Queue deaktivieren'
                  : 'Queue aktivieren'}
            </button>
          </div>
        </div>

        <div className="tab-strip output-subtabs">
          {outputPlatformTabs.map((tab) => (
            <button
              key={tab.id}
              type="button"
              className={outputView === tab.id ? 'tab-chip active' : 'tab-chip'}
              onClick={() => setOutputView(tab.id)}
            >
              {tab.label}
            </button>
          ))}
        </div>

        {outputView === 'live' ? (
          <div className="settings-grid">
            {platformCards.map((card) => (
              <article key={card.key} className={`status-card settings-status-card settings-tone-${card.tone}`}>
                <div className="settings-card-head">
                  <p className="section-title">{card.title}</p>
                  <span className={`status-chip ${card.tone}`}>{card.value}</span>
                </div>
                <h3 className="card-title">{card.value}</h3>
                <p className="meta-text">{card.detail}</p>
              </article>
            ))}
            <article className="status-card settings-status-card settings-tone-info">
              <div className="settings-card-head">
                <p className="section-title">Queue</p>
                <span className={`status-chip ${outputSnapshot?.controls?.outputQueueEnabled ? 'success' : 'warning'}`}>
                  {outputSnapshot?.controls?.outputQueueEnabled ? 'Aktiv' : 'Aus'}
                </span>
              </div>
              <h3 className="card-title">{outputSnapshot?.controls?.outputQueueEnabled ? 'Aktiv' : 'Deaktiviert'}</h3>
              <p className="meta-text">Finale Sends laufen nur, wenn die Queue freigegeben ist.</p>
            </article>
          </div>
        ) : null}

        {outputView === 'security' ? (
          <div className="settings-grid">
            {securityChannels.map((channel) => renderOutputChannelCard(channel))}
            {!securityChannels.length ? (
              <article className="info-card">
                <p className="section-title">Sicherheitsansicht</p>
                <h3 className="card-title">Keine gesperrten Kanaele gefunden</h3>
                <p className="meta-text">
                  Sobald ein Kanal blockiert, deaktiviert oder sicherheitsbedingt uebersprungen wird, erscheint er hier.
                </p>
              </article>
            ) : null}
          </div>
        ) : null}

        {outputView !== 'live' && outputView !== 'security' ? (
          <div className="settings-grid">
            {filteredOutputChannels.map((channel) => renderOutputChannelCard(channel))}
            {!filteredOutputChannels.length ? (
              <article className="info-card">
                <p className="section-title">Keine Kanaele</p>
                <h3 className="card-title">Noch keine Ziele gespeichert</h3>
                <p className="meta-text">Sobald fuer diese Plattform Ziele vorhanden sind, erscheinen sie hier mit Status und Test-Button.</p>
              </article>
            ) : null}
          </div>
        ) : null}
      </section>
    );
  }

  function renderTelegramTab() {
    return (
      <section className="settings-panel card">
        <div className="settings-panel-header">
          <div>
            <p className="section-title">Telegram</p>
            <h2 className="page-title">Reader, Session und Output Gruppen</h2>
          </div>
          <div className="settings-panel-actions">
            <button type="button" className="secondary" disabled={refreshing} onClick={() => void loadSettings(true)}>
              Reader Status neu laden
            </button>
            <Link to="/copybot/telegram-sources" className="tab-chip">
              Quellen oeffnen
            </Link>
          </div>
        </div>
        <div className="settings-grid">
          <article className="status-card">
            <p className="section-title">Reader Status</p>
            <h3 className="card-title">{liveStatus?.telegram?.readerStatus || '-'}</h3>
            <p className="meta-text">
              Aktive Session {liveStatus?.telegram?.activeSessionName || '-'} | letzte Nachricht {formatDateTime(liveStatus?.telegram?.lastMessageAt)}
            </p>
          </article>
          <article className="status-card">
            <p className="section-title">User API</p>
            <h3 className="card-title">{modules?.telegramUserApi?.apiIdConfigured ? 'vorhanden' : 'fehlt'}</h3>
            <p className="meta-text">
              Hash {modules?.telegramUserApi?.apiHashConfigured ? 'vorhanden' : 'fehlt'} | Modus {liveStatus?.telegram?.loginMode || '-'} | Telefon {liveStatus?.telegram?.phoneMasked || '-'}
            </p>
          </article>
          <article className="status-card">
            <p className="section-title">Bot Output</p>
            <h3 className="card-title">{liveStatus?.telegram?.botStatus || '-'}</h3>
            <p className="meta-text">
              {liveStatus?.telegram?.botTargets || 0} aktive Ziele | Status {liveStatus?.telegram?.outputGroupsStatus || '-'}
            </p>
          </article>
          <article className="info-card">
            <p className="section-title">Maskierte Zugaenge</p>
            <h3 className="card-title">{modules?.telegramBot?.maskedToken || 'kein Token'}</h3>
            <p className="meta-text">
              Default Chat {modules?.telegramBot?.maskedDefaultChatId || '-'} | Test Chat {modules?.telegramBot?.maskedTestChatId || '-'}
            </p>
          </article>
        </div>
      </section>
    );
  }

  function renderAmazonTab() {
    return (
      <section className="settings-panel card">
        <div className="settings-panel-header">
          <div>
            <p className="section-title">Amazon</p>
            <h2 className="page-title">Creator API, PAAPI und letzte Fehlerlage</h2>
          </div>
          <div className="settings-panel-actions">
            <button type="button" className="secondary" disabled={refreshing} onClick={() => void loadSettings(true)}>
              Status neu laden
            </button>
            <Link to="/logs" className="tab-chip">
              API Logs oeffnen
            </Link>
          </div>
        </div>
        <div className="settings-grid">
          <article className="status-card">
            <p className="section-title">Creator API</p>
            <h3 className="card-title">{liveStatus?.amazon?.creatorApiStatus || '-'}</h3>
            <p className="meta-text">
              Partner Tag {liveStatus?.amazon?.creatorPartnerTagMasked || '-'} | Konfiguriert {liveStatus?.amazon?.creatorApiConfigured ? 'ja' : 'nein'}
            </p>
          </article>
          <article className="status-card">
            <p className="section-title">PAAPI</p>
            <h3 className="card-title">{liveStatus?.amazon?.paapiStatus || '-'}</h3>
            <p className="meta-text">
              Partner Tag {liveStatus?.amazon?.partnerTagMasked || '-'} | Konfiguriert {liveStatus?.amazon?.paapiConfigured ? 'ja' : 'nein'}
            </p>
          </article>
          <article className="info-card">
            <p className="section-title">Letzte erfolgreiche Anfrage</p>
            <h3 className="card-title">{formatDateTime(liveStatus?.amazon?.lastSuccessfulRequest)}</h3>
            <p className="meta-text">Zuletzt bekannte Erfolgsspur aus dem Backend-Logspeicher.</p>
          </article>
          <article className="warning-card">
            <p className="section-title">Letzte Fehlermeldung</p>
            <h3 className="card-title">{formatDateTime(liveStatus?.amazon?.lastErrorAt)}</h3>
            <p className="meta-text">{liveStatus?.amazon?.lastErrorMessage || 'Keine aktuelle Fehlermeldung gespeichert.'}</p>
          </article>
        </div>
      </section>
    );
  }

  function renderWhatsappTab() {
    return (
      <section className="settings-panel card">
        <div className="settings-panel-header">
          <div>
            <p className="section-title">WhatsApp</p>
            <h2 className="page-title">Client, Session, Worker und Alert-Lage</h2>
          </div>
          <div className="settings-panel-actions">
            <button type="button" className="secondary" disabled={refreshing} onClick={() => void loadSettings(true)}>
              Status neu laden
            </button>
            <Link to="/publishing/whatsapp" className="tab-chip">
              WhatsApp Output oeffnen
            </Link>
          </div>
        </div>
        <div className="settings-grid">
          <article className="status-card">
            <p className="section-title">Client</p>
            <h3 className="card-title">{liveStatus?.whatsapp?.clientStatus || '-'}</h3>
            <p className="meta-text">Retry Limit {liveStatus?.whatsapp?.retryLimit || 0} | Queue offen {liveStatus?.whatsapp?.queueOpen || 0}</p>
          </article>
          <article className="info-card">
            <p className="section-title">Gateway</p>
            <h3 className="card-title">{liveStatus?.whatsapp?.endpointStatus || '-'}</h3>
            <p className="meta-text">{liveStatus?.whatsapp?.endpointMasked || 'Kein Endpoint gesetzt'} | Control {liveStatus?.whatsapp?.controlStatus || '-'}</p>
          </article>
          <article className="status-card">
            <p className="section-title">Sender</p>
            <h3 className="card-title">{liveStatus?.whatsapp?.senderStatus || '-'}</h3>
            <p className="meta-text">{liveStatus?.whatsapp?.sender || 'Nicht gesetzt'}</p>
          </article>
          <article className="status-card">
            <p className="section-title">Session</p>
            <h3 className="card-title">{liveStatus?.whatsapp?.connectionStatus || '-'}</h3>
            <p className="meta-text">
              Worker {liveStatus?.whatsapp?.workerStatus || '-'} | Health {liveStatus?.whatsapp?.healthStatus || '-'}
            </p>
          </article>
          <article className="info-card">
            <p className="section-title">Letzter Post</p>
            <h3 className="card-title">{formatDateTime(liveStatus?.whatsapp?.lastSuccessfulPostAt)}</h3>
            <p className="meta-text">Zuletzt erfolgreicher WhatsApp Output.</p>
          </article>
          <article className="warning-card">
            <p className="section-title">Letzter Fehler</p>
            <h3 className="card-title">{formatDateTime(liveStatus?.whatsapp?.lastErrorAt)}</h3>
            <p className="meta-text">{liveStatus?.whatsapp?.lastError || 'Keine aktuelle Fehlermeldung gespeichert.'}</p>
          </article>
        </div>
      </section>
    );
  }

  function renderFacebookTab() {
    return (
      <section className="settings-panel card">
        <div className="settings-panel-header">
          <div>
            <p className="section-title">Facebook</p>
            <h2 className="page-title">Worker und Session Modus</h2>
          </div>
          <Link to="/publishing/facebook" className="tab-chip">
            Worker oeffnen
          </Link>
        </div>
        <div className="settings-grid">
          <article className="status-card">
            <p className="section-title">Worker</p>
            <h3 className="card-title">{liveStatus?.facebook?.workerStatus || '-'}</h3>
            <p className="meta-text">Retry Limit {liveStatus?.facebook?.retryLimit || 0}</p>
          </article>
          <article className="info-card">
            <p className="section-title">Session Modus</p>
            <h3 className="card-title">{liveStatus?.facebook?.sessionMode || '-'}</h3>
            <p className="meta-text">Default Ziel {liveStatus?.facebook?.defaultTargetMasked || '-'}</p>
          </article>
        </div>
      </section>
    );
  }

  function renderSystemTab() {
    return (
      <section className="settings-panel card">
        <div className="settings-panel-header">
          <div>
            <p className="section-title">System</p>
            <h2 className="page-title">Backend, Scheduler, Queue und Neustart</h2>
          </div>
          <div className="settings-panel-actions">
            <button type="button" className="secondary" disabled={refreshing} onClick={() => void loadSettings(true)}>
              Status neu laden
            </button>
            <button
              className="primary"
              type="button"
              onClick={handleRestartBackend}
              disabled={loading || restarting || restartLocked}
            >
              {restarting ? 'Neustart laeuft...' : 'Backend neu starten'}
            </button>
          </div>
        </div>
        <div className="settings-grid">
          <article className="status-card">
            <p className="section-title">Backend</p>
            <h3 className="card-title">{liveStatus?.system?.backendOnline ? 'Online' : 'Offline'}</h3>
            <p className="meta-text">Gestartet {formatDateTime(liveStatus?.system?.backendStartedAt)}</p>
          </article>
          <article className="status-card">
            <p className="section-title">Queue Worker</p>
            <h3 className="card-title">{buildStatusText(liveStatus?.system?.queueWorkerActive, 'Aktiv', 'Pausiert')}</h3>
            <p className="meta-text">{liveStatus?.system?.openQueueCount || 0} offene Jobs</p>
          </article>
          <article className="status-card">
            <p className="section-title">Scheduler</p>
            <h3 className="card-title">{buildStatusText(liveStatus?.system?.schedulerActive, 'Aktiv', 'Pausiert')}</h3>
            <p className="meta-text">
              Keepa {buildStatusText(liveStatus?.system?.keepaSchedulerActive, 'an', 'aus')} | Werbung {buildStatusText(liveStatus?.system?.advertisingSchedulerActive, 'an', 'aus')}
            </p>
          </article>
          <article className="warning-card">
            <p className="section-title">Letzter Fehler</p>
            <h3 className="card-title">{formatDateTime(liveStatus?.system?.lastErrorAt)}</h3>
            <p className="meta-text">{liveStatus?.system?.lastError || restartStatus || 'Kein aktueller Fehler gespeichert.'}</p>
          </article>
        </div>
      </section>
    );
  }

  function renderSecurityTab() {
    return (
      <section className="settings-panel card">
        <div className="settings-panel-header">
          <div>
            <p className="section-title">Sicherheit</p>
            <h2 className="page-title">Maskierte Zugaenge und Copy Button Pflege</h2>
          </div>
        </div>
        <div className="settings-grid">
          <article className="info-card">
            <p className="section-title">Maskierte Pfade</p>
            <h3 className="card-title">Nur sichere Hinweise im UI</h3>
            <p className="meta-text">ENV {liveStatus?.security?.envPathMasked || '-'} | DB {liveStatus?.security?.dbPathMasked || '-'}</p>
          </article>
          <article className="status-card">
            <p className="section-title">Copybot Verlauf</p>
            <h3 className="card-title">{liveStatus?.copybot?.enabled ? 'Aktiv' : 'Pausiert'}</h3>
            <p className="meta-text">
              Letzter Wechsel {formatDateTime(liveStatus?.copybot?.lastStatusChange)} | durch {liveStatus?.copybot?.changedBy || '-'} | Quelle {liveStatus?.copybot?.changedFrom || '-'}
            </p>
          </article>
        </div>

        {isAdmin ? (
          <section className="rule-card settings-inner-panel">
            <div>
              <p className="section-title">Telegram Copy Button Text</p>
              <h3 className="card-title">Anzeige unter Rabattcode Posts</h3>
              <p className="meta-text">Der Text bleibt serverseitig gespeichert und wird nach dem Speichern direkt nachgeladen.</p>
            </div>
            <label className="settings-field">
              <span className="section-title">Text</span>
              <input
                value={telegramCopyButtonText}
                onChange={(event) => setTelegramCopyButtonText(event.target.value)}
                placeholder={DEFAULT_TELEGRAM_COPY_BUTTON_TEXT}
                disabled={loading || saving}
              />
            </label>
            <div className="settings-panel-actions">
              <button className="primary" onClick={handleSave} disabled={loading || saving}>
                {saving ? 'Speichert...' : 'Speichern'}
              </button>
              <span className="meta-text">{settingsStatus || 'Nur maskierte und sichere Statusdaten im Frontend.'}</span>
            </div>
          </section>
        ) : null}
      </section>
    );
  }

  function renderActiveTab() {
    if (activeTab === 'output') {
      return renderOutputTab();
    }
    if (activeTab === 'telegram') {
      return renderTelegramTab();
    }
    if (activeTab === 'amazon') {
      return renderAmazonTab();
    }
    if (activeTab === 'whatsapp') {
      return renderWhatsappTab();
    }
    if (activeTab === 'facebook') {
      return renderFacebookTab();
    }
    if (activeTab === 'system') {
      return renderSystemTab();
    }
    if (activeTab === 'security') {
      return renderSecurityTab();
    }
    return renderLiveTab();
  }

  return (
    <Layout>
      <div className="settings-page">
        <section className="card settings-hero">
          <div className="settings-hero-copy">
            <p className="section-title">Einstellungen</p>
            <h1 className="page-title">Zugaenge, Live Status und sichere Systempflege</h1>
            <p className="page-subtitle">
              Alle sensiblen Daten bleiben im Backend. Im UI erscheinen nur maskierte Hinweise, Live Status und sichere Admin-Aktionen.
            </p>
          </div>
          <div className="settings-hero-grid">
            {heroCards.map((card) => (
              <article key={card.title} className={`status-card settings-status-card settings-tone-${card.tone}`}>
                <div className="settings-card-head">
                  <p className="section-title">{card.title}</p>
                  <span className={`status-chip ${card.tone}`}>{card.value}</span>
                </div>
                <h2 className="card-value">{card.value}</h2>
                <p className="meta-text">{card.detail}</p>
              </article>
            ))}
          </div>
          {settingsStatus ? (
            <div className="warning-card">
              <p className="section-title">Statushinweis</p>
              <p className="meta-text">{settingsStatus}</p>
            </div>
          ) : null}
        </section>

        <section className="card settings-tabs-shell">
          <div className="tab-strip">
            {tabs.map((tab) => (
              <button
                key={tab.id}
                type="button"
                className={activeTab === tab.id ? 'tab-chip active' : 'tab-chip'}
                onClick={() => setActiveTab(tab.id)}
              >
                {tab.label}
              </button>
            ))}
          </div>
          {loading ? <p className="meta-text">Settings werden geladen...</p> : renderActiveTab()}
        </section>
      </div>

      {toast ? (
        <div className={`toast ${toast.tone === 'error' ? 'error' : toast.tone === 'info' ? 'info' : 'success'}`}>
          <p>{toast.message}</p>
        </div>
      ) : null}
    </Layout>
  );
}

export default SettingsPage;
