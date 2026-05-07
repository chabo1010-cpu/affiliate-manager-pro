import { useEffect, useMemo, useState } from 'react';
import Layout from '../components/layout/Layout';
import { useAuth } from '../context/AuthContext';
import './Advertising.css';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:4000';
const MIN_MODULE_SLOTS = 5;

const PRIORITY_LABELS = {
  low: 'niedrig',
  medium: 'mittel',
  high: 'hoch',
  very_high: 'sehr hoch'
};

const FREQUENCY_LABELS = {
  once: 'einmalig',
  daily: 'taeglich',
  weekly: 'woechentlich',
  weekdays: 'bestimmte Wochentage',
  every_x_hours: 'alle X Stunden',
  every_x_days: 'alle X Tage'
};

const WEEKDAY_OPTIONS = [
  { value: 'mon', label: 'Mo' },
  { value: 'tue', label: 'Di' },
  { value: 'wed', label: 'Mi' },
  { value: 'thu', label: 'Do' },
  { value: 'fri', label: 'Fr' },
  { value: 'sat', label: 'Sa' },
  { value: 'sun', label: 'So' }
];

const EMPTY_DASHBOARD = {
  overview: {
    activeModuleCount: 0,
    plannedTodayCount: 0,
    nextPlannedPost: null,
    lastSuccess: null,
    lastFailure: null
  },
  modules: [],
  upcoming: [],
  history: [],
  logs: [],
  channelCatalog: {
    telegram: {
      enabled: false,
      tokenConfigured: false,
      fallbackChatConfigured: false,
      targets: [],
      effectiveTargets: []
    },
    whatsapp: {
      enabled: false,
      endpointConfigured: false,
      senderConfigured: false,
      sender: '',
      retryLimit: 0
    }
  },
  publishing: {
    queueCount: 0,
    failedCount: 0,
    sentCount: 0
  }
};

function formatDateTime(value) {
  if (!value) {
    return '-';
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return new Intl.DateTimeFormat('de-DE', {
    dateStyle: 'short',
    timeStyle: 'short'
  }).format(date);
}

function getToneClass(value) {
  const normalized = String(value || '').toLowerCase();
  if (normalized.includes('failed') || normalized.includes('error') || normalized.includes('pause')) {
    return 'danger';
  }

  if (normalized.includes('retry') || normalized.includes('queued') || normalized.includes('warning')) {
    return 'warning';
  }

  if (normalized.includes('sent') || normalized.includes('active') || normalized.includes('success')) {
    return 'success';
  }

  return 'info';
}

function getTodayDateInput() {
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, '0');
  const day = String(now.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

function createModuleShell(slotNumber, overrides = {}) {
  return {
    id: overrides.id ?? null,
    slotNumber,
    moduleName: overrides.moduleName || `Werbemodul ${slotNumber}`,
    status: overrides.status || 'paused',
    priority: overrides.priority || 'medium',
    startDate: overrides.startDate || getTodayDateInput(),
    endDate: overrides.endDate || '',
    frequencyMode: overrides.frequencyMode || 'daily',
    timesText: overrides.timesText || '09:00',
    weekdays: Array.isArray(overrides.weekdays) ? overrides.weekdays : [],
    intervalHours: Number(overrides.intervalHours || 6),
    intervalDays: Number(overrides.intervalDays || 1),
    maxPerDay: Number(overrides.maxPerDay || 1),
    mainText: overrides.mainText || '',
    extraText: overrides.extraText || '',
    imageDataUrl: overrides.imageDataUrl || '',
    imageFilename: overrides.imageFilename || '',
    telegramEnabled: overrides.telegramEnabled === undefined ? true : overrides.telegramEnabled === true,
    telegramTargetIds: Array.isArray(overrides.telegramTargetIds) ? overrides.telegramTargetIds : [],
    whatsappEnabled: overrides.whatsappEnabled === true,
    whatsappTargetsText: overrides.whatsappTargetsText || ''
  };
}

function moduleToForm(module = {}) {
  return createModuleShell(Number(module.slotNumber || module.id || 1), {
    id: module.id ?? null,
    moduleName: module.moduleName || '',
    status: module.status || 'paused',
    priority: module.priority || 'medium',
    startDate: module.startDate || '',
    endDate: module.endDate || '',
    frequencyMode: module.frequencyMode || 'daily',
    timesText: Array.isArray(module.times) ? module.times.join('\n') : '09:00',
    weekdays: Array.isArray(module.weekdays) ? module.weekdays : [],
    intervalHours: Number(module.intervalHours || 6),
    intervalDays: Number(module.intervalDays || 1),
    maxPerDay: Number(module.maxPerDay || 1),
    mainText: module.mainText || '',
    extraText: module.extraText || '',
    imageDataUrl: module.imageDataUrl || '',
    imageFilename: module.imageFilename || '',
    telegramEnabled: module.telegramEnabled === true,
    telegramTargetIds: Array.isArray(module.telegramTargetIds) ? module.telegramTargetIds : [],
    whatsappEnabled: module.whatsappEnabled === true,
    whatsappTargetsText: Array.isArray(module.whatsappTargets)
      ? module.whatsappTargets.map((item) => [item.ref || '', item.label || ''].filter(Boolean).join('|')).join('\n')
      : ''
  });
}

function mergeDashboard(data = {}) {
  return {
    ...EMPTY_DASHBOARD,
    ...data,
    overview: {
      ...EMPTY_DASHBOARD.overview,
      ...(data.overview || {})
    },
    channelCatalog: {
      ...EMPTY_DASHBOARD.channelCatalog,
      ...(data.channelCatalog || {}),
      telegram: {
        ...EMPTY_DASHBOARD.channelCatalog.telegram,
        ...(data.channelCatalog?.telegram || {})
      },
      whatsapp: {
        ...EMPTY_DASHBOARD.channelCatalog.whatsapp,
        ...(data.channelCatalog?.whatsapp || {})
      }
    },
    publishing: {
      ...EMPTY_DASHBOARD.publishing,
      ...(data.publishing || {})
    }
  };
}

function buildModuleForms(modules = []) {
  const mappedModules = Array.isArray(modules) ? modules.map((module) => moduleToForm(module)) : [];
  const slotMap = new Map(mappedModules.map((module) => [module.slotNumber, module]));
  const baseModules = Array.from({ length: MIN_MODULE_SLOTS }, (_, index) => {
    const slotNumber = index + 1;
    return createModuleShell(slotNumber, slotMap.get(slotNumber));
  });
  const extraModules = mappedModules.filter((module) => module.slotNumber > MIN_MODULE_SLOTS);

  return [...baseModules, ...extraModules].sort((left, right) => left.slotNumber - right.slotNumber);
}

function getBusyKey(slotNumber) {
  return `slot-${slotNumber}`;
}

function AdvertisingPage() {
  const { user } = useAuth();
  const isAdmin = user?.role === 'admin';
  const [dashboard, setDashboard] = useState(EMPTY_DASHBOARD);
  const [moduleForms, setModuleForms] = useState(() => buildModuleForms());
  const [expandedSlot, setExpandedSlot] = useState(1);
  const [status, setStatus] = useState('');
  const [loading, setLoading] = useState(true);
  const [busyKey, setBusyKey] = useState('');

  useEffect(() => {
    void loadPage();
  }, [user?.role]);

  async function apiFetch(path, options = {}) {
    const response = await fetch(`${API_BASE_URL}${path}`, {
      ...options,
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
  }

  async function loadPage() {
    try {
      setLoading(true);
      setStatus('');
      const data = mergeDashboard(await apiFetch('/api/advertising/dashboard'));
      setDashboard(data);
      setModuleForms(buildModuleForms(data.modules || []));
    } catch (error) {
      setDashboard(EMPTY_DASHBOARD);
      setModuleForms((current) => (current.length ? current : buildModuleForms()));
      setStatus(error instanceof Error ? error.message : 'Werbung konnte nicht geladen werden.');
    } finally {
      setLoading(false);
    }
  }

  function updateModuleForm(slotNumber, patch) {
    setModuleForms((current) =>
      current.map((item) =>
        item.slotNumber === slotNumber
          ? {
              ...item,
              ...patch
            }
          : item
      )
    );
  }

  function toggleWeekday(slotNumber, weekday) {
    setModuleForms((current) =>
      current.map((item) => {
        if (item.slotNumber !== slotNumber) {
          return item;
        }

        const nextSet = new Set(item.weekdays || []);
        if (nextSet.has(weekday)) {
          nextSet.delete(weekday);
        } else {
          nextSet.add(weekday);
        }

        return {
          ...item,
          weekdays: Array.from(nextSet)
        };
      })
    );
  }

  function toggleTelegramTarget(slotNumber, targetId) {
    setModuleForms((current) =>
      current.map((item) => {
        if (item.slotNumber !== slotNumber) {
          return item;
        }

        const nextSet = new Set(item.telegramTargetIds || []);
        if (nextSet.has(targetId)) {
          nextSet.delete(targetId);
        } else {
          nextSet.add(targetId);
        }

        return {
          ...item,
          telegramTargetIds: Array.from(nextSet)
        };
      })
    );
  }

  async function onImageChange(slotNumber, file) {
    if (!file) {
      updateModuleForm(slotNumber, {
        imageDataUrl: '',
        imageFilename: ''
      });
      return;
    }

    const dataUrl = await new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(typeof reader.result === 'string' ? reader.result : '');
      reader.onerror = () => reject(new Error('Bild konnte nicht gelesen werden.'));
      reader.readAsDataURL(file);
    });

    updateModuleForm(slotNumber, {
      imageDataUrl: dataUrl,
      imageFilename: file.name
    });
  }

  function buildModulePayload(form) {
    return {
      moduleName: form.moduleName,
      status: form.status,
      priority: form.priority,
      startDate: form.startDate,
      endDate: form.endDate || '',
      frequencyMode: form.frequencyMode,
      times: form.timesText,
      weekdays: form.weekdays,
      intervalHours: form.intervalHours,
      intervalDays: form.intervalDays,
      maxPerDay: form.maxPerDay,
      mainText: form.mainText,
      extraText: form.extraText,
      imageDataUrl: form.imageDataUrl,
      imageFilename: form.imageFilename,
      telegramEnabled: form.telegramEnabled,
      telegramTargetIds: form.telegramTargetIds,
      whatsappEnabled: form.whatsappEnabled,
      whatsappTargets: form.whatsappTargetsText
    };
  }

  async function persistModule(form) {
    if (!form?.id) {
      throw new Error('Werbemodul ist noch nicht mit der Datenbank verbunden.');
    }

    await apiFetch(`/api/advertising/modules/${form.id}`, {
      method: 'PUT',
      body: JSON.stringify(buildModulePayload(form))
    });
  }

  async function saveModule(slotNumber) {
    const form = moduleForms.find((item) => item.slotNumber === slotNumber);
    if (!form) {
      return;
    }

    try {
      setBusyKey(getBusyKey(slotNumber));
      await persistModule(form);
      setStatus(`Werbemodul ${form.moduleName || `Werbemodul ${slotNumber}`} gespeichert.`);
      await loadPage();
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'Werbemodul konnte nicht gespeichert werden.');
    } finally {
      setBusyKey('');
    }
  }

  async function togglePause(slotNumber, paused) {
    const form = moduleForms.find((item) => item.slotNumber === slotNumber);
    if (!form?.id) {
      setStatus('Werbemodul ist noch nicht mit der Datenbank verbunden.');
      return;
    }

    try {
      setBusyKey(getBusyKey(slotNumber));
      await apiFetch(`/api/advertising/modules/${form.id}/pause`, {
        method: 'POST',
        body: JSON.stringify({ paused })
      });
      setStatus(paused ? 'Werbemodul pausiert.' : 'Werbemodul aktiviert.');
      await loadPage();
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'Status konnte nicht geaendert werden.');
    } finally {
      setBusyKey('');
    }
  }

  async function sendTest(slotNumber) {
    const form = moduleForms.find((item) => item.slotNumber === slotNumber);
    if (!form) {
      return;
    }

    try {
      setBusyKey(getBusyKey(slotNumber));
      await persistModule(form);
      await apiFetch(`/api/advertising/modules/${form.id}/test`, {
        method: 'POST',
        body: JSON.stringify({})
      });
      setStatus('Testsendung ueber Queue erzeugt.');
      await loadPage();
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'Testsendung fehlgeschlagen.');
    } finally {
      setBusyKey('');
    }
  }

  async function syncScheduler() {
    try {
      setBusyKey('sync');
      await apiFetch('/api/advertising/sync', {
        method: 'POST',
        body: JSON.stringify({})
      });
      setStatus('Werbung wurde synchronisiert.');
      await loadPage();
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'Synchronisierung fehlgeschlagen.');
    } finally {
      setBusyKey('');
    }
  }

  const nextOccurrenceByModule = useMemo(() => {
    const map = new Map();
    (dashboard.upcoming || []).forEach((item) => {
      if (!map.has(item.moduleId)) {
        map.set(item.moduleId, item);
      }
    });
    return map;
  }, [dashboard.upcoming]);

  const telegramTargets = dashboard.channelCatalog.telegram.targets || [];

  return (
    <Layout>
      <div className="ad-page">
        <div className="ad-sticky-top">
          <section className="card ad-hero">
            <div className="ad-hero-grid">
              <div className="ad-hero-copy">
                <p className="section-title">Werbung</p>
                <h1 className="page-title">Freie Werbemodule fuer automatische Werbe- und Infoposts</h1>
                <p className="page-subtitle">
                  Werbung bleibt ein eigener Pfad ueber Queue, Retry, Publisher, Dashboard und Historie. Die bestehende
                  Struktur bleibt erhalten.
                </p>
              </div>
              <div className="ad-hero-actions">
                <span className="badge">5 Module immer sichtbar</span>
                <span className="badge">Queue und Publisher bleiben aktiv</span>
                {loading ? <span className="badge">Lade Daten...</span> : null}
                {isAdmin ? (
                  <button type="button" className="secondary" onClick={() => void syncScheduler()} disabled={busyKey === 'sync'}>
                    {busyKey === 'sync' ? 'Synchronisiert...' : 'Jetzt synchronisieren'}
                  </button>
                ) : null}
              </div>
            </div>
            {status ? (
              <div className="ad-inline-alert">
                <span className={`status-chip ${getToneClass(status)}`}>{getToneClass(status)}</span>
                <p>{status}</p>
              </div>
            ) : null}
          </section>

          <section className="card ad-panel ad-overview-panel">
            <div className="ad-panel-header">
              <div>
                <p className="section-title">1. Uebersicht</p>
                <h2 className="page-title">Status, Planung und letzte Ausfuehrungen</h2>
              </div>
            </div>
            <div className="ad-overview-grid">
              <article className="ad-card ad-tone-success">
                <div className="ad-card-head">
                  <p className="section-title">Aktive Module</p>
                  <span className="status-chip success">live</span>
                </div>
                <h3>{dashboard.overview.activeModuleCount || 0}</h3>
                <p>Frei konfigurierbare Werbemodule mit eigenem Timing und eigenen Zielkanaelen.</p>
              </article>
              <article className="ad-card ad-tone-info">
                <div className="ad-card-head">
                  <p className="section-title">Geplant heute</p>
                  <span className="status-chip info">today</span>
                </div>
                <h3>{dashboard.overview.plannedTodayCount || 0}</h3>
                <p>Anzahl der noch geplanten Werbe-Posts fuer den aktuellen Tag.</p>
              </article>
              <article className="ad-card ad-tone-info">
                <div className="ad-card-head">
                  <p className="section-title">Naechster Post</p>
                  <span className="status-chip info">next</span>
                </div>
                <h3>{dashboard.overview.nextPlannedPost?.moduleName || '-'}</h3>
                <p>{formatDateTime(dashboard.overview.nextPlannedPost?.scheduledFor)}</p>
              </article>
              <article className="ad-card ad-tone-success">
                <div className="ad-card-head">
                  <p className="section-title">Letzte erfolgreiche Ausfuehrung</p>
                  <span className="status-chip success">ok</span>
                </div>
                <h3>{dashboard.overview.lastSuccess?.moduleName || '-'}</h3>
                <p>{formatDateTime(dashboard.overview.lastSuccess?.sentAt || dashboard.overview.lastSuccess?.updatedAt)}</p>
              </article>
              <article className="ad-card ad-tone-warning">
                <div className="ad-card-head">
                  <p className="section-title">Letzte fehlgeschlagene Ausfuehrung</p>
                  <span className="status-chip warning">warn</span>
                </div>
                <h3>{dashboard.overview.lastFailure?.moduleName || '-'}</h3>
                <p>{formatDateTime(dashboard.overview.lastFailure?.failedAt || dashboard.overview.lastFailure?.updatedAt)}</p>
              </article>
            </div>
          </section>
        </div>

        <section className="card ad-panel ad-module-panel">
          <div className="ad-panel-header">
            <div>
              <p className="section-title">2. Freie Werbemodule</p>
              <h2 className="page-title">Mindestens 5 unabhaengige Module</h2>
            </div>
            <span className="ad-header-note">Leere Module bleiben sichtbar und koennen direkt befuellt werden.</span>
          </div>

          <div className="ad-module-stack">
            {moduleForms.map((module) => {
              const nextOccurrence = module.id ? nextOccurrenceByModule.get(module.id) : null;
              const hasTelegramTargets = telegramTargets.length > 0;
              const moduleBusy = busyKey === getBusyKey(module.slotNumber);
              const isExpanded = expandedSlot === module.slotNumber;

              return (
                <article key={module.id || `slot-${module.slotNumber}`} className={`ad-module-card ${isExpanded ? 'is-open' : ''}`}>
                  <div className="ad-module-head">
                    <div className="ad-module-title">
                      <div className="ad-module-title-top">
                        <span className="badge">Werbemodul {module.slotNumber}</span>
                        <span className={`status-chip ${module.status === 'active' ? 'success' : 'warning'}`}>
                          {module.status === 'active' ? 'aktiv' : 'pausiert'}
                        </span>
                      </div>
                      <span className="text-muted">
                        {PRIORITY_LABELS[module.priority]} | {FREQUENCY_LABELS[module.frequencyMode]} |{' '}
                        {nextOccurrence ? formatDateTime(nextOccurrence.scheduledFor) : 'kein naechster Termin'}
                      </span>
                    </div>
                    <div className="ad-module-actions">
                      <button
                        type="button"
                        className="secondary"
                        onClick={() => setExpandedSlot((current) => (current === module.slotNumber ? null : module.slotNumber))}
                      >
                        {isExpanded ? 'Schliessen' : 'Bearbeiten'}
                      </button>
                      <button
                        type="button"
                        className="secondary"
                        disabled={!isAdmin || !module.id || moduleBusy || loading}
                        onClick={() => void sendTest(module.slotNumber)}
                      >
                        Test senden
                      </button>
                    </div>
                  </div>

                  <div className="ad-module-toolbar">
                    <label className="ad-inline-field">
                      <span>Name</span>
                      <input
                        value={module.moduleName}
                        onChange={(event) => updateModuleForm(module.slotNumber, { moduleName: event.target.value })}
                        disabled={!isAdmin}
                      />
                    </label>
                    <label className="ad-inline-field ad-inline-field-status">
                      <span>Status</span>
                      <select
                        value={module.status}
                        onChange={(event) => updateModuleForm(module.slotNumber, { status: event.target.value })}
                        disabled={!isAdmin}
                      >
                        <option value="active">aktiv</option>
                        <option value="paused">pausiert</option>
                      </select>
                    </label>
                  </div>

                  {isExpanded ? (
                    <div className="ad-module-body">
                      <div className="ad-module-grid">
                        <label>
                          <span>Startdatum</span>
                          <input
                            type="date"
                            value={module.startDate}
                            onChange={(event) => updateModuleForm(module.slotNumber, { startDate: event.target.value })}
                          />
                        </label>
                        <label>
                          <span>Enddatum optional</span>
                          <input
                            type="date"
                            value={module.endDate}
                            onChange={(event) => updateModuleForm(module.slotNumber, { endDate: event.target.value })}
                          />
                        </label>
                        <label>
                          <span>Haeufigkeit</span>
                          <select
                            value={module.frequencyMode}
                            onChange={(event) => updateModuleForm(module.slotNumber, { frequencyMode: event.target.value })}
                          >
                            {Object.entries(FREQUENCY_LABELS).map(([value, label]) => (
                              <option key={value} value={value}>
                                {label}
                              </option>
                            ))}
                          </select>
                        </label>
                        <label>
                          <span>Prioritaet</span>
                          <select value={module.priority} onChange={(event) => updateModuleForm(module.slotNumber, { priority: event.target.value })}>
                            {Object.entries(PRIORITY_LABELS).map(([value, label]) => (
                              <option key={value} value={value}>
                                {label}
                              </option>
                            ))}
                          </select>
                        </label>
                        <label>
                          <span>Max. Ausspielung pro Tag</span>
                          <input
                            type="number"
                            min="1"
                            value={module.maxPerDay}
                            onChange={(event) => updateModuleForm(module.slotNumber, { maxPerDay: Number(event.target.value || 1) })}
                          />
                        </label>
                        <label>
                          <span>Alle X Stunden</span>
                          <input
                            type="number"
                            min="1"
                            value={module.intervalHours}
                            onChange={(event) => updateModuleForm(module.slotNumber, { intervalHours: Number(event.target.value || 1) })}
                            disabled={module.frequencyMode !== 'every_x_hours'}
                          />
                        </label>
                        <label>
                          <span>Alle X Tage</span>
                          <input
                            type="number"
                            min="1"
                            value={module.intervalDays}
                            onChange={(event) => updateModuleForm(module.slotNumber, { intervalDays: Number(event.target.value || 1) })}
                            disabled={module.frequencyMode !== 'every_x_days'}
                          />
                        </label>
                        <label className="ad-span-2">
                          <span>Uhrzeiten</span>
                          <textarea
                            rows={4}
                            value={module.timesText}
                            onChange={(event) => updateModuleForm(module.slotNumber, { timesText: event.target.value })}
                            placeholder={'09:00\n15:00\n22:00'}
                          />
                        </label>
                        <label className="ad-span-2">
                          <span>Haupttext</span>
                          <textarea
                            rows={5}
                            value={module.mainText}
                            onChange={(event) => updateModuleForm(module.slotNumber, { mainText: event.target.value })}
                          />
                        </label>
                        <label className="ad-span-2">
                          <span>Zusatztext optional</span>
                          <textarea
                            rows={3}
                            value={module.extraText}
                            onChange={(event) => updateModuleForm(module.slotNumber, { extraText: event.target.value })}
                          />
                        </label>
                      </div>

                      {module.frequencyMode === 'weekdays' ? (
                        <div className="ad-weekdays">
                          {WEEKDAY_OPTIONS.map((weekday) => (
                            <label key={weekday.value} className="ad-weekday-chip">
                              <span>{weekday.label}</span>
                              <input
                                type="checkbox"
                                checked={module.weekdays.includes(weekday.value)}
                                onChange={() => toggleWeekday(module.slotNumber, weekday.value)}
                              />
                            </label>
                          ))}
                        </div>
                      ) : null}

                      <div className="ad-subsection">
                        <div className="ad-subsection-header">
                          <strong>Zielkanaele</strong>
                          <span className="text-muted">Ausgabe laeuft weiter ueber die bestehende Publishing-Queue.</span>
                        </div>

                        <div className="ad-channel-grid">
                          <label className="ad-toggle">
                            <span>Telegram aktiv</span>
                            <input
                              type="checkbox"
                              checked={module.telegramEnabled}
                              onChange={(event) => updateModuleForm(module.slotNumber, { telegramEnabled: event.target.checked })}
                            />
                          </label>
                          <label className="ad-toggle">
                            <span>WhatsApp aktiv</span>
                            <input
                              type="checkbox"
                              checked={module.whatsappEnabled}
                              onChange={(event) => updateModuleForm(module.slotNumber, { whatsappEnabled: event.target.checked })}
                            />
                          </label>
                        </div>

                        {module.telegramEnabled ? (
                          <div className="ad-targets-box">
                            <p className="section-title">Telegram Ziele</p>
                            {hasTelegramTargets ? (
                              <div className="ad-target-list">
                                {telegramTargets.map((target) => (
                                  <label key={target.id} className="ad-target-chip">
                                    <span>{target.name}</span>
                                    <input
                                      type="checkbox"
                                      checked={module.telegramTargetIds.includes(target.id)}
                                      onChange={() => toggleTelegramTarget(module.slotNumber, target.id)}
                                    />
                                  </label>
                                ))}
                              </div>
                            ) : (
                              <p className="text-muted" style={{ margin: 0 }}>
                                Keine persistenten Telegram Ziele vorhanden. Leer bedeutet: Standardziel aus dem Output-Menue.
                              </p>
                            )}
                          </div>
                        ) : null}

                        {module.whatsappEnabled ? (
                          <label className="ad-span-2">
                            <span>WhatsApp Ziele</span>
                            <textarea
                              rows={4}
                              value={module.whatsappTargetsText}
                              onChange={(event) => updateModuleForm(module.slotNumber, { whatsappTargetsText: event.target.value })}
                              placeholder={'ziel-ref-1|Channel Alpha\nziel-ref-2|Channel Beta'}
                            />
                          </label>
                        ) : null}
                      </div>

                      <div className="ad-subsection">
                        <div className="ad-subsection-header">
                          <strong>Bild</strong>
                          <span className="text-muted">Upload wird direkt im Modul gespeichert und spaeter ueber Queue versendet.</span>
                        </div>
                        <div className="ad-image-row">
                          <input
                            type="file"
                            accept="image/*"
                            onChange={(event) => {
                              const file = event.target.files?.[0];
                              if (file) {
                                void onImageChange(module.slotNumber, file);
                              }
                            }}
                          />
                          {module.imageFilename ? <span className="badge">{module.imageFilename}</span> : <span className="badge">kein Bild</span>}
                        </div>
                        {module.imageDataUrl ? (
                          <img className="ad-image-preview" src={module.imageDataUrl} alt={module.moduleName || 'Werbebild'} />
                        ) : null}
                      </div>

                      <div className="ad-module-footer">
                        <button type="button" className="primary" disabled={!isAdmin || !module.id || moduleBusy} onClick={() => void saveModule(module.slotNumber)}>
                          {moduleBusy ? 'Speichert...' : 'Speichern'}
                        </button>
                        <button
                          type="button"
                          className="secondary"
                          disabled={!isAdmin || !module.id || moduleBusy}
                          onClick={() => void togglePause(module.slotNumber, module.status === 'active')}
                        >
                          {module.status === 'active' ? 'Pausieren' : 'Aktivieren'}
                        </button>
                      </div>
                    </div>
                  ) : null}
                </article>
              );
            })}
          </div>
        </section>

        <section className="ad-history-grid">
          <section className="card ad-panel ad-scroll-panel">
            <div className="ad-panel-header">
              <div>
                <p className="section-title">3. Naechste geplante Sendungen</p>
                <h2 className="page-title">Queue-Vorlauf</h2>
              </div>
            </div>
            <div className="ad-list">
              {dashboard.upcoming.length ? (
                dashboard.upcoming.map((item, index) => (
                  <article key={`${item.moduleId}-${item.scheduledFor}-${index}`} className="ad-list-item">
                    <div className="ad-card-head">
                      <strong>{item.moduleName}</strong>
                      <span className={`status-chip ${getToneClass(item.priority)}`}>{PRIORITY_LABELS[item.priority] || item.priority}</span>
                    </div>
                    <p>{formatDateTime(item.scheduledFor)}</p>
                  </article>
                ))
              ) : (
                <p className="text-muted" style={{ margin: 0 }}>
                  Keine naechsten Sendungen geplant.
                </p>
              )}
            </div>
          </section>

          <section className="card ad-panel ad-scroll-panel">
            <div className="ad-panel-header">
              <div>
                <p className="section-title">Historie / Fehler</p>
                <h2 className="page-title">Letzte Werbe-Posts</h2>
              </div>
            </div>
            <div className="ad-list">
              {dashboard.history.length ? (
                dashboard.history.map((item) => (
                  <article key={item.id} className="ad-list-item">
                    <div className="ad-card-head">
                      <strong>{item.moduleName}</strong>
                      <span className={`status-chip ${getToneClass(item.status)}`}>{item.status}</span>
                    </div>
                    <p>
                      {formatDateTime(item.scheduledFor)} | Queue {item.queueId ?? '-'} | Retry {item.retryCount || 0}
                    </p>
                    <p>
                      {item.deliveredChannels?.length ? item.deliveredChannels.join(' | ') : 'Noch keine bestaetigten Zielkanaele'}{' '}
                      {item.lastError ? `| Fehler: ${item.lastError}` : ''}
                    </p>
                  </article>
                ))
              ) : (
                <p className="text-muted" style={{ margin: 0 }}>
                  Noch keine Werbe-Historie vorhanden.
                </p>
              )}
            </div>
          </section>
        </section>
      </div>
    </Layout>
  );
}

export default AdvertisingPage;
