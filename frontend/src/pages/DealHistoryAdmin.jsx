import { useEffect, useMemo, useState } from 'react';
import Layout from '../components/layout/Layout';
import { Toast, useToast } from '../components/Toast';
import './DealHistoryAdmin.css';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:4000';
const adminSettingsApiUrl = `${API_BASE_URL}/api/deals/settings`;
const adminHistoryApiUrl = `${API_BASE_URL}/api/deals/history`;

function formatAdminDate(value) {
  if (!value) {
    return '-';
  }

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return '-';
  }

  return new Intl.DateTimeFormat('de-DE', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit'
  }).format(parsed);
}

async function requestJson(url, options = {}) {
  let response;

  try {
    response = await fetch(url, {
      ...options,
      credentials: 'include',
      headers: {
        Accept: 'application/json',
        ...(options.body ? { 'Content-Type': 'application/json' } : {}),
        ...(options.headers || {})
      }
    });
  } catch (error) {
    throw new Error(
      error instanceof Error && error.message
        ? `Backend nicht erreichbar: ${error.message}`
        : 'Backend nicht erreichbar.'
    );
  }

  const rawText = await response.text();
  let data = null;

  if (rawText) {
    try {
      data = JSON.parse(rawText);
    } catch {
      throw new Error(`Unerwartete Antwort vom Server (${response.status}).`);
    }
  }

  if (!response.ok) {
    throw new Error(data?.error || `Request fehlgeschlagen (${response.status})`);
  }

  return data;
}

function DealHistoryAdminPage() {
  const [settings, setSettings] = useState({
    repostCooldownEnabled: true,
    repostCooldownHours: 12
  });
  const [savingSettings, setSavingSettings] = useState(false);
  const [settingsSaveStatus, setSettingsSaveStatus] = useState(null);
  const [loading, setLoading] = useState(true);
  const [items, setItems] = useState([]);
  const [filters, setFilters] = useState({
    startDate: '',
    endDate: '',
    marketplaceType: '',
    asin: '',
    url: '',
    title: ''
  });
  const { toast, showToast } = useToast();
  const presetValue =
    settings.repostCooldownHours === 3
      ? '3h'
      : settings.repostCooldownHours === 12
        ? '12h'
        : settings.repostCooldownHours === 24
          ? '24h'
          : 'custom';

  useEffect(() => {
    document.title = 'Sperrzeiten - Affiliate Manager Pro';
    console.info('DEAL HISTORY RENAMED TO SPERRZEITEN', {
      route: '/sperrzeiten'
    });
  }, []);

  const loadSettings = async () => {
    try {
      const data = await requestJson(adminSettingsApiUrl);
      setSettings({
        repostCooldownEnabled: Boolean(data?.repostCooldownEnabled),
        repostCooldownHours: Number(data?.repostCooldownHours)
      });
    } catch (error) {
      showToast(error instanceof Error ? error.message : 'Repost-Einstellungen konnten nicht geladen werden');
    }
  };

  const loadHistory = async (nextFilters = filters) => {
    setLoading(true);
    try {
      if (
        nextFilters.startDate &&
        nextFilters.endDate &&
        new Date(nextFilters.startDate).getTime() > new Date(nextFilters.endDate).getTime()
      ) {
        showToast('Der Startzeitraum darf nicht nach dem Endzeitraum liegen');
        setItems([]);
        return;
      }

      const params = new URLSearchParams();
      Object.entries(nextFilters).forEach(([key, value]) => {
        if (value) {
          params.set(key, value);
        }
      });

      const data = await requestJson(`${adminHistoryApiUrl}?${params.toString()}`);
      setItems(Array.isArray(data?.items) ? data.items : []);
    } catch (error) {
      showToast(error instanceof Error ? error.message : 'Sperrzeiten konnten nicht geladen werden');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void Promise.all([loadSettings(), loadHistory()]);
  }, []);

  const stats = useMemo(() => ({ deals: items.length }), [items]);

  const saveSettings = async () => {
    setSavingSettings(true);
    setSettingsSaveStatus(null);
    try {
      const payload = { ...settings };
      const data = await requestJson(adminSettingsApiUrl, {
        method: 'POST',
        body: JSON.stringify(payload)
      });
      const confirmed = await requestJson(adminSettingsApiUrl);
      const safeHours = Number(confirmed?.repostCooldownHours ?? data?.repostCooldownHours);

      if (Number.isNaN(safeHours)) {
        throw new Error('Ungueltige Antwort vom Settings-Speichern');
      }

      const nextSettings = {
        repostCooldownEnabled: Boolean(confirmed?.repostCooldownEnabled ?? data?.repostCooldownEnabled),
        repostCooldownHours: safeHours
      };

      if (Number.isNaN(nextSettings.repostCooldownHours)) {
        throw new Error('Ungültige Antwort vom Settings-Speichern');
      }

      setSettings(nextSettings);
      showToast('Repost-Sperre gespeichert');
      setSettingsSaveStatus({
        kind: 'success',
        label: 'Letzte Speicherung erfolgreich',
        time: new Date().toLocaleTimeString('de-DE')
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Senden fehlgeschlagen';
      showToast(message);
      setSettingsSaveStatus({
        kind: 'error',
        label: 'Letzte Speicherung fehlgeschlagen',
        time: new Date().toLocaleTimeString('de-DE')
      });
      return;
    } finally {
      setSavingSettings(false);
    }
  };

  return (
    <Layout showSidebar>
      <div className="deal-admin-page">
        <section className="card deal-admin-hero">
          <p className="section-title">AFFILIATE MANAGER</p>
          <h1 className="page-title">Sperrzeiten</h1>
          <p className="page-subtitle">
            Interne Repost-Sperre, Verlauf und Preisentwicklung an einer Stelle.
          </p>
        </section>

        <section className="responsive-grid deal-admin-summary">
          <article className="card deal-admin-stat">
            <span>Repost-Sperre</span>
            <strong>{settings.repostCooldownEnabled ? `${settings.repostCooldownHours} Stunden` : 'Deaktiviert'}</strong>
          </article>
          <article className="card deal-admin-stat">
            <span>Eintraege</span>
            <strong>{stats.deals}</strong>
          </article>
        </section>

        <div className="deal-admin-layout">
          <section className="card deal-admin-panel">
            <p className="section-title">Sperrzeit</p>
            <div className="deal-admin-settings">
              <label className="generator-checkbox-row">
                <input
                  type="checkbox"
                  checked={settings.repostCooldownEnabled}
                  onChange={(event) =>
                    setSettings((prev) => ({ ...prev, repostCooldownEnabled: event.target.checked }))
                  }
                />
                <span>Repost-Sperre aktivieren</span>
              </label>

              {!settings.repostCooldownEnabled && <p className="text-muted">Sperre deaktiviert</p>}

              <label>
                <span>Preset</span>
                <select
                  value={presetValue}
                  disabled={!settings.repostCooldownEnabled}
                  onChange={(event) => {
                    const nextMode = event.target.value;
                    const presetHours =
                      nextMode === '3h'
                        ? 3
                        : nextMode === '12h'
                          ? 12
                          : nextMode === '24h'
                            ? 24
                            : settings.repostCooldownHours;
                    setSettings((prev) => ({ ...prev, repostCooldownHours: presetHours }));
                  }}
                >
                  <option value="3h">3 Stunden</option>
                  <option value="12h">12 Stunden</option>
                  <option value="24h">24 Stunden</option>
                  <option value="custom">Benutzerdefiniert</option>
                </select>
              </label>

              <label>
                <span>Stunden</span>
                <input
                  type="number"
                  min="1"
                  step="1"
                  value={settings.repostCooldownHours}
                  disabled={!settings.repostCooldownEnabled}
                  onChange={(event) =>
                    setSettings((prev) => ({
                      ...prev,
                      repostCooldownHours: Number(event.target.value || 12)
                    }))
                  }
                />
              </label>

              <button type="button" className="primary" onClick={saveSettings} disabled={savingSettings}>
                {savingSettings ? 'Speichert...' : 'Sperrzeit speichern'}
              </button>
              {settingsSaveStatus && (
                <p className={`text-muted ${settingsSaveStatus.kind === 'error' ? 'generator-form-error' : ''}`}>
                  {settingsSaveStatus.label}
                  {settingsSaveStatus.time ? ` · Zuletzt gespeichert: ${settingsSaveStatus.time}` : ''}
                </p>
              )}
            </div>
          </section>

          <section className="card deal-admin-panel">
            <p className="section-title">Filter</p>
            <div className="deal-admin-filters">
              <label>
                <span>Zeitraum von</span>
                <input
                  type="date"
                  value={filters.startDate}
                  onChange={(event) => setFilters((prev) => ({ ...prev, startDate: event.target.value }))}
                />
              </label>
              <label>
                <span>Zeitraum bis</span>
                <input
                  type="date"
                  value={filters.endDate}
                  onChange={(event) => setFilters((prev) => ({ ...prev, endDate: event.target.value }))}
                />
              </label>
              <label>
                <span>Typ</span>
                <select
                  value={filters.marketplaceType}
                  onChange={(event) => setFilters((prev) => ({ ...prev, marketplaceType: event.target.value }))}
                >
                  <option value="">Alle</option>
                  <option value="AMAZON">Amazon</option>
                  <option value="FBA">FBA</option>
                  <option value="FBM">FBM</option>
                </select>
              </label>
              <label>
                <span>ASIN</span>
                <input
                  type="text"
                  value={filters.asin}
                  onChange={(event) => setFilters((prev) => ({ ...prev, asin: event.target.value }))}
                  placeholder="B0..."
                />
              </label>
              <label>
                <span>Titel</span>
                <input
                  type="text"
                  value={filters.title}
                  onChange={(event) => setFilters((prev) => ({ ...prev, title: event.target.value }))}
                  placeholder="Produkttitel"
                />
              </label>
              <label>
                <span>URL</span>
                <input
                  type="text"
                  value={filters.url}
                  onChange={(event) => setFilters((prev) => ({ ...prev, url: event.target.value }))}
                  placeholder="amazon.de/..."
                />
              </label>
            </div>

            <div className="deal-admin-actions">
              <button type="button" className="secondary" onClick={() => void loadHistory(filters)}>
                Filter anwenden
              </button>
              <button
                type="button"
                className="secondary"
                onClick={() => {
                  const clearedFilters = {
                    startDate: '',
                    endDate: '',
                    marketplaceType: '',
                    asin: '',
                    url: '',
                    title: ''
                  };
                  setFilters(clearedFilters);
                  void loadHistory(clearedFilters);
                }}
              >
                Filter leeren
              </button>
            </div>
          </section>
        </div>

        <section className="card deal-admin-panel">
          <div className="deal-admin-table-header">
            <div>
              <p className="section-title">Gespeicherte Beitraege</p>
              <h2 className="page-title">Sperrzeiten-Verlauf</h2>
            </div>
            {loading && <span className="badge">Laedt...</span>}
          </div>

          <div className="deal-admin-table">
            <div className="deal-admin-row deal-admin-row-head">
              <span>Datum</span>
              <span>Titel</span>
              <span>Preis</span>
              <span>Typ</span>
              <span>ASIN</span>
            </div>

            {items.map((item) => (
              <div className="deal-admin-row" key={item.id}>
                <span>{formatAdminDate(item.postedAt)}</span>
                <div>
                  <strong>{item.title || item.asin || item.normalizedUrl}</strong>
                  <p>{item.url || item.normalizedUrl}</p>
                </div>
                <span>{item.price || '-'}</span>
                <span>{item.sellerType}</span>
                <span>{item.asin || '-'}</span>
              </div>
            ))}

            {!loading && items.length === 0 && <p className="text-muted">Keine Eintraege fuer den aktuellen Filter.</p>}
          </div>
        </section>

        {toast && <Toast message={toast.message} duration={toast.duration} />}
      </div>
    </Layout>
  );
}

export default DealHistoryAdminPage;
