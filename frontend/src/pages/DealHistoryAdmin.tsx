import { useEffect, useMemo, useState } from 'react';
import Layout from '../components/layout/Layout';
import { Toast, useToast } from '../components/Toast';
import './DealHistoryAdmin.css';

const API_BASE_URL = 'http://localhost:4000';
const adminSettingsApiUrl = `${API_BASE_URL}/api/deals/settings`;
const adminHistoryApiUrl = `${API_BASE_URL}/api/deals/history`;

interface AdminSettings {
  repostCooldownEnabled: boolean;
  repostCooldownHours: number;
}

interface HistoryItem {
  id: number;
  asin: string;
  url: string;
  normalizedUrl: string;
  title: string;
  price: string;
  oldPrice: string;
  sellerType: string;
  postedAt: string;
  channel: string;
  couponCode: string;
}

function formatAdminDate(value: string) {
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

function getPresetLabel(hours: number) {
  if (hours === 3) {
    return '3 Stunden';
  }

  if (hours === 12) {
    return '12 Stunden';
  }

  if (hours === 24) {
    return '24 Stunden';
  }

  return 'Benutzerdefiniert';
}

function parseEnabledFlag(value: unknown) {
  return value === true || value === 1 || value === '1';
}

function DealHistoryAdminPage() {
  const [settings, setSettings] = useState<AdminSettings>({
    repostCooldownEnabled: true,
    repostCooldownHours: 12
  });
  const [savingSettings, setSavingSettings] = useState(false);
  const [settingsSaveStatus, setSettingsSaveStatus] = useState<{
    kind: 'success' | 'error';
    label: string;
    time: string;
  } | null>(null);
  const [loading, setLoading] = useState(true);
  const [items, setItems] = useState<HistoryItem[]>([]);
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
  const presetLabel =
    presetValue === '3h'
      ? '3 Stunden'
      : presetValue === '12h'
        ? '12 Stunden'
        : presetValue === '24h'
          ? '24 Stunden'
          : 'Benutzerdefiniert';

  const loadSettings = async () => {
    const response = await fetch(adminSettingsApiUrl);
    const data = await response.json();
    console.log('FRONTEND SETTINGS LOAD RESPONSE', data);
    if (response.ok) {
      const nextSettings = {
        repostCooldownEnabled: Boolean(data?.repostCooldownEnabled),
        repostCooldownHours: Number(data?.repostCooldownHours)
      };
      setSettings(nextSettings);
      console.log('FRONTEND SETTINGS STATE AFTER LOAD', {
        status: nextSettings.repostCooldownEnabled ? 'Aktiviert' : 'Deaktiviert',
        enabled: nextSettings.repostCooldownEnabled,
        hours: nextSettings.repostCooldownHours
      });
    }
  };

  const loadHistory = async () => {
    setLoading(true);
    try {
      const params = new URLSearchParams();
      Object.entries(filters).forEach(([key, value]) => {
        if (value) {
          params.set(key, value);
        }
      });

      const response = await fetch(`${adminHistoryApiUrl}?${params.toString()}`);
      const data = await response.json();

      if (!response.ok) {
        showToast(data?.error || 'Deal-Historie konnte nicht geladen werden');
        return;
      }

      setItems(Array.isArray(data?.items) ? data.items : []);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void Promise.all([loadSettings(), loadHistory()]);
  }, []);

  const stats = useMemo(() => ({ deals: items.length }), [items]);

  const saveSettings = async () => {
    console.log('SETTINGS SAVE CLICK');
    setSavingSettings(true);
    try {
      const payload = { ...settings };
      console.log('SETTINGS SAVE URL', adminSettingsApiUrl);
      console.log('SETTINGS SAVE PAYLOAD', payload);
      console.log('FRONTEND SETTINGS SAVE PAYLOAD', payload);
      console.log('DEAL HISTORY LOCAL HOURS', settings.repostCooldownHours);
      console.log('DEAL HISTORY LOCAL PRESET', settings.repostCooldownEnabled ? presetLabel : 'Sperre deaktiviert');
      const response = await fetch(adminSettingsApiUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      });
      console.log('SETTINGS SAVE RESPONSE STATUS', response.status);
      const data = await response.json().catch(() => null);
      console.log('SETTINGS SAVE RESPONSE DATA', data);

      if (!response.ok) {
        throw new Error(data?.error || 'Senden fehlgeschlagen');
      }

      if (!data) {
        throw new Error('Leere Antwort vom Settings-Speichern');
      }

      const nextSettings = {
        repostCooldownEnabled: Boolean(data?.repostCooldownEnabled),
        repostCooldownHours: Number(data?.repostCooldownHours)
      };

      if (Number.isNaN(nextSettings.repostCooldownHours)) {
        throw new Error('Ungültige Antwort vom Settings-Speichern');
      }

      setSettings(nextSettings);
      console.log('SETTINGS LOAD RESPONSE', data);
      console.log('FRONTEND SETTINGS LOAD RESPONSE', data);
      console.log('FRONTEND SETTINGS STATE AFTER LOAD', {
        status: nextSettings.repostCooldownEnabled ? 'Aktiviert' : 'Deaktiviert',
        enabled: nextSettings.repostCooldownEnabled,
        hours: nextSettings.repostCooldownHours
      });
      showToast('Repost-Sperre gespeichert');
      setSettingsSaveStatus({
        kind: 'success',
        label: 'Letzte Speicherung erfolgreich',
        time: new Date().toLocaleTimeString('de-DE')
      });
    } catch (error) {
      console.error('SETTINGS SAVE FETCH ERROR', error);
      const message = error instanceof Error ? error.message : 'Senden fehlgeschlagen';
      console.log('SETTINGS SAVE RESPONSE DATA', error);
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
          <h1 className="page-title">Deal Historie</h1>
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
            <span>Historien-Eintraege</span>
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
            </div>

            <div className="deal-admin-actions">
              <button type="button" className="secondary" onClick={() => void loadHistory()}>
                Filter anwenden
              </button>
              <button
                type="button"
                className="secondary"
                onClick={() => {
                  setFilters({
                    startDate: '',
                    endDate: '',
                    marketplaceType: '',
                    asin: '',
                    url: '',
                    title: ''
                  });
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
              <h2 className="page-title">Historie</h2>
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
