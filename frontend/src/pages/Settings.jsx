import { useEffect, useState } from 'react';
import Layout from '../components/layout/Layout';
import { useAuth } from '../context/AuthContext';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:4000';
const settingsApiUrl = `${API_BASE_URL}/api/deals/settings`;
const DEFAULT_TELEGRAM_COPY_BUTTON_TEXT = '📋 Zum Kopieren hier klicken';

function SettingsPage() {
  const { user } = useAuth();
  const isAdmin = user?.role === 'admin';
  const [telegramCopyButtonText, setTelegramCopyButtonText] = useState(DEFAULT_TELEGRAM_COPY_BUTTON_TEXT);
  const [loading, setLoading] = useState(isAdmin);
  const [saving, setSaving] = useState(false);
  const [status, setStatus] = useState('');

  useEffect(() => {
    if (!isAdmin) {
      setLoading(false);
      return;
    }

    const loadSettings = async () => {
      try {
        const response = await fetch(settingsApiUrl, {
          headers: {
            'X-User-Role': user?.role || ''
          }
        });
        const data = await response.json().catch(() => ({}));

        if (response.ok) {
          setTelegramCopyButtonText(data?.telegramCopyButtonText || DEFAULT_TELEGRAM_COPY_BUTTON_TEXT);
        } else {
          setStatus(data?.error || 'Einstellungen konnten nicht geladen werden');
        }
      } catch (error) {
        setStatus(error instanceof Error ? error.message : 'Einstellungen konnten nicht geladen werden');
      } finally {
        setLoading(false);
      }
    };

    void loadSettings();
  }, [isAdmin, user?.role]);

  const handleSave = async () => {
    if (!isAdmin || saving) {
      return;
    }

    setSaving(true);
    setStatus('');

    try {
      const response = await fetch(settingsApiUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-User-Role': user?.role || ''
        },
        body: JSON.stringify({
          telegramCopyButtonText
        })
      });
      const data = await response.json().catch(() => ({}));

      if (!response.ok) {
        setStatus(data?.error || 'Speichern fehlgeschlagen');
        return;
      }

      setTelegramCopyButtonText(data?.telegramCopyButtonText || DEFAULT_TELEGRAM_COPY_BUTTON_TEXT);
      setStatus('Telegram Copy-Button Text gespeichert');
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'Speichern fehlgeschlagen');
    } finally {
      setSaving(false);
    }
  };

  return (
    <Layout>
      <div style={{ display: 'grid', gap: '1rem' }}>
        <section className="card" style={{ padding: '1.25rem' }}>
          <p className="section-title">Einstellungen</p>
          <h2 style={{ margin: '0.25rem 0 1rem', fontSize: '1.5rem' }}>Systemstatus & sichere Konfiguration</h2>
        </section>
        <section className="card" style={{ padding: '1.25rem', display: 'grid', gap: '1rem' }}>
          <div className="radio-card" style={{ display: 'grid', gap: '0.4rem' }}>
            <strong>API-Keys bleiben im Backend</strong>
            <p className="text-muted" style={{ margin: 0 }}>
              Keepa-, Telegram- und weitere Zugangsdaten werden ausschliesslich serverseitig per `.env` verwaltet.
            </p>
          </div>
          <div className="radio-card" style={{ display: 'grid', gap: '0.4rem' }}>
            <strong>Frontend speichert keine Secrets</strong>
            <p className="text-muted" style={{ margin: 0 }}>
              Im UI werden nur sichere Statusinformationen und bewusst maskierte Hinweise angezeigt.
            </p>
          </div>
          <div className="radio-card" style={{ display: 'grid', gap: '0.4rem' }}>
            <strong>Integrationen werden serverseitig gepflegt</strong>
            <p className="text-muted" style={{ margin: 0 }}>
              Kanal- und Integrationsdetails gehoeren in Backend-Module und nicht in Browser-Formulare.
            </p>
          </div>
        </section>
        {isAdmin && (
          <section className="card" style={{ padding: '1.25rem', display: 'grid', gap: '0.85rem' }}>
            <div>
              <p className="section-title">Telegram</p>
              <h2 style={{ margin: '0.25rem 0 0.5rem', fontSize: '1.35rem' }}>Copy Button Text</h2>
              <p style={{ margin: 0, color: '#94a3b8' }}>
                Zentraler Text fuer den Telegram Copy-Button unter Rabattcode-Posts.
              </p>
            </div>
            <label style={{ display: 'grid', gap: '0.45rem' }}>
              <span className="section-title">Telegram Copy-Button Text</span>
              <input
                value={telegramCopyButtonText}
                onChange={(event) => setTelegramCopyButtonText(event.target.value)}
                placeholder={DEFAULT_TELEGRAM_COPY_BUTTON_TEXT}
                disabled={loading || saving}
              />
            </label>
            <button className="primary small" onClick={handleSave} disabled={loading || saving}>
              {saving ? 'Speichert...' : 'Speichern'}
            </button>
            {status && <p style={{ margin: 0, color: '#cbd5e1' }}>{status}</p>}
          </section>
        )}
      </div>
    </Layout>
  );
}

export default SettingsPage;
