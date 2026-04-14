import { useEffect, useState } from 'react';
import Layout from '../components/layout/Layout';
import { useAuth } from '../context/AuthContext';

const API_BASE_URL = 'http://localhost:4000';
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
        }
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
          repostCooldownEnabled: true,
          repostCooldownHours: 12,
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
    } finally {
      setSaving(false);
    }
  };

  return (
    <Layout>
      <div style={{ display: 'grid', gap: '1rem' }}>
        <section className="card" style={{ padding: '1.25rem' }}>
          <p className="section-title">Einstellungen</p>
          <h2 style={{ margin: '0.25rem 0 1rem', fontSize: '1.5rem' }}>Profil & technische Platzhalter</h2>
        </section>
        <section className="card" style={{ padding: '1.25rem', display: 'grid', gap: '1rem' }}>
          <div>
            <label className="section-title">Profil</label>
            <input placeholder="Name" />
          </div>
          <div>
            <label className="section-title">API Platzhalter</label>
            <input placeholder="API Token" />
          </div>
          <div>
            <label className="section-title">Partner Tag</label>
            <input placeholder="Partner Tag" />
          </div>
          <div>
            <label className="section-title">Kanal Platzhalter</label>
            <input placeholder="Telegram / WhatsApp Kanal" />
          </div>
          <button className="primary small">Speichern</button>
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
