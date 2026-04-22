import { useEffect, useMemo, useState } from 'react';
import { useAuth } from '../../context/AuthContext';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:4000';

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

function TelegramUserClientPanel({ onStatusChange }) {
  const { user } = useAuth();
  const isAdmin = user?.role === 'admin';
  const [sessionName, setSessionName] = useState('default-user');
  const [phoneNumber, setPhoneNumber] = useState('');
  const [phoneCode, setPhoneCode] = useState('');
  const [password, setPassword] = useState('');
  const [statusData, setStatusData] = useState(null);
  const [dialogs, setDialogs] = useState([]);
  const [messages, setMessages] = useState([]);
  const [loadingStatus, setLoadingStatus] = useState(isAdmin);
  const [loadingDialogs, setLoadingDialogs] = useState(false);
  const [syncingMessages, setSyncingMessages] = useState(false);
  const [busyAction, setBusyAction] = useState('');

  const currentPendingLogin = useMemo(() => {
    const pendingLogins = Array.isArray(statusData?.pendingLogins) ? statusData.pendingLogins : [];
    return pendingLogins.find((item) => item.sessionName === sessionName) || pendingLogins[0] || null;
  }, [sessionName, statusData?.pendingLogins]);

  const watchedChannels = useMemo(() => {
    const channels = Array.isArray(statusData?.channels) ? statusData.channels : [];
    return channels.filter((item) => item.sessionName === sessionName);
  }, [sessionName, statusData?.channels]);

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

  async function loadStatus() {
    if (!isAdmin) {
      return;
    }

    setLoadingStatus(true);

    try {
      const data = await apiFetch('/api/telegram/user-client/status');
      setStatusData(data);
    } catch (error) {
      onStatusChange?.(error instanceof Error ? error.message : 'Telegram User Client konnte nicht geladen werden.');
    } finally {
      setLoadingStatus(false);
    }
  }

  async function loadDialogs(targetSessionName = sessionName) {
    if (!isAdmin) {
      return;
    }

    setLoadingDialogs(true);

    try {
      const data = await apiFetch(
        `/api/telegram/user-client/dialogs?sessionName=${encodeURIComponent(targetSessionName)}`
      );
      setDialogs(Array.isArray(data?.items) ? data.items : []);
      onStatusChange?.('Telegram Dialoge geladen.');
    } catch (error) {
      onStatusChange?.(error instanceof Error ? error.message : 'Telegram Dialoge konnten nicht geladen werden.');
    } finally {
      setLoadingDialogs(false);
    }
  }

  async function syncMessages(targetSessionName = sessionName) {
    if (!isAdmin) {
      return;
    }

    setSyncingMessages(true);

    try {
      const data = await apiFetch(
        `/api/telegram/user-client/messages/sync?sessionName=${encodeURIComponent(targetSessionName)}`
      );
      setMessages(Array.isArray(data?.items) ? data.items : []);
      onStatusChange?.('Neue Telegram Nachrichten gelesen.');
      await loadStatus();
    } catch (error) {
      onStatusChange?.(error instanceof Error ? error.message : 'Neue Telegram Nachrichten konnten nicht gelesen werden.');
    } finally {
      setSyncingMessages(false);
    }
  }

  async function handleStartPhoneLogin() {
    setBusyAction('phone-start');

    try {
      const data = await apiFetch('/api/telegram/user-client/login/phone/start', {
        method: 'POST',
        body: JSON.stringify({
          sessionName,
          phoneNumber
        })
      });

      setPhoneCode('');
      setPassword('');
      onStatusChange?.(data?.status === 'connected' ? 'Telegram Session wiederverwendet.' : 'Telefon-Code wurde angefordert.');
      await loadStatus();
    } catch (error) {
      onStatusChange?.(error instanceof Error ? error.message : 'Telefon-Login konnte nicht gestartet werden.');
    } finally {
      setBusyAction('');
    }
  }

  async function handleCompletePhoneLogin() {
    setBusyAction('phone-complete');

    try {
      const data = await apiFetch('/api/telegram/user-client/login/phone/complete', {
        method: 'POST',
        body: JSON.stringify({
          sessionName,
          phoneCode,
          password
        })
      });

      if (data?.status === 'connected') {
        onStatusChange?.('Telegram User Session verbunden.');
        setPhoneCode('');
        setPassword('');
        await loadDialogs(sessionName);
      } else if (data?.status === 'password_required') {
        onStatusChange?.('Telegram verlangt zusaetzlich das 2FA-Passwort.');
      }

      await loadStatus();
    } catch (error) {
      onStatusChange?.(error instanceof Error ? error.message : 'Telefon-Login konnte nicht abgeschlossen werden.');
    } finally {
      setBusyAction('');
    }
  }

  async function handleStartQrLogin() {
    setBusyAction('qr-start');

    try {
      await apiFetch('/api/telegram/user-client/login/qr/start', {
        method: 'POST',
        body: JSON.stringify({
          sessionName
        })
      });

      onStatusChange?.('QR-Login gestartet. QR-Code jetzt mit Telegram scannen.');
      await loadStatus();
    } catch (error) {
      onStatusChange?.(error instanceof Error ? error.message : 'QR-Login konnte nicht gestartet werden.');
    } finally {
      setBusyAction('');
    }
  }

  async function handleSubmitQrPassword() {
    setBusyAction('qr-password');

    try {
      await apiFetch('/api/telegram/user-client/login/qr/password', {
        method: 'POST',
        body: JSON.stringify({
          sessionName,
          password
        })
      });

      onStatusChange?.('2FA-Passwort fuer QR-Login uebergeben.');
      await loadStatus();
    } catch (error) {
      onStatusChange?.(error instanceof Error ? error.message : 'QR-2FA konnte nicht uebergeben werden.');
    } finally {
      setBusyAction('');
    }
  }

  async function handleDisconnect() {
    setBusyAction('disconnect');

    try {
      await apiFetch('/api/telegram/user-client/disconnect', {
        method: 'POST',
        body: JSON.stringify({
          sessionName
        })
      });

      setDialogs([]);
      setMessages([]);
      onStatusChange?.('Telegram User Session getrennt.');
      await loadStatus();
    } catch (error) {
      onStatusChange?.(error instanceof Error ? error.message : 'Session konnte nicht getrennt werden.');
    } finally {
      setBusyAction('');
    }
  }

  async function handleWatchDialog(item) {
    setBusyAction(`watch-${item.channelRef}`);

    try {
      await apiFetch('/api/telegram/user-client/channels/watch', {
        method: 'POST',
        body: JSON.stringify({
          sessionName,
          channelRef: item.channelRef,
          channelTitle: item.title,
          channelType: item.type
        })
      });

      onStatusChange?.(`"${item.title}" wird jetzt gelesen.`);
      await loadStatus();
      await loadDialogs(sessionName);
    } catch (error) {
      onStatusChange?.(error instanceof Error ? error.message : 'Channel konnte nicht uebernommen werden.');
    } finally {
      setBusyAction('');
    }
  }

  async function handleRemoveChannel(channelId) {
    setBusyAction(`remove-${channelId}`);

    try {
      await apiFetch(`/api/telegram/user-client/channels/${channelId}`, {
        method: 'DELETE'
      });

      onStatusChange?.('Channel aus der Watchlist entfernt.');
      await loadStatus();
    } catch (error) {
      onStatusChange?.(error instanceof Error ? error.message : 'Channel konnte nicht entfernt werden.');
    } finally {
      setBusyAction('');
    }
  }

  useEffect(() => {
    void loadStatus();
  }, [isAdmin, user?.role]);

  useEffect(() => {
    if (!currentPendingLogin) {
      return undefined;
    }

    if (!['code_requested', 'qr_waiting', 'password_required', 'authorizing', 'starting'].includes(currentPendingLogin.status)) {
      return undefined;
    }

    const intervalId = window.setInterval(() => {
      void loadStatus();
    }, 3000);

    return () => {
      window.clearInterval(intervalId);
    };
  }, [currentPendingLogin?.sessionName, currentPendingLogin?.status]);

  if (!isAdmin) {
    return null;
  }

  return (
    <section className="card" style={{ padding: '1.25rem', display: 'grid', gap: '1rem' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', flexWrap: 'wrap' }}>
        <div>
          <p className="section-title">Telegram Login</p>
          <h2 style={{ margin: '0.25rem 0 0.5rem', fontSize: '1.35rem' }}>Telegram User Client</h2>
          <p className="text-muted" style={{ margin: 0 }}>
            Echte Telegram User API mit Session-Wiederverwendung, Gruppen-/Channel-Lesen und strukturiertem Nachrichtensync.
          </p>
        </div>
        <div style={{ display: 'flex', gap: '0.75rem', flexWrap: 'wrap' }}>
          <span className={`status-chip ${statusData?.configured ? 'success' : 'warning'}`}>
            {statusData?.configured ? 'GramJS bereit' : 'API Konfiguration fehlt'}
          </span>
          <span className="status-chip info">{Array.isArray(statusData?.sessions) ? statusData.sessions.length : 0} Sessions</span>
          <span className="status-chip info">{Array.isArray(statusData?.channels) ? statusData.channels.length : 0} Watchlists</span>
        </div>
      </div>

      <div className="form-row">
        <label style={{ display: 'grid', gap: '0.35rem', flex: 1 }}>
          <span className="section-title">Session Name</span>
          <input value={sessionName} onChange={(event) => setSessionName(event.target.value)} placeholder="default-user" />
        </label>
        <label style={{ display: 'grid', gap: '0.35rem', flex: 1 }}>
          <span className="section-title">Telefonnummer</span>
          <input
            value={phoneNumber}
            onChange={(event) => setPhoneNumber(event.target.value)}
            placeholder="+49123456789"
          />
        </label>
      </div>

      <div style={{ display: 'flex', gap: '0.75rem', flexWrap: 'wrap' }}>
        <button className="primary" onClick={() => void handleStartPhoneLogin()} disabled={busyAction !== '' || loadingStatus}>
          {busyAction === 'phone-start' ? 'Fordert Code an...' : 'Login per Telefonnummer'}
        </button>
        <button className="secondary" onClick={() => void handleStartQrLogin()} disabled={busyAction !== '' || loadingStatus}>
          {busyAction === 'qr-start' ? 'Erzeugt QR...' : 'Login per QR'}
        </button>
        <button className="secondary" onClick={() => void handleDisconnect()} disabled={busyAction !== ''}>
          {busyAction === 'disconnect' ? 'Trennt...' : 'Session trennen'}
        </button>
        <button className="secondary" onClick={() => void loadStatus()} disabled={loadingStatus}>
          {loadingStatus ? 'Laedt...' : 'Status aktualisieren'}
        </button>
      </div>

      {currentPendingLogin?.status === 'code_requested' && (
        <div className="radio-card" style={{ display: 'grid', gap: '0.75rem' }}>
          <div>
            <strong>Telefon-Code eingeben</strong>
            <p className="text-muted" style={{ margin: '0.35rem 0 0' }}>
              Session {currentPendingLogin.sessionName} wartet auf den Telegram-Code
              {currentPendingLogin.isCodeViaApp ? ' aus der App' : ' per SMS'}.
            </p>
          </div>
          <div className="form-row">
            <input value={phoneCode} onChange={(event) => setPhoneCode(event.target.value)} placeholder="Telegram Code" />
            <input
              value={password}
              onChange={(event) => setPassword(event.target.value)}
              placeholder="2FA Passwort optional"
              type="password"
            />
            <button className="primary" onClick={() => void handleCompletePhoneLogin()} disabled={busyAction !== ''}>
              {busyAction === 'phone-complete' ? 'Meldet an...' : 'Telefon-Login abschliessen'}
            </button>
          </div>
        </div>
      )}

      {currentPendingLogin?.status === 'password_required' && (
        <div className="radio-card" style={{ display: 'grid', gap: '0.75rem' }}>
          <div>
            <strong>2FA erforderlich</strong>
            <p className="text-muted" style={{ margin: '0.35rem 0 0' }}>
              {currentPendingLogin.type === 'qr'
                ? `QR-Login wartet auf das Telegram Passwort${currentPendingLogin.passwordHint ? ` (${currentPendingLogin.passwordHint})` : ''}.`
                : 'Telefon-Login wartet auf das Telegram Passwort.'}
            </p>
          </div>
          <div className="form-row">
            <input
              value={password}
              onChange={(event) => setPassword(event.target.value)}
              placeholder="2FA Passwort"
              type="password"
            />
            {currentPendingLogin.type === 'qr' ? (
              <button className="primary" onClick={() => void handleSubmitQrPassword()} disabled={busyAction !== ''}>
                {busyAction === 'qr-password' ? 'Uebergibt...' : 'QR-2FA senden'}
              </button>
            ) : (
              <button className="primary" onClick={() => void handleCompletePhoneLogin()} disabled={busyAction !== ''}>
                {busyAction === 'phone-complete' ? 'Prueft...' : 'Telefon-Login abschliessen'}
              </button>
            )}
          </div>
        </div>
      )}

      {currentPendingLogin?.qrDataUrl && (
        <div className="radio-card" style={{ display: 'grid', gap: '0.75rem' }}>
          <div>
            <strong>QR-Login aktiv</strong>
            <p className="text-muted" style={{ margin: '0.35rem 0 0' }}>
              Session {currentPendingLogin.sessionName} wartet auf Scan
              {currentPendingLogin.qrExpiresAt ? ` bis ${formatDateTime(currentPendingLogin.qrExpiresAt)}` : ''}.
            </p>
          </div>
          <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap', alignItems: 'center' }}>
            <img
              src={currentPendingLogin.qrDataUrl}
              alt="Telegram QR Login"
              style={{ width: '220px', height: '220px', borderRadius: '18px', background: '#fff', padding: '0.75rem' }}
            />
            <div style={{ display: 'grid', gap: '0.5rem' }}>
              <strong>Telegram App</strong>
              <p className="text-muted" style={{ margin: 0 }}>
                QR mit der Telegram Mobile App scannen. Danach wird die Session automatisch gespeichert und wiederverwendet.
              </p>
              <code style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>{currentPendingLogin.qrUrl}</code>
            </div>
          </div>
        </div>
      )}

      <div className="responsive-grid">
        {(statusData?.sessions || []).map((session) => (
          <article key={session.name} className="card" style={{ padding: '1rem', display: 'grid', gap: '0.35rem' }}>
            <strong>{session.name}</strong>
            <span className={`status-chip ${session.status === 'connected' ? 'success' : 'info'}`}>{session.status}</span>
            <p className="text-muted" style={{ margin: 0 }}>
              {session.phoneNumberMasked || 'ohne Telefonnummer'} · letzter Connect {formatDateTime(session.lastConnectedAt)}
            </p>
          </article>
        ))}
        {!loadingStatus && !(statusData?.sessions || []).length && (
          <article className="card" style={{ padding: '1rem' }}>
            <p className="text-muted" style={{ margin: 0 }}>Noch keine Telegram User Session vorhanden.</p>
          </article>
        )}
      </div>

      <div style={{ display: 'flex', gap: '0.75rem', flexWrap: 'wrap' }}>
        <button className="secondary" onClick={() => void loadDialogs()} disabled={busyAction !== '' || loadingDialogs}>
          {loadingDialogs ? 'Laedt Dialoge...' : 'Gruppen & Channels lesen'}
        </button>
        <button className="secondary" onClick={() => void syncMessages()} disabled={busyAction !== '' || syncingMessages}>
          {syncingMessages ? 'Liest neue Nachrichten...' : 'Neue Nachrichten erkennen'}
        </button>
      </div>

      {!!dialogs.length && (
        <div style={{ display: 'grid', gap: '0.75rem' }}>
          <div>
            <p className="section-title">Dialoge</p>
            <h3 style={{ margin: '0.25rem 0 0' }}>Lesbare Gruppen und Channels</h3>
          </div>
          {dialogs.map((item) => (
            <div key={item.channelRef} className="radio-card" style={{ justifyContent: 'space-between', alignItems: 'start' }}>
              <div>
                <strong>{item.title}</strong>
                <p className="text-muted" style={{ margin: '0.35rem 0 0' }}>
                  {item.type} · {item.username || item.channelRef}
                </p>
              </div>
              <button className="secondary" onClick={() => void handleWatchDialog(item)} disabled={busyAction !== '' || item.watched}>
                {item.watched ? 'Bereits in Watchlist' : 'Als Quelle lesen'}
              </button>
            </div>
          ))}
        </div>
      )}

      <div style={{ display: 'grid', gap: '0.75rem' }}>
        <div>
          <p className="section-title">Watchlist</p>
          <h3 style={{ margin: '0.25rem 0 0' }}>Aktive Gruppen und Channels</h3>
        </div>
        {watchedChannels.map((item) => (
          <div key={item.id} className="radio-card" style={{ justifyContent: 'space-between', alignItems: 'start' }}>
            <div>
              <strong>{item.channelTitle || item.channelRef}</strong>
              <p className="text-muted" style={{ margin: '0.35rem 0 0' }}>
                {item.channelType} · letzter Stand {formatDateTime(item.lastSeenMessageAt)}
              </p>
            </div>
            <button className="secondary" onClick={() => void handleRemoveChannel(item.id)} disabled={busyAction !== ''}>
              Entfernen
            </button>
          </div>
        ))}
        {!watchedChannels.length && <p className="text-muted">Fuer diese Session werden noch keine Gruppen oder Channels gelesen.</p>}
      </div>

      <div style={{ display: 'grid', gap: '0.75rem' }}>
        <div>
          <p className="section-title">Neue Nachrichten</p>
          <h3 style={{ margin: '0.25rem 0 0' }}>Strukturierte Ausgabe</h3>
        </div>
        {messages.map((item, index) => (
          <pre
            key={`${item.group}-${item.timestamp}-${index}`}
            className="radio-card"
            style={{ margin: 0, whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}
          >
{`{
  text: ${JSON.stringify(item.text || '')},
  link: ${JSON.stringify(item.link || '')},
  group: ${JSON.stringify(item.group || '')},
  timestamp: ${JSON.stringify(item.timestamp || '')}
}`}
          </pre>
        ))}
        {!messages.length && <p className="text-muted">Noch keine neuen strukturierten Nachrichten gelesen.</p>}
      </div>
    </section>
  );
}

export default TelegramUserClientPanel;
