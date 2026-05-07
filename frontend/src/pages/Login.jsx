import { useState } from 'react';
import { Navigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import './Login.css';

function LoginPage() {
  const { user, authReady, authError, login } = useAuth();
  const [identifier, setIdentifier] = useState('admin');
  const [password, setPassword] = useState('Admin12345!');
  const [error, setError] = useState('');
  const [submitting, setSubmitting] = useState(false);

  if (authReady && user) {
    return <Navigate to="/" replace />;
  }

  async function handleSubmit(event) {
    event.preventDefault();
    setSubmitting(true);
    setError('');

    try {
      await login({ identifier, username: identifier, password });
    } catch (loginError) {
      setError(loginError instanceof Error ? loginError.message : 'Login fehlgeschlagen.');
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="login-shell">
      <div className="login-background" aria-hidden="true" />
      <div className="login-grid">
        <section className="login-showcase">
          <p className="section-title">Affiliate Manager Pro</p>
          <h1>Admin-Zentrale fuer Login, Output-Steuerung und mobile Arbeit.</h1>
          <p>
            Sichere Anmeldung, geschuetzte Backend-Routen und ein Bedienkonzept, das auch auf Handy klar und direkt
            bleibt.
          </p>
          <div className="login-highlights">
            <article className="login-highlight-card">
              <strong>Echtes Login</strong>
              <span>Persistente Session statt UI-Platzhalter.</span>
            </article>
            <article className="login-highlight-card">
              <strong>Admin sichtbar</strong>
              <span>Rolle, Benutzername und Logout bleiben im Layout erkennbar.</span>
            </article>
            <article className="login-highlight-card">
              <strong>Output sicher</strong>
              <span>Live-Kanaele koennen getrennt und bewusst freigeschaltet werden.</span>
            </article>
          </div>
        </section>

        <form className="card login-card" onSubmit={handleSubmit}>
          <div className="login-card-header">
            <p className="section-title">Sicherer Zugang</p>
            <h2>Administrator anmelden</h2>
            <p>
              Anmeldung mit E-Mail oder Benutzername und Passwort. Die Sitzung bleibt nach Reload erhalten, bis sie
              abgemeldet wird.
            </p>
          </div>

          <label className="login-field">
            <span>E-Mail oder Benutzername</span>
            <input
              value={identifier}
              autoComplete="username"
              placeholder="admin oder admin@affiliate-manager.local"
              onChange={(event) => setIdentifier(event.target.value)}
            />
          </label>

          <label className="login-field">
            <span>Passwort</span>
            <input
              type="password"
              value={password}
              autoComplete="current-password"
              placeholder="Passwort eingeben"
              onChange={(event) => setPassword(event.target.value)}
            />
          </label>

          <div className="login-hint">
            <strong>Bootstrap lokal</strong>
            <span>
              Standardmaessig gilt <code>admin</code> / <code>Admin12345!</code>, sofern keine Auth-ENV gesetzt ist.
            </span>
          </div>

          {error || authError ? <p className="login-error">{error || authError}</p> : null}

          <button
            className="primary full login-submit"
            type="submit"
            disabled={submitting || !authReady || !identifier.trim() || !password.trim()}
          >
            {submitting ? 'Anmeldung wird geprueft...' : 'Anmelden'}
          </button>
        </form>
      </div>
    </div>
  );
}

export default LoginPage;
