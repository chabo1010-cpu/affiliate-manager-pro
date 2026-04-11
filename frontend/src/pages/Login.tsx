import { useState } from 'react';
import { useAuth } from '../context/AuthContext';

const roles = ['admin', 'editor', 'poster', 'viewer'];

function LoginPage() {
  const { login } = useAuth();
  const [username, setUsername] = useState('poster');

  return (
    <div style={{ minHeight: '100vh', display: 'grid', placeItems: 'center', padding: '1rem', background: '#02040a' }}>
      <div className="card" style={{ maxWidth: '420px', width: '100%', padding: '2rem' }}>
        <p className="section-title">Login</p>
        <h1 style={{ margin: '0 0 0.75rem', fontSize: '1.9rem' }}>Affiliate Manager Pro</h1>
        <p style={{ marginBottom: '1.5rem', color: '#94a3b8' }}>Mobile-optimiertes Posting-Tool für Telegram & WhatsApp.</p>
        <label style={{ display: 'block', marginBottom: '0.75rem', color: '#cbd5e1' }}>Benutzername</label>
        <select value={username} onChange={(e) => setUsername(e.target.value)}>
          {roles.map((role) => (
            <option key={role} value={role}>{role}</option>
          ))}
        </select>
        <button className="primary full" style={{ marginTop: '1.5rem' }} onClick={() => login(username)}>
          Anmelden
        </button>
      </div>
    </div>
  );
}

export default LoginPage;
