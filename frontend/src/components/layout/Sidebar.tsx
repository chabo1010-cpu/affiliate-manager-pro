import { NavLink } from 'react-router-dom';
import { useAuth } from '../../context/AuthContext';

const navItems = [
  { label: 'Home', path: '/', icon: '\u{1F3E0}' },
  { label: 'Generator Poster', path: '/generator', icon: '\u{1F4DD}' },
  { label: 'Templates', path: '/templates', icon: '\u{1F9E9}' },
  { label: 'Scraper', path: '/scraper', icon: '\u{1F50D}' },
  { label: 'Autobot', path: '/autobot', icon: '\u{1F916}' },
  { label: 'Copybot', path: '/copybot', icon: '\u{1F5C2}' },
  { label: 'Publishing', path: '/publishing', icon: '\u{1F4E6}' },
  { label: 'Logs', path: '/logs', icon: '\u{1F4CB}' },
  { label: 'Deal Historie', path: '/deal-history', icon: '\u23F2\uFE0F' },
  { label: 'Team', path: '/team', icon: '\u{1F465}' },
  { label: 'Einstellungen', path: '/settings', icon: '\u2699\uFE0F' }
];

function Sidebar() {
  const { user, logout } = useAuth();

  return (
    <aside className="sidebar card" style={{ padding: '1.25rem', minWidth: '240px', maxWidth: '280px' }}>
      <div style={{ marginBottom: '1.5rem' }}>
        <p className="section-title">Affiliate Manager Pro</p>
        <p style={{ margin: 0, fontSize: '0.95rem', color: '#cbd5e1' }}>Rolle: {user?.role}</p>
      </div>

      <nav style={{ display: 'grid', gap: '0.6rem' }}>
        {navItems.map((item) => (
          <NavLink
            key={item.path}
            to={item.path}
            className={({ isActive }) => `nav-link ${isActive ? 'active' : ''}`}
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: '0.85rem',
              padding: '0.95rem 1rem',
              borderRadius: '16px',
              background: 'rgba(255,255,255,0.03)',
              color: '#e2e8f0'
            }}
          >
            <span>{item.icon}</span>
            <span>{item.label}</span>
          </NavLink>
        ))}
      </nav>

      <button className="secondary full" style={{ marginTop: '1.5rem' }} onClick={logout}>
        Abmelden
      </button>
    </aside>
  );
}

export default Sidebar;
