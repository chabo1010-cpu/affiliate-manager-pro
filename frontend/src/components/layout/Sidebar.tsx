import { NavLink } from 'react-router-dom';
import { useAuth } from '../../context/AuthContext';

const navItems = [
  { label: 'Home', path: '/', icon: '🏠' },
  { label: 'Generator Poster', path: '/generator', icon: '📝' },
  { label: 'Templates', path: '/templates', icon: '🧩' },
  { label: 'Scraper', path: '/scraper', icon: '🔍' },
  { label: 'Autobot', path: '/autobot', icon: '🤖' },
  { label: 'Logs', path: '/logs', icon: '📋' },
  { label: 'Team', path: '/team', icon: '👥' },
  { label: 'Einstellungen', path: '/settings', icon: '⚙️' }
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
            className={({ isActive }) =>
              `nav-link ${isActive ? 'active' : ''}`
            }
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
