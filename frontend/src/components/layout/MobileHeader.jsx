import { NavLink } from 'react-router-dom';
import { useAuth } from '../../context/AuthContext';

const baseNav = [
  { label: 'Dashboard', path: '/', icon: '\u{1F3E0}' },
  { label: 'Generator', path: '/generator', icon: '\u{1F4DD}' },
  { label: 'Scrapper', path: '/scraper', icon: '\u{1F50D}' },
  { label: 'Copybot', path: '/copybot', icon: '\u{1F4E8}' },
  { label: 'Templates', path: '/templates', icon: '\u{1F9F1}' },
  { label: 'Autobot', path: '/autobot', icon: '\u2699\uFE0F' },
  { label: 'Publishing', path: '/publishing', icon: '\u{1F4E4}' },
  { label: 'Sperrzeiten', path: '/sperrzeiten', icon: '\u23F2\uFE0F' },
  { label: 'Logs', path: '/logs', icon: '\u{1F4CA}' },
  { label: 'Einstellungen', path: '/settings', icon: '\u{1F527}' }
];

function MobileHeader() {
  const { user } = useAuth();
  const mobileNav =
    user?.role === 'admin'
      ? [...baseNav, { label: 'Logik-Zentrale', path: '/learning', icon: '\u{1F9E0}' }]
      : baseNav;

  return (
    <header className="mobile-header">
      <div className="top-line">
        <div className="title-block">
          <h1>Affiliate Manager</h1>
          <p>Internet zuerst, Keepa als Fallback, Queue und Sperrmodul aktiv</p>
        </div>
        <span className="user-chip">{user?.role}</span>
      </div>
      <nav className="mobile-nav">
        {mobileNav.map((item) => (
          <NavLink
            key={item.path}
            to={item.path}
            end={item.path === '/'}
            className={({ isActive }) => (isActive ? 'mobile-nav-link active' : 'mobile-nav-link')}
          >
            <span>{item.icon}</span>
            <span>{item.label}</span>
          </NavLink>
        ))}
      </nav>
    </header>
  );
}

export default MobileHeader;
