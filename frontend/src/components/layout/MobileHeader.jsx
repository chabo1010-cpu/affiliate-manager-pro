import { NavLink } from 'react-router-dom';
import { useAuth } from '../../context/AuthContext';

const baseMobileNav = [
  { label: 'Home', path: '/', icon: '\u{1F3E0}' },
  { label: 'Generator', path: '/generator', icon: '\u{1F4DD}' },
  { label: 'Scrapper', path: '/scraper', icon: '\u{1F50D}' },
  { label: 'Publishing', path: '/publishing', icon: '\u{1F4E6}' }
];

function MobileHeader() {
  const { user } = useAuth();
  const mobileNav =
    user?.role === 'admin'
      ? [
          ...baseMobileNav.slice(0, 3),
          { label: 'Logik-Zentrale', path: '/learning', icon: '\u{1F50C}' },
          ...baseMobileNav.slice(3)
        ]
      : baseMobileNav;

  return (
    <header className="mobile-header">
      <div className="top-line">
        <div className="title-block">
          <h1>Affiliate Manager</h1>
          <p>Dashboard, Arbeitsbereiche und Output klar getrennt</p>
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
