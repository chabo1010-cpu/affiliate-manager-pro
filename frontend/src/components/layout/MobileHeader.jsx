import { useEffect, useState } from 'react';
import { NavLink, useLocation } from 'react-router-dom';
import { useAuth } from '../../context/AuthContext';
import { getFlatNavigation, getMobilePrimaryNavigation } from './navigation';

function MobileHeader() {
  const { user, logout } = useAuth();
  const location = useLocation();
  const [menuOpen, setMenuOpen] = useState(false);
  const mobilePrimaryNavigation = getMobilePrimaryNavigation(user?.role);
  const allNavigation = getFlatNavigation(user?.role);

  useEffect(() => {
    setMenuOpen(false);
  }, [location.pathname]);

  return (
    <>
      <header className="mobile-header">
        <div className="mobile-topbar">
          <div className="mobile-brand-block">
            <p className="mobile-kicker">Affiliate Manager Pro</p>
            <h1>Administrator</h1>
            <span>{user?.username || '-'} | {user?.email || '-'}</span>
          </div>

          <div className="mobile-topbar-actions">
            <button
              type="button"
              className="secondary mobile-menu-button"
              aria-expanded={menuOpen}
              aria-controls="mobile-navigation-drawer"
              onClick={() => setMenuOpen((current) => !current)}
            >
              {menuOpen ? 'Schliessen' : 'Menue'}
            </button>
          </div>
        </div>
      </header>

      {menuOpen ? <button type="button" className="mobile-drawer-backdrop" onClick={() => setMenuOpen(false)} aria-label="Menue schliessen" /> : null}

      <aside
        id="mobile-navigation-drawer"
        className={menuOpen ? 'mobile-drawer mobile-drawer-open' : 'mobile-drawer'}
        aria-hidden={menuOpen ? 'false' : 'true'}
      >
        <div className="mobile-drawer-header">
          <div>
            <p className="section-title">Angemeldet als Administrator</p>
            <h2>{user?.displayName || 'Administrator'}</h2>
            <p>{user?.username || '-'} | {user?.email || '-'}</p>
          </div>
          <button type="button" className="secondary small" onClick={() => void logout()}>
            Logout
          </button>
        </div>

        <nav className="mobile-drawer-nav">
          {allNavigation.map((item) => (
            <NavLink
              key={item.path}
              to={item.path}
              end={item.path === '/'}
              className={({ isActive }) => `mobile-drawer-link ${isActive ? 'active' : ''}`}
            >
              <span className="mobile-drawer-link-icon">{item.icon}</span>
              <span>{item.label}</span>
            </NavLink>
          ))}
        </nav>
      </aside>

      <nav className="mobile-bottom-nav" aria-label="Mobile Hauptnavigation">
        {mobilePrimaryNavigation.map((item) => (
          <NavLink
            key={item.path}
            to={item.path}
            end={item.path === '/'}
            className={({ isActive }) => `mobile-bottom-link ${isActive ? 'active' : ''}`}
          >
            <span className="mobile-bottom-link-icon">{item.icon}</span>
            <span>{item.label}</span>
          </NavLink>
        ))}
      </nav>
    </>
  );
}

export default MobileHeader;
