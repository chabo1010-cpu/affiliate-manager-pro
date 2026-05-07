import { NavLink } from 'react-router-dom';
import { useAuth } from '../../context/AuthContext';
import { getNavigationSections } from './navigation';

function Sidebar() {
  const { user, logout } = useAuth();
  const navigationSections = getNavigationSections(user?.role);

  return (
    <aside className="sidebar card sidebar-card">
      <div className="sidebar-header">
        <div className="sidebar-brand-row">
          <div className="sidebar-mark" aria-hidden="true">
            AM
          </div>
          <div className="sidebar-brand">
            <p className="section-title">Affiliate Manager Pro</p>
            <h2 className="sidebar-brand-title">Control Center</h2>
            <p className="sidebar-role">Angemeldet als {user?.displayName || 'Administrator'}</p>
            <p className="sidebar-identity">{user?.username || '-'} | {user?.email || '-'}</p>
          </div>
        </div>
        <div className="sidebar-flow-note">
          <strong>Signal Flow</strong>
          <span>Input -&gt; Analyse -&gt; Varianten -&gt; Queue -&gt; Publishing</span>
        </div>
      </div>

      <nav className="sidebar-nav">
        {navigationSections.map((section) => (
          <div key={section.title} className="sidebar-section sidebar-group">
            <div className="sidebar-group-header">
              <p className="section-title sidebar-section-title">{section.title}</p>
              <p className="sidebar-group-note">{section.note}</p>
            </div>
            {section.items.map((item) => (
              <NavLink
                key={item.path}
                to={item.path}
                end={item.path === '/'}
                className={({ isActive }) => `nav-link sidebar-link ${isActive ? 'active' : ''}`}
              >
                <span className="sidebar-link-icon">{item.icon}</span>
                <span className="sidebar-link-label">{item.label}</span>
              </NavLink>
            ))}
          </div>
        ))}
      </nav>

      <button className="secondary full sidebar-logout" onClick={() => void logout()}>
        Logout
      </button>
    </aside>
  );
}

export default Sidebar;
