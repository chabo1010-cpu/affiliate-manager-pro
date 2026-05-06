import { NavLink } from 'react-router-dom';
import { useAuth } from '../../context/AuthContext';

const overviewSection = [{ label: 'Dashboard', path: '/', icon: 'DB' }];
const sourceSection = [
  { label: 'Generator', path: '/generator', icon: 'GN' },
  { label: 'Scrapper', path: '/scraper', icon: 'SC' },
  { label: 'Copybot', path: '/copybot', icon: 'CB' },
  { label: 'Templates', path: '/templates', icon: 'TP' }
];
const analysisSection = [
  { label: 'Deal Engine', path: '/deal-engine', icon: 'DE' },
  { label: 'Produkt-Intelligenz', path: '/product-intelligence', icon: 'PI' },
  { label: 'Logik-Zentrale', path: '/learning', icon: 'LZ', adminOnly: true }
];
const outputSection = [
  { label: 'Werbung', path: '/advertising', icon: 'AD' },
  { label: 'Publishing', path: '/publishing', icon: 'PB' },
  { label: 'Logs', path: '/logs', icon: 'LG' }
];
const automationSection = [
  { label: 'Autobot', path: '/autobot', icon: 'AB' },
  { label: 'Sperrzeiten', path: '/sperrzeiten', icon: 'SZ' }
];
const settingsSection = [
  { label: 'Einstellungen', path: '/settings', icon: 'ES' }
];

function Sidebar() {
  const { user, logout } = useAuth();
  const navigationSections = [
    { title: 'Dashboard', note: 'Live Status und Sofortzugriffe', items: overviewSection },
    { title: 'Quellen', note: 'Input, Vorlagen und Importwege', items: sourceSection },
    {
      title: 'Analyse',
      note: 'Regeln, Preisanker und Schutzschichten',
      items: analysisSection.filter((item) => !item.adminOnly || user?.role === 'admin')
    },
    { title: 'Output', note: 'Publishing, Werbung und Laufzeit-Logs', items: outputSection },
    { title: 'Automationen', note: 'Jobs, Sperrzeiten und automatische Pfade', items: automationSection },
    { title: 'Einstellungen', note: 'Zugaenge, System und Sicherheit', items: settingsSection }
  ];

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
            <p className="sidebar-role">Rolle: {user?.role}</p>
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

      <button className="secondary full sidebar-logout" onClick={logout}>
        Sitzung beenden
      </button>
    </aside>
  );
}

export default Sidebar;
