import { NavLink } from 'react-router-dom';
import { useAuth } from '../../context/AuthContext';

const overviewSection = [{ label: 'Dashboard', path: '/', icon: '\u{1F3E0}' }];
const sourceSection = [
  { label: 'Generator', path: '/generator', icon: '\u{1F4DD}' },
  { label: 'Scrapper', path: '/scraper', icon: '\u{1F50D}' },
  { label: 'Copybot', path: '/copybot', icon: '\u{1F4E8}' },
  { label: 'Templates', path: '/templates', icon: '\u{1F9F1}' }
];
const regulatorSection = [
  { label: 'Autobot', path: '/autobot', icon: '\u2699\uFE0F' },
  { label: 'Deal Engine', path: '/deal-engine', icon: '\u2696\uFE0F' },
  { label: 'Produkt-Intelligenz', path: '/product-intelligence', icon: '\u{1F4E6}' },
  { label: 'Logik-Zentrale', path: '/learning', icon: '\u{1F9E0}', adminOnly: true }
];
const outputSection = [
  { label: 'Werbung', path: '/advertising', icon: '\u{1F4E3}' },
  { label: 'Publishing', path: '/publishing', icon: '\u{1F4E4}' },
  { label: 'Sperrzeiten', path: '/sperrzeiten', icon: '\u23F2\uFE0F' },
  { label: 'Logs', path: '/logs', icon: '\u{1F4CA}' },
  { label: 'Einstellungen', path: '/settings', icon: '\u{1F527}' }
];

function Sidebar() {
  const { user, logout } = useAuth();
  const navigationSections = [
    { title: 'Dashboard', items: overviewSection },
    { title: 'Quellen', items: sourceSection },
    { title: 'Regler', items: regulatorSection.filter((item) => !item.adminOnly || user?.role === 'admin') },
    { title: 'Output', items: outputSection }
  ];

  return (
    <aside className="sidebar card" style={{ padding: '1.25rem', minWidth: '240px', maxWidth: '300px' }}>
      <div style={{ marginBottom: '1.25rem', display: 'grid', gap: '0.7rem' }}>
        <div>
          <p className="section-title">Affiliate Manager Pro</p>
          <p style={{ margin: 0, fontSize: '0.95rem', color: '#cbd5e1' }}>Rolle: {user?.role}</p>
        </div>
        <div className="sidebar-flow-note">
          <strong>Deal-Flow</strong>
          <span>Sperrcheck -&gt; Internetvergleich -&gt; Keepa Fallback -&gt; Queue -&gt; Publisher</span>
        </div>
      </div>

      <nav style={{ display: 'grid', gap: '1rem' }}>
        {navigationSections.map((section) => (
          <div key={section.title} style={{ display: 'grid', gap: '0.65rem' }}>
            <p className="section-title" style={{ marginBottom: 0 }}>
              {section.title}
            </p>
            {section.items.map((item) => (
              <NavLink
                key={item.path}
                to={item.path}
                end={item.path === '/'}
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
          </div>
        ))}
      </nav>

      <button className="secondary full" style={{ marginTop: '1.5rem' }} onClick={logout}>
        Abmelden
      </button>
    </aside>
  );
}

export default Sidebar;
