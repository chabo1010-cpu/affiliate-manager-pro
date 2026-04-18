import { NavLink, useLocation } from 'react-router-dom';
import { useAuth } from '../../context/AuthContext';

const workflowSections = [
  {
    id: 'overview',
    title: 'Uebersicht',
    note: 'Dashboard und zentrale Orientierung.',
    items: [{ label: 'Dashboard', path: '/', icon: '\u{1F3E0}' }]
  },
  {
    id: 'work',
    title: 'Arbeitsbereiche',
    note: 'Manuell arbeiten, Rohdeals erfassen, Quellen pruefen.',
    items: [
      { label: 'Generator', path: '/generator', icon: '\u{1F4DD}' },
      { label: 'Scrapper', path: '/scraper', icon: '\u{1F50D}' },
      { label: 'Copybot', path: '/copybot', icon: '\u{1F5C2}' },
      { label: 'Templates', path: '/templates', icon: '\u{1F9E9}' },
      { label: 'Autobot', path: '/autobot', icon: '\u{1F916}' }
    ]
  },
  {
    id: 'output',
    title: 'Output & Verlauf',
    note: 'Versand, Historie, Logs und Einstellungen.',
    items: [
      { label: 'Publishing', path: '/publishing', icon: '\u{1F4E6}' },
      { label: 'Deal Historie', path: '/deal-history', icon: '\u23F2\uFE0F' },
      { label: 'Logs', path: '/logs', icon: '\u{1F4CB}' },
      { label: 'Einstellungen', path: '/settings', icon: '\u2699\uFE0F' }
    ]
  }
];

const learningNavItem = {
  label: 'Logik-Zentrale',
  path: '/learning',
  icon: '\u{1F50C}',
  children: [
    { label: 'Flow Dashboard', path: '/learning' },
    { label: 'Manuelle Suche', path: '/learning/manual-search' },
    { label: 'Automatik', path: '/learning/automatik' },
    { label: 'Ergebnisse', path: '/learning/ergebnisse' },
    { label: 'Benachrichtigungen', path: '/learning/benachrichtigungen' },
    { label: 'Verbrauch & Logs', path: '/learning/verbrauch-logs' },
    { label: 'Fake-Drop Analyse', path: '/learning/fake-drop-analyse' },
    { label: 'Review Queue', path: '/learning/review-queue' },
    { label: 'Lern-Datenbank', path: '/learning/lern-datenbank' },
    { label: 'Einstellungen', path: '/learning/einstellungen' }
  ]
};

function Sidebar() {
  const { user, logout } = useAuth();
  const location = useLocation();
  const normalizedLocationPath = location.pathname.replace('/learning', '/keepa');
  const isAdmin = user?.role === 'admin';
  const navSections = isAdmin
    ? [
        workflowSections[0],
        workflowSections[1],
        {
          id: 'logic',
          title: 'Logik & Quellen',
          note: 'Bewertung, Muster-Unterstuetzung und API-nahe Steuerung.',
          items: [learningNavItem]
        },
        workflowSections[2]
      ]
    : workflowSections;

  return (
    <aside className="sidebar card" style={{ padding: '1.25rem', minWidth: '240px', maxWidth: '300px' }}>
      <div style={{ marginBottom: '1.25rem', display: 'grid', gap: '0.7rem' }}>
        <div>
          <p className="section-title">Affiliate Manager Pro</p>
          <p style={{ margin: 0, fontSize: '0.95rem', color: '#cbd5e1' }}>Rolle: {user?.role}</p>
        </div>
        <div className="sidebar-flow-note">
          <strong>Flow</strong>
          <span>Quelle -&gt; Logik -&gt; Entscheidung -&gt; Output</span>
        </div>
      </div>

      <nav style={{ display: 'grid', gap: '1rem' }}>
        {navSections.map((section) => (
          <section key={section.id} className="sidebar-group">
            <div className="sidebar-group-header">
              <p className="section-title" style={{ marginBottom: '0.35rem' }}>
                {section.title}
              </p>
              <p className="sidebar-group-note">{section.note}</p>
            </div>

            <div style={{ display: 'grid', gap: '0.55rem' }}>
              {section.items.map((item) => {
                const isLearningGroup = item.path === '/learning';
                const showChildren =
                  Boolean(item.children) &&
                  isLearningGroup &&
                  (location.pathname.startsWith('/learning') || location.pathname.startsWith('/keepa'));

                return (
                  <div key={item.path} style={{ display: 'grid', gap: showChildren ? '0.45rem' : 0 }}>
                    <NavLink
                      to={item.path}
                      end={item.path === '/' || item.path === '/learning'}
                      className={({ isActive }) => `nav-link ${isActive || showChildren ? 'active' : ''}`}
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

                    {showChildren && (
                      <div style={{ display: 'grid', gap: '0.35rem', paddingLeft: '0.75rem' }}>
                        {item.children.map((child) => {
                          const normalizedChildPath = child.path.replace('/learning', '/keepa');
                          const isChildActive = normalizedLocationPath === normalizedChildPath;

                          return (
                            <NavLink
                              key={child.path}
                              to={child.path}
                              end={child.path === '/learning'}
                              className={() => `nav-link ${isChildActive ? 'active' : ''}`}
                              style={{
                                padding: '0.65rem 0.85rem',
                                borderRadius: '14px',
                                fontSize: '0.88rem',
                                background: 'rgba(255,255,255,0.02)',
                                color: '#cbd5e1'
                              }}
                            >
                              {child.label}
                            </NavLink>
                          );
                        })}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          </section>
        ))}
      </nav>

      <button className="secondary full" style={{ marginTop: '1.5rem' }} onClick={logout}>
        Abmelden
      </button>
    </aside>
  );
}

export default Sidebar;
