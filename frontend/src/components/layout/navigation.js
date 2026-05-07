const overviewSection = [{ label: 'Dashboard', path: '/', icon: 'DB' }];
const sourceSection = [
  { label: 'Generator', path: '/generator', icon: 'GN' },
  { label: 'Scraper', path: '/scraper', icon: 'SC' },
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
  { label: 'Telegram Output', path: '/publishing/telegram', icon: 'TG' },
  { label: 'WhatsApp Output', path: '/publishing/whatsapp', icon: 'WA' },
  { label: 'Output Steuerung', path: '/settings?tab=output', icon: 'OS' },
  { label: 'Logs', path: '/publishing/logs', icon: 'LG' }
];
const automationSection = [
  { label: 'Autobot', path: '/autobot', icon: 'AB' },
  { label: 'Sperrzeiten', path: '/sperrzeiten', icon: 'SZ' }
];
const settingsSection = [{ label: 'Einstellungen', path: '/settings', icon: 'ES' }];

export function getNavigationSections(role = '') {
  const normalizedRole = String(role || '').toLowerCase();
  return [
    { title: 'Dashboard', note: 'Live Status und Sofortzugriffe', items: overviewSection },
    { title: 'Quellen', note: 'Input, Vorlagen und Importwege', items: sourceSection },
    {
      title: 'Analyse',
      note: 'Regeln, Preisanker und Schutzschichten',
      items: analysisSection.filter((item) => !item.adminOnly || normalizedRole === 'admin')
    },
    { title: 'Output', note: 'Publishing, Werbung und Laufzeit-Logs', items: outputSection },
    { title: 'Automationen', note: 'Jobs, Sperrzeiten und automatische Pfade', items: automationSection },
    { title: 'Einstellungen', note: 'Zugaenge, System und Sicherheit', items: settingsSection }
  ];
}

export function getFlatNavigation(role = '') {
  return getNavigationSections(role).flatMap((section) => section.items);
}

export function getMobilePrimaryNavigation(role = '') {
  const flatNavigation = getFlatNavigation(role);
  const preferredPaths = ['/', '/generator', '/copybot', '/deal-engine', '/publishing'];

  return preferredPaths
    .map((path) => flatNavigation.find((item) => item.path === path))
    .filter(Boolean);
}
