export const quickLinks = [
  { title: 'Generator Poster', path: '/generator', icon: '📝' },
  { title: 'Scraper', path: '/scraper', icon: '🔍' },
  { title: 'Autobot', path: '/autobot', icon: '🤖' },
  { title: 'Logs', path: '/logs', icon: '📋' }
];

export const homeCards = [
  { title: 'Offene Posts', value: '8', subtitle: 'für Telegram & WhatsApp bereit' },
  { title: 'Autobot Queue', value: '5', subtitle: 'Vorschläge in Prüfung' },
  { title: 'Letzter Status', value: 'Live', subtitle: 'System läuft stabil' }
];

export const templates = [
  { id: 1, type: 'Textbaustein', label: 'Standard-Post', content: 'Jetzt im Angebot! Gleich sichern.' },
  { id: 2, type: 'CTA', label: 'Schnell zugreifen', content: 'Nur kurze Zeit verfügbar.' },
  { id: 3, type: 'Preis', label: 'Deal', content: 'Jetzt statt 49,99€ nur 29,99€' }
];

export const scraperItems = [
  { id: 1, title: 'Headset', status: 'Aktiv', price: '24,90€', action: 'Überprüfen' },
  { id: 2, title: 'Wasserkocher', status: 'Rabatt', price: '19,49€', action: 'Posten' },
  { id: 3, title: 'Smartwatch', status: 'Blitzangebot', price: '79,99€', action: 'Sichern' }
];

export const botData = {
  status: 'Bereit',
  queue: 5,
  reviewed: 12,
  entries: [
    { id: 1, title: 'Coupon prüfen', status: 'Warten' },
    { id: 2, title: 'Preisupdate', status: 'Freigabe' }
  ]
};

export const logs = [
  { id: 1, user: 'Lena', action: 'Generator gespeichert', time: '3m', status: 'erfolgreich' },
  { id: 2, user: 'Tobias', action: 'Template aktualisiert', time: '15m', status: 'erfolgreich' },
  { id: 3, user: 'Jan', action: 'Scraper geprüft', time: '42m', status: 'pending' }
];

export const team = [
  { id: 1, name: 'Lena Müller', role: 'admin', status: 'aktiv' },
  { id: 2, name: 'Tobias Klein', role: 'editor', status: 'aktiv' },
  { id: 3, name: 'Sofie Rehm', role: 'poster', status: 'pausiert' },
  { id: 4, name: 'Jan Richter', role: 'viewer', status: 'aktiv' }
];
