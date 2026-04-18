export const quickLinks = [
  { title: 'Generator Poster', path: '/generator', icon: '\u{1F4DD}' },
  { title: 'Lern-Logik', path: '/learning', icon: '\u{1F50C}' },
  { title: 'Copybot', path: '/copybot', icon: '\u{1F5C2}' },
  { title: 'Logs', path: '/logs', icon: '\u{1F4CB}' }
];

export const homeCards = [
  { title: 'Offene Posts', value: '8', subtitle: 'fuer Telegram und WhatsApp bereit' },
  { title: 'Lern-Logik', value: 'Live', subtitle: 'Keepa- und Feedback-Logik getrennt angebunden' },
  { title: 'Letzter Status', value: 'Stabil', subtitle: 'Frontend und Backend arbeiten gemeinsam' }
];

export const templates = [
  { id: 1, type: 'Textbaustein', label: 'Standard-Post', content: 'Jetzt im Angebot! Gleich sichern.' },
  { id: 2, type: 'CTA', label: 'Schnell zugreifen', content: 'Nur kurze Zeit verfuegbar.' },
  { id: 3, type: 'Preis', label: 'Deal', content: 'Jetzt statt 49,99 EUR nur 29,99 EUR' }
];

export const scraperItems = [
  { id: 1, title: 'Headset', status: 'Aktiv', price: '24,90 EUR', action: 'Ueberpruefen' },
  { id: 2, title: 'Wasserkocher', status: 'Rabatt', price: '19,49 EUR', action: 'Posten' },
  { id: 3, title: 'Smartwatch', status: 'Blitzangebot', price: '79,99 EUR', action: 'Sichern' }
];

export const botData = {
  status: 'Bereit',
  queue: 5,
  reviewed: 12,
  entries: [
    { id: 1, title: 'Coupon pruefen', status: 'Warten' },
    { id: 2, title: 'Preisupdate', status: 'Freigabe' }
  ]
};

export const logs = [
  { id: 1, user: 'Lena', action: 'Generator gespeichert', time: '3m', status: 'erfolgreich' },
  { id: 2, user: 'Tobias', action: 'Template aktualisiert', time: '15m', status: 'erfolgreich' },
  { id: 3, user: 'Jan', action: 'Scraper geprueft', time: '42m', status: 'pending' }
];
