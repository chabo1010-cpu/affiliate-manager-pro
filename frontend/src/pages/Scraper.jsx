import { useState } from 'react';
import Layout from '../components/layout/Layout';
import { scraperItems } from '../data/mock';

function ScraperPage() {
  const [status, setStatus] = useState('');

  return (
    <Layout>
      <div style={{ display: 'grid', gap: '1rem' }}>
        <section className="card" style={{ padding: '1.25rem' }}>
          <p className="section-title">Scraper</p>
          <h2 style={{ margin: '0.25rem 0 1rem', fontSize: '1.5rem' }}>Gefundene Produkte</h2>
          <p className="text-muted">Mock-Daten fuer schnelle Kontrolle.</p>
        </section>

        {status && (
          <section className="card" style={{ padding: '1rem' }}>
            <p style={{ margin: 0 }}>{status}</p>
          </section>
        )}

        <div style={{ display: 'grid', gap: '0.85rem' }}>
          {scraperItems.map((item) => (
            <section key={item.id} className="card" style={{ padding: '1rem', display: 'grid', gap: '0.5rem' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem' }}>
                <div>
                  <h3 style={{ margin: 0 }}>{item.title}</h3>
                  <p className="text-muted" style={{ margin: '0.35rem 0 0' }}>{item.status}</p>
                </div>
                <span className="badge">{item.price}</span>
              </div>
              <button
                type="button"
                className="secondary small"
                onClick={() =>
                  setStatus(
                    `"${item.title}" ist derzeit nur als sichere Vorschau sichtbar, weil fuer den Scraper keine riskante Live-Import-Logik ohne erlaubte Quelle aktiviert wird.`
                  )
                }
              >
                {item.action}
              </button>
            </section>
          ))}
        </div>
      </div>
    </Layout>
  );
}

export default ScraperPage;
