import { useState } from 'react';
import Layout from '../components/layout/Layout';
import { templates } from '../data/mock';

function TemplatesPage() {
  const [status, setStatus] = useState('');

  return (
    <Layout>
      <div style={{ display: 'grid', gap: '1rem' }}>
        <section className="card" style={{ padding: '1.25rem' }}>
          <p className="section-title">Templates</p>
          <h2 style={{ margin: '0.25rem 0 1rem', fontSize: '1.5rem' }}>Textbausteine & CTA-Management</h2>
          <button
            type="button"
            className="primary small"
            onClick={() => setStatus('Der Template-Editor ist als sichere Placeholder vorbereitet und kann spaeter an ein eigenes Backend-Modul angebunden werden.')}
          >
            Neu erstellen
          </button>
        </section>
        {status && (
          <section className="card" style={{ padding: '1rem' }}>
            <p style={{ margin: 0 }}>{status}</p>
          </section>
        )}
        <div className="responsive-grid">
          {templates.map((item) => (
            <section key={item.id} className="card" style={{ padding: '1.25rem' }}>
              <p className="text-muted">{item.type}</p>
              <h3 style={{ margin: '0.5rem 0 0.75rem' }}>{item.label}</h3>
              <p style={{ color: '#cbd5e1' }}>{item.content}</p>
              <button
                type="button"
                className="secondary small"
                style={{ marginTop: '1rem' }}
                onClick={() =>
                  setStatus(`"${item.label}" ist aktuell als gepflegtes Platzhalter-Template hinterlegt und noch nicht an einen persistenten Editor angebunden.`)
                }
              >
                Bearbeiten
              </button>
            </section>
          ))}
        </div>
      </div>
    </Layout>
  );
}

export default TemplatesPage;
